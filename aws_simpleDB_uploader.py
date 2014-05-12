import datetime
import multiprocessing
import os
import time
import uuid
import exceptions

import boto.sdb
import boto.exception


AWS_WEST_OR_REGION = 'us-west-2'


def get_all_files(root_dir, recursive=True, ext_filter=None):
    """Walk from the root_dir to retrieve a list of files
    @param: root_dir - Path to the root directory
    @param: recursive - set to True to recursively get all files
    @param: ext_filter - file extension filter
    @return: a list of file paths
    """
    files = []
    for (dirpath, dirnames, filenames) in os.walk(root_dir):
        for filename in filenames:
            if ext_filter:
                _, f_ext = os.path.splitext(filename)
                if f_ext.replace('.', '').lower() != ext_filter.lower():
                    continue
            files.append(os.path.join(dirpath, filename))
        if recursive:
            for dirname in dirnames:
                files.extend(get_all_files(os.path.join(dirpath, dirname),
                                           recursive, ext_filter))
    return files


def sdb_batch_put((file_path, chunk_count, chunk_index, sdb_conn, domain_name,
                  column_header, key_column, column_delimiter, show_progress)):
    """Upload all lines in the line iterator object to the SimpleDB domain.
    @param: a tuple of args.  Needed to unpack multiple params from a single
    param that multiprocess limiting it.
        @subparam: file_handler - the file object
        @param: chunk_count - the number of section to divide this file into
        @param: chunk_index - the index of the chunk to read from
        @param: lines - an line iterator
        @param: sdb_conn - a boto.sdb connection
        @param: domain_name - a SimpleDB domain object
        @param: column_header - a delimited column name
        @param: key_column - the field to be use as simpleDB ItemName()
        @param: column_delimiter - a delimiter to for the column's header and
                                   each line in the lines iterator
        @param: show_progress - Show the progess of item processed so far
    """

    # AWS SimpleDB API limit
    SIMPLE_DB_BATCH_LIMIT = 25
    # Counter to show progress
    item_counter = 0
    # A set of items to upload in a batch
    items_batch = {}
    attributes = column_header.split(column_delimiter)
    item_domain = sdb_conn.get_domain(domain_name)
    print("pid: %s\t%s\t%s" % (os.getpid(), str(datetime.datetime.now()),
                               "Started"))
    with open(file_path, "r") as file_handler:
        for line in file_chunker(file_handler, chunk_count, chunk_index):
            line = line.strip()
            item = dict(zip(attributes, line.split(column_delimiter)))
            if key_column:
                item_name = item[key_column]
            else:
                item_name = uuid.uuid4()
            items_batch[item_name] = dict(zip(attributes,
                                              line.split(column_delimiter)))
            if len(items_batch) == SIMPLE_DB_BATCH_LIMIT:
                item_counter += len(items_batch)
                if show_progress and item_counter % 100000 < SIMPLE_DB_BATCH_LIMIT:
                    print("pid: %s\t%s\t%s" %
                          (os.getpid(), str(datetime.datetime.now()),
                           item_counter))
                item_domain.batch_put_attributes(items_batch, replace=True)
                items_batch = {}
        # Put the last batch
        if items_batch:
            item_domain.batch_put_attributes(items_batch, replace=True)
            if show_progress:
                item_counter += len(items_batch)
                print("pid: %s\t%s\t%s == Done!" %
                      (os.getpid(), str(datetime.datetime.now()), item_counter))

    return item_counter


def file_chunker(file_handler, chunk_count, chunk_index):
    """Access a chunk of a file
    @param: file_handler - the file object
    @param: chunk_count - the number of section to divide this file into
    @param: chunk_index - the index of the chunk to read from

    @yield: lines of the chunk identified by the chunk_index
    """
    if 1 > chunk_count:
        raise exceptions.ValueError("Chunk count must be > 0 ")
    if chunk_index > chunk_count:
        raise exceptions.IndexError("Chunk index out of bound")
    # seek to the end of file
    file_handler.seek(0, 2)
    file_size = file_handler.tell()
    # in case file is too small
    chunk_size = max(1, file_size // chunk_count)
    chunk_start = chunk_index * chunk_size
    chunk_end = chunk_start + chunk_size

    # Set the file position at the start of the first line in the chunk
    if chunk_start == 0:
        file_handler.seek(0)
    else:
        file_handler.seek(chunk_start - 1)
        file_handler.readline()

    # Start yielding line within the chunk identified by the chunk index
    while file_handler.tell() < chunk_end:
        line = file_handler.readline()
        if not line:
            # End of file, readline() include new line character for blank line
            break
        # Next line
        yield line.strip()


def get_item_count(sdb_domain, consistent_read=False):
    """SELECT COUNT(*) FROM sdb_domain
    @param: sdb_domain - an SDB domain object to query the SELECT COUNT(*)
    @consistent_read - set to true for consistent read, default to False
    """
    select_count_query = ("""SELECT COUNT(*) FROM `%s` """ % sdb_domain.name)
    select_count = list(sdb_domain.select(select_count_query,
                                          consistent_read=consistent_read))
    return sum([int(c["Count"]) for c in select_count])


def upload_to_simpleDB(sdb_conn, domain_name, input_dir, column_header,
                       key_column=None, column_delimiter=",", recursive=False,
                       ext_filter=None, reuse_domain=False, worker_count=None):
    """Upload each line in text files found in input_dir to SimpleDB domain
    @param: sdb_conn - A Boto.sdb connection object
    @param: domain_name - The domain name to upload the data to
    @param: input_dir - root of the directory to upload the files to
    @param: column_header - The column header of the content as a string. For
            example "col1,col2,col3" where "," is the column_delimiter
    @param: key_column - The column to be used as item key, this should be
            unique or it may result in attribute with more than one value.
            Default to UUID.uuid.uuid4()
    @param: column_delimiter - The delimiter to parse the column_header and each
            line in the data
    @param: recursive - Set to true to recursive search for all file under the
            input_dir
    @param: ext_filter - The file extension to filter in the input_dir.
    @param: reuse_domain - Set to true to use an existing domain if one exists.
            Be careful when setting this option to true as it may pollute the
            existing data.  You have been warned!
    @param: work_count - The number of process to divide each file to run in
            parallel.  Depend on the number of row in each file, setting this
            number high to about max out your network speed as SimpleDB send
            request of 25 items per batch put attributes request.  Default to
            number of CPU count.
    """
    files = get_all_files(input_dir, recursive=recursive, ext_filter=ext_filter)
    total_lines = get_line_count(input_dir, recursive=recursive,
                                 ext_filter=ext_filter)
    file_count = len(files)
    print("-" * 20)
    print("Found %s file%s" % (file_count, "s" if file_count > 1 else ""))
    for f in files:
        print("\t%s" % f)
    print("-" * 20)
    print("Total of %s line%s" % (total_lines, "s" if total_lines > 1 else ""))
    print("-" * 20)

    if file_count == 0:
        # Nothing to do
        return
    try:
        # The domain exists, use it
        destination_domain = sdb_conn.get_domain(domain_name)
        if not reuse_domain:
            raise exceptions.RuntimeError("Domain '%s' already exists and "
                                          "'reuse_domain' is set to %s.  Be "
                                          "careful when uploading data to an "
                                          "existing domain as the new data may "
                                          "pollute the existing data int the "
                                          "domain."
                                          % (domain_name, reuse_domain))
        print("Selected existing domain '%s'" % domain_name)
    except boto.exception.SDBResponseError:
        # The domain does not exist, create it
        destination_domain = sdb_conn.create_domain(domain_name)
        print("Created new domain '%s'" % domain_name)
        # Delay to let the new domain to be consistent
        n = 5
        print("Delaying %s seconds after created new domain" % n)
        time.sleep(n)

    count_before = get_item_count(destination_domain, consistent_read=True)

    # Divide the file into multiple chunks for concurrent process
    # Normally, the worker pool should line up with the multiprocessing.cpu_count().
    # However, since most of the work will be querying AWS, so it is ok to wait.
    if not worker_count:
        worker_count = multiprocessing.cpu_count()
    workers = multiprocessing.Pool(worker_count)

    print("Before upload starts item count: %s" % count_before)
    time_before = datetime.datetime.now()
    for current_file in files:
        print("_" * 10)
        print("Begin upload the content of '%s' to '%s' domain" %
              (current_file, domain_name))
        sdb_batch_put_args = zip(
            worker_count * [current_file],
            worker_count * [worker_count],
            [i for i in range(worker_count)],
            worker_count * [sdb_conn],
            worker_count * [domain_name],
            worker_count * [column_header],
            worker_count * [key_column],
            worker_count * [column_delimiter],
            worker_count * [True])
        workers.map(sdb_batch_put, sdb_batch_put_args)
        print("-" * 10)
    count_after = get_item_count(destination_domain, consistent_read=True)
    time_after = datetime.datetime.now()
    print("Before upload starts item count:   %s" % count_before)
    print("After upload completed item count: %s" % count_after)
    print("Upload start:    %s" % str(time_before))
    print("Upload complete: %s" % str(time_after))
    print("-" * 10)
    print("Total upload time %s" % str(time_after - time_before))


def get_line_count(root_dir, recursive=True, ext_filter=None):
    """Count the lines in all files found in under root_dir
    @param: root_dir - directory path to start the search
    @param: recursive - set to True to recursive search
    @param: ext_filter - file to filter based on its extension
    """
    global files, file_stats, f, f1, line_count, total_lines, k, v
    files = get_all_files(root_dir, recursive=recursive, ext_filter=ext_filter)
    file_stats = {}
    for f in files:
        with open(f, 'r') as f1:
            line_count = len(f1.readlines())
            file_stats[f] = line_count
    total_lines = 0
    for k, v in file_stats.items():
        print("%s\t%s" % (v, k))
        total_lines += v
    print("Total lines: %s" % total_lines)
    return total_lines


if __name__ == "__main__":
    sdb_conn = boto.sdb.connect_to_region(AWS_WEST_OR_REGION)
    # Store your AWS access key in 'BOTO_CONFIG' 
    # See Boto's document for more detail.
    
    # Call upload_to_simpleDB to upload csv file to aws_simpleDB
    # upload_to_simpleDB(sdb_conn, domain_name, input_dir, column_header,
    #                   key_column=None, column_delimiter=",", recursive=False,
    #                   ext_filter=None, reuse_domain=False, worker_count=None)
