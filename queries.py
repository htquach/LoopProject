"""
PSU Winter 2014
CS 410/510 Data Management in the Cloud
Course Project

Team A:
Alawini, Abdussalam
Fetters, Tyler
Moen, Jon
Nelson, Jason
Quach, Hong
"""

import exceptions
import multiprocessing
from multiprocessing.pool import ThreadPool

import boto.sdb
# AWS Boto API http://aws.amazon.com/sdkforpython/

# http://docs.aws.amazon.com/general/latest/gr/rande.html#sdb_region

AWS_WEST_OR_REGION = 'us-west-2'

DETECTOR_DOMAIN = 'TeamA_Detector'
LOOP_DOMAIN = 'TeamA_Loop'
STATION_DOMAIN  = 'TeamA_Station'

STATION_CLASS_MAINLINE = '1'
DETECTOR_CLASS_MAINLINE = '1'
LOOP_STATUS_OK = '2'

# Global variables for data access
conn = None
detector_dom = None
loop_dom = None
station_dom = None
detector_meta = None
loop_meta = None
station_meta = None


def show_domains_stat():
    """Print some stat about on the three domains"""
    print("Name:\tDetector\tLoopData\tStation")
    for attr in dir(detector_meta):
        if attr.startswith('_'):
            continue
        print("%s:\t%s\t%s\t%s" % (attr,
                                   getattr(detector_meta, attr),
                                   getattr(loop_meta, attr),
                                   getattr(station_meta, attr)))


def query_top_5_samples():
    """Show 5 items from the each domain"""
    print("Top 5 %" % DETECTOR_DOMAIN)
    d_query = 'SELECT * FROM `%s`' % DETECTOR_DOMAIN
    detectors = detector_dom.select(d_query, max_items=5)
    for detector in detectors:
        print detector

    print("Top 5 %" % STATION_DOMAIN)
    s_query = 'SELECT * FROM `%s`' % STATION_DOMAIN
    stations = station_dom.select(s_query, max_items=5)
    for station in stations:
        print station

    print("Top 5 %" % LOOP_DOMAIN)
    l_query = 'SELECT * FROM `%s`' % LOOP_DOMAIN
    loops = loop_dom.select(l_query, max_items=5)
    for loop in loops:
        print loop


def single_day_station_travel_times():
    """Find travel time for each NB station for 5 minute intervals for
    Sept 22, 2011.
    """
    print('Query a: Single-Day Station Travel Times')
    print(NotImplemented)


def _hourly_speed_group_by((station_id, loop_result_iter)):
    """Helper function to group by the result and process each
    result in the loop_result_iter.  SimpleDB does not support any
    function in select beside COUNT(*), so combining the station_id
    constant and derive hourly starttime need to be done outside
    the query.
    Required attributes in loop_result_iter
        "starttime" with format "YYYY-MM-ddTHH:mm:ss"
        "speed" with format of integer string

    Return a list of tuple (station_id, starttime_hour, speed)
    """
    result = []
    #TODO: test one item for the expected attribute

    #TODO: implement a query select instead of roundtrip every query.

    loop_result = list(loop_result_iter)
    for loop in loop_result:
        try:
            starttime_hour = "%s:00:00" % loop["starttime"][:-6]
            speed = int(loop["speed"])
        except exceptions.ValueError:
            continue
        result.append((station_id, starttime_hour, speed))
    return({station_id: result})


def hourly_corridor_travel_times(from_station_name=None, to_station_name=None,
                                 highway_name=None, short_direction=None):
    """Find travel time for the entire I-205 NB freeway
    section in the data set (Sunnyside Rd to the river - all NB stations
    in the data set) for each hour in the 2-month test period.
    """
    print('Query b: Hourly Corridor Travel Times')
    # 1.  Using a 'for-loop' to query the sequence of stations from the starting
    # station to the ending station (Sunnyside to River).
    # Traverse by 'downstream' station ID.
    # Query the first station
    first_station_query = ("""SELECT * FROM `%s`
        WHERE locationtext = "%s"
            AND highwayname = "%s"
            AND shortdirection = "%s"
            AND stationclass = "%s" """
        % (STATION_DOMAIN, from_station_name, highway_name, short_direction, STATION_CLASS_MAINLINE))
    station_result = list(station_dom.select(first_station_query, max_items=1))

    all_stations = []
    station_id_chain = []
    # Traverse the station through the downstream station attribute until
    # no downstream station or terminated by to_station_name
    while station_result:
        current_station = station_result[0]
        all_stations.append(current_station)
        station_id_chain.append(current_station["stationid"])
        if current_station["locationtext"] == to_station_name:
            break
        station_query = ("""SELECT * FROM `%s`
            WHERE stationid = "%s"
                AND stationclass = "%s" """
            % (STATION_DOMAIN, current_station["downstream"], STATION_CLASS_MAINLINE))
        station_result = list(station_dom.select(station_query, max_items=1))
    # Frozen list to maintain sequence
    station_id_chain = tuple(station_id_chain)
    print("Station IDs chain:  %s" % (" --> ".join(station_id_chain)))

    # 2.  Query the list of detectors in the list of stations found in step 1
    all_detectors = []
    detector_ids_by_station_chain = {}
    for station_id in station_id_chain:
        detector_query = ("""SELECT * FROM `%s`
            WHERE stationid="%s"
                AND detectorclass = "%s" """
            % (DETECTOR_DOMAIN, station_id, DETECTOR_CLASS_MAINLINE))
        detector_result = list(detector_dom.select(detector_query))
        all_detectors.extend(detector_result)
        detector_ids_by_station_chain[station_id] = [d["detectorid"] for d in detector_result]

    print("Detector IDs group by Station ID chain:")
    for station_id in station_id_chain:
        print("%s: %s" % (station_id, detector_ids_by_station_chain[station_id]))

    # 3.  Query all loop data having detector ID in the list of detectors found in step 2

    loop_query_by_station = {}
    # Build a query to select all loop data for each station
    for station_id in station_id_chain:
        query_or_clauses = []
        for detector_id in detector_ids_by_station_chain[station_id]:
            query_or_clauses.append('detectorid="%s"' % (detector_id))
        all_loops_query = ("""SELECT starttime, speed FROM `%s`
            WHERE status = "%s"
                AND speed IS NOT NULL
                AND speed != ""
                AND (%s) """
            % (LOOP_DOMAIN, LOOP_STATUS_OK, " OR ".join(query_or_clauses)))
        all_loops_query += " LIMIT 10"
        loop_query_by_station[station_id] = all_loops_query

    # 4.  Using a mapper to map speed by starttime and station ID.
    # Because SimpleDB can store multiple values in one attributes,
    # we will map all the speed data of a given station within a given
    # hour into a value of that station with the starttime as key.
    hourly_average_speed_by_station = {}
    group_by_args1 = []
    group_by_args2 = []
    for station_id, loop_query in loop_query_by_station.items():
        print("Query for station ID# %s" % station_id)
        print(loop_query)
        #Debug note: apply the max_items for sample result
        loop_result_iter = loop_dom.select(loop_query)
        group_by_args1.append(station_id)
        group_by_args2.append(loop_result_iter)

    groupers = multiprocessing.Pool(multiprocessing.cpu_count()*2)
    for result in groupers.map(_hourly_speed_group_by, zip(group_by_args1, group_by_args2)):
        # TODO pick one and remove the other, depend on the reduce step.
        # save to memory
        hourly_average_speed_by_station[result.keys()[0]] = result[result.keys()[0]]
        # save to disc
        with open('query_2_station_%s_loop_hourly.txt' % result.keys()[0], 'w') as result_file:
            result_file.write("\n".join(result[result.keys()[0]]))

    # 5.  Reduce to starhour, travelduration
    # TODO:  implement reducer
    print("Still need to reduce the data.  Do it here or use AWS EMR")


def mid_weekday_peak_period_travel_times():
    """Find the average travel time for 7-9AM and 4-6PM on Tuesdays,
    Wednesdays and Thursdays for the I-205 NB freeway during the 2-month test
    period.
    """
    print('Query c: Mid-Weekday Peak Period Travel Times')

    
    s_query1 = 'SELECT stationid, length_mid  FROM `%s` WHERE shortdirection = "N" AND highwayname ="I-205" and length_mid != ""' %STATION_DOMAIN
    
    
    stations = station_dom.select(s_query1)
        
    sDict = {}
    sList = []
    for ea in stations:
        print ea
        sDict[ea['stationid']] = ea['length_mid']
        sList.append(ea['stationid'])


    stationCount = len(sList)

    dList = []
    dDict = {}
    for ea in range(0, len(sList)):
        detector_query = 'SELECT detectorid From `%s` WHERE stationid = "%s"' %(STATION_DOMAIN, sList[ea])
        
        print detector_query
        d_query = detector_dom.select(detector_query)
        for i in d_query:
            dDict[i['detectorid']] = sDict[sList[ea]]
            dList.append(i['detectorid'])




    """#NEED TO STILL GET dLIST SOMEHOW
    dList = [1345,1346,1347,1348,1353,1354,1355,1361,1362,1363,1369,1370,1371,1809,1810,1811,1941,1942,1943,1949,1950,1951]

    detectorCount = len(dList)

    #NEED TO CREAT FUNCTION FOR detector mid length dDict
    dDict = {1345 : 0.94,1346 : 0.94,1347 : 0.94,1348 : 0.94,1353 : 1.89,1354 : 1.89,1355 : 1.89,1361 : 1.6,1362 : 1.6,1363 : 1.6,1369 : 0.86,1370 : 0.86,1371 : 0.86,1809 : 0.84,1810 : 0.84,1811 : 0.84,1941 : 2.14,1942 : 2.14,1943 : 2.14,1949 : 1.82,1950 : 1.82,1951 : 1.82}"""

    resCount = 0
    totalTime = 0.0

    file = open('results.txt', 'w')

    per = "%"
    year = 2011
    #------------------------------------ September ------------------------------------
    start = 258
    month = "09"
    begin = 15
    end = 30
    for day in range (begin, end):
        if (start % 7) in [4,5,6]:
            for det in dList:
                for hr in  ["07","08","16","17"]:
                    l_query = 'SELECT speed FROM `%s` WHERE detectorid = "%s" AND starttime like "%s-%s-%s %s:%s" AND speed is not null ' %(LOOP_DOMAIN, det, year, month, day, hr, per)
                    data = loop_dom.select(l_query)
                    for d in data:
                        resCount += 1
                            #need to determine what impact 0 speeds will have on the results
                        if not (float(d['speed']) == 0):
                            file.write('%s\n' %(float(dDict[det])/float(d['speed'])))
                            totalTime += float(dDict[det])/float(d['speed'])
        start += 1

    #------------------------------------ October ------------------------------------
    start = 274
    month = 10
    begin = 1
    end = 31
    for day in range (begin, end):
        if (start % 7) in [4,5,6]:
            for det in dList:
                for hr in  ["07","08","16","17"]:
                    l_query = 'SELECT speed FROM `%s` WHERE detectorid = "%s" AND starttime like "%s-%s-%s %s:%s" AND speed is not null ' %(LOOP_DOMAIN, det, year, month, day, hr, per)
                    data = loop_dom.select(l_query)
                    for d in data:
                        resCount += 1
                        #need to determine what impact 0 speeds will have on the results
                        if not (float(d['speed']) == 0):
                            file.write('%s\n' %(float(dDict[det])/float(d['speed'])))
                            totalTime += float(dDict[det])/float(d['speed'])
        start += 1

    #------------------------------------ December ------------------------------------
    start = 305
    month = 11
    begin = 1
    end = 15
    for day in range (begin, end):
        if (start % 7) in [4,5,6]:
            for det in dList:
                for hr in  ["07","08","16","17"]:
                    l_query = 'SELECT speed FROM `%s` WHERE detectorid = "%s" AND starttime like "%s-%s-%s %s:%s" AND speed is not null ' %(LOOP_DOMAIN, det, year, month, day, hr, per)
                    data = loop_dom.select(l_query)
                    for d in data:
                        resCount += 1
                        #need to determine what impact 0 speeds will have on the results
                        if not (float(d['speed']) == 0):
                            file.write('%s\n' %(float(dDict[det])/float(d['speed'])))
                            totalTime += float(dDict[det])/float(d['speed'])
        start += 1

    file.close()
    #decide if running map reduce job makes sense or not

    print totalTime
    print resCount
                                          
    print "Average Commute Time: ", totalTime/(resCount/stationCount)

    
    s_query1 = 'SELECT stationid  FROM `%s` WHERE Shortdirection = "N" AND highwayname ="I-205" and detectorclass="1"' %DETECTOR_DOMAIN
    
    stations = detector_dom.select(s_query1)
        
    sDict = {}
    sList = []
    for ea in stations:
        sDict[ea['stationid']] = 0
        sList.append(ea['stationid'])

    sList2 = []
    for ea in sList:
        if ea not in sList2:
            sList2.append(ea)

    sList = sList2
    stationCount = len(sList)

    for ea in sList:
        length_query = 'SELECT length_mid From `%s` WHERE itemName() = "%s"' %(STATION_DOMAIN, ea)
        s_Length = station_dom.select(length_query)
        for i in s_Length:
            sDict[ea] = i['length_mid']


    #NEED TO STILL GET dLIST SOMEHOW
    dList = [1345,1346,1347,1348,1353,1354,1355,1361,1362,1363,1369,1370,1371,1809,1810,1811,1941,1942,1943,1949,1950,1951]

    detectorCount = len(dList)

    #NEED TO CREAT FUNCTION FOR detector mid length dDict
    dDict = {1345 : 0.94,1346 : 0.94,1347 : 0.94,1348 : 0.94,1353 : 1.89,1354 : 1.89,1355 : 1.89,1361 : 1.6,1362 : 1.6,1363 : 1.6,1369 : 0.86,1370 : 0.86,1371 : 0.86,1809 : 0.84,1810 : 0.84,1811 : 0.84,1941 : 2.14,1942 : 2.14,1943 : 2.14,1949 : 1.82,1950 : 1.82,1951 : 1.82}

    resCount = 0
    totalTime = 0.0

    file = open('results.txt', 'w')

    per = "%"
    year = 2011
    #------------------------------------ September ------------------------------------
    start = 258
    month = "09"
    begin = 15
    end = 30
    for day in range (begin, end):
        if (start % 7) in [4,5,6]:
            for det in dList:
                for hr in  ["07","08","16","17"]:
                    l_query = 'SELECT speed FROM `%s` WHERE detectorid = "%s" AND starttime like "%s-%s-%s %s:%s" AND speed is not null ' %(LOOP_DOMAIN, det, year, month, day, hr, per)
                    data = loop_dom.select(l_query)
                    for d in data:
                        resCount += 1
                            #need to determine what impact 0 speeds will have on the results
                        if not (float(d['speed']) == 0):
                            file.write('%s\n' %(float(dDict[det])/float(d['speed'])))
                            totalTime += float(dDict[det])/float(d['speed'])
        start += 1

    #------------------------------------ October ------------------------------------
    start = 274
    month = 10
    begin = 1
    end = 31
    for day in range (begin, end):
        if (start % 7) in [4,5,6]:
            for det in dList:
                for hr in  ["07","08","16","17"]:
                    l_query = 'SELECT speed FROM `%s` WHERE detectorid = "%s" AND starttime like "%s-%s-%s %s:%s" AND speed is not null ' %(LOOP_DOMAIN, det, year, month, day, hr, per)
                    data = loop_dom.select(l_query)
                    for d in data:
                        resCount += 1
                        #need to determine what impact 0 speeds will have on the results
                        if not (float(d['speed']) == 0):
                            file.write('%s\n' %(float(dDict[det])/float(d['speed'])))
                            totalTime += float(dDict[det])/float(d['speed'])
        start += 1

    #------------------------------------ December ------------------------------------
    start = 305
    month = 11
    begin = 1
    end = 15
    for day in range (begin, end):
        if (start % 7) in [4,5,6]:
            for det in dList:
                for hr in  ["07","08","16","17"]:
                    l_query = 'SELECT speed FROM `%s` WHERE detectorid = "%s" AND starttime like "%s-%s-%s %s:%s" AND speed is not null ' %(LOOP_DOMAIN, det, year, month, day, hr, per)
                    data = loop_dom.select(l_query)
                    for d in data:
                        resCount += 1
                        #need to determine what impact 0 speeds will have on the results
                        if not (float(d['speed']) == 0):
                            file.write('%s\n' %(float(dDict[det])/float(d['speed'])))
                            totalTime += float(dDict[det])/float(d['speed'])
        start += 1

    file.close()
    #decide if running map reduce job makes sense or not

    print totalTime
    print resCount
                                          
    print "Average Commute Time: ", totalTime/(resCount/stationCount)


def station_to_station_travel_times():
    """Find travel time for all station-to-station NB pairs for 8AM on
    Sept 22, 2011.
    """
    print('Query d: Station-to-Station Travel Times')
    print(NotImplemented)


def init_conn():
    """Initialize global variables for data connection"""
    global conn, detector_dom, loop_dom, station_dom, detector_meta, loop_meta, station_meta

    # Store aws_access credential in Boto config file (not in source code)
    # http://boto.readthedocs.org/en/latest/boto_config_tut.html
    #   for Linux, /etc/boto.cfg or ~/.boto
    #   for Windows create BOTO_CONFIG environment variable that points to the
    #   config file
    conn = boto.sdb.connect_to_region(AWS_WEST_OR_REGION)  #DO NOT SPECIFY KEY

    #print(conn.get_all_domains())
    detector_dom = conn.get_domain(DETECTOR_DOMAIN)
    loop_dom = conn.get_domain(LOOP_DOMAIN)
    station_dom = conn.get_domain(STATION_DOMAIN)

    #print(detector_dom, loopdata_dom, station_dom)
    detector_meta = conn.domain_metadata(detector_dom)
    loop_meta = conn.domain_metadata(loop_dom)
    station_meta = conn.domain_metadata(station_dom)


def main():
    """Show the domain summary and run each query one at a time."""
    # show_domains_stat()
    # print("-" * 50)

    # query_top_5_detector()
    # print("-" * 50)

    ##Jason
    # single_day_station_travel_times()
    # print("-" * 50)

    ##Hong
    # hourly_corridor_travel_times(from_station_name='Sunnyside NB',
    #                              to_station_name='Columbia to I-205 NB',
    #                              highway_name='I-205',
    #                              short_direction='N')
    # print("-" * 50)

    ##Tyler -- About 7 minutes
    #mid_weekday_peak_period_travel_times()
    #print("-" * 50)

    ##Jon
    #station_to_station_travel_times()
    #print("-" * 50)


if __name__ == '__main__':

    init_conn()

    main()
