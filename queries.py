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

import boto.sdb
# AWS Boto API http://aws.amazon.com/sdkforpython/

# http://docs.aws.amazon.com/general/latest/gr/rande.html#sdb_region
AWS_EAST_VA_REGION = 'us-east-1'
AWS_WEST_OR_REGION = 'us-west-2'

DETECTOR_DOMAIN = 'TeamA_Detector'
LOOPDATA_DOMAIN = 'TeamA_LoopData'
STATION_DOMAIN  = 'TeamA_Station'


def show_domains_stat():
    """Print some stat about on the three domains"""
    print("Name:\tDetector\tLoopData\tStation")
    for attr in dir(detector_meta):
        if attr.startswith('_'):
            continue
        print("%s:\t%s\t%s\t%s" % (attr,
                                   getattr(detector_meta, attr),
                                   getattr(loopdata_meta, attr),
                                   getattr(station_meta, attr)))


def query_top_5_detector():
    """Show 5 items from the detector_domain"""
    d_query = 'SELECT * FROM `%s`' % DETECTOR_DOMAIN
    detectors = detector_dom.select(d_query, max_items=5)
    for detector in detectors:
        print detector


def single_day_station_travel_times():
    """Find travel time for each NB station for 5 minute intervals for
    Sept 22, 2011.
    """
    print('Query a: Single-Day Station Travel Times')
    print(NotImplemented)


def hourly_corridor_travel_times(from_station_name, to_station_name,
                                 highway_name, short_direction):
    """Find travel time for the entire I-205 NB freeway
    section in the data set (Sunnyside Rd to the river - all NB stations
    in the data set) for each hour in the 2-month test period.
    """
    print('Query b: Hourly Corridor Travel Times')
    print(NotImplemented)


def mid_weekday_peak_period_travel_times():
    """Find the average travel time for 7-9AM and 4-6PM on Tuesdays,
    Wednesdays and Thursdays for the I-205 NB freeway during the 2-month test
    period.
    """
    print('Query c: Mid-Weekday Peak Period Travel Times')


    
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
                    l_query = 'SELECT speed FROM `%s` WHERE detectorid = "%s" AND starttime like "%s-%s-%s %s:%s" AND speed is not null ' %(LOOPDATA_DOMAIN, det, year, month, day, hr, per)
                    data = loopdata_dom.select(l_query)
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
                    l_query = 'SELECT speed FROM `%s` WHERE detectorid = "%s" AND starttime like "%s-%s-%s %s:%s" AND speed is not null ' %(LOOPDATA_DOMAIN, det, year, month, day, hr, per)
                    data = loopdata_dom.select(l_query)
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
                    l_query = 'SELECT speed FROM `%s` WHERE detectorid = "%s" AND starttime like "%s-%s-%s %s:%s" AND speed is not null ' %(LOOPDATA_DOMAIN, det, year, month, day, hr, per)
                    data = loopdata_dom.select(l_query)
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

def station_to_Station_travel_times():
    """Find travel time for all station-to-station NB pairs for 8AM on
    Sept 22, 2011.
    """
    print('Query d: Station-to-Station Travel Times')
    print(NotImplemented)


def main():
    print("-" * 50)
    show_domains_stat()

    print("-" * 50)
    query_top_5_detector()

    print("-" * 50)
    single_day_station_travel_times()
    hourly_corridor_travel_times('Sunnyside NB', 'Columbia to I-205 NB',
                                 'I-205', 'N')
    mid_weekday_peak_period_travel_times()
    station_to_Station_travel_times()


if __name__ == '__main__':
    # Store aws_access credential in Boto config file (not in source code)
    # http://boto.readthedocs.org/en/latest/boto_config_tut.html
    #   for Linux, /etc/boto.cfg or ~/.boto
    #   for Windows create BOTO_CONFIG environment variable that points to the config file

    conn = boto.sdb.connect_to_region(AWS_WEST_OR_REGION, aws_access_key_id='AKIAJX2UIFIEVJ3LFLQQ', aws_secret_access_key='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')

    #print(conn.get_all_domains())
    detector_dom = conn.get_domain(DETECTOR_DOMAIN)
    loopdata_dom = conn.get_domain(LOOPDATA_DOMAIN)
    station_dom = conn.get_domain(STATION_DOMAIN)

    #print(detector_dom, loopdata_dom, station_dom)
    detector_meta = conn.domain_metadata(detector_dom)
    loopdata_meta = conn.domain_metadata(loopdata_dom)
    station_meta = conn.domain_metadata(station_dom)

    main()
