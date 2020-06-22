#!/usr/bin/env python

# -*- coding: utf-8 -*-
"""A script to process items from a redis queue."""
from __future__ import print_function, unicode_literals

import argparse
import json
import logging
import pprint
import sys
import time
import sqlite3 as sqlite
import csv 
import re
import xlsxwriter
from scrapy_redis import get_redis


logger = logging.getLogger('process_items')

def build_analysis(workbook):
    wa = workbook.add_worksheet("Analysis")
    wda = workbook.add_worksheet("Assignee Patent Lists")
    wdi = workbook.add_worksheet("Inventor Patent Lists")
    wdia= workbook.add_worksheet("Assignee and Inventor Patents")
    wdc = {}
    cities = ['Abbotsford',  'Chilliwack', 'Aldergrove', 'Mission', 'Langley', 'Surrey']
    for c in cities:
        wdc[c] = workbook.add_worksheet(c + " Patent Lists")
        wdc[c].write_row(0, 0, ["Assignees 1976-1986", "Assignees 1986-1995", "Assignees 1996-2005", "Assignees 2006-2015", "Assignees 2016-2018"])
        wdc[c].write_row(0, 6, ["Inventors 1976-1986", "Inventors 1986-1995", "Inventors 1996-2005", "Inventors 2006-2015", "Inventors 2016-2018"])
        wdc[c].write_row(0, 11, ["Inventors & Assignees 1976-1986", "Inventors & Assignees 1986-1995", "Inventors & Assignees 1996-2005", "Inventors & Assignees 2006-2015", "Inventors & Assignees 2016-2018"])
    wda.write_row(0, 0, cities)
    wdi.write_row(0, 0,  cities)
    wa.write_row(0, 0, ["City", "Assignees From City", "At least 1 Inventor from City", "Assignee or Inventor From City", "Current Population", "AVG # of Days to Patent", "AVG # of Inventors per Patent"])
    wa.write_column(1, 0, ["Abbotsford", "Chilliwack", "Aldergrove", "Mission", "Langley", "Surrey"])
    
    i=0
    j=1
    sr = 11
    difference = 9
    for c in cities:
         build_assignee_count(c, wa, wda,  i, j)
         build_inventor_count(c, wa, wdi,  i, j)
         build_inventor_and_assignee_count(c, wa, wdia,  i, j)
         wa.write_column(1, 4, ["141397", "83788", "12363", "38833", "117285", "498720"])
         build_avg_days(c, wa, wdia,  i, j)
         build_avg_inventors(c, wa, wdia,  i, j)
         build_city_analysis(wa, workbook, sr, c)
         build_city_assignee_year(c, "1976-01-01", "1985-12-31", wa, wdc[c], 0, sr+2)
         build_city_assignee_year(c, "1986-01-01", "1995-12-31", wa, wdc[c], 1, sr+3)
         build_city_assignee_year(c, "1996-01-01", "2005-12-31", wa, wdc[c], 2, sr+4)
         build_city_assignee_year(c, "2006-01-01", "2015-12-31", wa, wdc[c], 3, sr+5)
         build_city_assignee_year(c, "2016-01-01", "2018-12-31", wa, wdc[c], 4, sr+6)

         build_city_inventor_year(c, "1976-01-01", "1985-12-31", wa, wdc[c], 6, sr+2)
         build_city_inventor_year(c, "1986-01-01", "1995-12-31", wa, wdc[c], 7, sr+3)
         build_city_inventor_year(c, "1996-01-01", "2005-12-31", wa, wdc[c], 8, sr+4)
         build_city_inventor_year(c, "2006-01-01", "2015-12-31", wa, wdc[c], 9, sr+5)
         build_city_inventor_year(c, "2016-01-01", "2018-12-31", wa, wdc[c], 10, sr+6)

         build_city_inventor_and_assignee_year(c, "1976-01-01", "1985-12-31", wa, wdc[c], 11, sr+2)
         build_city_inventor_and_assignee_year(c, "1986-01-01", "1995-12-31", wa, wdc[c], 12, sr+3)
         build_city_inventor_and_assignee_year(c, "1996-01-01", "2005-12-31", wa, wdc[c], 13, sr+4)
         build_city_inventor_and_assignee_year(c, "2006-01-01", "2015-12-31", wa, wdc[c], 14, sr+5)
         build_city_inventor_and_assignee_year(c, "2016-01-01", "2018-12-31", wa, wdc[c], 15, sr+6)

         if c == "Abbotsford":
            wa.write(sr+2, 4,"25260")
            wa.write(sr+3, 4, "39458")
            wa.write(sr+4, 4, "18461")
            wa.write(sr+5, 4, "17533")
         elif c == "Chilliwack":
            wa.write(sr+2, 4,  "4282")
            wa.write(sr+3, 4, "18849")
            wa.write(sr+4, 4, "6290")
            wa.write(sr+5, 4, "8719")
         elif c == "Mission":
            wa.write(sr+2, 4, "5059")
            wa.write(sr+3, 4, "4317")
            wa.write(sr+4, 4, "3986")
            wa.write(sr+5, 4, "4328")
         elif c == "Langley":
            wa.write(sr+2, 4, "23209")
            wa.write(sr+3, 4, "32711")
            wa.write(sr+4, 4, "14630")
            wa.write(sr+5, 4, "11926")
         elif c == "Surrey":
            wa.write(sr+2, 4, "64950")
            wa.write(sr+3, 4, "63732")
            wa.write(sr+4, 4, "123030")
            wa.write(sr+5, 4, "103744")

         build_avg_year_days(c, "2016-01-01", "2018-12-31", wa, wdc[c], 11, sr+2)
         build_avg_year_days(c, "1986-01-01", "1995-12-31", wa, wdc[c], 12, sr+3)
         build_avg_year_days(c, "1996-01-01", "2005-12-31", wa, wdc[c], 13, sr+4)
         build_avg_year_days(c, "2006-01-01", "2015-12-31", wa, wdc[c], 14, sr+5)
         build_avg_year_days(c, "2016-01-01", "2018-12-31", wa, wdc[c], 15, sr+6)

         build_avg_year_inventors(c, "2016-01-01", "2018-12-31", wa, wdc[c], 11, sr+2)
         build_avg_year_inventors(c, "1986-01-01", "1995-12-31", wa, wdc[c], 12, sr+3)
         build_avg_year_inventors(c, "1996-01-01", "2005-12-31", wa, wdc[c], 13, sr+4)
         build_avg_year_inventors(c, "2006-01-01", "2015-12-31", wa, wdc[c], 14, sr+5)
         build_avg_year_inventors(c, "2016-01-01", "2018-12-31", wa, wdc[c], 15, sr+6)

        
         i = i+1
         j = j+1
         sr = sr + difference

def build_city_analysis(worksheeta, workbook, startrow, city):
    worksheeta.write(startrow, 0, city)
    worksheeta.write_row(startrow+1, 0, ["Year Range", "Assignees From City", "At least 1 Inventor from City", "Assignee or Inventor From City", "Change in Population", "AVG # of Days to Patent", "AVG # of Inventors per Patent"])
    worksheeta.write_column(startrow+2, 0, ["1976-1985", "1986-1995", "1996-2005", "2006-2015", "2016-2018"])



def build_assignee_count(city, worksheeta, worksheetd, column, row):
    conn = sqlite.connect('./scrapedata.db')
    cursor = conn.cursor()
    cursor.execute(
        'select count(*) from General_Info where assignee_city like "%' +city + '%" OR assignee2_city like "%' +city+'%"'
    )
    rows = cursor.fetchall()
    rows = list(rows) 
    worksheeta.write(row, 1, rows[0][0])
    cursor.execute('select General_Info.patentno from General_Info where assignee_city like "%' +city+ '%" OR assignee2_city like "%' +city+'%"')
    patentlist = cursor.fetchall()
    patentlist = list(patentlist) 
    patentlist = [ "%s" % x for x in patentlist ]
    worksheetd.write_column(1, column, patentlist)

def build_inventor_count(city, worksheeta, worksheetd, column, row):
    conn = sqlite.connect('./scrapedata.db')
    cursor = conn.cursor()
    cursor.execute(
        'select COUNT(DISTINCT General_Info.patentno) from General_Info JOIN Inventors on Inventors.patentno = General_Info.patentno where Inventors.city like "%'+city+'%" '
    )
    rows = cursor.fetchall()
    rows = list(rows) 
    worksheeta.write(row, 2, rows[0][0])
    cursor.execute('select DISTINCT General_Info.patentno from General_Info JOIN Inventors on Inventors.patentno = General_Info.patentno where Inventors.city like "%' +city+'%"')
    patentlist = cursor.fetchall()
    patentlist = list(patentlist) 
    patentlist = [ "%s" % x for x in patentlist ]
    worksheetd.write_column(1, column, patentlist)

def build_inventor_and_assignee_count(city, worksheeta, worksheetd, column, row):
    conn = sqlite.connect('./scrapedata.db')
    cursor = conn.cursor()
    cursor.execute(
        'select COUNT( DISTINCT General_Info.patentno) from General_Info JOIN Inventors on Inventors.patentno = General_Info.patentno where (Inventors.city like "%' +city +'%") OR (Inventors.city like "%' +city +'%" AND  (assignee_city like "%' +city + '%" OR assignee2_city like "%' +city + '%")) OR (assignee_city like "%' +city + '%" OR assignee2_city like "%' +city + '%") OR (Inventors.city like "%' +city +'%"  AND  (assignee_city="" OR assignee2_city=""))')
    rows = cursor.fetchall()
    rows = list(rows) 
    worksheeta.write(row, 3, rows[0][0])
    cursor.execute('select DISTINCT General_Info.patentno from General_Info JOIN Inventors on Inventors.patentno = General_Info.patentno where (Inventors.city like "%' +city +'%") OR (Inventors.city like "%' +city +'%" AND  (assignee_city like "%' +city + '%" OR assignee2_city like "%' +city + '%")) OR (assignee_city like "%' +city + '%" OR assignee2_city like "%' +city + '%") OR (Inventors.city like "%' +city +'%"  AND  (assignee_city="" OR assignee2_city=""))')
    patentlist = cursor.fetchall()
    patentlist = list(patentlist) 
    patentlist = [ "%s" % x for x in patentlist ]
    worksheetd.write_column(1, column, patentlist)

def build_avg_days(city, worksheeta, worksheetd, column, row):
    conn = sqlite.connect('./scrapedata.db')
    cursor = conn.cursor()
    cursor.execute(
        'SELECT ROUND(AVG(days_between_filed_and_issue), 0) from (select days_between_filed_and_issue from General_Info JOIN Inventors on Inventors.patentno = General_Info.patentno where Inventors.city like "%' +city + '%" group by General_Info.patentno) '
    )
    rows = cursor.fetchall()
    rows = list(rows) 
    worksheeta.write(row, 5, rows[0][0])

def build_avg_inventors(city, worksheeta, worksheetd, column, row):
    conn = sqlite.connect('./scrapedata.db')
    cursor = conn.cursor()
    cursor.execute(
        'select ROUND(AVG(inventorcount),0) from (select  COUNT(Inventors.name) as inventorcount  from Inventors where Inventors.city like "%' +city +'%" group by Inventors.patentno);'
    )
    rows = cursor.fetchall()
    rows = list(rows) 
    worksheeta.write(row, 6, rows[0][0])

def build_city_assignee_year(city, syear, eyear, worksheeta, worksheetd, column, row):
    conn = sqlite.connect('./scrapedata.db')
    cursor = conn.cursor()
    cursor.execute(
        'select count(*) from General_Info where (assignee_city like "%' +city +'%" OR assignee2_city like "%' +city +'%") AND (DATETIME(issue_date) BETWEEN DATETIME("' + syear + '") AND  DATETIME("'+ eyear+ '"))'
    )
    rows = cursor.fetchall()
    rows = list(rows) 
    worksheeta.write(row, 1, rows[0][0])
    cursor.execute('select General_Info.patentno from General_Info where (assignee_city like "%' +city +'%" OR assignee2_city like "%' +city +'%") AND  DATETIME(issue_date) BETWEEN  DATETIME("' + syear + '") AND DATETIME("'+ eyear+ '")')
    patentlist = cursor.fetchall()
    patentlist = list(patentlist) 
    patentlist = [ "%s" % x for x in patentlist ]
    worksheetd.write_column(1, column, patentlist)    

def build_city_inventor_year(city, syear, eyear, worksheeta, worksheetd, column, row):
    conn = sqlite.connect('./scrapedata.db')
    cursor = conn.cursor()
    cursor.execute(
        'select COUNT(DISTINCT General_Info.patentno) from General_Info JOIN Inventors on Inventors.patentno = General_Info.patentno where Inventors.city like "%' +city +'%"  AND DATETIME(General_Info.issue_date) BETWEEN DATETIME("' + syear + '") AND DATETIME("' +eyear + '")'
    )
    rows = cursor.fetchall()
    rows = list(rows) 
    worksheeta.write(row, 2, rows[0][0])
    cursor.execute('select DISTINCT General_Info.patentno from General_Info JOIN Inventors on Inventors.patentno = General_Info.patentno where Inventors.city like "%' +city +'%"  AND DATETIME(General_Info.issue_date) BETWEEN DATETIME("' + syear + '") AND DATETIME("' +eyear + '")')
    patentlist = cursor.fetchall()
    patentlist = list(patentlist) 
    patentlist = [ "%s" % x for x in patentlist ]
    worksheetd.write_column(1, column, patentlist)    

def build_city_inventor_and_assignee_year(city, syear, eyear, worksheeta, worksheetd, column, row):
    conn = sqlite.connect('./scrapedata.db')
    cursor = conn.cursor()
    cursor.execute(
        'select COUNT( DISTINCT General_Info.patentno) from General_Info JOIN Inventors on Inventors.patentno = General_Info.patentno where ((Inventors.city like "%' +city +'%") OR (Inventors.city like "%' +city +'%" AND  (assignee_city like "%' +city + '%" OR assignee2_city like "%' +city + '%")) OR (assignee_city like "%' +city + '%" OR assignee2_city like "%' +city + '%") OR (Inventors.city like "%' +city +'%"  AND  (assignee_city="" OR assignee2_city=""))) AND DATETIME(General_Info.issue_date) BETWEEN DATETIME("' + syear + '") AND DATETIME("' +eyear + '")'
    )
    rows = cursor.fetchall()
    rows = list(rows) 
    worksheeta.write(row, 3, rows[0][0])
    cursor.execute('select DISTINCT General_Info.patentno from General_Info JOIN Inventors on Inventors.patentno = General_Info.patentno where ((Inventors.city like "%' +city +'%") OR (Inventors.city like "%' +city +'%" AND  (assignee_city like "%' +city + '%" OR assignee2_city like "%' +city + '%")) OR (assignee_city like "%' +city + '%" OR assignee2_city like "%' +city + '%") OR (Inventors.city like "%' +city +'%"  AND  (assignee_city="" OR assignee2_city=""))) AND DATETIME(General_Info.issue_date) BETWEEN DATETIME("' + syear + '") AND DATETIME("' +eyear + '")')
    patentlist = cursor.fetchall()
    patentlist = list(patentlist) 
    patentlist = [ "%s" % x for x in patentlist ]
    worksheetd.write_column(1, column, patentlist)  

def build_avg_year_days(city, syear, eyear, worksheeta, worksheetd, column, row):
    conn = sqlite.connect('./scrapedata.db')
    cursor = conn.cursor()
    cursor.execute(
        'SELECT ROUND(AVG(days_between_filed_and_issue), 0) from (select days_between_filed_and_issue from General_Info JOIN Inventors on Inventors.patentno = General_Info.patentno where Inventors.city like "%' +city + '%" AND DATETIME(General_Info.issue_date) BETWEEN DATETIME("' + syear + '") AND DATETIME("' +eyear + '") group by General_Info.patentno) '
    )
    rows = cursor.fetchall()
    rows = list(rows) 
    worksheeta.write(row, 5, rows[0][0])

def build_avg_year_inventors(city, syear, eyear, worksheeta, worksheetd, column, row):
    conn = sqlite.connect('./scrapedata.db')
    cursor = conn.cursor()
    cursor.execute(
        'select ROUND(AVG(inventorcount),0) from (select  COUNT(Inventors.name) as inventorcount  from Inventors JOIN General_Info on Inventors.patentno = General_Info.patentno where Inventors.city like "%' +city +'%"  AND DATETIME(General_Info.issue_date) BETWEEN DATETIME("' + syear + '") AND DATETIME("' +eyear + '") group by Inventors.patentno) '
    )
    rows = cursor.fetchall()
    rows = list(rows) 
    worksheeta.write(row, 6, rows[0][0])


def init_db():
    conn = sqlite.connect('./scrapedata.db')
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS `General_Info`;")
    cursor.execute("DROP TABLE IF EXISTS `Inventors`;")
    cursor.execute("DROP TABLE IF EXISTS `inventorcities`;")
    cursor.execute("DROP TABLE IF EXISTS `Assignee`;")
    cursor.execute("DROP TABLE IF EXISTS `AssigneeCity`;")
    cursor.execute("""
        CREATE TABLE `General_Info` (
        `id` INTEGER,
        `issue_date` DATE NULL DEFAULT NULL,
        'filed_date' DATE NULL DEFAULT NULL,
        'days_between_filed_and_issue' MEDIUMTEXT NULL DEFAULT NULL,
        `patent_title` MEDIUMTEXT NULL DEFAULT NULL,
        `patentno` MEDIUMTEXT NULL DEFAULT NULL,
        `USClass` MEDIUMTEXT NULL DEFAULT NULL,
        `Claim` MEDIUMTEXT NULL DEFAULT NULL,
        `CPPClass` MEDIUMTEXT NULL DEFAULT NULL,
        `FamilyID` INT(20) NULL DEFAULT NULL,
        `CIClass` MEDIUMTEXT NULL DEFAULT NULL,
        `ApplicationNo` MEDIUMTEXT NULL DEFAULT NULL,
        `Abstract` MEDIUMTEXT NULL DEFAULT NULL,
        'assignee_name' MEDIUMTEXT NULL DEFAULT NULL,
        'assignee_city' MEDIUMTEXT NULL DEFAULT NULL,
        'assignee2_name' MEDIUMTEXT NULL DEFAULT NULL,
        'assignee2_city' MEDIUMTEXT NULL DEFAULT NULL,
        'claim_count' MEDIUMTEXT NULL DEFAULT NULL,
        PRIMARY KEY (`id`)
        );
        """)
    cursor.execute(
        """
        CREATE TABLE `Inventors` (
            `id` INTEGER,
            `name` MEDIUMTEXT NULL DEFAULT NULL,
            `patentno` MEDIUMTEXT NULL DEFAULT NULL,
            `city` MEDIUMTEXT NULL DEFAULT NULL,
            PRIMARY KEY (`id`)
            );
        """)

def build_spreadsheet():
    #execute query to get data
    conn = sqlite.connect('./scrapedata.db')
    cursor = conn.cursor()
    cursor.execute(
        """
        select  General_Info.patentno, issue_date, filed_date,   days_between_filed_and_issue,  COUNT(Inventors.name) AS ninventors, 
         patent_title, Abstract, substr(Claim, 0, 100),
        USClass, CPPClass, FamilyID, CIClass, ApplicationNo,  assignee_name, 
        assignee_city,  assignee2_name, 
        assignee2_city, claim_count, group_concat(Inventors.name) AS iname, group_concat(Inventors.city) AS icity
        from General_Info
        left join Inventors on General_Info.patentno = Inventors.patentno
        group by Inventors.patentno;

        """
    )
    rows = cursor.fetchall()
    rows = list(rows) 

    logger.info("Loaded data")
    workbook   = xlsxwriter.Workbook('data.xlsx')
    result_data = workbook.add_worksheet("data")
    longest_row = getlongestrow(list(rows))
    #split inventor cities and nam es, and append them as seperate columns
    for r in range(len(rows)): 
        rows[r] = list(rows[r])
        cities = rows[r].pop()# get the last of each 
        names = rows[r].pop()

        names = names.split(';')#split into seperate columbs by column

        if len(names) > 1:
            names.append(names.pop() + ' '+names.pop(0))
        names = list(set(names))
        cities =  re.split("(?:)(,)(?=\w)", cities)
        cities = list(filter(lambda x: x!= ",", cities))
        cities = list(filter(lambda x: x!= "CA", cities))
            #get the longest field row so we can make the column names 
        rows[r][4] = len(names)
        diff = len(longest_row) - len(names)
        for x in range(diff):
            names.append(' ')
        rows[r].extend(names) #add them to the list 
        rows[r].extend(cities)
    logger.info("Longest row: " + str(len(longest_row)))
    fieldnames = [  'Patent Number', 'Issue Date', 'Filed Date', '# of Days between filed and issue dates', 
                        'Number of Inventors', 'Patent Title', 
                    'Abstract',  'Claim', 'CPP Class', 'Family ID',
                    'CI Class', 'Application Number',    'US Class', 'Assignee Name', 'Assignee City', 
                    'Assignee 2 Name', 'Assignee 2 City', 'Claim Count']    
    #put in the column names  
    for i in range(len(longest_row)):
        fieldnames.append("Inventor" + " "+ str(i+1))
    for i in range(len(longest_row)):
        fieldnames.append("Inventor" + " " + str(i+1) + " City")
    #actually write the csv file 
    result_data.write_row(0, 0, fieldnames)
    i =0
    j =1
    for row in rows:
        result_data.write_row(j, 0, row) 
        i = i+1
        j = j+1
    build_analysis(workbook)

def getlongestrow(rows):
    longest_row = []      
    #split inventor cities and names, and append them as seperate columns
    for r in range(len(rows)): 
        rows[r] = list(rows[r])
        cities = rows[r].pop()# get the last of each 
        names = rows[r].pop()
        names = names.split(';')#split into seperate columbs by column 
        if len(names) > 1:
            names.append(names.pop() + ' '+names.pop(0))
        names = list(set(names))
        if len(longest_row) < len(names):
            longest_row = names
    return longest_row


def process_item(item):
    conn = sqlite.connect('./scrapedata.db')
    cursor = conn.cursor()
    cursor.execute(
            """insert into General_Info (issue_date, 
                                        filed_date,
                                        days_between_filed_and_issue,
                                        patent_title, 
                                        patentno, 
                                        USClass,
                                        Claim,
                                        CPPClass,
                                        FamilyID,
                                        CIClass,
                                        ApplicationNo,
                                        Abstract,
                                        assignee_name,
                                        assignee_city,
                                        assignee2_name,
                                        assignee2_city,
                                        claim_count
                                        ) values ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (item['date'], 
                item['datefiled'],
                item['days'],
                item['patent'],
                item['patentno'],
                item['USClass'],
                item['Claim'],
                item['CPPClass'],
                item['FamilyID'],
                item['CIClass'],
                item['ApplicantNo'],
                item['Abstract'],
                item['Assignee'],
                item['AssigneeCity'],
                item['Assignee2'],
                item['Assignee2City'],
                item['ClaimCount']
                ))
    conn.commit()

    for invname, invcity in zip(item['InventorNames'], item['InventorCity']):
        if invname == '': continue
        cursor.execute("insert into Inventors (name, city, patentno) values (?, ?, ?)",
                            (invname, invcity, item['patentno']))
        conn.commit()
    return item

def process_items(r, keys, timeout, limit=3236, log_every=1000, wait=.1):
    """Process items from a redis queue.
    Parameters
    ----------
    r : Redis
        Redis connection instance.
    keys : list
        List of keys to read the items from.
    timeout: int
        Read timeout.
    """
    limit = limit or float('inf')
    processed = 0
    while processed < limit:
        # Change ``blpop`` to ``brpop`` to process as LIFO.
        ret = r.blpop(keys, timeout)
        # If data is found before the timeout then we consider we are done.
        if ret is None:
            time.sleep(wait)
            continue

        source, data = ret
        try:
            item = json.loads(data)
        except Exception:
            logger.exception("Failed to load item:\n%r", pprint.pformat(data))
            continue

        try:
            process_item(item)
            logger.debug(" Processing item: %s ", source)
        except KeyError:
            logger.exception("[%s] Failed to process item:\n%r",
                             source, pprint.pformat(item))
            continue

        processed += 1
        if processed % log_every == 0:
            logger.info("Processed %s items", processed)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('key', help="Redis key where items are stored")
    parser.add_argument('--host')
    parser.add_argument('--port')
    parser.add_argument('--timeout', type=int, default=5)
    parser.add_argument('--limit', type=int, default=0)
    parser.add_argument('--progress-every', type=int, default=100)
    parser.add_argument('-v', '--verbose', action='store_true')

    args = parser.parse_args()

    params = {}
    if args.host:
        params['host'] = args.host
    if args.port:
        params['port'] = args.port

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)

    r = get_redis(**params)
    host = r.connection_pool.get_connection('info').host
    logger.info("Waiting for items in '%s' (server: %s)", args.key, host)
    kwargs = {
        'keys': [args.key], 
        'timeout': args.timeout,
        'limit': args.limit,
        'log_every': args.progress_every,
    }
    try:
        init_db()
        process_items(r, **kwargs)
        logger.info("Building spreadsheet")
        build_spreadsheet()
        retcode = 0  # ok
    except KeyboardInterrupt:
        retcode = 0  # ok
    except Exception:
        logger.exception("Unhandled exception")
        retcode = 2

    return retcode


if __name__ == '__main__':
    sys.exit(main())