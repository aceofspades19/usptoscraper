# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
from scrapy import log
import sqlite3 as sqlite
import csv 

class UsptoPipeline(object):
    def __init__(self):
        self.conn = sqlite.connect('./scrapedata.db')
        self.cursor = self.conn.cursor()
        self.cursor.execute("DROP TABLE IF EXISTS `General_Info`;")
        self.cursor.execute("DROP TABLE IF EXISTS `Inventors`;")
        self.cursor.execute("DROP TABLE IF EXISTS `inventorcities`;")
        self.cursor.execute("DROP TABLE IF EXISTS `Assignee`;")
        self.cursor.execute("DROP TABLE IF EXISTS `AssigneeCity`;")
        self.cursor.execute("""
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
            PRIMARY KEY (`id`)
            );""")
        self.cursor.execute(
            """
            CREATE TABLE `Inventors` (
                `id` INTEGER,
                `name` MEDIUMTEXT NULL DEFAULT NULL,
                `patentno` MEDIUMTEXT NULL DEFAULT NULL,
                `city` MEDIUMTEXT NULL DEFAULT NULL,
                PRIMARY KEY (`id`)
                );
            """)

    def open_spider(self, spider):
        pass
    def close_spider(self, spider):
        pass
       # self.build_inventor_net()
       # self.build_assignee_spread()
    def process_item(self, item, spider):
        self.cursor.execute(
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
                                            assignee_city
                                            ) values ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
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
                    item['AssigneeCity']
                    ))
        self.conn.commit()

        for invname, invcity in zip(item['InventorNames'], item['InventorCity']):
            if invname == '': continue
            self.cursor.execute("insert into Inventors (name, city, patentno) values (?, ?, ?)",
                                (invname, invcity, item['patentno']))
            self.conn.commit()
        return item 
  
    def build_assignee_spread(self):
        cur = self.conn.cursor()
        cur.execute("select assignee_name, count(*) from General_Info group by assignee_name order by count(*)")
        rows = cur.fetchall()
        with open('assigneesAldergrove1976.csv', 'w') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["Assignee Names", "Count"])
            for r in rows:
                writer.writerow([r[0].strip("\n"), r[1]])



    def build_inventor_net(self):
        cur = self.conn.cursor()
        cur.execute("select a.name, b.name from Inventors a JOIN Inventors b ON a.patentno=b.patentno order by a.name")
        rows = cur.fetchall()
        self.make_netfile(rows)

    def make_netfile(self, rows):
        pairings = {}
        verticies = []
        counter1 = 0
        counter2 = 0
 
        for row in rows:
            if row[0] not in verticies:
                verticies.append(row[0])
        
            counter1 = verticies.index(row[0])

            try:
                counter2 = verticies.index(row[1])
            except ValueError:
                counter2 = False  
          #  print(str(counter2)+ ":" + str(counter1))
             
            if counter2 and counter1 != counter2: 
                if (counter1, counter2) in pairings.keys():
                    pairings[counter1, counter2] = pairings[counter1, counter2] +1 
                else:
                    pairings[counter1, counter2] = 1 
            file = open('abbotsfordassignees.net', 'w')
            file.write("*Vertices " + str(len(verticies)) + "\r\n")

            for index, value in enumerate(verticies):
                print_index = index + 1 
                file.write(str(print_index) + " \"" + value + "\"" + "\r\n")
            file.write("*edges" + "\n")
            for i in range(len(verticies)):
                for j in range(len(verticies)):
                    value = pairings.get((i+1, j+1))
                    if value:
                        file.write(str((i + 1)) + " " + str((j+1)) + " " + str(value) + "\r\n") 
            file.close()
        


