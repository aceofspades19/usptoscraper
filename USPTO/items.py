# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy



class Patent(scrapy.Item):
     issue_date = Field()
     filed_date = Field()
     days_between_filed_and_issue = Field()
     patent_title = Field()
     patentno = Field() 
     USClass = Field()
     Claim = Field()
     CPPClass = Field()
     FamilyID = Field()
     CIClass = Field()
     ApplicationNo = Field()
     Abstract = Field()
     assignee_name = Field()
     assignee_city = Field()
     assignee2 = Field()
     assignee2city = Field()
     inventor_name = Field()
     inventor_city = Field() 