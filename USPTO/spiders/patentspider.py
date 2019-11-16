import scrapy
from bs4 import BeautifulSoup
import re 
from dateutil.parser import parse
from datetime import *
import urllib
from scrapy_redis.spiders import RedisSpider


class PatentsSpider(scrapy.spiders.CrawlSpider):
    name = "patents"
    allowed_domains = ['uspto.gov']

    def start_requests(self):
        cities = ['Chilliwack', 'Aldergrove', 'Abbotsford', 'Surrey', 'Langley', 'Mission']
        urls = self.generate_urls(cities)
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def generate_urls(self, cities):
        urls = []
        for city in cities:
            urls.append("http://patft.uspto.gov/netacgi/nph-Parser?Sect1=PTO2&Sect2=HITOFF&u=%2Fnetahtml%2FPTO%2Fsearch-adv.htm&r=0&p=1&f=S&l=50&Query=+((IC%2F" +city+ "+and+ICN%2FCA)+OR+(AC%2F" +city +"+and+ACN%2FCA))+ANDNOT+IS%2FCA+&d=PTXT")
        return urls 

    def getclaims(self, response):
        pattern = re.compile("\s+\d+\.\s\s\w")
        pattern1 = re.compile("<\/center>([\s\S]*?)<center>")
        pattern2 = re.compile("\d+\.\s\s\w")
        claims = response.css("coma").extract_first()
        if claims is not None:
            claims = pattern1.findall(claims)
            claims = BeautifulSoup(claims[1], 'html.parser').get_text().replace("\n", '')
            claims = pattern.findall(claims)
            count = len(claims)
        else:
            try:
                claims = response.css("body").extract_first()
                claims = pattern1.findall(claims)
                claims = BeautifulSoup(claims[6], 'html.parser').get_text().replace("\n", '')
                claims = pattern.findall(claims)
                count = len(claims)
                if count == 0:
                    claims = response.css("body").extract_first()
                    claims = pattern1.findall(claims)
                    claims = BeautifulSoup(claims[4], 'html.parser').get_text().replace("\n", '')
                    claims = pattern2.findall(claims)
                    count = len(claims)
                    if count == 0:
                        claims = response.css("body").extract_first()
                        claims = pattern1.findall(claims)
                        claims = BeautifulSoup(claims[3], 'html.parser').get_text().replace("\n", '')
                        claims = pattern2.findall(claims)
                        count = len(claims)
                        if count == 0:
                            claims = response.css("body").extract_first()
                            claims = pattern1.findall(claims)
                            claims = BeautifulSoup(claims[7], 'html.parser').get_text().replace("\n", '')
                            claims = pattern2.findall(claims)
                            count = len(claims)
            except AttributeError:
                return 0 
            except KeyError:
                return 0
            except IndexError:
                return 0 
        print(count) 
        return count 
       


    def parse(self, response):
        #get page data 
        try:
            pageno = urllib.parse.parse_qs(urllib.parse.urlparse(response.url).query)['Page'][0]
        except KeyError:
            pageno=""
        p = urllib.parse.parse_qs(urllib.parse.urlparse(response.url).query)['p'][0]
        r = urllib.parse.parse_qs(urllib.parse.urlparse(response.url).query)['r'][0]
         
        if r == "0" or pageno == "Next":
            text= response.css('center:nth-of-type(1) > a:nth-of-type(2) > img::attr(alt)').extract_first() #next button if there is no previous
            href = response.css('center:nth-of-type(1) > a:nth-of-type(2)::attr(href)').extract_first() 
            href2 = response.css('center:nth-of-type(1) > a:nth-of-type(3)::attr(href)').extract_first() #sometimes next button 
            #cycle through all the pages
            if text == "[NEXT_LIST]": 
                yield scrapy.Request(response.urljoin(href), callback=self.parse)
            else:
                 yield scrapy.Request(response.urljoin(href2), callback=self.parse)
            href = response.css('td:nth-of-type(4) a::attr(href)').extract()
            for h in href:
                #get all the actual patents 
                yield scrapy.Request(response.urljoin(h), callback=self.parse)
            
        else:
            #process patent
            claimre = re.compile("1[.](?:.|\r)+")
            claimre2 = re.compile("([I|We] claim:\s)1[.](?:.|\n\r)+")
          #  try: 
            if response.css("tr:contains('Family ID:') td").extract_first() is not None:
                familyid = BeautifulSoup(response.css("tr:contains('Family ID:') td").extract_first(), 'html.parser').get_text().strip("\n")
            else:
                familyid = ''

            if response.css("tr:contains('Assignee:') td").extract_first() is not None:
                assignee = BeautifulSoup(response.css("tr:contains('Assignee:') td").extract_first(), 'html.parser').get_text().strip("\n"),
            else:
                assignee = 'Individual'

            if response.css("coma").extract_first() is not None:
                claims = claimre.search(BeautifulSoup(response.css("coma").extract_first(), 'html.parser').get_text().replace("\n", ''))
                try:
                    strclaims = claims.group(0)
                except AttributeError:
                    strclaims = ''
            else:
                claims = claimre.search(BeautifulSoup(response.css("body").extract_first(), 'html.parser').get_text().replace("\n", ''))
                try:
                    strclaims = claims.group(0)
                except AttributeError:
                    strclaims = ''


            if response.css("tr:contains('Current CPC Class:') td:nth-of-type(2)").extract_first() is not None:
                cppc = BeautifulSoup(response.css("tr:contains('Current CPC Class:') td:nth-of-type(2)").extract_first(), 'html.parser').get_text().strip("\n")
                cppc = cppc.replace("&nbsp", "")
            else:
                cppc = ''

            if response.css("body > font").extract_first() is not None:
                patent = BeautifulSoup(response.css("body > font").extract_first(), 'html.parser').get_text().strip("\n")
            else:
                patent = ''


            if response.css("td td:nth-of-type(1)").extract_first() is not None:
                applicantname = BeautifulSoup(response.css("td td:nth-of-type(1)").extract_first(), 'html.parser').get_text().strip("\n")
            else:
                applicantname = ''

            if response.css("td td:nth-of-type(1)").extract_first() is not None:
                appcity = BeautifulSoup(response.css("td td:nth-of-type(1)").extract_first(), 'html.parser').get_text().strip("\n")
            else:
                appcity = ''

            if response.css("td td:nth-of-type(4)").extract_first() is not None:
                appcountry = BeautifulSoup(response.css("td td:nth-of-type(4)").extract_first(), 'html.parser').get_text().strip("\n")
            else:
                appcountry = ''

            if response.css("tr:contains('Appl. No.:') td").extract_first() is not None:
                appno = BeautifulSoup(response.css("tr:contains('Appl. No.:') td").extract_first(), 'html.parser').get_text().strip("\n")
            else:
                appno = '' 
            
            if response.css("tr:contains('Filed:') td").extract_first() is not None:
                datefiled = BeautifulSoup(response.css("tr:contains('Filed:') td").extract_first(), 'html.parser').get_text().strip("\n")
                datefiled = parse(datefiled)
            else:
                datefiled =  parse(str(MINYEAR) + "-01" + "-01")

            if response.css("tr:contains('Current International Class:') td:nth-of-type(2)").extract_first() is not None:
                cicclass = BeautifulSoup(response.css("tr:contains('Current U.S. Class:') td:nth-of-type(2)").extract_first(), 'html.parser').get_text().strip("\n")
                cicclass = cicclass.replace("&nbsp", "")
            else:
                cicclass = ''
            if response.css("table:nth-of-type(2) tr:nth-of-type(2) td:nth-child(2)").extract_first() is not None:
                date1 = BeautifulSoup(response.css("table:nth-of-type(2) tr:nth-of-type(2) td:nth-child(2)").extract_first().replace("\n", "").strip("\n"), 'html.parser').get_text()
                try:
                    date = parse(date1)
                except ValueError:
                    date = parse(str(MINYEAR) + "-01" + "-01")
            else:
                date = parse(str(MINYEAR) + "-01" + "-01")

            
            count = self.getclaims(response)
        

            delta = date - datefiled
          
            dateyear = parse("2019-01-01")
            sdateyear = parse("1975-12-31")
            inventornames = BeautifulSoup(response.css("tr:contains('Inventors:') td").extract_first(), 'html.parser').get_text().strip("\n")
            names = inventornames.split(")")
            cities = []   
            rnames = []
            for n in names:
                result = n.split("(")
                if len(result) > 1:
                    cities.append(result[1])
                    rnames.append(result[0].strip(", "))
                else:
                    rnames.append(result[0].strip(" "))

            asscities = []   
            assnames = []
            if len(assignee) > 0:
                assn = assignee[0].split(")")
                for n in assn:
                    result = n.split("(")
                    if len(result) > 1:
                        asscities.append(result[1])
                        assnames.append(result[0].strip(", "))
                    else:
                        assnames.append(result[0].strip(" "))
                        asscities.append("Not Listed")
            else:
                assnames.append('Individual')
                asscities.append('Individual')

            if len(assnames) < 2:
                assnames.append("")
                asscities.append("")
            if dateyear > date and sdateyear < date:
                yield {
                    'patent':patent,
                    'date': date.strftime('%Y-%m-%d'),
                    'datefiled':datefiled.strftime('%Y-%m-%d'),
                    'days':delta.days,
                    'patentno': BeautifulSoup(response.css("tr:contains('United States Patent') td:nth-child(2)").extract_first(), 'html.parser').get_text().strip("\n"),
                    'InventorNames':rnames,
                    'InventorCity':cities,
                    'ApplicantNo':appno,
                    'ApplicantName':applicantname,
                    'ApplicantCity':appcity,
                    'ApplicantCountry':appcountry,
                    'Assignee':assnames[0],
                    'AssigneeCity':asscities[0],
                    'Assignee2':assnames[1],
                    'Assignee2City':asscities[1],
                    'Abstract':BeautifulSoup(response.css("p:nth-of-type(1)").extract_first(), 'html.parser').get_text().strip("\n"),
                    'FamilyID':familyid,
                    'USClass':BeautifulSoup(response.css("tr:contains('Current U.S. Class:') td:nth-of-type(2)").extract_first(), 'html.parser').get_text().strip("\n"),
                    'CPPClass':cppc,
                    'CIClass':cicclass,
                    'Claim':strclaims,
                    'ClaimCount': count

                }
        #    except TypeError:
        #        pass
        #    except AttributeError:
        #        pass
            
  