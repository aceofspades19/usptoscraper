1) mkdir scraper
2) virtualenv env
3) source env/bin/activate
4) git clone git@github.com:aceofspades19/usptoscraper.git
5) pip install -Iv Scrapy==1.5.0
pip install bs4
 pip install python-dateutil –upgrade
pip install scrapy_redis
pip install xlsxwriter
6) start redis server
7)  ../env/bin/scrapy runspider USPTO/spiders/patentspider.py
	this starts the spider that collects the information
8) run python process_items.py patents at the same time or after
9) you should have a spreadsheet with the data in it 


database is stored in scrapedata.db as a sqlite database 
cities can be added or taken away in spiders/patentspider.py line 15