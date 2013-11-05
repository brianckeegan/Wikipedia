# -*- coding: utf-8 -*-

#!/usr/bin/env python

'''
pageview-scraper.py - batch download wiki pageview history
    Usage: python pageview-scraper.py -s <yyyymmdd> -e <yyyymmdd> -d <datapath>
    E.g.   python pageview-scraper.py -s 20130410 -e 20130417 -d /Data/
    The parameters will be set as default if not given.
    
@author: Brian Keegan and Yu-Ru Lin 
@contact: bkeegan@gmail.com and yuruliny@gmail.com 
@date: October 22, 2013

'''

import sys,os,urllib2,hashlib
from bs4 import BeautifulSoup
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta

#datapath = os.getcwd()+'/Data/'

# From http://stackoverflow.com/questions/4028697/how-do-i-download-a-zip-file-in-python-using-urllib2/4028728
def get_file(url,filepath,md5_dict):
    # Open the url
    if len(md5_dict.keys()) > 0: #sometimes there's no hash file (eg, December 2008)
        try:
            f = urllib2.urlopen(url)

            # Open our local file for writing
            with open(filepath+os.path.basename(url), "wb") as local_file:
                read_file = f.read()
                file_md5 = hashlib.md5(read_file).hexdigest()
                if file_md5 == md5_dict[os.path.basename(url)]:
                    local_file.write(read_file)
                else:
                    raise ValueError('MD5 checksum on {0} does not match master.'.format(os.path.basename(url)))

        #handle errors
        except urllib2.HTTPError, e:
            print "HTTP Error:", e.code, url
        except urllib2.URLError, e:
            print "URL Error:", e.reason, url
    else:
        print "No hashes found, proceeding without checking hashes!"
        try:
            f = urllib2.urlopen(url)

            # Open our local file for writing
            with open(filepath+os.path.basename(url), "wb") as local_file:
                read_file = f.read()
                local_file.write(read_file)

        #handle errors
        except urllib2.HTTPError, e:
            print "HTTP Error:", e.code, url
        except urllib2.URLError, e:
            print "URL Error:", e.reason, url

def get_dumps(start,end,datapath=os.getcwd()+'/Data/'):
    start_dt = datetime.strptime(start,'%Y%m%d')
    end_dt = datetime.strptime(end,'%Y%m%d')
    url_base = 'http://dumps.wikimedia.org/other/pagecounts-raw/'
    
    if not os.path.exists(datapath): 
        os.system('mkdir -p {0}'.format(datapath))
    
    # Enforce time boundaries
    if start_dt < datetime(2007,12,10,0,0,0):
        raise ValueError('Time range must be after 10 Dec 2007')
    elif end_dt > datetime.today():
        raise ValueError('Time range must end before today') 
    
    index = start_dt
    while index < end_dt:
        year = datetime.strftime(index,'%Y')
        month = datetime.strftime(index,'%m')
        
        # Make md5 checksum dictionary that can be checked after downloading files below
        md5_url = url_base + '{0}/{0}-{1}/'.format(year,month) + 'md5sums.txt'
        md5s = urllib2.urlopen(md5_url).readlines()
        md5_dict = dict([i.strip().split('  ') for i in md5s if 'pagecounts' in i])
        md5_dict = {v:k for k,v in md5_dict.iteritems()}
        
        # Can't pass clean filenames since seconds field varies, scrape filenames instead
        url = url_base + '{0}/{0}-{1}/'.format(year,month)
        soup = BeautifulSoup(urllib2.urlopen(url))
        pagecount_links = [link.get('href') for link in soup.findAll('a') if 'pagecounts' in link.get('href')]
        
        # Return list of links in valid timespan
        valid_links = [datetime.strptime(i,'pagecounts-%Y%m%d-%H%M%S.gz') for i in pagecount_links]
        valid_links = [i for i in valid_links if i > start_dt and i < end_dt]
        valid_links = [datetime.strftime(i,'pagecounts-%Y%m%d-%H%M%S.gz') for i in valid_links]
        
        # If the directory doesn't exist, create it
        filepath = datapath+'{0}/{1}/'.format(year,month)
        
        if not os.path.exists(filepath): 
            os.system('mkdir -p {0}'.format(filepath))
        
        # Get the data
        for link in valid_links:
            url = url_base + '{0}/{0}-{1}/'.format(year,month) + link
            # Check if file already exists and has non-zero length
            if os.path.exists(filepath+link) and os.path.getsize(filepath+link) > 0: 
                print link + ' already exists!'
                continue
            
            try:
                print "Retrieving pageviews for " + link
                get_file(url,filepath,md5_dict) # Don't forget the md5_dict too
            except (KeyboardInterrupt, SystemExit):
                sys.exit(0)
                break
        
        index += relativedelta(months=1)

def main(argv):
    args = [a.lower() for a in argv]
    for i,arg in enumerate(args):
        if arg in ['-h','--help']: 
            print 'Usage: python pageview_scraper.py -s <yyyymmdd> -e <yyyymmdd> -d <datapath>'
        elif arg in ['-s','--start']:
            try: 
                start = argv[i+1]
            except: 
                start = datetime.strftime(datetime.today()- timedelta(days=1),'%Y%m%d')
        elif arg in ['-e','--end']:
            try: 
                end = argv[i+1]
            except: 
                end = datetime.strftime(datetime.today(),'%Y%m%d')
        elif arg in ['-d','--dir']:
            try:
                datapath = argv[i+1]
            except: 
                datapath = os.getcwd()+'/Data/'

    if not os.path.exists(datapath):
	    os.system('mkdir -p {0}'.format(datapath))    

    get_dumps(start,end,datapath)

if __name__ == '__main__': 
    main(sys.argv[1:])

