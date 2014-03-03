# -*- coding: utf-8 -*-
'''
Wikipedia data scraping functions

This notebook contains a variety of functions primarily for accessing the MediaWiki API to extract data page revisions, user revisions, article hyperlinks, category membership, and pageview dynamics.
These scripts invoke several non-standard libraries:

* WikiTools - https://code.google.com/p/python-wikitools/
* NetworkX - http://networkx.github.io/
* Pandas - http://pandas.pydata.org/
 
This code was primarily authored by Brian Keegan (bkeegan@gmail.com) in 2012 and 2013 with contributions from Nick Bennett (nick271828@gmail.com).
'''

from wikitools import wiki, api
import networkx as nx
import numpy as np
from operator import itemgetter
from collections import Counter
import re, random, datetime, urlparse, urllib2, simplejson, copy
import pandas as pd
from bs4 import BeautifulSoup

def is_ip(ip_string, masked=False):
	# '''
	# Input:
	# ip_string - A string we'd like to check if it matches the pattern of a valid IP address.
	# Output:
	# A boolean value indicating whether the input was a valid IP address.
	# '''
	if not isinstance(ip_string, str) and not isinstance(ip_string, unicode):
		return False
	if masked:
		ip_pattern = re.compile('((([\d]{1,3})|([Xx]{1,3}))\.){3}(([\d]{1,3})|([Xx]{1,3}))', re.UNICODE)
	else:
		ip_pattern = re.compile('([\d]{1,3}\.){3}([\d]{1,3})', re.UNICODE)
	if ip_pattern.match(ip_string):
		return True
	else:
		return False

def convert_to_datetime(string):
    dt = datetime.datetime.strptime(string,'%Y-%m-%dT%H:%M:%SZ')
    return dt
    
def convert_from_datetime(dt):
    string = dt.strftime('%Y%m%d%H%M%S')
    return string

def convert_datetime_to_epoch(dt):
    epochtime = (dt - datetime.datetime(1970,1,1)).total_seconds()
    return epochtime

def wikipedia_query(query_params,lang='en'):
	site = wiki.Wiki(url='http://'+lang+'.wikipedia.org/w/api.php')
	request = api.APIRequest(site, query_params)
	result = request.query()
	return result[query_params['action']]

def short_wikipedia_query(query_params,lang='en'):
	site = wiki.Wiki(url='http://'+lang+'.wikipedia.org/w/api.php')
	request = api.APIRequest(site, query_params)
	# Don't do multiple requests
	result = request.query(querycontinue=False)
	return result[query_params['action']]

def random_string(le, letters=True, numerals=False):
	def rc():
		charset = []
		cr = lambda x,y: range(ord(x), ord(y) + 1)
		if letters:
			charset += cr('a', 'z')
		if numerals:
			charset += cr('0', '9')
		return chr(random.choice(charset))
	def rcs(k):
		return [rc() for i in range(k)]
	return ''.join(rcs(le))

def clean_revision(rev):
	# We must deal with some malformed user/userid values. Some 
	# revisions have the following problems:
	# 1. no 'user' or 'userid' keys and the existence of the 'userhidden' key
	# 2. 'userid'=='0' and 'user'=='Conversion script' and 'anon'==''
	# 3. 'userid'=='0' and 'user'=='66.92.166.xxx' and 'anon'==''
	# 4. 'userid'=='0' and 'user'=='204.55.21.34' and 'anon'==''
	# In these cases, we must substitute a placeholder value
	# for 'userid' to uniquely identify the respective kind
	# of malformed revision as above. 
	revision = rev.copy()
	if 'userhidden' in revision:
		revision['user'] = random_string(15, letters=False, numerals=True)
		revision['userid'] = revision['user']
	elif 'anon' in revision:
		if revision['user']=='Conversion script':
			revision['user'] = random_string(14, letters=False, numerals=True)
			revision['userid'] = revision['user']
		elif is_ip(revision['user']):
			# Just leaving this reflection in for consistency
			revision['user'] = revision['user']
			# The weird stuff about multiplying '0' by a number is to 
			# make sure that IP addresses end up looking like this:
			# 192.168.1.1 -> 192168001001
			# This serves to prevent collisions if the numbers were
			# simply joined by removing the periods:
			# 215.1.67.240 -> 215167240
			# 21.51.67.240 -> 215167240
			# This also results in the number being exactly 12 decimal digits.
			revision['userid'] = ''.join(['0' * (3 - len(octet)) + octet \
											for octet in revision['user'].split('.')])
		elif is_ip(revision['user'], masked=True):
			# Let's distinguish masked IP addresses, like
			# 192.168.1.xxx or 255.XXX.XXX.XXX, by setting 
			# 'user'/'userid' both to a random 13 digit number
			# or 13 character string. 
			# This will probably be unique and easily 
			# distinguished from an IP address (with 12 digits
			# or characters). 
			revision['user'] = random_string(13, letters=False, numerals=True)
			revision['userid'] = revision['user']
	return revision

def cast_to_unicode(string):
    if isinstance(string,str):
        try:
            string2 = string.decode('utf8')
        except:
            try:
                string2 = string.decode('latin1')
            except:
                print "Some messed up encoding here"
    elif isinstance(string,unicode):
        string2 = string
    return string2

def chunk_maker(a_list,size):
    chunk_num = len(a_list)/size
    chunks = list()
    for c in range(chunk_num + 1):
        start = c * (size + 1)
        end = (c + 1) * (size + 1)
        elements = list(itertools.islice(a_list,start,end))
        if len(elements) > 0:
            chunks.append(elements)
    return chunks

def get_single_revision(article_list,lang):
    chunks = chunk_maker(article_list,50)
    revisions = dict()
    for chunk in chunks:
        titles = '|'.join(chunk)
        try:
            result = do_short_query({'titles': titles,
		'prop': 'revisions',
		'rvprop': 'ids|timestamp|user|userid|size|content',
		'rvlimit': '1',
		'rvdir': 'older',
		'action':'query',
		'redirects':'True'},lang)
            if result and 'pages' in result.keys() and '-1' not in result['pages'].keys():
                page_number = result['pages'].keys()[0]
                revs = result['pages'][page_number]['revisions']
                revs = sorted(revs, key=lambda r: r['timestamp'])
                for r in revs:
                    r['pageid'] = page_number
                    r['title'] = result['pages'][page_number]['title']
                    # Sometimes the size key is not present, so set it to 0 in those cases
                    r['size'] = r.get('size', 0)
                    r['timestamp'] = convert_datetime(r['timestamp'])
                    try:
                        # from http://stackoverflow.com/questions/4929082/python-regular-expression-with-wiki-text
                        links = re.findall(r'\[\[(?:[^|\]]*\|)?([^\]]+)\]\]',r['*'])
                        r['links'] = remove_spurious_links(links)
                    except KeyError:
                        r['links'] = list()
                        r['*'] = unicode()
                    revisions[a] = revs[0]
        except api.APIError:
            print u"Error in processing article list"
            pass
	return revisions

def get_user_revisions(user,dt_end,lang):
    '''
    Input: 
    user - The name of a wikipedia user with no "User:" prefix, e.g. 'Madcoverboy' 
    dt_end - a datetime object indicating the maximum datetime to return for revisions
    lang - a string (typically two characters) indicating the language version of Wikipedia to crawl

    Output:
    revisions - A list of revisions for the given article, each given as a dictionary. This will
            include all properties as described by revision_properties, and will also include the
            title and id of the source article. 
    '''
    user = cast_to_unicode(user)
    revisions = list()
    dt_end_string = convert_from_datetime(dt_end)
    result = wikipedia_query({'action':'query',
                              'list': 'usercontribs',
                              'ucuser': u"User:"+user,
                              'ucprop': 'ids|title|timestamp|sizediff',
                              #'ucnamespace':'0',
                              'uclimit': '500',
                              'ucend':dt_end_string},lang)
    if result and 'usercontribs' in result.keys():
            r = result['usercontribs']
            r = sorted(r, key=lambda revision: revision['timestamp'])
            for revision in r:
                    # Sometimes the size key is not present, so we'll set it to 0 in those cases
                    revision['sizediff'] = revision.get('sizediff', 0)
                    revision['timestamp'] = convert_to_datetime(revision['timestamp'])
                    revisions.append(revision)
    return revisions

def get_user_properties(user,lang):
    '''
    Input:
    user - a string with no "User:" prefix corresponding to the username ("Madcoverboy"
    lang - a string (usually two digits) for the language version of Wikipedia to query

    Output:
    result - a dictionary containing attrubutes about the user
    '''
    user = cast_to_unicode(user)
    result = wikipedia_query({'action':'query',
                                'list':'users',
                                'usprop':'blockinfo|groups|editcount|registration|gender',
                                'ususers':user},lang)
    return result
    
def make_user_alters(revisions):
    '''
    Input:
    revisions - a list of revisions generated by get_user_revisions

    Output:
    alters - a dictionary keyed by page name that returns a dictionary containing
        the count of how many times the user edited the page, the timestamp of the user's
        earliest edit to the page, the timestamp the user's latest edit to the page, and 
        the namespace of the page itself
    '''
    alters = dict()
    for rev in revisions:
        if rev['title'] not in alters.keys():
            alters[rev['title']] = dict()
            alters[rev['title']]['count'] = 1
            alters[rev['title']]['min_timestamp'] = rev['timestamp']
            alters[rev['title']]['max_timestamp'] = rev['timestamp']
            alters[rev['title']]['ns'] = rev['ns']
        else:
            alters[rev['title']]['count'] += 1
            alters[rev['title']]['max_timestamp'] = rev['timestamp']
    return alters

def rename_on_redirect(article_title,lang='en'):
    '''
    Input:
    article_title - a string with the name of the article or page that may be redirected to another title
    lang - a string (typically two characters) indicating the language version of Wikipedia to crawl

    Output:
    article_title - a string with the name of the article or page that the redirect resolves to
    '''
    result = short_wikipedia_query({'titles': article_title,
                                  'prop': 'info',
                                  'action': 'query',
                                  'redirects': 'True'},lang)
    if 'redirects' in result.keys() and 'pages' in result.keys():
        article_title = result['redirects'][0]['to']
    return article_title

def get_page_revisions(article_title,dt_start,dt_end,lang):
    '''
    Input: 
    article - A string with the name of the article or page to crawl
    dt_start - A datetime object indicating the minimum datetime to return for revisions
    dt_end - a datetime object indicating the maximum datetime to return for revisions
    lang - a string (typically two characters) indicating the language version of Wikipedia to crawl
    
    Output:
    revisions - A list of revisions for the given article, each given as a dictionary. This will
            include all properties as described by revision_properties, and will also include the
            title and id of the source article. 
    '''
    article_title = rename_on_redirect(article_title,lang=lang)
    dt_start_string = convert_from_datetime(dt_start)
    dt_end_string = convert_from_datetime(dt_end) 
    revisions = list()
    result = wikipedia_query({'titles': article_title,
                              'prop': 'revisions',
                              'rvprop': 'ids|timestamp|user|userid|size',
                              'rvlimit': '5000',
                              'rvstart': dt_start_string,
                              'rvend': dt_end_string,
                              'rvdir': 'newer',
                              'action': 'query'},lang)
    if result and 'pages' in result.keys():
            page_number = result['pages'].keys()[0]
            try:
                r = result['pages'][page_number]['revisions']
                for revision in r:
                        revision['pageid'] = page_number
                        revision['title'] = result['pages'][page_number]['title']
                        # Sometimes the size key is not present, so we'll set it to 0 in those cases
                        revision['username'] = revision['user']
                        revision['size'] = revision.get('size', 0)
                        revision['timestamp'] = convert_to_datetime(revision['timestamp'])
                        revisions.append(revision)
            except KeyError:
                revisions = list()
    return revisions

def make_page_alters(revisions):
    '''
    Input:
    revisions - a list of revisions generated by get_page_revisions

    Output:
    alters - a dictionary keyed by user name that returns a dictionary containing
    the count of how many times the user edited the page, the timestamp of the user's
    earliest edit to the page, the timestamp the user's latest edit to the page, and 
    the namespace of the page itself
    '''
    alters = dict()
    for rev in revisions:
        if rev['user'] not in alters.keys():
            alters[rev['user']] = dict()
            alters[rev['user']]['count'] = 1
            alters[rev['user']]['min_timestamp'] = rev['timestamp']
            alters[rev['user']]['max_timestamp'] = rev['timestamp']
        else:
            alters[rev['user']]['count'] += 1
            alters[rev['user']]['max_timestamp'] = rev['timestamp']
    return alters

def get_page_content(page_title,dt_start,dt_end,lang):
    '''
    Input: 
    page_title - A string with the name of the article or page to crawl
    lang - A string (typically two characters) indicating the language version of Wikipedia to crawl

    Output:
    revisions_dict - A dictionary of revisions for the given article keyed by revision ID returning a 
            a dictionary of revision attributes. These attributes include all properties as described 
            by revision_properties, and will also include the title and id of the source article. 
    '''
    page_title = rename_on_redirect(page_title,lang=lang)
    dt_start_string = convert_from_datetime(dt_start)
    dt_end_string = convert_from_datetime(dt_end)
    revisions_dict = dict()
    result = wikipedia_query({'titles': page_title,
                              'prop': 'revisions',
                              'rvprop': 'ids|timestamp|user|userid|size|content',
                              'rvlimit': '5000',
                              'rvstart': dt_start_string,
                              'rvend': dt_end_string,
                              'rvdir': 'newer',
                              'action': 'query'},lang)
    if result and 'pages' in result.keys():
        page_number = result['pages'].keys()[0]
        try:
            revisions = result['pages'][page_number]['revisions']
            for revision in revisions:
                rev = dict()
                rev['pageid'] = page_number
                rev['title'] = result['pages'][page_number]['title']
                rev['size'] = revision.get('size', 0) # Sometimes the size key is not present, so we'll set it to 0 in those cases
                rev['timestamp'] = convert_to_datetime(revision['timestamp'])
                rev['content'] = revision.get('*',unicode()) # Sometimes content hidden, return with empty unicode string
                rev['links'] = link_finder(rev['content'])
                rev['username'] = revision['user']
                rev['revid'] = revision['revid']
                revisions_dict[revision['revid']] = rev
        except KeyError:
            pass
    return revisions_dict

def adjacency_calcs(revisions):
    revisions = sorted(revisions,key=itemgetter('pageid','timestamp'))
    revisions[0]['position'] = 0
    revisions[0]['edit_lag'] = datetime.timedelta(0)
    revisions[0]['bytes_added'] = revisions[0]['size']
    revisions[0]['unique_users'] = [revisions[0]['username']]
    revisions[0]['unique_users_count'] = 1
    revisions[0]['article_age'] = 0
    for num,rev in enumerate(revisions[:-1]):
        revisions[num+1]['position'] = rev['position'] + 1
        revisions[num+1]['edit_lag'] = revisions[num+1]['timestamp'] - rev['timestamp']
        revisions[num+1]['bytes_added'] = revisions[num+1]['size'] - rev['size']
        
        revisions[num+1]['unique_users'] = rev['unique_users']
        revisions[num+1]['unique_users'].append(revisions[num+1]['username'])
        revisions[num+1]['unique_users'] = list(set(revisions[num+1]['unique_users']))
        
        revisions[num+1]['unique_users_count'] = len(revisions[num+1]['unique_users'])
        revisions[num+1]['article_age'] = revisions[num+1]['timestamp'] - revisions[0]['timestamp']
    return revisions

def get_category_members(category_name, depth, lang='en'):
    '''
    Input: 
    category_name - The name of a Wikipedia(en) category, e.g. 'Category:2001_fires'. 
    depth - An integer in the range [0,n) reflecting the number of sub-categories to crawl
    lang - A string (typically two-digits) corresponding to the language code for the Wikipedia to crawl

    Output:
    articles - A list of articles that are found within the given category or one of its
        subcategories, explored recursively. Each article will be a dictionary object with
        the keys 'title' and 'id' with the values of the individual article's title and 
        page_id respectively. 
    '''
    articles = []
    if depth < 0:
        return articles
    
    #Begin crawling articles in category
    results = wikipedia_query({'list': 'categorymembers',
                                   'cmtitle': category_name,
                                   'cmtype': 'page',
                                   'cmlimit': '500',
                                   'action': 'query'},lang)  
    if 'categorymembers' in results.keys() and len(results['categorymembers']) > 0:
        for i, page in enumerate(results['categorymembers']):
            article = page['title']
            articles.append(article)
    
    # Begin crawling subcategories
    results = wikipedia_query({'list': 'categorymembers',
                                   'cmtitle': category_name,
                                   'cmtype': 'subcat',
                                   'cmlimit': '500',
                                   'action': 'query'},lang)
    subcategories = []
    if 'categorymembers' in results.keys() and len(results['categorymembers']) > 0:
        for i, category in enumerate(results['categorymembers']):
            cat_title = category['title']
            subcategories.append(cat_title)
    for category in subcategories:
        articles += get_category_members(category,depth-1)      
    return articles

def get_page_categories(page_title,lang='en'):
    '''
    Input:
    page_title - A string with the name of the article or page to crawl
    lang - A string (typically two-digits) corresponding to the language code for the Wikipedia to crawl

    Output:
    categories - A list of the names of the categories of which the page is a member
    '''
    page_title = rename_on_redirect(page_title,lang=lang)
    results = wikipedia_query({'prop': 'categories',
                                   'titles': page_title,
                                   'cllimit': '500',
                                   'clshow':'!hidden',
                                   'action': 'query'},lang)
    if 'pages' in results.keys():
        page_number = results['pages'].keys()[0]
        categories = results['pages'][page_number]['categories']
        categories = [i['title'] for i in categories]
        categories = [i for i in categories if i != u'Category:Living people']
    else:
        print u"{0} not found in category results".format(page_title)
    return categories

def get_article_logevents(article_name,lang,start_timestamp):
    '''
    Given a string article_name, two-digit language string (lang) and
    datetime timestamps is the start date of the range
    '''
    log_events = list()
    results = do_query({'action':'query',
                            'list':'logevents',
                            'letitle':article_name,
                            'ledir':'newer',
                            'leprop':'ids|title|type|user|userid|timestamp|comment|details|tags',
                            'lestart':start_timestamp.strftime("%Y%m%d%H%M%S")},
                            lang)
    events = results['logevents']
    
    if len(events) > 0:
        for event in events:
            new_event = dict()
            new_event['action'] = event['action']
            new_event['type'] = event['type']
            new_event['start_timestamp'] = convert_datetime(event['timestamp'])
            
            new_event['comment1'] = event.get('comment',unicode())
            new_event['comment2'] = event.get('0',unicode())
            
            # Loop to get if there are defined expiry dates on page protections
            if 'expires' in new_event['comment1']:
                new_event['expiry'] = event['comment']
            elif 'expires' in new_event['comment2']:
                new_event['expiry'] = event['0']
                
            # Assume protection is indefinite unless changed below
            new_event['end_timestamp'] = u'indefinite'
            
            # Extract end timestamps from comment fields
            try:
                date_string = re.findall(r'\(expires\s?([^\)]*)\s\(UTC\)\)',new_event['expiry'])[0]
                end_timestamp = datetime.datetime.strptime(date_string,"%H:%M, %d %B %Y")
                new_event['end_timestamp'] = end_timestamp
            except:
                pass
            
            log_events.append(new_event)
    else:
         print u"WARNING! {0} HAS NO LOG EVENTS!".format(article_name)
    
    return log_events

def get_page_outlinks(page_title,lang='en'):
    '''
    Input:
    page_title - A string with the name of the article or page to crawl
    lang - A string (typically two-digits) corresponding to the language code for the Wikipedia to crawl

    Output:
    outlinks - A list of all "alter" pages that link out from the current version of the "ego" page

    Notes:
    This uses API calls to return all [[links]] which may be slower and result in overlinking from templates
    '''
    # This approach is susceptible to 'overlinking' as it includes links from templates
    page_title = cast_to_unicode(page_title)
    page_title = rename_on_redirect(page_title,lang=lang)
    result = wikipedia_query({'titles': page_title,
                                  'prop': 'links',
                                  'pllimit': '500',
                                  'plnamespace':'0',
                                  'action': 'query'},lang)
    if 'pages' in result.keys():
        page_number = result['pages'].keys()[0]
        results = result['pages'][page_number]['links']
        outlinks = [l['title'] for l in results]
    else:
        print u"Error: No links found in {0}".format(page_title)
    return outlinks

def get_page_inlinks(page_title,lang='en'):
    '''
    Input:
    page_title - A string with the name of the article or page to crawl
    lang - A string (typically two-digits) corresponding to the language code for the Wikipedia to crawl

    Output:
    inlinks - A list of all "alter" pages that link in to the current version of the "ego" page
    '''
    page_title = cast_to_unicode(page_title)
    page_title = rename_on_redirect(page_title,lang=lang)
    result = wikipedia_query({'bltitle': page_title,
                                  'list': 'backlinks',
                                  'bllimit': '500',
                                  'blnamespace':'0',
                                  'blfilterredir':'nonredirects',
                                  'action': 'query'},lang)
    if 'backlinks' in result.keys():
        results = result['backlinks']
        inlinks = [l['title'] for l in results]
    else:
        print u"Error: No links found in {0}".format(article_title)
    return inlinks

# Links inside templates are included which results in completely-connected components
# Remove links from templates by getting a list of templates used across all pages
def get_page_templates(page_title,lang):
    '''
    Input:
    page_title - A string with the name of the article or page to crawl
    lang - A string (typically two-digits) corresponding to the language code for the Wikipedia to crawl

    Output:
    templates - A list of all the templates (which contain redundant links) in the current version
    '''
    page_title = cast_to_unicode(page_title)
    page_title = rename_on_redirect(page_title,lang=lang)
    result = wikipedia_query({'titles': page_title,
                                  'prop': 'templates',
                                  'tllimit': '500',
                                  'action': 'query'},lang)
    if 'pages' in result.keys():
        page_id = result['pages'].keys()[0]
        templates = [i['title'] for i in result['pages'][page_id]['templates']]
    return templates

def get_page_links(page_title,lang='en'):
    '''
    Input:
    page_title - A string with the name of the article or page to crawl that is the "ego" page
    lang - A string (typically two-digits) corresponding to the language code for the Wikipedia to crawl

    Output:
    links - A dictionary keyed by ['in','out'] of all "alter" pages that link in to and out from the 
        current version of the "ego" page
    '''
    links=dict()
    links['in'] = get_page_inlinks(page_title,lang)
    links['out'] = get_page_outlinks(page_title,lang)
    return links

# Identify links based on content of revisions
def link_finder(content_string):
    '''
    Input:
    content_string - A string containing the raw wiki-markup for a page

    Output:
    links - A list of all "alter" pages that link out from the current version of the "ego" page

    Notes:
    This uses regular expressions to coarsely parse the content for instances of [[links]] and likely returns messy data
    '''
    links = list()
    for i,j in re.findall(r'\[\[([^|\]]*\|)?([^\]]+)\]\]',content_string):
        if len(i) == 0:
            links.append(j)
        elif u'#' not in i :
            links.append(i[:-1])
        elif u'#' in i:
            new_i = i[:i.index(u'#')]
            links.append(new_i)
    links = [l for l in links if u'|' not in l and u'Category:' not in l and u'File:' not in l]
    return links

def get_page_outlinks_from_content(page_title,lang='en'):
    '''
    Input:
    page_title - A string with the name of the article or page to crawl that is the "ego" page
    lang - A string (typically two-digits) corresponding to the language code for the Wikipedia to crawl

    Output:
    links - A list of all "alter" pages that link out from the current version of the "ego" page

    Notes:
    This uses regular expressions to coarsely parse the content for instances of [[links]] and may be messy
    '''
    page_title = cast_to_unicode(page_title)
    page_title = rename_on_redirect(page_title,lang=lang)
    
    # Get content from most recent revision of an article
    result = short_wikipedia_query({'titles': page_title,
                                  'prop': 'revisions',
                                  'rvlimit': '1',
                                  'rvprop':'ids|timestamp|user|userid|content',
                                  'action': 'query'},lang)
    if 'pages' in result.keys():
        page_id = result['pages'].keys()[0]
        content = result['pages'][page_id]['revisions'][0]['*']
        links = link_finder(content)
    else:
        print u'...Error in {0}'.format(page_title)
        links = list()
        
    return links
    
def get_user_outdiscussion(user_name,dt_end,lang='en'):
    '''
    Input:
    user_name - The name of a "ego" wikipedia user with no "User:" prefix, e.g. 'Madcoverboy' 
    dt_end - a datetime object indicating the maximum datetime to return for revisions
    lang - a string (typically two characters) indicating the language version of Wikipedia to crawl

    Output:
    users - A list of all "alter" user talk pages that the ego has ever posted to
    '''
    # User revision code in only user namespace
    user_name = cast_to_unicode(user_name)
    users = dict()
    dt_end_string = convert_from_datetime(dt_end)
    result = wikipedia_query({'action':'query',
                                  'list': 'usercontribs',
                                  'ucuser': u"User:"+user_name,
                                  'ucprop': 'ids|title|timestamp|sizediff',
                                  'ucnamespace':'3',
                                  'uclimit': '500',
                                  'ucend':dt_end_string},lang)
    if result and 'usercontribs' in result.keys():
        r = result['usercontribs']
        for rev in r:
            alter = rev['title'][10:] # Ignore "User talk:"
            if alter not in users.keys():
                users[alter] = dict()
                users[alter]['count'] = 1
                users[alter]['min_timestamp'] = rev['timestamp']
                users[alter]['max_timestamp'] = rev['timestamp']
            else:
                users[alter]['count'] += 1
                users[alter]['max_timestamp'] = rev['timestamp']
    return users

def get_user_indiscussion(user_name,dt_end,lang='en'):
    '''
    Input:
    user_name - The name of a "ego" wikipedia user with no "User:" prefix, e.g. 'Madcoverboy' 
    dt_end - a datetime object indicating the maximum datetime to return for revisions
    lang - a string (typically two characters) indicating the language version of Wikipedia to crawl

    Output:
    users - A list of all "alter" user talk pages that have ever posted to the user's talk page
    '''
    # Article revision code in only user talk page
    user_name = cast_to_unicode(user_name)
    users = dict()
    dt_end_string = convert_from_datetime(dt_end)
    result = wikipedia_query({'titles': u'User talk:'+user_name,
                                  'prop': 'revisions',
                                  'rvprop': 'ids|timestamp|user|userid|size',
                                  'rvlimit': '5000',
                                  'rvend': dt_end_string,
                                  'action': 'query'},lang)
    if result and 'pages' in result.keys():
        page_number = result['pages'].keys()[0]
        try:
            r = result['pages'][page_number]['revisions']
            for rev in r:
                if rev['user'] not in users.keys():
                    users[rev['user']] = dict()
                    users[rev['user']]['count'] = 1
                    users[rev['user']]['min_timestamp'] = rev['timestamp']
                    users[rev['user']]['max_timestamp'] = rev['timestamp']
                else:
                    users[rev['user']]['count'] += 1
                    users[rev['user']]['max_timestamp'] = rev['timestamp']
        except KeyError:
            pass
    return users

def get_user_discussion(user_name,dt_end,lang='en'):
    '''
    Input:
    user_name - The name of a "ego" wikipedia user with no "User:" prefix, e.g. 'Madcoverboy' 
    dt_end - a datetime object indicating the maximum datetime to return for revisions
    lang - a string (typically two characters) indicating the language version of Wikipedia to crawl

    Output:
    users - A dictionary keyed by the values ['in','out'] that combines both get_user_outdiscussion and
        get_user_indiscussion
    '''
    users=dict()
    users['out'] = get_user_outdiscussion(user_name,dt_end,lang)
    users['in'] = get_user_indiscussion(user_name,dt_end,lang)
    return users

def make_article_trajectory(revisions):
    '''
    Input:
    revisions - A list of revisions generated by get_page_revisions

    Output:
    g - A NetworkX DiGraph object corresponding to the trajectory of an article moving between users
        Nodes are users and links from i to j exist when user i made a revision immediately following user j
    '''
    g = nx.DiGraph()
    # Sort revisions on ascending timestamp
    sorted_revisions = sorted(revisions,key=lambda k:k['timestamp'])

    # Don't use the last revision
    for num,rev in enumerate(sorted_revisions[:-1]):
        # Edge exists between user and user in next revision
        edge = (rev['user'],revisions[num+1]['user'])
        if g.has_edge(*edge):
            g[edge[0]][edge[1]]['weight'] += 1
        else:
            g.add_edge(*edge,weight=1)
    return g

def make_editor_trajectory(revisions):
    '''
    Input:
    revisions - A list of revisions generated by get_user_revisions

    Output:
    g - A NetworkX DiGraph object corresponding to the trajectory of a user moving between articles
        Nodes are pages and links from i to j exist when page i was edited by the user immediately following page j
    '''
    g = nx.DiGraph()
    # Sort revisions on ascending timestamp
    sorted_revisions = sorted(revisions,key=lambda k:k['timestamp'])

    # Don't use the last revision
    for num,rev in enumerate(sorted_revisions[:-1]):
        # Edge exists between user and user in next revision
        edge = (rev['title'],revisions[num+1]['user'])
        if g.has_edge(*edge):
            g[rev['title']][revisions[num+1]['user']]['weight'] += 1
        else:
            g.add_edge(*edge,weight=1)
    return g

def fixurl(url):
    # turn string into unicode
    if not isinstance(url,unicode):
        url = url.decode('utf8')

    # parse it
    parsed = urlparse.urlsplit(url)

    # divide the netloc further
    userpass,at,hostport = parsed.netloc.rpartition('@')
    user,colon1,pass_ = userpass.partition(':')
    host,colon2,port = hostport.partition(':')

    # encode each component
    scheme = parsed.scheme.encode('utf8')
    user = urllib2.quote(user.encode('utf8'))
    colon1 = colon1.encode('utf8')
    pass_ = urllib2.quote(pass_.encode('utf8'))
    at = at.encode('utf8')
    host = host.encode('idna')
    colon2 = colon2.encode('utf8')
    port = port.encode('utf8')
    path = '/'.join(  # could be encoded slashes!
        urllib2.quote(urllib2.unquote(pce).encode('utf8'),'')
        for pce in parsed.path.split('/')
    )
    query = urllib2.quote(urllib2.unquote(parsed.query).encode('utf8'),'=&?/')
    fragment = urllib2.quote(urllib2.unquote(parsed.fragment).encode('utf8'))

    # put it back together
    netloc = ''.join((user,colon1,pass_,at,host,colon2,port))
    return urlparse.urlunsplit((scheme,netloc,path,query,fragment))

def convert_months_to_strings(m):
	if len(str(m)) > 1:
		new_m = unicode(m)
	else:
		new_m = u'0'+unicode(m)
	return new_m

def get_url(article_name,lang,month,year):
    url = u"http://stats.grok.se/json/" + lang + u"/" + unicode(year) + convert_months_to_strings(month) + u"/" + article_name
    fixed_url = fixurl(url)
    return fixed_url

def requester(url):
    opener = urllib2.build_opener()
    req = urllib2.Request(url)
    f = opener.open(req)
    r = simplejson.load(f)
    result = pd.Series(r['daily_views'])
    return result

def clean_timestamps(df):
    to_drop = list()
    for d in df.index:
        try:
            datetime.date(int(d[0:4]),int(d[5:7]),int(d[8:10]))
        except ValueError:
            to_drop.append(d)
    df2 = df.drop(to_drop,axis=0)
    df2.index = pd.to_datetime(df2.index)
    return df2

def get_pageviews(article,lang,min_date,max_date):
    article = rename_on_redirect(article,lang=lang)
    rng = pd.date_range(min_date,max_date,freq='M')
    rng2 = [(i.month,i.year) for i in rng]
    ts = pd.Series()
    for i in rng2:
        url = get_url(article,lang,i[0],i[1])
        result = requester(url)
        ts = pd.Series.append(result,ts)
    ts = ts.sort_index()
    ts = clean_timestamps(ts)
    ts = ts.asfreq('D')
    return ts

def make_pageview_df(article_list,lang,min_date,max_date):
    df = pd.DataFrame(index=pd.date_range(start=min_date,end=max_date))
    l = len(article_list)
    for num,article in enumerate(article_list):
        try:
            print u"{0} / {1} : {2}".format(num+1,l,article)
            ts = get_pageviews(article,lang,min_date,max_date)
            df[article] = ts
        except:
            print u'Something happened to {0}'.format(unicode(article))
            pass
    return df

def revision_counter(revisions,min_date,max_date):
    dd = dict()
    for r in revisions:
        d = r['timestamp'].date()
        try:
            dd[d] += 1
        except KeyError:
            dd[d] = 1
    di = [datetime.datetime.combine(i,datetime.time()) for i in dd.keys()]
    ts = pd.TimeSeries(dd.values(),index=di)
    ts = ts.reindex(pd.date_range(np.min(list(ts.index)),np.max(list(ts.index))),fill_value=0)
    return ts[min_date:max_date]

def size_counter(revisions,min_date,max_date):
    dd = dict()
    dd2 = dict()
    for r in revisions:
        d = r['timestamp'].date()
        try:
            dd[d].append(r['size'])
        except KeyError:
            dd[d] = [r['size']]
    for k,v in dd.items():
        dd2[k] = np.median(v)
    di = [datetime.datetime.combine(i,datetime.time()) for i in dd2.keys()]
    ts = pd.TimeSeries(dd2.values(),index=di)
    ts = ts.reindex(pd.date_range(np.min(list(ts.index)),np.max(list(ts.index))))
    ts = ts.fillna(method='ffill')
    return ts[min_date:max_date]

def pageview_counter(article_title,lang,min_date,max_date):
    ts = get_pageviews(article_title,lang,min_date,max_date)
    return ts[min_date:max_date]

def link_counter(revisions,min_date,max_date):
    ld = dict()
    ld2 = dict()
    for r in revisions:
        d = r['timestamp'].date()
        try:
            ld[d] += r['links']
        except KeyError:
            ld[d] = r['links']
    for k,v in ld.items():
        ld2[k] = len(set(ld[k]))
    di = [datetime.datetime.combine(i,datetime.time()) for i in ld2.keys()]
    ts = pd.TimeSeries(ld2.values(),index=di)
    ts = ts.reindex(pd.date_range(np.min(list(ts.index)),np.max(list(ts.index))))
    ts = ts.fillna(method='ffill')
    return ts[min_date:max_date]

def word_counter(revisions,min_date,max_date):
    ld = dict()
    ld2 = dict()
    for r in revisions:
        d = r['timestamp'].date()
        words1 = r['content'].lower().split()
        words2 = len([i for i in words1 if u":" not in i and u"{{" not in i and u"}}" not in i and u"|" not in i and u"=" not in i])
        try:
            ld[d].append(words2)
        except KeyError:
            ld[d] = [words2]
    for k,v in ld.items():
        ld2[k] = np.median(v)
    di = [datetime.datetime.combine(i,datetime.time()) for i in ld2.keys()]
    ts = pd.TimeSeries(ld2.values(),index=di)
    ts = ts.reindex(pd.date_range(np.min(list(ts.index)),np.max(list(ts.index))))
    ts = ts.fillna(method='ffill')
    return ts[min_date:max_date]
    
def user_counter(revisions,min_date,max_date):
    dd = dict()
    dd2 = dict()
    for r in revisions:
        d = r['timestamp'].date()
        try:
            dd[d].append(r['unique_users_count'])
        except KeyError:
            dd[d] = [r['unique_users_count']]
    for k,v in dd.items():
        dd2[k] = np.max(v)
    di = [datetime.datetime.combine(i,datetime.time()) for i in dd2.keys()]
    ts = pd.TimeSeries(dd2.values(),index=di)
    ts = ts.reindex(pd.date_range(np.min(list(ts.index)),np.max(list(ts.index))))
    ts = ts.fillna(method='ffill')
    return ts[min_date:max_date]

talk_dict = {'en':u"Talk:",'pt':"Discussão:".decode('utf-8'),'es':"Discusión:".decode('utf-8')}

def get_editing_dynamics(article_name,min_date,max_date,lang):
    if type(article_name) == str:
        try:
            article_name = article_name.decode('utf-8')
        except UnicodeDecodeError:
            print 'Cannot decode article name into Unicode using UTF8'
            
    talk_dict = {'en':u"Talk:",'pt':"Discussão:".decode('utf-8'),'es':"Discusión:".decode('utf-8')}
    
    r1 = get_page_content(article_name,datetime.datetime(2001,1,1),max_date,lang)
    r2 = get_page_content(talk_dict[lang]+article_name,datetime.datetime(2001,1,1),max_date,lang)
    
    r1 = adjacency_calcs(r1.values())
    
    ts1 = revision_counter(r1,min_date,max_date)
    ts2 = revision_counter(r2.values(),min_date,max_date)
    ts3 = user_counter(r1,min_date,max_date)
    ts4 = size_counter(r1,min_date,max_date)
    ts5 = link_counter(r1,min_date,max_date)
    ts6 = word_counter(r1,min_date,max_date)
    
    ts1_name = u"Article"
    ts2_name = u"Talk"
    ts3_name = u"Users"
    ts4_name = u"Size"
    ts5_name = u"Outlinks"
    ts6_name = u"Words"
    
    dft = pd.concat([ts1,ts2,ts3,ts4,ts5,ts6],axis=1)
    dft.columns = [ts1_name,ts2_name,ts3_name,ts4_name,ts5_name,ts6_name]
    dft.to_csv(article_name+u'.csv')
    return dft
    
def get_editing_dynamics2(article_name,min_date,max_date,lang):
    if type(article_name) == str:
        try:
            article_name = article_name.decode('utf-8')
        except UnicodeDecodeError:
            print 'Cannot decode article name into Unicode using UTF8'
    
    r1 = get_page_revisions(article_name,datetime.datetime(2001,1,1),max_date,lang)
    
    r1 = adjacency_calcs(r1)
    
    ts1 = revision_counter(r1,min_date,max_date)
    ts3 = user_counter(r1,min_date,max_date)
    ts4 = size_counter(r1,min_date,max_date)
    
    ts1_name = u"Article"
    ts3_name = u"Users"
    ts4_name = u"Size"
    
    dft = pd.concat([ts1,ts3,ts4],axis=1)
    dft.columns = [ts1_name,ts3_name,ts4_name]
    dft.to_csv(article_name+u'.csv')
    return dft

def editors_other_activity(article_title,dt_start,dt_end,ignorelist,lang):
    revisions = get_page_revisions(article_title,dt_start,dt_end,lang)
    revision_alters = make_page_alters(revisions)
    revision_alters2 = {k:v for k,v in revision_alters.iteritems() if k not in ignorelist}
    
    alter_contributions = dict()
    for num,editor_alter in enumerate(revision_alters2.keys()):
        print u"{0} / {1}: {2}".format(num+1,len(revision_alters2.keys()),editor_alter)
        alter_contributions[editor_alter] = get_user_revisions(editor_alter,dt_start,lang)
        
    #el = directed_dict_to_edgelist(alter_discussions)
    return revisions,alter_contributions

def editing_primary_discussion_secondary(article_title,dt_start,dt_end,ignorelist,lang):
    revisions = get_page_revisions(article_title,dt_start,dt_end,lang)
    revision_alters = make_page_alters(revisions)
    revision_alters2 = {k:v for k,v in revision_alters.iteritems() if k not in ignorelist}
    
    alter_discussions = dict()
    for num,editor_alter in enumerate(revision_alters2.keys()):
        print u"{0} / {1}: {2}".format(num+1,len(revision_alters2.keys()),editor_alter)
        alter_discussions[editor_alter] = get_user_discussion(editor_alter,dt)
        
    #el = directed_dict_to_edgelist(alter_discussions)
    return revisions,alter_discussions

def editing_primary_hyperlink_secondary(article_title,dt_start,dt_end,ignorelist):
    revisions = get_page_revisions(article_title,dt_start,dt_end,lang)
    revision_alters = make_page_alters(revisions)
    revision_alters2 = {k:v for k,v in revision_alters.iteritems() if k not in ignorelist}
    
    alter_hyperlinks = dict()
    for num,editor_alter in enumerate(revision_alters2.keys()):
        print u"{0} / {1}: {2}".format(num+1,len(revision_alters2.keys()),editor_alter)
        alter_discussions[editor_alter] = get_page_outlinks(editor_alter,dt)
        
    el = directed_dict_to_edgelist(alter_discussions)
    return revisions,alter_discussions,el

def two_step_editing(article_title,dt,ignorelist):
    revisions = get_page_revisions(article_title,dt)
    revision_alters = make_page_alters(revisions)
    revision_alters2 = {k:v for k,v in revision_alters.iteritems() if k not in ignorelist}
    
    alter_revisions = dict()
    for num,editor_alter in enumerate(revision_alters2.keys()):
        print u"{0} / {1}: {2}".format(num+1,len(revision_alters2.keys()),editor_alter)
        alter_revisions[editor_alter] = get_user_revisions(editor_alter,dt)
    return revisions, alter_revisions

def two_step_outlinks(page_title):
    page_alters = dict()
    templates_dict = dict()
    
    links = get_page_outlinks(page_title)
    page_alters[unicode(page_title)] = links
    
    templates = get_page_templates(page_title)
    templates_dict[page_title] = templates
    
    l = len(links)
    for num,link in enumerate(links):
        print u"{0} / {1} : {2}".format(num+1,l,link)
        try:
            page_alters[link] = get_page_outlinks(link)
            templates_dict[link] = get_page_templates(link)
        except:
            print u"...{0} doesn't exist".format(link)
            pass
    return page_alters,templates_dict

def two_step_outlinks_from_content(page_title):
    page_alters = dict()
    
    links = get_page_outlinks_from_content(page_title)
    unique_links = list(set(links))
    page_alters[unicode(page_title)] = unique_links
    
    l = len(unique_links)
    for num,link in enumerate(unique_links):
        print u"{0} / {1} : {2}".format(num+1,l,link)
        try:
            page_alters[link] = get_page_outlinks_from_content(link)
        except:
            print u"...{0} doesn't exist".format(link)
            pass
    return page_alters

def make_hyperlink_network(hyperlink_dict):
    hyperlink_g = nx.DiGraph()
    for page,links in hyperlink_dict.iteritems():
        for link in links:
            # Only include links to 1-step alter pages, not 2-step alters' alters
            if link in hyperlink_dict.keys():
                hyperlink_g.add_edge(page,link)
    return hyperlink_g

def make_shared_user_editing_network(alter_revisions_dict,threshold):
    # Make the graph
    net = nx.DiGraph()
    for editor,revisions in alter_revisions_dict.iteritems():
        articles = [r['title'] for r in revisions]
        for num,article in enumerate(articles[:-1]):
            if net.has_edge(article,articles[num+1]):
                net[article][articles[num+1]]['weight'] += 1
            else:
                net.add_edge(article,articles[num+1],weight=1)
                
    # If edge is below threshold, remove it            
    for i,j,d in net.edges_iter(data=True):
        if d['weight'] < threshold:
            net.remove_edge(i,j)
            
    # Remove self-loops
    for i,j,d in net.edges_iter(data=True):
        if i == j:
            net.remove_edge(i,j)
    
    # Remove resulting isolates
    isolates = nx.isolates(net)
    for isolate in isolates:
        net.remove_node(isolate)
    
    return net

# Take the alter_revisions_dict keyed by user with a list of revisions
# And return an inverted alter_pages keyed by page with a dictionary of users
def invert_alter_revisions(alter_revisions_dict):
    alter_pages = dict()
    for user,revisions in alter_revisions_dict.iteritems():
        temp_list = list()
        for revision in revisions:
            temp_list.append(revision['title'])
        alter_pages[user] = dict(Counter(temp_list))

    inverted_alter_pages = dict()
    for user,counts in alter_pages.iteritems():
        for article,count in counts.iteritems():
            try:
                inverted_alter_pages[article][user] = count
            except KeyError:
                inverted_alter_pages[article] = dict()
                inverted_alter_pages[article][user] = count
    
    return inverted_alter_pages

def make_shared_page_editing_network(alter_revisions_dict,threshold):
    
    inverted_alter_revisions_dict = invert_alter_revisions(alter_revisions_dict)
    
    # Make the graph
    g = nx.DiGraph()
    for page,users in inverted_alter_revisions_dict.iteritems():
        user_list = users.keys()
        for num,user in enumerate(user_list[:-1]):
            next_user = user_list[num+1]
            if g.has_edge(user,next_user):
                g[user][next_user]['weight'] += 1
            else:
                g.add_edge(user,next_user,weight=1)
                
    # If edge is below threshold, remove it            
    for i,j,d in g.edges_iter(data=True):
        if d['weight'] < threshold:
            g.remove_edge(i,j)
            
    # Remove self-loops
    for i,j,d in g.edges_iter(data=True):
        if i == j:
            g.remove_edge(i,j)
    
    # Remove resulting isolates
    isolates = nx.isolates(g)
    for isolate in isolates:
        g.remove_node(isolate)
    
    return g

def make_category_network(categories_dict):
    '''Takes a dictionary keyed by page name with list of categories as values
    Returns a two-mode (enforced by DiGraph) page-category
    '''
    g_categories=nx.DiGraph()

    for page,categories in categories_dict.iteritems():
        for category in categories:
            g_categories.add_node(page,node_type='page')
            g_categories.add_node(category,node_type='category')
            g_categories.add_edge(page,category)

    return g_categories
