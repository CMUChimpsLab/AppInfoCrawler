#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: shiftwidth=4 

__author__    = "Alexandru Nedelcu and Song Luan"

"""
Simple script for crawling the Android Marketplace.
See this article for details:

  http://bionicspirit.com/blog/2011/12/15/crawling-the-android-marketplace-155200-apps.html

Usage:

  python crawler.py path/to/destination.json_lines

Warnings:

  - Google may not allow this for long, you may get your IP blocked
  - this will eat several GB of your monthly allocated bandwidth
  - I ran this from a VPS in San Franscisco, with good bandwidth and
    it still took ~ 5 hours to complete
"""

"""
Song's comment
I use Alexandru code from github, but customize for my own purpose
"""

# we are using eventlet for concurrent requests by means of async I/O
# and greenthreads, see the sample at:
#   http://eventlet.net/doc/examples.html#recursive-web-crawler
import eventlet
import re
import urllib
import sys
from datetime import datetime

from eventlet.green import urllib2

# using PyQuery for querying retrieved HTML content using CSS3
# selectors (awesome!)
from pyquery import pyquery as pq


class AndroidMarketCrawler(object):
    """
    Our Marketplace crawler.

    Usage:
    
      for app in AndroidMarketCrawler(concurrency=10):
          # app is a dictionary with the values of a retrieved app
          print app['dev_name']
    """
    def __init__(self, concurrency=10):
        # a green pool is a pool of greenthreads - you're pushing
        # tasks to it and they get executed when eventlet's loop is
        # active
        self.pool = eventlet.GreenPool(concurrency)        
        # the queue receives URLs to visit
        self.queue = eventlet.Queue()
        # our root URL, the first to be fetched
        self.queue.put("https://play.google.com/store/apps")
        # after a fetch of an app is finished, results get pushed in
        # this queue
        self.results = eventlet.Queue()
        # we need to make sure we don't fetch the same URL more than
        # once, otherwise the script might never finish
        self.seen = set()
        # `seen_app_ids` cuts down on fetching apps that have been
        # fetched before; it is necessary in addition to `seen`
        self.seen_app_ids = set()
        # just a counter for statistics
        self.failed = 0
        self.cnt = 0

        # our opener
        self.browser = urllib2.build_opener()
        self.browser.addheaders.append(('Cookie', 'hlSession2=en'))

    def next(self):
        """
        Implements the iterator protocol for `AndroidMarketCrawler`
        (see usage example above)
        """

        # when there are results, then return them even though you've
        # got other things to do, too
        if not self.results.empty():
            return self.results.get()

        # as long as there are tasks scheduled in the queue, or as
        # long as there are active scripts running ...
        while not self.queue.empty() or self.pool.running() != 0:
            # gets a new URL from the queue, to be fetched. if the
            # queue is empty, then waits until it isn't (eventlet's
            # run-loop can continue processing during this wait)
            url = eventlet.with_timeout(2, self.queue.get, timeout_value='')

            # if we have a new URL, then we spawn another green thread for fetching the content
            if url:
                if url in self.seen: continue
                uid = self.get_id(url)
                if uid in self.seen_app_ids: continue
                self.seen.add(url)
                self.pool.spawn_n(self.fetch_content, url)


            #if self.cnt > 100:
            #    raise StopIteration
            # in case we have results waiting to be served, then
            # return
            if not self.results.empty():
                return self.results.get()

        raise StopIteration


    def fetch_content(self, url):
        """
        Fetches the content of an URL, gets app links from it and
        pushes them down the queue. Then parses the content to
        determine if it is an app and if it is, then push the parsed
        result in the `results` queue for later processing.

        This logic is getting executed inside green threads. You
        shouldn't spawn new green threads here, as this is not the
        parent and trouble may arise.
        """
        try:
            resp = self.browser.open(url)

        except urllib2.HTTPError, ex:
            # silently ignores errors, even though the script will not
            # block here.
            sys.stderr.write('1 ' + str(ex) + ': ' + url + '\n')
            if ex.code == 404: 
                return
            #503 error code means need input catcha, just try again
            if ex.code == 503:
                #sleep only blocks thread
                eventlet.sleep(1)
            try:
                resp = self.browser.open(url)

            except urllib2.HTTPError, ex:
                # silently ignores errors, even though the script will not
                # block here.
                sys.stderr.write('1_5 ' + str(ex) + ': ' + url + '\n')
                if ex.code == 404: 
                    return


            # this is a slight problem, it shouldn't happen but it
            # does sometimes, so keeping tracking is useful to see how
            # often it does happen
            self.failed += 1
            return

        except urllib2.URLError, ex:
            sys.stderr.write('2 ' + str(ex) + ': ' + url + '\n')
            self.failed += 1
            return

        try:
            content = resp.read()
            doc = pq.PyQuery(content)

            # we must do our best to ignore pages that are not
            # relevant (music, movies, other pages that don't have
            # links to apps in them)
            if not self.is_page_valid(url, doc):
                return         

            # I like keeping a log of URLs processed
            sys.stderr.write(url + "\n")

            # fetches links in this page, by regular expressions. 
            # we are interested in app links and publisher links.
            all_links = [
                a.attrib['href']
                for a in doc('a') 
                if re.search(r'\/(details|developer)[?]', a.attrib.get('href', '')) \
                and not re.search('reviewId', a.attrib.get('href', '')) \
                #and not re.search('accounts\/ServiceLogin', a.attrib.get('href', ''))
            ]

            # pushing new links down the queue for processing later
            for link in all_links:
                if not link: continue
                self.queue.put(self.absolute_url(link))

            # Currently we only need app id
            #https://play.google.com/store/apps/details?id=com.vegantaram.android.math.formulae.ultimatum.free
	    if url[35:42] == 'details': 
                app_id = self.get_id(url)
                if app_id:
                    # prevents going to already visited IDs
                    self.seen_app_ids.add(app_id)                
                    self.results.put(app_id)
                    self.cnt += 1
                    print self.cnt

        except Exception as ex:
            sys.stderr.write('3 '+ str(ex) + ':' + url + '\n')
            # we must ignore exceptions as sometimes we don't make the
            # best assumptions. Some fields may be missing, the page's
            # format can change slightly, etc... when I ran the script
            # the first time it froze halfway-through and had to start
            # all over again
            self.failed += 1
            pass

    def is_page_valid(self, url, doc):
        """
        This is a hackish method to determine if the visited page is
        useful at all.

        The big problem is that I cannot infer the type of item I've
        got just from the link. Links for audio, movies and apps have
        the same format.

        `doc` is therefore an instantiated PyQuery document with the
        fetched content.

        What this buys us is that we can then ignore links from
        invalid pages (as movies will tend to link to other movies,
        not to other apps).
        """
        # Currently the play store's url use "apps" to distinguish between apps and other categories 
        # Therefore now it is very easy to use "play.google.com/store/apps/" to select valid urls
        if url.startswith("https://play.google.com/store/apps"):
            return True
        else:
            return False

    def fetch_app_info(self, url, doc):
        """
        At this point, we are almost sure we have an app, so this
        method attempts parsing the content into a dictionary.

        We are using PyQuery and CSS3 selectors heavily.
        """
        params = self.query_vars(url)
        if not params.get('id'): return None
        if not doc('div.details-wrapper.apps.square-cover'): return None

        app_info = {
            'uid': params['id'],
            'name': doc('div.document-title').text(),
            'app_link': self.absolute_url('/details?id=' + params['id']),
            'dev_name': doc('a.doc-header-link').text(),
            'dev_link': self.absolute_url(doc('a.doc-header-link').attr['href']),
            'dev_web_links': list(set([
                self.query_vars(a.attrib['href'])['q'] 
                for a in doc('.doc-overview a') 
                if a.text and "Visit Developer's Website" in a.text
            ])),
            'dev_emails': list(set([
                a.attrib['href'][len('mailto:'):] 
                for a in doc('.doc-overview a') 
                if a.attrib.get('href', '').startswith('mailto:')
            ])),
            'rating_count': int(re.sub(r'\D+', '', doc('[itemprop=ratingCount]').text() or '0')),
            'rating_value': doc('[itemprop=ratingValue]').attr['content'],
            #TODO: Why detialed_rating, operating_systems, datepublished, content_rating
            'description_html': doc('#doc-original-text').html(),
            'users_also_installed': [
                self.query_vars(a.attrib['href'])['id'] 
                for a in doc('[data-analyticsid=users-also-installed] a.common-snippet-title')
            ],
            'users_also_viewed': [
                self.query_vars(a.attrib['href'])['id'] 
                for a in doc('[data-analyticsid=related] a.common-snippet-title')
            ],
            #Why 'permissions' is removed
        }

        match = re.findall(r'.*[\d\.]+', doc('.buy-button-price').text())
        if match:
            app_info['is_free'] = False
            app_info['price'] = match[0]
        else:
            app_info['is_free'] = True
            app_info['price'] = 0

        match = [a.text for a in doc('.doc-metadata-list dd a') if 'category' in a.attrib.get('href')]
        if match: app_info['category'] = match[0]
        
        match = re.findall('([\d,]+)\s*-\s*([\d,]+)', doc('[itemprop=numDownloads]').text() or '')
        if match:
            imin, imax = [re.sub(r'\D+', '', m) for m in match[0]]
            app_info['installs_min'] = int(imin)
            app_info['installs_max'] = int(imax)

        return app_info

    def get_id(self, url):
        """
        Extracts the ID param from a Marketplace URL.
        """
        params = self.query_vars(url)
        return params.get('id')

    def query_vars(self, url):
        """
        Parses the query part of an URL. It was faster to implement
        this myself, than to find something already available.
        """
        v = {}
        match = re.findall('[^?]+[?](.*)$', url)

        if match:
            query = match[0]
            parts = query.split('&')
            for part in parts:
                keyval = [urllib.unquote_plus(i) for i in part.split('=', 1)]
                key, val = keyval if len(keyval) == 2 else (keyval[0], '')
                v[key] = val

        return v

    def absolute_url(self, url):
        """
        Converts relative URL to a Marketplace absolute URL.
        """
        if url and url.startswith('/'):
            return "https://play.google.com" + url
        return url or ''

    def __iter__(self):
        return self



if __name__ == '__main__':
        
    nowString = datetime.now().strftime("%Y%m%d%H%M")
    fstderr = open('exception/exception_' + nowString, 'w')
    fstdout = open('app/app_' + nowString, 'w')
    sys.stderr = fstderr
    fstdout.write( str(datetime.utcnow())+ "UTC\n") 
    sys.stderr.write( str(datetime.utcnow())+ "UTC\n") 
   
    # we are dumping JSON objects, one on each line (this file will be
    # huge, so it's a bad idea to serialize the whole thing as an
    # array)
    # The huge file is broken into multiple smaller files  
    #the results are not set, so there is some chance duplicated app fetched. The reason is set is not sync between different thread
    for app in AndroidMarketCrawler(concurrency=10):
        fstdout.write(app + "\n")
        fstdout.flush()
                count += 1
        if(count == 250):
                fstdout.close()
                fstderr.close()

                nowString = datetime.now().strftime("%Y%m%d%H%M")
                fstderr = open('/home/vrudresh/Privacy/AppInfoCrawler/exception/exception_' + nowString, 'w')
                fstdout = open('/home/vrudresh/Privacy/AppInfoCrawler/app/app_' + nowString, 'w')
                sys.stderr = fstderr

                count = 0
