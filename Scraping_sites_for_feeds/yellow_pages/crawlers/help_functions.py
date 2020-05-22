'''common library'''
import re
import datetime as dt
import json
import pandas as pd
import numpy as np
import logging
import os
import tldextract
from urllib.request import urlopen
from urllib.parse import urlencode, urlparse
import urllib
import requests
from bs4 import BeautifulSoup
import concurrent
import concurrent.futures

logging.basicConfig(level=logging.INFO,
                    format='%(name)-12s %(levelname)-8s %(message)s',
                    filename='scrapError.log',
                    filemode='w')

'''common function'''
def normDom(url):
    '''This function get domain from url
    Parameters: url - str object for convert
    return: domain'''
    dom = tldextract.extract(url)
    dom = re.sub('www.', '', '.'.join(dom))
    return dom.strip('.')

def makebs(url):
    '''This function create BeautifullSoup object from url
    Parameters: url - string object like "https://pep8.ru/doc/pep8/"
    return: BeautifullSoup object'''
    try:
        html = urlopen(url)
    except:
        url = urllib.parse.urlsplit(url)
        url = list(url)
        url = [urllib.parse.quote(u) for u in url]
        url = urllib.parse.urlunsplit(url)
        try:
            html = urlopen(url)
        except:
            log.error('URL {} could not open'.format(url))
            return
    return BeautifulSoup(html, features="lxml")

def make_url(base_url, href):
    '''This function make new url from base_url and href
    Parameters: base_url - start url
                href - dynamic url or part of url
    return: new url for parsing'''
    if href.startswith('http://') or href.startswith('https://'):
        return href
    if not href.startswith('/'):
        return base_url + '/' + href
    base_url = base_url.split('/')
    base_url = base_url[0] + '//' + base_url[2]
    return base_url + href

