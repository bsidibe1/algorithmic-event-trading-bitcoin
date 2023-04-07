import pandas as pd
import numpy as np
import re
import requests
from zipfile import ZipFile
from pathlib import Path
from io import BytesIO

from bs4 import BeautifulSoup
import sys
import os
import logging
from tqdm.auto import tqdm


logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger(__name__)

#Charger les dates d'annonces 

def bs_cleaner(bs, html_tag_blocked=None): 
    if html_tag_blocked is None: 
        html_tag_blocked = ['style', 'script', '[document]',  'meta',  'a',  'span',  'label', 'strong', 'button', 
                     'li', 'h6',  'font', 'h1', 'h2',  'h3', 'h5', 'h4',  'em', 'body', 'head']
    return [sent_cleaner(t) for t in bs.find_all(text=True) 
            if (t.parent.name not in html_tag_blocked)&(len(sent_cleaner(t))>0)]

def sent_cleaner(s): 
    return s.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ').strip() 

def get_fomc_urls(from_year = 1999, switch_year=2017): 
    calendar_url = 'https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm'
    r = requests.get(calendar_url)
    soup = BeautifulSoup(r.text, 'html.parser')
    contents = soup.find_all('a', href=re.compile('^/newsevents/pressreleases/monetary\d{8}[ax].htm'))
    urls_ = [content.attrs['href'] for content in contents]

    for year in range(from_year, switch_year):
        yearly_contents = []
        fomc_yearly_url = f'https://www.federalreserve.gov/monetarypolicy/fomchistorical{year}.htm'
        r_year = requests.get(fomc_yearly_url)
        soup_yearly = BeautifulSoup(r_year.text, 'html.parser')
        yearly_contents = soup_yearly.findAll('a', text='Statement')
        for yearly_content in yearly_contents:
            urls_.append(yearly_content.attrs['href'])
    
    urls = ['https://www.federalreserve.gov' + url for url in urls_]
    return urls 

regexp = re.compile(r'\s+', re.UNICODE)

def feature_extraction(corpus, sent_filters=None):
    if sent_filters is None: 
        sent_filters = ['Board of Governors', 'Federal Reserve System',
                         '20th Street and Constitution Avenue N.W., Washington, DC 20551',
                         'Federal Reserve Board - Federal Reserve issues FOMC statement',
                         'For immediate release',
                         'Federal Reserve Board - FOMC statement',
                         'DO NOT REMOVE:  Wireless Generation',
                         'For media inquiries', 
                         'or call 202-452-2955.',
                         'Voting', 
                         'For release at', 
                         'For immediate release', 
                        'Last Update', 
                        'Last update'
                        ]

    text = [' '.join([regexp.sub(' ', s) for i, s in enumerate(c) 
                     if (i>1) & np.all([q not in s for q in sent_filters])])
            for c in corpus]
    
    release_date = [pd.to_datetime(c[1].replace('Release Date: ', '')) for c in corpus]
    last_update = [pd.to_datetime([s.replace('Last update:', '').replace('Last Update:', '').strip() 
                   for s in c if 'last update: ' in s.lower()][0]) for c in corpus]
    voting = [' '.join([s for s in c if 'Voting' in s]) for c in corpus]
    release_time = [' '.join([s for s in c if ('For release at' in s)|('For immediate release' in s)]) for c in corpus]
    
    return pd.DataFrame({'release_date': release_date, 
                         'last_update':  last_update, 
                         'text': text, 
                         'voting': voting, 
                         'release_time': release_time})


def load_fomc_statements(add_url=True, cache_dir=None, force_reload=False, 
                        progress_bar=True, from_year=1999):
    if cache_dir is None:
        cache_dir = Path(os.getcwd()) / "data"
    if isinstance(cache_dir, str):
        cache_dir = Path(cache_dir)
        
    filename = cache_dir / 'fomc_statements.parquet'
    if (filename.exists()) & (~force_reload):
        logger.info(f"logging from cache file: {filename}")
        statements = pd.read_parquet(filename)
    else: 
        logger.info("loading from external source")
        urls = get_fomc_urls(from_year = from_year)
        if progress_bar: 
            urls_ = tqdm(urls)
        else: 
            urls_ = urls 
        corpus = [bs_cleaner(BeautifulSoup(requests.get(url).text, 'html.parser')) for url in urls_]
        statements = feature_extraction(corpus).set_index('release_date')
        if add_url: 
            statements = statements.assign(url=urls)
        statements= statements.sort_index()
        logger.info(f"saving cache file {filename}")
        #statements.to_parquet(filename)
    return statements