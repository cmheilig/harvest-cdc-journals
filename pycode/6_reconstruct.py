#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 20 08:13:23 2024

@author: cmheilig
"""

#%% Set up environment
import os
import json
import zipfile
from pandas.api.types import CategoricalDtype
import pandas as pd
from tqdm import tqdm

os.chdir('/Users/cmheilig/cdc-corpora/_test')

#%% Reconstruct metadata for mirror and for corpus

## Information from zip archives of HTML mirrors
# mirror_df = pd.read_csv('cdc_mirror_list.csv')
# Series,Length,Method,Size,Cmpr,Date    Time,CRC-32,Name
# ['Series', 'Length', 'Method', 'Size', 'Cmpr', 'Date    Time', 'CRC-32', 'Name']

mirror_df = pd.read_csv(
    'cdc_mirror_list.csv',
    header=0, 
    names=['series', 'file_size', 'compress_type', 
           'compress_size', 'compress_pct', 'date_time', 'CRC', 
           'filename'],
    parse_dates=['date_time'], 
    dtype={'series': 'category', 'file_size': 'int', 'compress_type': 'category', 
           'compress_size': 'int', 'compress_pct': 'str', 'CRC': 'string', 
           'filename': 'string'})
mirror_df['compress_pct'] = pd.to_numeric(mirror_df.compress_pct.str[:-1])

## Information about processed corpus collections
series_cat = CategoricalDtype(
    ['mmwr', 'mmnd', 'mmrr', 'mmss', 'mmsu', 'eid', 'eid0', 'eid1', 'eid2',
     'pcd'], ordered=True)
level_cat = CategoricalDtype(
    ['home', 'series', 'volume', 'issue', 'article'], ordered=True)
stratum_cat = CategoricalDtype(
    ['mmwr_toc', 'mmwr_art', 'eid_toc', 'eid_art', 'pcd_toc', 'pcd_art'], 
    ordered=True)
collection_cat = CategoricalDtype(
    ['mmwr_toc_en', 'mmrr_toc_en', 'mmss_toc_en', 'mmsu_toc_en', 
     'mmwr_art_en', 'mmrr_art_en', 'mmss_art_en', 'mmsu_art_en', 
     'mmnd_art_en', 'mmwr_art_es', 
     'eid_toc_en', 'eid0_art_en', 'eid1_art_en', 'eid2_art_en', 
     'pcd_toc_en', 'pcd_toc_es', 'pcd_art_en', 'pcd_art_es', 
     'pcd_art_fr', 'pcd_art_zhs', 'pcd_art_zht'], ordered=True)

# pd.read_csv('cdc_corpus_df.csv').columns
# ['url', 'stratum', 'collection', 'series', 'level', 'lang', 'dl_year_mo', 'dl_vol_iss', 'dl_date', 'dl_page', 'dl_art_num', 'dateline', 'base', 'string', 'link_canon', 'mirror_path', 'md_citation_doi', 'title', 'md_citation_categories', 'dl_cat', 'md_kwds', 'md_desc', 'md_citation_author']

corpus_df = pd.read_csv(
    'cdc_corpus_df.csv',
    # parse_dates=['datetime'], 
    dtype={'url': 'string', 
           'stratum': stratum_cat, 'collection': collection_cat, 
           'series': series_cat, 'level': level_cat, 
           'lang': 'category', 'dl_year_mo': 'category', 
           'dl_vol_iss': 'category', 'dl_date': 'string', 'dl_page': 'string', 
           'dl_art_num': 'string', 'dateline': 'string', 'base': 'string', 
           'string': 'string', 'link_canon': 'string', 'mirror_path': 'string', 
           'md_citation_doi': 'string', 'title': 'string', 
           'md_citation_categories': 'string', 'dl_cat': 'string', 
           'md_kwds': 'string', 'md_desc': 'string', 
           'md_citation_author': 'string'})
# pd.to_datetime(corpus_df.dl_date, format='ISO8601'): YYYY-MM -> YYYY-MM-01


#%% Reconstruct contents extracted from corpus

# Location of zipped JSON collections

json_out_dir = '/Users/cmheilig/Documents/GitHub/harvest-cdc-journals/json-outputs/'

# Sample code to interrogate zip archive
# with zipfile.ZipFile(json_out_dir + 'txt/mmsu_toc_en_txt_json.zip', 'r') as json_zip:
#     json_zip.printdir()
#     print(json_zip.namelist())
#     print(json_zip.infolist())
#     print(json_zip.getinfo('mmsu_toc_en_txt.json'))
#     mmsu_toc_en_txt_json = json_zip.read('mmsu_toc_en_txt.json').decode(encoding='utf-8')

# with zipfile.ZipFile(json_out_dir + 'txt/mmsu_toc_en_txt_json.zip', 'r') as json_zip:
#     mmsu_toc_en_txt_dict = json.loads(json_zip.read('mmsu_toc_en_txt.json').decode(encoding='utf-8'))

# Construct complete contents dictionsaries from zip archives
html_from_json = dict()
md_from_json = dict()
txt_from_json = dict()

for fmt in ['html', 'md', 'txt']:
    for clxn in tqdm(collection_cat.categories):
        with zipfile.ZipFile(
                f'{json_out_dir}{fmt}/{clxn}_{fmt}_json.zip', 'r') as json_zip:
            dict_from_json = json.loads(json_zip.read(f'{clxn}_{fmt}.json')
                       .decode(encoding='utf-8'))
            eval(f'{fmt}_from_json').update(dict_from_json)
# 21/21 [00:19<00:00,  1.09it/s]
# 21/21 [00:08<00:00,  2.62it/s]
# 21/21 [00:04<00:00,  4.66it/s]
del fmt, clxn, json_zip, dict_from_json

# check mutual consistency
len(html_from_json) # 33567
len(md_from_json)   # 33567
len(txt_from_json)  # 33567
list(html_from_json) == list(md_from_json)      # True
list(html_from_json) == list(txt_from_json)     # True
list(html_from_json) == corpus_df.url.to_list() # True
