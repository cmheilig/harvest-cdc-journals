#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: cmheilig

Extract organize content from MMWR, EID, and PCD
Corpora shared publicly in JSON format

Corpora in JSON, keyed to path:
    mm{wr, rr, ss, su}_{toc, art_en}, mmwr_{toc, art}_es
    pcd_{toc, art_{en, es, fr, zhs, zht}}
    eid_{toc, art_en}

"""

#%% Set up environment
import pickle
import json
from collections import Counter, defaultdict
from bs4 import SoupStrainer
from dateutil.parser import parse as parse_date
import markdownify as md

only_head = SoupStrainer(name='head')
only_body = SoupStrainer(name='body')

os.chdir('/Users/cmheilig/cdc-corpora/_test')

#%% Retrieve (unpickle) trimmed, UTF-8 HTML 

cdc_corpus_df = pd.read_pickle('pickle-files/cdc_corpus_df.pkl')
# ['url', 'stratum', 'collection', 'series', 'level', 'lang', 
#  'dl_year_mo', 'dl_vol_iss', 'dl_date', 'dl_page', 'dl_art_num', 'dateline', 
#  'base', 'string', 'link_canon', 'mirror_path', 'md_citation_doi', 'title', 
#  'md_citation_categories', 'dl_cat', 'md_kwds', 'md_desc', 'md_citation_author']

# mmwr_cc_df = pickle.load(open('pickle-files/mmwr_cc_df.pkl', 'rb')) # (15297, 8)
mmwr_cc_df = pd.read_pickle('pickle-files/mmwr_cc_df.pkl')
mmwr_cc_df['mirror_path'] = mmwr_cc_df['mirror_path'].str.replace('\\', '/')

# eid_cc_df = pickle.load(open("pickle-files/eid_cc_df.pkl", "rb")) # (13100, 8)
eid_cc_df = pd.read_pickle('pickle-files/eid_cc_df.pkl')
eid_cc_df['mirror_path'] = eid_cc_df['mirror_path'].str.replace('\\', '/')

# pcd_cc_df = pickle.load(open("pickle-files/pcd_cc_df.pkl", "rb")) # (4786, 8)
pcd_cc_df = pd.read_pickle('pickle-files/pcd_cc_df.pkl')
pcd_cc_df['mirror_path'] = pcd_cc_df['mirror_path'].str.replace('\\', '/')

mmwr_toc_unx = pickle.load(open('pickle-files/mmwr_toc_unx.pkl', 'rb'))
mmwr_art_unx = pickle.load(open('pickle-files/mmwr_art_unx.pkl', 'rb'))
eid_toc_unx = pickle.load(open('pickle-files/eid_toc_unx.pkl', 'rb'))
eid_art_unx = pickle.load(open('pickle-files/eid_art_unx.pkl', 'rb'))
pcd_toc_unx = pickle.load(open('pickle-files/pcd_toc_unx.pkl', 'rb'))
pcd_art_unx = pickle.load(open('pickle-files/pcd_art_unx.pkl', 'rb'))

#%% Parse HTML <head> elements

mmwr_toc_head_soup = {
    path: BeautifulSoup(html, 'lxml', parse_only=only_head, multi_valued_attributes=None)
    for path, html in tqdm(mmwr_toc_unx.items())}
# 139/139 [00:00<00:00, 319.09it/s]
mmwr_art_head_soup = {
    path: BeautifulSoup(html, 'lxml', parse_only=only_head, multi_valued_attributes=None)
    for path, html in tqdm(mmwr_art_unx.items())}
# 15435/15435 [01:44<00:00, 148.18it/s]
eid_toc_head_soup = {
    path: BeautifulSoup(html, 'lxml', parse_only=only_head, multi_valued_attributes=None)
    for path, html in tqdm(eid_toc_unx.items())}
# 345/345 [00:03<00:00, 109.61it/s]
eid_art_head_soup = {
    path: BeautifulSoup(html, 'lxml', parse_only=only_head, multi_valued_attributes=None)
    for path, html in tqdm(eid_art_unx.items())}
# 13310/13310 [01:21<00:00, 163.47it/s]
pcd_toc_head_soup = {
    path: BeautifulSoup(html, 'lxml', parse_only=only_head, multi_valued_attributes=None)
    for path, html in tqdm(pcd_toc_unx.items())}
# 88/88 [00:00<00:00, 252.88it/s]
pcd_art_head_soup = {
    path: BeautifulSoup(html, 'lxml', parse_only=only_head, multi_valued_attributes=None)
    for path, html in tqdm(pcd_art_unx.items())}
# 5193/5193 [00:15<00:00, 331.12it/s]


#%% Bundle and export to JSON: HTML, plain-text, and markdown

import time

def print_lapse(start_time):
  print(f"Time elapsed: {int((time.time() - start_time) // 60):02d}:{round((time.time() - start_time) % 60, 2):05.2f}")
  return None

# extract contents from soup
# remove <head>, <script>, <noscript>, <form>, <svg>, <style>, <template>
elim_elem_list = ['svg', 'script', 'noscript', 'form', 'template']
def content_soup_fn(html, elim_elems=None):
    soup = BeautifulSoup(html, 'lxml', parse_only=only_body)
    if elim_elems:
        found_elems = soup.find_all(elim_elems)
        for tag in found_elems:
            tag.decompose()
    return soup

# convert to markdown with consistent options
md_opts = dict(autolinks=False, heading_style=md.ATX_CLOSED,
               strong_em_symbol=md.UNDERSCORE, sub_symbol='~', sup_symbol='^',
               strip=['script', 'style'])
def markdown_soup_fn(soup, md_opts=None):
    if md_opts:
        return md.MarkdownConverter(**md_opts).convert_soup(soup)
    else:
        return md.MarkdownConverter().convert_soup(soup)

start_time = time.time()
for (stratum, collection), coll_gp in \
    (cdc_corpus_df[['url', 'mirror_path', 'stratum', 'collection']]
        .groupby(['stratum', 'collection'], observed=True)):
    print(f'Begin stratum: {stratum:<8}; collection: {collection}')
    # bundle UTF-8 HTML contents in a dict
    html_dict = {
        row.url: eval(f'{stratum}_unx').get(row.mirror_path)
        for row in coll_gp.itertuples(index=False, name='Doc')}
    # write UTF-8 HTML contents to JSON
    json.dump(html_dict, open(f'{collection}_html.json', 'x'), indent=1)
    # parse HTML to support extracting plain-text and markdown
    soup_dict = {
        url: content_soup_fn(html, elim_elems=elim_elem_list)
        for url, html in tqdm(html_dict.items())}
    # bundle UTF-8 plain-text contents in a dict
    txt_dict = {
        url: re.sub('\s{2,}', ' ', soup.get_text().strip())
        for url, soup in (soup_dict.items())}
    # write ASCII plain-text contents to JSON
    json.dump(txt_dict, open(f'{collection}_txt.json', 'x'), indent=1)
    md_dict = {
        url: markdown_soup_fn(soup, md_opts=md_opts)
        for url, soup in tqdm(soup_dict.items())}
    # write UTF-8 markdown contents to JSON
    json.dump(md_dict, open(f'{collection}_md.json', 'x'), indent=1, ensure_ascii=False)
    print(f'End stratum: {stratum:<8}; collection: {collection}\n')
print_lapse(start_time)

#%%
# Begin stratum: mmwr_toc; collection: mmwr_toc_en
# 43/43 [00:00<00:00, 74.79it/s] 
# 43/43 [00:00<00:00, 71.11it/s] 
# End stratum: mmwr_toc; collection: mmwr_toc_en

# Begin stratum: mmwr_toc; collection: mmrr_toc_en
# 35/35 [00:00<00:00, 181.13it/s]
# 35/35 [00:00<00:00, 240.26it/s]
# End stratum: mmwr_toc; collection: mmrr_toc_en

# Begin stratum: mmwr_toc; collection: mmss_toc_en
# 37/37 [00:00<00:00, 188.01it/s]
# 37/37 [00:00<00:00, 253.89it/s]
# End stratum: mmwr_toc; collection: mmss_toc_en

# Begin stratum: mmwr_toc; collection: mmsu_toc_en
# 20/20 [00:00<00:00, 193.81it/s]
# 20/20 [00:00<00:00, 278.05it/s]
# End stratum: mmwr_toc; collection: mmsu_toc_en

# Begin stratum: mmwr_art; collection: mmwr_art_en
# 12928/12928 [01:46<00:00, 121.95it/s]
# 12928/12928 [01:04<00:00, 199.08it/s]
# End stratum: mmwr_art; collection: mmwr_art_en

# Begin stratum: mmwr_art; collection: mmrr_art_en
# 557/557 [00:05<00:00, 104.21it/s]
# 557/557 [00:08<00:00, 66.65it/s] 
# End stratum: mmwr_art; collection: mmrr_art_en

# Begin stratum: mmwr_art; collection: mmss_art_en
# 476/476 [01:17<00:00,  6.17it/s] 
# 476/476 [00:31<00:00, 15.03it/s] 
# End stratum: mmwr_art; collection: mmss_art_en

# Begin stratum: mmwr_art; collection: mmsu_art_en
# 256/256 [00:03<00:00, 80.29it/s] 
# 256/256 [00:03<00:00, 75.73it/s]
# End stratum: mmwr_art; collection: mmsu_art_en

# Begin stratum: mmwr_art; collection: mmnd_art_en
# 1195/1195 [06:34<00:00,  3.03it/s]
# 1195/1195 [02:47<00:00,  7.14it/s]
# End stratum: mmwr_art; collection: mmnd_art_en

# Begin stratum: mmwr_art; collection: mmwr_art_es
# 23/23 [00:00<00:00, 133.75it/s]
# 23/23 [00:00<00:00, 106.04it/s]
# End stratum: mmwr_art; collection: mmwr_art_es

# Begin stratum: eid_toc ; collection: eid_toc_en
# 345/345 [00:08<00:00, 39.79it/s]
# 345/345 [00:06<00:00, 54.73it/s]
# End stratum: eid_toc ; collection: eid_toc_en

# Begin stratum: eid_art ; collection: eid0_art_en
# 3402/3402 [03:00<00:00, 18.86it/s]  
# 3402/3402 [00:36<00:00, 92.88it/s] 
# End stratum: eid_art ; collection: eid0_art_en

# Begin stratum: eid_art ; collection: eid1_art_en
# 3360/3360 [01:19<00:00, 42.35it/s]
# 3360/3360 [00:36<00:00, 92.49it/s] 
# End stratum: eid_art ; collection: eid1_art_en

# Begin stratum: eid_art ; collection: eid2_art_en
# 3243/3243 [01:50<00:00, 29.33it/s] 
# 3243/3243 [00:36<00:00, 87.97it/s] 
# End stratum: eid_art ; collection: eid2_art_en

# Begin stratum: eid_art ; collection: eid3_art_en
# 3305/3305 [01:42<00:00, 32.38it/s]
# 3305/3305 [00:40<00:00, 81.94it/s] 
# End stratum: eid_art ; collection: eid3_art_en

# Begin stratum: pcd_toc ; collection: pcd_toc_en
# 50/50 [00:00<00:00, 85.18it/s] 
# 50/50 [00:00<00:00, 90.92it/s] 
# End stratum: pcd_toc ; collection: pcd_toc_en

# Begin stratum: pcd_toc ; collection: pcd_toc_es
# 36/36 [00:00<00:00, 157.19it/s]
# 36/36 [00:00<00:00, 161.98it/s]
# End stratum: pcd_toc ; collection: pcd_toc_es

# Begin stratum: pcd_art ; collection: pcd_art_en
# 3113/3113 [01:18<00:00, 39.58it/s] 
# 3113/3113 [00:30<00:00, 101.94it/s]
# End stratum: pcd_art ; collection: pcd_art_en

# Begin stratum: pcd_art ; collection: pcd_art_es
# 1011/1011 [00:03<00:00, 299.23it/s]
# 1011/1011 [00:02<00:00, 375.00it/s]
# End stratum: pcd_art ; collection: pcd_art_es

# Begin stratum: pcd_art ; collection: pcd_art_fr
# 357/357 [00:11<00:00, 30.32it/s] 
# 357/357 [00:00<00:00, 456.22it/s]
# End stratum: pcd_art ; collection: pcd_art_fr

# Begin stratum: pcd_art ; collection: pcd_art_zhs
# 356/356 [00:01<00:00, 346.60it/s]
# 356/356 [00:00<00:00, 497.81it/s]
# End stratum: pcd_art ; collection: pcd_art_zhs

# Begin stratum: pcd_art ; collection: pcd_art_zht
# 356/356 [00:01<00:00, 345.64it/s]
# 356/356 [00:00<00:00, 500.44it/s]
# End stratum: pcd_art ; collection: pcd_art_zht

# Time elapsed: 27:49.43
