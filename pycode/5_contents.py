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

# mmwr_cc_df = pickle.load(open("pickle-files/mmwr_cc_df.pkl", "rb"))
mmwr_cc_df = pd.read_pickle('pickle-files/mmwr_cc_df.pkl')
# eid_cc_df = pickle.load(open("pickle-files/eid_cc_df.pkl", "rb"))
eid_cc_df = pd.read_pickle('pickle-files/eid_cc_df.pkl')
# pcd_cc_df = pickle.load(open("pickle-files/pcd_cc_df.pkl", "rb"))
pcd_cc_df = pd.read_pickle('pickle-files/pcd_cc_df.pkl')

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
# 135/135 [00:01<00:00, 118.13it/s]
mmwr_art_head_soup = {
    path: BeautifulSoup(html, 'lxml', parse_only=only_head, multi_valued_attributes=None)
    for path, html in tqdm(mmwr_art_unx.items())}
# 15161/15161 [04:18<00:00, 58.57it/s]
eid_toc_head_soup = {
    path: BeautifulSoup(html, 'lxml', parse_only=only_head, multi_valued_attributes=None)
    for path, html in tqdm(eid_toc_unx.items())}
# 330/330 [00:08<00:00, 38.77it/s]
eid_art_head_soup = {
    path: BeautifulSoup(html, 'lxml', parse_only=only_head, multi_valued_attributes=None)
    for path, html in tqdm(eid_art_unx.items())}
# 12769/12769 [03:30<00:00, 60.58it/s]
pcd_toc_head_soup = {
    path: BeautifulSoup(html, 'lxml', parse_only=only_head, multi_valued_attributes=None)
    for path, html in tqdm(pcd_toc_unx.items())}
# 87/87 [00:00<00:00, 115.94it/s]
pcd_art_head_soup = {
    path: BeautifulSoup(html, 'lxml', parse_only=only_head, multi_valued_attributes=None)
    for path, html in tqdm(pcd_art_unx.items())}
# 5091/5091 [00:39<00:00, 128.04it/s]


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
        .groupby(['stratum', 'collection'])):
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
    print(f'\nEnd stratum: {stratum:<8}; collection: {collection}\n')
print_lapse(start_time)


#%%
# Begin stratum: mmwr_toc; collection: mmwr_toc_en
# 42/42 [00:01<00:00, 21.19it/s]
# 42/42 [00:00<00:00, 75.26it/s] 
# End stratum: mmwr_toc; collection: mmwr_toc_en

# Begin stratum: mmwr_toc; collection: mmrr_toc_en
# 34/34 [00:00<00:00, 48.62it/s]
# 34/34 [00:00<00:00, 250.48it/s]
# End stratum: mmwr_toc; collection: mmrr_toc_en

# Begin stratum: mmwr_toc; collection: mmss_toc_en
# 36/36 [00:00<00:00, 50.40it/s]
# 36/36 [00:00<00:00, 274.50it/s]
# End stratum: mmwr_toc; collection: mmss_toc_en

# Begin stratum: mmwr_toc; collection: mmsu_toc_en
# 19/19 [00:00<00:00, 58.23it/s]
# 19/19 [00:00<00:00, 278.10it/s]
# End stratum: mmwr_toc; collection: mmsu_toc_en

# Begin stratum: mmwr_art; collection: mmwr_art_en
# 12692/12692 [03:45<00:00, 56.28it/s]
# 12692/12692 [01:02<00:00, 204.09it/s]
# End stratum: mmwr_art; collection: mmwr_art_en

# Begin stratum: mmwr_art; collection: mmrr_art_en
# 551/551 [00:16<00:00, 32.87it/s]
# 551/551 [00:08<00:00, 63.74it/s] 
# End stratum: mmwr_art; collection: mmrr_art_en

# Begin stratum: mmwr_art; collection: mmss_art_en
# 467/467 [02:01<00:00,  3.84it/s]
# 467/467 [00:34<00:00, 13.46it/s] 
# End stratum: mmwr_art; collection: mmss_art_en

# Begin stratum: mmwr_art; collection: mmsu_art_en
# 234/234 [00:08<00:00, 27.95it/s]
# 234/234 [00:03<00:00, 75.72it/s]
# End stratum: mmwr_art; collection: mmsu_art_en

# Begin stratum: mmwr_art; collection: mmnd_art_en
# 1195/1195 [10:39<00:00,  1.87it/s]
# 1195/1195 [03:11<00:00,  6.25it/s]
# End stratum: mmwr_art; collection: mmnd_art_en

# Begin stratum: mmwr_art; collection: mmwr_art_es
# 22/22 [00:00<00:00, 45.17it/s]
# 22/22 [00:00<00:00, 82.77it/s]
# End stratum: mmwr_art; collection: mmwr_art_es

# Begin stratum: eid_toc ; collection: eid_toc_en
# 330/330 [00:24<00:00, 13.67it/s]
# 330/330 [00:06<00:00, 54.63it/s]
# End stratum: eid_toc ; collection: eid_toc_en

# Begin stratum: eid_art ; collection: eid0_art_en
# 3919/3919 [04:06<00:00, 15.88it/s] 
# 3919/3919 [00:30<00:00, 127.61it/s]
# End stratum: eid_art ; collection: eid0_art_en

# Begin stratum: eid_art ; collection: eid1_art_en
# 4439/4439 [04:12<00:00, 17.60it/s]  
# 4439/4439 [00:34<00:00, 127.72it/s]
# End stratum: eid_art ; collection: eid1_art_en

# Begin stratum: eid_art ; collection: eid2_art_en
# 4411/4411 [04:07<00:00, 17.84it/s]  
# 4411/4411 [00:36<00:00, 119.62it/s]
# End stratum: eid_art ; collection: eid2_art_en

# Begin stratum: pcd_toc ; collection: pcd_toc_en
# 49/49 [00:01<00:00, 27.81it/s]
# 49/49 [00:00<00:00, 69.80it/s]
# End stratum: pcd_toc ; collection: pcd_toc_en

# Begin stratum: pcd_toc ; collection: pcd_toc_es
# 36/36 [00:00<00:00, 53.46it/s]
# 36/36 [00:00<00:00, 111.83it/s]
# End stratum: pcd_toc ; collection: pcd_toc_es

# Begin stratum: pcd_art ; collection: pcd_art_en
# 3011/3011 [02:19<00:00, 21.53it/s]  
# 3011/3011 [00:29<00:00, 103.06it/s]
# End stratum: pcd_art ; collection: pcd_art_en

# Begin stratum: pcd_art ; collection: pcd_art_es
# 1011/1011 [00:09<00:00, 104.39it/s]
# 1011/1011 [00:03<00:00, 308.30it/s]
# End stratum: pcd_art ; collection: pcd_art_es

# Begin stratum: pcd_art ; collection: pcd_art_fr
# 357/357 [00:03<00:00, 115.87it/s]
# 357/357 [00:01<00:00, 316.38it/s]
# End stratum: pcd_art ; collection: pcd_art_fr

# Begin stratum: pcd_art ; collection: pcd_art_zhs
# 356/356 [00:02<00:00, 120.36it/s]
# 356/356 [00:01<00:00, 342.54it/s]
# End stratum: pcd_art ; collection: pcd_art_zhs

# Begin stratum: pcd_art ; collection: pcd_art_zht
# 356/356 [00:02<00:00, 120.73it/s]
# 356/356 [00:01<00:00, 342.44it/s]
# End stratum: pcd_art ; collection: pcd_art_zht

# Time elapsed: 41:14.78

