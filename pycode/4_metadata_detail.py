#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: cmheilig

Inventory metadata elements in <head> elements of HTML documents

"""

#%% Set up environment
import pickle
# import json
from collections import Counter, defaultdict
from bs4 import SoupStrainer
only_head = SoupStrainer(name='head')

os.chdir('/Users/cmheilig/cdc-corpora/_test')

#%% Retrieve (unpickle) trimmed, UTF-8 HTML
# mmwr_cc_df = pickle.load(open("pickle-files/mmwr_cc_df.pkl", "rb")) # 
# eid_cc_df = pickle.load(open("pickle-files/eid_cc_df.pkl", "rb")) # 
# eid_cc_df = pickle.load(open("pickle-files/eid_cc_df.pkl", "rb")) # 

mmwr_toc_unx = pickle.load(open('pickle-files/mmwr_toc_unx.pkl', 'rb'))
mmwr_art_unx = pickle.load(open('pickle-files/mmwr_art_unx.pkl', 'rb'))
eid_toc_unx = pickle.load(open('pickle-files/eid_toc_unx.pkl', 'rb'))
eid_art_unx = pickle.load(open('pickle-files/eid_art_unx.pkl', 'rb'))
pcd_toc_unx = pickle.load(open('pickle-files/pcd_toc_unx.pkl', 'rb'))
pcd_art_unx = pickle.load(open('pickle-files/pcd_art_unx.pkl', 'rb'))

# mmwr_toc_dl_df = pd.read_pickle('mmwr_toc_dl_df.pkl')
# mmwr_art_dl_df = pd.read_pickle('mmwr_art_dl_df.pkl')
# eid_toc_dl_df = pd.read_pickle('eid_toc_dl_df.pkl')
# eid_art_dl_df = pd.read_pickle('eid_art_dl_df.pkl')
# pcd_toc_dl_df = pd.read_pickle('pcd_toc_dl_df.pkl')
# pcd_art_dl_df = pd.read_pickle('pcd_art_dl_df.pkl')

#%% Parse HTML <head> elements

mmwr_head_soup = {
    path: BeautifulSoup(html, 'lxml', parse_only=only_head, multi_valued_attributes=None)
    for path, html in tqdm((mmwr_toc_unx | mmwr_art_unx).items())}
# 15296/15296 [04:23<00:00, 58.14it/s] 
eid_head_soup = {
    path: BeautifulSoup(html, 'lxml', parse_only=only_head, multi_valued_attributes=None)
    for path, html in tqdm((eid_toc_unx | eid_art_unx).items())}
# 13099/13099 [03:43<00:00, 58.49it/s]
pcd_head_soup = {
    path: BeautifulSoup(html, 'lxml', parse_only=only_head, multi_valued_attributes=None)
    for path, html in tqdm((pcd_toc_unx | pcd_art_unx).items())}
# 5178/5178 [00:41<00:00, 126.17it/s]

#%%

## Information on which tags are present
def tag_info_fn(tag):
    tag_name = tag.name
    tag_str = '' if not tag.string else tag.string
    tag_attr = tag.attrs
    n_attr = len(tag_attr)
    return dict(tag_name=tag_name, tag_str=tag_str, n_attr=n_attr, tag_attr=tag_attr)

head_tag_list = [
    dict(ser=ser, path=path, seq=seq, **tag_info_fn(tag))
    for ser in ['mmwr', 'eid', 'pcd']
    for path, soup in tqdm(eval(f'{ser}_head_soup').items())
    for seq, tag in enumerate(soup.find_all(True))]
head_tag_df = pd.DataFrame(head_tag_list) # (1396907, 7)

head_tag_attr_list = [
     dict(ser=tag['ser'], path=tag['path'], seq=tag['seq'], tag_name=tag['tag_name'], 
          attr_names='|'.join(sorted(tag['tag_attr'].keys())))
     for tag in tqdm(head_tag_list)]# if tag.get('tag_name') in {'link', 'meta'}]
head_tag_attr_df = pd.DataFrame(head_tag_attr_list) # (326825, 4)
(head_tag_attr_df
 .value_counts(subset=['ser', 'tag_name', 'attr_names'], sort=False, dropna=False)
 .reset_index(name='freq')).to_excel('head_tag_attr_df.xlsx', freeze_panes=(1,0))

## Focus on details of <link> and <meta> name, property, content
def head_count_fn(soup):
   """Process selected metadata from HTML <head> element.
   Using SoupStrainer makes this even more efficent."""
   title = len(soup.find_all('title'))
   link_canon = len(soup.find_all('link', attrs={'rel': 'canonical'}))
   meta_names = ['name:' + tag.get('name') 
                 for tag in soup.find_all(name='meta', attrs={'name': True})
                 if tag.get('content') != '']
   count_names = {z: meta_names.count(z) for z in sorted(set(meta_names))}
   meta_props = ['prop:' + tag.get('property') 
                 for tag in soup.find_all(name='meta', attrs={'property': True})
                 if tag.get('content') != '']
   count_props = {z: meta_props.count(z) for z in sorted(set(meta_props))}
   return dict(title=title, link_canon=link_canon, 
               **count_names, **count_props)

head_count_list = [
    dict(ser=ser, path=path, **head_count_fn(soup))
    for ser in ['mmwr', 'eid', 'pcd']
    for path, soup in tqdm(eval(f'{ser}_head_soup').items())]
# pd.DataFrame(head_count_list).shape # (33573, 86)

def head_meta_fn(soup):
   """Process selected metadata from HTML <head> element.
   Using SoupStrainer makes this even more efficent."""
   title = [
       ('title', '' if not tag.get_text() else tag.get_text(strip=True))
       for tag in soup.find_all('title')]
   link_canon = [
       ('link:rel:canonical', '' if not tag.get_text() else tag.get_text(strip=True))
       for tag in soup.find_all('link', attrs={'rel': 'canonical'})]
   meta_name = [
       ('meta:name:' + tag.get('name'), tag.get('content'))
       for tag in soup.find_all(name='meta', attrs={'name': True})
       if tag.get('content') != '']
   meta_property = [
       ('meta:property:' + tag.get('property'), tag.get('content'))
       for tag in soup.find_all(name='meta', attrs={'property': True})
       if tag.get('content') != '']
   return dict(title=title, link_canon=link_canon, 
               meta_name=meta_name, meta_property=meta_property)

head_meta_list = [
    dict(ser=ser, path=path, **head_meta_fn(soup))
    for ser in ['mmwr', 'eid', 'pcd']
    for path, soup in tqdm(eval(f'{ser}_head_soup').items())]
pd.DataFrame(head_meta_list).shape # (33573, 6)

[item
 for item in head_meta_list
 if (len(item['title']) > 1)]
# 'path': '/mmwr/preview/mmwrhtml/ss4808a2.htm'
[item
 for item in head_meta_list
 if (len(item['link_canon']) > 1)]
# []

meta_names = [
    dict(path=item['path'], **Counter([name for name, value in item['meta_name']]))
    for item in head_meta_list] # 33573
meta_props = [
    dict(path=item['path'], **Counter([name for name, value in item['meta_property']]))
    for item in head_meta_list] # 33573
pd.DataFrame(meta_names).to_excel('meta_names.xlsx', freeze_panes=(1,0)) # (33573, 53)
pd.DataFrame(meta_props).to_excel('meta_props.xlsx', freeze_panes=(1,0)) # (33573, 31)

# nonunique
# title occurs twice
# meta:name:Generator occurs twice
# meta:name:citation_author occurs >1
[item
 for item in head_meta_list
 if (len([x for x, y in item['meta_name'] if x == 'meta:name:Generator']) > 1)]
# 'path': '/mmwr/preview/mmwrhtml/ss4808a2.htm'
[item
 for item in head_meta_list
 if (len([x for x, y in item['meta_name'] if x == 'meta:name:citation_author']) > 1)]
# 11365 items, all in eid

meta_name_prop_list = [
    dict(ser=item['ser'], path=item['path'], attr_name=name, attr_value=value)
    for item in head_meta_list
    for name, value in item['meta_name'] + item['meta_property']] # 808303
meta_name_prop_df = pd.DataFrame(meta_name_prop_list) # (808303, 4)
meta_name_val_freq = (meta_name_prop_df
 .value_counts(subset=['ser', 'attr_name', 'attr_value'], dropna=False)
 .reset_index(name='freq')) # (220805, 4)
meta_name_freq = (meta_name_prop_df
 .value_counts(subset=['ser', 'attr_name'], dropna=False)
 .reset_index(name='freq')) # (141, 3)

with pd.ExcelWriter('meta_name_prop_freqs.xlsx') as xlw:
    (meta_name_freq
     .pivot(index='attr_name', columns='ser', values='freq')
     .astype('Int64')
     .reset_index()
     .to_excel(xlw,sheet_name='name only', freeze_panes=(1,0)))
    (meta_name_val_freq
     .pivot(index=['attr_name', 'attr_value'], columns='ser', values='freq')
     .astype('Int64')
     .reset_index()
     .to_excel(xlw,sheet_name='name and attr', freeze_panes=(1,0)))


#%%
mmwr
meta:name:Date
meta:name:Description
meta:name:Issue
meta:name:Issue_Num
meta:name:Keywords
meta:name:MMWR_Type
meta:name:Page
meta:name:Volume
meta:name:citation_author
meta:name:citation_categories
meta:name:citation_doi
meta:name:citation_title
meta:name:citation_volume
meta:name:description
meta:name:keywords

eid
meta:name:citation_author
meta:name:citation_doi
meta:name:citation_pdf_url
meta:name:citation_title
meta:name:description
meta:name:keywords
meta:property:cdc:created
meta:property:og:description
meta:property:og:title

pcd
meta:name:DC.Language
meta:name:Description
meta:name:Keywords
meta:name:citation_author
meta:name:citation_categories
meta:name:citation_doi
meta:name:citation_title
meta:name:citation_volume
meta:name:description
meta:name:keywords
