#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: cmheilig

Extract, correct, harmonize, and organize metadata from MMWR, EID, and PCD
(Older MMWR publications are less consistent than those since 2016.)
These elements are extracted from file paths, <head> elements, and elsewhere

Metadata shared publicly in CSV format, 1 file for all corpora
    title
    canonical link (? redundant with URL)
    referring URL
    referred string
    keywords
    description
    doi
    authors
    categories
"""

#%% Set up environment
import pickle
import json
from collections import Counter, defaultdict
from bs4 import SoupStrainer
only_head = SoupStrainer(name='head')

os.chdir('/Users/cmheilig/cdc-corpora/_test')

#%% Retrieve (unpickle) trimmed, UTF-8 HTML
mmwr_cc_df = pickle.load(open("pickle-files/mmwr_cc_df.pkl", "rb")) # 
eid_cc_df = pickle.load(open("pickle-files/eid_cc_df.pkl", "rb")) # (4786, 8)
eid_cc_df = pickle.load(open("pickle-files/eid_cc_df.pkl", "rb")) # 

mmwr_toc_unx = pickle.load(open('pickle-files/mmwr_toc_unx.pkl', 'rb'))
mmwr_art_unx = pickle.load(open('pickle-files/mmwr_art_unx.pkl', 'rb'))
eid_toc_unx = pickle.load(open('pickle-files/eid_toc_unx.pkl', 'rb'))
eid_art_unx = pickle.load(open('pickle-files/eid_art_unx.pkl', 'rb'))
pcd_toc_unx = pickle.load(open('pickle-files/pcd_toc_unx.pkl', 'rb'))
pcd_art_unx = pickle.load(open('pickle-files/pcd_art_unx.pkl', 'rb'))

mmwr_toc_dl_df = pd.read_pickle('mmwr_toc_dl_df.pkl')
mmwr_art_dl_df = pd.read_pickle('mmwr_art_dl_df.pkl')
eid_toc_dl_df = pd.read_pickle('eid_toc_dl_df.pkl')
eid_art_dl_df = pd.read_pickle('eid_art_dl_df.pkl')
pcd_toc_dl_df = pd.read_pickle('pcd_toc_dl_df.pkl')
pcd_art_dl_df = pd.read_pickle('pcd_art_dl_df.pkl')

#%% Parse HTML <head> elements

mmwr_head_soup = {
    path: BeautifulSoup(html, 'lxml', parse_only=only_head, multi_valued_attributes=None)
    for path, html in tqdm(mmwr_art_unx.items())}
# 15161/15161 [04:18<00:00, 58.68it/s] 
eid_head_soup = {
    path: BeautifulSoup(html, 'lxml', parse_only=only_head, multi_valued_attributes=None)
    for path, html in tqdm(eid_art_unx.items())}
# 12769/12769 [03:46<00:00, 56.40it/s]
pcd_head_soup = {
    path: BeautifulSoup(html, 'lxml', parse_only=only_head, multi_valued_attributes=None)
    for path, html in tqdm(pcd_art_unx.items())}
# 3990/3990 [00:41<00:00, 97.14it/s]

mmwr_toc_soup = {
    path: BeautifulSoup(html, 'lxml')
    for path, html in tqdm(mmwr_toc_unx.items())}
# 
mmwr_art_soup = {
    path: BeautifulSoup(html, 'lxml')
    for path, html in tqdm(mmwr_art_unx.items())}
# 
eid_toc_soup = {
    path: BeautifulSoup(html, 'lxml')
    for path, html in tqdm(eid_toc_unx.items())}
# 330/330 [00:22<00:00, 14.56it/s]
eid_art_soup = {
    path: BeautifulSoup(html, 'lxml')
    for path, html in tqdm(eid_art_unx.items())}
# 12769/12769 [08:51<00:00, 24.01it/s]
pcd_toc_soup = {
    path: BeautifulSoup(html, 'lxml')
    for path, html in tqdm(pcd_toc_unx.items())}
# 
pcd_art_soup = {
    path: BeautifulSoup(html, 'lxml')
    for path, html in tqdm(pcd_art_unx.items())}
# 


#%%
def tag_info(tag):
    tag_name = tag.name
    tag_str = '' if not tag.string else tag.string
    tag_attr = tag.attrs
    n_attr = len(tag_attr)
    return dict(tag_name=tag_name, tag_str=tag_str, n_attr=n_attr, tag_attr=tag_attr)

ser_head_tags = [
    dict(ser=ser, path=path, seq=seq, **tag_info(tag))
    for ser in ['mmwr', 'eid', 'pcd']
    for path, soup in tqdm(eval(f'{ser}_head_soup').items())
    for seq, tag in enumerate(soup.find_all(True))]
ser_head_tags_df = pd.DataFrame(ser_head_tags) # (1370454, 7)

ser_head_tag_attr_names = [
     dict(ser=tag['ser'], path=tag['path'], seq=tag['seq'], tag_name=tag['tag_name'], 
          attr_names='|'.join(sorted(tag['tag_attr'].keys())))
     for tag in tqdm(ser_head_tags)]# if tag.get('tag_name') in {'link', 'meta'}]
ser_head_tag_attr_names_df = pd.DataFrame(ser_head_tag_attr_names) # (326825, 4)
(ser_head_tag_attr_names_df
 .value_counts(subset=['ser', 'tag_name', 'attr_names'], sort=False, dropna=False)
 .reset_index(name='freq')).to_excel('ser_head_tag_attr_names_df.xlsx', freeze_panes=(1,0))

def soup_head_count(soup):
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

def soup_head_tags(soup):
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

head_tags = [
    dict(ser=ser, path=path, **soup_head_tags(soup))
    for ser in ['mmwr', 'eid', 'pcd']
    for path, soup in tqdm(eval(f'{ser}_head_soup').items())]

[item
 for item in head_tags
 if (len(item['title']) > 1)]
# 'path': '/mmwr/preview/mmwrhtml/ss4808a2.htm'
[item
 for item in head_tags
 if (len(item['link_canon']) > 1)]
# []

x_names = [
    dict(path=item['path'], **Counter([name for name, value in item['meta_name']]))
    for item in head_tags]
x_props = [
    dict(path=item['path'], **Counter([name for name, value in item['meta_property']]))
    for item in head_tags]
pd.DataFrame(x_names).to_excel('x_names.xlsx', freeze_panes=(1,0))
pd.DataFrame(x_props).to_excel('x_props.xlsx', freeze_panes=(1,0))

# nonunique
# title occurs twice
# meta:name:Generator occurs twice
# meta:name:citation_author occurs >1
[item
 for item in head_tags
 if (len([x for x, y in item['meta_name'] if x == 'meta:name:Generator']) > 1)]
# 'path': '/mmwr/preview/mmwrhtml/ss4808a2.htm'
[item
 for item in head_tags
 if (len([x for x, y in item['meta_name'] if x == 'meta:name:citation_author']) > 1)]
# 11365 items, all in eid
Counter([y['ser'] for y in x])
Counter({'eid': 11365})

x = [
     dict(ser=item['ser'], path=item['path'], attr_name=name, attr_value=value)
      for item in head_tags
      for name, value in item['meta_name'] + item['meta_property']]
x_df = pd.DataFrame(x)
y_name_val = (x_df
 .value_counts(subset=['ser', 'attr_name', 'attr_value'], dropna=False)
 .reset_index(name='freq')) # (219904, 4)
y_name = (x_df
 .value_counts(subset=['ser', 'attr_name'], dropna=False)
 .reset_index(name='freq')) # (219904, 4) # (132, 3)

with pd.ExcelWriter('mmwr_head_attr_freqs.xlsx') as xlw:
    (y_name
     .pivot(index='attr_name', columns='ser', values='freq')
     .astype('Int64')
     .reset_index()
     .to_excel(xlw,sheet_name='name only', freeze_panes=(1,0)))
    (y_name_val
     .pivot(index=['attr_name', 'attr_value'], columns='ser', values='freq')
     .astype('Int64')
     .reset_index()
     .to_excel(xlw,sheet_name='name and attr', freeze_panes=(1,0)))

 .sort_values(['attr_name', 'ser']))

head_tag_freqs_df = pd.DataFrame(head_tag_freqs) # (31920, 83)

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

#%% MMWR
"""
Metadata elements:
    title
    canonical link (? redundant with URL)
    referring URL
    referred string
    keywords
    description
    doi
    authors
    categories

Corpora in JSON, keyed to path:
    mm{wr, rr, ss, su}_{toc, art_en}, mmwr_art_es
"""

"""
/mmwr/ind####_su.html,index####.htm,index####.html,indrr_####.html,indss_####.html
/mmwr/preview/ind####_rr.html,ind####_ss.html,ind####_su.html,index##.html
"""
mmwr_soup = {
    path: BeautifulSoup(html, 'lxml')
    for path, html in tqdm(mmwr_art_unx.items())}
# 15161/15161 [13:21<00:00, 18.91it/s]

# 2a(3) compile all the values from soup and place them in a prescribed order
mmwr_meta_names = ['MMWR_Type', 'Volume', 'Issue_Num', 'Issue', 'Page', 'Date', 
    'citation_categories', 'citation_author', 'citation_doi', 
    'keywords', 'Keywords', 'description', 'Description']

def mmwr_soup_meta_fn(soup):
    """ """
    div_dateline = soup.find('div', class_='dateline')
    div_dateline = '_ / _ / _' if div_dateline is None else \
                   div_dateline.get_text(strip=True)

    title = '' if soup.title.string is None else soup.title.string.strip()
    link_canon = soup.find('link', attrs={'rel': 'canonical'})
    link_canon = '' if link_canon is None else link_canon.get('href').strip()

    name_vals = {'md_' + meta_name: '' for meta_name in mmwr_meta_names}
    name_vals.update({'md_' + meta_name: 
                 '' if tag is None else tag.get('content').strip()
                 for meta_name in mmwr_meta_names
                 for tag in [soup.find(name='meta', attrs={'name': meta_name})]
                 if (tag is not None) and (tag.get('content') != '')})
    # extract and normalize numeric and alphabetic components
    _, name_vals['md_MMWR_Type'] = num_anno(name_vals['md_MMWR_Type'])
    md_vol_num, md_vol_anno = num_anno(name_vals.get('md_Volume'))
    md_iss_num, md_iss_anno = num_anno(name_vals.get('md_Issue'))
    name_vals['md_Issue_Num'], _ = num_anno(name_vals.get('md_Issue_Num'))
    md_page_num, md_page_anno = num_anno(name_vals.get('md_Page'))
    
    md_vals = dict(
        title=title, link_canon=link_canon, div_dateline=div_dateline, 
        **name_vals, md_vol_num=md_vol_num, md_vol_anno=md_vol_anno, 
        md_iss_num=md_iss_num, md_iss_anno=md_iss_anno, #md_Issue_Num=md_Issue_Num, 
        md_page_num=md_page_num, md_page_anno=md_page_anno)
    return md_vals

soup_meta_list = [
    dict(path=path, **mmwr_soup_meta_fn(soup))
    for path, soup in tqdm(mmwr_soup.items())]
# 15161/15161 [07:05<00:00, 35.63it/s]
# pd.DataFrame(div_dateline_list).to_excel('div_dateline_df.xlsx', freeze_panes=(1,0))


#%% EID
"""
Metadata elements:
    title
    canonical link (? redundant with URL)
    referring URL
    referred string
    keywords
    description
    doi
    authors
    categories
"""

def eid_metadata_fn(soup):
   """Process selected metadata from HTML <head> element.
   Using SoupStrainer makes this even more efficent."""
   title = '' if soup.title.string is None else soup.title.string.strip()
   dl_toc = eid_dl_re.search(soup.find('a', string=eid_dl_re).string).groupdict(default='')
   dl_toc['dl_mo'] = month_to_num[dl_toc['dl_mon']]
   th_eid = soup.find(name='th', string='EID')
   citation = ''if th_eid is None else (
      soup.find(name='th', string='EID')
          .find_next_sibling(name='td', class_='citationCell')
          .get_text(strip=True))
   citation_author = '|'.join([item.get('content').strip()
      for item in soup.find_all('meta', attrs={'name': 'citation_author'})])
   citation_doi = soup.find('meta', attrs={'name': 'citation_doi'})
   citation_doi = '' if citation_doi is None else citation_doi.get('content').strip()
   citation_title = soup.find('meta', attrs={'name': 'citation_title'})
   citation_title = '' if citation_title is None else citation_title.get('content').strip()
   citation_pdf_url = soup.find('meta', attrs={'name': 'citation_pdf_url'})
   citation_pdf_url = '' if citation_pdf_url is None else citation_pdf_url.get('content').strip()
   description = soup.find('meta', attrs={'name': 'description'})
   description = '' if description is None else description.get('content').strip()
   keywords = soup.find('meta', attrs={'name': 'keywords'})
   keywords = '' if keywords is None else re.sub(', ', '|', keywords.get('content'))
   # cdc_created = soup.find('meta', attrs={'property': 'cdc:created'})
   # cdc_created = '' if cdc_created is None else cdc_created.get('content').strip()
   og_description = soup.find('meta', attrs={'property': 'og:description'})
   og_description = '' if og_description is None else og_description.get('content').strip()
   og_title = soup.find('meta', attrs={'property': 'og:title'})
   og_title = '' if og_title is None else og_title.get('content').strip()
   og_url = soup.find('meta', attrs={'property': 'og:url'})
   og_url = '' if og_url is None else og_url.get('content').strip()
   canonical_link = soup.find('link', attrs={'rel': 'canonical'})
   canonical_link = '' if canonical_link is None else canonical_link.get('href').strip()
   # volume and issue number derived from URL; also in title
   if canonical_link == '' or canonical_link is None:
      volume, issue, year = None, None, None
   else:
      vol_iss = re.search(r'/(?P<vol>\d{1,2})/(?P<iss>\d{1,2})/', canonical_link)
      if vol_iss is None:
         volume, issue, year = None, None, None
      else:
         volume, issue = int(vol_iss.group('vol')), int(vol_iss.group('iss'))
         year = volume + 1994
   return dict(
      title_head=title, volume_head=volume, issue_head=issue, year_head=year,
      **dl_toc, citation=citation,
      citation_doi=citation_doi, canonical_link=canonical_link,
      citation_title=citation_title, citation_author=citation_author,
      citation_pdf_url=citation_pdf_url,
      description=description, keywords=keywords, #cdc_create=cdc_created,
      og_title=og_title, og_description=og_description, og_url=og_url)

eid_metadata = [dict(path=path, **eid_metadata_fn(soup))
                for path, soup in tqdm(eid_soup.items())]
# {path: eid_metadata_fn(soup) for path, soup in tqdm(eid_soup.items())}
# 12769/12769 [02:47<00:00, 76.09it/s]
# pickle.dump(eid_metadata, open("eid_metadata.pkl", "xb"))
# eid_metadata = pickle.load(open("eid_metadata.pkl", "rb"))
pd.DataFrame(eid_metadata).to_excel('eid_metadata.xlsx', freeze_panes=(1,0))


#%% PCD
"""
Metadata elements:
    title
    canonical link (? redundant with URL)
    referring URL
    referred string
    keywords
    description
    doi
    authors
    categories
"""
pcd_art_soup = {
    path: BeautifulSoup(html, 'lxml')
    for path, html in tqdm(pcd_art_unx.items())}
# 5091/5091 [01:41<00:00, 50.09it/s]

def pcd_metadata_fn(soup):
   """Process selected metadata from HTML <head> element.
   Using SoupStrainer makes this even more efficent."""
   title = '' if soup.title.string is None else soup.title.string
   citation_author = soup.find('meta', attrs={'name': 'citation_author'})
   citation_author = '' if citation_author is None else \
      citation_author.get('content').strip()
   citation_categories = soup.find('meta', attrs={'name': 'citation_categories'})
   citation_categories = '' if citation_categories is None else \
      citation_categories.get('content').strip()
   citation_doi = soup.find('meta', attrs={'name': 'citation_doi'})
   citation_doi = '' if citation_doi is None else citation_doi.get('content')
   citation_publication_date = soup.find('meta', attrs={'name': 'citation_publication_date'})
   citation_publication_date = '' if citation_publication_date is None else \
      citation_publication_date.get('content')
   citation_title = soup.find('meta', attrs={'name': 'citation_title'})
   citation_title = '' if citation_title is None else \
      citation_title.get('content').strip()
   citation_volume = soup.find('meta', attrs={'name': 'citation_volume'})
   citation_volume = '' if citation_volume is None else citation_volume.get('content')
   language = soup.find('meta', attrs={'name': 'DC.language'})
   language = '' if language is None else \
      re.sub('(, )+', '|', language.get('content')).strip()
   keywords = soup.find('meta', attrs={'name': 'keywords'})
   keywords = '' if keywords is None else \
      re.sub('(, )+', '|', keywords.get('content')).strip()
   Keywords = soup.find('meta', attrs={'name': 'Keywords'})
   Keywords = '' if Keywords is None else \
      re.sub('(, )+', '|', Keywords.get('content')).strip()
   # description = soup.find('meta', attrs={'name': 'description'})
   # description = '' if description is None else \
   #    re.sub('(, )+', '|', description.get('content')).strip()
   Description = soup.find('meta', attrs={'name': 'Description'})
   Description = '' if Description is None else \
      re.sub('(, )+', '|', Description.get('content')).strip()
   canonical_link = soup.find('link', attrs={'rel': 'canonical'})
   canonical_link = '' if canonical_link is None else canonical_link.get('href')
   return dict(
      title=title, citation_categories=citation_categories, 
      citation_doi=citation_doi, canonical_link=canonical_link,
      citation_title=citation_title, citation_author=citation_author,
      citation_publication_date=citation_publication_date, 
      citation_volume=citation_volume, language=language,
      keywords=keywords, Keywords=Keywords, 
      # description=description, 
      Description=Description)

pcd_metadata = [dict(path=path, **pcd_metadata_fn(soup)) 
                for path, soup in tqdm(pcd_soup.items())]
# 3990/3990 [00:25<00:00, 159.52it/s]
# pickle.dump(pcd_head_data, open("pcd_head_data.pkl", "wb"))
# pcd_head_data = pickle.load(open("pcd_head_data.pkl", "rb"))
