#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Extract and organize metadata and text of EID

@author: chadheilig


Main product: 
"""

#%% Import modules and set up environment
# import from 0_cdc-corpora-header.py

import time
from bs4 import SoupStrainer

os.chdir('/Users/cmheilig/cdc-corpora/_test')
EID_BASE_PATH_u3 = normpath(expanduser('~/cdc-corpora/eid_u3/'))

# EID DataFrame, articles only
eid_dframe = pickle.load(open('pickle-files/eid_dframe.pkl', 'rb'))
eid_art_dframe = eid_dframe.loc[eid_dframe.level == 'article', ].reset_index(drop=True)
# [11211 rows x 8 columns]

#%% Read HTML from mirror into list of strings

# alternatively, restore from eid_uni_html.pkl and take subset

# eid_art_html = [read_uni_html(EID_BASE_PATH_u3 + path)
#                      for path in tqdm(eid_art_dframe.mirror_path)]
# 10640/10640 [01:00<00:00, 174.56it/s]
# pickle.dump(eid_art_html, open('eid_art_html.pkl', 'wb'))
# eid_art_html = pickle.load(open('eid_art_html.pkl', 'rb'))

eid_art_html = [html_reduce_space_u(read_uni_html(EID_BASE_PATH_u3 + path))
                     for path in tqdm(eid_art_dframe.mirror_path)]
# 11211/11211 [02:07<00:00, 88.07it/s]
# pickle.dump(eid_art_html, open('eid_art_html.pkl', 'wb'))
# eid_art_html = pickle.load(open('eid_art_html.pkl', 'rb'))

#%% Count frequency of various elements across EID corpus

x = BeautifulSoup(eid_art_html[9000], 'lxml')
y = [tag.name for tag in x.find_all(True)]
z = { w: y.count(w) for w in sorted(set(y)) }
# repr(z)
#  {'a': 122, 'address': 1, 'body': 1, 'br': 7, 'button': 11, 'circle': 8, 
#   'div': 95, 'em': 3, 'footer': 1, 'form': 3, 'g': 12, 'h3': 1, 'h4': 2, 
#   'h5': 3, 'head': 1, 'header': 1, 'hr': 2, 'html': 1, 'i': 3, 'img': 1, 
#   'input': 13, 'li': 84, 'link': 15, 'main': 1, 'meta': 35, 'nav': 6, 
#   'noscript': 1, 'ol': 1, 'p': 14, 'path': 206, 'polygon': 27, 'rect': 11, 
#   'script': 23, 'span': 70, 'strong': 4, 'style': 1, 'svg': 34, 'symbol': 50, 
#   'table': 1, 'td': 3, 'th': 3, 'title': 14, 'tr': 3, 'ul': 20, 'use': 10}

def count_tags(html):
   tag_list = [tag.name for tag in BeautifulSoup(html, 'lxml').find_all(True)]
   tag_dict = { tag: tag_list.count(tag) for tag in sorted(set(tag_list)) }
   return tag_dict

start_time = time.time()
eid_art_tags = [count_tags(html) for html in tqdm(eid_art_html)]
print(f"\nTime elapsed: {int((time.time() - start_time) // 60)} min {round((time.time() - start_time) % 60, 1)} sec")
# 13:22.0
eid_art_tags_df = pd.DataFrame(eid_art_tags).fillna(0) # fill in 0s; converts to float64
eid_art_tags_df.to_excel('eid_art_tags_df.xlsx', engine='openpyxl')

pd.concat(
   [eid_art_tags_df.quantile(axis=0, q=pct/100) for pct in range(0, 110, 10)], 
   axis=1
   ).to_excel('eid_art_tags_quantiles.xlsx', engine='openpyxl')
# [116 rows x 11 columns]

# rewrite to operation on soup rather than HTML
# which allows applying function to subsoup objects
def count_tags(soup):
   tag_list = [tag.name for tag in soup.find_all(True)]
   tag_dict = { tag: tag_list.count(tag) for tag in sorted(set(tag_list)) }
   return tag_dict

# count_tags(BeautifulSoup(eid_art_html[9000], 'lxml'))

start_time = time.time()
eid_art_tags = [count_tags(BeautifulSoup(html, 'lxml')) 
                for html in tqdm(eid_art_html)]
print(f"\nTime elapsed: {int((time.time() - start_time) // 60)} min {round((time.time() - start_time) % 60, 1)} sec")
# 10640/10640 [10:33<00:00, 16.79it/s]
eid_art_tags_df = pd.DataFrame(eid_art_tags).fillna(0) # fill in 0s; converts to float64
eid_art_tags_df.to_excel('eid_art_tags_df.xlsx', engine='openpyxl')

pd.concat(
   [eid_art_tags_df.quantile(axis=0, q=pct/100) for pct in range(0, 110, 10)], 
   axis=1
   ).to_excel('eid_art_tags_quantiles.xlsx', engine='openpyxl')
# [116 rows x 11 columns]

#%% Focus on elements in <head>

only_head = SoupStrainer(name='head')
x = BeautifulSoup(eid_art_html[8690], 'lxml') # 10/4/03-0509_article

def eid_soup_head_count(soup):
   """Process selected metadata from HTML <head> element.
   Using SoupStrainer makes this even more efficent."""
   citation_author = len(soup.find_all('meta', attrs={'name': 'citation_author'}))
   citation_doi = len(soup.find_all('meta', attrs={'name': 'citation_doi'}))
   citation_title = len(soup.find_all('meta', attrs={'name': 'citation_title'}))
   description = len(soup.find_all('meta', attrs={'name': 'description'}))
   keywords = len(soup.find_all('meta', attrs={'name': 'keywords'}))
   og_description = len(soup.find_all('meta', attrs={'property': 'og:description'}))
   og_title = len(soup.find_all('meta', attrs={'property': 'og:title'}))
   og_url = len(soup.find_all('meta', attrs={'property': 'og:url'}))
   canonical_link = len(soup.find_all('link', attrs={'rel': 'canonical'}))
   return dict(citation_author=citation_author, citation_doi=citation_doi,
      citation_title=citation_title, description=description, keywords=keywords,
      og_description=og_description, og_title=og_title, og_url=og_url,
      canonical_link=canonical_link)

eid_soup_head_count(x)

%timeit -r 11 -n 20 BeautifulSoup(eid_art_html[8690], 'lxml', parse_only=only_head)
# 29.4 ms ± 2.67 ms per loop (mean ± std. dev. of 11 runs, 20 loops each)
%timeit -r 11 -n 20 eid_soup_head_count(BeautifulSoup(eid_art_html[8690], 'lxml', parse_only=only_head))
# 30.8 ms ± 950 µs per loop (mean ± std. dev. of 11 runs, 20 loops each)
%timeit -r 11 -n 20 BeautifulSoup(eid_art_html[8690], 'lxml')
# 47.5 ms ± 4.67 ms per loop (mean ± std. dev. of 11 runs, 20 loops each)
%timeit -r 11 -n 20 eid_soup_head_count(BeautifulSoup(eid_art_html[8690], 'lxml'))
# 108 ms ± 2.24 ms per loop (mean ± std. dev. of 11 runs, 20 loops each)

y = pd.DataFrame([eid_soup_head_count(BeautifulSoup(html, 'lxml', parse_only=only_head)) 
                for html in tqdm(eid_art_html)])
y.to_excel('eid_soup_head_count.xlsx', engine='openpyxl')

# citation_author can appear 0, 1, or more times
# the others appear 1 time; description and keywords can appear 0 times
def eid_soup_head(soup):
   """Process selected metadata from HTML <head> element.
   Using SoupStrainer makes this even more efficent."""
   title = soup.title.string.strip()
   citation_author = '|'.join([item.get('content') 
      for item in soup.find_all('meta', attrs={'name': 'citation_author'})])
   citation_doi = soup.find('meta', attrs={'name': 'citation_doi'}).get('content')
   citation_title = soup.find('meta', attrs={'name': 'citation_title'}).get('content').strip()
   description = soup.find('meta', attrs={'name': 'description'})
   description = '' if description is None else description.get('content').strip()
   keywords = soup.find('meta', attrs={'name': 'keywords'})
   keywords = '' if keywords is None else re.sub(', ', '|', keywords.get('content'))
   og_description = soup.find('meta', attrs={'property': 'og:description'}).get('content').strip()
   og_title = soup.find('meta', attrs={'property': 'og:title'}).get('content').strip()
   og_url = soup.find('meta', attrs={'property': 'og:url'}).get('content')
   canonical_link = soup.find('link', attrs={'rel': 'canonical'}).get('href')
   # volume and issue number derived from URL; also in title
   vol_iss = re.search(r'/(?P<vol>\d{1,2})/(?P<iss>\d{1,2})/', canonical_link)
   volume, issue = int(vol_iss.group('vol')), int(vol_iss.group('iss'))
   year = volume + 1994
   return dict(
      title_head=title, volume_head=volume, issue_head=issue, year_head=year,
      citation_doi=citation_doi, canonical_link=canonical_link,
      citation_title=citation_title, citation_author=citation_author,
      description=description, keywords=keywords,
      og_title=og_title, og_description=og_description, og_url=og_url)

eid_soup_head(x)

# robust version - when file lacks some of these metadata elements
def eid_soup_head_(soup):
   """Process selected metadata from HTML <head> element.
   Using SoupStrainer makes this even more efficent."""
   title = soup.title.string.strip()
   citation_author = '|'.join([item.get('content') 
      for item in soup.find_all('meta', attrs={'name': 'citation_author'})])
   citation_doi = soup.find('meta', attrs={'name': 'citation_doi'})
   citation_doi = '' if citation_doi is None else citation_doi.get('content').strip()
   citation_title = soup.find('meta', attrs={'name': 'citation_title'})
   citation_title = '' if citation_title is None else citation_title.get('content').strip()
   description = soup.find('meta', attrs={'name': 'description'})
   description = '' if description is None else description.get('content').strip()
   keywords = soup.find('meta', attrs={'name': 'keywords'})
   keywords = '' if keywords is None else re.sub(', ', '|', keywords.get('content'))
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
      citation_doi=citation_doi, canonical_link=canonical_link,
      citation_title=citation_title, citation_author=citation_author,
      description=description, keywords=keywords,
      og_title=og_title, og_description=og_description, og_url=og_url)

eid_soup_head_(x)

eid_head_data = [eid_soup_head(BeautifulSoup(html, 'lxml', parse_only=only_head)) 
                for html in tqdm(eid_art_html)]
# 11211/11211 [04:47<00:00, 39.00it/s]
# pickle.dump(eid_head_data, open("eid_head_data.pkl", "wb"))
# eid_head_data = pickle.load(open("eid_head_data.pkl", "rb"))
pd.DataFrame(eid_head_data).to_excel('eid_head_data.xlsx', engine='openpyxl')

#%% Focus on elements in <main>

only_main = SoupStrainer(name='main') # contains main body of article

x = BeautifulSoup(eid_art_html[8690], 'lxml', parse_only=only_main)
len(x.main) # 36
len(list(x.main.children)) # 36

def eid_soup_main_count(soup):
   """Process selected metadata from HTML <main> element.
   Using SoupStrainer makes this even more efficent."""

   # main metadata
   vol_type = soup.find_all('h5') # volume and article type
   vol_type = sum([x.parent.name == 'main' for x in vol_type])
   title = len(soup.find_all('h3', attrs={'class': 'article-title'}))
   doi = len(soup.find_all('p', attrs={'id': 'article-doi-footer'}))

   # content
   abstract = len(soup.find_all('div', attrs={'id': 'abstract'}))
   mainbody = len(soup.find_all('div', attrs={'id': 'mainbody'}))
   figures = len(soup.find_all('ul', attrs={'id': 'figures'}))
   tables = len(soup.find_all('ul', attrs={'id': 'tables'}))

   # detailed metadata
   authors = len(soup.find_all('div', attrs={'id': 'authors'}))
   subauthors = len(soup.find_all('div', attrs={'id': 'sub-authors'}))
   author_affil = len(soup.find_all('div', attrs={'id': 'author-affiliations'}))
   subauthor_affil = len(soup.find_all('div', attrs={'id': 'sub-author-affiliations'}))
   acks0 = len(soup.find_all('a', attrs={'id': 'acknowledgements'}))
   acks1 = soup.find_all('div', attrs={'class': 'blockquote-indent'})
   acks1 = sum([x.parent.name == 'main' for x in acks1])
   refs0 = len(soup.find_all('a', attrs={'id': 'references'}))
   refs1 = len(soup.find_all('div', attrs={'id': 'articlereferences'}))
   subrefs = len(soup.find_all('div', attrs={'id': 'sub-references'}))

   return dict(
      vol_type=vol_type, title=title, doi=doi, 
      abstract=abstract, mainbody=mainbody, figures=figures, tables=tables, 
      authors=authors, subauthors=subauthors, 
      author_affil=author_affil, subauthor_affil=subauthor_affil,
      acks0=acks0, acks1=acks1, refs0=refs0, refs1=refs1, subrefs=subrefs)

eid_soup_main_count(x)

y = pd.DataFrame([eid_soup_main_count(BeautifulSoup(html, 'lxml', parse_only=only_main)) 
                for html in tqdm(eid_art_html)])
y.to_excel('eid_soup_main_count.xlsx', engine='openpyxl')
# 10640/10640 [15:43<00:00, 11.27it/s]

# conclusions: need to take care with vol_type and acks1 components
# remove all markup
# rember to combine ack0 and ack1, refs0 and refs1

# figure out various h5 main.children
eid_main_h5 = [h5 for html in tqdm(eid_art_html)
   for h5 in BeautifulSoup(html, 'lxml', parse_only=only_main).find_all('h5')
   if h5.parent.name == 'main']
with open('eid_main_h5.txt', 'w') as file_out:
   file_out.write(str(eid_main_h5))
## extract, recode
# <h5 class="header">Volume[^<>]</h5>    volume and month
# <h5 class="header"><em>[^<>]</em></h5> issue type
# <h5 class="header"><em></em></h5>      theme issue in Sep 2015
## omit
# <h5 class="header online-only">Peer Reviewed Report Available Online Only</h5>
# <h5>Figure</h5> <h5>Figures</h5>
# <h5>Table</h5>  <h5>Tables</h5>

eid_main_div_xml_section = [div for html in tqdm(eid_art_html)
    for div in BeautifulSoup(html, 'lxml', parse_only=only_main).find_all('div', class_='xml-section')
    if div.parent.name == 'main']
# <a id="acknowledgements"> is just the section heading
# <a id="references"> is just the section heading

# sub-authors, sub-author-affiliantions, and sib-references are where
# a letter or notice is followed by a reply on the same page
# there appear to be about 9 of them
# we will harvest the main letter but not the 'subbody'
# eid_art_dframe.iloc[[5590, 5591, 8497, 8534, 8540, 8541, 8624, 9865],:]

def eid_soup_main(soup):
   """Process selected metadata from HTML <main> element.
   Using SoupStrainer makes this even more efficent."""

   # find_all() where possibly >1; find() where 0 or 1
   # main metadata
   # volume and article type, but only for direct children of <main> element
   vol_type = [h5 for h5 in soup.find_all('h5') if h5.parent.name == 'main']
   vol_re = re.compile(r'''
      Volume\ (?P<vol>\d{1,2}),\                         # volume
      (Number\ (?P<iss>\d{1,2})|(?P<supp>Supplement)).+? # issue (# or Supp)
      (?P<mon>\w+)\ (?P<year>\d{4})                    # month, year
      ''', re.VERBOSE)
   vol_match = vol_re.search(vol_type[0].get_text('|', strip=True))
   volume, year, month = vol_match.group('vol', 'year', 'mon')
   issue = vol_match.group('iss') if vol_match.group('iss') is not None \
      else vol_match.group('supp')
   type_ = vol_type[1].get_text('|', strip=True)
   # in rare case (21x) where empty, pull info from vol_type[0]
   if type_ == '':
      type_ = '|'.join(list(vol_type[0].stripped_strings)[1:])
   
   title = soup.find('h3', class_='article-title')
   title = '' if title is None else title.get_text('|', strip=True)
   doi = soup.find('p', id='article-doi-footer')
   doi = '' if doi is None else doi.get_text('|', strip=True)

   # content
   abstract = soup.find('div', id='abstract')
   abstract = '' if abstract is None else abstract.get_text('|', strip=True)
   mainbody = soup.find('div', id='mainbody')
   mainbody = '' if mainbody is None else mainbody.get_text('|', strip=True)
   figures = soup.find('ul', id='figures')
   figures = '' if figures is None else figures.get_text('|', strip=True)
   tables = soup.find('ul', id='tables')
   tables = '' if tables is None else tables.get_text('|', strip=True)

   # detailed metadata
   authors = soup.find('div', id='authors')
   authors = '' if authors is None else authors.get_text('|', strip=True)
   author_affil = soup.find('div', id='author-affiliations')
   author_affil = '' if author_affil is None else author_affil.get_text('|', strip=True)
   acknowls = soup.find('div', class_='blockquote-indent')
   acknowls = '' if acknowls is None else acknowls.get_text('|', strip=True)
   references = soup.find('div', id='articlereferences')
   references = '' if references is None else references.get_text('|', strip=True)

   return dict(
      title_main=title, volume_main=volume, issue_main=issue, year_main=year, 
      month_main=month, type_=type_, doi_main=doi, 
      abstract=abstract, mainbody=mainbody, figures=figures, tables=tables, 
      authors=authors, author_affil=author_affil, 
      acknowls=acknowls, references=references)

eid_soup_main(x)

# acknowledgements
x = BeautifulSoup(eid_art_html[5590], 'lxml', parse_only=only_main)

y = [elem for html in tqdm(eid_art_html[8490:9870])
   for elem in BeautifulSoup(html, 'lxml', parse_only=only_main).find_all('a', id='references')
   if elem.parent.name == 'main']
[5590, 5591, 8497, 8534, 8540, 8541, 8624, 9865]

eid_main_data = [eid_soup_main(BeautifulSoup(html, 'lxml', parse_only=only_main)) 
                for html in tqdm(eid_art_html)]
# 11211/11211 [07:50<00:00, 23.85it/s]
# pickle.dump(eid_main_data, open("eid_main_data.pkl", "wb"))
# eid_main_data = pickle.load(open("eid_main_data.pkl", "rb"))
pd.DataFrame(eid_main_data).to_excel('eid_main_data.xlsx', engine='openpyxl')

#%% Combine <head> and <main> data

# dict.update(other_dict) modifies dict in place, so first copy <head>
eid_parsed_data = eid_head_data.copy()

# now append <main> data in place to copy of <head> data
# this requires that keys in dict and other_dict be distinct
# (else other_dict will overwrite corresponding keys' values)
# since dict.update(other_dict) returns None, assign list result to nul
nul = [parsed.update(main) for parsed, main in 
       tqdm(zip(eid_parsed_data, eid_main_data))]
# takes less than a second

nul.count(None) # 10640
[len(x) for x in eid_parsed_data].count(28) # 10640
del nul

# pickle.dump(eid_parsed_data, open('eid_parsed_data.pkl', 'wb'))

import json
# write in JSON format using UTF-8 rather than ASCII
with open('eid_parsed_data.json', 'w') as json_out:
   json.dump(eid_parsed_data, json_out, ensure_ascii=False)

# eid_parsed_data_ = json.load(open('eid_parsed_data.json'))
# eid_parsed_data == eid_parsed_data_ # True

pd.DataFrame(eid_parsed_data).to_excel('eid_parsed_data.xlsx', engine='openpyxl')
# 42 mainbody fields and 2 references fields are truncated at 32767 characters
# must use pickle or json for full fidelity

#%% Output objects for learning resources
# eid_dframe -> CSV
# eid_art_html -> eid_art_html.json
# eid_art_dframe + eid_head_data + eid_main_data -> eid_parsed_data.{json,xlsx}

eid_dframe.to_csv('eid_dframe.csv')

with open('eid_art_html.json', 'w') as json_out:
   json.dump(eid_art_html, json_out, ensure_ascii=False)
# eid_art_html_ = json.load(open('eid_art_html.json'))
# eid_art_html == eid_art_html_ # True

eid_parsed_dframe = pd.concat([eid_art_dframe, pd.DataFrame(eid_head_data), 
                               pd.DataFrame(eid_main_data)], axis=1)
eid_parsed_dframe.sort_values(by="citation_doi", inplace=True)
eid_parsed_dframe.to_excel('eid_parsed_dframe.xlsx', engine='openpyxl')
eid_parsed_dframe.to_json('eid_parsed_dframe.json', orient='records')

# x = pd.read_json('eid_parsed_dframe.json')
# x.to_excel('x-temp.xlsx', engine='openpyxl')
# differences appear to be: (1) index, (2) dtype of volume_main, year_main
# differences do not appear to be substantive
