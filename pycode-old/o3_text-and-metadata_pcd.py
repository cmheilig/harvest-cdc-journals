#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Extract and organize metadata and text of PCD

@author: chadheilig


Main product: 
"""

#%% Import modules and set up environment
# import from 0_cdc-corpora-header.py

import time
from dateutil.parser import parse
import copy

os.chdir('/Users/chadheilig/cdc-corpora/_test')
PCD_BASE_PATH_u3 = normpath(expanduser('~/cdc-corpora/pcd_u3/'))

# PCD DataFrame, reduced to 2 columns for articles only
pcd_dframe = pickle.load(open('pickle-files/pcd_dframe.pkl', 'rb'))
pcd_art_frame = pcd_dframe.loc[pcd_dframe.level == 'article', 'mirror_path':'string']
pcd_art_frame.index = range(3542)
# [3542 rows x 2 columns]

#%% Read HTML from mirror into list of strings

# pcd_art_html = [read_uni_html(PCD_BASE_PATH_u3 + path)
#                      for path in tqdm(pcd_art_frame.mirror_path)]
# 3237/3237 [00:09<00:00, 326.39it/s]
# pickle.dump(pcd_art_html, open('pcd_art_html.pkl', 'wb'))
# pcd_art_html = pickle.load(open('pcd_art_html.pkl', 'rb'))

pcd_art_html = [html_reduce_space_u(read_uni_html(PCD_BASE_PATH_u3 + path))
                     for path in tqdm(pcd_art_frame.mirror_path)]
# 3542/3542 [00:22<00:00, 156.17it/s]
# pickle.dump(pcd_art_html, open('pcd_art_html.pkl', 'wb'))
# pcd_art_html = pickle.load(open('pcd_art_html.pkl', 'rb'))

# Storing all soup works for PCD because of its size
only_body = SoupStrainer(name='body')
pcd_art_soup = [BeautifulSoup(html, 'lxml', parse_only=only_body) 
                for html in tqdm(pcd_art_html)]
# 3542/3542 [01:45<00:00, 33.50it/s]

# attempting to dump as pickle exceeds recursion depth
# crashes with sys.setrecursionlimit(50000)


#%% 3 exemplars to examine

# https://www.cdc.gov/pcd/issues/2019/19_0035.htm
# https://www.cdc.gov/pcd/issues/2014/14_0047.htm
# https://www.cdc.gov/pcd/issues/2014/14_0047_es.htm
# https://www.cdc.gov/pcd/issues/2011/jul/10_0177.htm
# https://www.cdc.gov/pcd/issues/2011/jul/10_0177_es.htm
# https://www.cdc.gov/pcd/issues/2011/jan/09_0236.htm
# https://www.cdc.gov/pcd/issues/2011/jan/09_0236_es.htm
# https://www.cdc.gov/pcd/issues/2004/jan/03_0005.htm

with pd.option_context("display.max_colwidth", 36):
   display(pd.concat([pcd_art_frame.loc[pcd_art_frame.mirror_path.str.contains(stem)] 
      for stem in ('19_0035', '14_0047', '10_0177', '09_0236', '03_0005')]))
#                               mirror_path                               string
# 73           /pcd/issues/2019/19_0035.htm  Chronic Obstructive Pulmonary Di...
# 968          /pcd/issues/2014/14_0047.htm  Characteristics of the Built Env...
# 2533      /pcd/issues/2014/14_0047_es.htm  Características del entorno cons...
# 1554     /pcd/issues/2011/jul/10_0177.htm  Forecasting Diabetes Prevalence ...
# 2900  /pcd/issues/2011/jul/10_0177_es.htm  Proyección de la prevalencia de ...
# 1645     /pcd/issues/2011/jan/09_0236.htm  Systems-Level Smoking Cessation ...
# 2963  /pcd/issues/2011/jan/09_0236_es.htm  Actividades para la cesación del...
# 2488     /pcd/issues/2004/jan/03_0005.htm  Osteoporosis and Health-Related ...

# pcd_art_frame.iloc[[73, 968, 2533, 1554, 2900, 1645, 2963, 2488], :]

with open('19_0035-pretty.html', 'w') as file_out:
   file_out.write(html_prettify_u(pcd_art_html[73]))
with open('14_0047-pretty.html', 'w') as file_out:
   file_out.write(html_prettify_u(pcd_art_html[968]))
with open('14_0047_es-pretty.html', 'w') as file_out:
   file_out.write(html_prettify_u(pcd_art_html[2533]))
with open('10_0177-pretty.html', 'w') as file_out:
   file_out.write(html_prettify_u(pcd_art_html[1554]))
with open('10_0177_es-pretty.html', 'w') as file_out:
   file_out.write(html_prettify_u(pcd_art_html[2900]))
with open('09_0236-pretty.html', 'w') as file_out:
   file_out.write(html_prettify_u(pcd_art_html[1645]))
with open('09_0236_es-pretty.html', 'w') as file_out:
   file_out.write(html_prettify_u(pcd_art_html[2963]))
with open('03_0005-pretty.html', 'w') as file_out:
   file_out.write(html_prettify_u(pcd_art_html[2488]))
del file_out

#%% Focus on elements in <head>

only_head = SoupStrainer(name='head')
x = BeautifulSoup(pcd_art_html[73], 'lxml').head # 2019/19_0035.htm

def pcd_soup_head_count(soup):
   """Process selected metadata from HTML <head> element.
   Using SoupStrainer makes this even more efficent."""
   citation_author = len(soup.find_all('meta', attrs={'name': 'citation_author'}))
   citation_categories = len(soup.find_all('meta', attrs={'name': 'citation_categories'}))
   citation_doi = len(soup.find_all('meta', attrs={'name': 'citation_doi'}))
   citation_issn = len(soup.find_all('meta', attrs={'name': 'citation_issn'}))
   citation_journal_abbrev = len(soup.find_all('meta', attrs={'name': 'citation_journal_abbrev'}))
   citation_publication_date = len(soup.find_all('meta', attrs={'name': 'citation_publication_date'}))
   citation_title = len(soup.find_all('meta', attrs={'name': 'citation_title'}))
   citation_volume = len(soup.find_all('meta', attrs={'name': 'citation_volume'}))
   dc_date = len(soup.find_all('meta', attrs={'name': 'DC.date'}))
   description = len(soup.find_all('meta', attrs={'name': 'description'}))
   keywords = len(soup.find_all('meta', attrs={'name': 'keywords'}))
   canonical_link = len(soup.find_all('link', attrs={'rel': 'canonical'}))
   return dict(citation_author=citation_author, 
      citation_categories=citation_categories, citation_doi=citation_doi,
      citation_issn=citation_issn, citation_journal_abbrev=citation_journal_abbrev,
      citation_publication_date=citation_publication_date,
      citation_title=citation_title, citation_volume=citation_volume,
      dc_date=dc_date, description=description, keywords=keywords,
      canonical_link=canonical_link)

pcd_soup_head_count(x)

y = pd.DataFrame([pcd_soup_head_count(BeautifulSoup(html, 'lxml', parse_only=only_head)) 
                for html in tqdm(pcd_art_html)])
# 3237/3237 [00:51<00:00, 63.34it/s]
y.to_excel('pcd_soup_head_count.xlsx', engine='openpyxl')

# citation_categories is article type
# citation_issn is fixed at '1545-1151'
# citation_journal_abbrev is 'Prev Chronic Dis' (2012-2014) 
#    or 'Prev. Chronic Dis.' (2015-2020)
# description is 1 of 2 fixed values
#    Preventing Chronic Disease (PCD) is a peer-reviewed electronic journal established by the National Center for Chronic Disease Prevention and Health Promotion. PCD provides an open exchange of information and knowledge among researchers, practitioners, policy makers, and others who strive to improve the health of the public through chronic disease prevention.
#    Preventing Chronic Disease (PCD) is a peer-reviewed electronic journal established by the National Center for Chronic Disease Prevention and Health Promotion. PCD provides an open exchange on the very latest in chronic disease prevention, research findings, public health interventions, and the exploration of new theories and concepts. 
# DC.date is not useful

def pcd_soup_head(soup):
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
   keywords = soup.find('meta', attrs={'name': 'keywords'})
   keywords = '' if keywords is None else \
      re.sub('(, )+', '|', keywords.get('content')).strip()
   canonical_link = soup.find('link', attrs={'rel': 'canonical'})
   canonical_link = '' if canonical_link is None else canonical_link.get('href')
   return dict(
      title_head=title, type_head=citation_categories, 
      citation_doi=citation_doi, canonical_link=canonical_link,
      citation_title=citation_title, citation_author=citation_author,
      citation_publication_date=citation_publication_date, 
      citation_volume=citation_volume,
      keywords=keywords)

pcd_soup_head(x)

pcd_head_data = [pcd_soup_head(BeautifulSoup(html, 'lxml', parse_only=only_head)) 
                for html in tqdm(pcd_art_html)]
# 3542/3542 [00:57<00:00, 61.27it/s]
# pickle.dump(pcd_head_data, open("pcd_head_data.pkl", "wb"))
# pd.DataFrame(pcd_head_data).to_excel('pcd_head_data.xlsx', engine='openpyxl')
# pcd_head_data = pickle.load(open("pcd_head_data.pkl", "rb"))

#%% Focus on elements in <body>

# most PCD files do not have a <main> element
# only_main = SoupStrainer(name='main') # contains main body of article
x = BeautifulSoup(pcd_art_html[73], 'lxml') # 2019/19_0035.htm

# Inspection of HTML <body>s shows which elements contain content and metatdata
# of interest

# Article segments largely delimited by variations of 'Back to top'
# Systematically assemble elements that might serve as these delimiters

#%% 1. Find <a>, <p>, and <span> elements whose text content contains word top

za_toptext = [tag for soup in tqdm(pcd_art_soup)
                 for tag in soup.find_all('a', href=True)
                 if tag.get_text() is not None and
                    re.search(r'\btop\b', tag.get_text(), re.I)]
zp_toptext = [tag for soup in tqdm(pcd_art_soup)
                 for tag in soup.find_all('p')
                 if tag.find('a', href=True, recursive=False) is not None and
                    tag.get_text() is not None and
                    re.search(r'\btop\b', tag.get_text(), re.I)]
zspan_toptext = [tag for soup in tqdm(pcd_art_soup)
                 for tag in soup.find_all('span')
                 if tag.find('a', href=True, recursive=False) is not None and
                    tag.get_text() is not None and
                    re.search(r'\btop\b', tag.get_text(), re.I)]

len(set([str(tag) for tag in za_toptext])) # 11
len(set([tag.get_text('|', strip=True) for tag in za_toptext])) # 7
len(set([str(tag) for tag in zp_toptext])) # 71
len(set([tag.get_text('|', strip=True) for tag in zp_toptext])) # 65
len(set([str(tag) for tag in zspan_toptext])) # 2
len(set([tag.get_text('|', strip=True) for tag in zspan_toptext])) # 1

# za_toptext
z = za_toptext # 21957
z_str = sorted([str(tag) for tag in z])
z_text = sorted([tag.get_text('|', strip=True) for tag in z])
{ item: z_str.count(item) for item in sorted(set(z_str)) } # 11
{ item: z_text.count(item) for item in sorted(set(z_text)) } # 7
# delete 3 entries (4 occurrences) with '"top 10"' and 'top priority'
# 21037 <a> texts contain ['Back to top', 'Top', 'Top of Page']

{'<a class="psmall" href="#top"> Back to top </a>': 1,
 '<a class="tp-link-policy" href="#"> Top </a>': 8,
 '<a href="#"> Back to top </a>': 7580,
 '<a href="#"> Top </a>': 8071,
 '<a href="#"> Top of Page </a>': 5369,
 '<a href="#top"> Back to top </a>': 3,
 '<a href="#ttop"> Back to top </a>': 5,
 '<a href="/pcd/for_authors/top_five.htm"> Top 20 Manuscript Problems </a>': 916}
{'Back to top': 7589,
 'Top': 8079,
 'Top 20 Manuscript Problems': 916,
 'Top of Page': 5369}

# zspan_toptext
z = zspan_toptext # 16
z_str = sorted([str(tag) for tag in z])
z_text = sorted([tag.get_text('|', strip=True) for tag in z])
{ item: z_str.count(item) for item in sorted(set(z_str)) }
{ item: z_text.count(item) for item in sorted(set(z_text)) }
# Among <span> with <a> child, 16 texts contain 'Top'

{'<span class="text-right d-block"> <span class="icon-angle-up"> <!-- --> </span> <a href="#"> Top </a> </span>': 8,
 '<span class="toTop"> <span class="icon-angle-up"> <!-- --> </span> <a class="tp-link-policy" href="#"> Top </a> </span>': 8}
{'Top': 16}

# zp_toptext
z = zp_toptext # 21080
z_str = sorted([str(tag) for tag in z])
z_text = sorted([tag.get_text('|', strip=True) for tag in z])

with open('zp_toptext-str.txt', 'w') as file_out:
   for z_s in sorted(set(z_str)):
      file_out.write(f'{z_s}\n')
with open('zp_toptext-text.txt', 'w') as file_out:
   for z_t in sorted(set(z_text)):
      file_out.write(f'{z_t}\n')
del file_out, z_s, z_t

print(sorted([len(x) for x in set(z_str)], reverse=True))
# z_str = sorted([str(tag) for tag in z if len(str(tag))]) # 21080
print(sorted([len(x) for x in set(z_text)], reverse=True))
# z_text = sorted([tag.get_text('|', strip=True) for tag in z 
#                  if len(tag.get_text('|', strip=True))])   # 21080
# review of results suggests limiting lengths: str 66, text 13 
z_str = sorted([str(tag) for tag in z if len(str(tag)) <= 66]) # 21019
z_text = sorted([tag.get_text('|', strip=True) for tag in z 
                 if len(tag.get_text('|', strip=True)) <= 13]) # 21019
{ item: z_str.count(item) for item in sorted(set(z_str)) }
{ item: z_text.count(item) for item in sorted(set(z_text)) }
# Among <p> with <a> child, 21019 texts contain
#   ['Back to top', 'Back to top|]', 'Top', 'Top of Page']

{'<p align="left" class="psmall"> <a href="#"> Back to top </a> </p>': 1,
 '<p class="float-right"> <a href="#"> Top </a> </p>': 8063,
 '<p class="psmall" dir="ltr"> <a href="#"> Back to top </a> </p>': 9,
 '<p class="psmall"> <a href="#"> Back to top </a> </p>': 7555,
 '<p class="psmall"> <a href="#"> Back to top </a> ] </p>': 2,
 '<p class="psmall"> <a href="#top"> Back to top </a> </p>': 3,
 '<p class="psmall"> <a href="#ttop"> Back to top </a> </p>': 5,
 '<p class="topOPage"> <a href="#"> Top of Page </a> </p>': 5369,
 '<p> <a class="psmall" href="#top"> Back to top </a> </p>': 1,
 '<p> <a href="#"> Back to top </a> </p>': 11}
{'Back to top': 7585, 'Back to top|]': 2, 'Top': 8063, 'Top of Page': 5369}

pd.DataFrame(\
   [{ 'markup': z_s, 
      'content': BeautifulSoup(z_s, 'lxml').p.get_text('|', strip=True),
      'freq': z_str.count(z_s) }
         for z_s in sorted(set(z_str))]).to_excel('zp_toptext.xlsx', engine='openpyxl')

# The 20137 <a> texts that contain ['Back to top', 'Top', 'Top of Page']
#    include 16 <span> texts with 'Top'
#    and 21019 <p> texts with ['Back to top', 'Back to top|]', 'Top', 'Top of Page']
# Seek 2 other instances of <a> with 'top' in content but not in <p> or <span>

# Elements with child <a> element and text with word 'top' (ingore case)
z_all_toptext = [tag for soup in tqdm(pcd_art_soup)
                 for tag in soup.find_all(True)
                 if tag.find('a', href=True, recursive=False) is not None and
                    tag.get_text() is not None and
                    re.search(r'\btop\b', tag.get_text(), re.I)]
# 22965 items in list
set([z.name for z in z_all_toptext]) 
# ('caption', 'div', 'h5', 'li', 'p', 'span', 'td')
{ elem: [z.name for z in z_all_toptext].count(elem) 
         for elem in ('caption', 'div', 'h5', 'li', 'p', 'span', 'td') }
{'caption': 2, 'div': 2, 'h5': 8, 'li': 1855, 'p': 21080, 'span': 16, 'td': 2}

# Review <p>s not in zp_toptext, based on text length; there are 61
z_all_toptext_p = [tag for tag in z_all_toptext
                  if (tag.name == 'p' and len(tag.get_text('|', strip=True)) > 13)]
# Inspect <a> elements that are direct children of the <p> element
[(i, tag.find_all('a', recursive=False)) for (i, tag) in enumerate(z_all_toptext_p)]
# (46, [<a href="#"> Back to top </a>])
z_all_toptext_p[46].find('a', recursive=False).parent
print(z_all_toptext_p[46].find('a', recursive=False).parent.\
      prettify(formatter='minimal'))
# Go up DOM tree to find enclosing <body> element
z_all_toptext_p[46].find('a', recursive=False).parent.parent.parent.parent.parent.name
# Write <body> to file to inspect
with open('z_all_toptext_p46.htm', 'w') as file_out:
   file_out.write(z_all_toptext_p[46].find('a', recursive=False).\
                  parent.parent.parent.parent.parent.prettify(formatter='minimal'))
del file_out
# Pinpointed: http://www.cdc.gov/pcd/issues/2010/mar/09_0106.htm,
# which has '<a href="#">Back to top</a>' stuck at the end of a 
# <p class="caption">, not <p class="small"> as in the rest of the doc.

# Review <div>s not in zp_toptext
z_all_toptext_div = [tag for tag in z_all_toptext
                  if (tag.name == 'div')]
# Inspect <a> elements that are direct children of the <div> element
[tag.find_all('a', recursive=False) for tag in z_all_toptext_div]
# Zero in on the 2nd result, which has '<a href="#"> Back to top </a>'
z_all_toptext_div[1].find('a', recursive=False).parent
# Pinpointed: http://www.cdc.gov/pcd/issues/2010/nov/10_0104.htm,
# which has '<a href="#">Back to top</a>' not inside <p> or <span>.
# Rendering engine inserts missing <p> to correct for malformed HTML.

# Conclusion 1: 
# Each <a> element with href that begins '#' and content including 'top'
#    ends a document segment. 
# All but 1 are direct children of <p> or <span>.
# All but 2 are direct children of <p> or <span> that contain no additional text.

#%% 2. Find elements that might contain corresponding Spanish-language text
#      <a> with href in {'#top', '#ttop'}; <p> with class 'topOPage'
#      inspection of PCD Spanish-language pages

za_toptext = [tag for soup in tqdm(pcd_art_soup)
                 for tag in soup.find_all('a', href=['#top', "#ttop"])]
# 415; sum([x.get_text() is None for x in za_toptext])
z = za_toptext # 415
z_str = sorted([str(tag) for tag in z])
z_text = sorted([tag.get_text('|', strip=True) for tag in z])
{ item: z_str.count(item) for item in sorted(set(z_str)) }
# { item: z_text.count(item) for item in sorted(set(z_text)) }

{'<a class="psmall" href="#top"> Back to top </a>': 1,
 '<a href="#top"> Back to top </a>': 3,
 '<a href="#top"> Inicio de página </a>': 28,
 '<a href="#top"> Volver al Inicio </a>': 58,
 '<a href="#top"> Volver al comienzo </a>': 282,
 '<a href="#top"> Volver al comienzoo </a>': 1,
 '<a href="#top"> Volver al inicio </a>': 37,
 '<a href="#ttop"> Back to top </a>': 5}
['Back to top', 'Inicio de página', 'Volver al Inicio', 'Volver al comienzo', 
 'Volver al comienzoo', 'Volver al inicio']

zp_toptext = [tag for soup in tqdm(pcd_art_soup)
                 for tag in soup.find_all('p', class_='topOPage')]
# [str(tag) for tag in zp_toptext if tag.find('a', href=True, recursive=False) is None]
# ['<p class="topOPage"> </p>']
z = zp_toptext # 5572
z_str = sorted([str(tag) for tag in z])
z_text = sorted([tag.get_text('|', strip=True) for tag in z])
{ item: z_str.count(item) for item in sorted(set(z_str)) }
# { item: z_text.count(item) for item in sorted(set(z_text)) }

{'<p class="topOPage"> </p>': 1,
 '<p class="topOPage"> <a href="#"> Inicio de la página </a> </p>': 153,
 '<p class="topOPage"> <a href="#"> Top of Page </a> </p>': 5369,
 '<p class="topOPage"> <a href="#"> Volver al comienzo </a> </p>': 49}
['', 'Inicio de la página', 'Top of Page', 'Volver al comienzo']

# Conclusion 2: Add 'comienzo' and 'inicio' to 'top'

#%% 3. Find elements that have <a> child with 'top', 'comienzo', or 'inicio'
#      especially <p> and <span> elements

z_all_toptext = [tag for soup in tqdm(pcd_art_soup)
                 for tag in soup.find_all(True)
                 if tag.find('a', href=True, recursive=False) is not None and
                    tag.get_text() is not None and
                    re.search(r'\b(top|inicio|comienzoo?)\b', tag.get_text(), re.I)]
# 23587; 3542/3542 [00:35<00:00, 100.63it/s]

z = z_all_toptext # 23587
z_str = sorted([str(tag) for tag in z])
z_text = sorted([tag.get_text('|', strip=True) for tag in z])
{ item: z_str.count(item) for item in sorted(set(z_str)) }
# { item: z_text.count(item) for item in sorted(set(z_text)) }

with open('z_all_toptext-str.txt', 'w') as file_out:
   for z_s in sorted(set(z_str)):
      file_out.write(f'{z_s}\n')
with open('z_all_toptext-text.txt', 'w') as file_out:
   for z_t in sorted(set(z_text)):
      file_out.write(f'{z_t}\n')
del file_out, z_s, z_t

z_toptext = [tag for tag in z_all_toptext 
             if re.search(r'\b(top|inicio|comienzoo?)\b', tag.a.get_text(), re.I)
                and len(tag.a.get_text('|', strip=True)) < 20]
# 21644
# hard limit of length 20; reducing upper bound loses 'Inicio de la página'

# z_toptext_ = [tag for tag in tqdm(z_all_toptext)
#              if len(tag.a.get_text('|', strip=True)) < 20
#                 and re.search(r'\b(top|inicio|comienzoo?)\b', tag.a.get_text(), re.I) is None]
# 987: {'': 57, 'Authorâ€™s Corner': 2, 'Author’s Corner': 914, 'CrossRef': 7, 
#       'Figure 2': 1, 'Home': 2, 'PubMed': 3, 'Table 3': 1}

z = z_toptext # 21644
z_str = sorted([str(tag) for tag in z])
z_text = sorted([tag.get_text('|', strip=True) for tag in z])
{ item: z_str.count(item) for item in sorted(set(z_str)) }
# { item: z_text.count(item) for item in sorted(set(z_text)) }

{'<p align="left" class="psmall"> <a href="#"> Back to top </a> </p>': 1,
 '<p class="caption"> <a href="#top"> Volver al comienzo </a> </p>': 1,
 '<p class="caption"> [...] <a href="#"> Back to top </a> </p>': 1,
 '<p class="float-right"> <a href="#"> Top </a> </p>': 8063,
 '<p class="psmall" dir="ltr"> <a href="#"> Back to top </a> </p>': 9,
 '<p class="psmall" dir="ltr"> <a href="#top"> Volver al comienzo </a> </p>': 1,
 '<p class="psmall"> <a href="#"> Back to top </a> </p>': 7555,
 '<p class="psmall"> <a href="#"> Back to top </a> ] </p>': 2,
 '<p class="psmall"> <a href="#top"> Back to top </a> </p>': 3,
 '<p class="psmall"> <a href="#top"> Inicio de página </a> </p>': 28,
 '<p class="psmall"> <a href="#top"> Volver al Inicio </a> </p>': 58,
 '<p class="psmall"> <a href="#top"> Volver al comienzo </a> </p>': 279,
 '<p class="psmall"> <a href="#top"> Volver al comienzoo </a> </p>': 1,
 '<p class="psmall"> <a href="#top"> Volver al inicio </a> </p>': 37,
 '<p class="psmall"> <a href="#ttop"> Back to top </a> </p>': 5,
 '<p class="topOPage"> <a href="#"> Inicio de la página </a> </p>': 153,
 '<p class="topOPage"> <a href="#"> Top of Page </a> </p>': 5369,
 '<p class="topOPage"> <a href="#"> Volver al comienzo </a> </p>': 49,
 '<p> <a class="psmall" href="#top"> Back to top </a> </p>': 1,
 '<p> <a href="#"> Back to top </a> </p>': 11,
 '<p> <a href="#top"> Volver al comienzo </a> </p>': 1,
 '<span class="text-right d-block"> <span class="icon-angle-up"> <!-- --> </span> <a href="#"> Top </a> </span>': 8,
 '<span class="toTop"> <span class="icon-angle-up"> <!-- --> </span> <a class="tp-link-policy" href="#"> Top </a> </span>': 8}

['Abbreviation: ...|Back to top', 
 'Back to top', 'Back to top|]', 'Inicio de la página', 'Inicio de página', 
 'Top', 'Top of Page', 'Volver al Inicio', 'Volver al comienzo', 
 'Volver al comienzoo', 'Volver al inicio']

pcd_toptext = [
   dict(year=int(path[12:16]), markup=str(tag), content=tag.a.get_text('|', strip=True))
      for path, soup in tqdm(zip(pcd_art_frame.mirror_path, pcd_art_soup), total=3542)
      for tag in soup.find_all(True)
          if tag.find('a', href=True, recursive=False) is not None
             and tag.get_text() is not None
             and re.search(r'\b(top|inicio|comienzoo?)\b', tag.get_text(), re.I) 
             and len(tag.a.get_text('|', strip=True)) < 20]
# 22631
# 3542/3542 [00:57<00:00, 61.24it/s] 
# pd.DataFrame(pcd_toptext).to_excel('pcd_toptext.xlsx', engine='openpyxl')

pcd_toptext = [
   dict(year=int(path[12:16]), markup=str(every_tag), content=a_tag.get_text('|', strip=True))
      for path, soup in tqdm(zip(pcd_art_frame.mirror_path, pcd_art_soup), total=3542)
      for every_tag in soup.find_all(True)
      for a_tag in every_tag.find_all('a', href=True, recursive=False)
          if a_tag.get_text() is not None
             and len(a_tag.get_text('|', strip=True)) < 20
             and re.search(r'\b(top|inicio|comienzoo?)\b', a_tag.get_text(), re.I) 
             and len(str(every_tag)) < 120]
# length limits of 120 and 20 are empirically determined hard bounds
# 21643
# 3542/3542 [00:51<00:00, 69.18it/s]
pcd_toptext_dframe = pd.DataFrame(pcd_toptext)
# pcd_toptext_dframe.to_excel('pcd_toptext.xlsx', engine='openpyxl')

pd.crosstab(pcd_toptext_dframe.markup, pcd_toptext_dframe.year, margins=True).\
   iloc[[22, 5, 0, 8, 18, 13, 3, 9, 12, 7, 10, 6, 1, 4, 11, 17, 19, 15, 16, 14, 2, 20, 21], :]

pcd_topstrings = set([x['content'] for x in pcd_toptext])
# {'Back to top', 'Inicio de la página', 'Inicio de página', 'Top', 'Top of Page', 
#  'Volver al Inicio', 'Volver al comienzo', 'Volver al comienzoo', 'Volver al inicio'}
# in rder by language and approximate order of appearance:
pcd_topstrings = {'Back to top', 'Top of Page', 'Top', 
   'Inicio de página', 'Volver al inicio', 'Volver al Inicio', 
   'Volver al comienzo', 'Volver al comienzoo', 'Inicio de la página'}

# Conclusion 3: 9 anchor content strings delineate end-of-section

#%% Explore <h1>, <h2> and other elements that dellinete document segments 

x = pcd_art_soup[73] # 2019/19_0035.htm

def depth(object, ancestor_name = 'body'):
   for depth, ancestor in enumerate(object.parents, start=1):
      if ancestor.name == ancestor_name: break
   if ancestor.name is None: depth = None
   return depth

[(depth(tag), tag.name, tag.get_text('|', strip=True)) 
 for tag in x.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'])]

def children(object):
   child_names = [y.name for y in object.children if y.name is not None]
   uniq_names = sorted(set(child_names))
   # print(uniq_names)
   if len(uniq_names) > 0:
      children = '|'.join(['|'.join([child_name, str(child_names.count(child_name))])
                           for child_name in uniq_names])
   else: children = ''
   return children

children(x.find('table'))

[(depth(tag), tag.name, tag.get_text('|', strip=True), children(tag)) 
 for tag in x.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'])]

h_children = [{'path': path, 'name': tag.name, 'depth': depth(tag), 
               'string': tag.get_text('|', strip=True), 
               'attrs': '' if tag.name is None else str(tag.attrs),
               'children': children(tag)}
   for (path, soup) in tqdm(zip(pcd_art_frame.mirror_path, pcd_art_soup), total=3542)
   for tag in soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'])]
# 3542/3542 3542/3542 [00:52<00:00, 67.90it/s]
pd.DataFrame(h_children).to_excel('h_children.xlsx', engine='openpyxl')

# Which articles have a <main> element?
has_main = sorted([\
   path for path, soup in tqdm(zip(pcd_art_frame.mirror_path, pcd_art_soup))
        if soup.main is not None])
# Answer: exactly those published in 2015 or later
# Among those with a <main> element, what are the attributes of child <div>s?
has_main_sub = [(path, soup.main is not None, 
                        soup.find('div', class_='content') is not None,
                        soup.find('div', class_='content-fullwidth') is not None)
   for path, soup in tqdm(zip(pcd_art_frame.mirror_path, pcd_art_soup), total=3542)]
# pd.DataFrame(has_main_sub).to_clipboard()
# All 916 documents with <main> also have <div class="content content-fullwidth">.
 
# How many lack <main> but have <div class="main-inner">?
has_main_inner = [(path, soup.main is not None, 
                        soup.find('div', class_='main-inner') is not None)
   for path, soup in tqdm(zip(pcd_art_frame.mirror_path, pcd_art_soup), total=3542)]
# pd.DataFrame(has_main_inner).to_clipboard()
# All 928 documents with <div class="main-inner"> do not have <main>
# Is it feasible to remove <div class="onthispageChrono"> elements?
onthispageChrono = [
   dict(i=i, j=j, k=k, parent_name=elem.parent.name, name_=elem.name, attrs=elem.attrs, depth=depth(elem))
   for i, soup in tqdm(enumerate(pcd_art_soup), total=3542)
   for j, chrono in enumerate(soup.find_all('div', class_='onthispageChrono'))
   for k, elem in enumerate(chrono.descendants)
      if isinstance(elem, Tag)]
# pd.DataFrame(onthispageChrono).to_excel('onthispageChrono.xlsx', engine='openpyxl')

onthispageChrono_text = [
   dict(i=i, j=j, text=chrono.get_text('|', strip=True), depth=depth(chrono))
   for i, soup in tqdm(enumerate(pcd_art_soup), total=3542)
   for j, chrono in enumerate(soup.find_all('div', class_='onthispageChrono'))]
# pd.DataFrame(onthispageChrono_text).to_excel('onthispageChrono_text.xlsx', engine='openpyxl')

# Which articles have an element <td width="80%">?
has_width_80 = sorted([\
   path for path, soup in tqdm(zip(pcd_art_frame.mirror_path, pcd_art_soup), total=3542)
        if soup.find('td', width="80%") is not None])
# Answer: exactly those published through 2011
has_width_80_sub = [(path, soup.find('td', width="80%") is not None)
   for path, soup in tqdm(zip(pcd_art_frame.mirror_path, pcd_art_soup), total=3542)]
# pd.DataFrame(has_width_80_sub).to_clipboard()
# The remaining 1698 documents have <td width="80%">
# Among these 1698 documents, which <td width=“80%”> contain content of interest?
has_width_80_n = [\
   dict(year=int(path[12:16]), n_width_80=len(soup.find_all('td', width="80%")))
      for path, soup in tqdm(zip(pcd_art_frame.mirror_path, pcd_art_soup), total=3542)]
# pd.DataFrame(has_width_80_n).to_clipboard()

has_width_80_children = [\
   dict(path=path, year=int(path[12:16]), i=i, j=j, k=k, n_width_80=len(td), child_name=child.name,
        child_attrs=child.attrs)
      for i, (path, soup) in tqdm(enumerate(zip(pcd_art_frame.mirror_path, pcd_art_soup)), total=3542)
      for j, td in enumerate(soup.find_all('td', width="80%"))
      for k, child in enumerate(td.children)
         if td.name is not None and child.name is not None]
# pd.DataFrame(has_width_80_children).to_clipboard()

has_style_width_80 = [(path, soup.find('td', style=re.compile('80%')) is not None)
   for path, soup in tqdm(zip(pcd_art_frame.mirror_path, pcd_art_soup), total=3542)]
# occurs with 3 files
# /pcd/issues/2011/nov/11_0062.htm
# /pcd/issues/2011/nov/10_0191.htm
# /pcd/issues/2011/nov/10_0276.htm
has_style_width_80 = [(i, path)
   for i, (path, soup) in tqdm(enumerate(zip(pcd_art_frame.mirror_path, pcd_art_soup)), total=3542)
      if soup.find('td', style=re.compile('80%')) is not None]
# [(1514, '/pcd/issues/2011/nov/11_0062.htm'),
#  (1521, '/pcd/issues/2011/nov/10_0191.htm'),
#  (1522, '/pcd/issues/2011/nov/10_0276.htm')]

# list of 12:
[(path, td.attrs) for i, path in has_style_width_80
    for td in pcd_art_soup[i].find_all('td')
    if (td.has_attr('width') and re.search('80%', td['width'])) or
       (td.has_attr('style') and re.search('80%', td['style']))]
# list of 3 lists of 4 each:
[ [(path, td.attrs) for td in pcd_art_soup[i].find_all('td')
     if (td.has_attr('width') and re.search('80%', td['width'])) or
       (td.has_attr('style') and re.search('80%', td['style']))]
    for i, path in has_style_width_80]
# in all 3 cases, <td style="width: 80%"> appears once, 
# followed by <td width="80%" 4 times>

[dict(i=i, path=path, k=k, child_name=child.name, child_attrs=child.attrs) 
    for i, path in has_style_width_80
    for k, child in enumerate(pcd_art_soup[i].find('td', style='width: 80%').children)
         if child.name is not None]
# same structure as many <td width="80%">: <img>, <br>, <table>, 
#    <p align="right" class="psmall"> (with vol, iss, date), 
#    <div class="syndicate"> (with type, title),
#    <form name="eMailer">,
#    <div class="syndicate">

table_width_soup = [(i, soup)
   for i, soup in tqdm(enumerate(pcd_art_soup), total=3542)
   if soup.find('div', class_=['content-fullwidth', 'main-inner']) is None]
# 3542/3542 [00:15<00:00, 236.02it/s]
table_width_info = [
   dict(i=i, j=j, elem=elem.name, depth=depth(elem), 
        width=no(elem.get('width')), style=no(elem.get('style')))
   for i, soup in tqdm(table_width_soup)
   for j, elem in enumerate(soup.find_all(name=['table', 'td']))
      if elem.has_attr('width') or elem.has_attr('style')]
# pd.DataFrame(table_width_info).to_excel('table_width_info.xlsx', engine='openpyxl')


# Summary
# 2004-2011: 1698 <table><tr><td width=“80%”>
# 2012-2014:  928 <div class=“main-inner”>
# 2015-2020:  916 <main><div class=“content”>

# Which articles have a <div class="syndicate"> element?
has_syndicate = sorted([\
   path for path, soup in tqdm(zip(pcd_art_frame.mirror_path, pcd_art_soup), total=3542)
        if soup.find('div', class_="syndicate") is not None])
# Answer: mostly those published in July 2010 or later
#    Some exceptions: through May 2010, 6/1279 have this element, 14/2242 do not
   
# How can datelines be identified?
from bs4 import Comment, Tag

z = [dateline for soup in tqdm(pcd_art_soup)
     for volume in soup.find_all('p', string=re.compile('Volume'))
     for dateline in volume]
# 1572

# For the following markup, soup.string returns None because of the comment
# <p align="right" class="psmall">
#  <!-- Write the date of the issue here -->
#  Volume 1: No. 1, January 2004
# </p>
# So find_all('p', string=...) doesn't catch those

z = [(path, dateline.get_text('|', strip=True), dateline.get('class')) 
     for path, soup in tqdm(zip(pcd_art_frame.mirror_path, pcd_art_soup), total=3542)
     for dateline in soup.find_all('p')
        if dateline.strings is not None and re.search('Volume', dateline.get_text())]
# 3277
len(set([path for path, text, class_ in z]))
# This includes 6 where "Volume" is in longer text
# 3270 that look like datelines, max string length 65
# Is there a better way to capture those 3270? 
# What about the other 272? All exceptions published in 2012, but not all 2012 
# publications are excepted. There are 8 errata, and the rest have 
# <div class="syndicate">.

# Proposed rule: Use <div class="syndicate">, if it is available (in which case, 
# check also for dateline in absence of <div class="dateline">).
# When <div class="syndicate"> is not available, look for <td width="80%">.
# When both are lacking, use <div class="main-inner">

dateline.name is not None and 

z = [psmall.get_text('|', strip=True) for soup in tqdm(pcd_art_soup)
     for all_psmall in soup.find_all('p', class_='psmall')
     for psmall in all_psmall 
        if isinstance(psmall, Tag) and psmall.get_text() is not None 
           and re.search('\bVolume', psmall.get_text())]
dateline1 = [ dateline.get_text('|', strip=True) 
             for dateline in soup.find('p', class_='psmall')

   path for path, soup in tqdm(zip(pcd_art_frame.mirror_path, pcd_art_soup), total=3542)
        if soup.find('div', class_="syndicate") is not None]

isinstance(tag.next_element, Comment)
# <p <div class="dateline">

# Elements <h1>, ..., <h6> and their descendants
h_all = [dict(path=path, year=int(path[12:16]),
              h_name=h_elem.name, h_depth=depth(h_elem), h_str=str(h_elem), 
              h_text=h_elem.get_text('|', strip=True),
              h_attrs=str(h_elem.attrs))
         for path, soup in tqdm(zip(pcd_art_frame.mirror_path, pcd_art_soup), total=3542)
         for h_elem in soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'])]
pd.DataFrame(h_all).to_excel('h_all.xlsx', engine='openpyxl')

h_descendants = [dict(path=path, year=int(path[12:16]),
              h_name=h_elem.name, h_depth=depth(h_elem), h_attrs=str(h_elem.attrs), 
              h_desc_name=h_desc.name, h_desc_depth=depth(h_desc), h_desc_attrs=str(h_desc.attrs), 
              h_str=str(h_elem), h_desc_str=str(h_desc),
              h_text=h_elem.get_text('|', strip=True),
              h_desc_text = '' if h_desc.get_text() is None else h_desc.get_text('|', strip=True))
         for path, soup in tqdm(zip(pcd_art_frame.mirror_path, pcd_art_soup), total=3542)
         for h_elem in soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'])
         for h_desc in h_elem.descendants
             if h_desc.name is not None]
# 3542/3542 [01:05<00:00, 54.17it/s]
pd.DataFrame(h_descendants).to_excel('h_descendants.xlsx', engine='openpyxl')

# conclusion: every <h[1-6]> is of interest; 
# descendant <a> or <span> could be, as well

#%% Assemble forgoing information into comprehensive inventory
#      content containers: class="content" OR "main-inner", td width="80%"
#      within content containers:
#         <div class="syndicate">, <div class="dateline">
#         content: p, h[1-6], r'\bVolumen?\b'
#         extraneous: Comment, Script, Stylesheet; eMailer, onthispageChrono, tp-on-this-page
#         delimiters: p/span with short content that includes top, comienzo, inicio

pcd_year = [int(path[12:16]) for path in pcd_art_frame.mirror_path]

# Inventory candidate containers
def pcd_containers(soup):
   # soup_main = soup.main
   main = len(soup.find_all('main'))
   div_content = len(soup.find_all('div', class_='content'))
   div_content_fw = len(soup.find_all('div', class_='content-fullwidth'))
   div_main_inner = len(soup.find_all('div', class_='main-inner'))
   td_w_80 = len(soup.find_all('td', width='80%'))
   td_w_70 = len(soup.find_all('td', width='70%'))
   td_w_80_s = len(soup.find_all('td', style='width: 80%'))
   div_syndicate = len(soup.find_all('div', class_='syndicate'))
   return dict(main=main, div_content=div_content, 
      div_content_fw=div_content_fw, div_main_inner=div_main_inner, 
      td_w_80=td_w_80, td_w_80_s=td_w_80_s, td_w_70=td_w_70, 
      div_syndicate=div_syndicate)

pcd_container_freq = [pcd_containers(soup) for soup in tqdm(pcd_art_soup)]
# 3542/3542 [02:00<00:00, 29.38it/s]
pd.concat([pd.Series(pcd_art_frame.mirror_path, name='path'), 
           pd.Series(pcd_year, name='year'), 
           pd.DataFrame(pcd_container_freq)],
          axis=1).to_excel('pcd_container_freq.xlsx', engine='openpyxl')
# [3542 rows x 9 columns]

# Anchor text that links back to top of page, thus delimits document segments
# Cast as a set for efficient calculation of membership
pcd_topstrings = {'Back to top', 'Top', 'Top of Page', 
   'Inicio de la página', 'Inicio de página', 'Volver al Inicio', 
   'Volver al comienzo', 'Volver al comienzoo', 'Volver al inicio'}

def td_w_80_fn(tag):
   if tag.name == 'td':
      td_width = tag.has_attr('width') and tag['width'] in {'80%', '70%'}
      td_style = tag.has_attr('style') and tag['style'] == 'width: 80%'
      td_w_80 = td_width or td_style
   else:
      td_w_80 = False
   return td_w_80

#%% Split into 4 phases and process each separately

# Start over with HTML, sorting filenames for convenience

# x = ['/pcd/issues/2007/apr/06_0105_es.htm', '/pcd/issues/2019/19_0199.htm']
# re.search(r'/(?P<year>\d{4})/(?:[a-z]{3}/)?(?P<file>[\w-]+?).htm', x[1]).groupdict()
# '/pcd/issues/2007/apr/06_0105_es.htm' # {'year': '2007', 'file': '06_0105_es'}
# '/pcd/issues/2019/19_0199.htm'        # {'year': '2019', 'file': '19_0199'}

# Sort in descending order by year of publication and article filename
# This is close to but not exactly the same as reverse of order published
year_file_re = re.compile(r'''
   /pcd/issues/(?P<year>\d{4})/ # match year
   (?:[a-z]{3}/)?               # match and discard month, if it exists
   (?P<file>[\w-]+?).htm        # match filename root
   ''', re.VERBOSE)
sorted_paths = sorted(pcd_art_frame.mirror_path, 
       key=lambda path: re.search(year_file_re, path).groups(), 
       reverse=True)

# Epochs fall out (roughly) as
#    pcd1: 2004-2010/05; pcd2: 2010/07-2011; pcd3: 2012-2014; pcd4: 2015-2020

pcd1_html = list(); pcd2_html = list(); pcd3_html = list(); pcd4_html = list()
for path in tqdm(sorted_paths):
   html = html_reduce_space_u(read_uni_html(PCD_BASE_PATH_u3 + path))
   soup = BeautifulSoup(html, 'lxml')

   if soup.find('div', class_='content-fullwidth') is not None:
      pcd4_html.append(dict(path=path, html=html))
   elif soup.find('div', class_='main-inner') is not None:
      pcd3_html.append(dict(path=path, html=html))
   elif soup.find('div', class_='syndicate') is not None:
      pcd2_html.append(dict(path=path, html=html))
   else:
      pcd1_html.append(dict(path=path, html=html))
del year_file_re, path, html, soup
# 3542/3542 [02:42<00:00, 21.76it/s]
# [len(x) for x in [pcd1_html, pcd2_html, pcd3_html, pcd4_html]]
# [1284, 414, 928, 916]
# pickle.dump(pcd1_html, open('pcd1_html.pkl', 'wb'))
# pickle.dump(pcd2_html, open('pcd2_html.pkl', 'wb'))
# pickle.dump(pcd3_html, open('pcd3_html.pkl', 'wb'))
# pickle.dump(pcd4_html, open('pcd4_html.pkl', 'wb'))
# pcd1_html = pickle.load(open('pcd1_html.pkl', 'rb'))
# pcd2_html = pickle.load(open('pcd2_html.pkl', 'rb'))
# pcd3_html = pickle.load(open('pcd3_html.pkl', 'rb'))
# pcd4_html = pickle.load(open('pcd4_html.pkl', 'rb'))

# Storing all soup works for PCD because of its size
pcd1_soup = [BeautifulSoup(html['html'], 'lxml') for html in tqdm(pcd1_html)]
# 1284/1284 [00:27<00:00, 47.05it/s]
pcd2_soup = [BeautifulSoup(html['html'], 'lxml') for html in tqdm(pcd2_html)]
# 414/414 [00:10<00:00, 38.52it/s]
pcd3_soup = [BeautifulSoup(html['html'], 'lxml') for html in tqdm(pcd3_html)]
# 928/928 [00:24<00:00, 37.48it/s]
pcd4_soup = [BeautifulSoup(html['html'], 'lxml') for html in tqdm(pcd4_html)]
# 916/916 [00:56<00:00, 16.08it/s]

# Epochs 3 and 4 appear to be the most straightforward to parse
pcd4_html[0]['path'] # '/pcd/issues/2020/19_0391.htm'
with open('2020-19_0391-pretty.htm', 'w') as file_out:
   file_out.write(html_prettify_u(pcd4_html[0]['html']))
x = pcd4_soup[0].find('div', class_='syndicate')
for j in range(30):
   print(f'{j}: \'{len(str(x))}\'')
   if x is None: break
   x = x.next_sibling

print([z for z in x.find_all_next(string=True) if z != ' '])

y = [len(soup.find_all('div', class_='col-md-4')) for soup in tqdm(pcd4_soup)]
# 153 have 1; 763 have 0
y = [dict(n=len(soup.find_all('div', class_='col-md-4')),
      dateline=soup.find('div', class_='dateline').div.p.get_text(strip=True))
     for soup in tqdm(pcd4_soup)]
pd.DataFrame(y).to_clipboard()

pcd4_html[179]['path'] # '/pcd/issues/2019/18_0288.htm'
with open('pretty/2019-18_0288-pretty.htm', 'w') as file_out:
   file_out.write(html_prettify_u(pcd4_html[179]['html']))

y = [dict(i=i, j=j, k=k, text=div_synd_div.get_text('|', strip=True),
          parent_name=div_synd_div.parent.name, attrs=div_synd_div.attrs)
     for i, soup in tqdm(enumerate(pcd4_soup), total=916)
     for j, div_synd in enumerate(soup.find_all('div', class_='syndicate'))
     for k, div_synd_div in enumerate(div_synd.find_all('div'))
     if len(div_synd.find_all('div')) > 0]

div_bg_secondary = [soup.find('div', class_='bg-secondary').get_text('|', strip=True)
   for soup in tqdm(pcd4_soup)
   if soup.find('div', class_='bg-secondary') is not None]
# set(div_bg_secondary) # {'Summary'}
# div_bg_secondary.count('Summary') # 153

div_card_text = [(i, card_text)
   for i, soup in tqdm(enumerate(pcd4_soup), total=916)
   for card_text in soup.find_all('div', class_='card-text')
   # for p in card_text.find_all('p')
   # if card_text.p.string is not None and card_text.p.string[:4] == 'What']
   if card_text.p.get_text() is not None
   and card_text.p.get_text(strip=True)[:4] == 'What'
   and len(card_text) != 13]
# 66, 99
card_text_0 = '|'.join([p.get_text(strip=True) 
                               for i, p in enumerate(div_card_text[0][1].find_all('p'))
                               if i in (0, 2, 4)])
card_text_0 = card_text_0.replace('What is already known on this topic?', '', 1)
card_text_1 = '|'.join([p.get_text(strip=True) 
                               for i, p in enumerate(div_card_text[1][1].find_all('p'))
                               if i in (1, 3, 5)])

y = [len(soup.find_all('div', class_='card-text')) for soup in tqdm(pcd4_soup)]
[y.count(x) for x in range(4)] # [763, 147, 0, 6]

y = [soup.find_all('div', class_='card-text') for soup in tqdm(pcd4_soup)
     if len(soup.find_all('div', class_='card-text')) > 1]

def div_card_text_fn(soup):
   div_card_text = soup.find_all('div', class_='card-text')
   div_len = len(div_card_text)

   # Length is 0, 1, or 3; extract usable div element
   if div_len == 3:
      div_card_text = div_card_text[2]
   elif div_len == 1:
      div_card_text = div_card_text[0]

   if div_len > 0:
      card_text_p = div_card_text.find_all('p')
      # Length is 5, 6, or 7; kludge lengths 5 and 7
      if len(card_text_p) == 5:
         card_text = '|'.join([p.get_text(strip=True) 
                               for i, p in enumerate(card_text_p)
                               if i in (0, 2, 4)])
         card_text = card_text.replace('What is already known on this topic?', '')
      elif len(card_text_p) >= 6:
         card_text = '|'.join([p.get_text(strip=True) 
                               for i, p in enumerate(card_text_p)
                               if i in (1, 3, 5)])
   else: card_text = ''
   return card_text

div_card_text_fn(pcd4_soup[66])
div_card_text_fn(pcd4_soup[99])
[(x, div_card_text_fn(pcd4_soup[x])) for x in range(66, 99+11, 11)]

card_texts = [div_card_text_fn(soup) for soup in tqdm(pcd4_soup)]
len([x for x in card_texts if x != '']) # 153

pcd4_soup[415].find('div', class_='dateline').p.string
# ' EDITOR IN CHIEFÂ€™S COLUMN â€” Volume 14 â€” February 2, 2017 '
pcd4_soup[734].find('div', class_='dateline').p.string
# ' EDITORÂ€™S CHOICE â€” Volume 12 â€” June 25, 2015 '

x = str(pcd4_soup[415].find('div', class_='dateline').p.string)
x.replace('Â€™', "'").replace('â€”', '—') # em dash \u2014
x = str(pcd4_soup[734].find('div', class_='dateline').p.string)
x.replace('Â€™', "'").replace('â€”', '—') # em dash \u2014

x.replace('Â€™', "'").replace('â€”', '—').strip().split(' — ')
dict(zip(['type', 'volume', 'date'], \
    x.replace('Â€™', "'").replace('â€”', '—').strip().split(' — ')))

def process_dateline(soup):
   div_dateline = soup.find('div', class_='dateline')
   if div_dateline.get_text() is None:
      dateline = dict(type='', volume=None, date=None)
   else:
      dateline_text = div_dateline.get_text(strip=True) 
      if 'â€”' in dateline_text:
         dateline_text = dateline_text.replace('Â€™', "'").replace('â€”', '—')
      dateline = dateline_text.split(' — ')
      dateline = dict(type=dateline[0], 
         volume=int(re.search(r'Volume (?P<vol>\d{1,2})', dateline[1]).group('vol')),
         date=parse(dateline[2]).strftime('%Y-%m-%d'))

   return dateline

process_dateline(pcd4_soup[415])
process_dateline(pcd4_soup[734])
datelines = [process_dateline(soup) for soup in tqdm(pcd4_soup)]

x = [len(soup.find_all('h1')) for soup in tqdm(pcd4_soup)] # all 916
x = [soup.find('h1').attrs for soup in tqdm(pcd4_soup)] # all "{'id': 'content'}"

h1_content = [soup.find('h1').get_text('|', strip=True) for soup in tqdm(pcd4_soup)]

x = [len(soup.find_all('h4')) for soup in tqdm(pcd4_soup)] # all 916
set(x) # 1-17, mode at 2; 1st appears to be authors
[(y, x.count(y)) for y in range(1, 18)]
# [(1, 36), (2, 781), (3, 30), (4, 12), (5, 17), (6, 16), (7, 12), (8, 3), 
#  (9, 3), (10, 0), (11, 1), (12, 1), (13, 1), (14, 2), (15, 0), (16, 0), (17, 1)]

x = [soup.find('h4').get_text('|', strip=True).\
     replace('(|View author affiliations|)','')
     for soup in tqdm(pcd4_soup)]
# begins and ends with |, with mix of digit, space, comma, hyphen
xx = [re.sub(r'\|[|\d, -]+\|', '', y) for y in x]
# |2|,3|,4|,5|; |1|, 2|; |1-4|

# What's up with 'Exit Notification/Disclaimer Policy'?
y = [soup.find_all('h4')
     for soup in tqdm(pcd4_soup)
     if soup.find('h4').get_text('|', strip=True) == 'Exit Notification/Disclaimer Policy']
# occurs 36 times; only <h4> when it does
y = [soup.find('h1')
     for soup in tqdm(pcd4_soup)
     if soup.find('h4').get_text('|', strip=True) == 'Exit Notification/Disclaimer Policy']
# errata, addenda, retractions

# If the first <h4> is authors and 2 <h4>s is the mode, what's the 2nd <h4>?
y = [str(soup.find_all('h4')[1])
     for soup in tqdm(pcd4_soup)
     if len(soup.find_all('h4')) > 1]
# mostly "<h4 class=""modal-title"" id=""extModalTitle""> Exit Notification/Disclaimer Policy </h4>"

z = [dict(soup_num=i, child_num=j, child_name=child.name, 
        child_attrs=('' if child.name is None else str(child.attrs)),
        grandchildren=(0 if child.name is None else len(child.contents)))
      for i, soup in tqdm(enumerate(pcd4_soup), total=916)
      for j, child in enumerate(soup.find('div', class_='content-fullwidth').children)]

# Direct children of <div class="content-fullwidth">
# Children 1, 2, 4, and 5 below appear in all 916; child appears in only 39
# <div class="syndicate">
# <div class="row col-12 dateline">
# <div class="language-link mb-3 fs0875">
# <div class="mb-3 card tp-related-pages fourth-level-nav d-none">
# <div class="syndicate">

z_lank_link = [(i, str(soup.find('div', class_='language-link'))) 
   for i, soup in tqdm(enumerate(pcd4_soup), total=916)
   if soup.find('div', class_='language-link') is not None]
# [ 10,  50, 375, 412, 420, 421, 422, 428, 434, 436, 437, 440, 444, 445, 447, 
#  449, 455, 463, 470, 472, 480, 483, 486, 512, 515, 536, 598, 649, 687, 769, 
#  834, 840, 870, 871, 879, 880, 885, 886, 887]
# for child 3, content (if any) is noninformative
#    class="language-link" is unique to these 39; other classes are not

block_names = ('h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'p', 'ul', 'ol', 'blockquote')
z = [dict(soup_num=i, child_num=j, child_name=child.name, 
        grchildren=(0 if child.name is None else len(child.contents)),
        grchild_num=k, grchild_name=grchild.name, grchild_depth=depth(grchild),
        len=len('' if grchild.name is None else str(grchild.attrs)),
        grchild_attrs=('' if grchild.name is None else str(grchild.attrs))
        )
      for i, soup in tqdm(enumerate(pcd4_soup), total=916)
      for j, child in enumerate(soup.find('div', class_='content-fullwidth').\
                         find_all('div', class_=('syndicate', 'dateline'), recursive=False))
      for k, grchild in enumerate(child.find_all(name=True, recursive=False))]
pd.DataFrame(z).to_clipboard()

z_div = [dict(soup_num=i, child_num=j, child_name=child.name, 
        grchildren=(0 if child.name is None else len(child.contents)),
        grchild_num=k, grchild_name=grchild.name, grchild_depth=depth(grchild),
        len=len('' if grchild.name is None else str(grchild.attrs)),
        grchild_attrs=('' if grchild.name is None else str(grchild.attrs)),
        grchild_str=repr(grchild)
        )
      for i, soup in tqdm(enumerate(pcd4_soup), total=916)
      for j, child in enumerate(soup.find('div', class_='content-fullwidth').\
                         find_all('div', class_=('syndicate', 'dateline'), recursive=False))
      for k, grchild in enumerate(child.find_all(name='div', recursive=False))]
pd.DataFrame(z_div).to_clipboard()

# In 2 instances, there is a 3rd 'syndicate', but it is not a direct child
[pcd4_html[x]['path'] for x in (270, 288)]
['/pcd/issues/2018/17_0535.htm', '/pcd/issues/2018/17_0434.htm']
list(pcd4_soup[288].find('div', class_='content-fullwidth').find_all('div', class_=('syndicate', 'dateline')))[3]

[pcd4_html[x]['path'] for x in (439, 441, 447, 53)]
[pcd4_html[x]['path'] for x in (447,441,858,769,574,439,477,415)]

# CME
y = [(i, soup.find('h1').get_text(strip=True))
     for i, soup in tqdm(enumerate(pcd4_soup), total=916)
     if soup.find('div', class_=('medscapeCME', 'cme')) is not None]
y = [(i, soup.find('div', class_='dateline').get_text(strip=True))
     for i, soup in tqdm(enumerate(pcd4_soup), total=916)
     if soup.find('div', class_=('medscapeCME', 'cme')) is not None]

[pcd4_html[x]['path'] for x in (73,135,544,852,858,894,901,904)]

p_smallgrey = [len(soup.find_all('p', class_='smallgrey')) 
               for soup in tqdm(pcd4_soup)]
# occurs 2x in 914; 3x in 2

p_smallgrey = [[str(p)[:60] for p in soup.find_all('p', class_='smallgrey')]
               for soup in tqdm(pcd4_soup)]
[p for p in p_smallgrey if len(p)==3]
[['<p class="smallgrey"> <i> Suggested citation for this articl',
  '<p class="smallgrey"> The opinions expressed by authors cont',
  '<p class="smallgrey"> The opinions expressed by authors cont'],
 ['<p class="smallgrey"> <em> Suggested citation for this artic',
  '<p class="smallgrey"> <em> Retracted by authors in: </em> Re',
  '<p class="smallgrey"> The opinions expressed by authors cont']]

[p[0] for p in p_smallgrey].count('<p class="smallgrey"> <em> Suggested citation for this artic')
# 117
[p[0] for p in p_smallgrey].count('<p class="smallgrey"> <i> Suggested citation for this articl')
# 799
[p[1] for p in p_smallgrey].count('<p class="smallgrey"> The opinions expressed by authors cont')
# 915, plus the 1 above where it's p[2]

p_smallgrey = [(i, j, k, p_smallgrey.get_text('|', strip=True)[:37])
               for i, soup in tqdm(enumerate(pcd4_soup), total=916)
               for j, div_synd in enumerate(soup.find_all('div', class_='syndicate'))
               for k, p_smallgrey in enumerate(div_synd.find_all('p', class_='smallgrey'))]
pd.DataFrame(p_smallgrey).to_clipboard()

p_peervwd = [(html['path'], '' if peer_text.get_text() is None else peer_text.get_text(strip=True))
   for html, soup in tqdm(zip(pcd4_html, pcd4_soup), total=916)
   for peer_text in soup.find_all('p', class_='peerreviewed') ]
pd.DataFrame(p_peervwd).to_clipboard()

from bs4 import SoupStrainer
cont_fw_strain = SoupStrainer('div', class_='content-fullwidth')

# check documents with >2 <div class="syndicate">
[str(synd)[:200]
 for soup in tqdm(pcd4_soup)
 for synd in soup.find('div', class_='content-fullwidth').find_all('div', class_='syndicate')
 if len(soup.find('div', class_='content-fullwidth').find_all('div', class_='syndicate'))>2]
# 2 cases, both where 3rd is JPG
# [<div class="syndicate"> <p class="text-center"> <a href="/pcd/issues/2018/images/17_0535_01.jpg" target="new"> High-resolution JPG for print <span class="sr-only"> image icon </span> <span aria-hidden="true" class="fi cdc-icon-image x16 fill-image"> </span> <span class="file-details"> </span> </a> </p> </div>,
#  <div class="syndicate"> <p class="text-center"> <a href="/pcd/issues/2018/images/17_0434_large.gif" target="new"> High-resolution JPG for print <span class="sr-only"> image icon </span> <span aria-hidden="true" class="fi cdc-icon-image x16 fill-image"> </span> <span class="file-details"> </span> </a> </p> </div>]

def preprocess_pcd4(soup):
   soup = copy.copy(soup.find('div', class_='content-fullwidth'))
   # 3 child <div> elements of interest: syndicate, dateline, syndicate
   # dateline
   dateline = process_dateline(soup)
   # syndicate <div>s
   div_synd1, div_synd2 = soup.find_all('div', class_='syndicate')[:2]
   title = div_synd1.h1.get_text('|', strip=True) # syndicate 1
   summary = div_card_text_fn(div_synd2) # syndicate 2
   
   # process the rest of syndicate 2
   syn_children = div_synd2.find_all(name=True, recursive=False)
   syn_child_attrs = [(x.name, '|'.join(x.get_attribute_list('class')) 
                         if x.has_attr('class') else '',
                         x.get_text('|', strip=True)[:40])
                         for x in syn_children]

   syn_child_names = [':'.join(x[:2]).rstrip(':') for x in syn_child_attrs]
   # Run-length encoding: initialize and iterate to completion
   elems = [[syn_child_names[0], 1]]
   for name in syn_child_names[1:]:
      if name == elems[-1][0]:
         elems[-1][1] += 1
      else:
         elems += [[name, 1]]
   suite = '+'.join([elem[0] if elem[1]==1 else elem[0]+'*'+str(elem[1]) for elem in elems])
   
   return dict(title=title, **dateline, summary=summary,
               # syn_attrs=syn_child_attrs, 
               suite=suite)

preprocess_pcd4(pcd4_soup[0])

suites = [preprocess_pcd4(soup)['suite'] for soup in tqdm(pcd4_soup)]

x = pd.DataFrame(dict(path=[html['path'] for html in pcd4_html], suite=suites))

#%%

# pcd4_html = pickle.load(open('pcd4_html.pkl', 'rb'))
pcd4_soup = [BeautifulSoup(html['html'], 'lxml') for html in tqdm(pcd4_html)]
# 916/916 [00:56<00:00, 16.08it/s]

## review <div>s that typically appear on first part of page

# div:col-md-4|float-right|cr as ('col-md-4', 'cr')
x = [str(div.attrs) for soup in tqdm(pcd4_soup) 
     for div in soup.find('div', class_='content-fullwidth').\
        find_all('div', class_='syndicate', recursive=False)[1].\
        find_all('div', class_=('col-md-4', 'cr'), recursive=False)]
# set(x) # {"{'class': ['col-md-4', 'float-right', 'cr']}"}
#    searching on only 'col-md-4' or only on 'cr' yields the same results
#    including 'float-right' does not
# 153 occurrences of syndicate child <div class="col-md-4 float-right cr">
#    these occurrences are unique within each file where they occur
#    all "Summary" -- keep

# div:card|tp-on-this-page|mb-3|w-33|float-right|d-none|d-lg-block
# ('card', 'tp-on-this-page', 'mb-3', 'w-33', 'float-right', 'd-none', 'd-lg-block')
x = [str(div.attrs) for soup in tqdm(pcd4_soup) 
     for div in soup.find('div', class_='content-fullwidth').\
        find_all('div', class_='syndicate', recursive=False)[1].\
        find_all('div', class_=('tp-on-this-page', 'w-33', 'd-none', 'd-lg-block'), 
                 recursive=False)]
# set(x) # {"{'class': ['card', 'tp-on-this-page', 'mb-3', 'w-33', 'float-right', 'd-none', 'd-lg-block']}"}
#    or searcing on only 'tp-on-this-page', 'w-33', 'd-none', or 'd-lg-block'
#    these 4 classes occur exclusively together within syndicate
# 877 occurrences of syndicate child; unique in file when it exists
#    <div class="card tp-on-this-page mb-3 w-33 float-right d-none d-lg-block">
# one get_text: 'On This Page|Abstract|Introduction|Methods|Results|Discussion|Acknowledgments|Author Information|References|Tables'

# div:row|bg-gray-l1|medscapeCME, div:medscapeCME, div:cme
x = [str(div.attrs) for soup in tqdm(pcd4_soup) 
     for div in soup.find('div', class_='content-fullwidth').\
        find_all('div', class_='syndicate', recursive=False)[1].\
        find_all('div', class_=('cme', 'medscapeCME'), recursive=False)]
        # find_all('div', class_=re.compile('cme', re.IGNORECASE), recursive=False)]
# 46 occurrences; set(x) returns 3 unique values; unique in file when they exist
# <div class="cme"> 17 times
# <div class="medscapeCME"> 23 times
# <div class="row bg-gray-l1 medscapeCME"> 6 times

# div:edNote
x = [str(div.attrs) for soup in tqdm(pcd4_soup) 
     for div in soup.find('div', class_='content-fullwidth').\
        find_all('div', class_='syndicate', recursive=False)[1].\
        find_all('div', class_=('edNote'), recursive=False)]
# 4 occurrences; set(x) returns 1 unique value; unique in file when it exists
# <div class="edNote">
# winners of '2017 Student Research Paper Contest'

# div:d-block|text-center|pt-2
x = [str(div.attrs) for soup in tqdm(pcd4_soup) 
     for div in soup.find('div', class_='content-fullwidth').\
        find_all('div', class_='syndicate', recursive=False)[1].\
        # find_all('div', class_=('d-block', 'text-center', 'pt-2'), 
        find_all('div', class_=('pt-2',), recursive=False)]
# 373 for ('d-block', 'text-center', 'pt-2'); 372 for ('pt-2',)
# {y: x.count(y) for y in sorted(set(x))}
# "{'class': ['d-block', 'text-center', 'pt-2']}"       366 times
# "{'class': ['d-block', 'text-center']}"                 1 time - omit
# "{'class': ['float-md-left', 'cl', 'mr-md-3', 'pt-2']}" 6 times
x = [len(soup.find('div', class_='content-fullwidth').\
        find_all('div', class_='syndicate', recursive=False)[1].\
        find_all('div', class_=('pt-2',), recursive=False))
     for soup in tqdm(pcd4_soup)]
# {y: x.count(y) for y in sorted(set(x))}
# occurs 0-4 times per document: {0: 665, 1: 148, 2: 88, 3: 12, 4: 3}
# 372 strings in 251 files
x = [soup.find('div', class_='content-fullwidth').\
        find_all('div', class_='syndicate', recursive=False)[1].\
        find_all('div', class_=('pt-2',), recursive=False)
     for soup in tqdm(pcd4_soup)]
# [[z.img['alt'].strip() for z in y] for y in x if len(y) > 2] # 15 examples
# [z.img['alt'].strip() for y in x for z in y if len(y) > 2] # single list of 48

# alt_exclude is from below; exclusions remove only 3 instances
xx = [[z.img['alt'].strip(' \n') for z in y 
       if z.img['alt'].strip(' \n') not in alt_exclude] 
      for y in x]
# { y: [len(z) for z in xx].count(y) for y in range(5) }
# {0: 668, 1: 145, 2: 88, 3: 12, 4: 3}

# harvest <img alt=""> attribute text for these
# <img> alt attribute text for images under <div class="pt-2">
xx = [div.find('img').get('alt') if div.find('img') is not None else ''
      for soup in tqdm(pcd4_soup) 
      for div in soup.find('div', class_='content-fullwidth').\
        find_all('div', class_='syndicate', recursive=False)[1].\
        find_all('div', class_=('pt-2',), recursive=False)]

# <img> alt attribute text for all images under <div class="syndicate">
alt_text = [img.get('alt').strip(' \n') for soup in tqdm(pcd4_soup) 
   for img in soup.find('div', class_='content-fullwidth').\
      find_all('div', class_='syndicate', recursive=False)[1].\
      find_all('img', alt=True)]
# len(alt_text); len(set(alt_text)) # 2887 images, 770 unique strings

alt_text_freq = {text: alt_text.count(text) for text in sorted(set(alt_text))}
set(alt_text_freq.values()) # {1, 2, 3, 4, 8, 16, 2080}
[(freq, alt_text) for alt_text, freq in alt_text_freq.items() if freq > 1]
# [(3, ''),
#  (2, '14_0299'),
#  (2, '14_0378'),
#  (2, 'A map of New York State counties shows geographic distribution of obesity among men based on ordinary least squares (OLS) coefficients. OLS coefficients are larger in the southeast and smaller in the west.'),
#  (2, 'Change in patients’ general knowledge of diabetes over time, measured with the Diabetes, Hypertension and Hyperlipidemia (DHL) knowledge instrument (15), for patients participating in the health coaching program. The change in score (possible range, 0–28) was assessed over time in A) all patients (n = 238), and in B) patients who completed the assessment at all time points. Scores for A at each time point after baseline were compared with baseline scores by using the Wilcoxon matched-pairs signed-rank test. Scores for B at each time point after baseline were compared with baseline scores by using the Friedman test. Error bars indicate standard deviation. '),
#  (3, 'Changes in Density of On-Premises'),
#  (16, 'Joint Accredited Provider Interprofessional Continuing Education '),
#  (4, 'Jointly Accredited Provider Interprofessional Continuing Education Logo'),
#  (3, 'Leonard Jack'),
#  (8, 'Leonard_Jack'),
#  (2, 'Mean Sitting Time, by United Hospital Fund, 34 Neighborhoods in New York City, Physical Activity and Transit Survey,'),
#  (2080, 'Return to your place in the text'),
#  (3, 'Three advertisements used in New York State Department of Health promotion of tobacco cessation patient interventions among health care providers. ')]
# plus 757 unique strings # sum([freq for alt_text, freq in alt_text_freq.items() if freq == 1])
pd.DataFrame([alt_text for alt_text, freq in alt_text_freq.items() if freq == 1]).to_clipboard()

# [text for text, freq in alt_text_freq.items() if freq > 1 and len(text) > 10 and len(text) < 100]
# rule to remove noninformative values
alt_exclude = ('Joint Accredited Provider Interprofessional Continuing Education', 
   'Jointly Accredited Provider Interprofessional Continuing Education Logo', 
   'Leonard Jack', 'Leonard_Jack', 'Return to your place in the text', '')
alt_text = [x for x in alt_text if len(x) > 10 and x not in alt_exclude]
# len(alt_text); len(set(alt_text)) # 753 and 746, down from 2887 and 770
pd.DataFrame([dict(len=len(x), alt_text=x) for x in alt_text]).to_clipboard()

alt_text_elems = [img for soup in tqdm(pcd4_soup) 
   for img in soup.find('div', class_='content-fullwidth').\
      find_all('div', class_='syndicate', recursive=False)[1].\
      find_all('img', alt=True)
      if len(img.get('alt').strip(' \n')) > 10
         and img.get('alt').strip(' \n') not in alt_exclude]

x = [(depth(y), y.parent.name) for y in alt_text_elems]
{y: x.count(y) for y in sorted(set(x))}
{(6, 'div'): 22, (7, 'center'): 9, (7, 'div'): 374, (7, 'p'): 336, 
 (8, 'a'): 5, (10, 'td'): 7}

def branch(object, ancestor_name = 'body'):
   branch = [object.name]
   for depth, ancestor in enumerate(object.parents, start=1):
      branch = [ancestor.name] + branch # could use deque
      if ancestor.name == ancestor_name: break
   if ancestor.name is None: depth = None
   return dict(depth = depth, branch='>'.join(branch))

# branch(alt_text_elems[0])
x = [branch(elem) for elem in alt_text_elems]
xx = [tuple(y.values()) for y in x]
{y: xx.count(y) for y in sorted(set(xx))}
# {( 6, 'body>div>main>div>div>div>img'):                  22,
#  ( 7, 'body>div>main>div>div>div>center>img'):            9,
#  ( 7, 'body>div>main>div>div>div>div>img'):             374,
#  ( 7, 'body>div>main>div>div>div>p>img'):               336,
#  ( 8, 'body>div>main>div>div>div>div>a>img'):             3,
#  ( 8, 'body>div>main>div>div>div>p>a>img'):               2,
#  (10, 'body>div>main>div>div>div>table>tbody>tr>td>img'): 7}
# depth 6 is child of <div class="syndicate">

# example of 2nd div:syndicate, race lineage up and down
y = pcd4_soup[0].find('div', class_='content-fullwidth').\
      find_all('div', class_='syndicate', recursive=False)[1]
branch(y)
# {'depth': 5, 'branch': 'body>div>main>div>div>div'}

(lambda x: (x.name, x.attrs))(y)
('div', {'class': ['syndicate']})
(lambda x: (x.name, x.attrs))(y.parent)
('div', {'class': ['col', 'content', 'content-fullwidth']})
 (lambda x: (x.name, x.attrs))(y.parent.parent)
('div', {'class': ['row']})
(lambda x: (x.name, x.attrs))(y.parent.parent.parent)
('main', {'class': ['col-12', 'order-lg-2']})
(lambda x: (x.name, x.attrs))(y.parent.parent.parent.parent)
('div', {'class': ['container', 'd-flex', 'flex-wrap', 'body-wrapper', 'bg-white']})
(lambda x: (x.name, x.attrs))(y.parent.parent.parent.parent.parent)
('body', {'class': ['no-js']})
(lambda x: (x.name, x.attrs))(y.parent.parent.parent.parent.parent.parent)
('html', {'class': ['theme-green'], 'lang': 'en-us'})

def branch2(object, ancestor_name = 'body'):
   branch = [(object.name, object.attrs)]
   for depth, ancestor in enumerate(object.parents, start=1):
      branch = [(ancestor.name, ancestor.attrs)] + branch # could use deque
      if ancestor.name == ancestor_name: break
   if ancestor.name is None: depth = None
   return dict(depth = depth, branch = branch)
branch2(y.h4)

# gather ancestry for all <img>s
x = [branch2(elem) for elem in alt_text_elems]
# trim by removing ancestry through <div class="syndicate"> and terminal <img>
xx = [(y['depth']-6, y['branch'][6:-1]) for y in x]
xy = [(y[0], str(y[1])) for y in xx]
# xx = [tuple(y.values()) for y in x]
{y: xy.count(y) for y in sorted(set(xy))}

x = [(i, j, h2.get_text('|', strip=True))
   for i, soup in tqdm(enumerate(pcd4_soup), total=916)
   for j, h2 in enumerate(soup.find('div', class_='content-fullwidth').\
      find_all('div', class_='syndicate', recursive=False)[1].\
      find_all('h2'))
   if soup.find('div', class_='content-fullwidth').\
        find('div', class_=('cme', 'medscapeCME'))
        is not None]

   x = [str(div.attrs) for soup in tqdm(pcd4_soup) 
     for div in s]


block_names = ('h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'p', 'ul', 'ol', 'blockquote')

def process_pcd4(soup):
   soup_ = copy.copy(soup.find('div', class_='content_fullwidth'))
   div_synd1, div_dateline, div_synd2 = soup_.find_all('div', 
      class_=('syndicate', 'dateline'))[:3]
   # div_synd1: title
   # div_dateline: article type, volume number, publication date
   # div_synd2: authors, citation, peer-reviewed, summary; content
   pass


#%%
from bs4 import Comment, Script, Stylesheet
def count_subs(elem):
   elem_name = elem.name
   elem_parent_name = '' if elem.parent.name is None else elem.parent.name
   elem_depth = depth(elem)
   elem_descendants = list(elem.descendants)
   elem_desc_types = [str(type(desc)) for desc in elem_descendants]
   elem_desc_type_freq = { type_: elem_desc_types.count(type_) 
                          for type_ in sorted(set(elem_desc_types)) }
   elem_desc_names = ['' if desc.name is None else desc.name
                      for desc in elem_descendants]
   elem_desc_name_freq = { name_: elem_desc_names.count(name_) 
                          for name_ in sorted(set(elem_desc_names)) }
   div_classes = ['' if class_ is None else class_
                for div_elem in elem.find_all('div')
                for class_ in div_elem.get_attribute_list('class')]
   div_class_freq = { class_: div_classes.count(class_) 
                   for class_ in sorted(set(div_classes)) }

   h_classes = ['' if class_ is None else class_
                for h_elem in elem.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'])
                for class_ in h_elem.get_attribute_list('class')]
   h_class_freq = { class_: h_classes.count(class_) 
                   for class_ in sorted(set(h_classes)) }
   h_depths = [depth(h_elem) for h_elem in elem.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'])]
   h_depth_min = min(h_depths, default = -1)
   h_depth_max = max(h_depths, default = -1)

   p_classes = ['' if class_ is None else class_
                for p_elem in elem.find_all('p')
                for class_ in p_elem.get_attribute_list('class')]
   p_class_freq = { class_: p_classes.count(class_) 
                   for class_ in sorted(set(p_classes)) }
   p_depths = [depth(p_elem) for p_elem in elem.find_all('p')]
   p_depth_min = min(p_depths, default = -1)
   p_depth_max = max(p_depths, default = -1)
   p_topstrings = [p_elem 
                       for p_elem in elem.find_all(name=['p', 'span'])
                       if p_elem.a is not None
                       and p_elem.a.get_text() is not None
                       and p_elem.a.get_text(strip=True) in pcd_topstrings]
   n_topstrings = len(p_topstrings)
   p_ts_depths = [depth(p_elem) for p_elem in p_topstrings]
   p_ts_depth_min = min(p_ts_depths, default=-1)
   p_ts_depth_max = max(p_ts_depths, default=-1)
   n_form_eMailer = len(elem.find_all('form', attrs={'name': 'eMailer'}))
   n_div_onthispageChrono = len(elem.find_all('div', class_='onthispageChrono'))
   n_div_tponthispage = len(elem.find_all('div', class_='tp-on-this-page'))
   n_span_es = len(elem.find_all('span', lang='es'))
   return dict(elem_name=elem_name, elem_parent_name=elem_parent_name,
      elem_depth=elem_depth, elem_desc_names=elem_desc_name_freq,
      elem_desc_types=elem_desc_type_freq, 
      div_classes=div_class_freq,
      p_classes=p_class_freq,
      p_depth_min=p_depth_min, p_depth_max=p_depth_max, 
      n_topstrings=n_topstrings, 
      p_ts_depth_min=p_ts_depth_min, p_ts_depth_max=p_ts_depth_max,
      n_form_eMailer=n_form_eMailer, 
      n_div_onthispageChrono=n_div_onthispageChrono,
      n_div_tponthispage=n_div_tponthispage,
      n_span_es=n_span_es)


# Inventory components of containers
def pcd_container_components(soup):
   td_w_80 = soup.find_all(td_w_80_fn)
   div_main_inner = soup.find_all('div', class_='main-inner')
   div_content_fw = soup.find_all('div', class_='content-fullwidth')

   # the following 3 conditions are mutually exclusive
   if len(td_w_80):
      components = ['td_w_80'] + [count_subs(elem) for elem in td_w_80]
   elif len(div_main_inner):
      components = ['div_main_inner'] + [count_subs(elem) for elem in div_main_inner]
   elif len(div_content_fw):
      components = ['div_content_fw'] + [count_subs(elem) for elem in div_content_fw]
   # else:
   #    pass
   
   # div_syndicate = len(soup.find_all('div', class_='syndicate'))
   return components

# pcd_container_freq_ = [pcd_container_components(soup) for soup in tqdm(pcd_art_soup)]
# 3542/3542 [01:18<00:00, 44.94it/s]
# confirmed that the 3 categories are mutually exclusive and exhaustive

pcd_container_list = [pcd_container_components(soup) for soup in tqdm(pcd_art_soup)]
# 3542/3542 [02:07<00:00, 27.81it/s]
# pickle.dump(pcd_container_list, open("pcd_container_list.pkl", "wb"))

[pcd_container_list[i] for i in [73, 968, 2533, 1554, 2900, 1645, 2963, 2488]]

# set([len(x) for x in pcd_container_list])
{ j: [len(x) for x in pcd_container_list].count(j) for j in range(2, 9) }
# {2: 1844, 3: 5, 4: 1057, 5: 623, 6: 9, 7: 3, 8: 1}

{ struct_: [x[0] for x in pcd_container_list].count(struct_)
           for struct_ in ('td_w_80', 'div_main_inner', 'div_content_fw') }
# {'td_w_80': 1698, 'div_main_inner': 928, 'div_content_fw': 916}

# single-value elements
{ k: v for k, v in pcd_container_list[0][1].items() if type(v) is not dict }
# ['elem_name', 'elem_parent_name', 'elem_depth', 'p_depth_min', 'p_depth_max', 
#  'n_topstrings', 'p_ts_depth_min', 'p_ts_depth_max', 'n_form_eMailer', 
#  'n_div_onthispageChrono', 'n_div_tponthispage', 'n_span_es']

# dictionary-value elements
{ k: v for k, v in pcd_container_list[0][1].items() if type(v) is dict }
# ['elem_desc_names', 'elem_desc_types', 'div_classes', 'p_classes']

pcd_comps_per_file = [len(x)-1 for x in pcd_container_list]
{ j: pcd_comps_per_file.count(j) for j in range(1, 8) }
# {1: 1844, 2: 5, 3: 1057, 4: 623, 5: 9, 6: 3, 7: 1}

pcd_comps_dframe = pd.concat([
   pd.DataFrame(
      [dict(path = path, year = int(path[12:16]), container = comps[0])
          for path, comps in zip(pcd_art_frame.mirror_path, pcd_container_list)
          for comp_dict in comps[1:]]),
   pd.DataFrame(
      [{ k: v for k, v in comp_dict.items() if type(v) is not dict } 
         for comps in pcd_container_list
         for comp_dict in comps[1:]])
   ], axis=1)
# [7587 rows x 15 columns]
pcd_comps_dframe.to_excel('pcd_comps_dframe.xlsx', engine='openpyxl')

pd.concat([
   pcd_comps_dframe.iloc[:, 0:3],
   pd.DataFrame(
      [comp_dict['elem_desc_names'] 
         for comps in pcd_container_list
         for comp_dict in comps[1:]]).fillna(0)
   ], axis=1).to_excel('pcd_comps_desc_names.xlsx', engine='openpyxl')
# [7587 rows x 777 columns]
pd.concat([
   pcd_comps_dframe.iloc[:, 0:3],
   pd.DataFrame(
      [comp_dict['elem_desc_types'] 
         for comps in pcd_container_list
         for comp_dict in comps[1:]]).fillna(0)
   ], axis=1).to_excel('pcd_comps_desc_types.xlsx', engine='openpyxl')
# [7587 rows x 6 columns]
pd.concat([
   pcd_comps_dframe.iloc[:, 0:3],
   pd.DataFrame(
      [comp_dict['p_classes'] 
         for comps in pcd_container_list
         for comp_dict in comps[1:]]).fillna(0)
   ], axis=1).to_excel('pcd_comps_p_classes.xlsx', engine='openpyxl')
# [7587 rows x 44 columns]
pd.concat([
   pcd_comps_dframe.iloc[:, 0:3],
   pd.DataFrame(
      [comp_dict['div_classes'] 
         for comps in pcd_container_list
         for comp_dict in comps[1:]]).fillna(0)
   ], axis=1).to_excel('pcd_comps_div_classes.xlsx', engine='openpyxl')
# [7587 rows x 116 columns]

# Test conditions on child element and its descendants
def child_keep(child):
   child_truth = child.name is not None \
      and (child.name in {'h1', 'h2', 'h3', 'h4', 'h5', 'h6'} 
         or child.find(['h1', 'h2', 'h3', 'h4', 'h5', 'h6']) is not None 
         or (child.name == 'div' and child.has_attr('class') 
            and {'syndicate', 'dateline'}.intersection(child.get('class'))) 
         or child.find('div', class_=['syndicate', 'dateline']) is not None)
   return child_truth

# Inventory components of containers
def pcd_container_components2(path, soup):
   td_w_80 = soup.find_all(td_w_80_fn)
   div_main_inner = soup.find_all('div', class_='main-inner')
   div_content_fw = soup.find_all('div', class_='content-fullwidth')

   if len(td_w_80):
      for i, elem in enumerate(td_w_80):
         for j, child in enumerate(elem.children):
            if child.name is not None:
               if child_keep(child):
                  td_width_80_in.write(f'<!--{path} elem {i}, child {j}, {child.name}-->\n')
                  td_width_80_in.write(child.prettify())
               else:
                  td_width_80_out.write(f'<!--{path} elem {i}, child {j}, {child.name}-->\n')
                  td_width_80_out.write(child.prettify())
      
   elif len(div_main_inner):
      # div_main_inner should be list of length 1
      for i, elem in enumerate(div_main_inner):
         for j, child in enumerate(elem.children):
            if child.name is not None:
               if child_keep(child):
                  div_main_inner_in.write(f'<!--{path} elem {i}, child {j}, {child.name}-->\n')
                  div_main_inner_in.write(child.prettify())
               else:
                  div_main_inner_out.write(f'<!--{path} elem {i}, child {j}, {child.name}-->\n')
                  div_main_inner_out.write(child.prettify())

   elif len(div_content_fw):
      # div_content_fw should be list of length 1
      for i, elem in enumerate(div_content_fw):
         for j, child in enumerate(elem.children):
            if child.name is not None:
               if child_keep(child):
                  div_content_fw_in.write(f'<!--{path} elem {i}, child {j}, {child.name}-->\n')
                  div_content_fw_in.write(child.prettify())
               else:
                  div_content_fw_out.write(f'<!--{path} elem {i}, child {j}, {child.name}-->\n')
                  div_content_fw_out.write(child.prettify())

   else:
      other_components.write('<!--' + path + '-->\n')
   
   # div_syndicate = len(soup.find_all('div', class_='syndicate'))
   return None

div_content_fw_in  = open('div_content-fullwidth_in.html', 'w')
div_content_fw_out = open('div_content-fullwidth_out.html', 'w')
div_main_inner_in  = open('div_main-inner_in.html', 'w')
div_main_inner_out = open('div_main-inner_out.html', 'w')
td_width_80_in     = open('td_width_80_in.html', 'w')
td_width_80_out    = open('td_width_80_out.html', 'w')
other_components   = open('other-components.html', 'w')

for file_ in [div_content_fw_in, div_content_fw_out,
   div_main_inner_in, div_main_inner_out, td_width_80_in, td_width_80_out,
   other_components]:
   file_.write('<html><body>\n')

for path, soup in tqdm(zip(pcd_art_frame.mirror_path, pcd_art_soup), total=3542):
   pcd_container_components2(path, soup)
# 3542/3542 [02:16<00:00, 25.87it/s]

for file_ in [div_content_fw_in, div_content_fw_out,
   div_main_inner_in, div_main_inner_out, td_width_80_in, td_width_80_out,
   other_components]:
   file_.write('</body></html>\n')

div_content_fw_in.close()
div_content_fw_out.close()
div_main_inner_in.close()
div_main_inner_out.close()
td_width_80_in.close()
td_width_80_out.close()
other_components.close()
del div_content_fw_in, div_content_fw_out, div_main_inner_in, div_main_inner_out, \
   td_width_80_in, td_width_80_out, other_components, file_

#%% Reduce each html.body soup for further processing

# 1. Reduce to smallest element enclosing content of interest
# 2. Remove remaining components of no interest (e.g., navigaion <div>s)
# 3. Write reduced soup as html strings to new list object; pickle it
# 4. Reread reduced-soup html and parse into soup
# 5. Assess equivalence of reduced html to html->soup->html (lose any info?)

# When <main> occurs in a file, it occurs exactly once
def main_descendants(soup):
   import copy
   soup_ = copy.copy(soup)
   try:
      content = soup_.find('main').find_all('div', class_=['syndicate', 'dateline'])
      div_ml_3 = soup_.find('div', class_='ml-3')
      if div_ml_3 is not None: div_ml_3.decompose()
      div_tp_page = soup_.find('div', class_='tp-on-this-page')
      if div_tp_page is not None: div_tp_page.decompose()
      content = re.sub(r'<!--.*?-->', '', ' '.join([str(elem) for elem in content]))
      content = re.sub(r'\s+', ' ', content)
   except:
      content = None
   return content

# When <div class="main-inner"> occurs in a file, it occurs exactly once
from bs4 import Comment

# use BeautifulSoup to extract <div class="main-inner">
# convert to string, use regex to remove comments, parse back to soup
# remove elements <div class="onthispageChrono">
# convert back to string

def main_inner_descendants(soup):
   try:
      content = soup.find('div', class_='main-inner')
      soup_ = BeautifulSoup(re.sub(r'<!--.*?-->', '', str(content)), 'lxml').\
         find('div', class_='main-inner')
      if soup_.find('div', class_='onthispageChrono') is not None:
         for j in range(len(soup_.find_all('div', class_='onthispageChrono'))):
            soup_.find('div', class_='onthispageChrono').decompose()
      content = re.sub(r'\s+', ' ', str(soup_))
   except:
      content = ''
   return content

xx = copy.copy(pcd_art_soup[1088]) # len(str(xx)) # 26663
yy = main_inner_descendants(xx) # len(_) # 8491
# str(BeautifulSoup(main_descendants(x), 'lxml'))

[(i, path) for i, (path, soup) in enumerate(zip(pcd_art_frame.mirror_path, pcd_art_soup))
 if len(soup.find_all('div', class_='onthispageChrono')) > 4]
xx = copy.copy(pcd_art_soup[1088])

len(xx.find_all('div', class_='onthispageChrono')) # 5
xx.find('div', class_='onthispageChrono').decompose()
len(xx.find_all('div', class_='onthispageChrono')) # 4
xx.find('div', class_='onthispageChrono').decompose()
len(xx.find_all('div', class_='onthispageChrono')) # 3
xx.find('div', class_='onthispageChrono').decompose()
len(xx.find_all('div', class_='onthispageChrono')) # 2
xx.find('div', class_='onthispageChrono').decompose()
len(xx.find_all('div', class_='onthispageChrono')) # 1
xx.find('div', class_='onthispageChrono').decompose()
len(xx.find_all('div', class_='onthispageChrono')) # 0

xx = copy.copy(pcd_art_soup[1088])
for j in range(len(xx.find_all('div', class_='onthispageChrono'))):
   xx.find('div', class_='onthispageChrono').decompose()
   print(len(xx.find_all('div', class_='onthispageChrono')))

def pcd_soup_body(soup):
   has_main = (soup.main is not None)
   has_main_inner = (soup.find('div', class_='main-inner') is not None)
   has_width_80 = (soup.find('td', width='80%') is not None)
   
   
   


#%% miscellaneous bits from past efforts -- junk yard

z = za_toptext # 21957
z_str = sorted([str(tag) for tag in z])
z_text = sorted([tag.get_text('|', strip=True) for tag in z])
z_str_freq = { item: z_str.count(item) for item in sorted(set(z_str)) } # 11
z_text_freq = { item: z_text.count(item) for item in sorted(set(z_text)) } # 7

# [text for text in sorted(set(z_text)) if len(text) <= 13]
# ['Back to top', 'Top', 'Top of Page']
za_toptext_ = [tag for tag in za_toptext 
   if tag.get_text('|', strip=True) in {'Back to top', 'Top', 'Top of Page'}]
# 21037
               
set([za.parent.name for za in za_toptext_]) # {'div', 'p', 'span'}
[[za.parent.name for za in za_toptext_].count(elem) 
   for elem in ['div', 'p', 'span']]
# [1, 21020, 16]


za_toptext = [tag for soup in tqdm(pcd_art_soup)
                 for tag in soup.find_all('a', href=True)
                 if tag.get_text() is not None and
                    re.search(r'\btop\b', tag.get_text(), re.I)]

[len(x) for x in set([z.get_text('|', strip=True) for z in z_all_toptext])]
[len(x) for x in set([str(z) for z in z_all_toptext])]
with open('z_all_toptext-str.txt', 'w') as file_out:
   for z_s in sorted(set([str(z) for z in z_all_toptext])):
      file_out.write(f'{z_s}\n')


x_attrs = [tag.attrs for tag in x.descendants if tag.name is not None]
len(x.main) # 6
len(list(x.main.children)) # 6

import json
with open('pcd-example-attrs.json', 'w') as json_out:
   json.dump(x_attrs, json_out)

# a bit that is not in 
x.find(name='div', class_='bl-primary').get_text('|', strip=True)


# need to figure out Spanish version(s), so see full English versions
def top_list(tag):
   # cond1 = tag.name in ('h1', 'h2', 'h3', 'h4', 'h5', 'h6')
   cond2 = (tag.name == 'p') and \
      (tag.get_text(strip=True) in ('Back to top', 'Top', 'Top of Page'))
   return cond1 or cond2

pcd_body_p_tops = [str(tag) for html in tqdm(pcd_art_html)
                   for tag in BeautifulSoup(html, 'lxml').find_all('p')
                   if tag.get_text(strip=True) in ('Back to top', 'Top', 'Top of Page')]
{ elem: pcd_body_p_tops.count(elem) for elem in sorted(set(pcd_body_p_tops)) }

def top_list(tag):
   p_classes = {'float-right', 'psmall', 'topOPage'} # p classes
   a_hrefs = {'#', '#toTop', '#ttop', '#top'} # a hrefs
   top_strings = {'Back to top', 'Top', 'Top of Page'}
   cond1 = (tag.name == 'p') and tag.find('p', class_=p_classes)
   cond2 = (tag.name == 'a') and tag.find('a', href=a_hrefs)
   cond3 = (tag.name == 'p') and \
      (tag.get_text(strip=True) in top_strings)
   return cond1 or cond2 or cond3

p_classes = {'float-right', 'psmall', 'topOPage'} # p classes
a_hrefs = {'#toTop', '#ttop', '#top'} # a hrefs; also '#'
top_strings = {'Back to top', 'Top', 'Top of Page'}
# top_strings_re = {re.compile(string) for string in top_strings}
top_strings_re = re.compile(r'.*(top|volver|inicio|comienzo).*', re.I)
# {re.compile(string) for string in ['top', 'Top', 'Volver', 'Inicio', 'inicio', 'comienzo']}
# ['top', 'Top', 'Volver', 'Inicio', 'inicio', 'comienzo']

zp_topOPage = [tag for html in tqdm(pcd_art_html)
                   for tag in BeautifulSoup(html, 'lxml').find_all('p', class_='topOPage')]
# 3540/3540 [03:03<00:00, 19.25it/s]
z_uniq = sorted(set([str(zz) for zz in zp_topOPage])) # 4
['<p class="topOPage"> </p>',
 '<p class="topOPage"> <a href="#"> Inicio de la página </a> </p>',
 '<p class="topOPage"> <a href="#"> Top of Page </a> </p>',
 '<p class="topOPage"> <a href="#"> Volver al comienzo </a> </p>']
z = [str(zz) for zz in zp_topOPage] # 4
{ string: z.count(string) for string in z_uniq }
sorted(set([zz.get_text(strip=True) for zz in zp_topOPage])) # 4
['', 'Inicio de la página', 'Top of Page', 'Volver al comienzo']

za_top = [tag for html in tqdm(pcd_art_html)
                   for tag in BeautifulSoup(html, 'lxml').find_all('a', href=a_hrefs)]
# 3540/3540 [03:41<00:00, 15.95it/s]
z_uniq = sorted(set([str(zz) for zz in za_top])) # 8
['<a class="psmall" href="#top"> Back to top </a>',
 '<a href="#top"> Back to top </a>',
 '<a href="#top"> Inicio de página </a>',
 '<a href="#top"> Volver al Inicio </a>',
 '<a href="#top"> Volver al comienzo </a>',
 '<a href="#top"> Volver al comienzoo </a>',
 '<a href="#top"> Volver al inicio </a>',
 '<a href="#ttop"> Back to top </a>']
z = [str(zz) for zz in za_top] # 4
{ string: z.count(string) for string in z_uniq }
sorted(set([zz.get_text(strip=True) for zz in za_top])) # 6
['Back to top', 'Inicio de página', 'Volver al Inicio', 'Volver al comienzo', 
 'Volver al comienzoo', 'Volver al inicio']

# check string, which is not as meaningful as checking get_text()
zstr_top_a = [tag for html in tqdm(pcd_art_html)
                   for tag in BeautifulSoup(html, 'lxml').\
                      find_all('a', string=top_strings_re)]
# 3540/3540 [03:09<00:00, 18.69it/s]
z_uniq0 = set([str(zz) for zz in zstr_top_a if len(str(zz)) < 55]) # 16
z_uniq1 = set([str(zz) for zz in zstr_top_a if len(zz.string) < 30]) # 18
# sorted(z_uniq0 & z_uniq1) # intersection # 14
['<a class="psmall" href="#top"> Back to top </a>',
 '<a class="tp-link-policy" href="#"> Top </a>',
 '<a href="#"> Back to top </a>',
 '<a href="#"> Inicio de la página </a>',
 '<a href="#"> Top </a>',
 '<a href="#"> Top of Page </a>',
 '<a href="#"> Volver al comienzo </a>',
 '<a href="#top"> Back to top </a>',
 '<a href="#top"> Inicio de página </a>',
 '<a href="#top"> Volver al Inicio </a>',
 '<a href="#top"> Volver al comienzo </a>',
 '<a href="#top"> Volver al comienzoo </a>',
 '<a href="#top"> Volver al inicio </a>',
 '<a href="#ttop"> Back to top </a>']
z = [str(zz) for zz in zstr_top_a]
{ string: z.count(string) for string in sorted(z_uniq0 & z_uniq1) }
sorted(set([zz.get_text(strip=True) for zz in zstr_top_a 
            if len(zz.get_text(strip=True)) < 25])) # 0
['Back to top', 'Inicio de la página', 'Inicio de página', 'Top', 
 'Top of Page', 'Volver al Inicio', 'Volver al comienzo', 
 'Volver al comienzoo', 'Volver al inicio']

top_strings = {'Back to top', 'Inicio de la página', 'Inicio de página', 'Top', 
   'Top of Page', 'Volver al Inicio', 'Volver al comienzo', 
   'Volver al comienzoo', 'Volver al inicio'}

zstr_top_p = [tag for html in tqdm(pcd_art_html[:10])
                   for tag in BeautifulSoup(html, 'lxml').\
                      find_all('p')#, string=top_strings_re)
                      if tag.a is not None and \
                         tag.a.get_text(strip=True) in top_strings]
z_uniq = sorted(set([str(zz) for zz in zstr_top_p 
            if len(zz.get_text(strip=True)) < 50]))
# 3540/3540 [06:36<00:00,  8.94it/s]
['<p align="left" class="psmall"> <a href="#"> Back to top </a> </p>',
 '<p class="caption"> <a href="#top"> Volver al comienzo </a> </p>',
 '<p class="float-right"> <a href="#"> Top </a> </p>',
 '<p class="float-right"> <span class="text-right d-block"> <span class="icon-angle-up"> <!-- --> </span> <a href="#"> Top </a> </span> </p>',
 '<p class="psmall" dir="ltr"> <a href="#"> Back to top </a> </p>',
 '<p class="psmall" dir="ltr"> <a href="#top"> Volver al comienzo </a> </p>',
 '<p class="psmall"> <a href="#"> Back to top </a> </p>',
 '<p class="psmall"> <a href="#"> Back to top </a> ] </p>',
 '<p class="psmall"> <a href="#top"> Back to top </a> </p>',
 '<p class="psmall"> <a href="#top"> Inicio de página </a> </p>',
 '<p class="psmall"> <a href="#top"> Volver al Inicio </a> </p>',
 '<p class="psmall"> <a href="#top"> Volver al comienzo </a> </p>',
 '<p class="psmall"> <a href="#top"> Volver al comienzoo </a> </p>',
 '<p class="psmall"> <a href="#top"> Volver al inicio </a> </p>',
 '<p class="psmall"> <a href="#ttop"> Back to top </a> </p>',
 '<p class="pull-right"> <span class="toTop"> <span class="icon-angle-up"> <!-- --> </span> <a class="tp-link-policy" href="#"> Top </a> </span> </p>',
 '<p class="topOPage"> <a href="#"> Inicio de la página </a> </p>',
 '<p class="topOPage"> <a href="#"> Top of Page </a> </p>',
 '<p class="topOPage"> <a href="#"> Volver al comienzo </a> </p>',
 '<p> <a class="psmall" href="#top"> Back to top </a> </p>',
 '<p> <a href="#"> Back to top </a> </p>',
 '<p> <a href="#top"> Volver al comienzo </a> </p>']
z = [str(zz) for zz in zstr_top_p] # 4
{ string: z.count(string) for string in z_uniq }

# evaluate id attributes for the word top (ignore case); result: no concern
z_id = [tag for html in tqdm(pcd_art_html)
                   for tag in BeautifulSoup(html, 'lxml').\
                      find_all(id=True)]
z_id_names = sorted(set([x.name for x in tqdm(z_id)])) # 24
z_id_vals = [x['id'] for x in tqdm(z_id)] # 1733
z_id_vals_uniq = sorted(set(z_id_vals)) # 1733
z_id_val_freqs = { string: z_id_vals.count(string) for string in z_id_vals_uniq }
print(sorted(z_id_val_freqs.values(), reverse=True)[:25])
print({k: v for k, v in z_id_val_freqs.items() if v > 800})

z_id_tops = [z for z in tqdm(z_id) if z.get_text() is not None and \
             re.search(r'\btop\b', z.get_text(), re.I)]
z_id_top_vals = [x['id'] for x in tqdm(z_id_tops)] # 1733
z_id_top_vals_uniq = sorted(set(z_id_top_vals)) # 19
z_id_top_val_freqs = { string: z_id_top_vals.count(string) for string in z_id_top_vals_uniq }
print(sorted(z_id_top_val_freqs.values(), reverse=True)[:25])
print(z_id_top_val_freqs)

z_id_top_texts = [x.get_text('|', strip=True) for x in tqdm(z_id_tops)] # 1733
z_id_top_texts_uniq = sorted(set(z_id_top_texts)) # 2852
z_id_top_text_freqs = { string: z_id_top_texts.count(string) for string in z_id_top_texts_uniq }
print(sorted(z_id_top_text_freqs.values(), reverse=True)[:25])
print(z_id_top_text_freqs)
print({k: v for k, v in z_id_top_text_freqs.items() if v > 800})

# evaluate images for alt text; result: no concern
z_img = [tag for html in tqdm(pcd_art_html)
                   for tag in BeautifulSoup(html, 'lxml').\
                      find_all('img', alt=True)]
z_img_alts = [x['alt'] for x in tqdm(z_img)] # 
z_img_alts_uniq = sorted(set(z_img_alts)) # 1249
z_img_alt_freqs = { string: z_img_alts.count(string) for string in z_img_alts_uniq }
print(sorted(z_img_alt_freqs.values(), reverse=True)[:25])
print({k: v for k, v in z_img_alt_freqs.items() if v > 300})

   

z = [tag for html in tqdm(pcd_art_html)
                   for tag in BeautifulSoup(html, 'lxml').find_all('a', href=a_hrefs)]
z_str = [str(zz) for zz in z]
z_string = [zz.get_text(strip=True) for zz in z]
{ elem: z_str.count(elem) for elem in sorted(set(z_str)) }
{ elem: z_string.count(elem) for elem in sorted(set(z_string)) }

z2 = [tag for html in tqdm(pcd_art_html)
                   for tag in BeautifulSoup(html, 'lxml').find_all('p', class_='topOPage')]
z2_str = [str(zz) for zz in z2]
z2_string = [zz.get_text(strip=True) for zz in z2]
{ elem: z2_str.count(elem) for elem in sorted(set(z2_str)) }
{ elem: z2_string.count(elem) for elem in sorted(set(z2_string)) }

pcd_art_frame.loc[pcd_art_frame.mirror_path.str.contains('13_0137')]

def in_block_list(tag):
   cond1 = tag.name in ('h1', 'h2', 'h3', 'h4', 'h5', 'h6')
   cond2 = (tag.name == 'p') and \
      (tag.get_text(strip=True) in ('Back to top', 'Top', 'Top of Page'))
   return cond1 or cond2


[str(y) for y in x.find_all(in_block_list)]

def pcd_soup_body_blox(soup):
   # check for empty list?
   # block_names = ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']
   # block_elems += ['div', 'p', 'ul', 'ol']
   block_elems = soup.find_all(in_block_list)
#    block_info = [{
#       'name': el.name, 
# #      'attrs': el.attrs, 
#       'attrs': '|'.join(el.attrs.keys()), 
#       'text': el.get_text('|', strip=True)
# #      'text_len': len(el.get_text(' ', strip=True))
#       } for el in block_elems]
   block_info = [[el.name, '|'.join(el.attrs.keys()), el.get_text('|', strip=True)]
      for el in block_elems]
   return block_info

pcd_soup_body_blox(x)

pcd_body_blocks = [[path] + block
   for (path, html) in tqdm(zip(pcd_art_frame.mirror_path, pcd_art_html), total=3237)
   for block in pcd_soup_body_blox(BeautifulSoup(html, 'lxml'))]
# 3237/3237 [02:51<00:00, 18.83it/s]
pd.DataFrame(pcd_body_blocks).to_excel('pcd_body_blocks.xlsx', engine='openpyxl')

# changed pcd_soup_body_blox between previous call and this one
pcd_body_blocks2 = [[path] + block
   for (path, html) in tqdm(zip(pcd_art_frame.mirror_path, pcd_art_html), total=3237)
   for block in pcd_soup_body_blox(BeautifulSoup(html, 'lxml'))]
# 3237/3237 [02:27<00:00, 21.91it/s]
pd.DataFrame(pcd_body_blocks2).to_excel('pcd_body_blocks2.xlsx', engine='openpyxl')

sum([y[3] in ('Back to top', 'Top of Page', 'Top') for y in pcd_body_blocks2])
# 15646 # 21010 when including  'Top of Page'
