#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Analyze the structure and broad properties of PCD online archive

@author: cmheilig

Sections of this script, based on levels of MMWR archive:
0. PCD home https://www.cdc.gov/pcd/index.htm
1. Contents of series, including Spanish; list of archive volumes
2. List and contents of volumes
3. List of articles
4. Complete list of PCD files

Main product: pcd_cc_df
"""

#%% Import modules and set up environment
# import from 0_cdc-corpora-header.py

os.chdir('/Users/cmheilig/cdc-corpora/_test')

#%% 0. Start with PCD home https://www.cdc.gov/pcd/index.htm
base_url = 'https://www.cdc.gov/pcd/index.htm'
home_a = BeautifulSoup(get_html_from_url(base_url), 'lxml')\
   .find('a', href=re.compile('pcd/index.htm'), 
            string=re.compile('Preventing Chronic Disease'))
# process_aTag(home_a, base_url)
# {'base': 'https://www.cdc.gov/pcd/index.htm',
#  'href': '/pcd/index.htm',
#  'url': 'https://www.cdc.gov/pcd/index.htm',
#  'path': '/pcd/index.htm',
#  'filename': 'index.htm',
#  'mirror_path': '/pcd/index.htm',
#  'string': 'Preventing Chronic Disease'}

home_dframe = pd.DataFrame(process_aTag(home_a, base_url), index = [0])
# home_dframe.loc[:, ['path', 'string']]
#              path                      string
# 0  /pcd/index.htm  Preventing Chronic Disease
home_html = get_html_from_url(home_dframe.url[0]) # len(home_html) # 196924
home_soup = BeautifulSoup(home_html, 'lxml')

# review all anchor-hrefs from home URL
# len(home_soup.find_all('a', href=True)) # 148
pd.DataFrame([process_aTag(aTag, home_dframe.url[0]) 
    for aTag in home_soup.find_all('a', href=True)])\
    .to_excel('pcd-home-anchors.xlsx', engine='openpyxl', freeze_panes=(1,0))
# [153 rows x 7 columns]

#%% 1. Contents of series, including Spanish; list of archive volumes

# Review of anchor elements in home page, pcd-home-anchors.xlsx
# https://www.cdc.gov/pcd/current_issue.htm  # current volume
#    all issues and articles in 2024 (to date)
# https://www.cdc.gov/pcd/issues/archive.htm # past volumes
#    all volumes and articles in 2004-2011, volumes in 2012-2023

series_a = home_soup.find_all('a', href=re.compile('archive'))
# [<a href="/pcd/issues/archive.htm">Issue Archive</a>]

# Home page does not point to Spanish-language archive
# https://www.cdc.gov/pcd/es/archive_es.htm
home_es_url = 'https://www.cdc.gov/pcd/es/archive_es.htm'
series_es_a = BeautifulSoup(get_html_from_url(home_es_url), 'lxml')\
   .find('a', href=re.compile('pcd/es/archive_es.htm'))
# process_aTag(series_es_a, home_es_url)
# {'base': 'https://www.cdc.gov/pcd/es/archive_es.htm',
#  'href': '/pcd/es/archive_es.htm',
#  'url': 'https://www.cdc.gov/pcd/es/archive_es.htm',
#  'path': '/pcd/es/archive_es.htm',
#  'filename': 'archive_es.htm',
#  'mirror_path': '/pcd/es/archive_es.htm',
#  'string': 'Archivo de números en español'}

series_dframe = pd.DataFrame(
   [process_aTag(series_a[0], home_dframe.url[0]), 
    process_aTag(series_es_a, home_es_url)])
# series_dframe.loc[:, ['path', 'string']]
#                       path                         string
# 0  /pcd/issues/archive.htm                  Issue Archive
# 1   /pcd/es/archive_es.htm  Archivo de números en español

series_html = [get_html_from_url(url) for url in series_dframe.url]
# [len(x) for x in series_html] # [215589, 35870]
series_soup = [BeautifulSoup(html, 'lxml') for html in series_html]

# review all anchor-hrefs from series URL
# pd.DataFrame([process_aTag(aTag, url) 
#     for soup, url in zip(series_soup, series_dframe.url) 
#     for aTag in soup.find_all('a', href=True)])\
#     .to_excel('pcd-series-anchors.xlsx', engine='openpyxl', freeze_panes=(1,0))
# [355 rows x 7 columns]

#%% 2. List and contents of volumes

# Review of anchor elements in series page, pcd-series-anchors.xlsx
# https://www.cdc.gov/pcd/current_issue.htm  # current volume
#     current volume, all issues in 2024 (to date)
# https://www.cdc.gov/pcd/issues/yyyy/yyyy_TOC.htm
#     all volumes and articles in 2012-2023
# https://www.cdc.gov/pcd/issues/yyyy/mmm/toc.htm
#     all volumes and articles in 2004-2011
# https://www.cdc.gov/pcd/es/yyyy_toc.htm
#     all volumes and articles in 2012-2014
# https://www.cdc.gov/pcd/es/yyyy_mmm_toc.htm
#     all volumes and articles in 2005-2011
# https://www.cdc.gov/pcd/spanish/current_issue_es.htm # can ignore
#     last updated 2015

# pcd_vol_re = re.compile(r'(current_issue|\d{4}.*(TOC|toc)).htm')
pcd_vol_re = re.compile(r'\d{4}.*(TOC|toc).htm')
volumes_a = [soup.find_all('a', href=pcd_vol_re) for soup in series_soup]
volumes_a_n = [len(x) for x in volumes_a] # sum(volumes_a_n) # [49, 36]

volumes_dframe = pd.DataFrame([process_aTag(aTag, url) 
   for a_list, url in zip(volumes_a, series_dframe.url) 
   for aTag in a_list])
# volumes_dframe.loc[:, ['path', 'string']]
#                              path     string
# 0   /pcd/issues/2023/2023_TOC.htm       2023
# 1   /pcd/issues/2022/2022_TOC.htm       2022
# 2   /pcd/issues/2021/2021_TOC.htm       2021
# 3   /pcd/issues/2020/2020_TOC.htm       2020
# 4   /pcd/issues/2019/2019_TOC.htm       2019
# ..                            ...        ...
# 80       /pcd/es/2005_nov_toc.htm  Noviembre
# 81       /pcd/es/2005_oct_toc.htm    Octubre
# 82       /pcd/es/2005_jul_toc.htm      Julio
# 83       /pcd/es/2005_apr_toc.htm      Abril
# 84       /pcd/es/2005_jan_toc.htm      Enero

volumes_html = [get_html_from_url(url) for url in tqdm(volumes_dframe.url)]
# 85/85 [00:22<00:00,  3.76it/s]
# repr([len(x) for x in volumes_html])
# [297672, 272458, 284662, 345217, 332816, 411821, 397426, 428662, 470944, ...]
volumes_soup = [BeautifulSoup(html, 'lxml') for html in volumes_html]

# review all anchor-refs from volumes URLs
# pd.DataFrame([process_aTag(aTag, url) 
#     for soup, url in zip(volumes_soup, volumes_dframe.url) 
#     for aTag in soup.find_all('a', href=True)])\
#     .to_excel('pcd-volumes-anchors.xlsx', engine='openpyxl', freeze_panes=(1,0))
# [11825 rows x 7 columns]

#%% 3. List of articles

# Review of anchor elements in volumes page, pcd-volumes-anchors.xlsx
# mostly filenames of form dd_dddd.htm or dd_dddd_es.htm
# Retrieve files under https://www.cdc.gov/pcd/issues/
# Regular expressions for full paths
#     # 2004-2011
#     20\d{2}/(jan|mar|apr|may|jul|sep|oct|nov)/cover.htm
#     20\d{2}/(jan|mar|apr|may|jul|sep|oct|nov)/\d{2}_\d{4,5}[ab]?(_(es|fr|zhs|zht))?.htm
#     # 2012-2023
#     20\d{2}/\d{2}_\d{4}(e|r|_es)?.htm
# Regular expression for hrefs: \d{2}_\d{4,5}([aber]|_es)?.htm
# Omitted: memoriam,htm (1), _pt.htm (1), _vi.htm (1)

pcd_art_re = re.compile(r'((\d{2}_\d{4,5}([aber]|_es|_fr|_zhs|_zht)?)|cover).htm')
articles_a = [soup.find_all('a', href=pcd_art_re) for soup in volumes_soup]
articles_a_n = [len(x) for x in articles_a] # sum(articles_a_n) # 5804
# [117, 89, 104, 169, 166, 166, 142, 181, 231, 230, 216, 179, 67, 40, 38, ...]

articles_dframe = pd.DataFrame([process_aTag(aTag, url) 
   for a_list, url in zip(articles_a, volumes_dframe.url) 
   for aTag in a_list])
# (5804, 7)
# articles_dframe.loc[:, ['path', 'string']]
with pd.option_context("display.max_colwidth", 35):
    display(articles_dframe.loc[:, ['path', 'string']])
#                                     path                              string
# 0           /pcd/issues/2023/23_0198.htm  Substance Use, Sleep Duration, ...
# 1           /pcd/issues/2023/23_0173.htm  Prevalence of Testing for Diabe...
# 2           /pcd/issues/2023/23_0182.htm  Linking Adverse Childhood Exper...
# 3           /pcd/issues/2023/23_0199.htm  Disaggregation of Breastfeeding...
# 4           /pcd/issues/2023/23_0155.htm  Geospatial Determinants of Food...
#                                  ...                                 ...
# 5799  /pcd/issues/2005/jan/04_0079_es...  De la investigación a la prácti...
# 5800  /pcd/issues/2005/jan/04_0075_es...  Pasos Adelante:|La eficacia de ...
# 5801  /pcd/issues/2005/jan/04_0076_es...  El índice de sanidad escolar (S...
# 5802  /pcd/issues/2005/jan/04_0083_es...  El desarrollo y la adaptación d...
# 5803  /pcd/issues/2005/jan/04_0077_es...  La|Border Health Strategic Init...

# Before reviewing duplicates, redirect some filenames, as
# some Spanish-language document filenames do not end with _es.htm
articles_dframe.loc[articles_dframe['base'].str.contains('/es/') &
                    ~articles_dframe['url'].str.endswith('_es.htm')] # 16
set_es = articles_dframe.loc[articles_dframe['base'].str.contains('/es/') &
                             ~articles_dframe['url'].str.endswith('_es.htm')].index # 16
# Change url, path, filename, and mirror_path to include _es
for col in ['url', 'path', 'filename', 'mirror_path']:
    articles_dframe.loc[set_es, col] = (
        articles_dframe.loc[set_es, col].str.replace('.htm', '_es.htm', regex=False))
articles_dframe.loc[set_es, ['url', 'path', 'filename', 'mirror_path']]

# Check for duplicate URLs
articles_dupe = articles_dframe.path.duplicated(keep = False)
articles_review = articles_dframe.loc[articles_dupe].index # 1423
# articles_dframe.loc[articles_review, ['base', 'url', 'string']]
# articles_dframe.loc[articles_review].to_clipboard()

# 3 conditions for records to drop
# 1. url ends with '2018/17_0395.htm' (drop 1, keep 1)
# articles_dframe.loc[articles_dframe['url'].str.endswith('17_0395.htm')].index # 2
# [763, 777]
# 2. string is empty
articles_dframe.loc[articles_dframe['string'] == ''] # 4
# 3. string starts wtih 'Este ' and url ends with '_es.htm'
articles_dframe.loc[articles_dframe['string'].str.startswith('Este ') &
                    articles_dframe['url'].str.endswith('_es.htm') &
                    articles_dframe.path.duplicated(keep = False)] # 708

articles_dframe = (
    articles_dframe
        .drop(index=[777])
        .drop(articles_dframe.loc[articles_dframe['string'] == ''].index)
        .drop(articles_dframe.loc[articles_dframe['string'].str.startswith('Este ') &
                            articles_dframe['url'].str.endswith('_es.htm') &
                            articles_dframe.path.duplicated(keep = False)].index)
        .reset_index(drop=True) ) # (5091, 7)
# articles_dframe.loc[articles_dframe.path.duplicated(keep = False)].index # []

articles_dframe.to_excel('pcd-articles_dframe.xlsx', engine='openpyxl', freeze_panes=(1,0))

#%% 4. Complete list of PCD files
pcd_cc_df = pd.concat([
   home_dframe.assign(level='home'),
   series_dframe.assign(level='series'),
   volumes_dframe.assign(level='volume'),
   articles_dframe.assign(level='article')],
   axis = 0, ignore_index = True)
# (5179, 8)

# pickle
# pickle.dump(pcd_cc_df, open('pcd_cc_df.pkl', 'xb'))
pcd_cc_df.to_pickle('pcd_cc_df.pkl')

# Excel; coulad also use engine=
pcd_cc_df.to_excel('pcd_cc_df.xlsx', engine='openpyxl', freeze_panes=(1,0))

#%% 5. Mirror PCD to local disk

# pcd_cc_df = pickle.load(open("pcd_cc_df.pkl", "rb")) # (5177, 8)
pcd_cc_df = pd.read_pickle('pickle-files/pcd_cc_df.pkl')

PCD_BASE_PATH_b0 = normpath(expanduser('~/cdc-corpora'))

x = create_mirror_tree(PCD_BASE_PATH_b0, calculate_mirror_dirs(pcd_cc_df.path))
# { key: (0 if val is None else len(val)) for (key, val) in x.items() }

pcd_sizes_b0 = [
    mirror_raw_html(url, PCD_BASE_PATH_b0 + path, print_url = False)
    for url, path in tqdm(zip(pcd_cc_df.url[_temp], pcd_cc_df.mirror_path[_temp]),
                          total=len(pcd_cc_df.mirror_path[_temp]))]
# sum([x==0 for x in pcd_sizes_b0]) # retry those with 0 length
for j in range(len(pcd_sizes_b0)):
   if pcd_sizes_b0[j] == 0:
      pcd_sizes_b0[j] = mirror_raw_html(pcd_cc_df.url[j], 
         PCD_BASE_PATH_b0 + pcd_cc_df.mirror_path[j], timeout=5)
# sum([x==0 for x in pcd_sizes_b0]) # retry those with 0 length

# pickle.dump(pcd_sizes_b0, open('pcd_sizes_b0.pkl', 'wb'))

#%% 6. Routine for reading all files into a single list
pcd_html_b0 = [read_raw_html(PCD_BASE_PATH_b0 + path)
               for path in tqdm(pcd_cc_df.mirror_path)]
# 5179/5179 [00:01<00:00, 2638.44it/s]
pickle.dump(pcd_html_b0, open('pcd_raw_html.pkl', 'xb'))
