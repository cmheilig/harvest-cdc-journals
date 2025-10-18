#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Analyze the structure and broad properties of EID online archive

@author: chadheilig

Sections of this script, based on levels of EID archive:
0. EID  home https://www.cdc.gov/eid/ (home)
1. List of volumes (and some articles)
2. List and contents of volumes
3. List and contents of issues (tables of contents)
4. List of articles
3. Complete list of EID files

Main product: eid_cc_df
"""

#%% Import modules and set up environment
# import from 0_cdc-corpora-header.py

# os.chdir('/Users/cmheilig/cdc-corpora/_test')
os.chdir(r"C:\Temp\eid_2024")


#%% 0. Start with EID home https://wwwnc.cdc.gov/eid/

base_url = 'https://wwwnc.cdc.gov/eid/'
pd.DataFrame([process_aTag(aTag, base_url) 
    for aTag in BeautifulSoup(get_html_from_url(base_url), 'lxml')\
    .find_all('a', href=True)])\
    .to_excel('eid-base-anchors.xlsx', engine='openpyxl', freeze_panes=(1,0))
# [306 rows x 7 columns]

home_a = BeautifulSoup(get_html_from_url(base_url), 'lxml')\
   .find('a', href=re.compile('eid'), string=re.compile('EID Journal'))
# process_aTag(home_a, base_url)
# {'base': 'https://wwwnc.cdc.gov/eid/',
#  'href': '/eid/',
#  'url': 'https://wwwnc.cdc.gov/eid/',
#  'path': '/eid/',
#  'filename': '',
#  'mirror_path': '/eid/index.html',
#  'string': 'EID Journal'}

home_dframe = pd.DataFrame(process_aTag(home_a, base_url), index = [0])
# home_dframe.loc[:, ['path', 'string']]
#     path       string
# 0  /eid/  EID Journal
home_html = get_html_from_url(home_dframe.url[0]) # len(home_html) # 583854
home_soup = BeautifulSoup(home_html, 'lxml')

# review all anchor-hrefs from home URL
# len(home_soup.find_all('a', href=True)) # 302
# pd.DataFrame([process_aTag(aTag, home_dframe.url[0]) 
#     for aTag in home_soup.find_all('a', href=True)])\
#     .to_excel('eid-home-anchors.xlsx', engine='openpyxl', freeze_panes=(1,0))
# same as eid-base-anchors.xlsx

#%% 1. List of volumes (and some articles)

# Review of anchor elements in home page, eid-home-anchors.xlsx
# https://www.cdc.gov/eid/current  # current issue
#    all articles in current issue (January 2025)
# https://wwwnc.cdc.gov/eid/past-issues/volume-30 # past volumes
#    all previous volumes (1995-2024)

series_a = home_soup.find_all('a', string=re.compile('Past Issues'))
# [<a aria-expanded="true" href="/eid/past-issues/volume-30">Past Issues</a>]

series_dframe = pd.DataFrame(
   [process_aTag(aTag, home_dframe.url[0]) for aTag in series_a], index=[0])
# series_dframe.loc[:, ['path', 'string']]
#                          path       string
# 0  /eid/past-issues/volume-30  Past Issues

series_html = get_html_from_url(series_dframe.url[0]) # len(series_html) # 334835
series_soup = BeautifulSoup(series_html, 'lxml')

# review all anchor-hrefs from series URL
# len(series_soup.find_all('a', href=True)) # 235
pd.DataFrame([process_aTag(aTag, series_dframe.url[0]) 
    for aTag in series_soup.find_all('a', href=True)])\
    .to_excel('eid-series-anchors.xlsx', engine='openpyxl', freeze_panes=(1,0))
# [235 rows x 7 columns]

#%% 2. List and contents of volumes

# Review of anchor elements in series page, eid-series-anchors.xlsx
# eid/past-issues/volume{1-30}
# href contains 'volume-\d{1,2}' and string contains 'Volume'
# obtain volumes 2-present from volume-1 and Volume 1 from volume-2
eid_vol_re0 = re.compile(r'volume-\d{1,2}')
eid_vol_re1 = re.compile(r'Volume.+?\d{4}') # 1995-2023
eid_vol_re2 = re.compile(r'Volume.+?1995')  # 1995
volumes_a = BeautifulSoup(get_html_from_url(
      'https://wwwnc.cdc.gov/eid/past-issues/volume-1'), 'lxml').\
      find_all('a', href=eid_vol_re0, string=eid_vol_re1) + \
   BeautifulSoup(get_html_from_url(
      'https://wwwnc.cdc.gov/eid/past-issues/volume-2'), 'lxml').\
      find_all('a', href=eid_vol_re0, string=eid_vol_re2)
# len(volumes_a) # 30

volumes_dframe = pd.DataFrame(
   [process_aTag(aTag, series_dframe.url[0]) for aTag in volumes_a])
# volumes_dframe.loc[:, ['path', 'string']]
#                           path          string
# 0   /eid/past-issues/volume-30  Volume 30—2024
# 1   /eid/past-issues/volume-29  Volume 29—2023
# 2   /eid/past-issues/volume-28  Volume 28—2022
# ...
# 27   /eid/past-issues/volume-3   Volume 3—1997
# 28   /eid/past-issues/volume-2   Volume 2—1996
# 29   /eid/past-issues/volume-1   Volume 1—1995

volumes_html = [get_html_from_url_(url) for url in volumes_dframe.url]
# [len(x) for x in volumes_html]
# [336593, 334400, 335501, 334403, 334403, 334400, 334397, 335454, 334395, ...]
volumes_soup = [BeautifulSoup(html, 'lxml') for html in volumes_html]

# review all anchor-refs from volumes URLs
# pd.DataFrame([process_aTag(aTag, url) 
#     for soup, url in zip(volumes_soup, volumes_dframe.url) 
#     for aTag in soup.find_all('a', href=True)])\
#     .to_excel('eid-volumes-anchors.xlsx', engine='openpyxl', freeze_panes=(1,0))
# [6630 rows x 7 columns]

#%% 3. List and contents of issues (tables of contents)

# Review of anchor elements in volumes page, eid-volumes-anchors.xlsx
# All 315 issue paths have the form /eid/articles/issue/#0/#0/table-of-contents,
#    or href containing regex '\d{1,2}/\d{1,2}/table-of-contents'
# They also all have string 'Table of Contents'

eid_iss_re = re.compile(r'Table of Contents')
issues_a = [soup.find_all('a', string=eid_iss_re) for soup in volumes_soup]
issues_a_n = [len(x) for x in issues_a] # sum(issues_a_n) # 315
# [14, 12, 13, 12, 12, 12, 12, 13, 12, 12, 12, 12, 12, 12, 12, 
#  12, 12, 12, 12, 12, 12, 12, 12,  7,  6,  6,  4,  4,  4,  4]

issues_dframe = pd.DataFrame([process_aTag(aTag, url) 
   for a_list, url in zip(issues_a, volumes_dframe.url) 
   for aTag in a_list])
# (315, 7)
# issues_dframe.loc[:, ['path', 'string']]
#                                             path             string
# 0    /eid/articles/issue/30/12/table-of-contents  Table of Contents
# 1    /eid/articles/issue/30/11/table-of-contents  Table of Contents
# 2    /eid/articles/issue/30/14/table-of-contents  Table of Contents
# 3    /eid/articles/issue/30/10/table-of-contents  Table of Contents
# 4     /eid/articles/issue/30/9/table-of-contents  Table of Contents
# ..                                           ...                ...
# 310    /eid/articles/issue/2/1/table-of-contents  Table of Contents
# 311    /eid/articles/issue/1/4/table-of-contents  Table of Contents
# 312    /eid/articles/issue/1/3/table-of-contents  Table of Contents
# 313    /eid/articles/issue/1/2/table-of-contents  Table of Contents
# 314    /eid/articles/issue/1/1/table-of-contents  Table of Contents

issues_repeated = { 
   label: content.loc[content.duplicated(keep = False)].index.to_list()
      for label, content 
      in issues_dframe.loc[:, ['href', 'url', 'path', 'filename']].items() }
# { k: len(v) for k, v in issues_repeated.items() }
# {'href': 0, 'url': 0, 'path': 0, 'filename': 315}

# pickle.dump(issues_dframe, open("issues_dframe.pkl", "wb"))

# issues_dframe.to_excel('eid-issues_dframe.xlsx', engine='openpyxl', freeze_panes=(1,0))
issues_html = [get_html_from_url(url, print_url=False, timeout=1) 
               for url in tqdm(issues_dframe.url)]
# 315/315 [02:38<00:00,  1.99it/s]
# sum([len(x)==0 for x in issues_html]) # 32
# no_html = [idx for idx, html in enumerate(issues_html) if len(html) == 0]
# check for failed requests -- those with length 0; repeat until there are none
for iss in tqdm(range(315)):
   if issues_html[iss] == '':
      issues_html[iss] = get_html_from_url(issues_dframe.url[iss], print_url=True, timeout=5)
# sum([len(x)==0 for x in issues_html]) # 0

# [len(x) for x in issues_html]
# [478627, 474856, 396141, 475403, 476158, 481383, 469056, 479126, 469057, ...]
issues_soup = [BeautifulSoup(html, 'lxml') for html in tqdm(issues_html)] #, total=315
# 315/315 [00:30<00:00, 10.46it/s]

# review all anchor-refs from issue URLs
# pd.DataFrame([process_aTag(aTag, url) 
#     for soup, url in zip(issues_soup, issues_dframe.url) 
#     for aTag in soup.find_all('a', href=True)])\
#     .to_excel('eid-issues-anchors.xlsx', engine='openpyxl', freeze_panes=(1,0))
# [77593 rows x 7 columns]

#%% 4. List of articles

# Review of anchor elements in volumes page, eid-issues-anchors.xlsx
# All 13640 article paths have form /eid/article/#0/#0/
#    For nearly all articles (13625), the path ends in '_article'
#    The exception is 15 photo quizzes, which we omit
# Most paths (13625) follow pattern '/\d{1,2}/\d{1,2}/\d{2}-\d{4}_article'

eid_art_re = re.compile(r'_article$')
articles_a = [soup.find_all('a', href=eid_art_re) for soup in issues_soup]
articles_a_n = [len(x) for x in articles_a] # sum(articles_a_n) # 13625

articles_dframe = pd.DataFrame([process_aTag(aTag, url) 
   for a_list, url in zip(articles_a, issues_dframe.url) 
   for aTag in a_list])
# (13625, 7)
# with pd.option_context("display.max_colwidth", 35):
#     display(articles_dframe.loc[:, ['path', 'string']])
#                                      path                              string
# 0      /eid/article/30/12/ac-3012_article                                    
# 1      /eid/article/30/12/24-0389_article  Homelessness and Organ Donor–De...
# 2      /eid/article/30/12/24-0310_article  Bartonella quintana|Infection i...
# 3      /eid/article/30/12/24-0538_article  Increase in Adult Patients with...
# 4      /eid/article/30/12/23-1659_article  Historical Assessment and Mappi...
#                                   ...                                 ...
# 13620    /eid/article/1/1/95-0108_article  Electronic Communication and th...
# 13621    /eid/article/1/1/ac-0101_article                   Volume 1, Issue 1
# 13622    /eid/article/1/1/95-0109_article  Communicable Diseases Intelligence
# 13623    /eid/article/1/1/95-0110_article  DxMONITOR: Compiling Veterinary...
# 13624    /eid/article/1/1/95-0111_article  WHO Scientific Working Group on...

# omit 315 entries where string is empty
# all these are "about the cover", which are linked twice each
articles_dframe = articles_dframe.loc[articles_dframe['string'] != ''].reset_index(drop=True)
# (13310, 7)

#%% 5. Complete list of EID files
eid_cc_df = pd.concat([
   home_dframe.assign(level='home'),
#  series_dframe.assign(level='series'), # omit as redundant with volumes
   volumes_dframe.assign(level='volume'),
   issues_dframe.assign(level='issue'),
   articles_dframe.assign(level='article')],
   axis = 0, ignore_index = True)
# (13656, 8)

# pickle
# pickle.dump(eid_cc_df, open("eid_cc_df.pkl", "xb"))
eid_cc_df.to_pickle('eid_cc_df.pkl')

eid_cc_df.to_excel('eid_cc_df.xlsx', engine='openpyxl', freeze_panes=(1,0))


#%%
# eid_cc_df = pickle.load(open("eid_cc_df.pkl", "rb"))
eid_cc_df = pd.read_pickle('pickle-files/eid_cc_df.pkl')

EID_BASE_PATH_b0 = normpath(expanduser('~/cdc-corpora/'))

x = create_mirror_tree(EID_BASE_PATH_b0, calculate_mirror_dirs(eid_cc_df.path))
# { key: (0 if val is None else len(val)) for (key, val) in x.items() }
# {'base_path': 1, 'paths': 632, 'norm_paths': 632}

vol_29_30 = (eid_cc_df.url.str.contains('/(29|30)/') | 
             eid_cc_df.url.str.contains('volume-(29|30)'))
eid_cc_df_ = eid_cc_df.loc[vol_29_30, :]
eid_sizes_b0 = [
    mirror_raw_html(url, EID_BASE_PATH_b0 + path, print_url = False, timeout = 8)
    for url, path in tqdm(zip(eid_cc_df_.url, eid_cc_df_.mirror_path),
                          total=len(eid_cc_df_.mirror_path))]
# sum([x==0 for x in eid_sizes_b0]) # retry those with 0 length
for j in len(eid_cc_df.mirror_path):
   if eid_sizes_b0[j] == 0:
      eid_sizes_b0[j] = mirror_raw_html(eid_cc_df.url[j], 
         EID_BASE_PATH_b0 + eid_cc_df.mirror_path[j], timeout=5)
# pickle.dump(eid_sizes_b0, open('eid_sizes_b0.pkl', 'wb'))
eid_sizes_b0 = pickle.load(open("eid_sizes_b0.pkl", "rb"))

#%% 6. Routine for reading all files into a single list
eid_html_b0 = [read_raw_html(EID_BASE_PATH_b0 + path)
               for path in tqdm(eid_cc_df_.mirror_path)]
# 1041/1041 [00:00<00:00, 2075.76it/s]
pickle.dump(eid_html_b0, open('eid_raw_html.pkl', 'xb'))
