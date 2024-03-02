#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Retrieve and store local representation of MMWR online with minimal processing,
which can include conversion to UTF-8 and basic parsing of HTML.

Update previous mirrors
  EID  2021-02-17 -> 2023-11-10
  MMWR 2022-05-27 -> 2023-11-11
  PCD  2021-02-17 -> 2023-11-10

@author: chadheilig

Begin with journal-specific dframe, containing a complete list of files.
MMWR, PCD, EID, PHR

_dframe/_update pandas DataFrame
   base     URL from which href values were harvested
   href     hypertext reference (bs.a['href']); + base URL -> absolute URL
   url      absolute URL constructed from href and base URL
   path     path from absolute URL
   filename name of HTML file in hypertext reference
   mirror_path path on local mirror
   string   string from anchor element content
   level    in concatentated DataFrame, volume or article

Main products: local mirror of online archive as raw copy (bytes) and lightly
formatted UTF-8 (string), as well as pickle representations for ease of 
continuity. See also 2_retrieve-and-store-experiments.py for timing trials.
"""

#%% Import modules and set up environment
# import from 0_cdc-corpora-header.py

os.chdir('/Users/cmheilig/cdc-corpora/_test')

#%% Retrieve journal-specific DataFrames from pickle files

## MMWR 2022-05-27 -> 2023-11-11
mmwr_dframe_20220527 = pickle.load(open('pickle-files/mmwr_dframe_20220527.pkl', 'rb'))
# (14751, 8)
mmwr_dframe_20231111 = pickle.load(open('pickle-files/mmwr_dframe_20231111.pkl', 'rb'))
# (15258, 8)
# mmwr_pdf_dframe = pickle.load(open("pickle-files/mmwr_pdf_dframe.pkl", "rb"))
# (2066, 9)

mmwr_dframe_20220527.level.value_counts(sort=False) # 14620 articles
mmwr_dframe_20231111.level.value_counts(sort=False) # 15122 articles

mmwr_url_20220527 = set(mmwr_dframe_20220527
                        .loc[mmwr_dframe_20220527['level']=='article', 'url']
                        .to_list())
mmwr_url_20231111 = set(mmwr_dframe_20231111
                        .loc[mmwr_dframe_20231111['level']=='article', 'url']
                        .to_list())
len(mmwr_url_20220527 - mmwr_url_20231111) # 0
len(mmwr_url_20231111 - mmwr_url_20220527) # 502

mmwr_update = mmwr_dframe_20231111[
    mmwr_dframe_20231111.url.isin(mmwr_url_20231111 - mmwr_url_20220527)]
# [502 rows x 8 columns]

## EID  2021-02-17 -> 2023-11-10
eid_dframe_20210217 = pickle.load(open('pickle-files/eid_dframe_20210217.pkl', 'rb'))
# (11504, 8)
eid_dframe_20231110 = pickle.load(open('pickle-files/eid_dframe_20231110.pkl', 'rb'))
# (13020, 8)

eid_dframe_20210217.level.value_counts(sort=False) # 11211 articles
eid_dframe_20231110.level.value_counts(sort=False) # 12691 articles

eid_url_20220527 = set(eid_dframe_20210217
                        .loc[eid_dframe_20210217['level']=='article', 'url']
                        .to_list())
eid_url_20231110 = set(eid_dframe_20231110
                        .loc[eid_dframe_20231110['level']=='article', 'url']
                        .to_list())
len(eid_url_20220527 - eid_url_20231110) # 0
len(eid_url_20231110 - eid_url_20220527) # 1480

eid_update = eid_dframe_20231110[
    eid_dframe_20231110.url.isin(eid_url_20231110 - eid_url_20220527)]
# [1480 rows x 8 columns]

## PCD  2021-02-17 -> 2023-11-10
pcd_dframe_20210217 = pickle.load(open('pickle-files/pcd_dframe_20210217.pkl', 'rb'))
# (4485, 8)
pcd_dframe_20231110 = pickle.load(open('pickle-files/pcd_dframe_20231110.pkl', 'rb'))
# (4772, 8)

pcd_dframe_20210217.level.value_counts(sort=False) # 3691 articles
pcd_dframe_20231110.level.value_counts(sort=False) # 3976 articles

pcd_url_20220527 = set(pcd_dframe_20210217
                        .loc[pcd_dframe_20210217['level']=='article', 'url']
                        .to_list())
pcd_url_20231110 = set(pcd_dframe_20231110
                        .loc[pcd_dframe_20231110['level']=='article', 'url']
                        .to_list())
len(pcd_url_20220527 - pcd_url_20231110) # 0
len(pcd_url_20231110 - pcd_url_20220527) # 285

pcd_update = pcd_dframe_20231110[
    pcd_dframe_20231110.url.isin(pcd_url_20231110 - pcd_url_20220527)]
# [285 rows x 8 columns]


#%% Set up local mirror directories for unprocessed HTML (b0)
MMWR_BASE_PATH_b0 = normpath(expanduser('~/cdc-corpora/mmwr_b0_update/'))
# MMWR_BASE_PATH_pdf = normpath(expanduser('~/cdc-corpora/mmwr_pdf/'))
EID_BASE_PATH_b0 = normpath(expanduser('~/cdc-corpora/eid_b0_update/'))
PCD_BASE_PATH_b0 = normpath(expanduser('~/cdc-corpora/pcd_b0_update/'))
# PHR_BASE_PATH_b0 = normpath(expanduser('~/cdc-corpora/phr_b0/'))

x = create_mirror_tree(MMWR_BASE_PATH_b0, calculate_mirror_dirs(mmwr_update.path))
# { key: (0 if val is None else len(val)) for (key, val) in x.items() }

x = create_mirror_tree(EID_BASE_PATH_b0, calculate_mirror_dirs(eid_update.path))
# { key: (0 if val is None else len(val)) for (key, val) in x.items() }

x = create_mirror_tree(PCD_BASE_PATH_b0, calculate_mirror_dirs(pcd_update.path))
# { key: (0 if val is None else len(val)) for (key, val) in x.items() }

#%% Mirror unprocessed HTML from internet to local archive (www -> b0)

## MMWR
mmwr_sizes_b0 = [mirror_raw_html(url, MMWR_BASE_PATH_b0 + path, print_url = False)
                     for url, path in tqdm(zip(mmwr_update.url, mmwr_update.mirror_path), 
                                           total=502)]
# 502/502 [02:54<00:00,  2.88it/s]

# sum([x==0 for x in mmwr_sizes_b0]) # retry those with 0 length
for j in tqdm(range(502)):
   if mmwr_sizes_b0[j] == 0:
      mmwr_sizes_b0[j] = mirror_raw_html(mmwr_update.url.iloc[j], 
         MMWR_BASE_PATH_b0 + mmwr_update.mirror_path.iloc[j], timeout=5)
# pickle.dump(mmwr_sizes_b0, open('mmwr_sizes_b0.pkl', 'wb'))

## EID
eid_sizes_b0 = [mirror_raw_html(url, EID_BASE_PATH_b0 + path, print_url = False, timeout = 8)
                     for url, path in tqdm(zip(eid_update.url, eid_update.mirror_path),
                                           total=1480)]
# sum([x==0 for x in eid_sizes_b0]) # retry those with 0 length
for j in range(1480):
   if eid_sizes_b0[j] == 0:
      eid_sizes_b0[j] = mirror_raw_html(eid_update.url.iloc[j], 
         EID_BASE_PATH_b0 + eid_update.mirror_path.iloc[j], timeout=5)
# pickle.dump(eid_sizes_b0, open('eid_sizes_b0.pkl', 'wb'))

## PCD
pcd_sizes_b0 = [mirror_raw_html(url, PCD_BASE_PATH_b0 + path, print_url = False)
                     for url, path in tqdm(zip(pcd_update.url, pcd_update.mirror_path),
                                           total=285)]
# sum([x==0 for x in pcd_sizes_b0]) # retry those with 0 length
for j in range(285):
   if pcd_sizes_b0[j] == 0:
      pcd_sizes_b0[j] = mirror_raw_html(pcd_update.url.iloc[j], 
         PCD_BASE_PATH_b0 + pcd_update.mirror_path.iloc[j], timeout=5)
# pickle.dump(pcd_sizes_b0, open('pcd_sizes_b0.pkl', 'wb'))


#%% Read unprocessed HTML from local mirror; store in pickle format

## MMWR
mmwr_html_b0 = [read_raw_html(MMWR_BASE_PATH_b0 + path)
                     for path in tqdm(mmwr_update.mirror_path)]
# 502/502 [00:00<00:00, 6974.74it/s]
pickle.dump(mmwr_html_b0, open('mmwr_raw_html.pkl', 'wb'))

## EID
eid_html_b0 = [read_raw_html(EID_BASE_PATH_b0 + path)
                     for path in tqdm(eid_update.mirror_path)]
# 1480/1480 [00:00<00:00, 4987.28it/s]
pickle.dump(eid_html_b0, open('eid_raw_html.pkl', 'wb'))

## PCD
pcd_html_b0 = [read_raw_html(PCD_BASE_PATH_b0 + path)
                     for path in tqdm(pcd_update.mirror_path)]
# 285/285 [00:00<00:00, 6853.60it/s]
pickle.dump(pcd_html_b0, open('pcd_raw_html.pkl', 'wb'))


#%% Set up local mirror directories for lightly processed HTML (u3)

MMWR_BASE_PATH_u3 = normpath(expanduser('~/cdc-corpora/mmwr_u3_update/'))
PCD_BASE_PATH_u3 = normpath(expanduser('~/cdc-corpora/pcd_u3_update/'))
EID_BASE_PATH_u3 = normpath(expanduser('~/cdc-corpora/eid_u3_update/'))
# PHR_BASE_PATH_u3 = normpath(expanduser('~/cdc-corpora/phr_u3/'))

x = create_mirror_tree(MMWR_BASE_PATH_u3, calculate_mirror_dirs(mmwr_update.path))
# { key: (0 if val is None else len(val)) for (key, val) in x.items() }

x = create_mirror_tree(EID_BASE_PATH_u3, calculate_mirror_dirs(eid_update.path))
# { key: (0 if val is None else len(val)) for (key, val) in x.items() }

x = create_mirror_tree(PCD_BASE_PATH_u3, calculate_mirror_dirs(pcd_update.path))
# { key: (0 if val is None else len(val)) for (key, val) in x.items() }

#%% Mirror unprocessed HTML to processed HTML (b0 -> u3)

for path in tqdm(mmwr_update.mirror_path):
   mirror_raw_to_uni(MMWR_BASE_PATH_b0 + path, MMWR_BASE_PATH_u3 + path, counter=None)
# 502/502 [00:36<00:00, 13.61it/s]

for path in tqdm(eid_update.mirror_path):
   mirror_raw_to_uni(EID_BASE_PATH_b0 + path, EID_BASE_PATH_u3 + path, counter=None)
# 1480/1480 [02:21<00:00, 10.49it/s]

for path in tqdm(pcd_update.mirror_path):
   mirror_raw_to_uni(PCD_BASE_PATH_b0 + path, PCD_BASE_PATH_u3 + path, counter=None)
# 285/285 [00:25<00:00, 11.24it/s]


#%% Read lightly processed HTML from local mirror; store in pickle format

mmwr_html_u3 = [read_uni_html(MMWR_BASE_PATH_u3 + path)
                     for path in tqdm(mmwr_update.mirror_path)]
# 502/502 [00:00<00:00, 2874.47it/s]
pickle.dump(mmwr_html_u3, open('mmwr_uni_html.pkl', 'wb'))

pcd_html_u3 = [read_uni_html(PCD_BASE_PATH_u3 + path)
                     for path in tqdm(pcd_update.mirror_path)]
# 285/285 [00:00<00:00, 2527.85it/s]
pickle.dump(pcd_html_u3, open('pcd_uni_html.pkl', 'wb'))

eid_html_u3 = [read_uni_html(EID_BASE_PATH_u3 + path)
                     for path in tqdm(eid_update.mirror_path)]
# 1480/1480 [00:00<00:00, 1890.18it/s]
pickle.dump(eid_html_u3, open('eid_uni_html.pkl', 'wb'))
