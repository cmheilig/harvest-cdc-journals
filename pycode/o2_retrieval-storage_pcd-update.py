#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Retrieve and store local representation of MMWR online with minimal processing,
which can include conversion to UTF-8 and basic parsing of HTML.

@author: chadheilig

Begin with journal-specific dframe, containing a complete list of files.
MMWR, PCD, EID, PHR

_dframe pandas DataFrame
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

os.chdir('/Users/chadheilig/cdc-corpora/_test')

#%% Retrieve journal-specific DataFrames from pickle files
pcd_dframe_old = pickle.load(open('pickle-files/pcd_dframe_202004171206.pkl', 'rb'))
# (3627, 8)
pcd_dframe = pickle.load(open('pcd_dframe.pkl', 'rb'))
# 4335

url_old = sorted(set(pcd_dframe_old.url)) # 3625
mirror_path_old = sorted(set(pcd_dframe_old.mirror_path)) # 3625
url_new = sorted(set(pcd_dframe.url)) # 3627
mirror_path_new = sorted(set(pcd_dframe.mirror_path)) # 3627

url_add = set(url_new).difference(url_old)
# {'https://www.cdc.gov/pcd/issues/2012/11_0345_es.htm',
#  'https://www.cdc.gov/pcd/issues/2012/12_0010_es.htm'}
mirror_path_add = set(mirror_path_new).difference(mirror_path_old)
# {'/pcd/issues/2012/11_0345_es.htm', '/pcd/issues/2012/12_0010_es.htm'}

#%% Set up local mirror directories for unprocessed HTML (b0)

PCD_BASE_PATH_b0 = normpath(expanduser('~/cdc-corpora/pcd_b0/'))

#%% Mirror unprocessed HTML from internet to local archive (www -> b0)

pcd_sizes_b0 = [mirror_raw_html(url, PCD_BASE_PATH_b0 + path, print_url=False)
                     for url, path in zip(url_add, mirror_path_add)]
# [26211, 26140]

# pickle.dump(pcd_sizes_b0, open('pcd_sizes_b0.pkl', 'wb'))

#%% Read unprocessed HTML from local mirror; store in pickle format

pcd_html_b0 = [read_raw_html(PCD_BASE_PATH_b0 + path)
                     for path in mirror_path_add]
# pickle.dump(pcd_html_b0, open('pcd_raw_html.pkl', 'wb'))

#%% Set up local mirror directories for lightly processed HTML (u3)

PCD_BASE_PATH_u3 = normpath(expanduser('~/cdc-corpora/pcd_u3/'))

#%% Mirror unprocessed HTML to processed HTML (b0 -> u3)

for path in mirror_path_add:
   mirror_raw_to_uni(PCD_BASE_PATH_b0 + path, PCD_BASE_PATH_u3 + path, counter=None)

#%% Read lightly processed HTML from local mirror; store in pickle format

pcd_html_u3 = [read_uni_html(PCD_BASE_PATH_u3 + path)
                     for path in mirror_path_add]
# pickle.dump(pcd_html_u3, open('pcd_uni_html.pkl', 'wb'))
