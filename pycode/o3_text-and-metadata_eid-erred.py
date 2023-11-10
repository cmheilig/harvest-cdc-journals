#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 19 18:32:11 2021

@author: cmheilig
"""

from bs4 import SoupStrainer

EID_BASE_PATH_b0 = normpath(expanduser('~/cdc-corpora/eid_b0/'))
x = create_mirror_tree(EID_BASE_PATH_b0, calculate_mirror_dirs(eid_dframe.path))
EID_BASE_PATH_u3 = normpath(expanduser('~/cdc-corpora/eid_u3/'))
x = create_mirror_tree(EID_BASE_PATH_u3, calculate_mirror_dirs(eid_dframe.path))

EID_BASE_PATH_u3 = normpath(expanduser('~/cdc-corpora/eid_u3_20210218/'))
eid_uni_html = [html_reduce_space_u(read_uni_html(EID_BASE_PATH_u3 + path))
                     for path in tqdm(eid_dframe.mirror_path)]
# 11504/11504 [00:09<00:00, 1153.86it/s]
# pickle.dump(eid_html_u3, open('eid_uni_html.pkl', 'wb'))
# pickle.dump(eid_uni_html, open('eid_uni_html.pkl', 'wb'))
# eid_uni_html = pickle.load(open('eid_uni_html_20210218.pkl', 'rb'))

eid_art_html = [html_reduce_space_u(read_uni_html(EID_BASE_PATH_u3 + path))
                     for path in tqdm(eid_art_frame.mirror_path)]
# 11211/11211 [02:07<00:00, 88.07it/s]
# pickle.dump(eid_art_html, open('eid_art_html.pkl', 'wb'))
# eid_art_html = pickle.load(open('eid_art_html.pkl', 'rb'))

only_title = SoupStrainer(name='title')

eid_uni_titles = [BeautifulSoup(html, 'lxml', parse_only=only_title).title.string.strip() 
                for html in tqdm(eid_uni_html)]
# pickle.dump(eid_uni_titles, open('eid_uni_titles.pkl', 'wb'))
# eid_uni_titles = pickle.load(open('eid_uni_titles.pkl', 'rb'))
eid_art_titles = [BeautifulSoup(html, 'lxml', parse_only=only_title).title.string.strip() 
                for html in tqdm(eid_art_html)]
# pickle.dump(eid_uni_titles, open('eid_art_titles.pkl', 'wb'))
# eid_art_titles = pickle.load(open('eid_art_titles.pkl', 'rb'))

sum([ title in ['500 - Emerging Infectious Diseases journal',
   'CDC - Website Temporarily Unavailable'] for title in eid_uni_titles ])

# Check for nonunique titles as a screen for errors
z_uni = { w: eid_uni_titles.count(w) for w in sorted(set(eid_uni_titles)) }
z_art = { w: eid_art_titles.count(w) for w in sorted(set(eid_art_titles)) }
# print([ v for v in z_uni.values() if v > 1 ]) # [260, 2, 2, 2, 2, 53, 2, ... ]

z_uni_freq = { k: v for (k, v) in z_uni.items() if v > 1 } # length 38
z_art_freq = { k: v for (k, v) in z_art.items() if v > 1 } # length 38
# z_uni_freq == z_art_freq # True
# { w: list(z_uni_freq.values()).count(w) for w in sorted(set(z_uni_freq.values())) }
# {2: 30, 3: 5, 4: 1, 53: 1, 260: 1} # focus on titles that occur 53 or 260 times
{ k: v for (k, v) in z_art.items() if v > 4 }
# {'500 - Emerging Infectious Diseases journal': 260,
#  'CDC - Website Temporarily Unavailable': 53}

l_uni = [len(v) for v in eid_uni_titles]
l_art = [len(v) for v in eid_art_titles]

{ w: l_uni.count(w) for w in sorted(set(l_uni)) }
{ w: l_art.count(w) for w in sorted(set(l_art)) }

# [ v for v in z_uni.values() if v > 1 ]
{ k: v for (k, v) in z_uni.items() if v > 1 } == \
   { k: v for (k, v) in z_art.items() if v > 1 } # True # only articles erred

# z_uni or z_art where count > 1

# limit to articles where 
#    <title> is '500 - Emerging Infectious Diseases journal' or 'CDC - Website Temporarily Unavailable'

#%%

# Iterate over all u3 HTML files in eid_dframe
# 1. Read HTML file from disk to string in memory
# 2. Parse soup and extract only soup.title.strip()
# 3. Apply condition to detect titles indicative of errors
#    '500 - Emerging Infectious Diseases journal' or 
#    'CDC - Website Temporarily Unavailable'
#    anything else?
# Iterate over eid_dframe rows corresponding to erroneous titles
# 4. Attempt to retrieve b0 from web
#    If length-0 retrieved, try again
#    If length > 0, check soup.title
#       If title indicative of error, try again
#       Else write b0, write u3

EID_BASE_PATH_b0 = normpath(expanduser('~/cdc-corpora/eid_b0/'))
EID_BASE_PATH_u3 = normpath(expanduser('~/cdc-corpora/eid_u3/'))

uni_redo_tf = [ title in ['500 - Emerging Infectious Diseases journal',
   'CDC - Website Temporarily Unavailable'] for title in eid_uni_titles ]
eid_dframe_x = eid_dframe.loc[uni_redo_tf]

eid_sizes_x_b0 = [mirror_raw_html(url, EID_BASE_PATH_b0 + path, print_url = False, timeout = 8)
                     for url, path in tqdm(zip(eid_dframe_x.url, eid_dframe_x.mirror_path),
                                           total=313)]

eid_html_x_b0 = [read_raw_html(EID_BASE_PATH_b0 + path)
                     for path in tqdm(eid_dframe_x.mirror_path)]

for path in tqdm(eid_dframe_x.mirror_path):
   mirror_raw_to_uni(EID_BASE_PATH_b0 + path, EID_BASE_PATH_u3 + path, counter=None)
