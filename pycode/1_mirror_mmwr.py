#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Analyze the structure and broad properties of MMWR online archive

@author: cmheilig

Sections of this script, based on levels of MMWR archive:
0. MMWR home https://www.cdc.gov/mmwr/about.html
1. List and contents of series 
2. List and contents of volumes (issues are integrated into volumes)
3. List of articles
4. Complete list of MMWR files

Main product: mmwr_cc_df
"""

#%% Import modules and set up environment
# import from 0_cdc-corpora-header.py

os.chdir('/Users/cmheilig/cdc-corpora/_test')

#%% 0. Start with MMWR home https://www.cdc.gov/mmwr/about.html
base_url = 'https://www.cdc.gov/mmwr/about.html'
home_a = BeautifulSoup(get_html_from_url(base_url), 'lxml')\
   .find('a', href=re.compile('about.html'))
# process_aTag(home_a, base_url)
# {'base': 'https://www.cdc.gov/mmwr/about.html',
#  'href': '/mmwr/about.html',
#  'url': 'https://www.cdc.gov/mmwr/about.html',
#  'path': '/mmwr/about.html',
#  'filename': 'about.html',
#  'mirror_path': '/mmwr/about.html',
#  'string': 'About|MMWR'}

home_dframe = pd.DataFrame(process_aTag(home_a, base_url), index = [0])
home_html = get_html_from_url_(home_dframe.url[0]) # len(home_html) # 187622
home_soup = BeautifulSoup(home_html, 'lxml')

# review all anchor-hrefs from home URL
# len(home_soup.find_all('a', href=True)) # 130
pd.DataFrame([process_aTag(aTag, home_dframe.url[0]) 
    for aTag in home_soup.find_all('a', href=True)])\
    .to_excel('mmwr-home-anchors.xlsx', engine='openpyxl', freeze_panes=(1,0))
# [113 rows x 7 columns]

#%% 1. List and contents of series (and some volumes)
#      Series: WR, RR, SS, SU, ND, NNC (top)

# Review of anchor elements in home page, mmwr-home-anchors.xlsx
# limit to regexes for series-specific volume lists; omit ND, NNC
series_a = home_soup.find_all('a', string=re.compile('Past Volumes'))
# [<a href="/mmwr/mmwr_wk/wk_pvol.html">Past Volumes (1982-2023)</a>,
#  <a href="/mmwr/mmwr_rr/rr_pvol.html">Past Volumes (1990-2022)</a>,
#  <a href="/mmwr/mmwr_ss/ss_pvol.html">Past Volumes (1983-2022)</a>,
#  <a href="/mmwr/mmwr_su/index.html">Past Volumes (1985-2023)</a>]

series_dframe = pd.DataFrame(
   [process_aTag(aTag, home_dframe.url[0]) for aTag in series_a])
# series_dframe.loc[:, ['path', 'string']]
#                          path                    string
# 0  /mmwr/mmwr_wk/wk_pvol.html  Past Volumes (1982-2023)
# 1  /mmwr/mmwr_rr/rr_pvol.html  Past Volumes (1990-2022)
# 2  /mmwr/mmwr_ss/ss_pvol.html  Past Volumes (1983-2022)
# 3    /mmwr/mmwr_su/index.html  Past Volumes (1985-2023)

series_html = [get_html_from_url_(url) for url in series_dframe.url] # list of 4
# [len(x) for x in series_html]
# [184781, 184778, 185484, 182764]
series_soup = [BeautifulSoup(html, 'lxml') for html in tqdm(series_html)]

# review all anchor-hrefs from series URLs
# [len(soup.find_all('a', href=True)) for soup in series_soup]
# [149, 139, 141, 126] # sum(_) # 555
pd.DataFrame([process_aTag(aTag, url) 
    for soup, url in zip(series_soup, series_dframe.url) 
    for aTag in soup.find_all('a', href=True)])\
    .to_excel('mmwr-series-anchors.xlsx', engine='openpyxl', freeze_panes=(1,0))
# [555 rows x 7 columns]

#%% 2. List and contents of volumes

# Review of anchor elements in volumes page, mmwr-series-anchors.xlsx
# regexes for index files, i.e., volume-specific issue lists
mmwr_ind_re0 = re.compile(r'/(ind\w*\d{2,4}\w*\.html?)')
volumes_a = [soup.find_all('a', href=mmwr_ind_re0) for soup in series_soup]
# a list of 4 lists; make a single, concatenated list
volumes_a_n = [len(x) for x in volumes_a]
# [46, 37, 39, 23] # 145
# reorganize 4 nested lists as a sinlge list of 145

volumes_dframe = pd.DataFrame([process_aTag(aTag, url) 
   for a_list, url in zip(volumes_a, series_dframe.url)
   for aTag in a_list])
# [145 rows x 7 columns]
# volumes_dframe.loc[:, ['path', 'string']]
#                               path                       string
# 0             /mmwr/index2023.html             Volume 72 (2023)
# 1             /mmwr/index2022.html             Volume 71 (2022)
# 2             /mmwr/index2021.html             Volume 70 (2021)
# 3             /mmwr/index2020.html             Volume 69 (2020)
# 4             /mmwr/index2019.html             Volume 68 (2019)
# ..                             ...                          ...
# 140  /mmwr/preview/ind1985_su.html             Volume 34 (1985)
# 141           /mmwr/index2024.html                Weekly Report
# 142          /mmwr/indrr_2023.html  Recommendations and Reports
# 143          /mmwr/indss_2023.html       Surveillance Summaries
# 144          /mmwr/ind2024_su.html                  Supplements

# Check for duplicate paths and for items after 2023
volumes_review = (
    volumes_dframe.loc[volumes_dframe.path.duplicated(keep = False) |
                       volumes_dframe.path.str.contains('202[34]')].index) # 18
volumes_dframe.loc[volumes_review, ['base', 'url', 'string']]#.to_clipboard()

# keep    [ 0, 122]
volumes_dframe.loc[[0, 122], 'string']
# 0      Volume 72 (2023)
# 122    Volume 72 (2023)
# relabel [80, 120]
# volumes_dframe.loc[[80, 120], 'string']
# 80     Recommendations and Reports
# 120         Surveillance Summaries
volumes_dframe.loc[[80, 120], 'string'] = 'Volume 72 (2023)'
# volumes_dframe.loc[[0, 80, 120, 122], 'string']
# drop    [42, 43, 44, 45, 79, 81, 82, 118, 119, 121, 141, 142, 143, 144]
volumes_dframe = volumes_dframe.drop(
    [42, 43, 44, 45, 79, 81, 82, 118, 119, 121, 141, 142, 143, 144])

# check again
volumes_dframe.loc[volumes_dframe.path.duplicated(keep = False) |
                   volumes_dframe.path.str.contains('2024')].index # []

volumes_dframe.reset_index(inplace=True, drop=True) # (131, 7)

volumes_html = [get_html_from_url(url) for url in tqdm(volumes_dframe.url)] # list of 131
# 131/131 [00:40<00:00,  3.27it/s]
# [len(x) for x in volumes_html]
# [273063, 279023, 288435, 295012, 267595, 276375, 296625, 382754, 378712, ...]
volumes_soup = [BeautifulSoup(html, 'lxml') for html in tqdm(volumes_html)]
# 131/131 [00:03<00:00, 43.53it/s]

# review all anchor-hrefs from volumes URLs
# [len(soup.find_all('a', href=True)) for soup in volumes_soup]
# [494, 517, 579, 628, 498, 561, 708, 723, 668, 722, ...] # len 131, sum 31840
pd.DataFrame([process_aTag(aTag, url) 
    for soup, url in zip(volumes_soup, volumes_dframe.url) 
    for aTag in soup.find_all('a', href=True)])\
    .to_excel('mmwr-volumes-anchors.xlsx', engine='openpyxl', freeze_panes=(1,0))
# [31840 rows x 7 columns]

#%% 3. List of articles

# Review of anchor elements in volumes page, mmwr-volumes-anchors.xlsx
# all article URLs contain /preview/mmwrhtml/ or /volumes/ and end with .htm
mmwr_art_re0 = re.compile(r'(mmwrhtml|volumes)/(\w|-|/)+.html?')
articles_a = [soup.find_all('a', href=mmwr_art_re0) for soup in tqdm(volumes_soup)]
articles_a_n = [len(x) for x in articles_a]
# sum(articles_a_n) # 15171
# reorganize 131 nested lists as a single list of 15171

articles_dframe = pd.DataFrame([process_aTag(aTag, url) 
   for a_list, url in zip(articles_a, volumes_dframe.url)
   for aTag in a_list])
# articles_dframe.shape # (15171, 7)
with pd.option_context("display.max_colwidth", 36):
    display(articles_dframe.loc[:, ['path', 'string']])
#                                       path                               string
# 0       /mmwr/volumes/72/wr/mm725253a1.htm  Second Nationwide Tuberculosis O...
# 1       /mmwr/volumes/72/wr/mm725253a2.htm  Notes from the Field|: Supply In...
# 2       /mmwr/volumes/72/wr/mm725253a5.htm  Notes from the Field|: Seizures,...
# 3       /mmwr/volumes/72/wr/mm725253a6.htm  QuickStats|: Rate of Triplet and...
# 4         /mmwr/volumes/72/wr/mm7251a1.htm  SARS-CoV-2 Rebound With and With...
#                                     ...                                  ...
# 15166  /mmwr/preview/mmwrhtml/00026330.htm  Guidelines for the Prevention an...
# 15167  /mmwr/preview/mmwrhtml/00014715.htm  Human Immunodeficiency Virus Inf...
# 15168  /mmwr/preview/mmwrhtml/00023587.htm  Recommendations for Prevention o...
# 15169  /mmwr/preview/mmwrhtml/00001773.htm  Premature Mortality in the Unite...
# 15170  /mmwr/preview/mmwrhtml/00001712.htm  Summaries of Current Intelligenc...

# articles_dframe.to_excel("articles_dframe.xlsx", engine='openpyxl', freeze_panes=(1,0))

articles_review = articles_dframe.loc[articles_dframe.path.duplicated(keep = False)].index # 18
articles_dframe.loc[articles_review, ['base', 'url', 'string']]#.to_clipboard()
# articles_dframe.loc[articles_review, :].to_clipboard()
[ 5123,  5166,  7888,  7889,  8666,  8673,  8682,  8689,  9040,
  9066,  9396,  9397, 14073, 14075, 14804, 14807, 15049, 15050],

# keep    [5123, 7888, 8689, 9066, 14073, 14807]
# relabel [9396, 15049]
articles_dframe.loc[9396, 'string'] = 'Staphylococcus aureus with Reduced Susceptibility to Vancomycin --- Illinois, 1999'
articles_dframe.loc[15049, 'string'] = 'Advisory Committee on Immunization Practices (ACIP) Recommended Immunization Schedules for Persons Aged 0 Through 18 Years and Adults Aged 19 Years and Older — United States, 2013'
# articles_dframe.loc[[9396, 15049], 'string']
# drop    [5166, 7889, 8666, 8673, 8682, 9040, 9397, 14075, 14804, 15050]
articles_dframe = articles_dframe.drop(
   [5166, 7889, 8666, 8673, 8682, 9040, 9397, 14075, 14804, 15050])
# articles_dframe.loc[articles_dframe.path.duplicated(keep = False)].index # []
articles_dframe.reset_index(inplace=True, drop=True)


#%% 4. Complete list of MMWR HTML files
mmwr_cc_df = pd.concat([
   home_dframe.assign(level='home'),
   series_dframe.assign(level='series'),
   volumes_dframe.assign(level='volume'),
   articles_dframe.assign(level='article')],
   axis = 0, ignore_index = True)
# (15297, 8)

# pickle
# pickle.dump(mmwr_cc_df, open('mmwr_cc_df.pkl', 'xb'))
mmwr_cc_df.to_pickle('mmwr_cc_df.pkl')

# Excel; coulad also use engine=
mmwr_cc_df.to_excel('mmwr_cc_df.xlsx', engine='openpyxl', freeze_panes=(1,0))

#%% 5. Mirror MMWR to local disk
##  Retrieve journal-specific DataFrames from pickle files
# mmwr_cc_df = pickle.load(open('pickle-files/mmwr_cc_df.pkl', 'rb'))
mmwr_cc_df = pd.read_pickle('pickle-files/mmwr_cc_df.pkl')
# (15297, 8)

MMWR_BASE_PATH_b0 = normpath(expanduser('~/cdc-corpora'))

x = create_mirror_tree(MMWR_BASE_PATH_b0, calculate_mirror_dirs(mmwr_cc_df.path))
# { key: (0 if val is None else len(val)) for (key, val) in x.items() }

mmwr_sizes_b0 = [
    mirror_raw_html(url, MMWR_BASE_PATH_b0 + path, print_url = False)
    for url, path in tqdm(zip(mmwr_cc_df.url, mmwr_cc_df.mirror_path), 
                          total=len(mmwr_cc_df.mirror_path))]

# sum([x==0 for x in mmwr_sizes_b0]) # retry those with 0 length
for j in tqdm(range(len(mmwr_sizes_b0))):
   if mmwr_sizes_b0[j] == 0:
      mmwr_sizes_b0[j] = mirror_raw_html(mmwr_cc_df.url.loc[j], 
         MMWR_BASE_PATH_b0 + mmwr_cc_df.mirror_path.loc[j], timeout=5)
# pickle.dump(mmwr_sizes_b0, open('mmwr_sizes_b0.pkl', 'wb'))

#%% 6. Routine for reading all files into a single list
mmwr_html_b0 = [read_raw_html(MMWR_BASE_PATH_b0 + path)
                for path in tqdm(mmwr_cc_df.mirror_path)]
# 15297/15297 [00:05<00:00, 2604.24it/s]
pickle.dump(mmwr_html_b0, open('mmwr_raw_html.pkl', 'xb'))
