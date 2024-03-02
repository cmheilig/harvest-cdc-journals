#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Analyze the structure and broad properties of MMWR online archive

@author: chadheilig

Sections of this script, based on levels of MMWR archive:
0. MMWR home https://www.cdc.gov/mmwr/about.html
1. List and contents of series 
2. List and contents of volumes (issues are integrated into volumes)
3. List of articles
4. Complete list of MMWR files

Main product: mmwr_dframe
"""

#%% Import modules and set up environment
# import from 0_cdc-corpora-header.py

os.chdir('/Users/cmheilig/cdc-corpora/_test')

#%% 0. Start with MMWR home https://www.cdc.gov/mmwr/about.html
base_url = 'https://www.cdc.gov/mmwr/about.html'
home_a = BeautifulSoup(get_html_from_url(base_url), 'lxml').\
   find('a', href=re.compile('about.html'))
# process_aTag(home_a, base_url)
# {'base': 'https://www.cdc.gov/mmwr/about.html',
#  'href': '/mmwr/about.html',
#  'url': 'https://www.cdc.gov/mmwr/about.html',
#  'path': '/mmwr/about.html',
#  'filename': 'about.html',
#  'mirror_path': '/mmwr/about.html',
#  'string': 'About|MMWR'}

home_dframe = pd.DataFrame(process_aTag(home_a, base_url), index = [0])
home_html = get_html_from_url_(home_dframe.url[0]) # len(home_html) # 194413
home_soup = BeautifulSoup(home_html, 'lxml')

# review all anchor-hrefs from home URL
# len(home_soup.find_all('a', href=True)) # 130
pd.DataFrame([process_aTag(aTag, home_dframe.url[0]) 
    for aTag in home_soup.find_all('a', href=True)]).\
    to_excel('mmwr-home-anchors.xlsx', engine='openpyxl', freeze_panes=(1,0))
# [130 rows x 7 columns]

#%% 1. List and contents of series (and some volumes)
#      Series: WR, RR, SS, SU, ND, NNC (top)

# Review of anchor elements in home page, mmwr-home-anchors.xlsx
# limit to regexes for series-specific volume lists; omit ND, NNC
series_a = home_soup.find_all('a', string=re.compile('Past Volumes'))
# [<a href="/mmwr/mmwr_wk/wk_pvol.html">Past Volumes (1982-2021)</a>,
#  <a href="/mmwr/mmwr_rr/rr_pvol.html">Past Volumes (1990-2020)</a>,
#  <a href="/mmwr/mmwr_ss/ss_pvol.html">Past Volumes (1983-2021)</a>,
#  <a href="/mmwr/mmwr_su/index.html">Past Volumes (1985-2020)</a>]

series_dframe = pd.DataFrame(
   [process_aTag(aTag, home_dframe.url[0]) for aTag in series_a])
# series_dframe.loc[:, ['path', 'string']]
#                          path                    string
# 0  /mmwr/mmwr_wk/wk_pvol.html  Past Volumes (1982-2021)
# 1  /mmwr/mmwr_rr/rr_pvol.html  Past Volumes (1990-2020)
# 2  /mmwr/mmwr_ss/ss_pvol.html  Past Volumes (1983-2021)
# 3    /mmwr/mmwr_su/index.html  Past Volumes (1985-2020)

# pool = multiprocessing.Pool(processes=multiprocessing.cpu_count() * 1) # * 3
# series_html = list(pool.imap(get_html_from_url_, series_dframe.url)) # list of 4
series_html = [get_html_from_url_(url) for url in series_dframe.url] # list of 4
# [len(x) for x in series_html]
# [192509, 191602, 192541, 190416]
series_soup = [BeautifulSoup(html, 'lxml') for html in tqdm(series_html)]

# review all anchor-hrefs from series URLs
# [len(soup.find_all('a', href=True)) for soup in series_soup]
# [165, 156, 159, 142] # sum(_) # 622
# pd.DataFrame([process_aTag(aTag, url) 
#     for soup, url in zip(series_soup, series_dframe.url) 
#     for aTag in soup.find_all('a', href=True)]).\
#     to_excel('mmwr-series-anchors.xlsx', engine='openpyxl', freeze_panes=(1,0))
# [426 rows x 7 columns]

#%% 2. List and contents of volumes

# Review of anchor elements in volumes page, mmwr-series-anchors.xlsx
# regexes for index files, i.e., volume-specific issue lists
mmwr_ind_re0 = re.compile(r'/(ind\w*\d{2,4}\w*\.html?)')
volumes_a = [soup.find_all('a', href=mmwr_ind_re0) for soup in series_soup]
# a list of 4 lists; make a single, concatenated list
volumes_a_n = [len(x) for x in volumes_a]
# [45, 37, 39, 22] # 143
# reorganize 4 nested lists as a sinlge list of 143

volumes_dframe = pd.DataFrame([process_aTag(aTag, url) 
   for a_list, url in zip(volumes_a, series_dframe.url)
   for aTag in a_list])
# [143 rows x 7 columns]
# volumes_dframe.loc[:, ['path', 'string']]
#                               path                       string
# 0             /mmwr/index2022.html             Volume 71 (2022)
# 1             /mmwr/index2021.html             Volume 70 (2021)
# 2             /mmwr/index2020.html             Volume 69 (2020)
# 3             /mmwr/index2019.html             Volume 68 (2019)
# 4             /mmwr/index2018.html             Volume 67 (2018)
# ..                             ...                          ...
# 138  /mmwr/preview/ind1985_su.html             Volume 34 (1985)
# 139           /mmwr/index2023.html                Weekly Report
# 140          /mmwr/indrr_2023.html  Recommendations and Reports
# 141          /mmwr/indss_2023.html       Surveillance Summaries
# 142          /mmwr/ind2023_su.html                  Supplements

# Check for duplicate values
# volumes_dframe.path.drop_duplicates() # drops from 143 to 127
volumes_repeated = volumes_dframe.loc[volumes_dframe.path.duplicated(keep = False)].index # (16,)
# 16 rows containing duplicate path values, with indices:
# [ 41,  42,  43,  44,  78,  79,  80,  81, 
#  117, 118, 119, 120, 139, 140, 141, 142]
volumes_dframe.loc[volumes_repeated, ['path', 'string']] 
# on inspection, keep indices 41, 42, 43, 44 - vol type corresponds to ser type
# delete 12 other indices
volumes_dframe = volumes_dframe.drop(
   [78, 79, 80, 81, 117, 118, 119, 120, 139, 140, 141, 142])
# Check again for duplicate values
# volumes_dframe.loc[volumes_dframe.path.duplicated(keep = False)].index # []
volumes_dframe.reset_index(inplace=True, drop=True) # (131, 7)

volumes_html = [get_html_from_url(url) for url in tqdm(volumes_dframe.url)] # list of 131
# 131/131 [00:40<00:00,  3.27it/s]
# [len(x) for x in volumes_html]
# [279591, 287193, 295440, 266187, 276324, 296574, 382703, 378661, 134350, ...]
volumes_soup = [BeautifulSoup(html, 'lxml') for html in tqdm(volumes_html)]
# 131/131 [00:03<00:00, 43.53it/s]

# review all anchor-hrefs from volumes URLs
# [len(soup.find_all('a', href=True)) for soup in volumes_soup]
# [517, 578, 629, 497, 561, 708, 723, 668, 722, 627, ...] # len 131, sum 31791
# pd.DataFrame([process_aTag(aTag, url) 
#     for soup, url in zip(volumes_soup, volumes_dframe.url) 
#     for aTag in soup.find_all('a', href=True)]).\
#     to_excel('mmwr-volumes-anchors.xlsx', engine='openpyxl', freeze_panes=(1,0))
# [31791 rows x 7 columns]

#%% 3. List of articles

# Review of anchor elements in volumes page, mmwr-volumes-anchors.xlsx
# all article URLs contain /preview/mmwrhtml/ or /volumes/ and end with .htm
mmwr_art_re0 = re.compile(r'(mmwrhtml|volumes)/(\w|-|/)+.html?')
articles_a = [soup.find_all('a', href=mmwr_art_re0) for soup in tqdm(volumes_soup)]
articles_a_n = [len(x) for x in articles_a]
# sum(articles_a_n) # 15132
# reorganize 131 nested lists as a single list of 15132

articles_dframe = pd.DataFrame([process_aTag(aTag, url) 
   for a_list, url in zip(articles_a, volumes_dframe.url)
   for aTag in a_list])
# articles_dframe.shape # (15132, 7)
with pd.option_context("display.max_colwidth", 36):
    display(articles_dframe.loc[:, ['path', 'string']])
#                                       path                               string
# 0         /mmwr/volumes/71/wr/mm7153a1.htm  Early Estimates of Bivalent mRNA...
# 1       /mmwr/volumes/71/wr/mm715152a1.htm  Epidemiologic and Clinical Featu...
# 2       /mmwr/volumes/71/wr/mm715152a2.htm  Demographic and Clinical Charact...
# 3       /mmwr/volumes/71/wr/mm715152e1.htm  Early Estimates of Bivalent mRNA...
# 4       /mmwr/volumes/71/wr/mm715152e2.htm  Early Estimates of Bivalent mRNA...
#                                     ...                                  ...
# 15127  /mmwr/preview/mmwrhtml/00026330.htm  Guidelines for the Prevention an...
# 15128  /mmwr/preview/mmwrhtml/00014715.htm  Human Immunodeficiency Virus Inf...
# 15129  /mmwr/preview/mmwrhtml/00023587.htm  Recommendations for Prevention o...
# 15130  /mmwr/preview/mmwrhtml/00001773.htm  Premature Mortality in the Unite...
# 15131  /mmwr/preview/mmwrhtml/00001712.htm  Summaries of Current Intelligenc...

# articles_dframe.to_excel("articles_dframe.xlsx", engine='openpyxl', freeze_panes=(1,0))

articles_repeated = { 
   label: content.loc[content.duplicated(keep = False)].index.to_list()
      for label, content 
      in articles_dframe.loc[:, ['href', 'url', 'path', 'filename', 'string']].items() }
# { k: len(v) for k, v in articles_repeated.items() }
# {'href': 16, 'url': 18, 'path': 18, 'filename': 18, 'string': 1888}
articles_dframe.iloc[articles_repeated['path'], 3] # 18 rows containing duplicate path values
articles_dframe.iloc[articles_repeated['path']].index
# [ 4807,  4850,  7572,  7573,  8350,  8357,  8366,  8373,  8724,
#   8750,  9080,  9081, 14060, 14062, 14785, 14788, 15010, 15011]
# on inspection, 
#   keep rows 4807, 7572, 8350,             8724, 9080, 14060, 14785, 15010
#   drop rows 4850, 7573, 8357, 8366, 8373, 8750, 9081, 14062, 14788, 15011
# articles_dframe.loc[[4807, 7572, 8350,             8724, 9080, 14060, 14785, 15010], 'path']
# articles_dframe.loc[[4850, 7573, 8357, 8366, 8373, 8750, 9081, 14062, 14788, 15011], 'path']
articles_dframe = articles_dframe.drop(
   [4850, 7573, 8357, 8366, 8373, 8750, 9081, 14062, 14788, 15011])
articles_dframe.reset_index(inplace=True, drop=True)


#%% 4. Complete list of MMWR HTML files
mmwr_dframe = pd.concat([
   home_dframe.assign(level='home'),
   series_dframe.assign(level='series'),
   volumes_dframe.assign(level='volume'),
   articles_dframe.assign(level='article')],
   axis = 0, ignore_index = True)
# (15258, 8)

# pickle
pickle.dump(mmwr_dframe, open("mmwr_dframe.pkl", "wb"))
# mmwr_dframe_ = pickle.load(open("mmwr_dframe.pkl", "rb"))
# mmwr_dframe.equals(mmwr_dframe_)

# Excel; coulad also use engine=
mmwr_dframe.to_excel('mmwr_dframe.xlsx', engine='openpyxl', freeze_panes=(1,0))
# Excelternatives
# mmwr_dframe.to_excel('mmwr_dframe.xlsx', engine='xlsxwriter') # pd default
# mmwr_dframe.to_excel('mmwr_dframe.xls', engine='xlwt')

#%% 5. Complete list of MMWR PDF files
mmwr_ind_pdf_re1 = re.compile(r'\w*\.pdf')
volumes_p_a = [soup.find_all('a', href=mmwr_ind_pdf_re1) for soup in series_soup]
# a list of 4 lists, all empty; [[], [], [], []]

# all occurrences of *.pdf in a subdirectory
mmwr_art_pdf_re1 = re.compile(r'.+?\.pdf')
articles_p_a = [soup.find_all('a', href=mmwr_art_pdf_re1) for soup in tqdm(volumes_soup)]
articles_p_a_n = [len(x) for x in articles_p_a]
# [51, 51, 52, 51, 51, 51, 0, 105, 103, 91, 51, 51, 51, 53, 51, 53, 52, 51, 52, 52, 51, 54, 51, 52, 53, 52, 52, 52, 52, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 21, 0, 0, 3, 0, 0, 0, 0, 0, 0, 6, 10, 5, 7, 12, 12, 10, 9, 17, 17, 15, 17, 19, 22, 16, 14, 20, 18, 15, 14, 15, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 14, 9, 10, 16, 10, 10, 13, 10, 12, 8, 9, 12, 11, 5, 10, 8, 5, 6, 6, 6, 3, 6, 0, 0, 0, 0, 0, 1, 3, 1, 4, 3, 4, 4, 2, 1, 1, 1, 1, 0, 0, 1, 0, 0]
# len(articles_p_a_n) # 126 # sum(articles_p_a_n) # 2148

articles_pdf_dframe = pd.DataFrame([process_aTag(aTag, url) 
   for a_list, url in zip(articles_p_a, volumes_dframe.url)
   for aTag in a_list])
# articles_pdf_dframe.shape # (2148, 7)
with pd.option_context("display.max_colwidth", 36):
    display(articles_pdf_dframe.loc[:, ['path', 'string']])
#                                      path                               string
# 0     /mmwr/volumes/70/wr/pdfs/mm70515...           PDF of this issue|pdf icon
# 1     /mmwr/volumes/70/wr/pdfs/mm7050-...           PDF of this issue|pdf icon
# 2     /mmwr/volumes/70/wr/pdfs/mm7049-...           PDF of this issue|pdf icon
# 3     /mmwr/volumes/70/wr/pdfs/mm7048-...           PDF of this issue|pdf icon
# 4     /mmwr/volumes/70/wr/pdfs/mm7047-...           PDF of this issue|pdf icon
#                                   ...                                  ...
# 2143            /mmwr/pdf/wk/mm54su01.pdf  Download.pdf document of this is...
# 2144            /mmwr/pdf/wk/mm53su01.pdf  Download .pdf document of this i...
# 2145            /mmwr/pdf/wk/mmSU5201.pdf  Download .pdf document of this i...
# 2146         /mmwr/pdf/other/highlite.pdf  Highlights in Public Health -|MM...
# 2147         /mmwr/pdf/other/mmsu3601.pdf  Revision of the CDC Surveillance...

mmwr_art_pdf_re2 = re.compile(r'(mm\d{4}md|highlite)\.pdf')
articles_pdf_dframe.loc[articles_pdf_dframe.filename.str.match(mmwr_art_pdf_re2), 'filename']

# articles_pdf_dframe.loc[articles_pdf_dframe.filename.str.match(mmwr_art_pdf_re2)]
# 146 x 7

articles_pdf_dframe = articles_pdf_dframe.loc[\
    ~(articles_pdf_dframe.filename.str.match(mmwr_art_pdf_re2) | \
      (articles_pdf_dframe.string == ''))]
# 1999 x 7

# articles_pdf_dframe.to_excel("articles_pdf_dframe.xlsx", engine='openpyxl', freeze_panes=(1,0))

articles_pdf_dframe['series'] = articles_pdf_dframe.filename.str[:2]
articles_pdf_dframe['volume'] = articles_pdf_dframe.filename.str[2:4]

# articles_pdf_dframe.loc[\
#     articles_pdf_dframe.filename.str.fullmatch('mm(501|su3601|SU5201).pdf'), 
#        ['url', 'series', 'volume']]

# ad hoc adjustments to volume number
# mm501 -> 54; mmsu3601 -> 36; mmSU5201 -> 52
articles_pdf_dframe.loc[\
    articles_pdf_dframe.filename.str.fullmatch('mm(501|su3601|SU5201).pdf'), 'volume'] = \
    ['54', '52', '36']
# 1999 x 9

# ad hoc inclusion of volume 64, as base files don't contain PDF hrefs
# base wk https://www.cdc.gov/mmwr/index2015.html
#      rr https://www.cdc.gov/mmwr/indrr_2015.html
#      ss https://www.cdc.gov/mmwr/indss_2015.html
# href wk /mmwr/pdf/wk/mm6401.pdf ... /mmwr/pdf/wk/mm6450.pdf, mm6452
#      rr /mmwr/pdf/rr/rr6401.pdf ... /mmwr/pdf/rr/rr6404.pdf
#      ss /mmwr/pdf/ss/ss6401.pdf ... /mmwr/pdf/ss/ss6412.pdf
# url  'https://www.cdc.gov' + href
# path href
# filename  wk mm6401.pdf ... mm6450.pdf, mm6452.pdf
#      rr rr6401.pdf ... rr6404.pdf
#      ss ss6401.pdf ... ss6412.pdf
# mirror_path href # ignore in favor of /mmwr/pdfs/<vol>/<filename>
# string ''
# series mm, rr, or ss
# volume 64

_mm_list = [f'{x:02d}' for x in list(range(1, 51)) + [52] ] # 51
_rr_list = [f'{x:02d}' for x in range(1, 5)]                #  4
_ss_list = [f'{x:02d}' for x in range(1, 13)]               # 12
_href = ['/mmwr/pdf/wk/mm64' + iss + '.pdf' for iss in _mm_list] + \
        ['/mmwr/pdf/rr/rr64' + iss + '.pdf' for iss in _rr_list] + \
        ['/mmwr/pdf/ss/ss64' + iss + '.pdf' for iss in _ss_list]
_flnm = ['mm64' + iss + '.pdf' for iss in _mm_list] + \
        ['rr64' + iss + '.pdf' for iss in _rr_list] + \
        ['ss64' + iss + '.pdf' for iss in _ss_list]

articles_vol64_pdf_dframe = pd.DataFrame(dict(
    base = ['https://www.cdc.gov/mmwr/' + iss for iss in 
               ['index2015.html' for iss in _mm_list] + 
               ['indrr_2015.html' for iss in _rr_list] +
               ['indss_2015.html' for iss in _ss_list]],
    href = _href,
    url = ['https://www.cdc.gov' + href for href in _href],
    path = _href,
    filename = _flnm,
    mirror_path = ['/mmwr/pdfs/64/' + flnm for flnm in _flnm],
    string = ['' for flnm in _flnm],
    series = ['mm' for iss in _mm_list] + \
               ['rr' for iss in _rr_list] + \
               ['ss' for iss in _ss_list],
    volume = ['64' for flnm in _flnm]
   ))

mmwr_pdf_dframe = pd.concat([articles_pdf_dframe, articles_vol64_pdf_dframe],
                                axis=0) # 2066 x 9

# pickle
pickle.dump(mmwr_pdf_dframe, open("mmwr_pdf_dframe.pkl", "wb"))
# mmwr_pdf_dframe_ = pickle.load(open("mmwr_pdf_dframe.pkl", "rb"))
# mmwr_pdf_dframe.equals(mmwr_pdf_dframe_)

mmwr_pdf_dframe.to_excel("mmwr_pdf_dframe.xlsx", engine='openpyxl', freeze_panes=(1,0))
