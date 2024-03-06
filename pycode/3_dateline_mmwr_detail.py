#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: cmheilig

Auxiliary code to scrutinize MMWR dateline information from source
Focus on articles, not tables of contents

Dateline elements:
    series: mmwr, mmrr, mmss, mmsu, mmnd
    date:   YYYY-MM-DD
    volume: %02d (2 digits, leading zero) = YYYY - 1951
    issue:  %02d (2 digits, leading zero, plus 5051, 5152, 5253)
    page:   %04d (first page, MMWR only)
    language: en, es

Dateline information takes 3 forms from 4 components:
    fn: (1) filename from path, including series, volume, issue, sequence
    md: (2) metadata from <meta> elements 
        MMWR_Type, Volume, Issue_Num, Issue, Page, Date
    dl: dateline string from (3) var declaration or (4) <div class="dateline">

The dateline string is especially tricky to parse over the full 1982-2023.

"""

#%% Set up environment
import pickle
import json
#x from collections import Counter, defaultdict
#x from bs4 import SoupStrainer
from dateutil.parser import parse as parse_date

os.chdir('/Users/cmheilig/cdc-corpora/_test')

#%% Retrieve (unpickle) trimmed, UTF-8 HTML
mmwr_art_unx = pickle.load(open('pickle-files/mmwr_art_unx.pkl', 'rb'))

mmwr_art_soup = {
    path: BeautifulSoup(html, 'lxml')
    for path, html in tqdm(mmwr_art_unx.items())}
# 15161/15161 [13:00<00:00, 19.42it/s]

#%% MMWR dateline information 
# path: path, filestem, file_; 
# html: var_dateline; 
# soup: div_dateline, title, link_canon, MMWR_Type, Volume, Issue_Num, Issue,
#       Page, Date, citation_categories, citation:author, citation_doi,
#       keywords, Keywords, description, Description

## in filename (path)
def dateline_path_fn(path):
    "Dateline information from path/filename"
    # "standard" format: (mm|rr|ss|su)\d{2}(\d{2,4}|SP)(a|e|md)\d{0,2}
    mmwr_filestan_re = re.compile(r'^/.*/(mm|rr|ss|su)\d{2}(\d{2,4}|SP)(a|e|md)\d*\.htm$')
    # a more sensitive expression to capture exceptions
    mmwr_filestem_re = re.compile(
        r'''^/.*/
            (?P<fn_stem>               # delimit whole string
             (?P<fn_num>\d{8}) |       # 8-digit filenames
             (
              (?P<fn_ser>mm|rr|ss|su)  # series marker
              (?P<fn_vol>\d{2})        # volume
              (?P<fn_iss>\d{2,4}|SP)?  # issue(s)
              (?P<fn_aem>a|e|fw|md|tc|toc|\-Immunizationa)? # a or e (e=early)
              (?P<fn_seq>\d*)          # sequence in issue
              (?P<fn_sfx>.*)           # any remaining suffix
             )
            )\.htm$''', re.VERBOSE | re.ASCII)
    fn_dict = dict(path=path, 
                   fn_stan=(re.match(mmwr_filestan_re, path) is not None),
                   **re.match(mmwr_filestem_re, path).groupdict(''))
    return fn_dict

dateline_path_list = [
    dateline_path_fn(path) for path in tqdm(mmwr_art_unx.keys())]
# 15161/15161 [00:00<00:00, 62913.71it/s]

## in var declaration (html)
def dateline_html_fn(html):
    'Dateline information from html var declaration'
    reportTitle = re.search(r"""reportTitle = ["'](.*?)["'];""", html)
    reportTitle = '_' if reportTitle is None else reportTitle.group(1)
    if reportTitle == '': reportTitle = '_'
    # begin with digit (Spanish-language dates) or 1st letter of month
    reportDate = re.search(r"""reportDate = ["'].*?([1-9ADFJMNOS].+?)["'];""", html)
    reportDate = '_ / _' if reportDate is None else reportDate.group(1)
    if reportDate == '': reportDate = '_ / _'
    var_dl = (reportTitle + ' / ' + reportDate)
    return var_dl

dateline_html_list = [
    dict(path=path, var_dl=dateline_html_fn(html))
    for path, html in tqdm(mmwr_art_unx.items())]
# 15161/15161 [00:02<00:00, 6911.73it/s]

## in <meta> and <div> elements (soup)
# <meta name="...">
mmwr_meta_names = ['MMWR_Type', 'Volume', 'Issue_Num', 'Issue', 'Page', 'Date'] 
    # ['citation_categories', 'citation_author', 'citation_doi', 
    # 'keywords', 'Keywords', 'description', 'Description']

def dateline_soup_fn(soup):
    '''Dateline information from soup 
       <meta name="" content=""> and <div class="dateline">'''
    name_vals = {'md_' + meta_name: '' for meta_name in mmwr_meta_names}
    name_vals.update({'md_' + meta_name: 
                 '' if tag is None else tag.get('content').strip()
                 for meta_name in mmwr_meta_names
                 for tag in [soup.find(name='meta', attrs={'name': meta_name})]
                 if (tag is not None) and (tag.get('content') != '')})
    
    div_dl = soup.find('div', class_='dateline')
    div_dl = '_ / _ / _' if div_dl is None else \
                   div_dl.get_text(strip=True)

    md_dict = dict(**name_vals, div_dl=div_dl)
    return md_dict

dateline_soup_list = [
    dict(path=path, **dateline_soup_fn(soup))
    for path, soup in tqdm(mmwr_art_soup.items())]
# 15161/15161 [07:24<00:00, 34.13it/s]

# pd.DataFrame(dateline_path_list).set_index('path') # (15161, 9)
# pd.DataFrame(dateline_html_list).set_index('path') # (15161, 1)
# pd.DataFrame(dateline_soup_list).set_index('path') # (15161, 14)

## merge daetline sources
mmwr_dateline_df = (
    pd.DataFrame(dateline_path_list).set_index('path')
    .merge(pd.DataFrame(dateline_html_list).set_index('path'), on='path')
    .merge(pd.DataFrame(dateline_soup_list).set_index('path'), on='path')) 
# (15161, 24)

pd.crosstab(mmwr_dateline_df.var_dl.str.len()>9, 
            mmwr_dateline_df.div_dl.str.len()>9, margins=True)
# div_dl  False  True    All
# var_dl                    
# False     106  3252   3358
# True    11803     0  11803
# All     11909  3252  15161

# var_dl and div_dl are never both full strings; both missing in 106 instances
# coalesce the dateline strings into a single field and note source
def which_dl_fn(row):
    if len(row['var_dl']) > 9:
        return dict(src='var_dl', dl=row['var_dl'])
    elif len(row['div_dl']) > 9:
        return dict(src='div_dl', dl=row['div_dl'])
    else:
        return dict(src='_no_dl', dl=row['div_dl'])

mmwr_dateline_df = (
    mmwr_dateline_df
    .merge(mmwr_dateline_df[['var_dl', 'div_dl']]
           .apply(lambda row: which_dl_fn(row), result_type='expand', axis=1),
           on='path')) # (15161, 26)
mmwr_dateline_df.src.value_counts().to_dict()
# {'var_dl': 11803, 'div_dl': 3252, '_no_dl': 106}

# ['fn_stan', 'fn_stem', 'fn_num', 'fn_ser', 'fn_vol', 'fn_iss', 'fn_aem', 
#  'fn_seq', 'fn_sfx', 'var_dl', 'md_MMWR_Type', 'md_Volume', 'md_Issue_Num', 
#  'md_Issue', 'md_Page', 'md_Date', 'md_citation_categories', 
#  'md_citation_author', 'md_citation_doi', 'md_keywords', 'md_Keywords', 
#  'md_description', 'md_Description', 'div_dl', 'src', 'dl']

mmwr_dateline_df.to_pickle('mmwr_dateline_df.pkl')

#%% Parse dateline string

# mmwr_dateline_df = pd.read_pickle('pickle-files/mmwr_dateline_df')
mmwr_dateline_dict = mmwr_dateline_df.dl.to_dict()

## Build up dictionaries and functions to parse dateline string

# map Roman numerals to their integer values
roman_map = dict(i=1, ii=2, iii=3, iv=4, v=5, vi=6, vii=7, viii=8, ix=9,
    x=10, xi=11, xii=12, xiii=13, xiv=14, xv=15)

# glean information from page ranges
def range_info_fn(range_str):
    """
    range_str is a single string that contains one of the following:
        a single roman numeral
        a hyphen-delimited range of 2 roman numerals in increasing order
        a single arabic numeral
        a hyphen-delimited range of 2 arabic integers, either
            in increasing order or
            where the second integer drops higher-order digits
    return a dictionary with total number of pages and normalized range;
        arabic integers remove leading zeroes and restore higher-order digits
    """
    has_hyphen = ('\N{HYPHEN-MINUS}' in range_str)
    is_roman = (re.search(r'\d', range_str) is None)
    if has_hyphen and not is_roman:
        num_fm, num_to = range_str.split('-')
        val_fm, val_to = [int(x) for x in (num_fm, num_to)]
        rep_fm, rep_to = [str(x) for x in (val_fm, val_to)]
        if (len(rep_fm) > len(rep_to)):
            rep_to = rep_fm[:(len(rep_fm)-len(rep_to))] + rep_to
            val_fm, val_to = [int(x) for x in (rep_fm, rep_to)]
        n_pages = val_to - val_fm + 1
        rep = rep_fm + '-' + rep_to
    elif has_hyphen and is_roman:
        rep_fm, rep_to = range_str.split('-')
        val_fm, val_to = [roman_map[num] for num in (rep_fm, rep_to)]
        n_pages = val_to - val_fm + 1
        rep = rep_fm + '-' + rep_to # == range_str
    else: # not has_hyphen
        n_pages = 1
        if not is_roman:
            rep = str(int(range_str))
        else:
            rep = range_str
    return dict(n_pages=n_pages, repr_str=rep)

# {k: range_info_fn(k) for k in ['v', 'i-v', '5', '5-9', '15-9', '105-9', '105-19']}

def pages_fn(page_str):
    """
    Given a string containing pages of an article, return a dictionary:
        dl_page: first arabic numeral, useful for sorting
        dl_repr: normalized string of pages
        dl_npgs: total number of pages
        dl_anno: annotation for ND, Q, SS, or S
    """
    page_str = page_str.lower()
    if not re.search(r'[ivx\d]', page_str): # check for any numbers, incl roman
        return dict(dl_page=0, dl_pgs='', dl_npgs=0, dl_pgs_anno='')#, ranges=0
    # strip extraneous characters; keep nd, q, s, i, v, x, 0-9
    page_str = re.match(r'\A[^ndqsivx\d]*(.*?)[^ndqsivx\d]*\Z', page_str).group(1)

    # determine presence of nd, q, ss, s; note and remove
    anno = '' # initialize annotation
    anno_remo = [('nd', 'nd-? ?'), ('q', 'q-?'), ('ss', 'ss'), ('s', 's')]
    for _anno, _remo in anno_remo:
        if _anno in page_str: 
            anno = _anno
            page_str = re.sub(_remo, '', page_str)
            break

    # determine first arabic page number
    if re.search(r'\d', page_str):
        first = int(re.search(r'\d+', page_str).group())
    else:
        first = 0
    # print(f'first: {first}')

    # normalize and remove spaces around EN DASH, HYPHEN, COMMA, SEMICOLON
    # COMMA|SPACE, HYPHEN|COMMA, HYPHEN|COMMA|SPACE -> COMMA
    page_str = (page_str
                .replace('\N{EN DASH}', '\N{HYPHEN-MINUS}').replace(' - ', '-')
                .replace(';', ',').replace('-,', ',').replace(', ', ','))
    # print(f'page_str.split: {page_str.split(",")!r}')
    ranges = [range_info_fn(range_str) for range_str in page_str.split(',')]
    n_pages = sum([range_str['n_pages'] for range_str in ranges])
    r_pages = ','.join([range_str['repr_str'] for range_str in ranges])
    return dict(dl_page=first, dl_repr=r_pages, dl_npgs=n_pages, dl_anno=anno)

# pages_fn('856-857;859-869')

# separate alphabetic from numeric in volume and issue
def num_anno_fn(vo_is):
    vo_is = vo_is.lower()
    num = re.sub(r'\D', '', vo_is)
    if num: num = f'{int(num):02d}'
    anno = re.sub(r'[\d-]', '', vo_is)[:2]
    if '&' in anno: anno = ''
    return num, anno

# series abbreviations for strings that appear in dateline
series_map = {
    'weekly': 'mm', 'quickg': 'mm', 'dispat': 'mm', 'semana': 'mm', 
    'recomm': 'rr', 'survei': 'ss', 'supple': 'su', '_': '_'}

def parse_dateline_fn(dl):
    """
    from str dateline, return series, date, volume, issue, 
    first page, page range(s), and number of pages
    """
    # prepare and split dateline into 3 segments at '/'
    n_solidus = dl.count('/') # number of forward slash characters
    if n_solidus < 2: dl += (2-n_solidus)*'/'
    # regular expression to parse dateline into a dictionary
    mmwr_dateline_re = re.compile(
        '(?P<series>.*?)/(?P<date>.*?)/(?P<vo_is_pg>.*)')

    dl_ser, dl_date, vo_is_pg = [
        x.strip() for x in mmwr_dateline_re.match(dl.lower()).groups()]
    # check/refine series, date, volume/issue/pages
    ## series
    dl_ser = series_map.get(dl_ser[:6], '')
    ## date
    try:    dl_date = parse_date(dl_date).date().isoformat()
    except: pass
    ## volume/issue/pages
    vo_is_pg_split = re.split(r' ?\(|\);?', vo_is_pg)
    if len(vo_is_pg_split) < 3:
        dl_vol, dl_iss, dl_pgs = ['_']*3
    else:
        dl_vol, dl_iss, dl_pgs = vo_is_pg_split
    dl_vol_num, dl_vol_anno = num_anno_fn(dl_vol)
    dl_iss_num, dl_iss_anno = num_anno_fn(dl_iss)
    try:
        dl_pages = pages_fn(dl_pgs)
    except:
        dl_pages = pages_fn('')
    return dict(dl_ser=dl_ser, dl_date=dl_date, 
                dl_vol_num=dl_vol_num, dl_vol_anno=dl_vol_anno, 
                dl_iss_num=dl_iss_num, dl_iss_anno=dl_iss_anno, 
                **dl_pages)

parse_dateline_list = [
    dict(path=path, dl=dl, **parse_dateline_fn(dl))
    for path, dl in tqdm(mmwr_dateline_dict.items())]
parse_dateline_df = (
    pd.DataFrame(parse_dateline_list)
    .set_index('path')
    .drop(columns=['dl_vol_anno', 'dl_pgs', 'dl_pgs_anno'])) # (15161, 10)
# ['dl', 'dl_ser', 'dl_date', 'dl_vol_num', 'dl_iss_num', 'dl_iss_anno',
#  'dl_page', 'dl_repr', 'dl_npgs', 'dl_anno']
parse_dateline_df.to_pickle('parse_dateline_df.pkl')
# parse_dateline_df.to_excel('parse_dateline_df.xlsx', freeze_panes=(1,0))

#%% Merge parsed dateline back with other dateline information

mmwr_dateline_parsed_df = (
    mmwr_dateline_df
    .drop(columns='dl') # same column in both DataFrames
    .merge(parse_dateline_df, on='path')) # (15161, 35)

md_by_type = {
    'dateline': 'var_dl', 'div_dl', 'src', 'dl', 'fn_stan', 'fn_stem', 'fn_num', 
    'series': 'fn_ser', 'md_MMWR_Type', 'dl_ser',
    'volume': 'fn_vol', 'md_Volume', 'dl_vol_num',
    'issue': 'fn_iss', 'md_Issue_Num', 'md_Issue', 'dl_iss_num', 'dl_iss_anno',
    'pages': 'md_Page', 'dl_page', 'dl_repr', 'dl_npgs', 'dl_anno',
    'date':  'md_Date', 'dl_date',
    'other': 'fn_aem', 'fn_seq', 'fn_sfx'}
md_by_type_cols = list(dict.fromkeys([v for val in md_by_type.values() for v in val]))
set(mmwr_dateline_parsed_df.columns) ^ set(md_by_type_cols) # set()

#%% resume here

mmwr_metadata_df = pd.DataFrame(mmwr_metadata_list)[md_by_type_cols] # (15122, 52)
# mmwr_metadata_df.to_excel('mmwr_metadata_df.xlsx', freeze_panes=(1,0))

set(md_by_src_cols) ^ set(mmwr_metadata_df.columns) # set()
set(md_by_type_cols) ^ set(mmwr_metadata_df.columns) # set()

with pd.ExcelWriter('mmwr_metadata_freqs.xlsx', engine='openpyxl') as xlwriter:
    mmwr_metadata_df.to_excel(xlwriter, sheet_name='all_data', freeze_panes=(1,0))
    for type_, cols in tqdm(md_by_type.items()):
        (mmwr_metadata_df
         .value_counts(subset=cols, dropna=False, sort=False)
         .reset_index().rename(columns={0: 'freq'}).fillna('')
         .to_excel(xlwriter, sheet_name=type_, freeze_panes=(1,0)))


### 2b. resolve and construct definitive dateline info
#   Parse publication series, date, and volume(issue);pages

# correct inconsistent or missing values
# check again for consistency and completeness

# dateline corrections started as a JSON file mapping 260 paths to revised datelines
dateline_cx = json.load(open("dateline_corrections_20231218_path.json"))

# integrate revised dateline values
mmwr_metadata_cx_list = [
    mmwr_metadata_fn(md['path'], dateline_cx) if md['path'] in dateline_cx else md
    for md in tqdm(mmwr_metadata_list)] # copy
# 15161/15161 [02:32<00:00, 99.45it/s]
# [md['dateline'] for md in mmwr_metadata_list    if md['path'] in list(dateline_cx)[:10]]
# [md['dateline'] for md in mmwr_metadata_cx_list if md['path'] in list(dateline_cx)[:10]]
# pickle.dump(mmwr_metadata_cx_list, open('mmwr_metadata_cx_list.pkl', 'wb'))
# mmwr_metadata_cx_list = pickle.load(open('mmwr_metadata_cx_list.pkl', 'rb'))

# check again for anomalies as with mmwr_metadata_fn
mmwr_metadata_cx_df = pd.DataFrame(mmwr_metadata_cx_list)[md_by_type_cols] # (15161, 52)
# mmwr_metadata_df.to_excel('mmwr_metadata_df.xlsx', freeze_panes=(1,0))

with pd.ExcelWriter('mmwr_metadata_cx_freqs.xlsx', engine='openpyxl') as xlwriter:
    mmwr_metadata_cx_df.to_excel(xlwriter, sheet_name='all_data', freeze_panes=(1,0))
    for type_, cols in tqdm(md_by_type.items()):
        (mmwr_metadata_cx_df
         .value_counts(subset=cols, dropna=False, sort=False)
         .reset_index().rename(columns={0: 'freq'}).fillna('')
         .to_excel(xlwriter, sheet_name=type_, freeze_panes=(1,0)))
del type_, cols

#%% leftover
#x only_head = SoupStrainer(name='head')
#x mmwr_cc_df = pickle.load(open("pickle-files/mmwr_cc_df.pkl", "rb")) # (4786, 8)
#x mmwr_cc_paths = mmwr_cc_df.mirror_path.to_list() # 5179

#x mmwr_toc_unx = pickle.load(open('pickle-files/mmwr_toc_unx.pkl', 'rb'))
#x%% MMWR
#x """
#x Dateline elements:
#x """
#x mmwr_toc_soup = {
#x     path: BeautifulSoup(html, 'lxml')
#x     for path, html in tqdm(mmwr_toc_unx.items())}
#x 135/135 [00:03<00:00, 41.54it/s]

x = [dict(path=path, **mmwr_path_re_fn(path)) 
     for path in mmwr_art_unx]
pd.DataFrame(x).to_excel('check_mmwr_paths.xlsx', freeze_panes=(1,0))

#x mmwr_soup = {
#x     path: BeautifulSoup(html, 'lxml')
#x     for path, html in tqdm(mmwr_art_unx.items())}
#x 15161/15161 [13:21<00:00, 18.91it/s]

# 2a. harvest series, volume, issue, pages, sequence, date
#       3 sources: (1) mirror_path, (2) HTML, (3) soup (parsed HTML)

# 2a(1) information from filename stem (only from mm|rr|ss|su)


filestem_list = [
    dict(path=path, stan=(re.match(mmwr_filestan_re, path) is not None),
         **re.match(mmwr_filestem_re, path).groupdict(''))
    for path in tqdm(mmwr_art_unx.keys())] # 15161
# pd.DataFrame(filestem).to_excel('mmwr_filestem_re_df.xlsx', freeze_panes=(1,0))


# 2a(2) dateline from Javascript comment, 1982-02-12/2016-01-08
def var_dateline_fn(html):

var_dateline_list = [
    dict(path=path, dateline=var_dateline_fn(html))
    for path, html in tqdm(mmwr_art_unx.items())] # 15122
# 15161/15161 [00:00<00:00, 20739.54it/s]
# pd.DataFrame(var_dateline_list).to_excel('var_dateline_df.xlsx', freeze_panes=(1,0))

# 2a(3) compile all the values from soup and place them in a prescribed order

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
    _, name_vals['md_MMWR_Type'] = num_anno_fn(name_vals['md_MMWR_Type'])
    md_vol_num, md_vol_anno = num_anno_fn(name_vals.get('md_Volume'))
    md_iss_num, md_iss_anno = num_anno_fn(name_vals.get('md_Issue'))
    name_vals['md_Issue_Num'], _ = num_anno_fn(name_vals.get('md_Issue_Num'))
    md_page_num, md_page_anno = num_anno_fn(name_vals.get('md_Page'))
    
    md_vals = dict(
        title=title, link_canon=link_canon, div_dateline=div_dateline, 
        **name_vals, md_vol_num=md_vol_num, md_vol_anno=md_vol_anno, 
        md_iss_num=md_iss_num, md_iss_anno=md_iss_anno, #md_Issue_Num=md_Issue_Num, 
        md_page_num=md_page_num, md_page_anno=md_page_anno)
    return md_vals

soup_meta_list = [
    dict(path=path, **mmwr_soup_meta_fn(soup))
    for path, soup in tqdm(mmwr_art_soup.items())]
# 15161/15161 [07:05<00:00, 35.63it/s]
# pd.DataFrame(div_dateline_list).to_excel('div_dateline_df.xlsx', freeze_panes=(1,0))

