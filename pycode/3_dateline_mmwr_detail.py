#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: cmheilig

Extract, correct, harmonize, and organize dateline from MMWR, EID, and PCD
(Older MMWR publications are less consistent than those since 2016.)
These elements are extracted from file paths, <head> elements, and elsewhere

Dateline elements:
    series: mmwr, mmrr, mmss, mmsu, mmnd, eid, pcd
    date:   YYYY, YYYY-MM, YYYY-MM-DD
    volume: %02d (2 digits, leading zero)
    issue:  %02d (2 digits, leading zero, plus 5051, 5152, 5253)
    page:   %04d (first page, MMWR only)
    article ID: MMWR, EID, PCD
    language: en, es, fr, zhs, zht
"""

#%% Set up environment
import pickle
import json
from collections import Counter, defaultdict
from bs4 import SoupStrainer
from dateutil.parser import parse as parse_date
only_head = SoupStrainer(name='head')

os.chdir('/Users/cmheilig/cdc-corpora/_test')

#%% Retrieve (unpickle) trimmed, UTF-8 HTML
mmwr_cc_df = pickle.load(open("pickle-files/mmwr_cc_df.pkl", "rb")) # (4786, 8)
mmwr_cc_paths = mmwr_cc_df.mirror_path.to_list() # 5179

mmwr_toc_unx = pickle.load(open('pickle-files/mmwr_toc_unx.pkl', 'rb'))
mmwr_art_unx = pickle.load(open('pickle-files/mmwr_art_unx.pkl', 'rb'))

#%% MMWR
"""
Dateline elements:
    series: mmwr, mmrr, mmss, mmsu, mmnd
    date:   YYYY, YYYY-MM, YYYY-MM-DD
    volume: %02d (2 digits, leading zero) = YYYY - 1951
    issue:  %02d (2 digits, leading zero, plus 5051, 5152, 5253)
    page:   %04d (first page, MMWR only)
    language: en, es
"""
mmwr_toc_soup = {
    path: BeautifulSoup(html, 'lxml')
    for path, html in tqdm(mmwr_toc_unx.items())}
# 135/135 [00:03<00:00, 41.54it/s]
mmwr_art_soup = {
    path: BeautifulSoup(html, 'lxml')
    for path, html in tqdm(mmwr_art_unx.items())}
# 15161/15161 [13:00<00:00, 19.42it/s]

#%% MMWR dateline details

x = [dict(path=path, **mmwr_path_re_fn(path)) 
     for path in mmwr_art_unx]
pd.DataFrame(x).to_excel('check_mmwr_paths.xlsx', freeze_panes=(1,0))

mmwr_soup = {
    path: BeautifulSoup(html, 'lxml')
    for path, html in tqdm(mmwr_art_unx.items())}
# 15161/15161 [13:21<00:00, 18.91it/s]

# 2a. harvest series, volume, issue, pages, sequence, date
#       3 sources: (1) mirror_path, (2) HTML, (3) soup (parsed HTML)

# 2a(1) information from filename stem (only from mm|rr|ss|su)
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

filestem_list = [
    dict(path=path, stan=(re.match(mmwr_filestan_re, path) is not None),
         **re.match(mmwr_filestem_re, path).groupdict(''))
    for path in tqdm(mmwr_art_unx.keys())] # 15161
# pd.DataFrame(filestem).to_excel('mmwr_filestem_re_df.xlsx', freeze_panes=(1,0))

# 2a(2) dateline from Javascript comment, 1982-02-12/2016-01-08
def var_dateline_fn(html):
    reportTitle = re.search(r"""reportTitle = ["'](.*?)["'];""", html)
    reportTitle = '_' if reportTitle is None else reportTitle.group(1)
    if reportTitle == '': reportTitle = '_'
    # begin with digit (Spanish-language dates) or 1st letter of month
    reportDate = re.search(r"""reportDate = ["'].*?([1-9ADFJMNOS].+?)["'];""", html)
    reportDate = '_ / _' if reportDate is None else reportDate.group(1)
    if reportDate == '': reportDate = '_ / _'
    TitleDate = (reportTitle + ' / ' + reportDate)
    return TitleDate

var_dateline_list = [
    dict(path=path, dateline=var_dateline_fn(html))
    for path, html in tqdm(mmwr_art_unx.items())] # 15122
# 15161/15161 [00:00<00:00, 20739.54it/s]
# pd.DataFrame(var_dateline_list).to_excel('var_dateline_df.xlsx', freeze_panes=(1,0))

# path: path, filestem, file_; 
# html: var_dateline; 
# soup: div_dateline, title, link_canon, MMWR_Type, Volume, Issue_Num, Issue,
#       Page, Date, citation_categories, citation:author, citation_doi,
#       keywords, Keywords, description, Description

# 2a(3) compile all the values from soup and place them in a prescribed order
mmwr_meta_names = ['MMWR_Type', 'Volume', 'Issue_Num', 'Issue', 'Page', 'Date', 
    'citation_categories', 'citation_author', 'citation_doi', 
    'keywords', 'Keywords', 'description', 'Description']

# separate alphabetic from numeric in volume and issue
def num_anno(vo_is):
    vo_is = vo_is.lower()
    num = re.sub(r'\D', '', vo_is)
    if num: num = f'{int(num):02d}'
    anno = re.sub(r'[\d-]', '', vo_is)[:2]
    if '&' in anno: anno = ''
    return num, anno

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
    _, name_vals['md_MMWR_Type'] = num_anno(name_vals['md_MMWR_Type'])
    md_vol_num, md_vol_anno = num_anno(name_vals.get('md_Volume'))
    md_iss_num, md_iss_anno = num_anno(name_vals.get('md_Issue'))
    name_vals['md_Issue_Num'], _ = num_anno(name_vals.get('md_Issue_Num'))
    md_page_num, md_page_anno = num_anno(name_vals.get('md_Page'))
    
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

# 2a(4) parse dateline
# use mapping (lookup rather than algorithm) to convert roman numerals up to xv
roman_to_arabic = dict(i=1, ii=2, iii=3, iv=4, v=5, vi=6, vii=7, viii=8, ix=9,
    x=10, xi=11, xii=12, xiii=13, xiv=14, xv=15)
def range_info(range_):
    """
    range_ is a single string that contains one of the following:
        a single roman numeral
        a hyphen-delimited range of 2 roman numerals in increasing order
        a single arabic numeral
        a hyphen-delimited range of 2 arabic integers, either
            in increasing order or
            where the second integer drops higher-order digits
    return a dictionary with total number of pages and normalized range;
        arabic integers remove leading zeroes and restore higher-order digits
    """
    has_hyphen = ('\N{HYPHEN-MINUS}' in range_)
    is_roman = (re.search(r'\d', range_) is None)
    if has_hyphen and not is_roman:
        num_fm, num_to = range_.split('-')
        val_fm, val_to = [int(x) for x in (num_fm, num_to)]
        rep_fm, rep_to = [str(x) for x in (val_fm, val_to)]
        if (len(rep_fm) > len(rep_to)):
            rep_to = rep_fm[:(len(rep_fm)-len(rep_to))] + rep_to
            val_fm, val_to = [int(x) for x in (rep_fm, rep_to)]
        n_pages = val_to - val_fm + 1
        rep = rep_fm + '-' + rep_to
    elif has_hyphen and is_roman:
        rep_fm, rep_to = range_.split('-')
        val_fm, val_to = [roman_to_arabic[num] for num in (rep_fm, rep_to)]
        n_pages = val_to - val_fm + 1
        rep = rep_fm + '-' + rep_to # == range_
    else: # not has_hyphen
        n_pages = 1
        if not is_roman:
            rep = str(int(range_))
        else:
            rep = range_
    return dict(n_pages=n_pages, rep=rep)

# {k: range_info(k) for k in ['v', 'i-v', '5', '5-9', '15-9', '105-9', '105-19']}

def pages(page_str):
    """
    Given a string containing pages of an article, return a dictionary:
        first:    first roman numeral, useful for sorting
        pages:    total number of pages
        ranges: total number of ranges
        anno:     annotation for ND, Q, SS, or S
        normed: normalized string of pages
    """
    page_str = page_str.lower()
    if not re.search(r'[ivx\d]', page_str): # check for any numbers, incl roman
        return dict(dl_page=0, dl_pgs='', dl_npgs=0, dl_pgs_anno='')#, ranges=0
    # strip extraneous characters; keep nd, q, s, i, v, x, 0-9
    page_str = re.match(r'\A[^ndqsivx\d]*(.*?)[^ndqsivx\d]*\Z', page_str).group(1)
    # strip initial SPACE or COLON
    # page_str = re.sub('^[ :]', '', page_str)

    # determine presence of nd, q, ss, s; note and remove
    anno = '' # initialize annotation
    anno_remo = [('nd', 'nd-? ?'), ('q', 'q-?'), ('ss', 'ss'), ('s', 's')]
    for _anno, _remo in anno_remo:
        if _anno in page_str: 
            anno = _anno
            page_str = re.sub(_remo, '', page_str)
            break
    # print(f'anno: {anno}')

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
    ranges = [range_info(range_) for range_ in page_str.split(',')]
    n_pages = sum([range_['n_pages'] for range_ in ranges])
    r_pages = ','.join([range_['rep'] for range_ in ranges])
    return dict(dl_page=first, dl_pgs=r_pages, dl_npgs=n_pages, dl_pgs_anno=anno)

# pages('856-857;859-869')

# regular expression to parse dateline
mmwr_dateline_re = re.compile(r'(?P<series>.*?)/(?P<date>.*?)/(?P<vo_is_pg>.*)')
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
    if n_solidus < 2:
        dl += (2-n_solidus)*'/'
    dl_ser, dl_date, vo_is_pg = [x.strip() 
                                 for x in mmwr_dateline_re.match(dl.lower()).groups()]
    # check/refine series, date, volume/issue/pages
    ## series
    dl_ser = series_map.get(dl_ser[:6], '')
    ## date
    try:
        dl_date = parse_date(dl_date).date().isoformat()
    except:
        pass
    ## volume/issue/pages
    vo_is_pg_split = re.split(r' ?\(|\);?', vo_is_pg)
    if len(vo_is_pg_split) < 3:
        dl_vol, dl_iss, dl_pgs = ['_']*3
    else:
        dl_vol, dl_iss, dl_pgs = vo_is_pg_split
    dl_vol_num, dl_vol_anno = num_anno(dl_vol)
    dl_iss_num, dl_iss_anno = num_anno(dl_iss)
    try:
        dl_pgs = pages(dl_pgs)
    except:
        dl_pgs = pages('')
    return dict(dl_ser=dl_ser, dl_date=dl_date, 
                dl_vol_num=dl_vol_num, dl_vol_anno=dl_vol_anno, 
                dl_iss_num=dl_iss_num, dl_iss_anno=dl_iss_anno, 
                **dl_pgs)

# Bring it all together
# by source

# optional argument alt_dateline is dict mapping path to alternate dateline
def mmwr_metadata_fn(path, alt_dateline=None):
    # path
    filestan = (re.match(mmwr_filestan_re, path) is not None)
    filestem = re.match(mmwr_filestem_re, path).groupdict(default='')
    filestem['fn_es'] = 'es' if 'ensp' in filestem['fn_stem'] else ''
    vol_ge_65 = (filestan and (int(filestem['fn_vol']) >= 65))
    # html
    var_dateline = var_dateline_fn(mmwr_art_unx[path])
    # soup
    soup_meta = mmwr_soup_meta_fn(mmwr_soup[path])
    # cross-check
    if alt_dateline is not None and path in alt_dateline:
        dateline = alt_dateline[path]
    else:
        dateline = soup_meta['div_dateline'] if vol_ge_65 else var_dateline
    del soup_meta['div_dateline']
    dl_meta = parse_dateline_fn(dateline)
    filestem['fn_nd'] = 'nd' if (filestem['fn_stem'].endswith('md') or
                                 soup_meta['title'].startswith('Notifiable Diseases')) \
                        else ''

    result = dict(path=path, vol_ge_65=vol_ge_65, filestan=filestan, 
                  **filestem, dateline=dateline, **dl_meta, **soup_meta)
    # exceptions
    result['x_doi'] = (
        (result['md_citation_doi'] !='') and 
        (result['md_citation_doi'] != ('10.15585/mmwr.' + result['fn_stem'])))
    result['x_ser'] = (
        len({result.get(k) for k in ['fn_ser', 'dl_ser', 'md_MMWR_Type']} -
            {'', '_', None}) != 1) # not uniquely nonblank
    result['x_vol'] = (
        len({result.get(k) for k in ['fn_vol', 'dl_vol_num', 'md_vol_num']} - 
            {'', None}) != 1) # not uniquely nonblank
    result['x_iss_num'] = (
        len({result.get(k) for k in ['fn_iss', 'dl_iss_num', 'md_Issue_Num', 'md_iss_num']} -
            {'', None}) != 1) # not uniquely nonblank
    result['x_iss_anno'] = (result['dl_iss_anno'] != result['md_iss_anno'])
    result['x_pgs'] = (result['dl_pgs'] == '')
    result['x_date'] = (
        (len(result['dl_date']) != 10) or 
        (len(result['md_Date']) not in {0, 10}) or
        ((result['dl_date'] != result['md_Date']) and (result['md_Date'] != '')))
    result['x_any'] = any(
        [result[x] for x in ['x_ser', 'x_vol', 'x_iss_num', 'x_iss_anno',
                             'x_pgs', 'x_date']])
    return result

# mmwr_metadata_fn('/mmwr/preview/mmwrhtml/su5501a1.htm')

mmwr_metadata_list = [mmwr_metadata_fn(path) for path in tqdm(mmwr_art_unx.keys())]
# 15161/15161 [07:14<00:00, 34.90it/s]
# mmwr_metadata_df = pd.DataFrame(mmwr_metadata_list) # (15122, 50)

md_by_src = {
 'main': ['path', 'vol_ge_65', 'filestan', 'dateline'], 
 'filestem': ['fn_stem', 'fn_num', 'fn_ser', 'fn_vol', 'fn_iss', 'fn_aem', 
              'fn_seq', 'fn_sfx', 'fn_nd', 'fn_es'], 
 'dl_meta': ['dl_ser', 'dl_date', 'dl_vol_num', 'dl_vol_anno', 'dl_iss_num', 'dl_iss_anno', 
             'dl_page', 'dl_pgs', 'dl_npgs', 'dl_pgs_anno'], 
 'soup_meta': ['title', 'link_canon', 'md_MMWR_Type', 
               'md_Volume', 'md_vol_num', 'md_vol_anno', 
               'md_Issue_Num', 'md_Issue', 'md_iss_num', 'md_iss_anno', 
               'md_Page', 'md_page_num', 'md_page_anno', 
               'md_Date', 'md_Keywords', 'md_Description', 
               'md_citation_categories', 'md_citation_author', 'md_citation_doi', 
               'md_keywords', 'md_description'],
 'check': ['x_doi', 'x_ser', 'x_vol', 'x_iss_num', 'x_iss_anno', 'x_pgs', 'x_date']}
md_by_src_cols = [v for val in md_by_src.values() for v in val]
# by type
md_by_type = {
 'filename': ['path', 'link_canon', 'filestan', 'fn_stem', 'fn_num', 'fn_nd', 'fn_es', 
              'md_citation_doi', 'x_doi'], 
 'series': ['fn_ser', 'dl_ser', 'md_MMWR_Type', 'x_ser'], 
 'volume': ['vol_ge_65', 'fn_vol', 'dl_vol_num', 'dl_vol_anno', 
            'md_Volume', 'md_vol_num', 'md_vol_anno', 'x_vol'], 
 'issue': ['fn_iss', 'dl_iss_num', 'dl_iss_anno', 
           'md_Issue_Num', 'md_Issue', 'md_iss_num', 'md_iss_anno', 'x_iss_num', 'x_iss_anno'], 
 'pages': ['dl_page', 'dl_pgs', 'dl_npgs', 'dl_pgs_anno', 'md_Page', 'md_page_num', 'md_page_anno', 
           'fn_aem', 'fn_seq', 'fn_sfx', 'x_pgs'], 
 'date': ['dl_date', 'md_Date', 'dl_vol_num', 'dl_iss_num', 'x_date'],
 'kwds': ['md_Keywords', 'md_keywords'],
 'desc': ['md_Description', 'md_description'], 
 'other': ['dateline', 'title', 'md_citation_categories', 'md_citation_author']}
# 'dl_vol_num', 'dl_iss_num' appear twice; list(fromkeys()) preserves order
md_by_type_cols = list(dict.fromkeys([v for val in md_by_type.values() for v in val]))
set(md_by_src_cols) ^ set(md_by_type_cols) # set()

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
