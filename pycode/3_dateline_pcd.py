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
only_head = SoupStrainer(name='head')

os.chdir('/Users/cmheilig/cdc-corpora/_test')

#%% Retrieve (unpickle) trimmed, UTF-8 HTML
# pcd_cc_df = pickle.load(open("pickle-files/pcd_cc_df.pkl", "rb")) # (4786, 8)
pcd_cc_df = pd.read_pickle('pickle-files/pcd_cc_df.pkl')
pcd_cc_paths = pcd_cc_df.mirror_path.to_list() # 5179
pcd_art_en_paths = [
    path for path in pcd_cc_paths
    if re.search(r'(\d{2}_\d{4}[aber\d]{0,2}|cover)\.htm', path)] # 3011
pcd_art_nen_paths = [
    path for path in pcd_cc_paths
    if re.search(r'\d{2}_\d{4}[aber\d]{0,2}_(es|fr|zhs|zht)\.htm', path)] # 2080

pcd_toc_unx = pickle.load(open('pickle-files/pcd_toc_unx.pkl', 'rb'))
pcd_art_unx = pickle.load(open('pickle-files/pcd_art_unx.pkl', 'rb'))

#%% PCD soup
"""
'lang', 'dl_vol_iss', 'dl_year_mo', 'dl_date', 'dl_art_num'
Dateline elements:
    series: pcd
    language: en, es, fr, zhs, zht
    date:   YYYY (vol), YYYY-MM (vol/iss),  (art)
    volume/issue: VV(I) vol is %02d (2 digits, leading zero) = YYYY - 2003
    year/month: YYYY-MM
    date: YYYY-MM-DD
    article number: [AE]\d{3} from TOCs, number is %03d (3 digits, leading zero)
"""
pcd_toc_soup = {
    path: BeautifulSoup(html, 'lxml')
    for path, html in tqdm(pcd_toc_unx.items())}
# 87/87 [00:01<00:00, 49.23it/s]
pcd_art_soup = {
    path: BeautifulSoup(html, 'lxml')
    for path, html in tqdm(pcd_art_unx.items())}
# 5091/5091 [01:41<00:00, 50.09it/s]

#%% PCD dateline: tables of contents

year_mo_to_vol_iss = json.load(open('pcd_year_mo_to_vol_iss.json', 'r'))
mon_to_num = {'jan': '01', 'mar': '03', 'apr': '04', 'may': '05', 
              'jul': '07', 'sep': '09', 'oct': '10', 'nov': '11'}
corrected_toc_datelines = {
 '/pcd/es/2006_jan_toc.htm': 'Volumen 3: Nº 1, enero de 2006',
 '/pcd/es/2010_jul_toc.htm': 'Volumen 7: Nº 4, julio 2010',
 '/pcd/es/2010_nov_toc.htm': 'Volumen 7: Nº 6, noviembre de 2010',
 '/pcd/es/2011_nov_toc.htm': 'Volumen 8: Nº 6, noviembre de 2011',
 '/pcd/es/2011_sep_toc.htm': 'Volumen 8: Nº 5, septiembre de 2011'}

def pcd_toc_dl_re_fn(path, html):
    Vol_Year_re = re.compile(r'Volume.*?\d{4}\)?', flags=re.S|re.M)
    issn_idx = re.search('ISSN', html)
    if issn_idx:
        issn_idx = issn_idx.end()
        found = Vol_Year_re.search(html, issn_idx)#, issn_idx+200)
        if found:
            dateline = re.sub(r'\s+', ' ', found.group())
        if path in corrected_toc_datelines:
            dateline = corrected_toc_datelines[path]
    else:
        dateline = ''

    pcd_path_re = re.compile(r"""
        /pcd/(issu)?es/(?P<fn_year>\d{4})            # 4-digit year
        ((?P<delim>[_/])
         (?P<fn_mon>jan|mar|apr|may|jul|sep|oct|nov) # 3-letter month
         (?P=delim)toc)? |
        (/(?P=fn_year)_TOC) |
        (_toc)
        \.htm""", flags=re.X)
    lang = 'es' if re.search('/es/', path) else 'en'
    if (dl_gp := pcd_path_re.search(path)):
        dl_gp = dl_gp.groupdict('')
        dl_mo = mon_to_num.get(dl_gp['fn_mon'], '')
        year_mo = (dl_gp['fn_year'] + '-' + dl_mo) if dl_mo else dl_gp['fn_year']
        vol_iss = year_mo_to_vol_iss.get(year_mo)
        dl_info = dict(lang=lang, dateline=dateline, dl_vol_iss=vol_iss, 
                       dl_year_mo=year_mo)#, **dl_gp)
    else:
        dl_info = dict(lang=lang, dateline=dateline, dl_vol_iss='', 
                       dl_year_mo='')
    
    return dl_info

# pcd_toc_dl_re_fn('/pcd/issues/2023/2023_TOC.htm')
# pcd_toc_dl_re_fn('/pcd/issues/2004/apr/toc.htm')
# pcd_toc_dl_re_fn('/pcd/es/2011_sep_toc.htm')

pcd_toc_dl_list = [
    dict(path=path, **pcd_toc_dl_re_fn(path, html))
    for path, html in tqdm(pcd_toc_unx.items())]
pcd_toc_dl_df = pd.DataFrame(pcd_toc_dl_list).sort_values(['dl_year_mo', 'lang']) # (87, 5)
# ['path', 'lang', 'dateline', 'dl_vol_iss', 'dl_year_mo']

pcd_toc_dl_df.to_excel('pcd_toc_dl_df.xlsx', freeze_panes=(1,0))

#%% PCD dateline: English-language articles
"""
## PCD dateline from article text
Volume dates are encoded in paths
Article dates are in tables of contents and article text
Harvest and parse dateline information from English-language articles
Fill in gaps (where English-language article doesn't contain dateline)
Associate completed dateline information with non-English articles
"""

# dict to map full month name to 2-digit month number
month_to_num = {
    'January': '01', 'February': '02', 'March': '03', 'April': '04', 
    'May': '05', 'June': '06', 'July': '07', 'August': '08', 
    'September': '09', 'October': '10', 'November': '11', 'December': '12'}
# dict to map vol(iss) to publication date
vol_iss_to_date = json.load(open('pcd_vol_iss_dates.json', 'r')) # 37
# dict to fill in datelines where missing
corrected_datelines = json.load(open('pcd_corrected_datelines.json', 'r')) # 145
# dict to map path to article number (from TOCs)
article_numbers = json.load(open('pcd_article_numbers.json', 'r')) # 2980

# set(article_numbers) ^ set(pcd_art_en_paths)
# # {'/pcd/issues/2006/oct/memoriam.htm'}

def pcd_art_en_dl_re_fn(path, soup):
    # regular expression to narrow search
    Vol_Mon_re = re.compile(
        r'Volume.{1,2}\d.{1,20}(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)',
        flags=re.I|re.S|re.M)
    found = soup.find_all(string=Vol_Mon_re)
    
    # zero in on dateline of interest
    if found:
        # select at most 1 where parent name is 'p' and string length < 120
        found = [
            str(x.parent) for x in found 
            if (x.parent.name == 'p') and len(str(x.parent)) < 200] # 120

    # Volumes 1-8
    pcd_dl1_re = re.compile(
        r'Volume (?P<dl_vol>\d{1,2}): '
        r'(No\. (?P<dl_iss>\d{1,2})|(?P<dl_sp>Special Issue)), '
        '(?P<dl_mon>(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*)'
        r' (?P<dl_year>\d{4})')
    # Volumes 9+; uses \N{EM DASH} and \N{RIGHT SINGLE QUOTATION MARK}
    pcd_dl2_re = re.compile(
        r"(?P<dl_cat>[A-Z][A-Za-z'’ ]*\w)?( — )?"
        r'Volume (?P<dl_vol>\d{1,2})( — )'
        '(?P<dl_mon>(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*)'
        r' (?P<dl_day>\d{1,2}), (?P<dl_year>\d{4})')
    if found:
        found = re.sub(r'\s+', ' ', re.sub('<.+?>', '', found[0]).strip())
        if   (dl_gp := pcd_dl1_re.search(found)):
            dl_gp = dl_gp.groupdict(default='')
            dl_vol = int(dl_gp['dl_vol'])
            dl_iss = 'S' if dl_gp['dl_sp'] else dl_gp['dl_iss']
            dl_vol_iss = f'{dl_vol:02d}({dl_iss})'
            dl_year = dl_gp['dl_year']
            dl_mo = month_to_num[dl_gp['dl_mon']]
            dl_info = {
                'dateline':   found,
                'dl_vol_iss': dl_vol_iss,
                'dl_year_mo': f'{dl_year}-{dl_mo}',
                'dl_date':    vol_iss_to_date.get(dl_vol_iss, 'xxxx-xx')}
        elif (dl_gp := pcd_dl2_re.search(found)):
            dl_gp = dl_gp.groupdict(default='')
            dl_vol = int(dl_gp['dl_vol'])
            dl_year = dl_gp['dl_year']
            dl_mo = month_to_num[dl_gp['dl_mon']]
            dl_day = int(dl_gp['dl_day'])
            dl_info = {
                'dateline':   re.search(r'Volume.*?\d{4}', found).group(),
                'dl_vol_iss': f'{dl_vol:02d}',
                'dl_year_mo': f'{dl_year}',
                'dl_date':    f'{dl_year}-{dl_mo}-{dl_day:02d}',
                'dl_cat':     dl_gp['dl_cat']}
        else:
            dl_info = dict(dl='', dl_vol_iss='vv(i)', dl_year_mo='yyyy-mm',
                           dl_date='yyyy-mm-dd')
    if path in corrected_datelines: # 145 mising datelines from volume 9, 2012; 5 corrections
        found = corrected_datelines[path]
        dl_gp = pcd_dl2_re.search(found).groupdict(default='')
        dl_vol = int(dl_gp['dl_vol'])
        dl_year = dl_gp['dl_year']
        dl_mo = month_to_num[dl_gp['dl_mon']]
        dl_day = int(dl_gp['dl_day'])
        dl_info = {
            'dateline':   re.search(r'Volume.*?\d{4}', found).group(),
            'dl_vol_iss': f'{dl_vol:02d}',
            'dl_year_mo': re.search(r'/(\d{4})/', path).group(1),
            'dl_date':    f'{dl_year}-{dl_mo}-{dl_day:02d}',
            'dl_cat':     dl_gp['dl_cat']}
    dl_info['dl_art_num'] = article_numbers.get(path, '')
    dl_info['lang'] = 'en'
    return dl_info

pcd_art_en_dl_list = [
    dict(path=path, **pcd_art_en_dl_re_fn(path, soup)) #, sort_key=art_sort(path))
    for path, soup in tqdm(pcd_art_soup.items())
    if path in pcd_art_en_paths] # English
pcd_art_en_dl_df = pd.DataFrame(pcd_art_en_dl_list) # (3011, 8)
# ['path', 'dateline', 'dl_vol_iss', 'dl_year_mo', 'dl_date', 'dl_art_num', 'lang', 'dl_cat']

pcd_art_en_dl_df.to_excel('pcd_art_en_dl_df.xlsx', freeze_panes=(1,0))

# Work out dateline information for TOCs
# Propagate ['dl_vol_iss', 'dl_year_mo', 'dl_date', 'dl_art_num', 'dl_cat', 'dl_lang']
#   to non-English articles

# from path for non-English, deduce language and English-language link
def path_nen_to_en_fn(path):
    lang_match = re.search(r'(?P<stem>.*?)_(?P<lang>es|fr|zhs|zht)\.htm', path)
    end = lang_match.end('stem')
    path_en = path[:end] + '.htm'
    lang = lang_match.group('lang')
    return dict(lang=lang, path_en=path_en)
    
path_nen_to_en_list = [
    dict(path=path, **path_nen_to_en_fn(path)) for path in pcd_art_nen_paths]
path_nen_to_en_df = pd.DataFrame(path_nen_to_en_list) # (2080, 3)
# path_nen_to_en_df.lang.value_counts()

pcd_art_nen_dl_df = pd.merge(
    left=path_nen_to_en_df, 
    right=(pcd_art_en_dl_df
           .rename(columns={'path': 'path_en'})
           .drop(columns=['lang'])), 
    how='left', on='path_en').drop(columns=['path_en']) # (2080, 8)
# ['path', 'lang', 'dl_vol_iss', 'dl_year_mo', 'dl_date', 'dl_art_num', dl_cat]
pcd_art_nen_dl_df.to_excel('pcd_art_nen_dl_df.xlsx', freeze_panes=(1,0))

pcd_art_dl_df = (
    pd.concat([pcd_art_en_dl_df, pcd_art_nen_dl_df])
    .fillna('')
    .sort_values(['dl_date', 'dl_art_num', 'lang'])) # (5091, 8)
# ['path', 'dateline', 'dl_vol_iss', 'dl_year_mo', 'dl_date', 'dl_art_num', 'lang', 'dl_cat']
pcd_art_dl_df.to_excel('pcd_art_dl_df.xlsx', freeze_panes=(1,0))

#%% DataFrames to and from pickle
pcd_toc_dl_df.to_pickle('pcd_toc_dl_df.pkl')
pcd_art_dl_df.to_pickle('pcd_art_dl_df.pkl')

pcd_toc_dl_df = pd.read_pickle('pcd_toc_dl_df.pkl')
pcd_art_dl_df = pd.read_pickle('pcd_art_dl_df.pkl')

#%% PCD extra

# Partition articles by language
pcd_langs = defaultdict(lambda: dict(en=False, es=False, fr=False, zhs=False, zht=False))
for _path in sorted(pcd_cc_df.loc[pcd_cc_df.level=='article', 'path'].to_list()):
    _lang = re.search(r'(?P<stem>.+?)_?(?P<lang>es|fr|zhs|zht)?\.htm', _path)
    _lang = _lang.groupdict(default='en') # default='en' or ''
    # if _langgroups['lang'] == '': _langgroups['lang'] = 'en'
    pcd_langs[_lang['stem']][_lang['lang']] = True
del _path, _lang

pcd_langs_df = pd.DataFrame(
    data=[langs for langs in pcd_langs.values()], index=pcd_langs.keys())
pcd_langs_df.sum(axis=0)#.to_dict()
# {'en': 3011, 'es': 1011, 'fr': 357, 'zhs': 356, 'zht': 356}
pcd_langs_df.sum(axis=1).value_counts()#.to_dict()
# {1: 2000, 2: 653, 5: 355, 3: 2, 4: 1}

l_ = {'en': '__', 'es': '__', 'fr': '__', 'zhs': '___', 'zht': '___'}
pcd_langs_pat = pd.Series(
    data=['|'.join([lang if tf else l_[lang] for lang, tf in langs.items()])
          for langs in pcd_langs.values()], index=pcd_langs.keys(), name='pat')
pcd_langs_pat.value_counts()#.to_dict()
# {'en|__|__|___|___': 2000,
#  'en|es|__|___|___': 653,
#  'en|es|fr|zhs|zht': 355,
#  'en|es|fr|___|___': 2,
#  'en|es|__|zhs|zht': 1}

# pd.concat([pcd_langs_df, pcd_langs_pat], axis=1).to_excel('pcd_stem_langs.xlsx', freeze_panes=(1,0))

pcd_vol = [dict(path=path, seq=seq, length=len(tag.string.strip()), 
                string=tag.string.strip(), tag_name=tag.name, tag=str(tag))
                 for path, soup in tqdm(pcd_soup.items())
                 for seq, tag in enumerate(soup.find_all(name=True, string=re.compile(r"Vol(\.|ume| )")))]

pcd_vol = [dict(path=path, seq=seq, length=len(tag.string.strip()), 
                string=tag.string.strip(), tag_name=tag.name, tag=str(tag))
                 for path, soup in tqdm(pcd_soup.items())
                 for seq, tag in enumerate(soup.find_all(name=True, string=re.compile(r"Volume")))]
pd.DataFrame(pcd_vol).to_excel('pcd_vol-temp.xlsx', freeze_panes=(1,0))

set(pcd_soup.keys()) - set([item['path'] for item in pcd_vol]) # 1796

{mon: f'{seq+1:02d}' 
 for seq, mon in enumerate('jan|.|mar|apr|may|.|jul|.|sep|oct|nov'.split('|'))}
{mon: f'{seq+1:02d}' for 
 seq, mon in enumerate('January|February|March|April|May|June|'
                       'July|August|September|October|November|December'.split('|'))}

#%% Parse path: year and month (~ volume and issue)

## PCD volume and issue from path
# /pcd/issues/ \d{4}/({months})/toc.htm
# /pcd/es/     \d{4}_({months})_toc.htm
# /pcd/issues/ \d{4}/\d{4}_TOC.htm
# /pcd/es/     \d{4}_toc.htm

# /pcd/issues/ \d{4}/({months})/ cover.htm
# /pcd/issues/ \d{4}/({months})/ \d{2}_\d{4}(_es|_fr|_zhs|_zht)?.htm
# /pcd/issues/ \d{4}/({months})/ \d{2}_\d{4}(a|b|\da|\d)]?.htm
# /pcd/issues/ \d{4}/            \d{2}_\d{4}(e|r|_es)?.htm
# article name: '[0123456789_abcefhorstvz]{5,11}'

mon_to_num = {'jan': '01', 'mar': '03', 'apr': '04', 'may': '05', 
              'jul': '07', 'sep': '09', 'oct': '10', 'nov': '11'}

def pcd_path_re_fn(path):
    # use regular expressions to capture year, month, and article name
    # volume regular expression to capture year and month
    pcd_path_re = re.compile(r"""
        /pcd/(issu)?es/(?P<fn_year>\d{4})            # 4-digit year
        ((?P<delim>[_/])
         (?P<fn_mon>jan|mar|apr|may|jul|sep|oct|nov) # 3-letter month
         (?P=delim)toc)? |
        (/(?P=fn_year)_TOC) |
        (_toc)
        \.htm""", flags=re.X)
    # article regular expression to capture year, month, and article name
    pcd_art_re = re.compile(r"""
        /pcd/issues/(?P<fn_year>\d{4})                  # 4-digit year
        (/(?P<fn_mon>jan|mar|apr|may|jul|sep|oct|nov))? # 3-letter month
        /(?P<fn_art>[a-z0-9_]{5,11})                    # article name
        \.htm""", flags=re.X)
    art = pcd_art_re.match(path)
    path_info = dict()
    if art:
        path_info = art.groupdict(default='')
    else:
        vol = pcd_path_re.match(path)
        if vol:
            path_info = vol.groupdict(default='')
    if path_info.get('fn_year'):
        path_info['fn_vol'] = '{0:02d}'.format(int(path_info['fn_year']) - 2003)
        path_info['fn_year_mo'] = path_info['fn_year']
    if path_info.get('fn_mon'):
        path_info['fn_mon'] = mon_to_num[path_info['fn_mon']]
        path_info['fn_year_mo'] = path_info['fn_year'] + '-' + path_info['fn_mon']
    return path_info

pcd_path_list = [
    dict(path=path, 
         path_=re.sub(r'\d', '#',
                      re.sub('jan|mar|apr|may|jul|sep|oct|nov', 'MMM', path)), 
         **pcd_path_re_fn(path)) 
    for path in pcd_cc_paths]
pcd_path_df = pd.DataFrame(pcd_path_list)
# pcd_path_df.to_excel('pcd_path_df.xlsx', freeze_panes=(1,0))
pcd_path_df.value_counts(subset=['fn_year', 'fn_mon'], sort=False, dropna=False)
pcd_path_df.value_counts(subset=['fn_year_mo'], sort=False, dropna=False)
pcd_path_df.value_counts(subset=['fn_year', 'fn_mon', 'fn_year_mo', 'fn_vol'], sort=False, dropna=False)

pcd_path_df.loc[pcd_path_df.fn_year.isna()].path.to_list()
# ['/pcd/index.htm', '/pcd/issues/archive.htm', '/pcd/es/archive_es.htm']

