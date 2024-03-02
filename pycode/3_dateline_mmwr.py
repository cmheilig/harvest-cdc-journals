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
# mmwr_cc_df = pickle.load(open("pickle-files/mmwr_cc_df.pkl", "rb")) # (4786, 8)
mmwr_cc_df = pd.read_pickle('pickle-files/mmwr_cc_df.pkl')
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

#%% MMWR dateline


"""
4 sources: path, html, soup, json (corrections)

/mmwr/ind####_su.html,index####.htm,index####.html,indrr_####.html,indss_####.html
/mmwr/preview/ind####_rr.html,ind####_ss.html,ind####_su.html,index##.html

         toc   art
dateline soup  html, soup, json
series   path  dateline, html<title>
lang     path  path
year_mo  path  dateline
vol_iss  path  dateline
date     path  dateline
page     NA    dateline
"""
mmwr_dateline_corrections = json.load(open('mmwr_dateline_corrections_20231218_path.json', 'r')) # 260
mmwr_series_map = {
    'weekly': 'wr', 'quickg': 'wr', 'dispat': 'wr', 'semana': 'wr', 
    'recomm': 'rr', 'survei': 'ss', 'supple': 'su', '_': '_'}

def mmwr_dateline_fn(path, html, soup):
    # "dateline" is used here in 2 senses:
    # 1. the actual dateline which appears in (almost) all articles
    # 2. dateline information across all journals: 
        # series, language, year_mo, vol_iss, date, page
        # source included for diagnosing code in development
    # regular expression to extract series and year from volume path
    mmwr_vol_re = re.compile(
        '/mmwr(/preview)?'
        r'/ind(?P<ser0>ex|rr|ss)?_?(?P<fn_year>\d{2,4})_?(?P<ser1>rr|ss|su)?'
        '\.html?')
    # regular expression to extract information from article path
    mmwr_art_re = re.compile(
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
    # initialize dateline metadata elements
    dl, src, series, _year_mo, _vol_iss, _date, _page = ('',)*7
    lang = 'en'

    if   (vol := mmwr_vol_re.match(path)):
        vol = vol.groupdict(default='')
        # isolating element with Past/Current Volume(s) is tricky
        past_vols = soup.find_all(string=re.compile('(Past|Current) Volumes? \('))
        for found in past_vols:
            if found.parent.name == 'h1':
                dl = found.parent.get_text()
                break
        series = vol['ser0'] if vol['ser0'] else vol['ser1']
        if series == 'ex': series = 'wr'
        series = 'mm' + series
        _year_mo = vol['fn_year']
        if (len(_year_mo) == 2): _year_mo = '19' + _year_mo
        _vol_iss = '{0:02d}'.format(int(_year_mo) - 1951)
        _date = _year_mo
    elif (art := mmwr_art_re.match(path)):
        art = art.groupdict(default='')
        if 'ensp' in art['fn_stem']: lang = 'es'
        # dateline from json
        if path in mmwr_dateline_corrections:
            src = 'cx'
            dl = mmwr_dateline_corrections[path]
        # dateline from soup
        elif (soup_dl := soup.find('div', class_='dateline')):
            src = 'div'
            if soup_dl: dl = soup_dl.get_text(strip=True)
        # dateline from html: reportTitle and reportDate
        if dl == '':
            rT = re.search('reportTitle = ["\'](.*?)["\'];', html)
            rT = '' if rT is None else rT.group(1)
            rD = re.search(
                'reportDate = ["\'].*?([1-9ADFJMNOS].+?)["\'];', html)
            rD = '' if rD is None else rD.group(1)
            src = 'var'
            dl = (rT + ' / ' + rD)
        # dateline string: '<series>/<date>/<vol(iss);pages'
        mmwr_dl_re = re.compile(
            '(?P<series>.*?)/(?P<date>.*?)/'
            r'\s*(?P<vol>\d+) ?\((?P<iss>.*?)\)[;,]?(?P<page>.*)'
            # r'(?P<vol>\d+)\((?P<iss>.*?\d+)\);[\D]*(?P<page>\d+)'
            )
        if (dl_gp := mmwr_dl_re.search(dl)):
            dl_gp = dl_gp.groupdict('')
            # map series portion of dateline to 2-letter value
            series = mmwr_series_map.get(dl_gp['series']
                                         .lstrip()[:6].lower(), '')
            # ad hoc Notifiable Diseases (nd) from <title> and 'md'
            if (title := soup.find('title')): 
                title = title.get_text(strip=True)
            else: title = ''
            if (art['fn_stem'].endswith('md') or 
                title.startswith('Notifiable Diseases')):
                series = 'nd'
            series = 'mm' + series
            ## date
            try:
                _date = parse_date(dl_gp['date']).date().isoformat()
            except:
                pass
            # issue: Special OR remove nondigits and format as 02d
            if dl_gp['iss'] == 'Special Issue':
                _iss = 'SP'
            else:
                _iss = '{0:02d}'.format(int(re.sub(r'\D', '', dl_gp['iss'])))
            # dl_vol_iss
            _vol_iss = '{0}({1})'.format(dl_gp['vol'], _iss)
            # dl_year_mo: year of volume might not equal year published
            _year_mo = '{0:04d}'.format(int(dl_gp['vol']) + 1951)
            # page
            if (dl_pg := re.search(r'\d+', dl_gp['page'])):
                _page = '{0:04d}'.format(int(dl_pg.group()))
            else:
                _page = re.search(r'[ivx]+', dl_gp['page']).group()
                        # ''.join(re.findall(r'[ivx]+', dl_gp['page']))
    else:
        pass
    return dict(src=src,
                dateline=dl, series=series, lang=lang, 
                dl_year_mo=_year_mo, dl_vol_iss=_vol_iss, 
                dl_date=_date, dl_page=_page)

# mmwr_paths = mmwr_cc_df.mirror_path.to_list()
mmwr_toc_dl_list = [
    dict(path=path, **mmwr_dateline_fn(path, html, mmwr_toc_soup[path]))
    for path, html in tqdm(mmwr_toc_unx.items())]
# 135/135 [00:00<00:00, 185.49it/s]
mmwr_art_dl_list = [
    dict(path=path, **mmwr_dateline_fn(path, html, mmwr_art_soup[path]))
    for path, html in tqdm(mmwr_art_unx.items())]
# 15161/15161 [00:32<00:00, 466.30it/s]

mmwr_toc_dl_df = pd.DataFrame(mmwr_toc_dl_list) # (135, 9)
mmwr_art_dl_df = pd.DataFrame(mmwr_art_dl_list) # (15161, 9)
# ['path', 'src', 'dateline', 'series', 'lang', 
#  'dl_year_mo', 'dl_vol_iss', 'dl_date', 'dl_page']

pd.concat([mmwr_toc_dl_df, mmwr_art_dl_df]).to_excel('mmwr_dl_df.xlsx', 
                                                     freeze_panes=(1,0))
#%% DataFrames to and from pickle
mmwr_toc_dl_df.to_pickle('mmwr_toc_dl_df.pkl')
mmwr_art_dl_df.to_pickle('mmwr_art_dl_df.pkl')

mmwr_toc_dl_df = pd.read_pickle('mmwr_toc_dl_df.pkl')
mmwr_art_dl_df = pd.read_pickle('mmwr_art_dl_df.pkl')
