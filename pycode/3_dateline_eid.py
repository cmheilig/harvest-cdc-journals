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
# eid_cc_df = pickle.load(open("pickle-files/eid_cc_df.pkl", "rb"))
eid_cc_df = pd.read_pickle('pickle-files/eid_cc_df.pkl')
eid_cc_df['mirror_path'] = eid_cc_df['mirror_path'].str.replace('\\', '/')
eid_cc_paths = eid_cc_df.mirror_path.to_list()

eid_toc_unx = pickle.load(open('pickle-files/eid_toc_unx.pkl', 'rb'))
eid_art_unx = pickle.load(open('pickle-files/eid_art_unx.pkl', 'rb'))


#%% EID
"""
Dateline elements:
    series: eid
    date:   YYYY, YYYY-MM
    volume: %02d (2 digits, leading zero) = YYYY - 1994
    issue:  %02d (2 digits, leading zero)
    language: en
"""
eid_toc_soup = {
    path: BeautifulSoup(html, 'lxml')
    for path, html in tqdm(eid_toc_unx.items())}
# 345/345 [00:08<00:00, 42.47it/s]
eid_art_soup = {
    path: BeautifulSoup(html, 'lxml')
    for path, html in tqdm(eid_art_unx.items())}
# 13310/13310 [06:43<00:00, 33.00it/s]

#%% EID dateline

"""
## EID volume and issue from path
/eid/past-issues/volume-(?P<vol>\d{1,2})
/eid/articles/issue/(?P<vol>\d{1,2})/(?P<iss>\d{1,2})/table-of-contents
/eid/article/(?P<vol>\d{1,2})/(?P<iss>\d{1,2})/(?P<art>[abcdeimnprt\d_-]{6,10})_article

/eid/article/##/##/##########_article
article name: '[0123456789_abcefhorstvz]{5,11}'
"""
month_to_num = {
    'January': '01', 'February': '02', 'March': '03', 'April': '04', 
    'May': '05', 'June': '06', 'July': '07', 'August': '08', 'September': '09', 
    'October': '10', 'November': '11', 'December': '12'}
missing_pages = json.load(open('json-inputs/eid_missing_pages.json', 'r'))

def eid_dl_re_fn(path, soup):
    path = path.removesuffix('.html')
    # volume/issue and year/month from path
    eid_vol_re = re.compile(r'/eid/past-issues/volume-(?P<fn_vol>\d{1,2})')
    eid_iss_re = re.compile(
        r'/eid/articles/issue/(?P<fn_vol>\d{1,2})/(?P<fn_iss>\d{1,2})'
        '/table-of-contents')
    eid_art_re = re.compile(
        r'/eid/article/(?P<fn_vol>\d{1,2})/(?P<fn_iss>\d{1,2})'
        '/(?P<fn_art>[a-z0-9_-]{6,10})_article')
    if   (dl_gp := eid_vol_re.match(path)):
        level = 'volume'
        dl_gp = dl_gp.groupdict(default='')
    elif (dl_gp := eid_iss_re.match(path)): 
        level = 'issue'
    #     dl_gp = dl_gp.groupdict(default='')
    elif (dl_gp := eid_art_re.match(path)): 
        level = 'article'
    #     dl_gp = dl_gp.groupdict(default='')

    # volume/issue and year/month from dateline text
    eid_dl_re = re.compile(
        'Volume (?P<dl_vol>\\d{1,2}), '
        '((Number (?P<dl_iss>\\d{1,2}))|(?P<dl_supp>Supplement))\N{EM DASH}'
        '(?P<dl_mon>[A-Sa-y]{3,9}) (?P<dl_year>\\d{4})')
    if (dl_str := soup.find(string=eid_dl_re)):
        dl_str = eid_dl_re.search(dl_str.string).group(0) # trim string
        dl_str_gp = eid_dl_re.search(dl_str).groupdict(default='')
        if level == 'volume':
            dl_str = dl_str[:dl_str.find(',')]
            dl_str_gp |= dict(dl_iss='', dl_mon='')
        else:
            dl_str = re.search(r'Volume.*\d{4}', dl_str).group(0) # trim string
    else:
        dl_str_gp = dict(dl_vol='', dl_iss='', dl_mon='', dl_year='')
    dl_str_vol = int(dl_str_gp.get('dl_vol', '0'))
    dl_str_iss = dl_str_gp.get('dl_iss', '')
    if (dl_str_gp.get('dl_supp', '') == 'Supplement'): 
        dl_str_iss = 'SU'
    elif (dl_str_iss != ''): 
        dl_str_iss = '{0:02d}'.format(int(dl_str_iss))
    dl_str_vol_iss = f'{dl_str_vol:02d}({dl_str_iss})' if dl_str_iss \
                     else f'{dl_str_vol:02d}'
    dl_str_mo = month_to_num.get(dl_str_gp.get('dl_mon', ''), '')
    dl_str_year_mo = dl_str_gp.get('dl_year', '0000') + \
        ('-' if dl_str_mo else '') + dl_str_mo
        
    dl_info = {
        'lang': 'en',
        'dateline': dl_str,
        # '_dl_vol': dl_str_vol, '_dl_iss': dl_str_iss,
        'dl_vol_iss': dl_str_vol_iss,
        'dl_year_mo': dl_str_year_mo}
    
    # if  level == 'volume':
    #     fn_vol = int(dl_gp.get('fn_vol', '0'))
    #     fn_year = f'{fn_vol+1994:04d}' if fn_vol else '0000'
    #     dl_info |= {
    #         'dl_vol_iss': f'{fn_vol:02d}',
    #         'dl_year_mo': fn_year}
    # elif level == 'issue':   pass
    # elif level == 'article': pass
    # else:                    dl_info = dict()
    
    # citation and harvest first page number
    if level == 'article':
        th_eid = soup.find(name='th', string='EID')
        citation = '' if th_eid is None else (
            soup.find(name='th', string='EID')
                .find_next_sibling(name='td', class_='citationCell')
                .get_text(strip=True))
        # 2017;23(12):2029
        page = re.search(r'\d{4};\d{1,2}\(\d{1,2}\):(?P<page>\d+)', citation)
        page = '' if not page else '{0:04d}'.format(int(page.group('page')))
        if (not page) and (path in missing_pages):
            page = missing_pages[path]
        dl_info['dl_page'] = page
    else: dl_info['dl_page'] = ''

    return dl_info

# ['path', 'lang', 'dateline', 'dl_vol_iss', 'dl_year_mo', 'dl_page']
eid_toc_dl_list = [
    dict(path=path, **eid_dl_re_fn(path, soup)) 
    for path, soup in tqdm(eid_toc_soup.items())] # 345
eid_toc_dl_df = pd.DataFrame(eid_toc_dl_list) # (345, 6)
eid_toc_dl_df.to_excel('eid_toc_dl_df.xlsx', freeze_panes=(1,0))

eid_art_dl_list = [
    dict(path=path, **eid_dl_re_fn(path, soup)) 
    for path, soup in tqdm(eid_art_soup.items())] # 13310
eid_art_dl_df = pd.DataFrame(eid_art_dl_list) # (13310, 6)
# 13310/13310 [00:15<00:00, 882.13it/s]
eid_art_dl_df.to_excel('eid_art_dl_df.xlsx', freeze_panes=(1,0))

#%% DataFrames to and from pickle
eid_toc_dl_df.to_pickle('eid_toc_dl_df.pkl')
eid_art_dl_df.to_pickle('eid_art_dl_df.pkl')

eid_toc_dl_df = pd.read_pickle('eid_toc_dl_df.pkl')
eid_art_dl_df = pd.read_pickle('eid_art_dl_df.pkl')

#%% EID extra
eid_vol = [dict(path=path, seq=seq, length=len(tag.string.strip()), 
                string=tag.string.strip(), tag_name=tag.name, tag=str(tag))
                 for path, soup in tqdm(eid_soup.items())
                 for seq, tag in enumerate(soup.find_all(name=True, string=re.compile(r"Vol(\.|ume| )")))]

# <title> string contains dateline Volume X, Volume 28, Number Y—[Month] [Year]
# <title>... - Volume X, Number Y—[Month] [Year] - Emerging Infectious Diseases journal - CDC</title>
#   except for /eid/article/28/8/22-0557_article.html
# <a href="...">Table of Contents – Volume X, Number Y—[Month] [Year]</a>
# <h5 class="header">Volume X, Number Y—[Month] [Year]</h5>
# <p class="mt-0 mb-0">Volume X, Number Y—[Month] [Year]</p>

# <title>               12693 +  76
# <a href>              12694 +  75
# <h5 class="header">   12204 + 565
# <p class="mt-0 mb-0"> 12694 +  75
# Table of Contents – Volume 23, Supplement—December 2017
# Table of Contents – Volume 28, Supplement—December 2022

def eid_dl_fn(soup):
   """Process selected metadata from HTML <head> element.
   Using SoupStrainer makes this even more efficent."""
   def str_dict(tag):
       if tag and tag.string and eid_dl_re.search(tag.string):
           _dict = eid_dl_re.search(tag.string).groupdict(default='')
           if _dict.get('dl_iss')=='':
               _dict['dl_iss'] = 'SU'
           return _dict
       else:
           return dict(dl_vol='', dl_iss='', dl_mon='', dl_yr='')
   dl_title = str_dict(soup.title)
   dl_toc_a = str_dict(soup.find('a', string=eid_dl_re))
   toc_a = soup.find('a', string=re.compile('Table of Contents')).string.strip()
   dl_header_h5 = str_dict(soup.find('h5', string=eid_dl_re))
   dl_mt0mb0_p = str_dict(soup.find('p', string=eid_dl_re))
   return dict(toc_a=toc_a, dl_title=dl_title, dl_toc_a=dl_toc_a, dl_header_h5=dl_header_h5,
               dl_mt0mb0_p=dl_mt0mb0_p)

eid_dl_list = [dict(path=path, **eid_dl_fn(soup))
                 for path, soup in tqdm(eid_soup.items())]
# 12769/12769 [00:42<00:00, 301.85it/s]
eid_dl_list_ = [dict(path=item['path'], toc_a=item['toc_a'],
                     dl_title='|'.join(item['dl_title'].values()),
                     dl_toc_a='|'.join(item['dl_toc_a'].values()),
                     dl_header_h5='|'.join(item['dl_header_h5'].values()),
                     dl_mt0mb0_p='|'.join(item['dl_mt0mb0_p'].values()))
                 for item in tqdm(eid_dl_list)]

Counter([len(soup.find_all(name='table', class_='citationTable'))
 for soup in tqdm(eid_soup.values())])
Counter({1: 12585, 0: 184})

Counter([len(soup.find_all(name='table', class_='citationTable'))
 for soup in tqdm(eid_soup.values())])
Counter({1: 12585, 0: 184})
x={path: str(soup(name='table', class_='citationTable'))
 for path, soup in tqdm(eid_soup.items())}
x=[dict(path=path, citable=citable)
 for path, citable in tqdm(x.items())]
x=[dict(path=path, citable=str(soup.find(name='table', class_='citationTable')))
 for path, soup in tqdm(eid_soup.items())]

citable_re = re.compile('<th class="citationHeader">(.+?)</th>')
y = [dict(path=item['path'], th='|'.join(sorted(citable_re.findall(item['citable']))))
     for item in x]
Counter([item['th'] for item in y])
Counter({'AMA|APA|EID': 12585, '': 184})

cite_ama_re = re.compile('<th class="citationHeader">AMA.*?class="citationCell">(.+?)</td>', flags=re.S)
cite_apa_re = re.compile('<th class="citationHeader">APA.*?class="citationCell">(.+?)</td>', flags=re.S)
cite_eid_re = re.compile('<th class="citationHeader">EID.*?class="citationCell">(.+?)</td>', flags=re.S)
y = [dict(path=item['path'], 
          ama='|'.join(cite_ama_re.findall(item['citable'])),
          apa='|'.join(cite_apa_re.findall(item['citable'])),
          eid='|'.join(cite_eid_re.findall(item['citable'])))
     for item in tqdm(x)]
with open('citationTable.txt', 'w') as f_out:
    f_out.write(json.dumps(y, indent=0))
pd.DataFrame(y).to_excel('citationTable.xlsx', freeze_panes=(1,0))

def eid_metadata_fn(soup):
    # th_eid = soup.find(name='th', string='EID')
    try:
        return soup.find(name='th', string='EID').find_next_sibling(name='td', class_='citationCell').get_text(strip=True)
    except:
        return ''
def eid_metadata_fn(soup):
    
z = [dict(path=path, citation=eid_metadata_fn(soup))
     for path, soup in tqdm(eid_soup.items())]

