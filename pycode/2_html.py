#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Preprocess raw HTML before focusing on metadata and content

@author: cmheilig

0. Set up environment
1. Retrieve mirror structure DataFrames *_cc_df
2. Read raw HTML files into dictionary objects {mmwr,eid,pcd}_{toc,raw}_raw
   2a,2b Pickle/unpickle *_*_raw
3. Resolve character sets (e.g., win-1252) and exceptions resulting from decoding
4. Resolve character and numeric entitity references {mmwr,eid,pcd}_{toc,raw}_uni
   "Unescape" HTML as Unicode text (UTF-8) and reduce space-like characters
   Resolve ad hoc errors, such as multiple </body> tags
   4a,4b Pickle/unpickle *_*_uni
5. Reduce space by trimming <svg> elements {mmwr,eid,pcd}_{toc,raw}_unx
   5a,5b Pickle/unpickle *_*_unx
"""

#%% 0. Set up environment
from collections import Counter
import html
import unicodedata
os.chdir('/Users/cmheilig/cdc-corpora/_test')

#%% 1. Retrieve mirror structure DataFrames

# kludge to correct an issue with construction of mirror_path

# mmwr_cc_df = pickle.load(open('pickle-files/mmwr_cc_df.pkl', 'rb')) # (15297, 8)
mmwr_cc_df = pd.read_pickle('pickle-files/mmwr_cc_df.pkl')
mmwr_cc_df['mirror_path'] = mmwr_cc_df['mirror_path'].str.replace('\\', '/')

# eid_cc_df = pickle.load(open("pickle-files/eid_cc_df.pkl", "rb")) # (13100, 8)
eid_cc_df = pd.read_pickle('pickle-files/eid_cc_df.pkl')
eid_cc_df['mirror_path'] = eid_cc_df['mirror_path'].str.replace('\\', '/')

# pcd_cc_df = pickle.load(open("pickle-files/pcd_cc_df.pkl", "rb")) # (4786, 8)
pcd_cc_df = pd.read_pickle('pickle-files/pcd_cc_df.pkl')
pcd_cc_df['mirror_path'] = pcd_cc_df['mirror_path'].str.replace('\\', '/')

ser_cc_df = pd.concat([
   mmwr_cc_df.assign(ser='mmwr'),
   eid_cc_df.assign(ser='eid'),
   pcd_cc_df.assign(ser='pcd')],
   axis = 0, ignore_index = True) # (33576, 9)
#pickle.dump(ser_cc_df, open('ser_cc_df.pkl', 'xb'))
ser_cc_df.to_pickle('ser_cc_df.pkl')
# ser_cc_df.to_excel('ser_cc_df.xlsx', engine='openpyxl', freeze_panes=(1,0))

# ser_cc_df = pickle.load(open("pickle-files/ser_cc_df.pkl", "rb")) # (33576, 9)
ser_cc_df = pd.read_pickle('pickle-files/ser_cc_df.pkl')

#%% 2. Read raw HTML files into dictionry objects
MMWR_BASE_PATH_b0 = normpath(expanduser('~/cdc-corpora/'))
EID_BASE_PATH_b0 = normpath(expanduser('~/cdc-corpora/'))
PCD_BASE_PATH_b0 = normpath(expanduser('~/cdc-corpora/'))

mmwr_toc_raw = {
    path: read_raw_html(MMWR_BASE_PATH_b0 + path)
    for path in tqdm(mmwr_cc_df.loc[mmwr_cc_df.level.isin(['series', 'volume']), 
                                     'mirror_path'].sort_values()) } # 139
mmwr_art_raw = {
    path: read_raw_html(MMWR_BASE_PATH_b0 + path)
    for path in tqdm(mmwr_cc_df.loc[mmwr_cc_df.level == 'article', 
                                     'mirror_path'].sort_values()) } # 15435

eid_toc_raw = {
    path: read_raw_html(EID_BASE_PATH_b0 + path)
    for path in tqdm(eid_cc_df.loc[eid_cc_df.level.isin(['volume', 'issue']), 
                                    'mirror_path'].sort_values()) } # 345
eid_art_raw = {
    path: read_raw_html(EID_BASE_PATH_b0 + path)
    for path in tqdm(eid_cc_df.loc[eid_cc_df.level == 'article',
                                    'mirror_path'].sort_values()) } # 13310

pcd_toc_raw = {
    path: read_raw_html(PCD_BASE_PATH_b0 + path)
    for path in tqdm(pcd_cc_df.loc[pcd_cc_df.level.isin(['series', 'volume']), 
                                    'mirror_path'].sort_values()) } # 88
pcd_art_raw = {
    path: read_raw_html(PCD_BASE_PATH_b0 + path)
    for path in tqdm(pcd_cc_df.loc[pcd_cc_df.level == 'article',
                                    'mirror_path'].sort_values()) } # 5194


#%% 2a. Pickle dictionaries of raw HTML files
pickle.dump(mmwr_toc_raw, open('mmwr_toc_raw.pkl', 'xb'))
pickle.dump(mmwr_art_raw, open('mmwr_art_raw.pkl', 'xb'))
pickle.dump(eid_toc_raw, open('eid_toc_raw.pkl', 'xb'))
pickle.dump(eid_art_raw, open('eid_art_raw.pkl', 'xb'))
pickle.dump(pcd_toc_raw, open('pcd_toc_raw.pkl', 'xb'))
pickle.dump(pcd_art_raw, open('pcd_art_raw.pkl', 'xb'))

#%% 2b. Unpickle dictionaries of raw HTML files
mmwr_toc_raw = pickle.load(open('pickle-files/mmwr_toc_raw.pkl', 'rb'))
mmwr_art_raw = pickle.load(open('pickle-files/mmwr_art_raw.pkl', 'rb'))
eid_toc_raw = pickle.load(open('pickle-files/eid_toc_raw.pkl', 'rb'))
eid_art_raw = pickle.load(open('pickle-files/eid_art_raw.pkl', 'rb'))
pcd_toc_raw = pickle.load(open('pickle-files/pcd_toc_raw.pkl', 'rb'))
pcd_art_raw = pickle.load(open('pickle-files/pcd_art_raw.pkl', 'rb'))

#%% 3. Review and test declared character sets

## Review declared character sets
meta_charset_reb = re.compile(rb'<meta.*charset="?(.*?)"', flags=re.M|re.I)
meta_utf8char_reb = re.compile(rb'^<meta.*charset="?utf-8.*$', flags=re.M|re.I)

Counter([str(meta_charset_reb.findall(html_b))
         for html_b in tqdm(mmwr_toc_raw.values())])
Counter({"[b'utf-8']": 139})

Counter([str(meta_charset_reb.findall(html_b))
         for html_b in tqdm(mmwr_art_raw.values())])
Counter({'[]': 5152,
         "[b'windows-1252']": 3557,
         "[b'windows-1252', b'windows-1252']": 2,
         "[b'utf-8']": 6712,
         "[b'UTF-8']": 12})

Counter([str(meta_charset_reb.findall(html_b))
         for html_b in tqdm(eid_toc_raw.values())])
Counter({"[b'utf-8']": 345})

Counter([str(meta_charset_reb.findall(html_b))
         for html_b in tqdm(eid_art_raw.values())])
Counter({"[b'utf-8']": 13310})

Counter([str(meta_charset_reb.findall(html_b))
         for html_b in tqdm(pcd_toc_raw.values())])
Counter({"[b'windows-1252']": 70, 
         "[b'iso-8859-1']": 4, 
         "[b'utf-8']": 14})

Counter([str(meta_charset_reb.findall(html_b))
         for html_b in tqdm(pcd_art_raw.values())])
Counter({"[b'windows-1252']": 2087,
         "[b'GB2312']": 357,                # Simplified Chinese
         "[b'big5']": 355,                  # Traditional Chinese
         "[b'utf-8']": 2087,
         "[b'iso-8859-1']": 303,
         "[b'utf-8', b'windows-1252']": 4})

Counter([str(meta_charset_reb.findall(html_b))
         for html_b in tqdm(mmwr_toc_raw.values())] +
        [str(meta_charset_reb.findall(html_b))
         for html_b in tqdm(mmwr_art_raw.values())] +
        [str(meta_charset_reb.findall(html_b))
         for html_b in tqdm(eid_toc_raw.values())] +
        [str(meta_charset_reb.findall(html_b))
         for html_b in tqdm(eid_art_raw.values())] +
        [str(meta_charset_reb.findall(html_b))
         for html_b in tqdm(pcd_toc_raw.values())] +
        [str(meta_charset_reb.findall(html_b))
         for html_b in tqdm(pcd_art_raw.values())])
Counter({"[b'utf-8']": 22607,
         '[]': 5152,
         "[b'windows-1252']": 5714,
         "[b'windows-1252', b'windows-1252']": 2,
         "[b'UTF-8']": 12,
         "[b'iso-8859-1']": 307,
         "[b'GB2312']": 357,
         "[b'big5']": 355,
         "[b'utf-8', b'windows-1252']": 4})
charset_freqs = _
sorted(set([y for x in list(charset_freqs) for y in eval(x)]))
[b'GB2312', b'UTF-8', b'big5', b'iso-8859-1', b'utf-8', b'windows-1252']


## Test declared character sets and undeclared codecs; convert to UTF-8
# ['ascii', 'windows-1252', 'iso-8859-1', 'utf-8']
def catch_decoders(path, html_b):
    result = {'ascii': None, 'windows-1252': None, 'iso-8859-1': None, 
              'big5': None, 'gb2312': None, 'utf-8': None, 
              'charset': '', 'mismatch': False}
    result['charset'] = str(meta_charset_reb.findall(html_b))
    for codec in ['ascii', 'windows-1252', 'iso-8859-1', 'big5', 'gb2312', 'utf-8']:
        try:
            html_b.decode(codec, errors='strict')
            result[codec] = True
        except UnicodeDecodeError as e:
            result[codec] = False
            if (codec in result['charset'].lower()):
                result['mismatch'] = True
    return result

ser_codecs = [
    dict(ser=f'{ser}_{lev}', path=path, **catch_decoders(path, html_b))
    for ser in ['mmwr', 'eid', 'pcd']
    for lev in ['toc', 'art']
    for path, html_b in tqdm(eval(f'{ser}_{lev}_raw').items())]
ser_codecs_df = pd.DataFrame(ser_codecs) # (34510, 10)
ser_codecs_df.to_excel('ser_codecs_df.xlsx', freeze_panes=(1,0))

ser_codes_freqs = {
    codec: pd.crosstab(ser_codecs_df.charset, ser_codecs_df[codec], margins=True)
           for codec in ['ascii', 'windows-1252', 'iso-8859-1', 'big5', 'gb2312', 'utf-8']}
pd.concat(ser_codes_freqs, axis=1).to_excel('ser_codes_freqs.xlsx', merge_cells=True)

# anomalies
codec_anom_paths = [
 '/mmwr/preview/mmwrhtml/mm4903a3.htm',  # 'windows-1252' declared twice
 '/mmwr/preview/mmwrhtml/ss4808a2.htm',  # 'windows-1252' declared twice, 'cp1252' errs 
 '/pcd/issues/2006/jan/05_0093_zhs.htm', # 'GB2312' declared but errs
 '/pcd/issues/2006/jul/05_0127_zhs.htm', # 'GB2312' declared but errs
 '/pcd/issues/2006/oct/05_0236_zhs.htm', # 'GB2312' declared but errs
 '/pcd/issues/2006/oct/05_0243_zhs.htm', # 'GB2312' declared but errs
 '/pcd/issues/2006/oct/06_0001_zhs.htm', # 'GB2312' declared but errs
 '/pcd/issues/2006/oct/06_0011_zhs.htm', # 'GB2312' declared but errs
 '/pcd/issues/2012/11_0132.htm',         # 'windows-1252' and 'utf-8' declared
 '/pcd/issues/2012/11_0212.htm',         # 'windows-1252' and 'utf-8' declared
 '/pcd/issues/2014/14_0326.htm',         # 'windows-1252' and 'utf-8' declared
 '/pcd/issues/2014/14_0361.htm',         # 'windows-1252' and 'utf-8' declared
 '/pcd/issues/2015/15_0187.htm',         # 'utf-8' declared but errs
 '/pcd/issues/2017/17_0013.htm']         # 'utf-8' declared but errs

codec_anoms = {}
codec_anoms.update({path: html_b for path, html_b in mmwr_art_raw.items() 
                    if path in codec_anom_paths})
codec_anoms.update({path: html_b for path, html_b in pcd_art_raw.items() 
                    if path in codec_anom_paths})
list(codec_anoms) == codec_anom_paths # True

for path, html_b in codec_anoms.items():
    fname = re.search(r'/(\w*?)\.', path).group(1)
    print(f'path: {path}')
    for codec in ['ascii', 'cp1252', 'latin1', 'utf_8', 'big5', 'gb2312']:
        print(f'  {fname}_{codec}.htm')
        with open(f'_anoms/{fname}_{codec}.htm', 'x') as f_out:
            f_out.write(html_b.decode(codec, errors='backslashreplace'))
# del codec_anoms, path, html_b, fname, f_out

# For 1 MMWR, replace b'\x81'
mmwr_art_raw['/mmwr/preview/mmwrhtml/ss4808a2.htm'] = (
    mmwr_art_raw['/mmwr/preview/mmwrhtml/ss4808a2.htm'].replace(b'\x81', b'\xfc'))
# for 2 PCD, replace b'\xc2\x80\x99'
pcd_art_raw['/pcd/issues/2015/15_0187.htm'] = (
    pcd_art_raw['/pcd/issues/2015/15_0187.htm'].replace(b'\xc2\x80\x99', b'\xe2\x80\x99'))
pcd_art_raw['/pcd/issues/2017/17_0013.htm'] = (
    pcd_art_raw['/pcd/issues/2017/17_0013.htm'].replace(b'\xc2\x80\x99', b'\xe2\x80\x99'))

# Convert to UTF-8
mmwr_toc_uni = {
    path: html_to_unicode_b(html_b, try_=False)
    for path, html_b in tqdm(mmwr_toc_raw.items())}
mmwr_art_uni = {
    path: html_to_unicode_b(html_b, try_=False)
    for path, html_b in tqdm(mmwr_art_raw.items())}
eid_toc_uni = {
    path: html_to_unicode_b(html_b, try_=False)
    for path, html_b in tqdm(eid_toc_raw.items())}
eid_art_uni = {
    path: html_to_unicode_b(html_b, try_=False)
    for path, html_b in tqdm(eid_art_raw.items())}
pcd_toc_uni = {
    path: html_to_unicode_b(html_b, try_=False)
    for path, html_b in tqdm(pcd_toc_raw.items())}
pcd_art_uni = {
    path: html_to_unicode_b(html_b, try_=False)
    for path, html_b in tqdm(pcd_art_raw.items())}

Counter([html_b.count(b'\xef\xbb\xbf') for html_b in mmwr_art_raw.values()])
# Counter({0: 12289, 1: 3145, 60: 1})
Counter([html_b.count(b'\xef\xbb\xbf') for html_b in eid_art_raw.values()])
# Counter({0: 13298, 1: 5, 2: 5, 3: 1, 9: 1})
Counter([html_b.count(b'\xef\xbb\xbf') for html_b in pcd_art_raw.values()])
# Counter({0: 4566, 3: 437, 2: 179, 4: 9, 1: 2})

Counter([html_u.count('xef\xbb\xbf') for html_u in mmwr_art_uni.values()])
# Counter({0: 15435})
Counter([html_u.count('xef\xbb\xbf') for html_u in eid_art_uni.values()])
# Counter({0: 13310})
Counter([html_u.count('xef\xbb\xbf') for html_u in pcd_art_uni.values()])
# Counter({0: 5193})

#%% 4. Resolve character and numeric entitity references
char_ent_re = re.compile(r'&[#]?\w+;') # established elsewhere that all end w ';'
mmwr_ent_freqs = Counter([
    ent for html in tqdm((mmwr_toc_uni | mmwr_art_uni).values()) 
    for ent in char_ent_re.findall(html)]) # 257
eid_ent_freqs = Counter([
    ent for html in tqdm((eid_toc_uni | eid_art_uni).values()) 
    for ent in char_ent_re.findall(html)]) # 163
pcd_ent_freqs = Counter([
    ent for html in tqdm((pcd_toc_uni | pcd_art_uni).values()) 
    for ent in char_ent_re.findall(html)]) # 2635
len(mmwr_ent_freqs | eid_ent_freqs | pcd_ent_freqs) # 2757

def resolve_ref(ref):
    map_ords = {9: r'\t', 10: r'\n', 11: r'\v', 12: r'\f', 13: r'\v'}
    unesc = html.unescape(ref)
    if len(unesc) == 1:
        ord_ = ord(unesc)
        unesc_name = unicodedata.name(unesc, 'no name')
        ref_norm = html.entities.codepoint2name.get(ord_)
        if ord_ in map_ords: unesc = map_ords[ord_] # because \f makes to_excel cough
    else:
        ord_ = None
        unesc_name = ''
        ref_norm = None
    return dict(unesc=unesc, ord=ord_, unesc_name=unesc_name, ref_norm=ref_norm)

pd.concat([
    pd.DataFrame([
        dict(ser='mmwr', ent=ent, freq=freq, **resolve_ref(ent))
        for ent, freq in mmwr_ent_freqs.items()]),
    pd.DataFrame([
        dict(ser='eid', ent=ent, freq=freq, **resolve_ref(ent))
        for ent, freq in eid_ent_freqs.items()]),
    pd.DataFrame([
        dict(ser='pcd', ent=ent, freq=freq, **resolve_ref(ent))
        for ent, freq in pcd_ent_freqs.items()]),
    ]).to_excel('resolve_refs_all.xlsx', freeze_panes=(1,0))
# [3055 rows x 7 columns]

[path for path, html in eid_art_uni.items() if '&E;' in html]
['/eid/article/28/4/21-2334_article.html']
# '&E;' -> '&amp;E'
[path for path, html in pcd_art_uni.items() if '&8212;' in html]
['/pcd/issues/2008/oct/08_0063.htm']
# 'val&8212;n' -> 'valign'; '&8212;' -> '&mdash;'

eid_art_uni['/eid/article/28/4/21-2334_article.html'] = (
    eid_art_uni['/eid/article/28/4/21-2334_article.html'].replace('&E;', '&amp;E'))
pcd_art_uni['/pcd/issues/2008/oct/08_0063.htm'] = (
    pcd_art_uni['/pcd/issues/2008/oct/08_0063.htm'].replace('val&8212;n', 'valign'))

[path for path, html in pcd_art_uni.items() if '&#61620;' in html]
['/pcd/issues/2015/14_0368.htm']
[path for path, html in mmwr_art_uni.items() if '&#1467;' in html]
['/mmwr/volumes/70/wr/mm7044a1.htm']
[path for path, html in mmwr_art_uni.items() if '&#1560;' in html]
['/mmwr/volumes/67/wr/mm6713e1.htm']
[path for path, html in eid_art_uni.items() if '&rlm;' in html]
['/eid/article/17/12/11-0453_article.html']
[path for path, html in pcd_art_uni.items() if '&#8289;' in html]
['/pcd/issues/2019/18_0441.htm', '/pcd/issues/2024/23_0433.htm']
[path for path, html in mmwr_art_uni.items() if '&#65533;' in html]
['/mmwr/volumes/67/wr/mm6713e1.htm']

# Clean up entity reference anomalies and reduce space

# Reduce space differently if <pre> element present
Counter([re.search('<pre[ >]', html, flags=re.I) is not None for html in mmwr_art_uni.values()])
Counter({False: 14400, True: 1035})
Counter([re.search('<pre[ >]', html, flags=re.I) is not None for html in eid_art_uni.values()])
Counter({False: 13310})
Counter([re.search('<pre[ >]', html, flags=re.I) is not None for html in pcd_art_uni.values()])
Counter({False: 5193})

def ent_repair(html_u):
    # &thinsp; is sometimes large-number delimiter
    if '&thinsp;' in html_u:
        html_u = re.sub(r'&thinsp;(?=\d{3})', ',', html_u) # comma for thousands
    # entities to replace or delete; spaces will be converted and reduced
    html_u = (html_u
        .replace('&#129;', '&uuml;')  # Cc -> LATIN SMALL LETTER U WITH DIAERESIS
        .replace('&#157;', '')        # OPERATING SYSTEM COMMAND
        .replace('&#822;', '&ndash;') # COMBINING LONG STROKE OVERLAY -> EN DASH
        .replace('&#1467;', '')       # HEBREW POINT QUBUTS
        .replace('&#1560;', '')       # ARABIC SMALL FATHA
        .replace('&#8203;', '')       # ZERO WIDTH SPACE
        .replace('&lrm;', '')         # LEFT-TO-RIGHT MARK
        .replace('&rlm;', '')         # LEFT-TO-RIGHT MARK
        .replace('&#8288;', '')       # WORD JOINER
        .replace('&#8289;', '')       # FUNCTION APPLICATION
        .replace('&#11834;', '&mdash;') # TWO-EM DASH -> EM DASH
        .replace('&#61620;', '&times;') # MULTIPLICATION SIGN
        .replace('&#65533;', ''))     # REPLACEMENT CHARACTER
    html_u = html.unescape(html_u)
    return html_u

mmwr_toc_uni = {
    path: html_reduce_space_u(ent_repair(html_u), minim='<pre[ >]')
    for path, html_u in tqdm(mmwr_toc_uni.items())}
# 139/139 [00:00<00:00, 159.60it/s]
mmwr_art_uni = {
    path: html_reduce_space_u(ent_repair(html_u), minim='<pre[ >]')
    for path, html_u in tqdm(mmwr_art_uni.items())}
# 15435/15435 [02:34<00:00, 99.65it/s]
eid_toc_uni = {
    path: html_reduce_space_u(ent_repair(html_u))
    for path, html_u in tqdm(eid_toc_uni.items())}
# 345/345 [00:10<00:00, 31.62it/s]
eid_art_uni = {
    path: html_reduce_space_u(ent_repair(html_u))
    for path, html_u in tqdm(eid_art_uni.items())}
# 13310/13310 [05:24<00:00, 41.01it/s]
pcd_toc_uni = {
    path: html_reduce_space_u(ent_repair(html_u))
    for path, html_u in tqdm(pcd_toc_uni.items())}
# 88/88 [00:00<00:00, 170.11it/s]
pcd_art_uni = {
    path: html_reduce_space_u(ent_repair(html_u))
    for path, html_u in tqdm(pcd_art_uni.items())}
# 5193/5193 [00:39<00:00, 129.90it/s]

# Ad hoc corrections: 14 MMWRs have extra </body> and </html> tags
xtra_tags = [
 '/mmwr/preview/mmwrhtml/rr5806a4.htm',
 '/mmwr/preview/mmwrhtml/rr5807a2.htm',
 '/mmwr/preview/mmwrhtml/rr5808a1.htm',
 '/mmwr/preview/mmwrhtml/rr5809a1.htm',
 '/mmwr/preview/mmwrhtml/rr5810a1.htm',
 '/mmwr/preview/mmwrhtml/rr5810a1_ensp.htm',
 '/mmwr/preview/mmwrhtml/rr5812a1.htm',
 '/mmwr/preview/mmwrhtml/ss5803a1.htm',
 '/mmwr/preview/mmwrhtml/ss5805a1.htm',
 '/mmwr/preview/mmwrhtml/ss5806a2.htm',
 '/mmwr/preview/mmwrhtml/ss5807a1.htm',
 '/mmwr/preview/mmwrhtml/ss5808a1.htm',
 '/mmwr/preview/mmwrhtml/ss5809a2.htm',
 '/mmwr/preview/mmwrhtml/ss5810a2.htm']
for _path in xtra_tags:
    html_u = mmwr_art_uni[_path]
    html_u = re.sub('</body>', '', html_u, count=1, flags=re.I)
    html_u = re.sub('</html>', '', html_u, count=1, flags=re.I)
    mmwr_art_uni[_path] = html_u
del _path, html_u

#%% 4a. Pickle dictionaries of UTF-8 HTML files
pickle.dump(mmwr_toc_uni, open('mmwr_toc_uni.pkl', 'xb'))
pickle.dump(mmwr_art_uni, open('mmwr_art_uni.pkl', 'xb'))
pickle.dump(eid_toc_uni, open('eid_toc_uni.pkl', 'xb'))
pickle.dump(eid_art_uni, open('eid_art_uni.pkl', 'xb'))
pickle.dump(pcd_toc_uni, open('pcd_toc_uni.pkl', 'xb'))
pickle.dump(pcd_art_uni, open('pcd_art_uni.pkl', 'xb'))

#%% 4b. Unpickle dictionaries of UTF-8 HTML files
mmwr_toc_uni = pickle.load(open('pickle-files/mmwr_toc_uni.pkl', 'rb'))
mmwr_art_uni = pickle.load(open('pickle-files/mmwr_art_uni.pkl', 'rb'))
eid_toc_uni = pickle.load(open('pickle-files/eid_toc_uni.pkl', 'rb'))
eid_art_uni = pickle.load(open('pickle-files/eid_art_uni.pkl', 'rb'))
pcd_toc_uni = pickle.load(open('pickle-files/pcd_toc_uni.pkl', 'rb'))
pcd_art_uni = pickle.load(open('pickle-files/pcd_art_uni.pkl', 'rb'))

#%% 5. Reduce space by trimming <svg> elements

# If <SVG> tag is present, for each <svg> element,
#   remove attributes from all <svg> elements and their descendents
#   remove strings from furthest descendents

def trim_svg(html):
    soup = BeautifulSoup(html, 'lxml')
    if not re.search('<svg[ >]', html, flags=re.I):
        return str(soup)
    for svg_tag in soup.find_all('svg'):
        for child in svg_tag.find_all(True):
            child.attrs = dict()
            if (not child.find(True)) and (child.string):
                child.string = ""
        svg_tag.attrs = dict()
    return str(soup)

mmwr_toc_unx = {path: trim_svg(html) for path, html in tqdm(mmwr_toc_uni.items())}
# 139/139 [00:01<00:00, 69.70it/s]
mmwr_art_unx = {path: trim_svg(html) for path, html in tqdm(mmwr_art_uni.items())}
# 15435/15435 [09:05<00:00, 28.27it/s]
eid_toc_unx = {path: trim_svg(html) for path, html in tqdm(eid_toc_uni.items())}
# 345/345 [00:16<00:00, 20.86it/s]
eid_art_unx = {path: trim_svg(html) for path, html in tqdm(eid_art_uni.items())}
# 13310/13310 [06:29<00:00, 34.20it/s]
pcd_toc_unx = {path: trim_svg(html) for path, html in tqdm(pcd_toc_uni.items())}
# 88/88 [00:01<00:00, 59.70it/s]
pcd_art_unx = {path: trim_svg(html) for path, html in tqdm(pcd_art_uni.items())}
# 5193/5193 [01:10<00:00, 73.65it/s]

#%% 5a. Pickle dictionaries of reduced UTF-8 HTML files
pickle.dump(mmwr_toc_unx, open('mmwr_toc_unx.pkl', 'xb'))
pickle.dump(mmwr_art_unx, open('mmwr_art_unx.pkl', 'xb'))
pickle.dump(eid_toc_unx, open('eid_toc_unx.pkl', 'xb'))
pickle.dump(eid_art_unx, open('eid_art_unx.pkl', 'xb'))
pickle.dump(pcd_toc_unx, open('pcd_toc_unx.pkl', 'xb'))
pickle.dump(pcd_art_unx, open('pcd_art_unx.pkl', 'xb'))

#%% 5b. Unpickle dictionaries of reduced UTF-8 HTML files
mmwr_toc_unx = pickle.load(open('pickle-files/mmwr_toc_unx.pkl', 'rb'))
mmwr_art_unx = pickle.load(open('pickle-files/mmwr_art_unx.pkl', 'rb'))
eid_toc_unx = pickle.load(open('pickle-files/eid_toc_unx.pkl', 'rb'))
eid_art_unx = pickle.load(open('pickle-files/eid_art_unx.pkl', 'rb'))
pcd_toc_unx = pickle.load(open('pickle-files/pcd_toc_unx.pkl', 'rb'))
pcd_art_unx = pickle.load(open('pickle-files/pcd_art_unx.pkl', 'rb'))
