#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Assess HTML document length taken up by <svg> elements

@author: cmheilig

0. Set up environment
1. Unpickle *_*_uni, UTF-8 HTML documents
2. Calculate number of documents with at least 1 <svg> element
3. Assess incremental string lengths, successively trimming <svg> elements
4. Convert incremental information to DataFrames
5. Reduce space by trimming <svg> elements
"""

#%% 0. Set up environment
from collections import Counter
# import html
# import unicodedata
os.chdir('/Users/cmheilig/cdc-corpora/_test')

#%% 1. Unpickle dictionaries of UTF-8 HTML files
mmwr_toc_uni = pickle.load(open('pickle-files/mmwr_toc_uni.pkl', 'rb'))
mmwr_art_uni = pickle.load(open('pickle-files/mmwr_art_uni.pkl', 'rb'))
eid_toc_uni = pickle.load(open('pickle-files/eid_toc_uni.pkl', 'rb'))
eid_art_uni = pickle.load(open('pickle-files/eid_art_uni.pkl', 'rb'))
pcd_toc_uni = pickle.load(open('pickle-files/pcd_toc_uni.pkl', 'rb'))
pcd_art_uni = pickle.load(open('pickle-files/pcd_art_uni.pkl', 'rb'))

#%% 2. Calculate number of documents with at least 1 <svg> element
# evaluate html, head, body, pre; reduce space

# n_svg_docs = Counter()
Counter([re.search('<svg[ >]', html, flags=re.I) is not None 
         for html in mmwr_toc_uni.values()])
# n_svg_docs.update(Counter({True: 34, False: 101}))
Counter([re.search('<svg[ >]', html, flags=re.I) is not None 
         for html in mmwr_art_uni.values()])
# n_svg_docs.update(Counter({False: 11805, True: 3356}))
Counter([re.search('<svg[ >]', html, flags=re.I) is not None 
         for html in eid_toc_uni.values()])
# n_svg_docs.update(Counter({True: 330}))
Counter([re.search('<svg[ >]', html, flags=re.I) is not None 
         for html in eid_art_uni.values()])
# n_svg_docs.update(Counter({True: 12769}))
Counter([re.search('<svg[ >]', html, flags=re.I) is not None 
         for html in pcd_toc_uni.values()])
# n_svg_docs.update(Counter({False: 77, True: 10}))
Counter([re.search('<svg[ >]', html, flags=re.I) is not None 
         for html in pcd_art_uni.values()])
# n_svg_docs.update(Counter({False: 3727, True: 1364}))

# 17863/33573 documents have at least 1 <svg> element
# Counter({True: 17863, False: 15710})

# Construct lists of paths, stratified by presence of <svg>
svg_paths = []
for _ser in (['mmwr', 'eid', 'pcd']):
    for _lev in (['toc', 'art']):
        for _path, _html in tqdm(eval(f'{_ser}_{_lev}_uni.items()')):
            svg_paths.append(dict(
                path=_path,
                series=_ser,
                level=_lev,
                n_svg=len(re.findall('<svg[ >]', _html, flags=re.I))))
            # if re.search('<svg[ >]', _html, flags=re.I):
            #     svg_paths.append(_path)
del _ser, _lev, _path, _html
# Counter([path.get('n_svg') for path in svg_paths]).most_common(10)
Counter([path.get('n_svg')>0 for path in svg_paths])
# Counter({True: 17863, False: 15710})

#%% 3. Assess incremental string lengths, successively trimming <svg> elements
#      without storing copies of all strings

# 0. as is
# sum([len(str(soup)) for soup in tqdm(all_svg_soup.values())])
# 1. set svg attr values to ""
# 2. remove svg attr attr
# 3. remove svg strings
# 4. clear svg elements
# 5. decompose svg elements

svg_len0 = dict()
svg_len1 = dict()
svg_len2 = dict()
svg_len3 = dict()
svg_len4 = dict()
svg_len5 = dict()
svg_ntag = dict()

for _ser in (['mmwr', 'eid', 'pcd']):
  for _lev in (['toc', 'art']):
    for _path, _html in tqdm(eval(f'{_ser}_{_lev}_uni.items()')):
      _soup = BeautifulSoup(_html, 'lxml')
      # 0: string length after parsing
      svg_len0[_path] = len(str(_soup))
      _svg_tags = _soup.find_all('svg')
      svg_ntag[_path] = len(_svg_tags)
      if _svg_tags:
        # 1. string length after removing attribute values
        for _svg_tag in _svg_tags:
          for _child in _svg_tag.find_all(True):
            _child.attrs = {key: "" for key in _child.attrs.keys()}
          _svg_tag.attrs = {key: "" for key in _svg_tag.attrs.keys()}
        svg_len1[_path] = len(str(_soup))
        # 2. remove attributes
        for _svg_tag in _svg_tags:
          for _child in _svg_tag.find_all(True):
            _child.attrs = dict()
          _svg_tag.attrs = dict()
        svg_len2[_path] = len(str(_soup))
        # 3. remove strings
        for _svg_tag in _svg_tags:
          for _child in _svg_tag.find_all(True):
            if (not _child.find(True)) and (_child.string):
              _child.string = ""
        svg_len3[_path] = len(str(_soup))
        # 4. remove SVG contents
        for _svg_tag in _svg_tags:
          _svg_tag.clear()
        svg_len4[_path] = len(str(_soup))
        # 5. remove SVG element altogether
        for _svg_tag in _svg_tags:
          _svg_tag.decompose()
        svg_len5[_path] = len(str(_soup))
      else:
        svg_len1[_path] = svg_len0[_path] # use copy()?
        svg_len2[_path] = svg_len0[_path]
        svg_len3[_path] = svg_len0[_path]
        svg_len4[_path] = svg_len0[_path]
        svg_len5[_path] = svg_len0[_path]
del _ser, _lev, _path, _html, _soup, _svg_tags, _svg_tag, _child

# 135/135 [00:06<00:00, 21.61it/s]
# 15161/15161 [31:46<00:00,  7.95it/s]  
# 330/330 [01:33<00:00,  3.51it/s]
# 12769/12769 [32:59<00:00,  6.45it/s]
# 87/87 [00:04<00:00, 18.87it/s]
# 5091/5091 [04:30<00:00, 18.80it/s]

#%% 4. Convert incremental information to DataFrames

# also merge svg_paths
svg_paths_df = pd.DataFrame(svg_paths) # (33573, 4)

svg_lens_df = pd.DataFrame(dict(
    ntag=pd.Series(svg_ntag), 
    len0=pd.Series(svg_len0), len1=pd.Series(svg_len1), 
    len2=pd.Series(svg_len2), len3=pd.Series(svg_len3), 
    len4=pd.Series(svg_len4), len5=pd.Series(svg_len5), 
    dif01=pd.Series(svg_len0).sub(pd.Series(svg_len1)),
    dif12=pd.Series(svg_len1).sub(pd.Series(svg_len2)),
    dif23=pd.Series(svg_len2).sub(pd.Series(svg_len3)),
    dif34=pd.Series(svg_len3).sub(pd.Series(svg_len4)),
    dif45=pd.Series(svg_len4).sub(pd.Series(svg_len5)))) # (33573, 12)

# note: svg_paths_df.n_svg can differ from svg_lens_df.ntag
#   n_svg comes from regular expression, ntag from parsed document
#   differnces are always 1 or 0

(pd.merge(svg_paths_df, svg_lens_df, left_on='path', right_index=True)
 .to_excel('svg_lengths_df_20240304.xlsx', freeze_panes=(1,0)))
(pd.merge(svg_paths_df, svg_lens_df, left_on='path', right_index=True)
 .to_pickle('svg_lengths_df_20240304.pkl'))


#%% 5. Reduce space by trimming <svg> elements

# If <svg> tag is present, for each <svg> element,
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
# 135/135 [00:04<00:00, 29.05it/s]
mmwr_art_unx = {path: trim_svg(html) for path, html in tqdm(mmwr_art_uni.items())}
# 15161/15161 [21:57<00:00, 11.50it/s]
eid_toc_unx = {path: trim_svg(html) for path, html in tqdm(eid_toc_uni.items())}
# 330/330 [00:37<00:00,  8.80it/s]
eid_art_unx = {path: trim_svg(html) for path, html in tqdm(eid_art_uni.items())}
# 12769/12769 [14:39<00:00, 14.52it/s]
pcd_toc_unx = {path: trim_svg(html) for path, html in tqdm(pcd_toc_uni.items())}
# 87/87 [00:03<00:00, 25.82it/s]
pcd_art_unx = {path: trim_svg(html) for path, html in tqdm(pcd_art_uni.items())}
# 5091/5091 [02:50<00:00, 29.83it/s]
