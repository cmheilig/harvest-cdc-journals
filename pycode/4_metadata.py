#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: cmheilig

Extract, correct, harmonize, and organize metadata from MMWR, EID, and PCD
(Older MMWR publications are less consistent than those since 2016.)
These elements are extracted from file paths, <head> elements, and elsewhere

Metadata shared publicly in CSV format, 1 file for all corpora
    title
    canonical link (? redundant with URL)
    referring URL
    referred string
    keywords
    description
    doi
    authors
    categories
"""

#%% Set up environment
import pickle
import json
import csv
from collections import Counter, defaultdict
from bs4 import SoupStrainer
from pandas.api.types import CategoricalDtype
only_head = SoupStrainer(name='head')

os.chdir('/Users/cmheilig/cdc-corpora/_test')

#%% Retrieve (unpickle) trimmed, UTF-8 HTML
# mmwr_cc_df = pickle.load(open("pickle-files/mmwr_cc_df.pkl", "rb"))
mmwr_cc_df = pd.read_pickle('pickle-files/mmwr_cc_df.pkl')
# eid_cc_df = pickle.load(open("pickle-files/eid_cc_df.pkl", "rb"))
eid_cc_df = pd.read_pickle('pickle-files/eid_cc_df.pkl')
# pcd_cc_df = pickle.load(open("pickle-files/pcd_cc_df.pkl", "rb"))
pcd_cc_df = pd.read_pickle('pickle-files/pcd_cc_df.pkl')

mmwr_toc_unx = pickle.load(open('pickle-files/mmwr_toc_unx.pkl', 'rb'))
mmwr_art_unx = pickle.load(open('pickle-files/mmwr_art_unx.pkl', 'rb'))
eid_toc_unx = pickle.load(open('pickle-files/eid_toc_unx.pkl', 'rb'))
eid_art_unx = pickle.load(open('pickle-files/eid_art_unx.pkl', 'rb'))
pcd_toc_unx = pickle.load(open('pickle-files/pcd_toc_unx.pkl', 'rb'))
pcd_art_unx = pickle.load(open('pickle-files/pcd_art_unx.pkl', 'rb'))

mmwr_toc_dl_df = pd.read_pickle('pickle-files/mmwr_toc_dl_df.pkl')
mmwr_art_dl_df = pd.read_pickle('pickle-files/mmwr_art_dl_df.pkl')
eid_toc_dl_df = pd.read_pickle('pickle-files/eid_toc_dl_df.pkl')
eid_art_dl_df = pd.read_pickle('pickle-files/eid_art_dl_df.pkl')
pcd_toc_dl_df = pd.read_pickle('pickle-files/pcd_toc_dl_df.pkl')
pcd_art_dl_df = pd.read_pickle('pickle-files/pcd_art_dl_df.pkl')

#%% Parse HTML <head> elements

mmwr_toc_head_soup = {
    path: BeautifulSoup(html, 'lxml', parse_only=only_head, multi_valued_attributes=None)
    for path, html in tqdm(mmwr_toc_unx.items())}
# 135/135 [00:01<00:00, 118.13it/s]
mmwr_art_head_soup = {
    path: BeautifulSoup(html, 'lxml', parse_only=only_head, multi_valued_attributes=None)
    for path, html in tqdm(mmwr_art_unx.items())}
# 15161/15161 [04:18<00:00, 58.57it/s]
eid_toc_head_soup = {
    path: BeautifulSoup(html, 'lxml', parse_only=only_head, multi_valued_attributes=None)
    for path, html in tqdm(eid_toc_unx.items())}
# 330/330 [00:08<00:00, 38.77it/s]
eid_art_head_soup = {
    path: BeautifulSoup(html, 'lxml', parse_only=only_head, multi_valued_attributes=None)
    for path, html in tqdm(eid_art_unx.items())}
# 12769/12769 [03:30<00:00, 60.58it/s]
pcd_toc_head_soup = {
    path: BeautifulSoup(html, 'lxml', parse_only=only_head, multi_valued_attributes=None)
    for path, html in tqdm(pcd_toc_unx.items())}
# 87/87 [00:00<00:00, 115.94it/s]
pcd_art_head_soup = {
    path: BeautifulSoup(html, 'lxml', parse_only=only_head, multi_valued_attributes=None)
    for path, html in tqdm(pcd_art_unx.items())}
# 5091/5091 [00:39<00:00, 128.04it/s]

#%% MMWR, EID, PCD
"""
Metadata elements:
    title
    canonical link (? redundant with URL)
    referring URL
    referred string
    keywords
    description
    doi
    authors
    categories
"""
# 7 <meta> names to focus on; see detail for deeper exploration
meta_names = [
    'citation_categories', 'citation_author', 'citation_doi', 
    'keywords', 'Keywords', 'description', 'Description']

def head_soup_meta_fn(soup):
    """ """
    title = '' if soup.title.string is None else soup.title.string.strip()
    link_canon = soup.find('link', attrs={'rel': 'canonical'})
    link_canon = '' if link_canon is None else link_canon.get('href').strip()

    name_vals = {'md_' + meta_name: '' for meta_name in meta_names}
    name_vals.update(
        {'md_' + meta_name: '' if tag is None else tag.get('content').strip()
         for meta_name in meta_names
         for tag in [soup.find(name='meta', attrs={'name': meta_name})]
         if (tag is not None) and (tag.get('content') != '')})
    return dict(title=title, link_canon=link_canon, **name_vals)

#%% MMWR
mmwr_toc_md_list = [
    dict(path=path, **head_soup_meta_fn(soup))
    for path, soup in tqdm(mmwr_toc_head_soup.items())]
# 135/135 [00:00<00:00, 1295.11it/s]
mmwr_art_md_list = [
    dict(path=path, **head_soup_meta_fn(soup))
    for path, soup in tqdm(mmwr_art_head_soup.items())]
# 15161/15161 [00:10<00:00, 1446.80it/s]

## Resolve keywords and description
def mmwr_keywords_fn(kwds):
    # there are 115 issues with nonspecific keywords, uniquely containing "(CSTE)"
    # could have used "(NNDSS)" or "Voice of CDC"
    if (kwds == '') or ('(CSTE)' in kwds): return {''}
    # replace some observed substrings with more easily parsed strings
    kwds = (kwds
        .replace('&amp;', '&')
        .replace('&lt;', '<')
        .replace(',', ', ').replace(',  ', ', ') # 1 space after every comma
        .replace(', and MMWR', '')
        .replace(', and fumes', ', fumes') # 'vapor-gas, dust, and fumes'
        # coccidioidomycosis, histoplasmosis, and blastomycosis
        .replace(', and blastomycosis', ', blastomycosis') 
        .replace('Add article keywords here.', '')
        .replace('botulinum toxins A, B, C, D, E, F, and G', 'botulinum toxins')
        .replace('Hand, Foot, and Mouth Disease', 'Hand Foot and Mouth Disease')
        .replace('Hand, Foot, And Mouth Disease', 'Hand Foot and Mouth Disease')
        .replace('Hand, and Mouth Disease', 'Hand Foot and Mouth Disease')
        .replace('Methamphetamine, Injection Drug, Heroin, And Syphilis',
                 'Methamphetamine Injection Drug Heroin Syphilis')
        .replace('STEADI, (Stopping Elderly Accidents, Deaths, and Injuries)', 
                 'STEADI (Stopping Elderly Accidents Deaths Injuries)')
        .replace('suicidal thoughts, planning, and attempts', 
                 'suicidal thoughts planning and attempts')
        .replace('suicidal thoughts, and attempts', 
                 'suicidal thoughts planning and attempts')
        .replace('Washington State Breast, Cervical, and Colon Health Program',
                 'Washington State Breast Cervical Colon Health Program')
        .replace(', and ', ', ')
        .replace(', And ', ', ')
        .replace('; ', ', ') )
    kwds = set([x.strip() for x in kwds.split(',')])
    return kwds

# keywords to ignore
mmwr_ignore_kwds = {'',
    'CDC',
    'Centers For Disease Control and Prevention',
    'Centers for Disease Control',
    'Centers for Disease Control and Prevention',
    'MMWR',
    'Morbidity & Mortality Weekly Report',
    'Morbidity and Mortality Report',
    'Morbidity and Mortality Weekly Report'} 

# use md_description (not md_Description), except ignore:
mmwr_ignore_desc = {
    'Add article description here.',
    'MMWR, Morbidity and Mortality Weekly Report, CDC, Centers for Disease Control and Prevention',
    'The Morbidity and Mortality Weekly Report (MMWR) Recommendations and Reports are prepared by the Centers for Disease Control and Prevention (CDC).',
    'The Morbidity and Mortality Weekly Report (MMWR) Series is prepared by the Centers for Disease Control and Prevention (CDC).',
    'The Morbidity and Mortality Weekly Report (MMWR) Surveillance Summaries are prepared by the Centers for Disease Control and Prevention (CDC).',
    'The Morbidity and Mortality Weekly Report (MMWR) is prepared by the Centers for Disease Control and Prevention (CDC).'}
 
# process md_keywords and md_Keywords, take union, remove ignored kwds,
# sort what's left, join with '|'
# use only md_description, not md_Description
for md in tqdm(mmwr_toc_md_list + mmwr_art_md_list):
    md['md_kwds'] = '|'.join(sorted((mmwr_keywords_fn(md['md_keywords']) | 
                     mmwr_keywords_fn(md['md_Keywords'])) - mmwr_ignore_kwds))
    if md['md_description'] in mmwr_ignore_desc:
        md['md_desc'] = ''
    else:
        md['md_desc'] = md['md_description']
del md

mmwr_toc_md_df = pd.DataFrame(mmwr_toc_md_list)
# mmwr_toc_md_df.to_excel('mmwr_toc_md_df.xlsx', freeze_panes=(1,0))
# mmwr_toc_md_df.to_pickle('mmwr_toc_md_df.pkl')
mmwr_art_md_df = pd.DataFrame(mmwr_art_md_list)
# mmwr_art_md_df.to_excel('mmwr_art_md_df.xlsx', freeze_panes=(1,0))
# mmwr_art_md_df.to_pickle('mmwr_art_md_df.pkl')

#%% EID
eid_toc_md_list = [
    dict(path=path, **head_soup_meta_fn(soup))
    for path, soup in tqdm(eid_toc_head_soup.items())]
# 330/330 [00:00<00:00, 974.38it/s]
eid_art_md_list = [
    dict(path=path, **head_soup_meta_fn(soup))
    for path, soup in tqdm(eid_art_head_soup.items())]
# 12769/12769 [00:18<00:00, 689.97it/s]

def eid_keywords_fn(kwds):
    kwds = (kwds
        # extraneous bits
        .replace('\n', ', ')
        .replace('\xad ', '-')
        .replace('\u200b', '')
        .replace('\\', '')
        .replace('Keywords: ', '')
        .replace('Keywords:', '')
        .replace('<!-- INSERT SHAPE -->Keywords: ', '')
        .replace(' <!-- INSERT PICT -->', '')
        .replace(': influenza A virus', '')
        # confounding commas and parentheses
        .replace(', (Borrelia turdi, ', ', Borrelia turdi, ')
        .replace(', Spain), ', ', Spain, ')
        .replace('(Culebra Cut, Panama Canal)', '(Culebra Cut / Panama Canal)')
        .replace('“tâche noire,”', '“tâche noire”,')
        .replace('“unassisted by education, and unfettered by the rules of art,” ',
                 '“unassisted by education and unfettered by the rules of art”, ')
        )
    kwds = re.sub('Suggested citation for this article:'
           r'.*?10\.3201/eid\d{4}\.(AC|\d{2})\d{4}', 
           '', kwds)
    kwds = re.sub('Suggested citation for this article:'
           # r'[^,]*?, [^,]*?, [^,]*?, [^,]*?, [^,]*?, [^,]*?(, |\Z)', 
           r'[^,]*?, ([^,]*?, )?([^,]*?, )?([^,]*?, )?([^,]*?, )?[^,]*?(, |\Z)', 
           '', kwds)
    kwds = set([x.strip() for x in kwds.split(',')])
    return kwds

eid_ignore_kwds = {'',
    'CDC',
    'Centers for Disease Control and Prevention',
    'Centers for Disease Control and Prevention (U.S.)',
    'National Center for Immunization and Respiratory Diseases'} 

# process md_keywords, remove ignored kwds,
# sort what's left, join with '|'
# use only md_description, not md_Description
for md in tqdm(eid_toc_md_list + eid_art_md_list):
    md['md_kwds'] = '|'.join(sorted(eid_keywords_fn(md['md_keywords']) - 
                                    eid_ignore_kwds))
    md['md_desc'] = md['md_description']
del md

eid_toc_md_df = pd.DataFrame(eid_toc_md_list)
# eid_toc_md_df.to_excel('eid_toc_md_df.xlsx', freeze_panes=(1,0))
# eid_toc_md_df.to_pickle('eid_toc_md_df.pkl')
eid_art_md_df = pd.DataFrame(eid_art_md_list)
# eid_art_md_df.to_excel('eid_art_md_df.xlsx', freeze_panes=(1,0))
# eid_art_md_df.to_pickle('eid_art_md_df.pkl')

#%% PCD
pcd_toc_md_list = [
    dict(path=path, **head_soup_meta_fn(soup))
    for path, soup in tqdm(pcd_toc_head_soup.items())]
# 87/87 [00:00<00:00, 2942.78it/s]
pcd_art_md_list = [
    dict(path=path, **head_soup_meta_fn(soup))
    for path, soup in tqdm(pcd_art_head_soup.items())]
# 5091/5091 [00:02<00:00, 2192.65it/s]

def pcd_keywords_fn(kwds):
    kwds = (kwds
        .replace(';', ',')
        .replace('\x92', '’')
        .replace('Key words: ', '')
        .replace('Key Words: ', '')
        .replace('Tianjin, China', 'Tianjin China')
        )
    # insert commma before run-on PCD (and ignore it later)
    kwds = re.sub(r'\BPreventing Chronic Disease', ',Preventing Chronic Disease',
                  kwds)
    kwds = re.sub(r'NCCDPHP\B', 'NCCDPHP,',
                  kwds)
    kwds = set([x.strip() for x in kwds.split(',')])
    return kwds

pcd_ignore_kwds = {'',
    'CDC',
    'CDC - Wide Page example keywords goes here',
    'Centers for Disease Control and Prevention',
    'Centers for Disease Control and Prevention (U.S.)',
    'NCCDPHP',
    'National Center for Chronic Disease Prevention and Health Promotion',
    'PCD',
    'PCD Journal (Preventing Chronic Disease)',
    'Preventing Chronic Disease',
    'Preventing Chronic Disease Journal',
    'Preventing Chronic Disease journal',
    'Preventing Chronic Disease revista',
    'Revista PCD (Preventing Chronic Disease [prevención de enfermedades crónicas])'}

# use md_description (not md_Description), except ignore:
pcd_ignore_desc = {
    'CDC - Wide Page example description goes here',
    'Preventing Chronic Disease (PCD) is a peer-reviewed electronic journal established by the National Center for Chronic Disease Prevention and Health Promotion. PCD provides an open exchange of information and knowledge among researchers, practitioners, policy makers, and others who strive to improve the health of the public through chronic disease prevention.',
    'Preventing Chronic Disease (PCD) is a peer-reviewed electronic journal established by the National Center for Chronic Disease Prevention and Health Promotion. PCD provides an open exchange on the very latest in chronic disease prevention, research findings, public health interventions, and the exploration of new theories and concepts.'}

# process md_keywords and md_Keywords, take union, remove ignored kwds,
# sort what's left, join with '|'
# use only md_description, not md_Description
for md in tqdm(pcd_toc_md_list + pcd_art_md_list):
    md['md_kwds'] = '|'.join(sorted((pcd_keywords_fn(md['md_keywords']) | 
                     pcd_keywords_fn(md['md_Keywords'])) - pcd_ignore_kwds))
    if md['md_Description'] in pcd_ignore_desc:
        md['md_desc'] = ''
    else:
        md['md_desc'] = md['md_Description']
del md

pcd_toc_md_df = pd.DataFrame(pcd_toc_md_list) # (87, 12)
# pcd_toc_md_df.to_excel('pcd_toc_md_df.xlsx', freeze_panes=(1,0))
# pcd_toc_md_df.to_pickle('pcd_toc_md_df.pkl')
pcd_art_md_df = pd.DataFrame(pcd_art_md_list) # (5091, 12)
# pcd_art_md_df.to_excel('pcd_art_md_df.xlsx', freeze_panes=(1,0))
# pcd_art_md_df.to_pickle('pcd_art_md_df.pkl')

# pd.DataFrame(eid_toc_md_list + eid_art_md_list).to_excel('eid-temp.xlsx', freeze_panes=(1,0))

#%% Combine metadata sources
#   *_cc_df, *_toc_dl_df, *_art_dl_df, *_toc_md_df, *_art_md_df

## retrieve data if not already in session environment
# mmwr_cc_df = pd.read_pickle('pickle-files/mmwr_cc_df.pkl')
# eid_cc_df = pd.read_pickle('pickle-files/eid_cc_df.pkl')
# pcd_cc_df = pd.read_pickle('pickle-files/pcd_cc_df.pkl')
# mmwr_toc_dl_df = pd.read_pickle('pickle-files/mmwr_toc_dl_df.pkl')
# mmwr_art_dl_df = pd.read_pickle('pickle-files/mmwr_art_dl_df.pkl')
# eid_toc_dl_df = pd.read_pickle('pickle-files/eid_toc_dl_df.pkl')
# eid_art_dl_df = pd.read_pickle('pickle-files/eid_art_dl_df.pkl')
# pcd_toc_dl_df = pd.read_pickle('pickle-files/pcd_toc_dl_df.pkl')
# pcd_art_dl_df = pd.read_pickle('pickle-files/pcd_art_dl_df.pkl')
# mmwr_toc_md_df = pd.read_pickle('pickle-files/mmwr_toc_md_df.pkl')
# mmwr_art_md_df = pd.read_pickle('pickle-files/mmwr_art_md_df.pkl')
# eid_toc_md_df = pd.read_pickle('pickle-files/eid_toc_md_df.pkl')
# eid_art_md_df = pd.read_pickle('pickle-files/eid_art_md_df.pkl')
# pcd_toc_md_df = pd.read_pickle('pickle-files/pcd_toc_md_df.pkl')
# pcd_art_md_df = pd.read_pickle('pickle-files/pcd_art_md_df.pkl')

# combine *_cc_df, *_toc_dl_df, *_art_dl_df, *_toc_md_df, *_art_md_df
keep_levels = {'volume', 'issue', 'article'}
map_levels = dict(home='', series='', volume='toc', issue='toc', article='art')
series_cat = CategoricalDtype(
    ['mmwr', 'mmnd', 'mmrr', 'mmss', 'mmsu', 'eid', 'pcd'], ordered=True)
level_cat = CategoricalDtype(
    ['home', 'series', 'volume', 'issue', 'article'], ordered=True)
corpus_columns = ['url', 'stratum', 'collection', 'series', 'level', 'lang', 
    'dl_year_mo', 'dl_vol_iss', 'dl_date', 'dl_page', 'dl_art_num', 'dateline', 
    'base', 'string', 'link_canon', 'mirror_path', 'md_citation_doi', 'title', 
    'md_citation_categories', 'dl_cat', 'md_kwds', 'md_desc', 'md_citation_author']
sort_order = ['series', 'level', 'lang', # ~collection
              'dl_vol_iss', 'dl_date', 'dl_page', 'dl_art_num']
# columns dropped
# ['href', 'path_x', 'filename', 'mirror_path', 'path_y', 'src', 'path', 
#  'md_keywords', 'md_Keywords', 'md_description', 'md_Description']

#%% MMWR: combine sources
mmwr_corpus_df = pd.merge(
    left=pd.merge(
        left=(mmwr_cc_df                                   # (15297, 8)
              .loc[mmwr_cc_df.level.isin(keep_levels)]),   # (15292, 8)
        right=pd.concat([mmwr_toc_dl_df, mmwr_art_dl_df]), # (15296, 9)
        how='inner',
        left_on='mirror_path',
        right_on='path'),                                  # (15292, 17)
    right=pd.concat([mmwr_toc_md_df, mmwr_art_md_df]),     # (15296, 12)
    how='inner',
    left_on='mirror_path',
    right_on='path')                                       # (15292, 29)

# 19 collections of documents in the corpus
mmwr_corpus_df['collection'] = (
    mmwr_corpus_df['series']
    .str.cat(mmwr_corpus_df['level'].map(map_levels), sep='_')
    .str.cat(mmwr_corpus_df['lang'], sep='_'))
# mmwr_corpus_df.loc[mmwr_corpus_df['lang'] == 'es', 'collection'].value_counts().to_dict()
# {'mmwr_art_es': 19, 'mmsu_art_es': 2, 'mmrr_art_es': 1}
# all Spanish-language articles, even rr and su, are grouped as mmwr_art_es
mmwr_corpus_df.loc[mmwr_corpus_df['lang'] == 'es', 'collection'] = \
    'mmwr_art_es'
# 2 MMWR strata: mmwr_toc, mmwr_art
mmwr_corpus_df['stratum'] = ('mmwr_' + mmwr_corpus_df['level'].map(map_levels))
# columns corresponding to PCD
mmwr_corpus_df['dl_art_num'] = ''
mmwr_corpus_df['dl_cat'] = ''
# categories to impose sort order
mmwr_corpus_df['series'] = mmwr_corpus_df['series'].astype(series_cat)
mmwr_corpus_df['level'] = mmwr_corpus_df['level'].astype(level_cat)

mmwr_corpus_df = mmwr_corpus_df[corpus_columns].sort_values(sort_order)
# (15292, 23)

# mmwr_corpus_df.to_excel('mmwr_corpus_df.xlsx', freeze_panes=(1,0))

#%% EID: combine sources
eid_corpus_df = pd.merge(
    left=pd.merge(
        left=(eid_cc_df                                  # (13100, 8)
              .loc[eid_cc_df.level.isin(keep_levels)]),  # (13099, 8)
        right=pd.concat([eid_toc_dl_df, eid_art_dl_df]), # (13099, 6)
        how='inner',
        left_on='mirror_path',
        right_on='path'),                                # (13099, 14)
    right=pd.concat([eid_toc_md_df, eid_art_md_df]),     # (13099, 12)
    how='inner',
    left_on='mirror_path',
    right_on='path')                                     # (13099, 26)

eid_corpus_df['series'] = 'eid'

# collections of documents in the corpus
eid_corpus_df['collection'] = (
    eid_corpus_df['series']
    .str.cat(eid_corpus_df['level'].map(map_levels), sep='_')
    .str.cat(eid_corpus_df['lang'], sep='_'))
# 2 EID strata: eid_toc, eid_art
eid_corpus_df['stratum'] = ('eid_' + eid_corpus_df['level'].map(map_levels))
# columns corresponding to PCD
eid_corpus_df['dl_art_num'] = ''
eid_corpus_df['dl_cat'] = ''
eid_corpus_df['dl_date'] = ''
# categories to impose sort order
eid_corpus_df['series'] = eid_corpus_df['series'].astype(series_cat)
eid_corpus_df['level'] = eid_corpus_df['level'].astype(level_cat)

eid_corpus_df = eid_corpus_df[corpus_columns].sort_values(sort_order)
# (13099, 23)

# eid_corpus_df.to_excel('eid_corpus_df.xlsx', freeze_panes=(1,0))

#%% PCD: combine sources
pcd_corpus_df = pd.merge(
    left=pd.merge(
        left=(pcd_cc_df                                  # (5179, 8)
              .loc[pcd_cc_df.level.isin(keep_levels)]),  # (5176, 8)
        right=pd.concat([pcd_toc_dl_df, pcd_art_dl_df]), # (5178, 8)
        how='inner',
        left_on='mirror_path',
        right_on='path'),                                # (5176, 16)
    right=pd.concat([pcd_toc_md_df, pcd_art_md_df]),     # (5178, 12)
    how='inner',
    left_on='mirror_path',
    right_on='path').fillna('')                          # (5176, 28)

pcd_corpus_df['series'] = 'pcd'

# collections of documents in the corpus
pcd_corpus_df['collection'] = (
    pcd_corpus_df['series']
    .str.cat(pcd_corpus_df['level'].map(map_levels), sep='_')
    .str.cat(pcd_corpus_df['lang'], sep='_'))
# 2 PCD strata: mmwr_toc, mmwr_art
pcd_corpus_df['stratum'] = ('pcd_' + pcd_corpus_df['level'].map(map_levels))
# columns corresponding to MMWR and EID
pcd_corpus_df['dl_page'] = ''
# categories to impose sort order
pcd_corpus_df['series'] = pcd_corpus_df['series'].astype(series_cat)
pcd_corpus_df['level'] = pcd_corpus_df['level'].astype(level_cat)

pcd_corpus_df = pcd_corpus_df[corpus_columns].sort_values(sort_order)
# (5176, 23)

# pcd_corpus_df.to_excel('pcd_corpus_df.xlsx', freeze_panes=(1,0))

#%% Combine MMWR, EID, PCD
stratum_cat = CategoricalDtype(
    ['mmwr_toc', 'mmwr_art', 'eid_toc', 'eid_art', 'pcd_toc', 'pcd_art'], 
    ordered=True)
collection_cat = CategoricalDtype(
    ['mmwr_toc_en', 'mmrr_toc_en', 'mmss_toc_en', 'mmsu_toc_en', 
     'mmwr_art_en', 'mmrr_art_en', 'mmss_art_en', 'mmsu_art_en', 
     'mmnd_art_en', 'mmwr_art_es', 'eid_toc_en', 'eid_art_en', 
     'pcd_toc_en', 'pcd_toc_es', 'pcd_art_en', 'pcd_art_es', 
     'pcd_art_fr', 'pcd_art_zhs', 'pcd_art_zht'], ordered=True)

cdc_corpus_df = pd.concat([mmwr_corpus_df, eid_corpus_df, pcd_corpus_df])
cdc_corpus_df['stratum'] = cdc_corpus_df['stratum'].astype(stratum_cat)
cdc_corpus_df['collection'] = cdc_corpus_df['collection'].astype(collection_cat)
cdc_corpus_df = cdc_corpus_df.sort_values(['collection'] + sort_order)
# (33567, 23)

cdc_corpus_df.collection.value_counts(sort=False)
# {'mmwr_toc_en': 42, 'mmrr_toc_en': 34, 'mmss_toc_en': 36, 'mmsu_toc_en': 19, 
#  'mmwr_art_en': 12692, 'mmrr_art_en': 551, 'mmss_art_en': 467, 'mmsu_art_en': 234, 
#  'mmnd_art_en': 1195, 'mmwr_art_es': 22, 'eid_toc_en': 330, 'eid_art_en': 12769, 
#  'pcd_toc_en': 49, 'pcd_toc_es': 36, 'pcd_art_en': 3011, 'pcd_art_es': 1011, 
#  'pcd_art_fr': 357, 'pcd_art_zhs': 356, 'pcd_art_zht': 356}
pd.crosstab(cdc_corpus_df.collection, cdc_corpus_df.stratum, margins=True)

# use cdc_corpus_df[['url', 'mirror_path', 'stratum', 'collection']]
# to construct 19 collection segments from 6 html dicts

cdc_corpus_df.to_pickle('cdc_corpus_df.pkl')
# cdc_corpus_df = pd.read_pickle('pickle-files/cdc_corpus_df.pkl')
cdc_corpus_df.to_excel('cdc_corpus_df.xlsx', freeze_panes=(1,0))

cdc_corpus_df.to_csv('cdc_corpus_df.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)

json.dump(cdc_corpus_df.to_dict(orient='records'),
          open('cdc_corpus_df.json', 'x'))
