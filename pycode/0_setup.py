#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Mirror MMWR, EID, and PCD locally; extract and organize metadata and plain 
content.
@author: chadheilig

This suite of scripts sets up to harvest the text and hypertext from the online
archives of 3 public health publication series. The harvesting is limited to 
English and Spanish publications, and it omits nontext components, such as 
media and items in the Portable Document Format.

Each series follows a hierarchy that includes some of the following levels:
* base: a source that points to the journal's home page
* home: the journal's canonical home page, with and overview of the archive
      MMWR  https://www.cdc.gov/mmwr/about.html
      EID   https://wwwnc.cdc.gov/eid/
      PCD   https://www.cdc.gov/pcd/index.htm
* series:   the series that comprise components of the journal; 
            MMWR has 4: Weekly Report, Recommendations and Reports,
            Surveillance Summaries, and Supplements
* volumes:  yearly groupings of journal issues
      MMWR  volumes 31-72 (1982-2023)
      EID   volumes 1-29 (1995-2023)
      PCD   volumes 1-20 (2004-2023)
* issues:   groupings of articles typically published simultaneously
* articles: primary unit of publication
* floats:   separate files that correspond to figures and tables (EID only)

This suite of scripts uses the level desginations above in names of
corresponding data objects.

Most levels begin with HTML sources to generate the following:
_a      list of anchor Tag ResultSets from a soup object
_a_n    number of Tags in each ResultSet in _a
_cc_df pandas DataFrame
   base     URL from which href values were harvested
   href     hypertext reference (bs.a['href']); + base URL -> absolute URL
   url      absolute URL constructed from href and base URL
   path     path from absolute URL
   filename name of HTML file in hypertext reference
   mirror_path path on local mirror
   string   string from anchor element content
   level    in concatentated DataFrame, volume or article
_repeated list of indices for URLs that are duplicated
_html   full HTML text as retrieved and processed from each URL
_soup   soup objects that parse HTML text

The "analyze-structure" scripts follow a systematic approach to harvesting each
level of the journal's hierarchy, resulting in a series-specific DataFrame (a
tabular data object) with the URL and other information on each file to be
mirrored in a local archive for further processing and analysis.
"""

#%% Import modules and set up environment
# operating system muodules to work with filenames and paths
import os
from os.path import join, expanduser, normpath
# built-in urllib module to work with URLs
from urllib.parse import urlparse, urljoin, urlunparse
# requests module to retrieve HTML files over the internet
import requests
# built-in regular expression module to work with patterns of text
import re
# BeautifulSoup module to parse, analyze, and write HTML
# UnicodeDammit module to handle character encoding gracefully
from bs4 import BeautifulSoup, UnicodeDammit
# pandas module to organize metadata in DataFrame structure
import pandas as pd
# built-in pickle module to serialize and store intermediate data structures
import pickle
# tqdm module to visualize progress oover iterators
from tqdm import tqdm

# global, session-specific pandas option
pd.set_option('display.expand_frame_repr', False) # show/wrap all DF columns

#%% 1. Functions for operating on URLs and paths

# requests.get(url).text decodes to utf-8, which could mismatch
# requests.get(url).content is bytestream
# UnicodeDammit(content, ["utf-8", "windows-1252"]) tries to improve match
#    tries utf-8 first, but falls back to windows-1252 if there's an error
#    .unicode_markup appears to be the same as .markup
# reduce all non-HTML whitespace to single space character
def get_html_from_url(url, print_url = False, timeout = 1):
   """
   Parameters
   url : str
      Absolute URL from which to retrieve HTML document.

   1. Absolute URL passes to requests.get
   2. bytes get() result passes to UnicodeDammit
   3. UnicodeDammit attempts decoding to UTF-8 else to Windows-1252
   4. unicode_markup is (one hopes) clean UTF-8-encoded HTML
   5. all whitespace sequences (including \r, \n) reduce to single space (' ')

   Returns
   str
      UTF-8 encoded HTML string with minimal white space.
   
   requests.get(timeout=5) is hardcoded to deal with sluggish EID responses
   """
   if(print_url):
      print(f'Retrieving URL {url}')
   try:
      html = re.sub(r'\s+', ' ', 
         UnicodeDammit(
            requests.get(url, timeout=timeout).content, 
            ["utf-8", "windows-1252"]).unicode_markup)
   except:
      html = ''
   return html

# curried version that prints URL
def get_html_from_url_(url):
   return get_html_from_url(url, print_url = True, timeout = 5)

def process_aTag(aTag, base_url='https://www.cdc.gov'):
   """
   aTag: bs4.element.Tag anchor element from soup
   base_url: str base URL containing (or that could contain) anchor element
   Construct absolute URL from anchor's hypertext reference and base URL
   returns dict: { base_url, href, url, path, file_in, file_out, string }
   """
   try:
      a_href = aTag['href']
   except:
      print('Unable to extract href from anchor.')
      return None
   joined = urljoin(base_url, a_href)
   parsed = urlparse(joined)
   parsed = parsed._replace(scheme='https', params='', query='', fragment='')
   a_url = parsed.geturl()
   a_path = parsed.path
   a_dirname = os.path.dirname(a_path)
   a_basename = os.path.basename(a_path)
   a_baseext = os.path.splitext(a_basename)
   # construct filename for local mirror if internet filename isn't *.htm*
   if a_basename == '':
      m_filename = 'index.html'
#   elif re.search(r'\.html?', a_fname) is None:
   elif a_baseext[1] == '':
      m_filename = a_baseext[0] + '.html'
   else:
      m_filename = a_basename
   m_path = os.path.join(a_dirname, m_filename)
   a_text = aTag.get_text('|', strip = True)
   return dict(base=base_url, href=a_href, url=a_url, path=a_path, 
      filename=a_basename, mirror_path=m_path, string=a_text)

#%% 2. Functions for minimally processing HTML files as bytes or strings

def calculate_mirror_dirs(paths):
   "Given set of paths (with or without filenames), determine unique folders"
   dirnames = [os.path.dirname(path) for path in paths]
   dirnames = sorted(set(dirnames))
   return dirnames

def create_mirror_tree(base_path, dirnames):
   """
   Nondestructively constructs directory tree for journal archive mirror based
   on paths in URLs to be retrieved.
   
   Parameters
   base_path : str
      base of path in which to construct journal mirror directory tree.
   dirnames : str
      list of paths to graft onto base_path and populate with HTML files

   Returns
   None
   """
   from os import makedirs
   from os.path import join, expanduser, normpath
   
   # process paths to separate head from tail
   # get unique set of path heads
   base_path = normpath(expanduser(base_path))
   norm_paths = [normpath(join(base_path, dir[1:] if dir[0]=='/' else dir)) 
                 for dir in dirnames]
 
   # clean up base_path, then try to construct tree
   try:
      makedirs(base_path, exist_ok=True) # the rest will work iff this does
      for norm_path in norm_paths:
          makedirs(norm_path, exist_ok=True)
      print(f"Successfully made directories from {base_path}")
      return dict(base_path = [base_path], paths = dirnames, 
                  norm_paths = norm_paths)
   except: # could try to trap each possible error type
      print(f"Unable to make directories from {base_path}")
      return None

# retrieve unprocessed HTML and immediately write it to local mirror
def mirror_raw_html(url, mirror_path, timeout = 1, print_url = True):
   """Use requests.get to retrieve raw (bytes) version of HTML file
   and write unprocessed HTML to local mirror."""
   if(print_url):
      # print('.', end = '')
      print(f'Processing URL {url}', end = '')
   try:
      b0 = requests.get(url, timeout = timeout).content
   except:
      b0 = b''
   with open(mirror_path, 'bw') as file_out:
       file_out.write(b0)
   if(print_url):
       print('.')
   return len(b0)

# Decode from raw HTML using the following epmirically derived algorithm
# based on the <meta> element's charset attribute
# In this order, search and apply 'utf-8', 'iso-8859-1', 'windows'1252'
#     If none is present, use 'windows-1252'
# See https://docs.python.org/3/library/codecs.html#standard-encodings
# meta_win_reb = re.compile(rb'<meta.*charset="?windows-1252.*>', flags=re.M|re.I)
def html_to_unicode_b(str_b, try_=False):
   # compiled regular expressions are cached, making repeated calls efficient
   meta_utf8_reb = re.compile(rb'<meta.*charset="?utf-8.*>', flags=re.M|re.I)
   meta_lat1_reb = re.compile(rb'<meta.*charset="?iso-8859-1.*>', flags=re.M|re.I)
   # simplified (gb2312) and traditional (big5) Chinese
   meta_zhs_reb = re.compile(rb'<meta.*charset="?gb2312.*>', flags=re.M|re.I)
   meta_zht_reb = re.compile(rb'<meta.*charset="?big5.*>', flags=re.M|re.I)

   # converts raw HTML (bytes) to Unicode HTML (str)
   # str_u = UnicodeDammit(str_b, ['utf-8', 'windows-1252']).unicode_markup
   if meta_utf8_reb.search(str_b):   # 'utf-8'
      codec = 'utf_8'
   elif meta_lat1_reb.search(str_b): # 'iso-8859-1'
      codec = 'latin_1'
   elif meta_zhs_reb.search(str_b):  # 'gb2312'
      codec = 'gb2312'
   elif meta_zht_reb.search(str_b):  # 'big5'
      codec = 'big5'
   else:                             # 'windows-1252
      codec = 'cp1252'
   if try_:
      try:
         str_u = str_b.decode(encoding=codec, errors='strict')
      except UnicodeDecodeError as e:
         print(f'UnicodeDecodeError {e} [{e.start}:{e.end}]\n')
   str_u = str_b.decode(encoding=codec, errors='backslashreplace')
   return str_u

# Small utilities to separate sequence of operations on HTML
# x_b take bytes; x_u take strings (UTF-8)
# sub(' ?(<|>) ?', r'\1') works in well-formed HTML

# example: minim=r'<pre[ >]'
def html_reduce_space_u(str_u, minim=None):
    # compiled regular expressions are cached, making repeated calls efficient
   # universal newlines, except for '\n'
   newln_re = re.compile('[\n\x0b\x0c\r\x1c\x1d\x1e\x85\u2028\u2029]', flags=re.M)
   # everything that matches Unicode '\s' except ' ' and universal newlines
   space_re = re.compile('[\t\x1f\xa0\u1680\u2000\u2001\u2002\u2003\u2004\u2005'
      '\u2006\u2007\u2008\u2009\u200a\u202f\u205f\u3000]', flags=re.M)
   min_sp_re = re.compile('^ +(?=<)|( *$)', flags=re.M) # remove space before < or $
   mult_sp_re = re.compile(' {2,}')#, flags=re.M) # replace 2+ space with 1
   end_sp_re = re.compile('(^ *)|( *$)', flags=re.M) # remove space on either end
   mult_nl_re = re.compile('\n{3,}', flags=re.M) # replace 3+ newline with 2

   # str_u = re.sub(r'\s+', ' ', str_u) # \s includes newlines and other spaces
   str_u = space_re.sub(' ', str_u)      # replace alt-spaces with ' '
   str_u = str_u.replace('\r\n', '\n')   # replace '\r\n' with '\n'
   str_u = newln_re.sub('\n', str_u)     # replace alt-newlines with '\n'
   # remove some whitespace when <pre> is present, more when it's not
   if minim and re.search(minim, str_u, flags=re.I):
      str_u = min_sp_re.sub('', str_u)   # remove space before < or line-end
   else:
      str_u = mult_sp_re.sub(' ', str_u) # 2+ spaces -> ' '
      str_u = end_sp_re.sub('', str_u)   # empty line
   str_u = mult_nl_re.sub('\n\n', str_u) # 3+ newlines -> \n\n
   return str_u

def read_raw_html(path):
   "Read raw (bytes) local copy of HTML file."
   with open(path, 'rb') as file_in:
      raw_html = file_in.read()
   return raw_html

def read_uni_html(path):
   "Read Unicode (UTF-8 string) local copy of HTML file."
   with open(path, 'r') as file_in:
      uni_html = file_in.read()
   return uni_html
