#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Analyze the structure and broad properties of MMWR, PCD, EID, and PHR online
@author: chadheilig

This suite of scripts sets up to harvest the text and hypertext from the online
archives of 4 public health. The harvesting is limited to English and Spanish
publications, and it omits nontext components, such as media and items in the
Portable Document Format (though tools exist for harvesting text from those).

Each journal follows a hierarchy that includes some of the following levels:
* base: a source that points to the journal's home page
* home: the journal's canonical home page, with and overview of the archive
      MMWR  https://www.cdc.gov/mmwr/about.html
      PCD   https://www.cdc.gov/pcd/index.htm
      EID   https://wwwnc.cdc.gov/eid/
      PHR   https:////www.ncbi.nlm.nih.gov/pmc/journals/333/
* series:   the series that comprise components of the journal; MMWR has 4
* volumes:  yearly groupings of journal issues
      MMWR  volumes 31-69 (1982-2020)
      PCD   volumes 1-17 (2004-2020)
      EID   volumes 1-26 (1995-2020)
      PHR   volumes 116-133 (2001-2018)
* issues:   groupings of articles typically published simultaneously
* articles: primary unit of publication
* floats:   separate files that correspond to figures and tables (EID only)

This suite of scripts uses the level desginations above in names of
corresponding data objects.

Most levels begin with HTML sources to generate the following:
_a      list of anchor Tag ResultSets from a soup object
_a_n    number of Tags in each ResultSet in _a
_dframe pandas DataFrame
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
level of the journal's hierarchy, resulting in a journal-specific DataFrame (a
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
# UnicodeDammit module to handle character encoding gracefully
# BeautifulSoup module to parse, analyze, and write HTML
from bs4 import BeautifulSoup, UnicodeDammit
from bs4.formatter import HTMLFormatter
# pandas module to organize metadata in DataFrame structure
import pandas as pd
# built-in pickle module to serialize and store intermediate data structures
import pickle
# multiprocessing and tqdm modules for utilities to evaluate progress
import multiprocessing
from tqdm import tqdm, trange

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

# UnsortedAttributes formatter adapted from BeautifulSoup documentation
# from bs4.formatter import HTMLFormatter
# renders formatted HTML as close to input as feasible, with nice indents
class UnsortedAttributes(HTMLFormatter):
   def attributes(self, tag):
      for k, v in tag.attrs.items():
         yield k, v

# using UnsortedAttributes in this way can yield invalid HTML
# because of < or > in open text not being rendered as &lt; or &gt;
# not sure how to prevent sorting and ensure valid code 
# (short of hacking BeautifulSoup source)

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

# Small utilities to separate sequence of operations on HTML
# x_b take bytes; x_u take strings (UTF-8)
# sub(' ?(<|>) ?', r'\1') works in well-formed HTML
def html_reduce_space_b(str_b):
   str_b = re.sub(br'\s+', b' ', str_b)
   # str_b = re.sub(b' ?(<|>) ?', br'\1', str_b)
   return str_b

def html_insert_newlines_b(str_b):
   # assumes result of html_reduce_space_b(); judiciously insert newlines
   str_b = re.sub(b'>(.)', br'>\n\1', str_b)
   str_b = re.sub(br'([^\n])<', br'\1\n<', str_b)
   return str_b

def html_to_unicode_b(str_b):
   # converts raw HTML (bytes) to Unicode HTML (str)
   str_u = UnicodeDammit(str_b, ['utf-8', 'windows-1252']).unicode_markup
   return str_u

def html_reduce_space_u(str_u):
   str_u = re.sub(r'\s+', ' ', str_u)
   # str_u = re.sub(' ?(<|>) ?', r'\1', str_u)
   return str_u

def html_insert_newlines_u(str_u):
   # assumes result of html_reduce_space_u(); judiciously insert newlines
   str_u = re.sub('>(.)', r'>\n\1', str_u)
   str_u = re.sub(r'([^\n])<', r'\1\n<', str_u)
   return str_u

def html_prettify_u(str_u):
   str_u = BeautifulSoup(str_u, 'lxml')\
      .prettify(formatter = 'minimal')
      # .prettify(formatter = UnsortedAttributes())
   return str_u

def trim_leading_space_u(str_u):
   # assumes result of basic_prettify()
   return re.sub(r'^\s+', '', str_u, flags=re.M)

# Functions to work with local mirror
def mirror_raw_to_uni(b_path, u_path, counter = None):
   "Mirror local, unprocessed HTML to local, lightly processed HTML."
   if counter is not None:
      print(f'{counter:05d}', end = '')
   b0 = read_raw_html(b_path)
   u0 = html_to_unicode_b(b0)      # convert bytes to UTF-8
   u00 = html_reduce_space_u(u0)   # scrub u0 of excess space (esp. \r)
   u02 = html_prettify_u(u00)      # prettify u00
   u03 = trim_leading_space_u(u02) # scrub u02 of leading spaces
   with open(u_path, 'w') as file_out:
       file_out.write(u03)
   if counter is not None:
      print('.', end = ' ')
   return None

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
