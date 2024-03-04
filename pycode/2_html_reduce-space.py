#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: cmheilig

Develop more sensitive approaches to reducing space in HTML documents

1. Empirically assess characters matched by regular expression '\\s'
   or classified as "universal newlines"

   re module notes meanings of \s in ASCII and Unicode
   https://docs.python.org/3/library/re.html#index-32
   Matches '[ \t\n\r\f\v]' if the ASCII flag is used.
   Matches Unicode whitespace characters 

   string method str.splitlines
   https://docs.python.org/3/library/stdtypes.html#str.splitlines
   '[\n\r\v\f\x1c\x1d\x1e\x85\u2028\u2029]|\r\n',
   where \v == \x0b and \f == \x0c
   
2. Construct function to reduce newlines and other spaces

"""

#%% 0. Set up environment
# from collections import Counter
# import html
# import unicodedata
os.chdir('/Users/cmheilig/cdc-corpora/_test')

#%% 1. Empirically assess characters matched by regular expression '\\s'

# Use set comprehensions

# Loop over code points to find matches: 128 for ASCII, 65536 for Unicode

# Match \s in ASCII
ascii_s   = {codept for codept in range(0x007f) 
             if re.match(r'\s', chr(codept), flags=re.A)} # len == 6
# {9, 10, 11, 12, 13, 32}

# Match \s in Unicode
unicode_s = {codept for codept in range(0xffff) 
             if re.match(r'\s', chr(codept), flags=re.U)} # len == 29
# {9, 10, 11, 12, 13, 28, 29, 30, 31, 32, 133, 160, 5760, 8192, 8193, 8194, 8195, 
#  8196, 8197, 8198, 8199, 8200, 8201, 8202, 8232, 8233, 8239, 8287, 12288}

# Set of single-character, universal newlines + '\r\n
newline_set = {ord(char) for char in '\n\r\v\f\x1c\x1d\x1e\x85\u2028\u2029'}
# {10, 11, 12, 13, 28, 29, 30, 133, 8232, 8233}

# Unicode space characters that are not newlines
space_set = unicode_s - newline_set
# {9, 31, 32, 160, 5760, 8192, 8193, 8194, 8195, 8196, 8197, 8198, 8199, 8200, 
#  8201, 8202, 8239, 8287, 12288}

# Break \s whitespace class into newlines and spaces; remove '\n' and ' '
newline_str = ('[' + 
    ''.join([chr(codept) for codept in sorted(newline_set - {10})]) + ']')
# '[\x0b\x0c\r\x1c\x1d\x1e\x85\u2028\u2029]' # no \n chr(10)
space_str   = ('[' + 
    ''.join([chr(codept) for codept in sorted(space_set - {32})]) + ']')
# '[\t\x1f\xa0\u1680\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008'
# '\u2009\u200a\u202f\u205f\u3000]' # no space chr(32)

#%% 2. Construct function to reduce newlines and other spaces

# example: minim=r'<pre[ >]'
def html_reduce_space_u(str_u, minim=None):
    # compiled regular expressions are cached, making repeated calls efficient
    # universal newlines, except for '\n'
    newln_re = re.compile(
        '[\n\x0b\x0c\r\x1c\x1d\x1e\x85\u2028\u2029]', flags=re.M)
    # everything that matches Unicode '\s' except ' ' and universal newlines
    space_re = re.compile(
        '[\t\x1f\xa0\u1680\u2000\u2001\u2002\u2003\u2004\u2005'
        '\u2006\u2007\u2008\u2009\u200a\u202f\u205f\u3000]', flags=re.M)
    min_sp_re  = re.compile('^ +(?=<)|( *$)', flags=re.M) # space before < or $
    mult_sp_re = re.compile(' {2,}')#, flags=re.M)        # 2+ space with 1
    end_sp_re  = re.compile('(^ *)|( *$)', flags=re.M)    # space on either end
    mult_nl_re = re.compile('\n{3,}', flags=re.M)         # 3+ newline with 2

    # str_u = re.sub(r'\s+', ' ', str_u)   # \s includes newlines, other spaces
    str_u = space_re.sub(' ', str_u)       # replace alt-spaces with ' '
    str_u = str_u.replace('\r\n', '\n')    # replace '\r\n' with '\n'
    str_u = newln_re.sub('\n', str_u)      # replace alt-newlines with '\n'
    # remove some whitespace when <pre> is present, more when it's not
    if minim and re.search(minim, str_u, flags=re.I):
        str_u = min_sp_re.sub('', str_u)   # remove space before < or line-end
    else:
        str_u = mult_sp_re.sub(' ', str_u) # 2+ spaces -> ' '
        str_u = end_sp_re.sub('', str_u)   # empty line
    str_u = mult_nl_re.sub('\n\n', str_u)  # 3+ newlines -> \n\n
    return str_u

