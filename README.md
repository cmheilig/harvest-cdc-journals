# CDC text corpora for learners

## Objectives

- Retrieve, organize, and share the contents and metadata from CDC's 3 online, public-domain series (_MMWR_, _EID_, _PCD_)
  - Make as few changes to raw sources as possible, to support as many downstream uses as possible
  - Organize metadata to enrich potential uses of text contents
  - Use methods and formats that ease sharing (CSV for metadata, JSON for HTML, markdown, and plain-text contents)
- Benefits to learners
  - Explore text-analytic methods using contents and metadata
  - Use git-reposited Python code to replicate and contribute
 
This version was constructed on 2024-03-01.

## CDC's journals: public domain, public health

This collection of files includes mirrored copies of HTML articles from CDC's 3 online journals
- [_Morbidity and Mortality Weekly Report_](https://www.cdc.gov/mmwr/) (_MMWR_)
  - 4 series available in HTML 1982-2023 (volumes 31-72)
- [_Emerging Infectious Diseases_](https://wwwnc.cdc.gov/eid) (_EID_)
  - Available in HTML 1995-2023 (volumes 1-29)
- [_Preventing Chronic Disease_](https://www.cdc.gov/pcd/) (_PCD_)
  - Available in HTML 2004-2023 (volumes 1-20)

Result: 33,567 HTML documents, spanning 42 years (See [the Results section](#results) below for details.)

## Work sequence

These mirrors were constructed in stages. The 3 mirrors were constructed in similar (but not identical) ways. For dateline information, the code and repository include ad hoc adjustments to fill in missing information, correct inaccurate information, and organize auxiliary information not readily available by processing source HTML files.

0. **Set up** the Python environment. [0_setup.py](pycode/0_setup.py)

1. **Mirror raw HTML**. Perform a minimal set of queries to each journal website, sufficient to construct a complete hierarchy and list of HTML files to retrieve: lists of series components, volumes within series, issues within volumes, and articles within issues. Retrieve the raw HTML as binary streams, with no modification, to a mirrored structure on local disk. [1_mirror_mmwr.py](pycode/1_mirror_mmwr.py), [1_mirror_eid.py](pycode/1_mirror_eid.py), [1_mirror_pcd.py](pycode/1_mirror_pcd.py)

2. Convert to **Unicode HTML**, cleaning up anomalies. [2_html.py](pycode/2_html.py)

3. Extract and organize **dateline information**, including information on series, volume, issue, article, page, and publication date, cleaning up anomalies. [3_dateline_mmwr.py](pycode/3_dateline_mmwr.py), [3_dateline_eid.py](pycode/3_dateline_eid.py), [3_dateline_pcd.py](pycode/3_dateline_pcd.py)
   - _MMWR_ [mmwr_dateline_corrections](json-files/aux/mmwr_dateline_corrections.json)
   - _EID_ [eid_missing_pages](json-files/aux/eid_missing_pages.json)
   - _PCD_ [pcd_article_numbers](json-files/aux/pcd_article_numbers.json), [pcd_corrected_datelines](json-files/aux/pcd_corrected_datelines.json), [pcd_vol_iss_dates](json-files/aux/pcd_vol_iss_dates.json), [pcd_year_mo_to_vol_iss](json-files/aux/pcd_year_mo_to_vol_iss.json)

4. Extract and organize **other metadata** as available, including digital obeject identifier, title, keywords, description, and author(s). [4_metadata.py](pycode/4_metadata.py)

5. Process **text contents** to bundle 3 versions: UTF-8 HTML, UTF-8 markdown, and ASCII plain-text contents. [5_contents.py](pycode/5_contents.py)

## Results

Contents are organized in 19 collections, based on series, scope, and language
- Series
  - _MMWR Weekly Reports_ (mmwr)
  - _MMWR Recommendations and Reports_ (mmrr)
  - _MMWR Surveillance Summaries_ (mmss)
  - _MMWR Supplements_ (mmsu)
  - _MMWR_ Notifiable Diseases (mmnd), a subset of _Weekly Reports_, constructed ad hoc
  - _Emerging Infectious Diseases_ (eid)
  - _Preventing Chronic Disease_ (pcd)
- Scope
  - Table of contents for volumes and issues (toc)
  - Article, meaning any individual HTML document (art)
- Language
  - English (en)
  - Spanish (es) (_MMWR_ and _PCD_)
  - French (fr) (_PCD_ only)
  - Chinese (simplified: zhs), (traditional: zht) (_PCD_ only)

<details>
<summary>Table of 19 collections and links to zip archives</summary>

In this table, each zip archive is linked by collection and output format (UTF-8 HTML, UTF-8 markdown, and ASCII plain text).

collection | description | n | html | md | txt
--- | --- | --: | --- | --- | ---
mmwr_toc_en | _MMWR Weekly Reports_ table of contents | 42 | [html](json-files/html/mmwr_toc_en_html.zip) | [md](json-files/md/mmwr_toc_en_md.zip) | [txt](json-files/txt/mmwr_toc_en_txt.zip)
mmrr_toc_en | _MMWR Recommendations and Reports_ table of contents | 34 | [html](json-files/html/mmrr_toc_en_html.zip) | [md](json-files/md/mmrr_toc_en_md.zip) | [txt](json-files/txt/mmrr_toc_en_txt.zip)
mmss_toc_en | _MMWR Surveillance Summaries_ table of contents | 36 | [html](json-files/html/mmss_toc_en_html.zip) | [md](json-files/md/mmss_toc_en_md.zip) | [txt](json-files/txt/mmss_toc_en_txt.zip)
mmsu_toc_en | _MMWR Supplements_ table of contents | 19 | [html](json-files/html/mmsu_toc_en_html.zip) | [md](json-files/md/mmsu_toc_en_md.zip) | [txt](json-files/txt/mmsu_toc_en_txt.zip)
mmwr_art_en | _MMWR Weekly Reports_ English-language articles | 12,692 | [html](json-files/html/mmwr_art_en_html.zip) | [md](json-files/md/mmwr_art_en_md.zip) | [txt](json-files/txt/mmwr_art_en_txt.zip)
mmrr_art_en | _MMWR Recommendations and Reports_ English-language articles | 551 | [html](json-files/html/mmrr_art_en_html.zip) | [md](json-files/md/mmrr_art_en_md.zip) | [txt](json-files/txt/mmrr_art_en_txt.zip)
mmss_art_en | _MMWR Surveillance Summaries_ English-language articles | 467 | [html](json-files/html/mmss_art_en_html.zip) | [md](json-files/md/mmss_art_en_md.zip) | [txt](json-files/txt/mmss_art_en_txt.zip)
mmsu_art_en | _MMWR Supplements_ English-language articles | 234 | [html](json-files/html/mmsu_art_en_html.zip) | [md](json-files/md/mmsu_art_en_md.zip) | [txt](json-files/txt/mmsu_art_en_txt.zip)
mmnd_art_en | _MMWR_ notifiable diseases\* | 1,195 | [html](json-files/html/mmnd_art_en_html.zip) | [md](json-files/md/mmnd_art_en_md.zip) | [txt](json-files/txt/mmnd_art_en_txt.zip)
mmwr_art_es | _MMWR_ Spanish-language articles (19 WR, 1 RR, 2 SU)\* | 22 | [html](json-files/html/mmwr_art_es_html.zip) | [md](json-files/md/mmwr_art_es_md.zip) | [txt](json-files/txt/mmwr_art_es_txt.zip)
eid_toc_en | _EID_ table of contents | 330 | [html](json-files/html/eid_toc_en_html.zip) | [md](json-files/md/eid_toc_en_md.zip) | [txt](json-files/txt/eid_toc_en_txt.zip)
eid_art_en | _EID_ English-language articles\*\* | 12,769 | html<super>†</super> | md<super>†</super> | [txt](json-files/txt/eid_art_en_txt.zip)
pcd_toc_en | _PCD_ English-language table of contents | 49 | [html](json-files/html/pcd_toc_en_html.zip) | [md](json-files/md/pcd_toc_en_md.zip) | [txt](json-files/txt/pcd_toc_en_txt.zip)
pcd_toc_es | _PCD_ Spanish-language table of contents | 36 | [html](json-files/html/pcd_toc_es_html.zip) | [md](json-files/md/pcd_toc_es_md.zip) | [txt](json-files/txt/pcd_toc_es_txt.zip)
pcd_art_en | _PCD_ English-language articles | 3,011 | [html](json-files/html/pcd_art_en_html.zip) | [md](json-files/md/pcd_art_en_md.zip) | [txt](json-files/txt/pcd_art_en_txt.zip)
pcd_art_es | _PCD_ Spanish-language articles | 1,011 | [html](json-files/html/pcd_art_es_html.zip) | [md](json-files/md/pcd_art_es_md.zip) | [txt](json-files/txt/pcd_art_es_txt.zip)
pcd_art_fr | _PCD_ French-language articles | 357 | [html](json-files/html/pcd_art_fr_html.zip) | [md](json-files/md/pcd_art_fr_md.zip) | [txt](json-files/txt/pcd_art_fr_txt.zip)
pcd_art_zhs | _PCD_ Chinese-language (simplified) articles | 356 | [html](json-files/html/pcd_art_zhs_html.zip) | [md](json-files/md/pcd_art_zhs_md.zip) | [txt](json-files/txt/pcd_art_zhs_txt.zip)
pcd_art_zht | _PCD_ Chinese-language (traditional) articles | 356 | [html](json-files/html/pcd_art_zht_html.zip) | [md](json-files/md/pcd_art_zht_md.zip) | [txt](json-files/txt/pcd_art_zht_txt.zip)
Total | | 33,567 | | |

\* Collections mmnd_art_en and mmwr_art_es are ad hoc "series" constructed to collect similar documents for end-user convenience.

<super>†</super> _EID_ HTML and markdown files are larger than GitHub permits for this repository.

\*\* All _EID_ articles are in English, though some have non-English elements.
</details>

## Metadata elements

- Web information
  - Document URL, anchor text of document URL `<a>`, referring URL
  - Canonical link `<link>`, DOI `<meta>`
- Collection, series, level, language
- Dateline
  - Dateline string
  - Volume/issue, year/month of volume/issue, publication date
  - First arabic page number (_MMWR_, _EID_), article number (_PCD_)
- Additional metadata `<meta>`
  - Category, keywords, description, author(s)

## Python modules used
 
- File and session management
  - Base Python: [json](https://docs.python.org/3/library/json.html), [os](https://docs.python.org/3/library/os.html), [pickle](https://docs.python.org/3/library/pickle.html), [time](https://docs.python.org/3/library/time.html), [urllib](https://docs.python.org/3/library/urllib.html)
  - Contributed: [tqdm](https://tqdm.github.io/)
- Web/text
  - Base Python: [codecs](https://docs.python.org/3/library/codecs.html), [html](https://docs.python.org/3/library/html.html), [re](https://docs.python.org/3/library/re.html), [unicodedata](https://docs.python.org/3/library/unicodedata.html)
  - Contributed: [BeautifulSoup](https://www.crummy.com/software/BeautifulSoup/bs4/doc/), [markdownify](https://github.com/matthewwithanm/python-markdownify), [requests](https://requests.readthedocs.io/en/latest/)
- Data wrangling
  - Base Python: [collections](https://docs.python.org/3/library/collections.html), [datetime](https://docs.python.org/3/library/datetime.html)
  - Contributed: [dateutil](https://dateutil.readthedocs.io/en/stable/), [pandas](https://pandas.pydata.org/docs/user_guide/index.html)

