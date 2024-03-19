# CDC text corpora for learners

## Objectives

- Retrieve, organize, and share the contents and metadata from CDC's 3 online, public-domain series (_MMWR_, _EID_, _PCD_)
  - Make as few changes to raw sources as possible, to support as many downstream uses as possible
  - Organize metadata to enrich potential uses of text contents
  - Use methods and formats that ease sharing (CSV for metadata; JSON for HTML, markdown, and plain-text contents)
- Benefits to learners
  - Explore text-analytic methods using contents and metadata
  - Use git-reposited Python code to replicate and contribute
 
This version was constructed on 2024-03-01 using source content retrieved on 2024-01-09.

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

2. Convert to **Unicode (UTF-8) HTML**, cleaning up anomalies. [2_html.py](pycode/2_html.py)
   - Auxiliary script: Refine character classes to normalize **newlines and other spaces** and remove extras. [2_html_reduce-space.py](pycode/2_html_reduce-space.py)
   - Auxiliary script: Figure out how much space can be saved by **reducing `<svg>` elements**. [2_html_reduce-svg.py](pycode/2_html_reduce-svg.py)

3. Extract and organize **dateline information**, including information on series, volume, issue, article, page, and publication date, cleaning up anomalies. [3_dateline_mmwr.py](pycode/3_dateline_mmwr.py), [3_dateline_eid.py](pycode/3_dateline_eid.py), [3_dateline_pcd.py](pycode/3_dateline_pcd.py)
   - Auxiliary script: Scrutinize MMWR dateline information [3_dateline_mmwr_detail.py](pycode/3_dateline_mmwr_detail.py)
   - _MMWR_ JSON input: [mmwr_dateline_corrections.json](json-inputs/mmwr_dateline_corrections.json)
   - _EID_ JSON input: [eid_missing_pages.json](json-inputs/eid_missing_pages.json)
   - _PCD_ JSON inputs: [pcd_year_mo_to_vol_iss.json](json-inputs/pcd_year_mo_to_vol_iss.json), [pcd_vol_iss_dates.json](json-inputs/pcd_vol_iss_dates.json), [pcd_corrected_datelines.json](json-inputs/pcd_corrected_datelines.json), [pcd_article_numbers.json](json-inputs/pcd_article_numbers.json)

4. Extract and organize **other metadata** as available, including digital object identifier, title, keywords, description, and author(s). [4_metadata.py](pycode/4_metadata.py)
   - Auxiliary script: Investigate **elements in `<head>`**, especially `<meta>` elements with `name` or `property` attributes. [4_metadata_detail.py](pycode/4_metadata_detail.py)

5. Process **text contents** to bundle 3 versions: UTF-8 HTML, UTF-8 markdown, and ASCII plain-text contents. [5_contents.py](pycode/5_contents.py)

## Results

### Raw HTML
series | # files | original size | zipped size | file list | zip archive
--- | --: | --: | --: | --- | ---
_MMWR_ | 15,297 | 2,103 MiB | 413 MiB | [mmwr_1982-2023_zip.csv](html-mirrors/mmwr_1982-2023_zip.csv) | mmwr_1982-2023.zip\*
_EID_ | 13,100 | 4,622 MiB | 1,396 MiB | [eid_1995-2023_zip.csv](html-mirrors/eid_1995-2023_zip.csv) | eid_1995-2023.zip\*
_PCD_ | 5,179 | 559 MiB | 164 MiB | [pcd_2004-2023_zip.csv](html-mirrors/pcd_2004-2023_zip.csv) | pcd_2004-2023.zip\*
Total | 33,576 | 7,284 MiB | 1,972 MiB | | 

\* Zipped archives are larger than GitHub permits for this repository.

### Metadata fields

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

<details open>
<summary>Constructed metadata, with links (expand/collapse section)</summary>

The corpus metadata is available as an [uncompressed CSV file](csv-output/cdc_corpus_df.csv) and a [compressed zip archive](csv-output/cdc_corpus_df.zip). The metadata table includes the following fields, each of which was constructed as a string.

field | description
--- | ---
url | URL of retrieved document (primary key)
collection | series, level, and language code
series | CDC series (`mmwr`, `mmrr`, `mmss`, `mmsu`, `mmnd`, `eid`, `pcd`)
level | level in hierarchy (`home`, `series`, `volume`, `issue`, `article`)
lang | language (`en`, `es`, `fr`, `zhs`, `zht`)
dl_year_mo | year and month of volume (`YYYY` or `YYYY-MM`)
dl_vol_iss | volume and issue (`VV`, `VV(II)`, or `VV(IIII)`)
dl_date | date of publication (`YYYY-MM-DD`)
dl_page | first arabic page number of article (_MMWR_, _EID_) (`DDDD`)
dl_art_num | article number (_PCD_) (`ADDD` or `EDDD`)
dateline | dateline string from document or auxiliary
base | base URL from which document reference was harvested
string | text of `<a>` element referring to document
link_canon | canonical link from `<link>`
md_citation_doi | citation [DOI](https://www.doi.org/the-identifier/what-is-a-doi/) from `<meta>`
title | title from `<title>`
md_citation_categories | citation categories from `<meta>`
dl_cat | category from dateline
md_kwds | keywords from `<meta>`, pipe-delimited if \> 1
md_desc | description from `<meta>`
md_citation_author | citation author(s) from `<meta>`, pipe-delimited if \> 1

</details>

### Collections

Contents are organized in 21 mutually exclusive collections, based on series, scope, and language:
- Series
  - _MMWR Weekly Reports_ (`mmwr`)
  - _MMWR Recommendations and Reports_ (`mmrr`)
  - _MMWR Surveillance Summaries_ (`mmss`)
  - _MMWR Supplements_ (`mmsu`)
  - _MMWR_ Notifiable Diseases (`mmnd`), a subset of _Weekly Reports_, constructed ad hoc
  - _Emerging Infectious Diseases_ (`eid`)
  - _Preventing Chronic Disease_ (`pcd`)
- Scope
  - Table of contents for volumes and issues (`toc`)
  - Article, meaning any individual HTML document (`art`)
- Language
  - English (`en`)
  - Spanish (`es`) (_MMWR_ and _PCD_)
  - French (`fr`) (_PCD_ only)
  - Chinese (simplified: `zhs`), (traditional: `zht`) (_PCD_ only)

<details open>
<summary>Constructed collections, with links (expand/collapse section)</summary>

In the following table, each zip archive is linked by collection and output format (UTF-8 HTML, UTF-8 markdown, and ASCII plain text).

collection | description | n | html | md | txt
--- | --- | --: | --- | --- | ---
`mmwr_toc_en` | _MMWR Weekly Reports_ table of contents | 42 | [html](json-outputs/html/mmwr_toc_en_html_json.zip) | [md](json-outputs/md/mmwr_toc_en_md_json.zip) | [txt](json-outputs/txt/mmwr_toc_en_txt_json.zip)
`mmrr_toc_en` | _MMWR Recommendations and Reports_ table of contents | 34 | [html](json-outputs/html/mmrr_toc_en_html_json.zip) | [md](json-outputs/md/mmrr_toc_en_md_json.zip) | [txt](json-outputs/txt/mmrr_toc_en_txt_json.zip)
`mmss_toc_en` | _MMWR Surveillance Summaries_ table of contents | 36 | [html](json-outputs/html/mmss_toc_en_html_json.zip) | [md](json-outputs/md/mmss_toc_en_md_json.zip) | [txt](json-outputs/txt/mmss_toc_en_txt_json.zip)
`mmsu_toc_en` | _MMWR Supplements_ table of contents | 19 | [html](json-outputs/html/mmsu_toc_en_html_json.zip) | [md](json-outputs/md/mmsu_toc_en_md_json.zip) | [txt](json-outputs/txt/mmsu_toc_en_txt_json.zip)
`mmwr_art_en` | _MMWR Weekly Reports_ English-language articles | 12,692 | [html](json-outputs/html/mmwr_art_en_html_json.zip) | [md](json-outputs/md/mmwr_art_en_md_json.zip) | [txt](json-outputs/txt/mmwr_art_en_txt_json.zip)
`mmrr_art_en` | _MMWR Recommendations and Reports_ English-language articles | 551 | [html](json-outputs/html/mmrr_art_en_html_json.zip) | [md](json-outputs/md/mmrr_art_en_md_json.zip) | [txt](json-outputs/txt/mmrr_art_en_txt_json.zip)
`mmss_art_en` | _MMWR Surveillance Summaries_ English-language articles | 467 | [html](json-outputs/html/mmss_art_en_html_json.zip) | [md](json-outputs/md/mmss_art_en_md_json.zip) | [txt](json-outputs/txt/mmss_art_en_txt_json.zip)
`mmsu_art_en` | _MMWR Supplements_ English-language articles | 234 | [html](json-outputs/html/mmsu_art_en_html_json.zip) | [md](json-outputs/md/mmsu_art_en_md_json.zip) | [txt](json-outputs/txt/mmsu_art_en_txt_json.zip)
`mmnd_art_en` | _MMWR_ notifiable diseases\* | 1,195 | [html](json-outputs/html/mmnd_art_en_html_json.zip) | [md](json-outputs/md/mmnd_art_en_md_json.zip) | [txt](json-outputs/txt/mmnd_art_en_txt_json.zip)
`mmwr_art_es` | _MMWR_ Spanish-language articles (19 WR, 1 RR, 2 SU)\* | 22 | [html](json-outputs/html/mmwr_art_es_html_json.zip) | [md](json-outputs/md/mmwr_art_es_md_json.zip) | [txt](json-outputs/txt/mmwr_art_es_txt_json.zip)
`eid_toc_en` | _EID_ table of contents | 330 | [html](json-outputs/html/eid_toc_en_html_json.zip) | [md](json-outputs/md/eid_toc_en_md_json.zip) | [txt](json-outputs/txt/eid_toc_en_txt_json.zip)
`eid0_art_en` | _EID_ English-language articles, volumes 1-13\*\* | 3,919 | [html](json-outputs/html/eid0_art_en_html_json.zip) | [md](json-outputs/md/eid0_art_en_md_json.zip) | [txt](json-outputs/txt/eid0_art_en_txt_json.zip)
`eid1_art_en` | _EID_ English-language articles, volumes 14-21\*\* | 4,439 | [html](json-outputs/html/eid1_art_en_html_json.zip) | [md](json-outputs/md/eid1_art_en_md_json.zip) | [txt](json-outputs/txt/eid1_art_en_txt_json.zip)
`eid2_art_en` | _EID_ English-language articles, volumes 22-29\*\* | 4,411 | [html](json-outputs/html/eid2_art_en_html_json.zip) | [md](json-outputs/md/eid2_art_en_md_json.zip) | [txt](json-outputs/txt/eid2_art_en_txt_json.zip)
`pcd_toc_en` | _PCD_ English-language table of contents | 49 | [html](json-outputs/html/pcd_toc_en_html_json.zip) | [md](json-outputs/md/pcd_toc_en_md_json.zip) | [txt](json-outputs/txt/pcd_toc_en_txt_json.zip)
`pcd_toc_es` | _PCD_ Spanish-language table of contents | 36 | [html](json-outputs/html/pcd_toc_es_html_json.zip) | [md](json-outputs/md/pcd_toc_es_md_json.zip) | [txt](json-outputs/txt/pcd_toc_es_txt_json.zip)
`pcd_art_en` | _PCD_ English-language articles | 3,011 | [html](json-outputs/html/pcd_art_en_html_json.zip) | [md](json-outputs/md/pcd_art_en_md_json.zip) | [txt](json-outputs/txt/pcd_art_en_txt_json.zip)
`pcd_art_es` | _PCD_ Spanish-language articles | 1,011 | [html](json-outputs/html/pcd_art_es_html_json.zip) | [md](json-outputs/md/pcd_art_es_md_json.zip) | [txt](json-outputs/txt/pcd_art_es_txt_json.zip)
`pcd_art_fr` | _PCD_ French-language articles | 357 | [html](json-outputs/html/pcd_art_fr_html_json.zip) | [md](json-outputs/md/pcd_art_fr_md_json.zip) | [txt](json-outputs/txt/pcd_art_fr_txt_json.zip)
`pcd_art_zhs` | _PCD_ Chinese-language (simplified) articles | 356 | [html](json-outputs/html/pcd_art_zhs_html_json.zip) | [md](json-outputs/md/pcd_art_zhs_md_json.zip) | [txt](json-outputs/txt/pcd_art_zhs_txt_json.zip)
`pcd_art_zht` | _PCD_ Chinese-language (traditional) articles | 356 | [html](json-outputs/html/pcd_art_zht_html_json.zip) | [md](json-outputs/md/pcd_art_zht_md_json.zip) | [txt](json-outputs/txt/pcd_art_zht_txt_json.zip)
Total | | 33,567 | | |

\* Collections `mmnd_art_en` and `mmwr_art_es` were constructed ad hoc for end-user convenience.

\*\* All _EID_ articles are in English, though some have non-English elements.

## Python modules used
 
- File and session management
  - Base Python: [json](https://docs.python.org/3/library/json.html), [os](https://docs.python.org/3/library/os.html), [pickle](https://docs.python.org/3/library/pickle.html), [time](https://docs.python.org/3/library/time.html)
  - Contributed: [tqdm](https://tqdm.github.io/)
- Web/text
  - Base Python: [codecs](https://docs.python.org/3/library/codecs.html), [html](https://docs.python.org/3/library/html.html), [re](https://docs.python.org/3/library/re.html), [unicodedata](https://docs.python.org/3/library/unicodedata.html), [urllib](https://docs.python.org/3/library/urllib.html)
  - Contributed: [BeautifulSoup](https://www.crummy.com/software/BeautifulSoup/bs4/doc/), [markdownify](https://github.com/matthewwithanm/python-markdownify), [requests](https://requests.readthedocs.io/en/latest/)
- Data wrangling
  - Base Python: [collections](https://docs.python.org/3/library/collections.html), [datetime](https://docs.python.org/3/library/datetime.html)
  - Contributed: [dateutil](https://dateutil.readthedocs.io/en/stable/), [pandas](https://pandas.pydata.org/docs/user_guide/index.html)

## History and credit

Credit for earlier precursors to this effort:
- [Matt Maenner](https://github.com/mjmaenner) wrote R scripts to harvest the _MMWR_ around 2015 as a way to gather public-domain text in public health for exploring `word2vec` and related methods. Matt is the initial architect and inspiration behind the whole idea.
- [Scott Lee](https://github.com/scotthlee) advanced some of Matt's work, especially regarding use of machine learning models applied to _MMWR_ contents.
- [Sam Prausnitz-Weinbaum](https://github.com/sampdubs) ported earlier R functonality to Python in 2019 (after I had made an initial attempt in 2017). Sam also reimplemented `word2vec` and `doc2vec` models using the _MMWR_ corpus, and he added latent Dirichlet allocation (LDA) topic models. Sam and I presented his work to _MMWR_ science staff in July 2019 and to CDC's first Data Visualization Day in October 2019 (poster title: "Portals to the computable MMWR: a proof of concept for depicting (un)natural language in public health").
- Early in the Covid pandemic, I picked this Python project back up and extended it to include _EID_ and _PCD_ (and tried to include _Public Health Reports_, but that government-sponsored journal is not in the public domain). I chipped away at it every once in a while for a few years and developed the organizing principles listed at the top of this page. It isn't especially Pythonic, but it is meticulous. For now, it's a gift to learners who could use a manageable corpus on public health. I hope that users will contribute code back to this repository for the benefit of other learners. I might not be done with it yet, despite my earnest intentions.
