# Mirrored CDC journals

This collection of files includes mirrored copies of HTML articles from CDC's 3 online journals, along with the Python code that retrieved and constructed the mirrors and Excel workbooks with metadata about those mirrors.

-   MMWR <https://www.cdc.gov/mmwr/about.html>

-   EID <https://wwwnc.cdc.gov/eid/>

-   PCD <https://www.cdc.gov/pcd/index.htm>

These mirrors were constructed in stages, so the code is somewhat disjoint. As shown in the code, the 3 mirrors were constructed in similar (but not identical) ways:

0.  Set up the Python environment. The main libraries are urllib (manage URLs), requests (manage HTTP communications), re (use regular expressions), bs4 (BeautifulSoup to parse HTML and handle some Unicode idiosyncrasies), and pandas (manage metadata in DataFrames).

1.  Perform a minimal set of queries to each journal website, sufficient to construct a complete hierarchy and list of HTML files to retrieve. These hierarchies include lists of series components, volumes within series, issues within volumes, and articles within issues.

2.  Mirror the set of paths from each website to a local copy. Retrieve the raw HTML as binary streams, with no modification (labeled here as "b0"). Convert raw HTML to minimally "pretty-fied" (refined use of all white space) UTF-8 text files (labeled here as "u3").

3.  Additional code explores the internal structure of these HTML files with a view toward extracting issue-specific metadata and article contents.

This archive contains the zipped mirrors of raw HTML ("b0") and minimally pretty-fied HTML ("u3") in folder zipped/, metadata in folder dframes-xlsx/, and Python code in folder pycode/.

This version was constructed on 2024-03-01.

collection | description | n | html | md | txt
--- | --- | --: | --- | --- | ---
mmwr_toc_en | _MMWR Weekly Reports_ table of contents | 42 | [html](json-files/html/mmwr_toc_en_html.zip) | [md](json-files/md/mmwr_toc_en_md.zip) | [txt](json-files/txt/mmwr_toc_en_txt.zip)
mmwr_art_en | _MMWR Weekly Reports_ English-language articles | 12,692 | [html](json-files/html/mmwr_art_en_html.zip) | [md](json-files/md/mmwr_art_en_md.zip) | [txt](json-files/txt/mmwr_art_en_txt.zip)
mmrr_toc_en | _MMWR Recommendations and Reports_ table of contents | 34 | [html](json-files/html/mmrr_toc_en_html.zip) | [md](json-files/md/mmrr_toc_en_md.zip) | [txt](json-files/txt/mmrr_toc_en_txt.zip)
mmrr_art_en | _MMWR Recommendations and Reports_ English-language articles | 551 | [html](json-files/html/mmrr_art_en_html.zip) | [md](json-files/md/mmrr_art_en_md.zip) | [txt](json-files/txt/mmrr_art_en_txt.zip)
mmss_toc_en | _MMWR Surveillance Summaries_ table of contents | 36 | [html](json-files/html/mmss_toc_en_html.zip) | [md](json-files/md/mmss_toc_en_md.zip) | [txt](json-files/txt/mmss_toc_en_txt.zip)
mmss_art_en | _MMWR Surveillance Summaries_ English-language articles | 467 | [html](json-files/html/mmss_art_en_html.zip) | [md](json-files/md/mmss_art_en_md.zip) | [txt](json-files/txt/mmss_art_en_txt.zip)
mmsu_toc_en | _MMWR Supplements_ table of contents | 19 | [html](json-files/html/mmsu_toc_en_html.zip) | [md](json-files/md/mmsu_toc_en_md.zip) | [txt](json-files/txt/mmsu_toc_en_txt.zip)
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
