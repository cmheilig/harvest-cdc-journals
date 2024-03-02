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

collection | description | n
--- | --- | --:
mmwr_toc_en | _MMWR Weekly Reports_ table of contents | 42
mmwr_art_en | _MMWR Weekly Reports_ English-language articles | 12,692
mmrr_toc_en | _MMWR Recommendations and Reports_ table of contents | 34
mmrr_art_en | _MMWR Recommendations and Reports_ English-language articles | 551
mmss_toc_en | _MMWR Surveillance Summaries_ table of contents | 36
mmss_art_en | _MMWR Surveillance Summaries_ English-language articles | 467
mmsu_toc_en | _MMWR Supplements_ table of contents | 19
mmsu_art_en | _MMWR Supplements_ English-language articles | 234
mmnd_art_en | _MMWR_ notifiable diseases | 1,195
mmwr_art_es | _MMWR_ Spanish-language articles (19 WR, 1 RR, 2 SU)_ | 22
eid_toc_en  | _EID_ table of contents | 330
eid_art_en  | _EID_ English-language articles__ | 12,769
pcd_toc_en  | _PCD_ English-language table of contents | 49
pcd_toc_es  | _PCD_ Spanish-language table of contents | 36
pcd_art_en  | _PCD_ English-language articles | 3,011
pcd_art_es  | _PCD_ Spanish-language articles | 1,011
pcd_art_fr  | _PCD_ French-language articles | 357
pcd_art_zhs | _PCD_ Chinese-language (simplified) articles | 356
pcd_art_zht | _PCD_ Chinese-language (traditional) articles | 356
Total | | 33,567
