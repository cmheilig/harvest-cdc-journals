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

This version was constructed on 2023-11-11.
