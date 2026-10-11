# mmwr-index-eval

An evaluation of LLM readers for indexing *MMWR* reports by **how they make claims about data**. It covers summaries, comparisons, tests, standardization, weighting and regression, along with design, data source, condition and a graded health-equity reading. This is a pilot. If it works it will be promoted, and the output is not yet an index.

## Contents

| Path | What it is |
|---|---|
| `READER_BRIEF.md` | the readers' contract (version 3) |
| `SCHEMA.md` | the index schema (v2.1); the brief governs |
| `ADJUDICATOR_BRIEF.md` | how disagreements between the two readers are judged |
| `RUN_CLOUD_PILOT.md` | step-by-step instructions for the orchestrating session |
| `sample.tsv`, `batches.json` | 100 articles: 78 from 2023–2024 and 22 from 1999–2001, split into arms A and B; batches of 4 within each arm and stratum |
| `packets/<id>/` | per article: `article.txt` (text from the CDC page, reading order, references omitted), `tables/*.csv` (every HTML table cell), `prepass.json` (lexicon seeds) |
| `examples/` | 7 worked records, with every quote taken from the article |
| `tools/` | `validate.py` (JSON Schema plus rules), `merge_review.py`, `compare.py`, `score.py` |
| `out/`, `adjudication/`, `reports/`, `run_log.tsv` | outputs |

## Readers
- **Structural reader:** Claude Haiku 5.5, for presence and absence of features.
- **Judgment reader:** Claude Opus 5.5, for judgment, with self-reported confidence and an alternative level for the equity rating.
- **Both readers' values are kept.** Disagreement between them is information for whoever selects candidate reports.

## Source
The article text is derived from public *MMWR* pages (cdc.gov) by a separate extraction build. Each `article.txt` begins with its source URL.
