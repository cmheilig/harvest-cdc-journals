# mmwr-index-eval

An evaluation of LLM readers for indexing *MMWR* reports by **how they make claims about data**. It covers summaries, comparisons, tests, standardization, weighting and regression, along with design, data source, condition and a graded health-equity reading. The pilot closed on 2026-10-16 with brief v5.1; production follows `PRODUCTION_PLAN.md`, starting from `HANDOFF_LOCAL.md`. The output is not yet an index.

## Contents

| Path | What it is |
|---|---|
| `READER_BRIEF.md` | the readers' contract (version 5.2, from vol 72; vol 73 was read with 5.1) |
| `SCHEMA.md` | the index schema (v2.1, with v2.2 additions in §9); the brief governs |
| `BRIEF_V4_DECISIONS.md` | the instructor's decisions behind brief v4 (D1–D20) |
| `BRIEF_V5_DECISIONS.md` | the instructor's decisions behind brief v5 (V1–V21), from pilot 2, the confirmation run and the vol 73 audit |
| `RUN_ARMB_AUDIT.md`, `reports/AUDIT_REPORT.md` | the arm-B audit and its result |
| `RUN_PILOT2.md`, `tools/pilot2.py`, `p2_sample.tsv`, `reports/PILOT2_REPORT.md` | pilot 2 on brief v4: effort low vs medium, routing, condition grouping |
| `RUN_CONFIRM_V5.md`, `tools/confirm_v5.py`, `cv5_sample.tsv` | the brief v5 confirmation run; result in `reports/CONFIRM_V5_REPORT.md` |
| `PRODUCTION_PLAN.md`, `tools/estimate_cost.py` | the accepted plan for production (API batches, volume gating, audit) and its cost estimate |
| `HANDOFF_LOCAL.md` | where a disk-local session starts: what to assess, then build and run |
| `HANDOFF_QUERY.md` | where the query-tool session starts (production paused after vols 73–71) |
| `reports/VOL73_REPORT.md`, `VOL72_REPORT.md`, `VOL71_REPORT.md` | per-volume gate reports; vol 71's also sums up vols 73–71 and projects vols 70–65 |
| `reports/LOCAL_ASSESSMENT.md` | the disk-local assessment: packet builder, coverage, frame, API access; the owner's frame decisions (L1–L5) |
| `tools/build_packets.py` | builds packets from the extraction's frozen outputs; `check` reproduces `packets/`, `vol VOL` writes `prod_packets/<vol>/` (git-ignored; rebuild it) |
| `tools/api_read.py`, `API_PREAMBLE.md` | the API driver (calibrate, slice, submit, poll, collect, fixup, retry, status) and the preamble that replaces the brief's tool instructions; writes `out/prod/<vol>/`, `prod_ledger.tsv`, `prod_batches.tsv` |
| `tools/cost_report.py` | cost per article joined to report characteristics -> `reports/cost_by_article.tsv`, with summaries by type |
| `RUN_PRODUCTION.md`, `tools/audit_sample.py` | one volume end to end: read, fix up, audit (draw, second reads, blinded packets, score), groupwise check, condition grouping; writes `audit/<vol>/` |
| `CONDITION_RUBRIC.md`, `conditions_map.tsv`, `tools/group_conditions.py` | grouping readers' free-text conditions |
| `CONDITION_RUBRIC.md` | groups for the later condition-grouping pass (readers do not classify conditions) |
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
