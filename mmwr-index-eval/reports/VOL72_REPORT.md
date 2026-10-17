# Vol 72 (2023): production gate report

Second production volume: Claude API, Message Batches, brief v5.2 (tag `brief-v5.2`), prefix `30d57882dbf0` with the preamble self-check. Same frame as vol 73: the report family plus QuickStats, holdout omitted, so 267 packets. Records are in `out/prod/72/`, the audit in `audit/72/`, and costs in `prod_ledger.tsv` and `reports/cost_by_article.tsv`.

## Gate (PRODUCTION_PLAN.md §4.2)
| Gate | Result | Status |
|---|---|---|
| Valid record for every article, or a logged failure (≤ 1%) | 267 of 267 valid: 251 on the first read, 16 after one fix-up | pass |
| Audit field error ≤ 1.5% | 0 of 378 scored fields; the second reads had 3 | pass |
| Groupwise check | 115 records with entries; 17 judged (7 flagged, 10 random): 1 of 51 fields wrong | pass |
| Cost per article within 25% of the estimate | $0.120 against $0.143 at submit (−16%) | pass |
| Condition grouping pass | 164 new terms grouped; nothing missing | pass |

## What the vol 73 fixes did
| | Vol 73 (v5.1) | Vol 72 (v5.2, self-check) |
|---|---|---|
| Invalid first reads | 61 of 202 (30%) | 16 of 267 (6%) |
| Overlong quotes among them | 51 | 11 |
| Second fix-up turns needed | 4 | 0 |
| Cost per article (reads and fix-ups) | $0.168 | **$0.120** |
| Output tokens per read | 4,193 | 4,850 |

Two changes account for the drop: no batch caching (so no cache rewrites) and five times fewer fix-ups. Output rose about 16%, probably the self-check's extra thinking, which is small next to the savings.

**Cost by type, vol 72.** These are the clean per-type figures for pricing other frames:

| Type | n | Mean | Median | Packet tokens | Output tokens | Fix-ups |
|---|---|---|---|---|---|---|
| full_report | 159 | $0.141 | $0.134 | 12,990 | 5,798 | 9% |
| notes_from_the_field | 52 | $0.088 | $0.085 | 3,729 | 3,358 | 0% |
| quickstats | 44 | $0.084 | $0.080 | 1,240 | 3,162 | 5% |
| outbreak_report | 8 | $0.098 | $0.100 | 7,958 | 3,523 | 0% |
| vital_signs | 4 | $0.159 | $0.163 | 17,154 | 7,824 | 0% |

**Spend.** Vol 72 cost $32.09 for reads and fix-ups, plus $1.67 for the audit second reads: $33.76 in all. Platform spend to date is $70.76.

**Vol 71 projection.** Its 292 packets (206 full reports, 33 Notes from the Field, 47 QuickStats, 5 Vital Signs, 1 outbreak report) come to **~$37** at vol 72's per-type means. The audit adds about $2. The total would then reach about **$110**. The driver's own estimate ($43.69) is more cautious, because it still assumes vol 73's fix-up rate. The guard refuses at the current $100 line.

## Audit (§5)
- **Random second reads:** 14 articles (6 full reports, 4 Notes from the Field, 4 QuickStats), 378 scored fields. 10 fields differed:
  - production read wrong on 0;
  - second read wrong on 3: mm7202a3 `groupwise_dims` (one rurality pair counted as groupwise), mm7204a3 `equity_dimensions` (age descriptor left out), mm7228a4 `groupwise_assessments`;
  - the other 7 were ambiguities or a brief gap, mostly the comparison framework when the referent is a historical baseline or a notification threshold (mm7230a5, mm7234a1).
- **Groupwise looks:** 1 wrong field in 51 (vol 73: 5 in 66). The error, mm7216a4, is the CI-overlap case v5.2 addressed: the only significance rule is non-overlapping CIs, stated once, and the age claims were still recorded as side by side. The narrowed flag caught it. Its other flags were false alarms, mostly CI-overlap tests of non-SPD contrasts.
- **`uncertain_fields`:** 249 of 267 records flag something. Most often: `equity.level` 116, `comparison` 88, `groupwise` 59, `study_design` 55, `analytic_depth` 47. With flags this common, the flagged/unflagged comparison carries no information this volume (no errors in either).

## Brief issues (`audit/72/brief_issues.md`, 11 notes)
None changed a production value. They group as:
- **Comparison framework** (3): whether a historical baseline or a notification threshold is a fixed referent, and how to lead when 3 or more groups are shown but only some pairs are tested.
- **Groupwise edges** (4): a chi-square with two SPD margins; strata that differ in pair coverage; a non-SPD effect's size compared across SPD levels; SVI attached to a workplace's county.
- **Labels** (4): when target comparisons make a report "monitoring"; qualitative interview components; primary comparison vs stratifier; SchoolVaxView as a data source.

Together with vol 73's open issues 4–7, these are candidates for a later brief revision. None is urgent for vol 71: two volumes in a row show field error under 1%.

## Decisions for the owner
| Item | Proposal | Decision |
|---|---|---|
| H1 | Accept vol 72 as passed | owner: accept (2026-10-17) |
| H2 | Vol 71 on brief v5.2 as is, with the budget line raised to cover it (~$110–115 in all) | owner: accept; budget line $120 (2026-10-17) |
| H3 | After vol 71, pause production and build the query tool on vols 71–73 before deciding on vols 70–65 | owner: accept (2026-10-17) |
