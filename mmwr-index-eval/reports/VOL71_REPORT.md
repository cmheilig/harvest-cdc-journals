# Vol 71 (2022): production gate report, and the pause after vols 73–71

Third production volume: Claude API, Message Batches, brief v5.2, prefix `30d57882dbf0`. Same frame as vols 73 and 72: the report family plus QuickStats, holdout omitted, so 292 packets. Production now pauses for the query tool, as decided (H3 in `reports/VOL72_REPORT.md`).

## Gate (PRODUCTION_PLAN.md §4.2)
| Gate | Result | Status |
|---|---|---|
| Valid record for every article, or a logged failure (≤ 1%) | 292 of 292 valid: 272 on the first read, 20 after one fix-up | pass |
| Audit field error ≤ 1.5% | 3 of 405 scored fields (0.74%); the second reads had 1 | pass |
| Groupwise check | 137 records with entries; 27 judged (15 flagged, 12 random): 1 of 81 fields wrong | pass |
| Cost per article within 25% of the estimate | $0.124 against $0.125 at submit | pass |
| Condition grouping pass | 176 new terms grouped; nothing missing in vols 71–73 | pass |

## Audit (§5)
- **Random second reads:** 15 articles (10 full reports, 5 QuickStats), 405 scored fields. 6 fields differed:
  - production read wrong on 3;
  - mm7123e2 `equity_dimensions` and `groupwise_dims`: the production read followed the brief's literal rule that rurality of non-human units is not an SPD, but the units were schools. The adjudicator calls it a brief gap and sided with the second read.
  - mm7147a2 `study_design`: `before/after or trend` missed for period percentage changes (an omission).
  - the second read was wrong on 1 (mm7132e3: a sex descriptor left out); 2 were ambiguities.
- **Groupwise looks:** 1 wrong field in 81, in mm7146a2. "Sex" was added to an entry whose survey item asked only gender identity, so the dimension belongs under SOGI alone. Every side-by-side flag in vol 71 was a false alarm; most came from CI-overlap or chi-square tests of non-SPD contrasts, which readers now handle correctly under v5.2.
- **`uncertain_fields`:** 272 of 292 records flag something. Most often: `equity.level` 131, `comparison` 92, `groupwise` 72, `study_design` 58. As in vols 73 and 72, the flags are too common to separate errors (all 3 errors were in flagged records, which are 378 of 405 fields).

## Vols 73–71 together
| | Vol 73 | Vol 72 | Vol 71 | All |
|---|---|---|---|---|
| Brief | v5.1 | v5.2 | v5.2 | |
| Articles indexed | 202 | 267 | 292 | **761** |
| First reads invalid | 30% | 6% | 7% | |
| Audit field error | 1 / 297 | 0 / 378 | 3 / 405 | **4 / 1,080 (0.37%)** |
| Groupwise looks, fields wrong | 5 / 66 | 1 / 51 | 1 / 81 | 7 / 198 |
| Cost, reads and fix-ups | $34.03 | $32.09 | $36.31 | $102.43 |
| Cost per article | $0.168 | $0.120 | $0.124 | $0.135 |
| Audit second reads | $2.21 | $1.67 | $1.58 | $5.46 |

Platform spend to date: **$108.64**. That is $102.43 for reads and fix-ups, $5.46 for the audits, $0.58 for the calibration slice and $0.18 for vol 73's cache warm-up. The budget line is $120.

**Cost by type, v5.2** (vols 72 and 71, 559 articles): full_report $0.139 (n = 365); notes_from_the_field $0.087 (85); quickstats $0.082 (91); vital_signs $0.189 (9); outbreak_report $0.099 (9). Within a type, cost follows length and analytic content. Per article: `reports/cost_by_article.tsv`; to regenerate: `python3 tools/cost_report.py`.

## Projections for vols 70–65 (for the decision after the query tool)
At the v5.2 per-type means, holdout omitted:

| Frame | Articles | Reads | With audit (~5%) |
|---|---|---|---|
| Full reports only | 1,212 | ~$169 | ~$177 |
| Report family plus QuickStats (as vols 73–71) | 1,859 | ~$229 | ~$240 |
| The holdout in these types, if the extraction holdout is released | 182 | ~$25 | |

The holdout ids held back from vols 73–71 (85 in scope) would cost about $11.

## Brief issues (`audit/71/brief_issues.md`, 11 notes)
None is urgent. Together with the open notes from vols 73 and 72, they cluster in four places:
- **Groupwise edges:** a 2-group SPD contrast compared across levels of another SPD; one tested pair beside an untested third group; a ranking of increases with no per-group data; a balance measure (SMD) across levels; a test named in Methods with no result.
- **Units that stand for people:** rurality of schools or clinics (V15(d)), and country- or state-level rate comparisons as "ecologic".
- **Cited rather than computed estimates:** VE figures in ACIP summaries, for `vaccine_outcome_type`.
- **Data sources:** registries that a sentinel network consults to verify vaccination status.

## Tooling notes
- **Side-by-side flag.** Its true hits came from vol 73 (v5.1) and one case in vol 72. In vol 71 it fired 23 times for no error. It is cheap to keep, but its looks could be capped at a few per volume.
- **`compare.norm`.** It treats an omitted `basis` and `"not stated"` as different. That field is best effort and not scored, but normalizing it would remove a recurring false difference.

## Decisions for the owner
| Item | Proposal | Decision |
|---|---|---|
| J1 | Accept vol 71 as passed; production paused after vols 73–71 (761 articles, $108.64) | |
| J2 | Next: the query tool over vols 71–73, then decide the frame and pace for vols 70–65 with the projections above | |
