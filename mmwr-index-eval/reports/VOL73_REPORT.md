# Vol 73 (2024): production gate report

First production volume: Claude API, Message Batches, brief v5.1, prefix `f4030e441a8d`. Frame per `reports/LOCAL_ASSESSMENT.md`: the report family plus QuickStats, holdout omitted, so 202 packets. Records are in `out/prod/73/`, the audit in `audit/73/`, and costs in `prod_ledger.tsv`.

## Gate (PRODUCTION_PLAN.md §4.2)
| Gate | Result | Status |
|---|---|---|
| Valid record for every article, or a logged failure (≤ 1%) | 202 of 202 valid: 141 on the first read, 57 after one fix-up turn, 4 after a second | pass |
| Audit field error ≤ 1.5% | 1 of 297 scored fields (0.34%); the second reads had 3 | pass |
| Groupwise check | 68 records with entries; 22 judged (17 flagged, 5 random): 5 of 66 fields wrong, in 3 records | reviewed; see below |
| Cost per article within 25% of the estimate | $0.169 against $0.123 (the uncached estimate at submit); +37% | **fail**, causes fixed |
| Condition grouping pass | 197 new terms grouped; nothing missing | pass |

## Coverage and fix-ups
- **First read:** 141 valid, 60 invalid, 1 unparsed.
  - 51 of the 61 failures had a quote over 25 words, almost all of 26–28 words: miscounting, not long excerpts.
  - The rest were shape slips: `loc` values such as `figure` without a number or `abstract`, extra keys in `equity`, one bad `uncertain_fields` value, one unparsable reply.
  - The cloud readers ran the validator themselves before saving; the API reader cannot.
- **Fix-up turn (batch):** 57 fixed, 4 failed.
- **Driver fault behind 3 of the 4 failures.** `validate.check()` reports schema errors first and checks quote lengths only once the schema passes. So those 3 fix-ups were never told about their overlong quotes. The driver now adds quote errors to any error list. A second turn in the same conversation (`fixup --again`) fixed all 4.
- **No refusals,** no truncations, no fallback-served reads.

## Cost
| Item | Requests | Cost |
|---|---|---|
| Reads (batch) | 202 | $27.05 |
| Fix-ups (batch) | 61 | $6.33 |
| Second fix-ups (synchronous) | 4 | $0.65 |
| Cache warm-up | 1 | $0.18 |
| **Vol 73 reads** | | **$34.21** ($0.169 per article) |
| Audit second reads and their fix-ups | 11 + 5 | $2.21 |
| **Vol 73 in all** | | **$36.42** |

Platform spend to date is $37.00 (the calibration slice adds $0.58), against the ~$100 spending note.

**Why the cost ran over:**
1. **Batch caching backfired.** Only 43 of 202 reads hit the cache; 159 wrote it again at the 1-hour price, 3.65M tokens costing $14.59. As plain batch input those tokens would have cost $7.29. Batch requests no longer carry a cache marker. The audit's 11 uncached batch reads cost $0.113 per article.
2. **Fix-ups ran at 30%** against the 15% the estimate assumed. Each costs about as much as a read ($0.104 here), because it resends the prefix and the packet. From vol 72 the preamble has a self-check item: quotes of 20 words or fewer, valid `loc` values, no extra keys (prefix `7ae6d10a9c46`).

**Projection for vol 72** (267 packets): about $30 for reads, $3–8 for fix-ups (10–30%) and about $3 for the audit, so **$36–41**. The running total would then be about $73–78.

## Audit (§5)
- **Random second reads:** 11 articles (8 full reports, 2 QuickStats, 1 outbreak report), 297 scored fields. 9 fields differed:
  - adjudicated: 4 reader errors, 4 ambiguities, 1 brief gap;
  - production read wrong on 1 field: mm7306a3 `equity_level`, given 0 where homelessness, recorded as a descriptor, puts it at 1;
  - second read wrong on 3: mm7314a1 `outcome_types`, mm7334a2 `has_determinants`, mm7340a2 `equity_level`;
  - all differences were in equity, outcome-type or data-source boundaries; no method field was wrong.
- **Groupwise looks:** 22 records, 66 fields, 5 wrong in 3 records:
  - mm7309a3 and mm7324a1: the article's only significance method is a CI-overlap rule, stated once under "highest / differed" claims across SPD groups. The reader logged the CI-overlap test but recorded the result as `side by side`; the verdict is `per-group vs referent only`.
  - mm7339a1: a chi-square across three PAHO target groups (young children, persons with comorbidities, older adults) recorded as an age omnibus test. Under rule F7 there is no groupwise entry; the adjudicator calls this a brief gap.
- **Mechanical check, narrowed after the looks.** "Side by side but the record has tests" fired 32 times and was right only in the two CI-overlap cases. It now fires only for CI-overlap or omnibus-type tests: 17 flagged records become 7, and both errors are kept. "Per-group vs referent" no longer requires a test, since ratios against a referent suffice. Quote matching ignores footnote symbols. The draw used the earlier version (`audit/73/groupwise_check.tsv`); the narrowed run is `groupwise_check_v2.tsv`.
- **`uncertain_fields`:** 188 of 202 records flag something. Most often: `equity.level` 88, `comparison` 60, `study_design` 41, `outcome_types` 38, `data_focus` 34. With flags this common, the flagged/unflagged comparison says little (1 error in 243 flagged fields, 0 in 54 unflagged).
- **Sample size:** 11 articles is the plan's minimum for a volume this size; one error more or less moves the rate by 0.34 points.

## Brief issues (`audit/73/brief_issues.md`, 11 notes)
Candidates for a brief v5.2, made between volumes with a new tag. The first three are tied to observed errors.
1. **Section F, CI overlap:** say what the brief says for a blanket t-test. A CI-overlap rule stated once under "highest / higher" claims across SPD groups gives `per-group vs referent only`, not `side by side`. (2 errors.)
2. **Rule F7, composite target groups:** program target groups that mix age bands with a clinical group are non-SPD groups (comparison only), unless 3 or more age groups are compared within one clinical level. (1 error.)
3. **Rule 6, scope of rule 2:** rule 2 applies only when the SPD is a margin of the test, not to a within-level test of a non-SPD contrast. (No error; the cause of most false flags.)
4. **Rule F4:** a ranking of per-group changes, or a uniform within-group trend claim, with no cross-group test, is `side by side`.
5. **Equity level:** whether per-level ORs of a covariate shown only in a table, or an outcome's composition across SPD groups without denominators, reach level 2.
6. **Section A:** whether a compared population characteristic (sex partners, age by vaccination status) makes an outcome continuous.
7. **Data sources:** NREVSS and similar CDC laboratory networks belong among the common cases.

## Driver changes made during vol 73
- Batch requests carry no cache marker, and there is no warm-up for batches.
- Each volume's system prompt is pinned in `out/prod/<vol>/prefix.json`, so fix-ups (append-only), retries and audit second reads keep the prefix the volume was read with.
- Error lists include quote-length errors even when the schema check fails first. `fixup --again` adds a second turn for failed fix-ups.
- No submit, fix-up or retry is allowed while the volume has an uncollected batch.
- The preamble self-check applies from vol 72.

## Decisions for the owner
| Item | Proposal | Decision |
|---|---|---|
| G1 | Accept vol 73 as passed: the cost overrun is explained and its causes fixed | |
| G2 | Brief v5.2 before vol 72, limited to issues 1–3, tagged `brief-v5.2`; issues 4–7 stay open for the audit to watch | |
| G3 | Go ahead with vol 72 (~$36–41), then stop and reassess at about $75 in all, as planned | |
