# Brief v5 confirmation run: report

Run date 2026-10-16, following `RUN_CONFIRM_V5.md`. Raw figures are in `cv5_run_log.tsv`, `reports/cv5_match.tsv`, `reports/cv5_scores.tsv`, `reports/cv5_brief_issues.md` and `reports/cv5_conditions.tsv`.

## Bottom line
- **Brief v5 works.** One v5 read per article, scored under v5, gets 0.8% of method and equity fields wrong (14 of 1,782), against 1.5% for pilot 2's medium reads on the same articles (27 of 1,782, scored under v4).
- **The disputes it targeted mostly resolved as intended.** The v5 read differed from pilot 2's reference value on 144 fields. Adjudicators sided with the v5 value on 100 of them, found both values defensible on 30, and sided with the reference on 14.
- **The routing placeholder (V17) now routes almost nothing.** It routes 6 of 66 articles, which hold none of the 14 errors, because v5 readers flag far less uncertainty. The routing design needs an owner decision (below).
- **Cost was a bit higher per article than pilot 2's medium arm.** 35K tokens, 55 s and 7.7 tool uses per article, against 31K, 39 s and 4.3. The whole run logged 4.0M tokens, about $38 at pilot 2's ratio of dollars to tokens.

## Models, articles, validation
- **Models:** readers were `claude-opus-5-5` at effort `medium`, self-reported in all 66 records. Adjudicators were `claude-opus-5-5` at default effort. The step-0 model check passed.
- **Articles:** 66 done in 17 reader agents (40 priority, 26 other). Refused: 0. Failed: 0.
- **First-pass validation (step 2): 66 of 66 valid, no re-runs.** Readers fixed their own validator failures before saving, as in pilot 2.
- **Process slips (no effect on results):**
  - one reader edited its own record with a Python command through Bash, against the Read/Edit rule;
  - another called the Artifact publish tool on a record by mistake, which was rejected, so nothing was published;
  - one adjudicator read `tools/confirm_v5.py` and so knew which label was the v5 read. See the blinding caveat below.

## Tokens, time and tool uses
From `cv5_run_log.tsv`.

| Agents | Articles | Tokens/article | Seconds/article | Tool uses/article |
|---|---|---|---|---|
| v5 readers, priority batches | 40 | 39,184 | 59.7 | 8.4 |
| v5 readers, other batches | 26 | 28,753 | 47.9 | 6.5 |
| **v5 readers, all** | 66 | **35,075** | **55.1** | **7.7** |
| pilot 2, medium arm (for comparison) | 100 | 30,892 | 39.1 | 4.3 |
| v5 adjudicators (12 agents) | 57 | 28,722 | – | – |

- **Totals:** reads 2.31M, adjudication 1.64M, model check 0.05M; **4.01M overall**. That is just past the 4M stop line set for this run; the overshoot came from the last adjudicator, already in flight.
- **Why v5 reads cost more:** reading every table and figure note, and using the Read tool instead of Bash dumps, both add tool calls. This is the price of V9(i) and of avoiding the personal-data blocks. No block occurred in this run.
- **Caveat:** as before, `subagent_tokens` measures context size, not billed tokens.

## Error rates
From `reports/cv5_scores.tsv`.
- **How a field was scored:** a field is right when the v5 value matches pilot 2's reference: the adjudicated verdict where pilot 2's reads disagreed (including any value its adjudicator accepted), the agreed value otherwise.
- **Differing fields:** where the v5 value differs, a v5 adjudicator ruled under v5. The v5 value counts as an error only when that adjudicator ruled it wrong.

| Segment | Group | v5 error | n | Pilot 2 medium, same fields |
|---|---|---|---|---|
| priority | method | 0.8% | 840 | 1.7% |
| priority | equity | 1.7% | 240 | 2.1% |
| other | method | 0.5% | 546 | 1.1% |
| other | equity | 0.0% | 156 | 1.3% |
| **all** | **all fields** | **0.8%** | 1,782 | **1.5%** |

**How the 144 differing fields resolved:**

| Outcome | Adjudicator category | Fields |
|---|---|---|
| v5 right, pilot-2 reference wrong under v5 | reader error | 91 |
| | omission | 5 |
| | brief gap | 4 |
| both defensible | ambiguity | 27 |
| | brief gap | 3 |
| v5 wrong | reader error | 7 |
| | omission | 7 |

**Where v5 changed the answer.** The fields where v5 most often overturned the pilot-2 reference:
- `outcome_types`: 13 (case totals, V4);
- `comparison_framework`: 11 (Summary headline, V7);
- `study_design`: 10 (review or synthesis, V12);
- `analytic_depth`: 8 (V3 and V5);
- `rf_nonindependence_approach`: 8 (null when silent, V11);
- `has_determinants`: 7 (V16);
- `procedure_classes`: 6.

One article accounts for 17 fields on its own. mm7332e1, an ACIP evidence summary, is now `limited` with empty method fields under V3(a), where pilot 2 had recorded the manufacturer's hazard-ratio model.

**The 14 v5 errors** are concentrated in the groupwise fields:

| Field | Errors |
|---|---|
| `groupwise_assessments` | 3 |
| `groupwise_any` | 2 |
| `groupwise_dims` | 2 |
| `analytic_depth` | 2 |
| `outcome_types` | 1 |
| `study_design` | 1 |
| `comparison_framework` | 1 |
| `has_determinants` | 1 |
| `equity_dimensions` | 1 |

Half of the errors sit in two articles: mm7334a2 (4, Table-1 composition tests) and mm7207a3 (3).

**Caveats.**
- **Weak blinding.** Each adjudication packet held only the v5 record, so the label with a full record was always the v5 read. One adjudicator also read the scoring tool. Adjudicators could tell which value was v5's, and they judged under the same brief the v5 reader followed. The 100 "v5 right" verdicts are therefore partly a check that the reader applied v5 as written, not an independent test.
- **Fields where v5 matched the reference were not re-examined.** The run looks only at differences.
- **One adjudication per field,** on 66 articles chosen because pilot 2 found problems in them. These are not a random sample.

## Did each V item settle its articles?
For each item: pilot 2's disputed fields among the articles it cites, and where the v5 read landed. "Matches" means v5 matched pilot 2's adjudicated value.

| Item | Disputed fields | Matches | v5 right (reference wrong) | Both defensible | v5 wrong |
|---|---|---|---|---|---|
| V3 limited records | 8 | 0 | 8 | 0 | 0 |
| V4 case totals | 6 | 2 | 3 | 1 | 0 |
| V5 summary | 3 | 2 | 0 | 0 | 1 |
| V6 groupwise | 18 | 13 | 2 | 0 | 3 |
| V7 framework | 3 | 3 | 0 | 0 | 0 |
| V8 inference | 7 | 4 | 3 | 0 | 0 |
| V9 procedures | 8 | 4 | 4 | 0 | 0 |
| V12 design | 6 | 1 | 3 | 2 | 0 |
| V15 dimensions | 4 | 1 | 2 | 0 | 1 |
| V16 determinants | 9 | 6 | 2 | 1 | 0 |
| V2, V10, V11, V14 | 1 each | 1 each | 0 | 0 | 0 |

V1 (flagging `data_focus`), V13 (cap) and V17–V18 have no pilot-2 disputed fields among their cited articles. **V6 (groupwise) is the one item still producing errors.**

## New rules and fields in use
Counts per 66 records, v5 vs pilot 2 medium on the same articles.

| | v5 | pilot 2 medium |
|---|---|---|
| `uncertain_fields`: 0 / 1 / 2 / 3+ entries | 20 / 40 / 5 / 1 | 9 / 23 / 24 / 10 |
| `data_focus` listed as uncertain | 10 | (not allowed) |
| `equity.level` listed as uncertain | 16 | 31 |
| groupwise: side by side / omnibus / per-group vs referent / pairwise (all pairs) | 25 / 10 / 12 / 7 | 33 / 9 / 17 / – |
| study_design `review or synthesis` | 6 | – |
| "CI overlap assertion" tests (`ci_overlap` true) | 3 (3) | – (3) |
| dimensions with `basis` | 26, in 66 records | – |
| equity `level_confidence` high / medium / low | 37 / 29 / 0 | 33 / 32 / 1 |
| `data_focus` yes / limited | 52 / 14 | 55 / 11 |

`basis` was used as intended. SOGI is recorded as behavior 5 times and self-identified 4 times. Sex is recorded as assigned or observed 2 times, self-identified 2 times and not stated 5 times. Race/ethnicity got a basis 7 times.

## Routing
Under V17 (two or more uncertain fields, or confidence low), v5 reads would send **6 of 66 articles** to a second reader: 3 of 40 priority, 3 of 26 other. Those 6 hold **none of the 14 v5 errors**. Low confidence never occurred, so in practice the rule is "two or more uncertain fields".

The v4 rule (any signal routes a priority report) would send 36 articles, 33 of the 40 priority ones, and catch 11 of the 14 errors.

**Interpretation.** v5 readers flag much less (2+ flags in 6 records against 34 in pilot 2), which fits a brief with fewer gaps. But the flags remain a weak guide to where errors are: the errors sit in groupwise and depth calls that readers did not mark as uncertain.

**Options for the owner:**
1. Drop systematic second reads. At 0.8% field error, audit a small random sample of production reads instead.
2. Route on content rather than flags, e.g. every priority report with a groupwise entry. In this sample that targets where the errors were.
3. Restore v4's any-signal rule for priority reports: 33 of 40 routed.

My suggestion is (1) plus a targeted check of groupwise entries.

## Conditions (V18)
- **Join coverage:** 133 conditions in the v5 records, joined with `tools/group_conditions.py`. 23 distinct terms are not in `conditions_map.tsv`.
- **New conditions (12):** arthritis, atrial fibrillation, COPD, coronary heart disease, diabetes, eating disorders, low birthweight, neonatal jaundice, stroke, xylazine exposure, canine leptospirosis, COVID-19–like illness.
- **New wordings of mapped concepts (11):** e.g. "opioid-involved overdose", "polio (wild and circulating vaccine-derived poliovirus)", "respiratory syncytial virus (rsv) disease", three wordings of serious adverse events and reactogenicity.
- **Consequence:** normalization has to run as a pass over new terms (the cheap model call the rubric describes). A lookup table alone will keep missing wordings. The join now also matches terms already written in canonical form.
- **Vaccine safety join:** the join-time rule tagged the vaccine-safety outcomes where `vaccine_outcome_type` is safety.

## Recurring brief issues (for a v5.1)
From readers' notes (`cv5_run_log.tsv`) and the 38 adjudicator notes (`reports/cv5_brief_issues.md`).
1. **No Summary to lead with.** V7's "Summary headline" rule has no counterpart for QuickStats, Notes from the Field or old reports (mm7303a5, mm7234a6, mm4834a2, mm7236a2, mm4948a1). The rule's parenthetical "group leads" also reads as competing with the headline rule (mm7320a3). Suggest: the first sentence of the findings stands in for the Summary, and the parenthetical applies only when the headline does not decide.
2. **Rule 2 of the brief contradicts V3(a).** It cites the Table 1 model in mm7332e1 as a missed procedure, but that model is the manufacturer's, so it is a cited method and not the article's own. Reword the example. V3(a) should also apply to every `data_focus` level, not only `limited` (mm7229a4, mm7241e1).
3. **Groupwise, where the v5 errors sit:**
   - rule 2 (Table-1 tests count the SPD margin) against rule 6 (composition is not groupwise) for per-level composition tests (mm7334a2, mm7320a3);
   - use `pairwise (all pairs)` only when every pair is shown (mm6953a1);
   - a QuickStats line "other differences not significant" reads as all pairs (mm7218a7).
4. **SDOH outcome against the level-2 cap** (mm7215a8, mm7218a7). Readers consistently gave 2 with 3 as the alternative, which is the intended reading; the brief could say so explicitly.
5. **"Always record a target"** when a target is cited but never compared against (mm7246e1, mm5016a4). Suggest: record it only when some claim compares against it.
6. **Smaller gaps:**
   - case-report `outcome_types` with no counts;
   - a case series' bare total (count/rate or not);
   - "Trends" in a title or annual cross-sections as `before/after or trend`;
   - a series compared only with cited referents, for `data_focus`;
   - imputation with no stated method;
   - functional-disability rows without social framing;
   - condition role for conditions a surveillance system monitors;
   - unequal vaccination coverage offered as a determinant (proposed: no, unless framed as access);
   - claims resting implicitly on a stated CI-overlap rule (proposed: yes, when no other test is given).
7. **Packet design.** Adjudicators could not rate the justification of the label with no record. A future run should build a full pseudo-record for the reference, or drop the justification rating.

## Recommendation
- **Adopt brief v5** for production.
- **Make the small v5.1 edits** in items 1–3 above. These are wording changes, not a new pilot.
- **Decide routing** (option 1 suggested).
- **Extend `conditions_map.tsv`** with the 23 new terms through one cheap grouping pass.

No further full pilot looks necessary.
