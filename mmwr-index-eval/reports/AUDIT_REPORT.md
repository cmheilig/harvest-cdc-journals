# Arm-B audit: report

Run 2026-10-13 under `RUN_ARMB_AUDIT.md`, on brief v3. Raw figures are in `audit_run_log.tsv`, `reports/audit_agreement.tsv`, `reports/audit_scores.tsv` and `reports/audit_brief_issues.md`.

**Question.** In arm B, does the merged record (Haiku's full read plus Opus review) hide errors that the pilot could not see? The pilot scored only articles where the merged and Haiku records disagreed, and it counted agreed fields as correct by default.

## Models and completion
- Audit readers: `claude-opus-5-5` (24/24 records self-reported). Adjudicators: `claude-opus-5-5`.
- **Done: 24/24** read, all valid on first pass. **Adjudicated: 24/24**, including articles where the two records fully agree. Refused: 0. Failed: 0.
- **Tokens:** 1.27M in the harness's per-agent metric, under the 2.5M guard. Readers used 717,636 (29.9K per article, 57 s per article); adjudicators used 549,328 (22.9K per article). As in the pilot, this metric is context size, not billed usage.

## Sample
- **18 undisputed:** every arm-B article where the pilot found no disagreement (13 from 2023–24, 5 from 1999–2001).
- **6 disputed:** for calibration (5 from 2023–24, 1 from 1999–2001).
- That makes 648 field judgments, 27 per article.

## Hidden-error rate (the main result)
Field error rate, judged by the audit adjudicators:

| Articles | Fields | Merged record (arm B) | Independent Opus read |
|---|---|---|---|
| 18 undisputed | all | **3.1%** (15/486) | 1.0% (5/486) |
| 18 undisputed | high-value* | **2.8%** (6/216) | 1.9% (4/216) |
| 6 disputed | all | 4.9% (8/162) | 3.1% (5/162) |
| 6 disputed | high-value* | 5.6% (4/72) | 2.8% (2/72) |

\* High-value: procedure classes and flags, regression families, study design, comparison framework and scale, equity level, equity flag, equity dimensions.

For comparison, the pilot reported the merged record's error as **0.2%**, and arm A's full Opus read as 1.4%. **The pilot understated arm B's error about fifteenfold.** The reason is that agreement between the merged and Haiku records was counted as correctness. In the 18 undisputed articles, the merged record equals Haiku's record on every normalized field, so the review changed nothing there. The independent read disagreed with it on 29 fields (6%).

**The 15 hidden errors** are in 7 of the 18 articles:
- **mm4847a4** (6 errors, one cause): an awareness-month notice marked `data_focus` none, which emptied every method field. The adjudicator ruled it `limited` (figures cited from earlier reports, a 39% fall). The empty outcome types, depth, comparison framework and scale, and study design all follow from that one call.
- **mm7206a1** (2): study design left empty for an ACIP schedule (`guidance or recommendations`); COVID and influenza missing from condition groups.
- **mm7240a3** (2): suppression of cells with fewer than 10 not recorded as a transformation; vaccination receipt not recorded as `coverage/uptake`.
- **mm4910a3** (1): age recorded as a dimension from a cited background sentence.
- **mm5034a3** (1): administrative coverage listed as an extra data source.
- **mm7316a5** (1): `count/rate` missing for yearly case counts.
- **mm7226a9** (1): `summary` depth resting on a footnote that is itself a sex comparison.
- **mm7213a5** (1): `record abstraction or interview` missing for a field investigation.

Equity level: the merged record and the independent read differ on only one article (mm7330a2, level 1 vs 0), and the adjudicator did not rule the merged level wrong. No hidden error changes an equity level, and no shared ("agreed") error was found in any of the 24 articles.

## Calibration (6 disputed articles)
The audit's adjudication and the pilot's agree on whether the merged record is right on **155 of 162** fields (96%). All 7 differences are in two articles (mm7229a4, mm7205e1). They are fields the pilot never examined, because the merged and Haiku records agreed there; only `equity_dimensions` was disputed in each. Six of the 7 turn on one brief gap: whether VE or RR results whose CIs exclude the null, with no explicit statement, imply an unnamed "CI vs fixed value" test. The seventh is ICATT as `sentinel/network`. So the adjudicators are consistent with each other. The differences show the same hidden-error mechanism, not a disagreement between adjudicators.

## How far review moved Haiku toward an independent read
From `reports/audit_agreement.tsv`:
- **Disputed articles:** Haiku differs from the independent read on 25 fields, the merged record on 17. Review closed about a third of the gap.
- **Undisputed articles:** 29 and 29. The review made no normalized change, so every Haiku value stood, including the 15 that turned out wrong.

## Recommendation
**Under the decision rule in `RUN_ARMB_AUDIT.md`, arm B passes, narrowly:**
- the high-value gap is 0.9 percentage points (2.8% vs 1.9%), within the 2-point margin;
- no hidden error changes an equity level;
- no primary procedure is missed. The one missed procedure is a cell-suppression transformation, and the `data_focus` error is on a notice with no procedures of its own.

**Arm B is nonetheless not recommended as the production design:**
1. **Review anchors on Haiku.** Where Opus saw no reason to object, Haiku's errors stood. Overall error is three times the independent read's (3.1% vs 1.0%).
2. **It doesn't save much.** The independent Opus full read cost about 30K per article, the same as the pilot's Opus review (30.6K per article in 2023–24). Arm B therefore pays for Haiku's full read *plus* a near-full Opus read, and gets a record anchored to the cheaper model.
3. **A better candidate: a single Opus full read per article,** with a second reader or adjudication only on a sample, or on fields flagged by low confidence. Billed cost per model should be checked with the credit balance before deciding.

**Caveats:**
- 18 articles (486 field judgments) detect large differences only. 15 vs 5 errors is suggestive, not precise.
- Six of the 15 hidden errors come from a single upstream call (mm4847a4).
- The adjudicator is Opus, as is the independent reader, so a same-model preference could favour the audit reader. A human check of a few `adjudication_audit/` files would settle that.

## Recurring brief issues (new or sharpened)
From `reports/audit_brief_issues.md` and the readers' handbacks:
- **Implied "CI vs fixed value" tests** when CIs all exclude the null but no judgment is stated (mm7229a4, mm7205e1, mm4910a3). This decides `has_test` and `inference`.
- **The `data_focus` none / limited boundary** for notices that cite figures (mm4847a4). Also whether `study_design` = `guidance or recommendations` is filled when `data_focus` is none (mm7206a1, mm4909a4). This bears on BRIEF_V4_DECISIONS D3 and D9.
- **Missing `condition_group` values:** asthma, hepatitis C.
- **Edge cases:** count-weighted pooling of jurisdiction medians as weighting; imputation of missing income as a procedure; whether context totals add `summary` depth; whether vaccination status among cases is `coverage/uptake`; long-term care residence as a social-position dimension; an area index used only as a covariate under determinants.
- **The prepass lexicon** produced false cues ("Yates County" read as the Yates correction; a Pearson correlation as chi-square).

## What went wrong
- One reader opened `tools/validate.py`, which was off its list (schema code only).
- Two records give `level_alternative: null` at high confidence; the validator accepts it (see BRIEF_V4_DECISIONS T4).
- No scratch-folder collisions this time: per-agent folders worked.
