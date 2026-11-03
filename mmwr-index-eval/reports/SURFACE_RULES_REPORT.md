# Surface rules for a first-pass index (2026-11-02)

*Claude's analysis, for the owner's review. No API calls were made.*

**Question.** Can the method judgments in the production records (vols 73–71, brief v5.1/v5.2) be turned into surface rules (regular expressions over the packet text) accurate enough for a first-pass index of earlier volumes? The richer reading would then go only to articles selected for teaching.

**Method.** `tools/surface_rules.py`.
- **Labels:** article-level presence of each value, taken from `out/prod/<vol>/mm*.json`. The records are the reference, so a rule can at best match them.
- **Cues:** `mine` collects 1–3-word phrases from each label's `ev` quotes and ranks them by how well their presence in `article.txt` predicts the label (`reports/surface_cues.tsv`).
- **Rules:** written by hand from the cues, with *traps* masked before matching (below), and tuned on vols 72–73 (469 articles) only.
- **Held-out check:** vol 71 (292 articles), scored once after the rules were frozen (`reports/surface_scores_test.tsv`). Misses by id are in `surface_misses_*.tsv`.

## Results on vol 71 (held out)

| Label | Articles with it | Precision | Recall |
|---|---|---|---|
| any regression | 65 | 0.91 | 0.97 |
| logistic | 37 | 0.97 | 0.81 |
| loglinear | 20 | 0.71 | 0.75 |
| event-time | 6 | 0.86 | 1.00 |
| CIs | 133 | 0.96 | 1.00 |
| tests (inference) | 132 | 0.98 | 0.93 |
| weighting | 39 | 0.90 | 0.97 |
| standardization | 29 | 0.96 | 0.93 |
| groupwise (any) | 137 | 0.84 | 0.88 |
| test-negative design | 16 | 0.82 | 0.88 |
| guidance or recommendations | 17 | 1.00 | 0.77 |
| equity level ≥ 2 | 100 | 0.97 | 0.74 |
| equity 3–4 candidate (see below) | 24 | 0.50 | 0.92 |
| stratified/adjusted depth | 116 | 0.78 | 0.77 |
| count/rate outcome | 122 | 0.61 | 0.87 |
| continuous outcome | 25 | 0.17 | 0.84 |
| before/after or trend | 84 | 0.63 | 0.54 |
| evaluation or monitoring | 24 | 0.59 | 0.54 |
| case report/series | 23 | 0.26 | 0.91 |

The full table, with dev beside it, is in `reports/surface_scores_{dev,test}.tsv`.

**Three groups.**
1. **Fields defined by naming hold up:** regression (any), CIs, tests, weighting, standardization, test-negative and guidance. On vol 71 they come within a few points of their dev scores.
2. **The regression family is weaker than its presence.** Logistic recall fell from 0.98 on dev to 0.81, loglinear from 0.90 to 0.75. Joinpoint has no surface family: the records give it linear or loglinear by reasoning, so the rules record it as a regression and leave the family open.
3. **Fields defined by what the claims rest on stay with the model:** outcome type (risk versus rate, a compared median), depth, and most designs (trend, evaluation, case series, cross-sectional versus survey). This matches how the brief defines them.

## The equity band

Predicted band against the recorded level, vol 71:

| Predicted | Level 0 | 1 | 2 | 3 | 4 | Articles |
|---|---|---|---|---|---|---|
| 0–1 | 110 | 80 | 25 | 0 | 1 | 216 |
| 2 | 1 | 0 | 30 | 1 | 0 | 32 |
| 3–4 candidate | 0 | 1 | 21 | 3 | 19 | 44 |

- **Band 2 (groups by social position):** an SPD named in a QuickStats-style title ("by family income"), or a contrast sentence naming an SPD group ("higher among … Black", "compared with uninsured") backed by SPD levels in a table. Age and sex alone do not count, as in the brief.
- **3–4 candidate:** band 2 plus a framing word anywhere in the text (disparity, inequity, inequality, equity, disproportionate). On vol 71 it flags 44 articles (15%) and holds 22 of the 24 at level 3 or 4. The full equity reading would go to those 44, not to 292.
- **Misses:** a quarter of level-2 articles fall into band 0–1, mostly group results in tables that the text does not discuss. One level-4 article in vol 71 was missed outright.

## Traps found in the records

Phrases that look like a method and are not, by the brief's rules. Each is masked before the rules match:
- "occupational hazard", "workplace hazards" (not event-time);
- "logistical", "logistic issues" (not logistic);
- "Poisson distribution", "exact Poisson method", "assuming a Poisson" (a CI method, not a regression);
- "log-linear chi-square tests" (a test);
- "least-squares means" (not a model);
- "EPIC-HR", a trial name (not a hazard ratio);
- an odds ratio from a 2 × 2 table in an outbreak report (arithmetic, not logistic regression);
- an annual percent change computed by hand (not joinpoint);
- "weighted mean percentage", "time-weighted average", "diffusion-weighted imaging", "not weighted", and body weight in its many forms (not survey weighting).

The reverse also occurs: regression without the word, for example SUDAAN's `RLOGIST` procedure, or "aPR" and "aOR" alone.

**Tests by CI inversion.** Vaccine-effectiveness and ratio reports often have no named test; the brief counts a CI judged against the null as one. The rule treats CIs plus a ratio or VE measure as a test, and suppresses tests in guidance reports, whose figures are cited (rule 2).

## Limits

- **Vocabulary from 2022–2024 only.** Reports from 1999–2010 phrase methods differently, often lack a Summary box, and have fewer CIs. The rules are untested there; the plan's §4.4 calibration slice would test them.
- **Cited methods.** Rules cannot tell an article's own method from one it cites (rule 2), except in guidance reports.
- **Tuned by one reader on one split.** The rules were written against dev misses, and vol 71 was scored once. The drop in the regression family suggests some fitting to dev.
- **For comparison:** pilot 1's structural reader (Claude Haiku 5.5, full brief) had 5–7% field error overall, and 37–47% error on `equity_dimensions` (`reports/RUN_REPORT.md`). That is a different metric on a different sample, but it suggests the rules are not far below a small model on the fields they cover.

## Possible next steps (each the owner's call)

1. **$0:** run the rules over vols 70–48 to see the candidate counts each band and family would produce. This is a sizing exercise only, with no accuracy claim.
2. **About $15:** full-brief reads of about 120 older articles, stratified by period and type, as reference labels. These test the rules on older vocabulary, and they are the plan's §4.4 slice.
3. **Then:** the model reads only what the rules cannot carry (outcome type, depth, design) or are unsure of, either Haiku quoting the methods sentences or a short brief. The full brief goes only to articles selected for teaching.
