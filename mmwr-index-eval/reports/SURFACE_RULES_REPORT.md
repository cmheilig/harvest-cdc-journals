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

Phrases that look like a method and are not that method, by the brief's rules. Each is masked from the *regression* rules only. Statistical uses among them stay findings in their own right (next section), so a Poisson CI or a log-linear chi-square is reclassified rather than dropped:
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

## Declared procedures: the finding-aid layer (added 2026-11-03)

*At the owner's request: the finding aid serves non-regression procedures too, and it records only what a report declares.*

`python3 tools/surface_rules.py findings --split all` reads each article sentence by sentence, with notes and table titles included, and records every procedure, CI method or measure the text names. Output: one row per article and finding (`reports/surface_findings_all.tsv`), and counts per finding (`reports/surface_findings_summary_all.tsv`). There are 49 findings in four families:
- **tests:** chi-square, survey chi-square, Fisher's exact, t, z, Wilcoxon, Kruskal-Wallis, trend, log-rank, and others;
- **CI methods:** Wilson, exact binomial, Korn-Graubard, Taylor series, bootstrap, gamma, credible intervals;
- **estimation:** Kaplan-Meier, standardization, suppression rules, smoothing, imputation, nowcasts, diagnostic accuracy, and others;
- **measures:** OR, RR or PR, rate ratio, HR, VE, APC, percentage points.

**Poisson, by sentence context.** A sentence naming Poisson is classed by what else it declares:

| Poisson finding | Articles |
|---|---|
| regression or model | 19 |
| confidence interval | 11 |
| test | 5 |
| distribution assumed (none of the above) | 0 |
| other mention | 2 |

One article can hold several. Only the "regression or model" sentences bear on the regression family. The others stay findings for the non-regression units.

**Joinpoint: only the declared scale.** The rule looks for a scale declared in the joinpoint sentence or the two after it: log (log-linear, log-transformed, logarithm) or linear (linear model or regression, untransformed). "Linear segments" is not a declaration, because it describes the shape on either scale. Of the 8 joinpoint articles in vols 73–71:
- **7 declare no scale anywhere in the text.** The records gave them loglinear (5) or linear (2) by inference.
- **1 declares "linear regressions with zero joinpoints"** and also reports APCs and AAPCs. The finding aid shows both declarations and leaves the reading to the user.

The regression-family rules never infer a family from joinpoint.

**Check against the records.** For 20 findings, the records' own procedure names, `inferred_from` and inference evidence can be compared with what the text declares (all 761 articles; "both" means text and record agree):

| Finding | Both | Text only | Record only |
|---|---|---|---|
| chi-square | 81 | 0 | 0 |
| t-test | 43 | 0 | 0 |
| Wilcoxon / Mann-Whitney | 27 | 0 | 0 |
| Fisher's exact | 21 | 0 | 0 |
| Kaplan-Meier / life table | 13 | 0 | 0 |
| Poisson CI | 10 | 1 | 3 |
| Taylor series | 9 | 0 | 0 |
| joinpoint (any scale) | 8 | 0 | 0 |
| standardization | 60 | 5 | 17 |
| imputation | 21 | 6 | 1 |
| suppression rule | 46 | 29 | 39 |

Named tests agree completely. Suppression rules agree least, and standardization has 17 record-only cases. Neither set of disagreements has been examined yet: they may be wording the patterns miss, ancillary rules a record leaves out, or false matches. Pattern fixes found by this check:
- the curly apostrophe in "Fisher’s";
- "t -test" with a stray space;
- "viral suppression";
- "RT-PCR" read as a reproduction number.

The vol 71 scores above are unchanged by this layer.

## Sizing run, vols 70–48 (1999–2021; added 2026-11-03)

*Counts only, with no accuracy claim for these volumes. No API calls.*

**Frame (owner, 11-03):** the report family plus QuickStats, as for vols 73–71. Before 2016 the extraction labels reports `report` rather than `full_report`, and also uses `update`, `dispatch` and `brief_report`. Those four are in the frame. Announcements and notices to readers are out, as before.
- Packets: `python3 tools/build_packets.py vol V --skip-no-id --types full_report,notes_from_the_field,vital_signs,outbreak_report,grand_rounds,quickstats,report,update,dispatch,brief_report`.
- `--skip-no-id` is new. It counts and skips corpus rows whose URL has no article id: 81 numeric pages in 1999 and 16 special pages in 2002–2011. None of these is in the extraction's views anyway. Without the flag, the builder still stops on them.
- Then `python3 tools/surface_rules.py size` writes `reports/surface_sizing.tsv` (per volume, then per period) and `reports/surface_sizing_findings.tsv` (declared findings per period). Periods break at 2015/2016, where production changed. Every volume rolls up to contiguous years.

**Articles in frame** (holdout and articles missing from the views omitted):

| Period | Articles | Equity 3–4 candidates | 2+ method families | Full-read tier* | Light read, all ($0.002 each) | Full read, tier ($0.13 each) |
|---|---|---|---|---|---|---|
| 1999–2015 (vols 48–64) | 3,680 | 183 | 441 | 545 | ~$7 | ~$71 |
| 2016–2021 (vols 65–70) | 1,859 | 221 | 363 | 470 | ~$4 | ~$61 |
| *2022–2024 (vols 71–73, read)* | *761* | *113* | *144* | *199* | | |

\* An equity 3–4 candidate, or two or more method families. The families are regression, a named test, survey weighting, standardization, survival, and a trend model. The tier is an illustration of the arithmetic, not a recommendation; it is the owner's to define.

For 1999–2021, a light read of every article plus a full read of that tier comes to about $143. A full read of all 5,539 articles would be about $720.

**Drift signs: where the rules, and any reader, see less.**
- **No tables before 2009.** The extraction's views carry table files only from 2009, plus a few in 1999–2000. Tables from 2001–2008 were GIF images or `<pre>` text: a placeholder ("Table 1") survives, the content mostly does not (the extraction's `ACCOUNTING.md`, H4). Rules that lean on tables undercount there: equity level ≥ 2 falls from 20–26% of articles (vols 59–73) to 0–12% (vols 57–49). The same gap would limit a model reading these packets.
- **No Summary box before 2009/2010.** It first appears in vol 58 (2009). Rules that read the Summary fall back to the body.
- **No QuickStats in the frame before vol 54 (2005).**
- **Fewer named tests in early volumes,** 4–7% of articles in vols 48–52 against 13–19% from 2009. That may be real practice, older wording, or both; only reference reads can say.
- **Regression appears in about 12% of articles in 1999–2015,** 18% in 2016–2021 and 19% in 2022–2024.

**A proposed older-volume slice** (the plan's §4.4, about $15–16 at $0.13). Draw about 120 articles in three strata set by the drift above: 2001–2008 (text only), 2009–2015 (tables, the old `report` type), and 2016–2021. Within each stratum, take about 20 at random and about 20 that the rules flag (regression, equity candidates, named tests), so the rarer labels can be scored. The drift signs place a break at 2008/2009 that sits inside the owner's 1999–2015 period; all strata are whole volumes, so they still roll up to contiguous years.

## Limits

- **Vocabulary from 2022–2024 only.** Reports from 1999–2010 phrase methods differently, often lack a Summary box, and have fewer CIs. The rules are untested there; the plan's §4.4 calibration slice would test them.
- **Cited methods.** Rules cannot tell an article's own method from one it cites (rule 2), except in guidance reports.
- **Tuned by one reader on one split.** The rules were written against dev misses, and vol 71 was scored once. The drop in the regression family suggests some fitting to dev.
- **For comparison:** pilot 1's structural reader (Claude Haiku 5.5, full brief) had 5–7% field error overall, and 37–47% error on `equity_dimensions` (`reports/RUN_REPORT.md`). That is a different metric on a different sample, but it suggests the rules are not far below a small model on the fields they cover.

## Possible next steps (each the owner's call)

1. **$0:** run the rules over vols 70–48 to see the candidate counts each band and family would produce. This is a sizing exercise only, with no accuracy claim.
2. **About $15:** full-brief reads of about 120 older articles, stratified by period and type, as reference labels. These test the rules on older vocabulary, and they are the plan's §4.4 slice.
3. **Then:** the model reads only what the rules cannot carry (outcome type, depth, design) or are unsure of, either Haiku quoting the methods sentences or a short brief. The full brief goes only to articles selected for teaching.
