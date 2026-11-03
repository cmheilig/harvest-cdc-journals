# Surface rules for a first-pass index (2026-11-02)

*Claude's analysis, for the owner's review. The rules and sizing made no API calls; the slice and the light-read test (last sections) were run by the owner on the Claude API, 2026-11-03.*

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
- **Tables before 2009 are words without structure.** The extraction's table CSVs start in 2009. Tables from 2000–2009 were GIF images; the extraction OCR'd 1,784 of the 1,789 GIF tables and boxes (`PILOT_GIF.md`, `gif/PILOT_REPORT.md`, `DECISIONS.md` #4b). The views carry that text below each placeholder as "OCR text (tesseract or Apple Vision, unverified)". 1999 used `<pre>` tables in the text.
  - **Words:** the pilot found label words reliable (median recall 0.97).
  - **Structure and cells:** not reliable (median numeric-cell recall 0.82, and lower in 2005–2009).
  - **Coverage in the packets:** 19–65% of articles in vols 49–57 carry OCR text. The rules now read those lines as table text (11-03); the vol 71–73 scores are unchanged.
  - **The equity band barely moved** after that change, and after accepting plural race terms ("blacks", "whites"). Equity level ≥ 2 stays at 20–26% of articles in vols 59–73 and 0–12% in vols 57–49. That gap is either older practice or older phrasing the rules miss; the slice can tell which.
- **No Summary box before 2009/2010.** It first appears in vol 58 (2009). Rules that read the Summary fall back to the body.
- **No QuickStats in the frame before vol 54 (2005).**
- **Fewer named tests in early volumes,** 4–7% of articles in vols 48–52 against 13–19% from 2009. That may be real practice, older wording, or both; only reference reads can say.
- **Regression appears in about 12% of articles in 1999–2015,** 18% in 2016–2021 and 19% in 2022–2024.

**A proposed older-volume slice** (the plan's §4.4, about $15–16 at $0.13). Draw about 120 articles in three strata set by the drift above: 2001–2008 (text only), 2009–2015 (tables, the old `report` type), and 2016–2021. Within each stratum, take about 20 at random and about 20 that the rules flag (regression, equity candidates, named tests), so the rarer labels can be scored. The drift signs place a break at 2008/2009 that sits inside the owner's 1999–2015 period; all strata are whole volumes, so they still roll up to contiguous years.

## The older-volume slice: the rules before 2022 (added 2026-11-03)

**The run.** 120 articles were drawn by `surface_rules.py sample` (seed 20261103; `reports/older_slice_sample.tsv`). Each of three strata (1999–2008, 2009–2015, 2016–2021) has 20 random articles and 20 the rules flagged (7 regression, 7 equity 2+, 6 named test). They were read with brief v5.2 on Opus 5.5 at effort medium, as for production, under the volume key `older-slice`. All 120 are valid (104 on the first read, 16 after one fix-up), for **$15.03** against an estimate of $14.64.

**Scoring** (`surface_rules.py score-slice` → `reports/surface_scores_slice.tsv`). The flagged draws are rule-positive by construction. So precision uses every article where a rule fired, and recall uses only the 20 random articles per stratum. Recall therefore rests on very few articles, often 1–5: read it for direction only.

| Label | 1999–2008 P (n) | R (n) | 2009–2015 P (n) | R (n) | 2016–2021 P (n) | R (n) |
|---|---|---|---|---|---|---|
| any regression | 0.94 (17) | 1.00 (4) | 1.00 (11) | 1.00 (2) | 0.86 (14) | 1.00 (4) |
| CIs | 1.00 (18) | 1.00 (5) | 0.94 (17) | 1.00 (4) | 0.88 (25) | 1.00 (10) |
| tests | 1.00 (14) | 1.00 (1) | 1.00 (20) | 0.75 (4) | 1.00 (27) | 0.90 (10) |
| weighting | 1.00 (13) | 0.75 (4) | 1.00 (10) | 1.00 (2) | 1.00 (13) | 1.00 (6) |
| standardization | 0.92 (12) | 1.00 (2) | 0.90 (10) | 1.00 (1) | 1.00 (6) | 1.00 (2) |
| groupwise | 1.00 (8) | 0.33 (3) | 0.95 (21) | 0.75 (4) | 0.89 (27) | 0.92 (12) |
| equity ≥ 2 | 1.00 (8) | 0.33 (3) | 0.88 (16) | 0.00 (1) | 0.96 (24) | 0.92 (12) |
| count/rate outcome | 0.71 (14) | 0.27 (11) | 0.52 (23) | 0.88 (8) | 0.55 (20) | 0.83 (6) |

- **Named methods hold in every period.** When a rule for regression, CIs, tests, weighting or standardization fires, the reference read agrees 86–100% of the time, as on vol 71.
- **Equity and groupwise recall fall before 2009.** In 1999–2008, the rules caught 1 of 3 random articles at level 2 or more. So the low early equity band in the sizing is at least partly the rules missing older phrasing, not only older practice. The 3 articles are too few to say how much.
- **Reference levels in the random draws:** 3 of 20 at level 2 or more in 1999–2008, 1 of 20 in 2009–2015, and 12 of 20 in 2016–2021. Only one article in all 60 reached level 3 or more.
- **Count/rate is the weakest rule** (recall 0.27 in 1999–2008). The light read below does much better on it.

## The light-read test: Haiku 5.5 on vols 71–73 (added 2026-11-03)

**The run.** `tools/light_read.py` with `LIGHT_BRIEF.md` (light-1.0) on Claude Haiku 5.5, effort medium, batch: 761 articles for **$0.95** ($0.0012 per article). 563 records passed every check. 198 failed at least one. Their values are kept and are compared below.

**Why records failed.** The failures are almost all form, not content (`out/light/claude-haiku-5-5/`):
- 186 quotes over 25 words: 57 in `data_focus` evidence, 55 in depth, 42 in methods sentences, 32 elsewhere;
- 59 section names used as locations ("methods", "discussion", "source");
- 35 quotes not found in the article. These are the substantive failures: paraphrase, or text stitched together.

**Agreement with the Opus records** (`light_read.py compare` → `reports/light_vs_opus_claude-haiku-5-5.tsv`; precision and recall, taking Opus as the reference):

| Field or value | Haiku P | Haiku R | Rules on vol 71, P / R |
|---|---|---|---|
| data focus (exact agreement) | 725 of 761 (95%) | | |
| binary/categorical | 0.97 | 0.98 | |
| count/rate | 0.87 | 0.96 | 0.61 / 0.87 |
| continuous | 0.69 | 0.75 | 0.17 / 0.84 |
| event-time | 0.79 | 0.92 | |
| depth: comparison | 0.95 | 0.92 | |
| depth: stratified/adjusted | 0.90 | 0.91 | 0.78 / 0.77 |
| depth: summary | 0.97 | **0.24** | |
| design: cross-sectional | 0.89 | 1.00 | 0.54 / 0.90 |
| design: test-negative | 0.94 | 1.00 | 0.82 / 0.88 |
| design: outbreak investigation | 0.85 | 0.93 | 0.68 / 0.82 |
| design: evaluation or monitoring | 0.79 | 0.92 | 0.59 / 0.54 |
| design: before/after or trend | 0.74 | 0.98 | 0.63 / 0.54 |
| design: surveillance/descriptive | 0.84 | 0.88 | |
| design: review or synthesis | 1.00 | 0.72 | 0.52 / 0.85 |
| design: ecologic | **0.39** | 1.00 | |

- **On the fields the rules cannot carry, Haiku is far better than the rules,** at about a tenth of a cent per article.
- **Three systematic patterns**, each fixable in the brief:
  1. **Depth "summary" is mostly left out (recall 0.24).** Haiku lists the highest depth only; Opus lists summary beside comparison when single-group quantities are presented as results.
  2. **Ecologic is over-called (precision 0.39)** on reports that give rates by state or county. The brief's definition (the unit of analysis is a group) stands; Haiku needs an example of what is not ecologic.
  3. **Before/after or trend is somewhat over-called (precision 0.74).**

**Proposed light-1.1, for the owner's decision.** Wording only, with no new fields:
- methods sentences become clauses of at most 25 words;
- `loc` names where the quote sits (`text`, `note`, `table N`), never the section heading;
- depth lists every level that applies, with summary beside comparison;
- one sentence on ecologic: rates set side by side by state or county are surveillance, not ecologic, unless the places are the units of an analysis.

A retest on the same 761 articles costs about $1. Adding the 120 slice articles (about $0.15) would also show how Haiku reads older volumes, against the slice's Opus records.

## Limits

- **Vocabulary from 2022–2024 only.** Reports from 1999–2010 phrase methods differently, often lack a Summary box, and have fewer CIs. The rules are untested there; the plan's §4.4 calibration slice would test them.
- **Cited methods.** Rules cannot tell an article's own method from one it cites (rule 2), except in guidance reports.
- **Tuned by one reader on one split.** The rules were written against dev misses, and vol 71 was scored once. The drop in the regression family suggests some fitting to dev.
- **For comparison:** pilot 1's structural reader (Claude Haiku 5.5, full brief) had 5–7% field error overall, and 37–47% error on `equity_dimensions` (`reports/RUN_REPORT.md`). That is a different metric on a different sample, but it suggests the rules are not far below a small model on the fields they cover.

## Possible next steps (each the owner's call)

1. **$0:** run the rules over vols 70–48 to see the candidate counts each band and family would produce. This is a sizing exercise only, with no accuracy claim.
2. **About $15:** full-brief reads of about 120 older articles, stratified by period and type, as reference labels. These test the rules on older vocabulary, and they are the plan's §4.4 slice.
3. **Then:** the model reads only what the rules cannot carry (outcome type, depth, design) or are unsure of, either Haiku quoting the methods sentences or a short brief. The full brief goes only to articles selected for teaching.
