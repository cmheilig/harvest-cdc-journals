# Reader brief: index fields for MMWR articles

*Version 3.1 (2026-10-11). It follows the Stage 6b local pilot, the instructor's rulings on it, and a smoke test. Everything you need is in this brief; you do not need to read `SCHEMA.md`.*

You read one MMWR article at a time and write one JSON record for it. The index helps a public health instructor find candidate reports by **how they make claims about data**: summaries and comparisons as well as regression. It also covers condition, design, data source and health equity. An error is bothersome, never fatal. **Justification matters most:** every value you give carries evidence from the article.

## 1. What you are given, per article (`packets/<id>/`)
- **`article.txt`:** the article text. In order: title, date line, Summary, body (`## ` headings), Notes, tables, figures. References are omitted.
- **`tables/*.csv`:** every HTML table cell, numbers included. **For table structure, the CSV is authoritative.** In `article.txt`, the numbers of mostly numeric tables are omitted, and the `Columns:`/`Rows:` lines only approximate the hierarchy.
- **`prepass.json`:** lexicon cues found by a script, with counts by location and one snippet each. These are **seeds, not answers.** Confirm each one, decide whether it is primary or ancillary, and drop false hits. Two known false-hit patterns:
  - "time-weighted average" is not survey weighting;
  - "surveillance sensitivity" is not diagnostic accuracy.

## 2. How to read the text
- **GIF-era images (2000–2009).** They appear as `[Table N shown only as an image]`, followed by "OCR text (… unverified)". OCR words are reliable; its numbers and column alignment are not. Quote OCR text with `loc: "ocr"`.
- **Duplicate figure descriptions.** A description can appear twice: as "Figure description:" and as a paragraph beginning "Alternate Text". Treat them as one.
- **Dates and names.** Use the date line for dates. Ignore any byline or list of names in the body.
- **Mis-nested tables.** Row hierarchy can be flattened or mis-nested. When stratification matters, read the CSV.
- **Problems with the text.** If something looks wrong, say so in `view_issues`. This includes the article contradicting itself.

## 3. Rules
1. **Read methods everywhere.** Methods are often stated only in table notes, figure notes or footnotes.
2. **Name procedures from the estimate and the test, not from the word "regression".**
3. **Record every method present.** Reports often combine methods. That is normal, not a conflict.
4. **Silent is not false.** If the article does not say, give `null` or an empty list. The exceptions are listed under `regression_fields`.
5. **Evidence:** each `ev` item is `{"loc": "title | summary | text | table N | figure N | note | ocr", "q": "verbatim quote, at most 25 words"}`.
   - Give one quote, or two if needed; never more than three.
   - Quote exactly.
   - A table label is valid evidence when quoted with its group, e.g. `Race/ethnicity › Black, non-Hispanic`.
   - An unnumbered table or figure is `table 1` / `figure 1`.
6. **Keep it lean.** No prose outside the JSON. `notes` has at most 3 sentences.

## 4. Fields

### `data_focus` (first)
Does the article produce **quantities about public health outcomes, or comparisons of them**?
- **yes:** it does.
- **limited:** a single case or a small case series with context; or figures only cited from earlier reports to frame a recommendation.
- **none:** no such quantities. Examples: an announcement, a notice, a cover box that only points to other reports, a guidance statement with no data.

If `none`, fill only `data_focus`, `public_health_question`, `condition_group`, `setting`, `equity` (level 0 unless the article frames equity) and the metadata. Leave the method fields empty or null: `outcome_types` [], `analytic_depth` [], `procedures` [], `comparison` with framework "none", `inference` "none", `regression_fields` null.

### A. `outcome_types` (list)
- **continuous:** means, medians, quantiles of a measured quantity.
- **binary/categorical:** counts and proportions.
- **count/rate:** rates per population or person-time, incidence, counts modelled as counts.
- **event-time:** time to an event, survival, Kaplan-Meier, hazards.

List the types the article's **claims** rest on. A count outcome fitted with a linear model is still count/rate: the procedure records the linear model.

### B. `analytic_depth` (list)
- **summary:** one group or one quantity.
- **comparison:** two or more groups or periods compared, **or a comparison with a fixed referent** (a target, a threshold, or a population value treated as fixed). A side-by-side comparison with no computed contrast is still `comparison`, with `scale` null.
- **stratified/adjusted:** a comparison within levels of a third variable, or an adjusted one. Adjustment includes standardization, weighting to adjust, a model, or **matching treated as stratification** (e.g. conditional OR).
  - A series over time by group is a comparison, not stratification.
  - Survey weighting that only yields population estimates does not make the depth stratified.

### C. `procedures` (list of objects)
`{"name", "class", "regression_family", "inferred_from", "role", "alternative_reading"?, "ev"}`
- **Classes:**
  - **test:** a named test, or a judgment that a CI includes or excludes a fixed value such as 1, 0 or a target (name it "CI vs fixed value"). An unnamed test implied by "significantly" or by p-values is `"name": "unnamed test"`.
  - **regression:** use the family table below.
  - **standardization:** direct, indirect, SMR.
  - **weighting:** survey weights, IPTW, raking, poststratification.
  - **transformation:** nonlinear transformations other than quantization, e.g. log, BMI-for-age percentiles or z-scores, smoothing (LOESS, splines drawn as trends, moving averages, seasonal decomposition such as STL); and **censoring or suppression** of observed values.
  - **other:** Kaplan-Meier, life tables, the screening method, resampling or bootstrap inference ("bootstrap" rather than a test name), diagnostic-accuracy measures (sensitivity, specificity, predictive values), or a model not otherwise classifiable.
- **Not procedures** (mention in `notes` only if they matter):
  - design or measurement steps: matching, power calculations, case definitions, case finding, record linkage, data cleaning, exclusion rules, covariate-selection rules;
  - arithmetic: rates or ratios computed by hand, VE = 100 × (1 − OR), percent change;
  - quantization: recoding, dichotomizing, cutpoints;
  - affine rescaling;
  - **normalization to rates or densities** (per 100,000, per person-time, per 1,000 kcal), because these are measures;
  - **laboratory methods**, unless they produce or qualify the analyzed quantity, e.g. Ct values analyzed as an outcome, or an assay's accuracy as the subject of the report. A lab test that only confirms case status is not a procedure.
  - A p-value for a model term is part of the model, not a separate test. CI overlap is not a test (note it under `inference`).
- **Regression families.** Make the link explicit in `inferred_from`:

| The report says | regression_family |
|---|---|
| least squares, linear regression, OLS, difference-in-difference with a linear model | linear |
| logistic, conditional logistic, GEE or mixed logistic | logistic |
| Poisson, modified Poisson, quasi-Poisson, negative binomial, log-binomial | loglinear |
| Cox, proportional hazards, Andersen-Gill, Fine-Gray, Weibull | event-time |
| joinpoint, segmented regression | state the family with a reason |
| robust or periodic (Serfling-type) regression, or a "regression" not otherwise named | other, with a reason |

- **alternative_reading** is optional, never required. Offer it only when a second family is genuinely defensible, e.g. least-squares on a log rate.
- **A regression used only for CIs or tests** (e.g. Poisson for coverage CIs) is a regression with role `ancillary`. Fill `regression_fields` with what is stated, null elsewhere.
- **role:** `primary` if the procedure carries the main claims. Examples: the tests behind the contrasts the text reports; survey weights that produce every estimate; a CI judged against a fixed value that underpins the headline claims. Otherwise `ancillary`, e.g. a chi-square comparing Table-1 characteristics.

### D. `comparison` (object)
- **framework:**
  - none;
  - fixed referent: a target, a threshold, 100% coverage, or a national or population estimate treated as fixed;
  - pair: two groups or periods;
  - group: three or more.

  When pair and group comparisons coexist, use the primary contrast.
- **scale:** absolute (differences, percentage points) · relative (ratios, APCs, percent change) · both · **null when values are only set side by side** with no difference or ratio computed.
- **referent:** as named, or null.
- **across:** the variable(s) compared.

### E. `inference`
- **v:** none · CIs · tests · CIs and tests.
  - A CI judged against a fixed value counts as a test.
  - CI overlap does not; record "CIs" and mention the overlap rule in `notes`.
- **method_stated:** the article says how the CIs or tests were computed, e.g. "exact binomial", "chi-square".

### `primary_measures` (list)
The article's own labels, e.g. "percentage", "rate per 100,000", "aOR", "VE", "APC", "SMR", "sensitivity".

### `regression_fields`
`null` unless `procedures` contains a regression. Each field is `{"v", "ev"}`.
- **nonindependence_approach:** GEE · mixed effects · clustered or robust SE · conditional (matched) · complex survey design · serial correlation · none.
  - Modified Poisson counts as robust SE.
  - "conditional (matched)" only when the model is stated to be conditional.
  - A national complex survey (NHIS, NHANES, BRFSS, YRBS, NIS) analyzed with survey software counts as "complex survey design" without further evidence.
- **unadjusted_results:**
  - true if the article reports an unadjusted analysis of the same exposure–outcome contrast: a crude ratio, or a bivariate comparison with a measure or test;
  - true also when the model itself has no covariates;
  - rates set side by side with no crude ratio are false.
- **adjusted_results:** true only for covariate adjustment or standardization within the model. A matched or conditional OR with no other covariates is `false`, with a note: matching is stratification.
- **confounders:** as listed. Use `v: null` when the model has no covariates. `nonindependence_approach: none` may have `ev: []`.
- **interaction_modeled:** a formal interaction term or test. This includes a test that a slope, APC or effect differs between groups or periods, joinpoint permutation tests, and a difference-in-difference term.
- **sensitivity_analysis:** a re-analysis on a subset, under an alternative definition or window, or with an alternative model; also a negative-control or bias check.
- **Defaults:** "not reported" means `false` for unadjusted, interaction and sensitivity. Use `null` only when the methods are too sketchy to tell.

### Context
- **public_health_question:** one sentence, with `ev`.
- **condition_group** (list):
  - **Values:** COVID · influenza · mpox · RSV · HIV · TB/respiratory · STI · vaccine-preventable (other) · diabetes · cardiovascular · cancer · obesity/nutrition · maternal/reproductive · mental health · substance use · injury/poisoning · environmental · health behaviors · other acute · other chronic.
  - **What goes in:** exposure and outcome conditions only.
  - **What stays out:** a condition that only defines a control group, or appears only as a model risk factor. The pandemic period does not make a report COVID, whether it is background or a period being compared (pre-pandemic vs pandemic). List COVID only when COVID infection or disease is an exposure or outcome.
  - **Empty** if nothing fits.
- **study_design** (list): cross-sectional · retrospective cohort · prospective cohort · case-control · test-negative · screening method · before/after or trend · ecologic · outbreak investigation · surveillance/descriptive · diagnostic accuracy · guidance or recommendations · case report/series · cost/economic · mixed/other.
  - Two time points compared count as before/after.
  - **Ecologic** means the unit of analysis is a group, e.g. counties or schools.
  - Record a cohort as retrospective or prospective only when the article says so, or the data are clearly existing records.
- **data_source_type** (list): CDC surveillance system · state or local surveillance · national survey · other survey · registry · electronic health records · administrative/claims · vaccination registry · sentinel/network · international surveillance · record abstraction or interview · other.
  - **List only the records analyzed.** Not denominators (Census, population estimates), not external estimates cited for comparison, not lists used only to build a roster.
  - **Common cases:**
    - NHIS, NHANES, BRFSS, YRBS, NIS: national survey;
    - NNDSS, NVSS: CDC surveillance system (NVSS can also be registry; either is acceptable);
    - COVID-NET, FluSurv-NET, VE networks: sentinel/network;
    - WHO and other international data: international surveillance;
    - a field investigation's charts and interviews: record abstraction or interview.
  - The jurisdictions feeding a national CDC system are not listed separately unless the article analyzes a state system's own data.
  - Use `registry` also for linkage to vital records such as the National Death Index.
  - Non-CDC national data systems go under `other`, e.g. NPDS poison-center calls, or under `administrative/claims` when they are claims.
- **data_source_named** (list): as named in the article: systems, surveys, databases. Not states.
- **setting:**
  - **US national:** national data or a nationwide sample.
  - **US subnational:** states, cities, counties, multistate networks.
  - **international**
  - **mixed:** national and subnational results reported together.
- **vaccine_outcome_type:** `{"v", "ev"}`, **only when the article quantifies vaccine epidemiology** (coverage or uptake, effectiveness, safety); otherwise `null`. Values: effectiveness · coverage/uptake · safety · other.
  - Trends in a vaccine-preventable disease alone do not count.
  - Classify from the primary question.

### Equity (`equity` object)
**A social-position dimension (SPD)** is a group characteristic associated with power, privilege or social position.

| Dimension | Notes |
|---|---|
| race/ethnicity | |
| SES | One value. Covers education, income, wealth, poverty, insurance as SES, area indexes (SVI, ADI, HPI), housing status (homelessness), incarceration. Name up to 3 `ses_components` as the article words them. Occupation and industry count as SES **only when framed as social position**, e.g. "unemployed". Occupation as a route of exposure (health care personnel, workplace hazards) is not SES. |
| rurality | |
| age | Recorded always, but it raises the level only when framed as social position or crossed with another SPD (e.g. children in poverty, older adults in congregate settings). |
| sex | Same rule as age. |
| sexual orientation and gender identity | MSM as an HIV transmission category is not SOGI unless framed as such. |
| disability | Only when framed as a social position or as access; not as a clinical condition. |
| country of birth/language/nativity | |
| other | |

**Not SPDs:**
- geography by itself (state, county, region, jurisdiction, policy status);
- clinical status;
- vaccination status;
- period.

An international setting is a characteristic, not an equity reading.

**`dimensions`:** `{"v", "role", "ses_components"?, "ev"}`.
- **Roles:** population · covariate · descriptor · stratifier · primary comparison · cross-classifier. One entry per role.
- **A dimension named only in a limitation** (e.g. "race was not available") gets the role `descriptor` and does not raise the level.

**`level`** (0–4): the highest level the article meets.

| Level | Meaning |
|---|---|
| 0 | No SPD; or SPDs only define the population; or **age or sex only** (including age standardization, age-group strata and single Table-1 counts) without social-position framing. |
| 1 | An SPD other than age or sex appears only as a covariate, matching or weighting variable, or descriptor; or equity is raised only as a limitation or recommendation. Race/ethnicity as a confounder in a VE model is level 1. |
| 2 | Group-specific results by an SPD are reported and described, but not framed as a disparity. **Capped at 2:** contrasts shown only with significance marks, values only set side by side (even with framing words), or an SPD contrast that appears only in an interpretive table (group estimates with CIs or tests) and is not discussed. |
| 3 | A disparity framed, **quantified** (a computed difference, ratio, or shortfall from a target) and interpreted; causes absent or generic. **In reports whose exposures are themselves social determinants** (SDOH or HRSN measures, area indexes as stratifiers), quantified contrasts across them meet level 3 without the word "disparity". |
| 4 | Level 3, plus specific social, structural or **political** determinants (policies, laws, governance) named as explanations, or measured and analyzed. Determinants that are the analyzed exposure meet level 4. Level 4 requires level 3. |

**The other equity fields:**
- **`level_reason`:** one or two sentences.
- **`level_confidence`** (high · medium · low) and **`level_alternative`** (`{"level", "reason"}`, or null): **required for the judgment reader (role "judgment")**; optional for others.
  - Use `medium` or `low` when a rule boundary decides the level.
  - Give the next most defensible level and why.
- **`determinants_of_health`:** the article's words for social, structural or political determinants, recorded even when the level is below 4. Excluded:
  - a factor named only as an unmeasured confounder;
  - a recommendation, e.g. "expand insurance coverage", unless the article also names that factor as a cause of the disparity.
- **`equity_action`:** true when the article evaluates an intervention, policy or program in relation to a disparity, or measures progress against a target or as an explicit change in the gap. Monitoring alone is false.
- **`equity_measurement`:** one short note (referent, proxy, groups collapsed or missing), or null.
- **Check:** level ≥ 2 needs an SPD in `comparison.across`. Level ≥ 3 needs a quantified contrast.

### Metadata
- **`reader`:** `{"model": "<your model id>", "reader_id": "<given>", "role": "structural" | "judgment"}`.
- **`confidence`:** for the record as a whole.
- **`notes`** (at most 800 characters), **`view_issues`** (at most 400), **`level_reason`** (at most 400): short, or null.

## 5. Output (full record)
Write one JSON object per article to the path in your task. Then run `python3 tools/validate.py <that file>` (that file only) and fix any FAIL. Read **at least two** examples in `examples/` before your first article.

## 6. Review mode (judgment reader, arm B only)
In arm B, the judgment reader receives the structural reader's record (`out/haiku/<id>.json`) as well as the packet.
- **If `data_focus` is `none`** and you agree with it, write `{"id", "reader", "agree_data_focus_none": true}` and stop.
- **Otherwise** write `out/opus_review/<id>.json`:
```json
{"id": "...", "reader": {"model": "...", "reader_id": "R3", "role": "judgment"},
 "changes": [{"field": "<top-level field, e.g. study_design>", "value": <the full replacement value, in schema form>, "why": "≤2 sentences"}],
 "equity": { <a complete equity object, including level_confidence and level_alternative> },
 "confidence": "high|medium|low", "notes": null}
```
- A `changes` value is the **whole** replacement for that top-level field, e.g. the full `procedures` list even if only one role changes.
- **Always** give the complete `equity` object, in the same shape as the examples. `level` has no `ev`; the evidence sits in `dimensions` and `determinants_of_health`. Give `changes` only for top-level fields you would set differently. Check the judgment fields especially: `analytic_depth`, `comparison`, `study_design`, `procedures` (roles and classes), `regression_fields`, `data_source_type`.
- **Merge:** `tools/merge_review.py` applies the changes to the structural record and validates the result.
