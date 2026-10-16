# Reader brief: index fields for MMWR articles

*Version 5 (2026-10-16). It follows pilot 2 (100 articles, two Opus reads each) and the instructor's decisions on it (`BRIEF_V5_DECISIONS.md`, V1–V18), on top of v4 (`BRIEF_V4_DECISIONS.md`, D1–D20). Everything you need is in this brief; you do not need to read `SCHEMA.md`.*

You read one MMWR article at a time and write one JSON record for it. The index helps a public health instructor find candidate reports by **how they make claims about data**: summaries and comparisons as well as regression. It also covers condition, design, data source and health equity. An error is bothersome, never fatal. **Justification matters most:** every value you give carries evidence from the article.

## 1. What you are given, per article (`packets/<id>/`)
- **`article.txt`:** the article text. In order: title, date line, Summary, body (`## ` headings), Notes, tables, figures. References are omitted.
- **`tables/*.csv`:** every HTML table cell, numbers included. **For table structure, the CSV is authoritative.** In `article.txt`, the numbers of mostly numeric tables are omitted, and the `Columns:`/`Rows:` lines only approximate the hierarchy.
- **`prepass.json`:** lexicon cues found by a script, with counts by location and one snippet each. These are **seeds, not answers.** Confirm each one, decide whether it is primary or ancillary, and drop false hits. Known false-hit patterns:
  - "time-weighted average" is not survey weighting;
  - "surveillance sensitivity" is not diagnostic accuracy;
  - place names such as "Yates County" are not the Yates correction;
  - "Pearson correlation" is not a Pearson chi-square test.

## 2. How to read the text
- **GIF-era images (2000–2009).** They appear as `[Table N shown only as an image]`, followed by "OCR text (… unverified)". OCR words are reliable; its numbers and column alignment are not. Quote OCR text with `loc: "ocr"`.
- **Duplicate figure descriptions.** A description can appear twice: as "Figure description:" and as a paragraph beginning "Alternate Text". Treat them as one.
- **Dates and names.** Use the date line for dates. Ignore any byline or list of names in the body.
- **Mis-nested tables.** Row hierarchy can be flattened or mis-nested. When stratification matters, read the CSV.
- **Problems with the text.** If something looks wrong, say so in `view_issues`. This includes the article contradicting itself.

## 3. Rules
1. **Read methods everywhere.** Methods are often stated only in table notes, figure notes or footnotes.
2. **Before writing `procedures`, read every table and figure note.** A model named only in a Table 1 note was pilot 2's largest single miss.
3. **Name procedures from the estimate and the test, not from the word "regression".**
4. **Record every method present.** Reports often combine methods. That is normal, not a conflict.
5. **Silent is not false.** If the article does not say, give `null` or an empty list. The exceptions are listed under `regression_fields`.
6. **Evidence:** each `ev` item is `{"loc": "title | summary | text | table N | figure N | note | ocr", "q": "verbatim quote, at most 25 words"}`.
   - Give one quote, or two if needed; never more than three.
   - Quote exactly. Count the words before saving: quotes over 25 words fail validation.
   - A full report's Abstract is quoted with `loc: "summary"`.
   - A table label is valid evidence when quoted with its group, e.g. `Race/ethnicity › Black, non-Hispanic`.
   - An unnumbered table or figure is `table 1` / `figure 1`.
7. **Value objects.** Every field shaped `{"v", "ev"}` keeps both keys. An empty one is `{"v": null, "ev": []}` (e.g. `"determinants_of_health": {"v": null, "ev": []}`); `equity_action` is `{"v": false, "ev": []}` when false.
8. **Keep it lean.** No prose outside the JSON. `notes` has at most 3 sentences.

## 4. Fields

### `data_focus` (first)
Does the article produce **quantities about public health outcomes, or comparisons of them**?
- **yes:** it does. This includes estimates the report computes itself from others' data, e.g. a work group's pooled estimates, weighted summaries, or new tables computed from trial data.
- **limited:** a single case, or a case series that only describes its cases; or figures only cited from earlier reports, even when tabulated with CIs (e.g. an ACIP evidence summary that computes nothing of its own).
- **Size does not decide it; analysis does.** A case series or cluster of any size is `yes` when it reports its own counts or proportions **and** compares them (groups, periods, attack rates, exposed vs unexposed).
- **Cited vs own.** When the headline quantities are cited from other reports, the article is `limited`, even if it adds a few counts of its own.
- **Program and system measures** (coverage, doses delivered, adoption, timeliness, completeness) are quantities about public health when the report computes them from its own data.
- **none:** no quantities at all. Examples: an announcement, a cover box that only points to other reports, a schedule or guidance statement with no data. **A notice that cites rates or a change from earlier reports is `limited`, not `none`.**

If `none`, fill only `data_focus`, `public_health_question`, `conditions`, `setting`, `study_design` (e.g. `guidance or recommendations` for a schedule), `equity` (level 0 unless the article frames equity; still record age and sex that define the population) and the metadata. Leave the method fields empty or null: `outcome_types` [], `analytic_depth` [], `procedures` [], `comparison` with framework "none", `inference` "none", `regression_fields` null, `groupwise` [].

If `limited`, method fields describe **only what the article itself computes or reports as its own result**. Methods behind cited estimates (e.g. a manufacturer's hazard ratios) are not procedures; mention them in `notes` if they matter.
- A single case or a descriptive case series: `outcome_types` from the counts or measures reported; `analytic_depth` `summary`; `procedures` []; `comparison` framework "none" unless the article compares something itself.
- A report of cited figures only: `outcome_types` [] and `analytic_depth` [], as for `none`.

### A. `outcome_types` (list)
- **continuous:** means, medians, quantiles of a measured quantity.
- **binary/categorical:** counts and proportions.
- **count/rate:** rates per population or person-time, incidence, counts modelled as counts.
- **event-time:** time to an event, survival, Kaplan-Meier, hazards.

List the types the article's **claims** rest on. A count outcome fitted with a linear model is still count/rate: the procedure records the linear model.
- **Risk vs rate.** A risk or proportion is unitless (numerator and denominator have the same dimension, e.g. persons with disease per person): binary/categorical. A rate has a unit (e.g. cases per person-time, per year): count/rate.
- Event counts per period, place or group with no denominator (cases by year, deaths by state): count/rate. A cumulative risk reported as %: binary/categorical.
- **A case total that only serves as the denominator of percentages does not add count/rate.**
- Descriptive medians of population characteristics (e.g. the age of cases) do **not** make an outcome continuous. Medians of the illness's own course (incubation period, duration) do, when a claim rests on them.
- A single descriptive mean or median makes an outcome continuous only if it is compared or interpreted. A score analyzed as a mean (e.g. a 0–3 composite) is continuous.
- Dollar outcomes and model-derived parameters (doubling time, growth rate): continuous.

### B. `analytic_depth` (list)
- **summary:** a single-group quantity **presented as a result**, e.g. an overall prevalence, a national percentage, a seroresponse range. Sample or enrollment sizes, and the reference value of a contrast, do not earn it.
- **comparison:** two or more groups or periods compared, **or a comparison with a fixed referent** (a target, a threshold, or a population value treated as fixed). A side-by-side comparison with no computed contrast is still `comparison`, with `scale` null.
- **stratified/adjusted:** a comparison within levels of a third variable, or an adjusted one. Adjustment includes standardization, weighting to adjust, a model, or **matching treated as stratification** (e.g. conditional OR).
  - A series over time by group is a comparison, not stratification. Period-to-period changes computed within levels of a group variable are such a series: `comparison`.
  - `stratified/adjusted` needs a contrast of interest estimated within levels of a third variable, or adjustment. The authors' word "stratified" for a two-way cross-tab does not decide it.
  - Survey weighting that only yields population estimates does not make the depth stratified.
- **Not comparisons:** case-confirmation steps (e.g. a fourfold rise in paired sera), classification cutpoints (e.g. a SNP-distance relatedness rule), the reference standard in a diagnostic-accuracy study, duplicate checks between data sources, and compliance or inspection counts in an outbreak report.

### C. `procedures` (list of objects)
`{"name", "class", "regression_family", "inferred_from", "role", "alternative_reading"?, "ev"}`
- **Classes:**
  - **test:** a named test, or a judgment that a CI includes or excludes a fixed value such as 1, 0 or a target (name it "CI vs fixed value"). An unnamed test implied by "significantly" or by p-values is `"name": "unnamed test"`.
    - **"Significantly" with no test anywhere.** It implies an unnamed test only when the article elsewhere reports p-values, CIs or a testing method. A colloquial "significantly" in a report with none of these is not a test; note it in `view_issues`.
    - **CIs alongside p-values.** Record the named test. Add a CI-inversion test only when a conclusion rests on the CI itself.
    - **Tests by CI inversion.** A CI that excludes (or includes) the null value tests that hypothesis by inversion, even when the article does not say so. Record `"name": "CI vs fixed value (inversion)"` when a CI for a contrast with a defined null (1 for ratios, 0 for differences and VE, or a stated target) is reported **and the article draws a conclusion from it** (an effect, a difference, protection, "no difference", "similar"). A CI reported with no conclusion is estimation only.
    - **The inferred method follows the CI construction.** When the article names or describes how the CI was built (Wald, exact or Clopper-Pearson, Wilson, profile likelihood, Taylor series, bootstrap, or model-based from a named regression), put it in `inferred_from` and set `inference.method_stated` true. Otherwise write "CI method not stated" in `inferred_from`. This is common in MMWR and is not an error.
    - **CI overlap assertions.** A claim that rests on overlapping or non-overlapping CIs of two separate estimates is recorded as a test **named "CI overlap assertion"**, because the article reports it as one. Also set `inference.ci_overlap` true, so these can be told apart from formal tests. A methods line stating such a rule, with no claim resting on it, is neither.
    - **Bayesian probabilities against a fixed value** (e.g. P(Rt > 1) binned into trend categories) are quantization of a model output, not a test.
  - **regression:** use the family table below.
  - **standardization:** direct, indirect, SMR.
  - **weighting:** survey weights, IPTW, raking, poststratification. Record it only when the article states it (weights, weighted estimates, survey design, survey software); do not infer it from the data source. Weighted means or medians of summary values (e.g. a median weighted by jurisdiction counts) are arithmetic, not weighting.
  - **transformation:** nonlinear transformations other than quantization, e.g. log, BMI-for-age percentiles or z-scores, smoothing (LOESS, splines drawn as trends, moving averages, seasonal decomposition such as STL); and **censoring or suppression** of observed values, including withholding imprecise or small-cell estimates (CI-width or relative-SE rules, small-cell suppression). Spline terms for covariates inside a model are part of the model, not a transformation.
  - **other:** Kaplan-Meier, life tables, the screening method, resampling or bootstrap inference ("bootstrap" rather than a test name), diagnostic-accuracy measures (sensitivity, specificity, predictive values), correlation coefficients (Pearson, Spearman; a test of a correlation is a `test`), multiple or model-based imputation whose values enter the estimates, or a model not otherwise classifiable.
- **Not procedures** (mention in `notes` only if they matter):
  - design or measurement steps: matching, power calculations, case definitions (including lab proxies and linkage that decide case status, e.g. SGTF for a variant, phylogenetic linkage), case finding, record linkage, data cleaning (including a single fixed fill-in of missing values), exclusion rules, covariate-selection rules;
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
| multinomial or generalized logit | logistic, noting "multinomial" in `inferred_from` |

- **alternative_reading** is optional, never required. Offer it only when a second family is genuinely defensible, e.g. least-squares on a log rate.
- **A regression used only for CIs or tests** (e.g. Poisson for coverage CIs), or only to set a threshold (e.g. a Serfling baseline for excess deaths), is a regression with role `ancillary`. Fill `regression_fields` with what is stated, null elsewhere.
- **Stated model vs reported estimates.** When the stated model contradicts its reported estimates (e.g. a log-binomial "RR" that reproduces ORs), follow the stated model, give the other family as `alternative_reading`, and note it in `view_issues`.
- **role:** `primary` if the procedure carries the main claims. Examples: the tests behind the contrasts the text reports; survey weights that produce every estimate; a CI judged against a fixed value that underpins the headline claims. Otherwise `ancillary`, e.g. a chi-square comparing Table-1 characteristics.

### D. `comparison` (object)
- **framework:**
  - none;
  - fixed referent: a target, a threshold, 100% coverage, or a national or population estimate treated as fixed;
  - pair: two groups or periods;
  - group: three or more.

  - **Which framework leads.** Lead with the framework of the Summary's headline claim. If that does not decide it: group > pair > fixed referent. Always record a target in `referent`, whichever framework leads. (When pair and group comparisons coexist, group leads; the pairwise contrasts are recorded through `procedures` and, for SPD groups, `groupwise.resolves_to_finer`.)
  - **Repeated measurements** over time (one person or one series) are a period comparison: pair for 2 time points, group for 3 or more. Thresholds they are judged against go in `referent`.
  - In an **indicator or nowcast evaluation**, comparing the indicator with later complete data is a comparison (pair or group by period), not a reference standard.
  - `framework` describes the **statistical** comparison, whatever is compared. Whether a comparison is groupwise in the equity sense is a separate question, asked only for groups defined by social position (`groupwise`).
- **scale:** absolute (differences, percentage points) · relative (ratios, APCs, percent change) · both · **null when values are only set side by side** with no difference or ratio computed. Scale follows the reported contrast, not the test: a test with no reported difference or ratio leaves scale null.
- **referent:** as named, or null.
- **across:** the variable(s) compared.

### E. `inference`
- **v:** none · CIs · tests · CIs and tests.
  - A CI judged against a fixed value counts as a test, including a test by CI inversion (see `procedures`). A reported CI used as an inversion test gives "CIs and tests".
  - A **CI overlap assertion** counts as a test too (see `procedures`), flagged with `ci_overlap: true`. If the article says no other method was available, say so in `notes`.
- **ci_overlap** (optional, boolean): true when a claim rests on CI overlap; omit or false otherwise. A methods line stating an overlap rule that no claim uses does not set it.
- **method_stated:** the article says how the CIs or tests were computed, e.g. "exact binomial", "chi-square".

### F. `groupwise` (list)
A result that assesses a difference **across 3 or more groups**, as distinct from one that assesses a difference between 2 groups. The groups are defined on a **social-position dimension** (the SPD table under Equity: race/ethnicity, SES, rurality, age, sex, SOGI, disability, nativity, other), even when the equity level is low. Groupwise results are typically invariant to the choice of referent but can mask subgroup effects. So record anything that **looks** groupwise, and whether it is truly groupwise or **resolves to something finer**. Comparisons across 3 or more non-SPD groups (vaccination status, periods, products, routes, regions, one person's repeated measurements) stay out; they belong in `comparison`, where pair or group describes the statistical comparison only. Empty list if none.

Each entry: `{"across": [<SPD dimension(s)>], "n_groups": <integer ≥ 3, or null if the count is unclear>, "ordered": <bool>, "assessment": <value>, "resolves_to_finer": <bool>, "ev": [...]}`.
- **assessment:** side by side (three or more group values shown or ranked, no inferential assessment across them) · omnibus test (homogeneity: any difference among the groups) · trend test · model joint test · interaction (departure from independence or additivity across two dimensions, not inequality) · pairwise (all pairs) (every pair of groups tested, with no single referent) · per-group vs referent only (the groups are assessed only by tests or ratios against a reference level; this includes each level vs the rest, and a blanket "differences were determined using t-tests" under "higher than most other groups" claims).
- **Precedence** when one result fits several values: model joint test > omnibus test > trend test > pairwise (all pairs) > per-group vs referent only > side by side. Record the first that applies; `resolves_to_finer` captures the finer assessments.
- **Rules:**
  1. Count the groups **actually compared in that result**, after any collapsing, not the levels of the variable. A five-level race variable reported only as Black vs White is pairwise: no entry.
  2. Symmetric tests count whichever margin is the SPD. A chi-square on case vs control by five race groups assesses association across five groups: `omnibus test`, n_groups 5. This is the common hidden case in Table-1 comparisons; check the table to count the categories when the text only says "chi-square tests".
  3. Cross-classified groups count by cells (race × sex with 2 × 2 levels is 4 groups) **only when the result treats the cells as one set of groups to compare**, e.g. ranks them or tests across them. A table that cross-classifies race and sex while the article compares each margin separately (Black vs White; female vs male) is pairwise in each dimension: no entry. Calling the cells groupwise would claim an assessment of homogeneity across all the cells (any difference), which is more than either margin's comparison and different from an interaction.
  4. `ordered` is true for ordered SPD levels (age groups, income or education gradients). `trend test` means a test across those ordered groups (e.g. Cochran-Armitage, chi-square for trend). A trend over calendar time is not groupwise; it becomes groupwise only when time trends are compared across 3 or more SPD groups (e.g. an interaction, or joinpoint APCs compared by group).
  5. `resolves_to_finer` is true when the article also reports pairwise contrasts or per-group estimates against a referent for the same groups (e.g. an omnibus chi-square followed by pairwise t-tests, or ORs for each category). It is also true whenever the assessment is itself `pairwise (all pairs)` or `per-group vs referent only`.
  6. **Composition is not groupwise.** A case series' distribution across SPD groups (shares of cases by race, with no outcome measured per group) is a descriptor, not a groupwise entry. Groupwise needs an outcome measured in each group: a rate, proportion or mean per group.
  7. **An SPD crossed with a clinical variable** (e.g. age × medical risk): count the SPD groups compared within one level of the other variable, and list only the SPD in `across`.
- **Recognition cues** (not a definition): Pearson chi-square on an r × c table; survey-adjusted chi-square (Wald F, Wald chi-square, Rao-Scott, "corrected for survey design"); a joint Wald test of a multi-level covariate; Kruskal-Wallis or ANOVA; Cochran-Armitage or chi-square for trend; an interaction of a multi-level SPD with time or period; multinomial or ordinal outcomes across SPD groups.

### `primary_measures` (list)
The article's own labels, e.g. "percentage", "rate per 100,000", "aOR", "VE", "APC", "SMR", "sensitivity".

### `regression_fields`
`null` unless `procedures` contains a regression. Each field is `{"v", "ev"}`.
- **nonindependence_approach:** GEE · mixed effects · clustered or robust SE · conditional (matched) · complex survey design · serial correlation · none.
  - Modified Poisson counts as robust SE.
  - "conditional (matched)" only when the model is stated to be conditional.
  - A national complex survey (NHIS, NHANES, BRFSS, YRBS, NIS) analyzed in a regression counts as "complex survey design" without further evidence.
  - **Silent:** `v: null`. Use "none" only when the article indicates independent units, or the model plainly has none to account for.
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
- **conditions** (list of `{"v", "role", "ev"}`): the health conditions the article studies: diseases, infections, injuries, substance use, health states (e.g. pregnancy) and health behaviors measured as outcomes (e.g. breastfeeding). **Do not classify them into categories**; a separate pass groups them (`CONDITION_RUBRIC.md`).
  - **v:** the condition named as the article names it, at the level of its title and Summary (e.g. "congenital syphilis", "COVID-19", "systemic lupus erythematosus"). You may infer it from the text (a positive SARS-CoV-2 test → "COVID-19"). Use the common name, not a code.
  - **role:** exposure · outcome · population (it defines who was studied, e.g. "persons with HIV") · vaccine target (the disease a studied vaccine prevents). One entry per condition and role.
  - **What stays out:** a condition that only defines a control group, or appears only as a model covariate or risk factor. The pandemic period does not make a report about COVID-19, whether it is background or a period being compared.
  - **Health care use is not a condition.** Visits, hospitalizations or a care setting (e.g. urgent care) are recorded as the condition that drives them, or not at all when the article names none.
  - **Vaccine adverse events** (reactogenicity, serious adverse events, a named event studied as an adverse event such as myocarditis) are conditions with role `outcome`.
  - Best effort: this field characterizes a report but never sends it to a second reader. Empty if nothing fits.
- **study_design** (list): cross-sectional · retrospective cohort · prospective cohort · case-control · test-negative · screening method · before/after or trend · ecologic · outbreak investigation · surveillance/descriptive · diagnostic accuracy · guidance or recommendations · case report/series · cost/economic · non-randomized trial · evaluation or monitoring · review or synthesis · mixed/other.
  - **Prefer the design the report names** ("evaluation", "case-control", "field trial", "cross-sectional survey"), mapped to the nearest value. Infer a design only when none is named, and say so in `notes`.
  - **evaluation or monitoring:** program, policy or intervention evaluation; evaluation of a surveillance system, indicator or nowcast; monitoring progress or program operations against stated goals or targets. Without goals or targets, reports stay `surveillance/descriptive`.
  - **review or synthesis:** systematic reviews, GRADE and evidence-to-recommendation summaries, and scholarly or narrative reviews or syntheses, including those inside a guidance report.
  - Announcements and awareness notices: `mixed/other`. Do not infer a design from figures cited from other reports.
  - A case series nested inside an outbreak investigation: `outbreak investigation` only.
  - **non-randomized trial:** an intervention assigned by investigators without randomization (e.g. a field test assigned by group), unless the report calls itself an evaluation.
  - Investigations of medication-error or adverse-event clusters: `outbreak investigation`.
  - Two time points compared count as before/after.
  - **Ecologic** means the unit of analysis is a group, e.g. counties or schools.
  - Record a cohort as retrospective or prospective only when the article says so, or the data are clearly existing records.
- **data_source_type** (list): CDC surveillance system · state or local surveillance · national survey · other survey · registry · electronic health records · administrative/claims · vaccination registry · sentinel/network · international surveillance · record abstraction or interview · other.
  - **List only the records analyzed.** Not denominators (Census, population estimates), not external estimates cited for comparison, not lists used only to build a roster.
  - **Common cases:**
    - NHIS, NHANES, BRFSS, YRBS, NIS: national survey;
    - NNDSS, NVSS: CDC surveillance system (NVSS can also be registry; either is acceptable);
    - COVID-NET, FluSurv-NET, VE networks: sentinel/network;
    - non-US national or WHO systems reporting on non-US populations: international surveillance. Classify by where the data come from, not by a WHO label: US laboratories in a WHO network report through the US system. A non-US cohort or program dataset: other;
    - non-US national surveys (e.g. PHIA): national survey, with setting international;
    - pharmacy or other testing programs used for VE (e.g. ICATT): sentinel/network;
    - a field investigation's charts and interviews: record abstraction or interview.
  - The jurisdictions feeding a national CDC system are not listed separately unless the article analyzes a state system's own data.
  - A sentinel network's own chart abstraction is `sentinel/network` only. Questionnaires collected inside a surveillance program belong to that system. Linked area indexes (SVI, ADI) are not data sources; note them under `equity_measurement`.
  - Use `registry` also for linkage to vital records such as the National Death Index.
  - Non-CDC national data systems go under `other`, e.g. NPDS poison-center calls, or under `administrative/claims` when they are claims.
- **data_source_named** (list): as named in the article: systems, surveys, databases. Not states.
- **setting:**
  - **US national:** national data or a nationwide sample.
  - **US subnational:** states, cities, counties, multistate networks.
  - **international**
  - **mixed:** any combination of setting types, e.g. national results with state strata, or US and international cases together.
  - A single site (a facility, a ship) is US subnational. A multistate outbreak found through national surveillance is US national. Data mainly from multinational trials are international, unless the report's own analysis is of US data.
  - `data_source_type` and `setting` are best-effort fields: they characterize a report but rarely decide whether it is selected.
- **vaccine_outcome_type:** `{"v", "ev"}`, **only when the article quantifies vaccine epidemiology** (coverage or uptake, effectiveness, safety); otherwise `null`. Values: effectiveness · coverage/uptake · safety · other.
  - Trends in a vaccine-preventable disease alone do not count.
  - Classify from the primary question. The operating notion is characteristics that bear on population-level mitigation of conditions with a measured vaccine effect.
  - Provider or public knowledge, attitudes and acceptability: other. Supply, stockpile and distribution metrics: other. A breakthrough-infection cluster with no VE computed: other, not effectiveness. Animal vaccines: null.

### Equity (`equity` object)
**A social-position dimension (SPD)** is a group characteristic associated with power, privilege or social position.

| Dimension | Notes |
|---|---|
| race/ethnicity | |
| SES | One value. Covers education, income, wealth, poverty, insurance as SES, area indexes (SVI, ADI, HPI), housing status (homelessness), incarceration, and employment-status categories (unemployed, retired, disabled). Name up to 3 `ses_components` as the article words them. Occupation and industry count as SES **only when framed as social position**, e.g. "unemployed". Occupation as a route of exposure (health care personnel, workplace hazards) is not SES. |
| rurality | |
| age | Recorded always, including age bounds that only define the population or a rate's denominator (role `population`), and also when `data_focus` is none. It raises the level only when framed as social position or crossed with another SPD (e.g. children in poverty, older adults in congregate settings). |
| sex | Same rule as age. |
| sexual orientation and gender identity | Includes sexual behavior (MSM, sex of partners) with `basis` "behavior"; the equity level still depends on framing. A composite sex-at-birth / gender-identity variable is SOGI (basis self-identified), with its sex-at-birth component recorded as sex (basis assigned or observed). |
| disability | Only when framed as a social position or as access; not as a clinical condition. |
| country of birth/language/nativity | Includes the nativity or citizenship of cases in an international report. National-origin subgroups of a race (e.g. Asian Indian, Chinese) are race/ethnicity, not nativity. |
| other | |

**Not SPDs:**
- geography by itself (state, county, region, jurisdiction, policy status), including a country's income group (World Bank level, Gavi eligibility); note it under `equity_measurement`;
- rurality of non-human units, or rurality that only stands for an outbreak's location;
- clinical status;
- vaccination status;
- period.

An international setting is a characteristic, not an equity reading.

**`dimensions`:** `{"v", "role", "ses_components"?, "basis"?, "ev"}`.
- **basis** (optional; for sex, SOGI and race/ethnicity): how the groups were defined. self-identified · assigned or observed (e.g. sex at birth, race recorded by an observer or on a death certificate) · behavior (e.g. MSM, sex of partners) · not stated. Give it when the article says; otherwise omit it or use "not stated".
- **Roles:** population · covariate · descriptor · stratifier · primary comparison · cross-classifier. One entry per role.
- **A dimension named only in a limitation or a recommendation** (e.g. "race was not available") gets the role `descriptor` and does not raise the level.
- A **composite stratifier** (e.g. VFC eligibility: insurance status plus AI/AN status) is recorded as each component dimension.
- **Sampling-design strata** on an SPD (e.g. urbanization, minority density) get the role `covariate`.
- **One patient's social position in a case report** is a `descriptor` (level 1 at most).
- **Only the article's own population definition** makes a `population` SPD. An eligibility pathway quoted from a program's general description (e.g. Medicare via disability) does not.
- **Age bounds of the patients a provider survey asks about** define the population: age, role `population`.

**`level`** (0–4): the highest level the article meets.

| Level | Meaning |
|---|---|
| 0 | No SPD; or SPDs only define the population; or **age or sex only** (including age standardization, age-group strata and single Table-1 counts) without social-position framing. |
| 1 | An SPD other than age or sex appears only as a covariate, matching or weighting variable, or descriptor; or equity is raised only as a limitation or recommendation. Race/ethnicity as a confounder in a VE model is level 1. |
| 2 | Group-specific results by an SPD are reported and described, but not framed as a disparity. **Capped at 2:** contrasts shown only with significance marks, values only set side by side, or an SPD contrast that appears only in an interpretive table (group estimates with CIs or tests) and is not discussed. **The cap holds whatever the framing** ("disproportionately affected") **and whatever determinants are named**; record the determinants anyway and give the higher reading as `level_alternative`. A group's share of cases set beside its population share is side by side unless the article states the difference, ratio or excess. |
| 3 | A disparity framed, **quantified** (a computed difference, ratio, stated multiple such as "more than twice", or shortfall from a target) and interpreted; causes absent or generic. **In reports whose exposures or outcomes are themselves social determinants** (SDOH or HRSN measures such as problems paying medical bills; area indexes as stratifiers), quantified contrasts across SPD groups meet level 3 without the word "disparity". **A shortfall from a target set for an SPD-defined population** (e.g. a Healthy People target for Medicaid enrollees) is a quantified contrast: put that SPD in `comparison.across`, with the target as `referent`. |
| 4 | Level 3, plus specific social, structural or **political** determinants (policies, laws, governance) named as explanations, or measured and analyzed. Determinants that are the analyzed exposure meet level 4. Level 4 requires level 3. |

**The other equity fields:**
- **`level_reason`:** one or two sentences.
- **`level_confidence`** (high · medium · low) and **`level_alternative`** (`{"level", "reason"}`, or null): **required for the judgment reader (role "judgment")**; optional for others.
  - Use `medium` or `low` when a rule boundary decides the level.
  - Give the next most defensible level and why.
- **`determinants_of_health`:** the article's words for social, structural or political determinants, recorded even when the level is below 4. Record one when the article offers it as an explanation **of a disparity or of an SPD group's outcome**, or analyzes it as the exposure or the outcome (e.g. problems paying medical bills as the outcome). It counts:
  - congregate or crowded living (shelters, prisons) offered as an explanation for a group defined by social position;
  - access offered as the explanation of a group difference ("more ready access to medical care").

  Excluded:
  - explanations of an overall trend with no tie to social position (an economic downturn; pandemic disruptions and their reversal);
  - environmental exposures (wildfire smoke, extreme weather), unless framed as unequal exposure by social position;
  - a cultural or household practice in a case report (an individual risk factor);
  - an analyzed exposure that is not itself a social or structural determinant, and a bias the authors say they controlled for;
  - a factor named only as an unmeasured confounder;
  - a recommendation, e.g. "expand insurance coverage", unless the article also names that factor as a cause of the disparity;
  - clinical or individual risk factors (e.g. one patient's homelessness in a case report is a descriptor, not a determinant);
  - health-system or service disruptions (e.g. pandemic clinic closures) with no equity framing;
  - regulation, market availability or screening policy, unless offered as an explanation of a disparity.
- **`equity_action`:** true when the article evaluates an intervention, policy or program in relation to a disparity, or measures progress against a target or as an explicit change in the gap. **The target must be defined as closing a gap, or set for an SPD-defined population** (e.g. a Healthy People target for a low-income population); general program targets (UNAIDS 95%, WHO treatment success, elimination goals) do not count. Monitoring alone is false.
- **`equity_measurement`:** one short note (referent, proxy, groups collapsed or missing), or null.
- **Check:** level ≥ 2 needs an SPD in `comparison.across`. Level ≥ 3 needs a quantified contrast.

### Metadata
- **`reader`:** `{"model": "<your model id>", "reader_id": "<given>", "role": "structural" | "judgment"}`.
- **`confidence`:** for the record as a whole.
- **`uncertain_fields`** (list, may be empty): the fields where a rule boundary or the article leaves your value genuinely uncertain. Values: data_focus · procedures · regression_fields · inference · comparison · analytic_depth · outcome_types · study_design · groupwise · equity.level · equity.dimensions. Best-effort fields (conditions, data sources, setting, vaccine outcome) are not listed. List only real doubt: two or more entries, or an equity `level_confidence` of low, can send the article to a second reader.
- **`notes`** (at most 800 characters), **`view_issues`** (at most 400), **`level_reason`** (at most 400): short, or null.

## 5. Output (full record)
Write one JSON object per article to the path in your task. Then run `python3 tools/validate.py <that file>` (that file only) and fix any FAIL. Read **at least two** examples in `examples/` before your first article.

## 6. Review mode (retired)
Review mode (arm B of pilot 1) is retired in v4: an audit found that review anchored on the first reader and saved little (`reports/AUDIT_REPORT.md`; `BRIEF_V4_DECISIONS.md` D18). Each article now gets one full read, with a second full read where uncertainty is flagged.
