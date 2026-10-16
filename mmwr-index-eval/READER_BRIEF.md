# Reader brief: index fields for MMWR articles

*Version 4 (2026-10-13). It follows cloud pilot 1 (100 articles), the arm-B audit and the instructor's decisions on both (`BRIEF_V4_DECISIONS.md`, D1–D18). Everything you need is in this brief; you do not need to read `SCHEMA.md`.*

You read one MMWR article at a time and write one JSON record for it. The index helps a public health instructor find candidate reports by **how they make claims about data**: summaries and comparisons as well as regression. It also covers condition, design, data source and health equity. An error is bothersome, never fatal. **Justification matters most:** every value you give carries evidence from the article.

## 1. What you are given, per article (`packets/<id>/`)
- **`article.txt`:** the article text. In order: title, date line, Summary, body (`## ` headings), Notes, tables, figures. References are omitted.
- **`tables/*.csv`:** every HTML table cell, numbers included. **For table structure, the CSV is authoritative.** In `article.txt`, the numbers of mostly numeric tables are omitted, and the `Columns:`/`Rows:` lines only approximate the hierarchy.
- **`prepass.json`:** lexicon cues found by a script, with counts by location and one snippet each. These are **seeds, not answers.** Confirm each one, decide whether it is primary or ancillary, and drop false hits. Two known false-hit patterns:
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
- **yes:** it does. This includes estimates the report computes itself from others' data, e.g. a work group's pooled estimates, weighted summaries, or new tables computed from trial data.
- **limited:** a single case or a small case series with context; or figures only cited from earlier reports, even when tabulated with CIs (e.g. an ACIP evidence summary that computes nothing of its own).
- **none:** no quantities at all. Examples: an announcement, a cover box that only points to other reports, a schedule or guidance statement with no data. **A notice that cites rates or a change from earlier reports is `limited`, not `none`.**

If `none`, fill only `data_focus`, `public_health_question`, `conditions`, `setting`, `study_design` (e.g. `guidance or recommendations` for a schedule), `equity` (level 0 unless the article frames equity; still record age and sex that define the population) and the metadata. Leave the method fields empty or null: `outcome_types` [], `analytic_depth` [], `procedures` [], `comparison` with framework "none", `inference` "none", `regression_fields` null, `groupwise` [].

### A. `outcome_types` (list)
- **continuous:** means, medians, quantiles of a measured quantity.
- **binary/categorical:** counts and proportions.
- **count/rate:** rates per population or person-time, incidence, counts modelled as counts.
- **event-time:** time to an event, survival, Kaplan-Meier, hazards.

List the types the article's **claims** rest on. A count outcome fitted with a linear model is still count/rate: the procedure records the linear model.
- **Risk vs rate.** A risk or proportion is unitless (numerator and denominator have the same dimension, e.g. persons with disease per person): binary/categorical. A rate has a unit (e.g. cases per person-time, per year): count/rate.
- Event counts per period with no denominator: count/rate. A cumulative risk reported as %: binary/categorical.
- Descriptive medians of population characteristics (e.g. the age of cases) do **not** make an outcome continuous. Medians of the illness's own course (incubation period, duration) do.
- Dollar outcomes and model-derived parameters (doubling time, growth rate): continuous.

### B. `analytic_depth` (list)
- **summary:** one group or one quantity.
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
    - **Tests by CI inversion.** A CI that excludes (or includes) the null value tests that hypothesis by inversion, even when the article does not say so. Record `"name": "CI vs fixed value (inversion)"` when a CI for a contrast with a defined null (1 for ratios, 0 for differences and VE, or a stated target) is reported **and the article draws a conclusion from it** (an effect, a difference, protection, "no difference", "similar"). A CI reported with no conclusion is estimation only.
    - **The inferred method follows the CI construction.** When the article names or describes how the CI was built (Wald, exact or Clopper-Pearson, Wilson, profile likelihood, Taylor series, bootstrap, or model-based from a named regression), put it in `inferred_from` and set `inference.method_stated` true. Otherwise write "CI method not stated" in `inferred_from`. This is common in MMWR and is not an error.
    - **Overlap is not a test.** Judgments from overlapping or non-overlapping CIs of two separate estimates are never a procedure; see `inference`.
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
| multinomial or generalized logit | logistic, noting "multinomial" in `inferred_from` |

- **alternative_reading** is optional, never required. Offer it only when a second family is genuinely defensible, e.g. least-squares on a log rate.
- **A regression used only for CIs or tests** (e.g. Poisson for coverage CIs) is a regression with role `ancillary`. Fill `regression_fields` with what is stated, null elsewhere.
- **role:** `primary` if the procedure carries the main claims. Examples: the tests behind the contrasts the text reports; survey weights that produce every estimate; a CI judged against a fixed value that underpins the headline claims. Otherwise `ancillary`, e.g. a chi-square comparing Table-1 characteristics.

### D. `comparison` (object)
- **framework:**
  - none;
  - fixed referent: a target, a threshold, 100% coverage, or a national or population estimate treated as fixed;
  - pair: two groups or periods;
  - group: three or more.

  When pair and group comparisons coexist, **use group** (lead with the groupwise comparison; the pairwise contrasts are recorded through `groupwise.resolves_to_finer` and `procedures`).
- **scale:** absolute (differences, percentage points) · relative (ratios, APCs, percent change) · both · **null when values are only set side by side** with no difference or ratio computed. Scale follows the reported contrast, not the test: a test with no reported difference or ratio leaves scale null.
- **referent:** as named, or null.
- **across:** the variable(s) compared.

### E. `inference`
- **v:** none · CIs · tests · CIs and tests.
  - A CI judged against a fixed value counts as a test, including a test by CI inversion (see `procedures`).
  - **CI overlap** does not. Record "CIs" and set `ci_overlap: true` when any claim rests on overlapping or non-overlapping intervals. If the article says no other method was available, say so in `notes`; it is still not a test.
- **ci_overlap** (optional, boolean): true when a claim rests on CI overlap; omit or false otherwise.
- **method_stated:** the article says how the CIs or tests were computed, e.g. "exact binomial", "chi-square".

### F. `groupwise` (list)
A result that assesses a difference **across 3 or more groups**, as distinct from one that assesses a difference between 2 groups. The groups are defined on a **social-position dimension** (the SPD table under Equity: race/ethnicity, SES, rurality, age, sex, SOGI, disability, nativity, other), even when the equity level is low. Groupwise results are typically invariant to the choice of referent but can mask subgroup effects. So record anything that **looks** groupwise, and whether it is truly groupwise or **resolves to something finer**. Comparisons across 3 or more non-SPD groups (vaccination status, periods, products, routes, regions) stay out; they belong in `comparison`. Empty list if none.

Each entry: `{"across": [<SPD dimension(s)>], "n_groups": <integer ≥ 3, or null if the count is unclear>, "ordered": <bool>, "assessment": <value>, "resolves_to_finer": <bool>, "ev": [...]}`.
- **assessment:** side by side (three or more group values shown or ranked, no inferential assessment across them) · omnibus test (homogeneity: any difference among the groups) · trend test · model joint test · interaction (departure from independence or additivity across two dimensions, not inequality) · per-group vs referent only (the groups are assessed only by pairwise tests or ratios against a reference level).
- **Rules:**
  1. Count the groups **actually compared in that result**, after any collapsing, not the levels of the variable. A five-level race variable reported only as Black vs White is pairwise: no entry.
  2. Symmetric tests count whichever margin is the SPD. A chi-square on case vs control by five race groups assesses association across five groups: `omnibus test`, n_groups 5. This is the common hidden case in Table-1 comparisons; check the table to count the categories when the text only says "chi-square tests".
  3. Cross-classified groups count by cells (race × sex with 2 × 2 levels is 4 groups) **only when the result treats the cells as one set of groups to compare**, e.g. ranks them or tests across them. A table that cross-classifies race and sex while the article compares each margin separately (Black vs White; female vs male) is pairwise in each dimension: no entry. Calling the cells groupwise would claim an assessment of homogeneity across all the cells (any difference), which is more than either margin's comparison and different from an interaction.
  4. `ordered` is true for ordered SPD levels (age groups, income or education gradients). `trend test` means a test across those ordered groups (e.g. Cochran-Armitage, chi-square for trend). A trend over calendar time is not groupwise; it becomes groupwise only when time trends are compared across 3 or more SPD groups (e.g. an interaction, or joinpoint APCs compared by group).
  5. `resolves_to_finer` is true when the article also reports pairwise contrasts or per-group estimates against a referent for the same groups (e.g. an omnibus chi-square followed by pairwise t-tests, or ORs for each category).
- **Recognition cues** (not a definition): Pearson chi-square on an r × c table; survey-adjusted chi-square (Wald F, Wald chi-square, Rao-Scott, "corrected for survey design"); a joint Wald test of a multi-level covariate; Kruskal-Wallis or ANOVA; Cochran-Armitage or chi-square for trend; an interaction of a multi-level SPD with time or period; multinomial or ordinal outcomes across SPD groups.

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
- **conditions** (list of `{"v", "role", "ev"}`): the health conditions the article studies: diseases, infections, injuries, substance use, health states (e.g. pregnancy) and health behaviors measured as outcomes (e.g. breastfeeding). **Do not classify them into categories**; a separate pass groups them (`CONDITION_RUBRIC.md`).
  - **v:** the condition named as the article names it, at the level of its title and Summary (e.g. "congenital syphilis", "COVID-19", "systemic lupus erythematosus"). You may infer it from the text (a positive SARS-CoV-2 test → "COVID-19"). Use the common name, not a code.
  - **role:** exposure · outcome · population (it defines who was studied, e.g. "persons with HIV") · vaccine target (the disease a studied vaccine prevents). One entry per condition and role.
  - **What stays out:** a condition that only defines a control group, or appears only as a model covariate or risk factor. The pandemic period does not make a report about COVID-19, whether it is background or a period being compared.
  - Best effort: this field characterizes a report but never sends it to a second reader. Empty if nothing fits.
- **study_design** (list): cross-sectional · retrospective cohort · prospective cohort · case-control · test-negative · screening method · before/after or trend · ecologic · outbreak investigation · surveillance/descriptive · diagnostic accuracy · guidance or recommendations · case report/series · cost/economic · non-randomized trial · evaluation or monitoring · mixed/other.
  - **Prefer the design the report names** ("evaluation", "case-control", "field trial", "cross-sectional survey"), mapped to the nearest value. Infer a design only when none is named, and say so in `notes`.
  - **evaluation or monitoring:** program, policy or intervention evaluation; evaluation of a surveillance system, indicator or nowcast; monitoring progress against targets. Routine surveillance reports stay `surveillance/descriptive`.
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
| sexual orientation and gender identity | MSM as an HIV transmission category is not SOGI unless framed as such. |
| disability | Only when framed as a social position or as access; not as a clinical condition. |
| country of birth/language/nativity | Includes the nativity or citizenship of cases in an international report. |
| other | |

**Not SPDs:**
- geography by itself (state, county, region, jurisdiction, policy status);
- clinical status;
- vaccination status;
- period.

An international setting is a characteristic, not an equity reading.

**`dimensions`:** `{"v", "role", "ses_components"?, "ev"}`.
- **Roles:** population · covariate · descriptor · stratifier · primary comparison · cross-classifier. One entry per role.
- **A dimension named only in a limitation or a recommendation** (e.g. "race was not available") gets the role `descriptor` and does not raise the level.
- A **composite stratifier** (e.g. VFC eligibility: insurance status plus AI/AN status) is recorded as each component dimension.
- **Sampling-design strata** on an SPD (e.g. urbanization, minority density) get the role `covariate`.

**`level`** (0–4): the highest level the article meets.

| Level | Meaning |
|---|---|
| 0 | No SPD; or SPDs only define the population; or **age or sex only** (including age standardization, age-group strata and single Table-1 counts) without social-position framing. |
| 1 | An SPD other than age or sex appears only as a covariate, matching or weighting variable, or descriptor; or equity is raised only as a limitation or recommendation. Race/ethnicity as a confounder in a VE model is level 1. |
| 2 | Group-specific results by an SPD are reported and described, but not framed as a disparity. **Capped at 2:** contrasts shown only with significance marks, values only set side by side (even with framing words), or an SPD contrast that appears only in an interpretive table (group estimates with CIs or tests) and is not discussed. A group's share of cases set beside its population share is side by side unless the article states the difference, ratio or excess. |
| 3 | A disparity framed, **quantified** (a computed difference, ratio, or shortfall from a target) and interpreted; causes absent or generic. **In reports whose exposures are themselves social determinants** (SDOH or HRSN measures, area indexes as stratifiers), quantified contrasts across them meet level 3 without the word "disparity". |
| 4 | Level 3, plus specific social, structural or **political** determinants (policies, laws, governance) named as explanations, or measured and analyzed. Determinants that are the analyzed exposure meet level 4. Level 4 requires level 3. |

**The other equity fields:**
- **`level_reason`:** one or two sentences.
- **`level_confidence`** (high · medium · low) and **`level_alternative`** (`{"level", "reason"}`, or null): **required for the judgment reader (role "judgment")**; optional for others.
  - Use `medium` or `low` when a rule boundary decides the level.
  - Give the next most defensible level and why.
- **`determinants_of_health`:** the article's words for social, structural or political determinants, recorded even when the level is below 4. Record one when the article offers it as an explanation of an outcome or a disparity, or analyzes it as the exposure or the outcome (e.g. problems paying medical bills as the outcome). Excluded:
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
- **`uncertain_fields`** (list, may be empty): the fields where a rule boundary or the article leaves your value genuinely uncertain. Values: procedures · regression_fields · inference · comparison · analytic_depth · outcome_types · study_design · groupwise · equity.level · equity.dimensions. Best-effort fields (conditions, data sources, setting, vaccine outcome) are not listed. An entry here, an equity `level_confidence` below high, or an `alternative_reading` on a procedure can send the article to a second reader.
- **`notes`** (at most 800 characters), **`view_issues`** (at most 400), **`level_reason`** (at most 400): short, or null.

## 5. Output (full record)
Write one JSON object per article to the path in your task. Then run `python3 tools/validate.py <that file>` (that file only) and fix any FAIL. Read **at least two** examples in `examples/` before your first article.

## 6. Review mode (retired)
Review mode (arm B of pilot 1) is retired in v4: an audit found that review anchored on the first reader and saved little (`reports/AUDIT_REPORT.md`; `BRIEF_V4_DECISIONS.md` D18). Each article now gets one full read, with a second full read where uncertainty is flagged.
