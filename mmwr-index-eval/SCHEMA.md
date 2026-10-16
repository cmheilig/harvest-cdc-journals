# MMWR index schema, version 2.1 (2026-10-11)

**Where this file and `READER_BRIEF.md` differ, the brief governs.** Replaces an earlier, regression-centred field list. It was built from the instructor's rulings and a calibration exercise, then revised after a three-model pilot on 80 articles.

**Purpose:** to help find candidate reports by **how they make claims about data**: summaries and comparisons as well as regression, preferring recent reports. An error is bothersome, never fatal.

**Justification over agreement:** every value a reader gives carries its evidence (a location and a quote of at most 25 words). In any review, a well-justified value counts for more than agreement with any reference. Many reports are ambiguous or use several methods.

## Columns used below
- **Priority:**
  - 1: core for finding reports;
  - 2: second tier, such as `vaccine_outcome_type`.
- **Reader:**
  - S (straightforward): a fast model can do it, often helped by a lexicon pre-pass;
  - J (judgment): it needs a stronger model.

Lists can hold several values, because reports combine methods.

## 1. Identification (from the views; no reader)
`id`, `date`, `year`, `title`, `article_type`, `family`, `awareness`, `url`.

## 2. How the report makes claims about data

| Field | Values | Pri | Rdr |
|---|---|---|---|
| **A. `outcome_types`** (list) | continuous (means, quantiles) · binary/categorical (counts, proportions) · count/rate (per population or person-time) · event-time | 1 | S |
| **B. `analytic_depth`** (list) | summary (univariate) · comparison (bivariate: difference, ratio, test) · stratified/adjusted (multivariable: standardization, weighting, stratification, model) | 1 | J |
| **C. `procedures`** (list of objects, below) | tests, regressions, standardization, weighting, transformations, other | 1 | S → J |
| **D. `comparison`** (object) | `framework`: none / fixed referent / pair / group · `scale`: absolute / relative / both · `referent`: free text · `across`: the variable(s) compared | 1 | J |
| **E. `inference`** | none · CIs · tests · CIs and tests; plus `method_stated` (yes/no) | 1 | S |
| `primary_measures` (list) | labels as used in the report (percentage, median, rate per 100,000, rate ratio, aOR, VE, APC …) | 1 | S |
| **F. evidence** | not a field. Each value in A–E and in §3–5 carries `{location: text / table N / figure N / note, quote}`. Readers record F to justify A–E, not as a goal in itself. | — | — |

**Fields only where `procedures` include a regression** (otherwise `n/a`, not silent):

| Field | Values | Pri | Rdr |
|---|---|---|---|
| `nonindependence_approach` | GEE · mixed effects · clustered or robust SE · conditional (matched) · complex survey design · serial correlation · none | 2 | J |
| `unadjusted_results` / `adjusted_results` | yes / no | 1 | S |
| `confounders` | free text, as listed | 2 | S |
| `interaction_modeled` | yes / no, with how it is interpreted | 2 | J |
| `sensitivity_analysis` | yes / no | 2 | S |

**Standardization counts as adjustment** (instructor's convention). For example, mm7041a2 age-standardizes without a regression model.

### C. `procedures`: one object per procedure
```json
{"name": "as named or described in the report", "class": "test | regression | standardization | weighting | transformation | other",
 "regression_family": "linear | logistic | loglinear | event-time | other | null",
 "inferred_from": "the phrase that identifies it", "role": "primary | ancillary",
 "alternative_reading": "optional: another defensible family, with a reason"}
```

**Regression families** (the four families of a regression course, plus other). Readers make the link from the report's words to the family explicit:

| Report says | Family |
|---|---|
| least squares, linear regression, OLS | linear |
| logistic, conditional logistic, GEE logistic, mixed-effects logistic, "odds ratio from a model" | logistic |
| Poisson, modified Poisson, quasi-Poisson, negative binomial, log-binomial, "rate ratio from a model" | loglinear |
| Cox, proportional hazards, Andersen-Gill, Fine-Gray | event-time |

Kaplan-Meier and logrank are `test` or `other` procedures whose outcome type is event-time. Joinpoint is a regression whose family the reader states with a reason.

**Ambiguity is welcome, never required.** One example is a linear model of the logarithm of a count or rate, such as least-squares APC or joinpoint on log rates: linear by estimator, loglinear in substance. A reader may offer this through `alternative_reading`, but should not spend effort looking for it.

**Primary vs ancillary:** a chi-square test on Table 1 characteristics is ancillary. Whatever carries the report's main claims is primary.

**Lexicon pre-pass** (deterministic, over all articles): this widens the v2 `estimate_signatures` to cover:
- medians, means and quantiles;
- percentages with CIs;
- rates per N;
- age-adjusted and standardized estimates;
- weighting;
- test eponyms (Pearson, Rao-Scott, Fisher, Wilcoxon, Mann-Whitney, Kruskal-Wallis, Wald, Yates, t, logrank, McNemar, Cochran);
- regression words.

The pre-pass seeds layers C and E. Readers confirm the seeds and assign each one a role.

## 3. Context

| Field | Values | Pri | Rdr |
|---|---|---|---|
| `public_health_question` | one sentence | 1 | J |
| `condition_group` (list: exposure and outcome) | COVID · influenza · mpox · RSV · HIV · TB/respiratory · STI · vaccine-preventable (other) · diabetes · cardiovascular · cancer · obesity/nutrition · maternal/reproductive · mental health · substance use · injury/poisoning · environmental · other acute · other chronic | 1 | S |
| `study_design` (list) | cross-sectional · retrospective cohort · prospective cohort · case-control (incl. matched) · test-negative · screening method · before/after or trend (segmented, joinpoint) · ecologic · outbreak investigation · surveillance/descriptive · diagnostic accuracy · guidance or recommendations · mixed/other | 1 | J |
| `data_source_type` (list) | CDC surveillance system · national survey · other survey (incl. web) · registry · electronic health records · administrative/claims · vaccination registry · sentinel/network · other | 2 | S |
| `data_source_named` (list) | NHANES, BRFSS, NIS, NHIS, VSD, COVID-NET, NNDSS, … | 1 | S |
| `setting` | US national · US subnational · international · mixed | 2 | S |
| `vaccine_outcome_type` (vaccine reports only) | effectiveness · coverage/uptake · safety · other | 2 | S |

International setting is a characteristic of interest, not an automatic equity reading (instructor ruling).

## 4. Health equity

**`equity_level`**, 0–4 per reader. The definitions, decision rules and boundary cases are in `out/EQUITY_DEFINITION.md`, as amended below.
- **0: not addressed.**
- **1: covariate or descriptor only.**
- **2: stratified or compared, not interpreted.**
- **3: a disparity framed and interpreted, with the contrast quantified.** The report gives a difference, ratio or shortfall, not only significance marks. Example: mm6949a2 reports pairwise comparisons without their magnitudes, so it stays at level 2 (instructor ruling).
- **4: level 3, plus determinants named or examined as explanations.** Example: mm7223a3.

**Derived:** `equity_flag` = true if **any** reader's level is ≥ 3. The level is always reported. Every report gets the same fixed number of readers, so flags are comparable across the corpus.

**`equity_dimensions`:** a list of `{dimension, role}`. Covariate-only dimensions are kept, because a reader has to identify covariates to discern their role.
- **dimension:**
  - race/ethnicity;
  - SES, as **one** value for indexing (education, income, wealth, occupation, poverty, insurance-as-SES, area deprivation);
  - rurality;
  - age;
  - sex;
  - sexual orientation and gender identity;
  - disability;
  - country of birth / language / nativity;
  - other.
- **role:** population · covariate · descriptor · stratifier · primary comparison · cross-classifier.
- **`ses_components`:** when `dimension` is SES, the reader also names what constitutes it in the report, **at most 3**, as the report words them (e.g. "household income <200% FPL", "education", "primary payor"). SES stays a single value for indexing; the components show the main reasons a report counts as SES (instructor ruling).
- **Not dimensions:**
  - **geography by itself** (state, county, region): it counts only when combined with position, power or privilege, as in rurality or area deprivation (instructor ruling);
  - clinical status, vaccination status and pathogen period.
- **Age and sex** are recorded, and they raise the level only when framed as social position.

**The other equity fields:**

| Field | Values | Pri | Rdr |
|---|---|---|---|
| `determinants_of_health` | free text, the report's own words; recorded even when the level is below 4 | 1 | J |
| `equity_action` | yes / no + note: an intervention, or progress toward eliminating a disparity, is evaluated | 2 | J |
| `equity_measurement` | **one** note: referent, proxies, groups collapsed or missing | 2 | J |

**Check between D and §4:**
- **Level ≥ 2** requires `comparison.across` to include an equity dimension, with a framework of pair or group.
- **Level ≥ 3** additionally requires the contrast to be quantified.

**VE and confounding:** race/ethnicity as a confounder in a VE report is level 1 by default. VE has no special equity criteria; the same reading applies as to other prevention measures (instructor ruling).

## 5. Processing metadata
`reader_model`, `reader_id`, `confidence` (high / medium / low), `notes`. Readers flag anything in the view they suspect is wrong (READER_NOTES.md).

## 6. Standing reading rules (all general)
1. **Read methods everywhere**, including table notes, figure notes and footnotes. Answers often appear only there.
2. **Name the procedure from the estimate and the test,** not from the word "regression". Make the link to the regression family explicit.
3. **Check the measure against the outcome type.** Flag any mismatch between a label and its quantity.
4. **An interaction requires a formal term or test.** Stratified estimates are not an interaction.
5. **For vaccine reports, classify the primary question first.** If a report contains two studies, record both.
6. **Record every method present.** Multiple methods are the norm, not a conflict.
7. **Silent is not false.** If the report does not say, record nothing and do not guess.

## 7. Changes from version 1
- **Layers replace the regression frame.**
  - `method_family` becomes `outcome_types` plus `procedures[].regression_family`.
  - `method_type` becomes `procedures`.
  - New: analytic depth, comparison structure and inference.
  - The regression-only fields are `n/a` when there is no model.
  - "VE-specific" is dropped; VE is a measure and a design, not a family.
  - `method_complexity` is dropped: the length of the procedure list carries it.
- **Equity changes.**
  - The boolean `equity_flag` is replaced by `equity_level`, with the flag derived.
  - Dimensions carry roles.
  - Rurality is added; SES stays one value, with up to 3 named components; geography alone is excluded.
  - New: `equity_action` and `equity_measurement`.
- **New context fields.** `setting`, `vaccine_outcome_type`, and the expanded `study_design` and `data_source_type`.
- **Justification and tiers.** Every value carries evidence, and every field has a priority and a reader tier.

## 8. Version 2.1 amendments (2026-10-11, after the Stage 6b pilot; reader brief v3)
- **`data_focus`** (new, first field): yes · limited · none. At `none`, the method fields are left empty. The structural reader sets it, and the judgment reader may be skipped.
- **`equity.level_confidence`** (high · medium · low) and **`equity.level_alternative`** (`{level, reason}` or null): required from the judgment reader.
- **New values:**
  - `study_design`: case report/series · cost/economic;
  - `data_source_type`: international surveillance · record abstraction or interview;
  - `condition_group`: health behaviors.
- **Rules from the pilot and the instructor's rulings:**
  - what is and is not a procedure;
  - which data sources are listed;
  - age and sex alone give level 0;
  - a quantified contrast is needed for level 3;
  - SDOH exposures meet level 3;
  - political determinants count for level 4;
  - occupation is SES only when framed as social position;
  - a dimension named only in a limitation is a `descriptor`;
  - a CI judged against a fixed value is a test;
  - matching counts as stratification.

  The full text is in `READER_BRIEF.md` §4.
- Every record keeps both readers' values in the index, so between-reader disagreement can inform candidate selection.

## 9. Version 2.2 additions (2026-10-13, after cloud pilot 1 and the arm-B audit; reader brief v4)
`READER_BRIEF.md` v4 governs; `tools/validate.py` encodes it. In summary (decisions in `BRIEF_V4_DECISIONS.md`):
- new required list `groupwise` (comparisons across 3 or more social-position groups: `across`, `n_groups`, `ordered`, `assessment`, `resolves_to_finer`, `ev`);
- new required list `uncertain_fields` (method and equity fields only), used to route an article to a second reader;
- `inference.ci_overlap` (optional flag); tests by CI inversion recorded as "CI vs fixed value (inversion)";
- `study_design` adds `non-randomized trial` and `evaluation or monitoring`;
- `condition_group` is replaced by free-text `conditions` (`v`, `role`: exposure · outcome · population · vaccine target, `ev`), grouped later with `CONDITION_RUBRIC.md`;
- evidence `loc` is restricted to title · summary · text · table N · figure N · note · ocr;
- review mode (arm B) is retired.

## 10. Version 2.3 additions (2026-10-16, after pilot 2; reader brief v5)
`READER_BRIEF.md` v5 governs; `tools/validate.py` encodes it. In summary (decisions in `BRIEF_V5_DECISIONS.md`, V1–V18, and `BRIEF_V4_DECISIONS.md` D16 rule 3):
- `uncertain_fields` accepts `data_focus`;
- `groupwise.assessment` adds `pairwise (all pairs)`, with a precedence order among the values;
- `study_design` adds `review or synthesis`;
- equity `dimensions` entries take an optional `basis` (self-identified · assigned or observed · behavior · not stated) for sex, SOGI and race/ethnicity;
- a claim resting on CI overlap is a `test` named "CI overlap assertion", with `inference.ci_overlap` true;
- the validator enforces the brief's equity checks: level ≥ 2 needs an SPD in `comparison.across`; level ≥ 3 needs `comparison.scale` not null;
- condition groups (`CONDITION_RUBRIC.md`) split TB from respiratory, add `respiratory - infectious`, `respiratory - noninfectious`, `viral hepatitis` and `vaccine safety`, and normalize terms through a `canonical` column.
