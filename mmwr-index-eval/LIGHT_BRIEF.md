# Light reader brief: data focus, outcome types, depth, design and methods sentences

*Version 1.0 (2026-11-03). A first-pass reading for MMWR vols 48–70 (1999–2021), beside the surface rules (`reports/SURFACE_RULES_REPORT.md`). The rules carry what reports declare by name: regression, tests, CIs, weighting, standardization, groupwise candidates and an equity band. This reading covers the fields the rules cannot. Its definitions are copied from `READER_BRIEF.md` v5.2 so that the two readings compare field by field; a change there is copied here under a new version.*

You read one MMWR article and reply with one JSON object. The index helps a public health instructor find candidate reports by **how they make claims about data**. An error is bothersome, never fatal. Every value carries evidence from the article.

## 1. What you are given
The user turn holds one article: `article.txt`, then each `tables/*.csv`, each after a line `=== <file name> ===`. A table file `<id>_T<N>.csv` is table N.
- `article.txt` holds, in order: title, date line, Summary, body (`## ` headings), Notes, tables, figures. References are omitted.
- **GIF-era images (2000–2009)** appear as `[Table N shown only as an image]`, followed by "OCR text (… unverified)". OCR words are reliable; its numbers and column alignment are not. Quote OCR text with `loc: "ocr"`.
- **Methods are often stated only in table notes, figure notes or footnotes.** Read them all.
- **Methods behind estimates the article only cites** (e.g. a manufacturer's hazard ratios in an ACIP summary) are not the article's own.

## 2. Fields

### `data_focus` (first)
Does the article produce **quantities about public health outcomes, or comparisons of them**?
- **yes:** it does. This includes estimates the report computes itself from others' data, e.g. a work group's pooled estimates, weighted summaries, or new tables computed from trial data.
- **limited:** a single case, or a case series that only describes its cases; or figures only cited from earlier reports, even when tabulated with CIs (e.g. an ACIP evidence summary that computes nothing of its own).
- **Size does not decide it; analysis does.** A case series or cluster of any size is `yes` when it reports its own counts or proportions **and** compares them (groups, periods, attack rates, exposed vs unexposed).
- **Cited vs own.** When the headline quantities are cited from other reports, the article is `limited`, even if it adds a few counts of its own.
- **Program and system measures** (coverage, doses delivered, adoption, timeliness, completeness) are quantities about public health when the report computes them from its own data.
- **none:** no quantities at all. Examples: an announcement, a cover box that only points to other reports, a schedule or guidance statement with no data. **A notice that cites rates or a change from earlier reports is `limited`, not `none`.**

If `none`, give `outcome_types` [] and `analytic_depth` []. If `limited`, describe **only what the article itself computes or reports as its own result**: a single case or a descriptive case series gets `outcome_types` from the counts or measures reported ([] when it reports none) and `analytic_depth` `summary`; a report of cited figures only gets `outcome_types` [] and `analytic_depth` [].

### `outcome_types` (list)
- **continuous:** means, medians, quantiles of a measured quantity.
- **binary/categorical:** counts and proportions.
- **count/rate:** rates per population or person-time, incidence, counts modelled as counts.
- **event-time:** time to an event, survival, Kaplan-Meier, hazards.

List the types the article's **claims** rest on. A count outcome fitted with a linear model is still count/rate.
- **Risk vs rate.** A risk or proportion is unitless (numerator and denominator have the same dimension, e.g. persons with disease per person): binary/categorical. A rate has a unit (e.g. cases per person-time, per year): count/rate.
- Event counts per period, place or group with no denominator (cases by year, deaths by state): count/rate. A cumulative risk reported as %: binary/categorical.
- **A case total that only serves as the denominator of percentages, or only gives a series' size, does not add count/rate.** Counts broken down by period, place or group do.
- Descriptive medians of population characteristics (e.g. the age of cases) do **not** make an outcome continuous. Medians of the illness's own course (incubation period, duration) do, when a claim rests on them.
- A single descriptive mean or median makes an outcome continuous only if it is compared or interpreted. A score analyzed as a mean (e.g. a 0–3 composite) is continuous.
- Dollar outcomes and model-derived parameters (doubling time, growth rate): continuous.

### `analytic_depth` (list)
- **summary:** a single-group quantity **presented as a result**, e.g. an overall prevalence, a national percentage, a seroresponse range. Sample or enrollment sizes, and the reference value of a contrast, do not earn it.
- **comparison:** two or more groups or periods compared, **or a comparison with a fixed referent** (a target, a threshold, or a population value treated as fixed). A side-by-side comparison with no computed contrast is still `comparison`.
- **stratified/adjusted:** a comparison within levels of a third variable, or an adjusted one. Adjustment includes standardization, weighting to adjust, a model, or **matching treated as stratification** (e.g. conditional OR).
  - A series over time by group is a comparison, not stratification. Period-to-period changes computed within levels of a group variable are such a series: `comparison`.
  - `stratified/adjusted` needs a contrast of interest estimated within levels of a third variable, or adjustment. The authors' word "stratified" for a two-way cross-tab does not decide it.
  - Survey weighting that only yields population estimates does not make the depth stratified.
- **Not comparisons:** case-confirmation steps (e.g. a fourfold rise in paired sera), classification cutpoints, the reference standard in a diagnostic-accuracy study, duplicate checks between data sources, and compliance or inspection counts in an outbreak report.

### `study_design` (list)
cross-sectional · retrospective cohort · prospective cohort · case-control · test-negative · screening method · before/after or trend · ecologic · outbreak investigation · surveillance/descriptive · diagnostic accuracy · guidance or recommendations · case report/series · cost/economic · non-randomized trial · evaluation or monitoring · review or synthesis · mixed/other.
- **Prefer the design the report names** ("evaluation", "case-control", "field trial", "cross-sectional survey"), mapped to the nearest value.
- **evaluation or monitoring:** program, policy or intervention evaluation; evaluation of a surveillance system, indicator or nowcast; monitoring progress or program operations against stated goals or targets. Without goals or targets, reports stay `surveillance/descriptive`.
- **review or synthesis:** systematic reviews, GRADE and evidence-to-recommendation summaries, and scholarly or narrative reviews or syntheses, including those inside a guidance report.
- Announcements and awareness notices: `mixed/other`. Do not infer a design from figures cited from other reports.
- Repeated cross-sections whose headline is a change over time (e.g. a QuickStats trend): add `before/after or trend` to the cross-sectional or surveillance design. A title word such as "Trends" alone does not name a design.
- A case series nested inside an outbreak investigation: `outbreak investigation` only.
- **non-randomized trial:** an intervention assigned by investigators without randomization, unless the report calls itself an evaluation.
- Investigations of medication-error or adverse-event clusters: `outbreak investigation`.
- Two time points compared count as before/after.
- **Ecologic** means the unit of analysis is a group, e.g. counties or schools.
- Record a cohort as retrospective or prospective only when the article says so, or the data are clearly existing records.

### `methods_quotes` (list, at most 6)
Verbatim sentences or clauses from the article that **state how its own quantities were computed, compared or tested**: tests, models, CI methods, weighting, standardization, suppression or reliability rules, trend analysis. Each is `{"loc", "q"}`. Prefer the one that names the method over one that only reports a result. Empty when the article states no method.

### `confidence` and `uncertain_fields`
`confidence` (high · medium · low) for the record as a whole. `uncertain_fields` (list, may be empty): any of `data_focus`, `outcome_types`, `analytic_depth`, `study_design` where a rule boundary or the article leaves your value genuinely uncertain.

## 3. Evidence and output
- Each value object is `{"v": <value>, "ev": [{"loc": "...", "q": "..."}]}`, with one or two quotes; never more than three.
- `loc` is one of `title`, `summary`, `text`, `table N`, `figure N`, `note`, `ocr` (an Abstract is `summary`; an unnumbered table or figure is `table 1` / `figure 1`).
- **Every `q` is copied exactly from the article and is at most 25 words.** The driver checks both: a quote it cannot find in the article, or one over 25 words, fails validation.
- Reply with one JSON object only, no text before or after it and no code fence:

```
{"id": "<article id>",
 "data_focus": {"v": "yes | limited | none", "ev": [...]},
 "outcome_types": [{"v": "...", "ev": [...]}],
 "analytic_depth": [{"v": "...", "ev": [...]}],
 "study_design": [{"v": "...", "ev": [...]}],
 "methods_quotes": [{"loc": "...", "q": "..."}],
 "confidence": "high | medium | low",
 "uncertain_fields": []}
```
