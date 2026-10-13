# Brief v4: decisions

This file turns the pilot's `reports/brief_issues.md` (67 adjudicator notes) and readers' questions into decisions for `READER_BRIEF.md` v4. Each item gives the issue (with pilot article ids), the options, a **proposed** rule and a **Decision** line.

**How to use it.** Edit only the items you care about, on the `Decision:` line: write `accept`, `reject`, `defer`, or your own rule. **A blank Decision line means the proposal is accepted.** Write `defer` to keep an item out of v4. You can edit this file on GitHub (pencil icon, then commit to this branch). Brief v4 itself is written on a new branch after these decisions (see the note at the end).

---

## D1. Groupwise comparisons (owner's decision, with open sub-questions)
**Owner's principle.** Index a result that assesses a difference **across 3 or more groups** distinctly from one that assesses a difference **between 2 groups**. "Groups" here are defined on an **equity (social-position) dimension**, even when the equity level is low. When an article has both, **lead with** the groupwise result. Groupwise results are typically invariant to the choice of referent, but they mask subgroup effects. So anything that *looks* groupwise is of interest, along with whether it is truly groupwise or **resolves to something finer** (pairwise contrasts, per-group estimates against a referent).

**Is the principle precise enough?** Yes, as a definition. It needs no list of procedures, because the brief's SPD table already says which groupings qualify. Age, sex, race/ethnicity, SES, rurality, SOGI, disability, nativity and "other" qualify. The brief explicitly excludes geography by itself, clinical status, vaccination status and period. Six operational rules would make it workable:
1. **Count the groups actually compared in that result**, after any collapsing, not the levels of the variable. A five-level race variable reported only as Black vs White is pairwise.
2. **Symmetric tests count whichever margin is the SPD.** A chi-square on a 2 × 5 table (case vs control by race) assesses association across 5 race groups, so it is groupwise. This is the most common hidden case (e.g. Table-1 comparisons).
3. **Cross-classified groups count by cells** (race × sex gives ≥ 3 groups).
4. **Ordered groups** (age, income or education gradients) are groupwise. Flag whether the assessment uses the order (trend test, joinpoint by group) or not.
5. **Include informal displays.** Three or more SPD-group values set side by side, or a ranking, count as groupwise displays with no inferential assessment.
6. **Record what it resolves to.** Note whether the article also reports pairwise contrasts or per-group estimates against a referent. Note too whether the groupwise display is assessed only by such pairwise tests (e.g. t-tests of each group against a reference level).

**Recognition cues** (examples for readers, not a definition; seen in the pilot and the 2024 MMWR mirror):
- Pearson chi-square on an r × c table;
- survey-adjusted chi-square (Wald F, Wald chi-square, Rao-Scott, "corrected for survey design");
- a joint Wald test of a multi-level covariate in an adjusted model;
- Kruskal-Wallis or ANOVA;
- Cochran-Armitage or chi-square for trend;
- joinpoint by group;
- an interaction of a multi-level SPD with time or period;
- multinomial or ordinal outcome models.

These tests are rare. About 14 of 235 reports in 2024 mention chi-square, and many of those are 2 × 2.

**Proposed schema change.** Add a list `groupwise`. Each entry is `{"across": <SPD dimension(s)>, "n_groups": int, "ordered": bool, "assessment": "side by side" | "omnibus test" | "trend test" | "model joint test" | "interaction" | "per-group vs referent only", "resolves_to_finer": bool, "ev": [...]}`. Leave the list empty when there is none. Replace "When pair and group comparisons coexist, use the primary contrast" with "lead with group". `comparison.framework` keeps its current meaning for non-SPD comparisons.

**Open sub-questions for the owner:**
- (a) Should 3+ groups on **non-SPD** variables (vaccination status, periods, vaccine products or routes, regions) be indexed too, or stay out? Proposed: stay out of `groupwise`; they remain in `comparison`.
- (b) Do informal side-by-side displays count (rule 5), or only assessed differences? Proposed: count them, with `assessment: "side by side"`.

Decision (principle): **accepted by owner.**
Decision (rules 1–6 and schema): **accept**, with the clarification that "trend" in this setting doesn't typically include calendar time
Decision (a): **accept proposal**
Decision (b): **accept proposal**

## D2. Equity level-2 cap vs level 3 (brief contradicts example `mm6925a1`)
**Issue.** The brief caps "values only set side by side (even with framing words)" at level 2 and requires a *computed* contrast for 3. Example `mm6925a1` gives level 3 for case shares set beside population or birth shares. This split readers in mm7343a3, mm7346a2, mm7235e1, mm4935a2 and mm5020a5.
**Options.** (i) Keep the cap and correct the example to level 2. (ii) Treat a share set against its population share as a quantified shortfall that meets level 3.
**Proposed:** (i). It keeps one test: a contrast is quantified only when the article states a difference, ratio or shortfall. It also matches `comparison.scale` = null for side-by-side values.
Decision: **accept**

## D3. When age and sex are recorded
**Issue.** "Recorded always" was applied unevenly. Open cases: population-defining ages ("adults", ≥60, "per 100,000 aged <15"), sex as the population (pregnant persons), and articles with `data_focus` none (mm7236a2, mm7313a1, mm7218a7, mm4821a7).
**Proposed:** Record age and sex whenever they define the population (role `population`), including age bounds that only define a denominator, and also when `data_focus` is none. Use role `stratifier` or `descriptor` when results are given by age or sex. They still raise the level only under the existing rule.
Decision: **accept**

## D4. Other dimension edge cases
**Proposed:**
- Nativity or citizenship of cases inside an international report → `country of birth/language/nativity` (mm4943a3).
- An SPD named only in a recommendation is a `descriptor`, like an SPD named only in a limitation (mm4836a2).
- Composite stratifiers such as VFC eligibility (insurance plus AI/AN status) → list each component (mm7349a3).
- Employment-status categories (unemployed, retired, disabled) → SES (mm7235a2).
- Sampling-design strata (urbanization, minority density) → the dimension with role `covariate` (mm4903a1).

Decision: **accept**

## D5. Scope of `determinants_of_health`
**Issue.** Readers disagreed over:
- pandemic service disruptions (mm7242a2, mm7247a2);
- individual risk factors such as homelessness or incarceration in a case report (mm5015a3);
- a social determinant that is the *outcome*, e.g. medical-bill problems (mm7215a8, mm7218a7);
- drug regulation or market availability (mm7304a5);
- a screening policy (mm5011a3).

**Proposed:** Record a determinant when it is social, structural or political **and** the article offers it as an explanation of an outcome or a disparity, or analyzes it as the exposure or outcome. Do not record clinical or individual risk factors, or health-system disruptions with no equity framing. Regulation or policy counts only when offered as an explanation of a disparity.
Decision: **accept**; consider forking as "this reader interprets thus" + "this is evidence of how authors framed it", with index weight toward explicit author framing

## D6. `equity_action` "progress against a target"
**Issue.** Does any target count (UNAIDS 95%, WHO 85% treatment success, elimination goals), or only a disparity target? Raised in mm7212e1, mm7314a2, mm5011a3 and by readers in AR07 and AO03.
**Proposed:** Only a target defined as closing a gap, or a target for an SPD-defined population (e.g. a Healthy People target for a low-income population). General program targets do not count.
Decision: **accept**; comparison to a fixed referent is of interest for teaching, but it is separate from the equity attributes

## D7. `analytic_depth`: stratified vs "series over time by group"; false comparisons
**Proposed:**
- Period-to-period changes computed within levels of an SPD or other variable are `comparison` (a series by group), not `stratified/adjusted` (mm7219a1, mm7225a3).
- `stratified/adjusted` needs a contrast of interest estimated within levels of a third variable, or adjustment.
- Authors calling a two-way cross-tab "stratified" does not make it stratified (mm7246e1).
- Not comparisons: case-confirmation steps (a paired-sera rise, mm7322a2), classification cutpoints (a 10-SNP relatedness rule, mm7246a5), reference standards in diagnostic accuracy (mm7316a2), duplicate checks (mm4922a3), and compliance or inspection counts in outbreak reports (mm7302a2).

Decision: **accept**

## D8. `comparison.scale` when a test is reported but no difference or ratio is computed
**Proposed:** `null`. Scale follows the reported contrast, not the test. This matches the equity level-2 cap (mm7203a2, mm7207a5, mm7251a4, mm7228a3, mm7335a4).
Decision: **accept**; this one is of particular interest, as are all comparisons without contrasts, notwithstanding the possible difficulties of attaching a meausre of uncertainty or variability to a measured comparison

## D9. `data_focus` for ACIP recommendation and evidence-summary reports
**Proposed:** `yes` when the report computes estimates of its own (e.g. work-group pooled estimates, new tables computed from trial data). `limited` when it only tabulates estimates cited from elsewhere, even with CIs (mm7241e1, mm7249a3, mm7332e1, mm7349a3, mm7229a4).
Decision: **accept**; reports that produce quantitative claims (including, e.g., weighted summaries) are of greater interest than reports that synthesize or ledger evidence only qualitatively

## D10. `data_source_type` categories
**Proposed:**
- Classify by where the data come from, not by a WHO label. US laboratories in a WHO network → the US system (mm4948a1). `international surveillance` = non-US national or WHO systems about non-US populations (mm4943a3, mm7242a2). A non-US cohort or program → `other` (mm5011a3).
- Non-US national surveys (PHIA) → `national survey`, with setting `international` (mm7212e1).
- A sentinel network's own chart abstraction → `sentinel/network` only (mm7240a1, mm7339a2).
- Linked area indexes (SVI) are not data sources; note them under `equity_measurement` (mm7346a2).
- Questionnaires collected inside a surveillance program → the surveillance system (mm7208a2).
- Pharmacy testing programs used for VE (ICATT) → `sentinel/network` (mm7207a3).

Decision: **accept**; this is best effort, not crucial, more likely to characterize a report than to select for or against it

## D11. `setting`
**Proposed:** `mixed` = any combination of setting types, including US plus international cases (mm4923a3) and national results with state strata (mm7346a2). A single site such as a facility or ship → `US subnational` (mm7230a5). Data mainly from multinational trials → `international`, unless the report's own analysis is of US data (mm7241e1, mm7229a4, mm7349a3). A multistate outbreak found by national surveillance → `US national` (mm7305a5).
Decision: **accept**; this is best effort, not crucial, more likely to characterize a report than to select for or against it

## D12. Missing vocabulary
**Proposed:**
- `study_design`: medication-error or adverse-event cluster investigations → `outbreak investigation` (mm4835a2). Add a value `non-randomized trial` (mm7305a4). Evaluations of a surveillance indicator or nowcast → `mixed/other`, with a note (mm7346a3).
- Regression families: multinomial or generalized logit → `logistic`, with a note (mm7224a2).
- `outcome_types`: dollar outcomes and model-derived parameters (doubling time, growth rate) → `continuous` (mm7331a2, mm7224a2).

Decision: **accept**; study designs that are edge cases are most likely to be selected for their analytic methods

## D13. `outcome_types` edge cases
**Proposed:** Descriptive medians of population characteristics (e.g. age of cases) do not make an outcome `continuous`. Medians of the illness's own course (incubation, duration) do (mm5020a5, mm5042a3, mm7327a3). Event counts per period with no denominator → `count/rate` (mm7218a5, mm7303a3). A cumulative risk reported as % → `binary/categorical` (mm7320a3).
Decision: **accept**, with general expectation that "risk" is unitless because numerator and denominator dimensions are the same (e.g., persons with disease per person in population), and "rate" has a unit because numerator and denominator dimensions vary (e.g., counter per unit time)

## D14. `vaccine_outcome_type`
**Proposed:** Provider or public knowledge, attitudes and acceptability → `other` (mm7236a2). Supply, stockpile and distribution metrics → `other` (mm7316a1). A breakthrough-infection cluster with no VE computed → `other`, not `effectiveness` (mm7225a6). Animal-vaccine reports → `null` (mm7327a1).
Decision: **accept**; the operating notion behind "vaccine epidemiology" is characteristics that relate to population-level mitigation of conditions with measured vaccine effect

## D15. `condition_group`
**Proposed:** Include the vaccine's target disease when the report is about that vaccine (mm7348a3). Tobacco and alcohol use → `substance use`. Reserve `health behaviors` for physical activity, diet, sleep and screening (mm7334a2).
Decision: include alcohol and vaping use with other substances; treat tobacco use as its own category; treat physical activity, diet, sleep as `health behaviors`

## D16. Tests by CI inversion (owner's decision, after reviewing audit adjudications)
**Issue.** Readers and adjudicators split over whether VE, RR or OR results whose CIs exclude the null imply a test when the article states no judgment (mm7229a4, mm7205e1, mm4910a3). This decided `has_test` and `inference`, and it accounts for most of the audit's calibration differences.
**Owner's principle.** A CI that excludes (or includes) the null value is a valid test of that hypothesis **by inversion**, even when the article does not say so. The inferred test method depends on whether the **CI construction method** is named or described.
**Proposed rules for the brief** (section C, `test`):
1. Record a test `"name": "CI vs fixed value (inversion)"`, with `inferred_from` saying so, when a CI for a contrast with a defined null value (1 for ratios, 0 for differences and VE, a stated target) is reported **and** the article draws a conclusion from it (an effect, a difference, protection, "no difference" or "similar").
2. **Method:** when the CI construction is named or described (Wald, exact or Clopper-Pearson, Wilson, profile likelihood, Taylor series, bootstrap, model-based from a named regression), record it in `inferred_from` and set `inference.method_stated` true. Otherwise write "CI method not stated", and `method_stated` is false.
3. **Overlap is not a test.** Judgments from overlapping or non-overlapping CIs of two separate estimates are never recorded as a test. Record them under `inference` with a flag (`ci_overlap: true`) so they can be found. Exception: when the article says no other operational method was available, add a note saying so; it is still not a test.

**Open choice:** does rule 1 need the article to draw a conclusion from the CI, or does any reported CI for a contrast count as a test by inversion? Proposed: a conclusion is required. A bare CI with no claim is estimation (`inference` "CIs").
Decision (principle): **accepted by owner.**
Decision (rules 1–3): 
Decision (open choice): **owner: only when the article draws a conclusion from the CI.** Owner's note: MMWR authors are fairly consistent in naming hypothesis-test results inferred from CIs, and much less consistent in saying how the intervals were constructed. So expect `CI method not stated` to be common, and do not treat it as a reader error.

## D17. A design category for evaluation, program evaluation and monitoring (owner's decision)
**Issue.** No `study_design` value fits program or policy evaluations, evaluations of a surveillance system or indicator, or performance monitoring against targets (mm7346a3, mm7314a2). A non-randomized field test of an intervention (mm7305a4) keeps D12's `non-randomized trial`, unless the report calls itself an evaluation.
**Owner's position.** Admit a category for these, and prefer **the design the report assigns itself** over a design inferred from described characteristics.
**Proposed:**
- Add the value `evaluation or monitoring` (program, policy or intervention evaluation; surveillance-system or indicator evaluation; monitoring progress against targets). Routine surveillance reports stay `surveillance/descriptive`.
- General rule for `study_design`: when the report names its design ("evaluation", "case-control", "field trial", "cross-sectional survey"), use that, mapped to the nearest value. Infer only when no design is named, and note it.
- This preference applies to `study_design`. For method fields, owner note D5 (record "this reader interprets thus" separately from "how the authors framed it") is the model. D7's rule that an authors' "stratified" label does not decide `analytic_depth` still stands.

Decision: 

## D18. Production design: one Opus reader, a selective second reader (owner's decision, held loosely)
**Basis.** The audit (`reports/AUDIT_REPORT.md`): review mode anchors on the first reader and saves little.
**Owner's position.** Dispatch **one Opus reader per article**. Apply a **second reader selectively**, concentrating on uncertainty about **statistical methods** and **health equity**, and on **full reports published 2016–2024 inclusive**. Fields the owner called best effort (data source D10, setting D11, and similar characterizing fields) do not trigger a second read.
**Needed to make it work:**
- The reader must flag uncertainty by field, not only for the whole record. Proposed: a list `uncertain_fields`, restricted to the method fields (`procedures`, `regression_fields`, `inference`, `comparison`, `analytic_depth`, `groupwise`, `outcome_types`, `study_design`) and the equity fields (`level`, `dimensions`, `groupwise`). A trigger is any entry there, equity `level_confidence` below high, or an `alternative_reading` on a procedure.
- `article_type` and publication year already exist in the sample metadata.

**Open choice:** is "full reports 2016–2024" (a) a stratum where *every* report gets a second reader, or (b) a stratum where uncertainty triggers a second reader at a lower threshold (any flagged method or equity field), while older and other article types need low confidence? Scale: the 2024 mirror list has about 4,350 MMWR weekly articles for 2016–2024; by the pilot's mix, roughly 2,500 are full reports. So (a) means about 2,500 second reads, while (b) scales with flagged uncertainty. Proposed: (b).
Decision (design): **accepted by owner, held loosely.**
Decision (open choice): **owner: (b).** Uncertainty triggers a second reader more easily in full reports published 2016–2024. Owner's metadata counts **1,885 full reports** in that period. This segment is the primary value of the corpus; the rest is of interest but less likely to supply primary examples.

---

## Tooling fixes (no decision needed unless you object)
- **T1.** The arm-B review prompt must validate the *merged* record, not just parse it (two pilot failures).
- **T2.** Each parallel agent gets its own scratch folder (pilot collisions; already done in `RUN_ARMB_AUDIT.md`).
- **T3.** `merge_review.py`: reject unknown arguments, and drop or flag structural notes that a review contradicts.
- **T4.** `validate.py`: enforce the evidence `loc` vocabulary, and decide whether `level_alternative: null` is allowed for the judgment reader.
- **T5.** If D1's schema change is accepted: add `groupwise` to `validate.py` and `compare.py`, and give `examples/` one worked groupwise case.
Decision: **concur**

## Where the work happens
These decisions are recorded on the pilot branch (`mmwr-index-eval`), next to the evidence they answer. Brief v4, the tool changes and the second pilot go on a **new branch** after this branch is squash-merged, so that pilot 1's results stay paired with brief v3.
