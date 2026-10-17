# Brief v5: decisions

This file turns pilot 2's findings (`reports/PILOT2_REPORT.md`, `reports/p2_brief_issues.md` with 50 adjudicator notes, and the readers' notes in `p2_run_log.tsv`) into decisions for `READER_BRIEF.md` v5. Each item gives the issue (with pilot-2 article ids), a **proposed** rule and a **Decision** line. Section numbers refer to brief v4.

**How to use it.** Edit the `Decision:` line of any item you care about: write `accept`, `reject`, `defer`, or your own rule. **A blank Decision line means the item is not yet reviewed (pending); only an explicit `accept` counts.** (Changed 2026-10-16 from v4's convention, where a blank meant accepted.) Items with sub-rules (a), (b), … can be decided together ("accept") or one by one ("accept; reject (c)").

**Why these items.** In pilot 2 most disagreements between the two Opus reads came from the brief, not from misreading: adjudicators ruled 25 of 135 disputed fields "brief gap" and 33 "ambiguity". The items below are ordered by how many errors they caused.

---

## Method fields

### V1. Let readers flag `data_focus` as uncertain
**Issue.** `data_focus` is the doubt readers raised most often and could not flag: `uncertain_fields` does not accept it (mm7229a4, mm7332e1, mm7241e1, mm7225a6, mm7303a3, mm7323a2, mm7327a1).
**Proposed.** Add `data_focus` to the `uncertain_fields` values. It routes like the other method fields.
Decision: accept

### V2. Where `data_focus` "limited" ends
**Issue.** Readers split on small case series and clusters (7, 23, 36 and 40 cases: mm7327a1, mm7302a2, mm7343a3, mm7225a6), on progress reports that mostly cite published estimates (mm7207a2), and on quantities that are not health outcomes (counts of countries adopting a surveillance system, mm7323a2; stockpile deliveries, mm7316a1).
**Proposed.**
- (a) **Size does not decide it; analysis does.** A case series or cluster of any size is `yes` when it reports its own counts or proportions **and** compares them (groups, periods, attack rates, exposed vs unexposed). It is `limited` when it only describes the cases.
- (b) **Cited vs own.** A report whose headline quantities are cited from other reports is `limited`, even when it adds a few counts of its own. It is `yes` when its headline quantities are computed from its own data or its own analysis of others' data (unchanged from v4).
- (c) **Program and system measures** (coverage, doses delivered, adoption, timeliness, completeness) count as quantities about public health when the report computes them from its own data: `yes`, or `limited` under (a) and (b).
Decision: accept

### V3. What a "limited" record fills in
**Issue.** The brief says what a `none` record leaves empty, but not what a `limited` one does. Readers filled method fields from cited evidence, from the article's own case counts, or not at all (mm4847a4, mm4923a3, mm5015a3, mm7219a4, mm7228a2, mm7249a3, mm7332e1).
**Proposed.**
- (a) Method fields describe **only what the article itself computes or reports as its own result**. Methods behind cited estimates (a manufacturer's hazard ratios, an FDA SCCS rate ratio) are not procedures; mention them in `notes` if they matter.
- (b) A single case or a descriptive case series: `outcome_types` from the case counts or measures reported (often binary/categorical); `analytic_depth` `summary`; `procedures` []; `comparison` framework "none" unless the article compares something itself.
- (c) A report of cited figures only: `outcome_types` [] and `analytic_depth` [], as for `none`.
Decision: accept

### V4. Case totals and `outcome_types`
**Issue.** "Event counts per period with no denominator: count/rate" was read as covering the total number of cases that frames a report's percentages. That caused 4 of the 5 disagreements in one adjudicator's set (mm4917a4, mm4922a3, mm4923a3, mm4943a3).
**Proposed.**
- (a) A case total that only serves as the denominator of percentages does **not** add count/rate. Count/rate needs counts reported as an outcome: by period, place or group (cases by year, deaths by state).
- (b) A single descriptive mean or median does not make an outcome continuous unless a claim rests on it, i.e. it is compared or interpreted (mm6953a1, mm7316a5). A score analyzed as a mean (e.g. a 0–3 composite) is continuous (mm7230a1).
Decision: accept

### V5. When `analytic_depth` earns "summary"
**Issue.** Readers gave `summary` for sample sizes, enrollment counts, the baseline end of a period contrast, and quotes that are themselves comparisons (mm7153a1, mm7303a5, mm7335a4, mm7226a9).
**Proposed.** `summary` needs a single-group quantity **presented as a result**, e.g. an overall prevalence, a national percentage, a seroresponse range. Sample or enrollment sizes and the reference value of a contrast do not earn it.
Decision: accept

### V6. Groupwise assessment values and counting
**Issue.** The assessment list has no value for all-pairs tests without a referent (common in NCHS QuickStats) or for each-level-vs-rest tests. Readers split on `resolves_to_finer` when the only assessment is per-group vs referent, and on results that both rank all groups and give some ratios (mm7218a7, mm6953a1, mm7303a5, mm7334a2, mm7339a2, mm7251a4, mm7207a5, mm7225a6, mm7235a2, mm7332e1).
**Proposed.**
- (a) **New value `pairwise (all pairs)`:** every pair of groups tested, with no single referent. **Each level vs the rest** counts as `per-group vs referent only`.
- (b) **Precedence** when one result fits several values: model joint test > omnibus test > trend test > pairwise (all pairs) > per-group vs referent only > side by side. Record the first that applies. `resolves_to_finer` captures the finer assessments.
- (c) **`resolves_to_finer` is true** whenever the assessment is itself pairwise or per-group (the result is already finer than groupwise).
- (d) **Composition is not groupwise.** A case series' distribution across SPD groups (shares of cases by race, with no outcome measured per group) is a descriptor, not a groupwise entry. Groupwise needs an outcome measured in each group: a rate, proportion or mean per group.
- (e) **SPD crossed with a clinical variable** (e.g. age × medical risk): count the SPD groups compared within one level of the other variable. List only the SPD in `across`.
- (f) A blanket "differences were determined using t-tests" under "higher than most other groups" claims counts as `per-group vs referent only`, referent as named or "unnamed".
Decision: accept

### V7. Which comparison framework leads
**Issue.** The brief settles pair vs group but not a fixed referent (a WHO or Healthy People target) coexisting with pair or group contrasts (mm5011a3, mm5034a3, mm7313a1, mm7202a2). Within-person serial values judged against thresholds (mm7330a2) and an indicator compared with later complete data (mm7346a3) were also unsettled.
**Proposed.**
- (a) Lead with the framework of the **Summary's headline claim**. If that does not decide it: group > pair > fixed referent. Always record a target in `referent`, whichever framework leads.
- (b) Repeated measurements over time (one person or one series) are a period comparison: pair for 2 time points, group for 3 or more. Thresholds they are judged against go in `referent`.
- (c) In an indicator or nowcast evaluation, comparing the indicator with later complete data is a comparison (pair or group by period), not a reference standard.
Decision: accept, with refinement for (b) and (c); the question of pairwise or groupwise for characterizing equity pertains primarily to groups compared on the basis of social standing; for individual persons or other cross-group comparisons, the question of pairwise or groupwise is about a statistical procedure apart from equity considerations
*Implementation:* `comparison.framework` describes the statistical comparison, whatever is compared. `groupwise` entries and the equity reading apply only to groups defined by social position. A person's repeated measurements, or an indicator compared with later data, are comparisons, never groupwise entries.

### V8. Inference edge cases
**Issue.** Readers split on stray significance language, method lines about CI overlap, Bayesian probabilities and the combined value (mm7242a2, mm7248a2, mm7241e1, mm7346a3, mm4910a3, mm7320a3).
**Proposed.**
- (a) **"Significantly" with no test anywhere.** It implies an unnamed test only when the article elsewhere reports p-values, CIs or a testing method. A colloquial "significantly" in a report with none of these is not a test; note it in `view_issues`.
- (b) **Overlap rule stated, not used.** A methods line saying non-overlapping CIs were "considered significant" sets `ci_overlap` only when some claim actually rests on comparing intervals.
- (c) **Bayesian probabilities against a fixed value** (e.g. P(Rt > 1) binned into trend categories) are quantization of a model output, not a test.
- (d) **A CI used as an inversion test** gives `inference` "CIs and tests": the reported CI contributes "CIs" and the inversion adds "tests".
- (e) **CIs alongside p-values.** Record the named test. Add a CI-inversion test only when a conclusion rests on the CI itself.
Decision: accept
*Implementation, with the owner's D16 rule-3 decision (2026-10-16):* a claim resting on CI overlap is recorded as a procedure of class `test`, `"name": "CI overlap assertion"`, and counts toward `inference` "tests"; `ci_overlap: true` is kept so these can be told apart from formal tests. (b) still applies: only when a claim actually rests on the overlap.

### V9. Procedure edge cases
**Issue.** Several recurring calls are not in the procedure lists (mm7205e1, mm7316a3, mm7207a3, mm7339a2, mm7207a6, mm7308a5, mm7305a4, mm7225a3, mm7234a6, mm4948a1, mm7332e1).
**Proposed.**
- (a) **Case-classifying lab proxies and linkage** (SGTF for a variant, phylogenetic linkage that decides case membership) are case definitions, not procedures.
- (b) **Withholding imprecise or small-cell estimates** (CI-width or relative-SE rules, small-cell suppression) is `transformation` (censoring or suppression). This is already the rule; readers missed it.
- (c) **Imputation:** multiple or model-based imputation whose values enter the estimates is a procedure of class `other`. A single fixed fill-in is data cleaning.
- (d) **Spline terms for covariates** inside a model are part of the model specification, not a transformation (also raised in pilot 1). Splines drawn as trends stay `transformation`.
- (e) **Stated model vs reported estimates.** When a stated model contradicts its reported estimates (a log-binomial "RR" that reproduces ORs, mm7305a4), follow the stated model, give the other family as `alternative_reading`, and note it in `view_issues`. Both readers already did this.
- (f) **Weighted means or medians of summary values** (e.g. a median weighted by jurisdiction counts) are arithmetic, not `weighting`. `weighting` is reserved for survey weights, IPTW, raking and poststratification.
- (g) **Correlation coefficients** (Pearson, Spearman) are class `other`. A test of a correlation is a `test`.
- (h) **A regression that only sets a threshold** (e.g. a Serfling baseline for excess deaths) is `ancillary`.
- (i) **Methods named only in table or figure notes.** Before writing `procedures`, read every table and figure note. One missed hazard-ratio model in a Table 1 note (mm7332e1) produced the run's largest single error, 8 fields.
Decision: accept, especially (i)
*Implementation:* the table- and figure-notes check becomes a numbered step in the brief's rules (§3) and in the reader prompt.

### V10. Survey weighting that is never stated
**Issue.** Readers split on recording weighting for NHIS or BRFSS estimates that never say "weighted" (mm7215a8, mm7218a7, mm7207a6). Adjudicators sided with not recording it.
**Proposed.** Record `weighting` only when the article states it (weights, weighted estimates, survey design, survey software). "Silent is not false" applies: do not infer weighting from the data source alone. `nonindependence_approach` "complex survey design" keeps its v4 shortcut for national surveys, but only when a regression is present.
Decision: accept

### V11. `nonindependence_approach` when the article is silent
**Issue.** The other `regression_fields` have defaults for silence; this one does not (mm7346a2: null vs "none").
**Proposed.** `null` when the article says nothing. Use "none" only when the article indicates independent units, or the model plainly has none to account for.
Decision: accept

### V12. `study_design` vocabulary and boundaries
**Issue.** There is no value for evidence reviews inside guidance (systematic review, GRADE, evidence to recommendations) or for announcements. "Progress Toward" reports and program-operations reports were unsettled (mm4847a4, mm7228a2, mm7241e1, mm7207a2, mm7316a1, mm7303a3).
**Proposed.**
- (a) Add `evidence review` (systematic review, GRADE, evidence-to-recommendations summaries).
- (b) Announcements and awareness notices: `mixed/other`, with `data_focus` none or limited. Do not infer a design from figures cited from other reports.
- (c) Reports on program operations or progress, measured against stated goals or targets: `evaluation or monitoring`. Without goals or targets: `surveillance/descriptive`.
- (d) A case series nested inside an outbreak investigation: `outbreak investigation` only.
Decision: accept, with refinement for (a): consider including scholarly synthesis to accommodate reviews that might not be interpreted as pertaining to evidence
*Implementation:* the new value is `review or synthesis` (systematic reviews, GRADE and evidence-to-recommendation summaries, scholarly or narrative reviews and syntheses).

## Equity fields

### V13. The level-2 cap against levels 3 and 4
**Issue.** The most frequent equity dispute. Framing words, named determinants or an SDOH measure appear beside contrasts that are only set side by side (mm7343a3, mm7207a5, mm7346a2, mm7215a8, mm7218a7, mm7246e1, mm7212a1, mm5020a5).
**Proposed.**
- (a) **The cap holds.** Values only set side by side stay at level 2, whatever the framing ("disproportionately affected") and whatever determinants are named. Record the determinants anyway; `level_alternative` carries the higher reading.
- (b) **SDOH as the outcome.** Extend the level-3 clause for SDOH exposures to SDOH outcomes (e.g. problems paying medical bills): quantified contrasts across SPD groups meet level 3 without the word "disparity". Side-by-side values stay capped at 2, so most QuickStats remain level 2.
- (c) **"More than twice" is a computed ratio.** A stated multiple or difference ("more than twice", "3 percentage points higher") counts as quantified.
Decision: accept, with note about (a): in practice, i expect to use the binary equity category less often than the reported level and mode of comparison, so the distinction here is worth keeping precise but doesn't lose much operationally
*Implementation:* no change to the rule.

### V14. A target for an SPD-defined population with no groups compared
**Issue.** mm7314a2: a coverage policy for an SES-defined population (Medicaid) is measured against a Healthy People target, with laws named as explanations, but no SPD groups are compared. Both readers gave level 1 with low confidence, offering 3 or 4 as the alternative.
**Proposed.** A shortfall from a target set for an SPD-defined population is a quantified contrast. Put that SPD in `comparison.across` with the target as `referent`. It can reach level 3 (framed) or 4 (determinants named), and `equity_action` is true. This matches v4's `equity_action` rule.
Decision: accept

### V15. Dimension coding cases
**Issue.** Several groups recur without a rule (mm7324a4, mm5042a3, mm7207a2, mm7327a1, mm5015a3, mm7301a1, mm7212a1, mm7236a2, mm4943a3).
**Proposed.**
- (a) A **composite sex-at-birth / gender-identity variable** (cisgender man, transgender woman, …) is coded as SOGI only, not also as sex.
- (b) **MSM** is SOGI when the article frames the group by sexual orientation or community ("gay and bisexual men"). As a behavioral transmission category it is not SOGI, in HIV or any other infection.
- (c) **Country income group** (World Bank level, Gavi eligibility) describes countries, not persons: not an SPD. Note it under `equity_measurement`.
- (d) **Rurality** counts only for human populations, and not when it only stands for an outbreak's location.
- (e) **One patient's social position in a case report** is a `descriptor` (level 1 at most).
- (f) **Only the article's own population definition** makes a `population` SPD. An eligibility pathway quoted from a program's general description (e.g. Medicare via disability) does not.
- (g) **National-origin subgroups of a race** (e.g. Asian Indian, Chinese) are race/ethnicity, not nativity.
- (h) **Age bounds of the patients a provider survey asks about** define the population: age, role `population`.
- (i) **Citizenship of cases abroad** (mm4943a3): keep v4's rule (nativity, descriptor, level 1).
Decision: accept, with refinement to (a) and (b): somehow capture distinction between how one identifies vs how others perceive, as with sex-at-birth or sexual behavior apart from identity
*Implementation (owner concurred 2026-10-16):* a new optional attribute `basis` on equity dimension entries for sex, SOGI and race/ethnicity: self-identified · assigned or observed · behavior · not stated. (a) A composite sex-at-birth/gender-identity variable is SOGI with basis self-identified; its sex-at-birth component is sex with basis assigned or observed. (b) MSM and sex of partners are SOGI with basis behavior, recorded rather than left out; the equity level still depends on framing.

### V16. Scope of `determinants_of_health`
**Issue.** `has_determinants` caused 11 disagreements, tied with `equity_dimensions` for the most of any equity field (mm4834a2, mm4836a2, mm5016a4, mm7219a1, mm7234a6, mm7212a1, mm7330a2, mm7248a2, mm7251a4, mm7301a1).
**Proposed.** Narrow the inclusion rule to: a determinant offered as an explanation **of a disparity or of an SPD group's outcome**, or analyzed as the exposure or outcome. Then:
- (a) Explanations of an overall trend with no tie to social position (an economic downturn, pandemic disruptions **and their reversal**) do not count.
- (b) Environmental exposures (wildfire smoke, extreme weather) count only when framed as unequal exposure by social position.
- (c) Congregate or crowded living (shelters, prisons) counts when offered as an explanation for a group defined by social position.
- (d) A cultural or household practice in a case report is an individual risk factor, not a determinant.
- (e) Access offered as the explanation of a group difference ("more ready access to medical care") counts; a recommendation to improve access does not (v4 rule).
- (f) The analyzed exposure is recorded only if it is itself a social or structural determinant. A bias the authors say they controlled for is not a determinant.
Decision: accept

## Routing, conditions and tooling

### V17. Routing trigger (amends D18; placeholder)
**Issue.** In pilot 2 the trigger routed 85% of priority reports. It caught 19 of 20 errors, but it was nearly every report.
**Proposed.** Priority segment: route when two or more `uncertain_fields` are listed, or equity `level_confidence` is low. In pilot 2 this routed 56% and caught 75% of errors. Other articles: unchanged. Re-examine after brief v5, since fewer brief gaps should mean fewer flags.
Decision: **owner: accept as a placeholder** (2026-10-14).

### V18. Conditions: vaccine safety, wording, care settings
**Issue.** The grouping pass left vaccine safety outcomes and a care-setting term ungrouped, and readers wrote one concept in several wordings (`conditions_map.tsv`).
**Proposed.**
- (a) **Vaccine safety (owner: group it).** Add a group `vaccine safety` for adverse events following immunization: reactogenicity, serious adverse events, and named events studied as vaccine adverse events (e.g. Guillain-Barré syndrome, myocarditis). A named event also keeps its own group. The join can use the record itself: a condition with role `outcome` in a record whose `vaccine_outcome_type` is safety also maps to `vaccine safety`.
- (b) **Normalize wordings (owner: yes).** The grouping pass first writes a `canonical` term for each reader term, then groups the canonical term. Normalization: lowercase; drop articles and word order in "A and B" lists; spell out abbreviations (RSV → respiratory syncytial virus); singular forms. `conditions_map.tsv` gains a `canonical` column.
- (c) **Care-setting and utilization terms** (e.g. "urgent care or retail health clinic visits"). These are not conditions, so the fix belongs in the reader brief. Health care use (visits, hospitalizations, a care setting) is recorded as the **condition that drives it**, or left out when the article names none. The question of interest lives in `public_health_question` and `outcome_types`. In the rubric, any such term that slips through maps to no group.
  - *The alternative* is a `health care use` group. It is worth it only if you expect to search the index for utilization reports by condition group; the full text and `public_health_question` already make them findable.
Decision (a): **owner: group vaccine safety** (rule as proposed unless edited)
Decision (b): **owner: normalize** (method as proposed unless edited)
Decision (c): accept: care-setting are not conditions
*Implementation:* the reader brief says health care use and care settings are not conditions; record the condition that drives them, or none.

### V19. Routing after the confirmation run (replaces V17)
**Issue.** In the confirmation run, v5 readers flagged far less uncertainty: V17 routed 6 of 66 articles, holding none of the 14 errors (`reports/CONFIRM_V5_REPORT.md`).
**Decision (owner, 2026-10-16): no systematic second reader.** Each article gets one v5 read at `medium`. Quality is checked by auditing a random sample of production reads, plus a targeted check of groupwise entries (where the v5 errors were). `uncertain_fields` and `level_confidence` stay in the record for analysis and audit sampling.
**Owner's note on production order:** gate by MMWR volume, starting with volumes 73 down to 65 (2024 back to 2016), then relax the gating for earlier volumes. This also manages cost and lets indexing of earlier years be deferred if warranted.
Decision: **owner: accept** (2026-10-16).

### V20. Brief v5.1 (wording fixes from the confirmation run)
**Issue.** The confirmation run's readers and adjudicators found a few rules that conflict or are silent (`reports/CONFIRM_V5_REPORT.md`, "Recurring brief issues", items 1–6).
**Change.** `READER_BRIEF.md` v5.1:
- the headline rule for reports without a Summary;
- the headline rule before "group > pair";
- targets recorded only when a claim compares against them;
- rule 2 reworded, with cited methods never procedures at any `data_focus` level (supersedes the V9(i) example);
- groupwise: rule 2 governs whenever a test is reported per SPD level; `pairwise (all pairs)` only when every pair is shown;
- SDOH outcomes stay capped at 2 when only side by side;
- repeated cross-sections with a change headline add `before/after or trend`;
- case-series totals and case reports without counts;
- preventive-service uptake as a determinant;
- implicit CI-overlap claims;
- unframed functional-limitation rows;
- monitored conditions as `outcome`;
- imputation without a stated method.

No schema change.
Decision: ~~**owner: requested** (2026-10-16).~~ Accept.

### V21. Brief v5.2 (groupwise clarifications from the vol 73 audit)
**Issue.** The vol 73 audit (`reports/VOL73_REPORT.md`) found 5 wrong groupwise fields in 3 records, and adjudicators logged 11 brief notes. Three notes are tied to those errors, or to most of the mechanical check's false flags. The owner accepted a v5.2 limited to them (G2, 2026-10-17); the other notes stay open for the audit to watch.
**Change.** `READER_BRIEF.md` v5.2, section F:
- `per-group vs referent only` also covers a CI-overlap rule stated once under "highest", "higher" or "differed" claims across the groups, which also gives a `CI overlap assertion` procedure (mm7309a3, mm7324a1: recorded as side by side);
- rule 6: rule 2 governs only when the SPD is a margin of the test; a non-SPD contrast tested within each SPD level makes no entry by itself (mm7324a3, and most false flags);
- rule 7: program target groups that mix age bands with a clinical group are not SPD groups, unless 3 or more age groups are compared within one clinical level (mm7339a1).

`API_PREAMBLE.md` names version 5.2. No schema or validator change. Applies from vol 72; vol 73 stays on v5.1 (its prefix is pinned).
Decision:

---

## Tooling fixes (no decision needed unless you object)
- **T1.** Fix §1 of the brief: "Two known false-hit patterns" lists four (reported by 35 of 52 readers).
- **T2.** Brief v5 shows the exact shape of `{v, ev}` fields, including the empty form (`"determinants_of_health": {"v": null, "ev": []}`). The Abstract of a full report is quoted with `loc: "summary"`.
- **T3.** `validate.py` enforces the brief's own checks: equity level ≥ 2 needs an SPD in `comparison.across` (mm7320a3 passed without one), and level ≥ 3 needs `comparison.scale` not null.
- **T4.** `compare.py` and `pilot2.py` also compare `comparison.across` and dimension roles, so inconsistencies within a record reach adjudication.
- **T5.** `pilot2.py` `routed()` is updated to V17. Runbooks batch articles by segment, so cost can be split by segment.
- **T6.** Readers and adjudicators read files with the Read tool and use Bash only for repo scripts (already in `CLAUDE.md` and `RUN_PILOT2.md`).

## Confirmation run (after brief v5)
Re-read only the articles these items cite, once each at `medium`: 66 articles. For 54 of them, score the v5 read against the existing pilot-2 adjudications. The other 12 were raised only in readers' notes, where both reads agreed, and need one fresh adjudication pass. That is roughly a third of pilot 2's cost. The ids are listed under each item above.

## Where the work happens
These decisions are recorded on the pilot branch (`mmwr-index-eval`), next to the evidence they answer, as for v4. Brief v5, the tool changes and the confirmation run go on a new branch after this one is squash-merged.
