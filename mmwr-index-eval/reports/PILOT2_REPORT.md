# MMWR index pilot 2: run report

Run date 2026-10-14. Orchestrated by a Claude Code cloud session following `RUN_PILOT2.md`, full sample (100 articles). Raw figures are in `p2_run_log.tsv`, `reports/p2_agreement.tsv`, `reports/p2_scores.tsv`, `reports/p2_routing.tsv`, `reports/p2_brief_issues.md` and `conditions_map.tsv`.

## Models and effort
| Role | Model id | Effort |
|---|---|---|
| Model check (step 0) | `claude-opus-5-5` | low (accepted) |
| Reader, arm `low` | `claude-opus-5-5` (self-reported in all 100 records) | low |
| Reader, arm `medium` | `claude-opus-5-5` (self-reported in all 100 records) | medium |
| Adjudicator | `claude-opus-5-5` | default |
| Condition grouping | `claude-haiku-5-5` | default |

## Articles done, refused, failed
- **Done: 100/100 in each arm** (52 reader agents, 26 batches × 2 arms). **Refused: 0. Failed: 0.**
- **First-pass validation (step 2): 200/200 records valid, 0 failures, no re-runs.** Readers validate each record before saving, so step 2 saw only fixed records. Readers did report failures they fixed on the way:
  - evidence quote over 25 words: 7 records across 4 agents (mm7226a9 low; 2 records in P2R04 medium; 3 in P2R07 medium; mm7301a1 low);
  - `equity.determinants_of_health: null` rejected (needs `{"v": null, "ev": []}`): 1 agent (P2R13 low);
  - `determinants_of_health` / `equity_action` written as list or boolean instead of `{v, ev}`: 1 agent (P2R15 low).
- **Tool-call blocks (not article refusals): 2.** A permission check flagged a reader's helper command as "PII data handling" twice (P2R05 low: a one-liner printing an example's fields; P2R18 low: paging an overflow file). Both readers re-read with direct file reads. The P2R18 low reader read only the first ~2 KB of `examples/mm7223a3.json`, so its "two examples" step was partial.
- **Compare (step 3):** 73 articles had some disagreement; 63 had a method or equity disagreement and were adjudicated (13 Opus agents, 5 articles each, the last 3).
- **Budget guard:** reads totalled 5.99M tokens (6.04M with the model check), under the 9M limit. The whole run logged **7.74M** (reads 5.99M, adjudication 1.63M, model check 0.05M, condition grouping 0.08M), inside the expected 6–8M.

## Tokens, time and tool uses per article
From `p2_run_log.tsv`. Batches are by stratum, and the 2023–24 batches mix priority and other articles, so cost can be split by stratum but not by segment. The priority segment (54 articles) is all inside 2023–24 (78 articles).

| Arm | Stratum | Agents | Articles | Tokens/article | Seconds/article | Tool uses/article |
|---|---|---|---|---|---|---|
| low | 2023–24 | 20 | 78 | 29,983 | 29.7 | 3.6 |
| medium | 2023–24 | 20 | 78 | 32,135 | 41.1 | 4.4 |
| low | 1999–2001 | 6 | 22 | 25,351 | 23.1 | 3.0 |
| medium | 1999–2001 | 6 | 22 | 26,485 | 32.1 | 3.7 |
| **low** | all | 26 | 100 | **28,964** | **28.3** | **3.4** |
| **medium** | all | 26 | 100 | **30,892** | **39.1** | **4.3** |
| adjudicator | – | 13 | 63 | 25,826 | 27.7 | 4.0 |

**Low vs medium:** −6% tokens, −28% time, −21% tool uses per article. About 52K of each agent's figure is fixed overhead (system prompt, tools, brief); on the variable part alone (agent mean minus 52K) low saves about 11%.

**Caveat on the metric (as in pilot 1).** `subagent_tokens` behaves like the agent's final context size, not billed tokens. Effort mostly changes thinking and output, which this metric may not capture, so the billed saving from `low` is probably larger than 6%. Wall time is the better proxy here: −28%.

## Error rates: low vs medium
From `reports/p2_scores.tsv`. Field-level error over the 63 adjudicated articles, method and equity fields only (27 fields per article; n = field verdicts per arm). An arm's value is an error when the adjudicator ruled against it on a disputed field, or listed it in `agreed_errors` (none were listed). Adjudicators saw the two records as A and B in random order.

| Segment | Stratum | Group | low | medium | n |
|---|---|---|---|---|---|
| all | all | **all fields** | **4.4%** | **1.8%** | 1,701 |
| all | all | method | 4.0% | 1.7% | 1,323 |
| all | all | equity | 5.8% | 1.9% | 378 |
| priority | 2023–24 | method | 4.3% | 1.8% | 819 |
| priority | 2023–24 | equity | 6.0% | 2.1% | 234 |
| other | 2023–24 | method | 2.8% | 3.2% | 252 |
| other | 2023–24 | equity | 6.9% | 1.4% | 72 |
| other | 1999–2001 | method | 4.4% | 0.0% | 252 |
| other | 1999–2001 | equity | 4.2% | 1.4% | 72 |

- **Counts:** low 75 field errors in 43 articles; medium 30 in 22 articles.
- **Not one outlier:** low's worst article (mm7332e1, 11 errors from one missed hazard-ratio model named only in a table note) does not explain the gap. Without it, low still has 64 errors to medium's 27.
- **Paired, by article:** low did worse in 37 articles and medium in 14 (3 ties); sign test p ≈ 0.002.
- **Fields:** low's excess is in `outcome_types` (9 vs 0), `equity_dimensions` (8 vs 1), `has_determinants` (7 vs 2), `comparison_scale` (5 vs 0) and `comparison_framework` (5 vs 2). `analytic_depth`, `procedure_classes` and the groupwise fields err about equally in both arms; those are mostly brief boundaries, not effort.
- **Only reversal:** non-priority 2023–24 method fields (2.8% vs 3.2%, n = 252: Notes from the Field and QuickStats), driven by mm7225a6 and mm7235a2 groupwise calls.
- **Equity level** itself agreed in 97/100 articles.

**Recommendation on effort: keep `medium` for the production reader.** `low` saves about a quarter of the wall time and a little of the token metric, but makes about 2.4 times as many method and equity errors, consistently across segments and strata. If cost forces a cut, `low` is least harmful on the 1999–2001 stratum's equity fields, but even there method errors rose from 0 to 4.4%.

**Small-sample caveat.** 63 adjudicated articles, one adjudicator per article (itself an Opus read), and errors are measured relative to the other arm. A value both arms got wrong only counts if the adjudicator lists it in `agreed_errors`; none were listed, so shared errors are undercounted for both arms. The direction (low worse) is clear; the size of the gap is uncertain.

## Routing
From `reports/p2_routing.tsv`. If one arm were the only primary read, `routed()` in `tools/pilot2.py` decides which articles go to a second reader:
- **priority segment:** any `uncertain_fields` entry, equity `level_confidence` below high, or an `alternative_reading`;
- **other articles:** equity `level_confidence` low, or two or more `uncertain_fields`.

The other arm stands in for the second reader.

| Primary arm | Segment | Articles | Routed | Share routed | Errors | Errors in routed articles |
|---|---|---|---|---|---|---|
| medium | priority | 54 | 46 | 85% | 20 | 19 (95%) |
| medium | other | 46 | 15 | 33% | 10 | 5 (50%) |
| medium | all | 100 | 61 | 61% | 30 | 24 (80%) |
| low | priority | 54 | 39 | 72% | 49 | 40 (82%) |
| low | other | 46 | 12 | 26% | 26 | 12 (46%) |
| low | all | 100 | 51 | 51% | 75 | 52 (69%) |

**The priority trigger is too loose.** With `medium` as the primary read it routes 85% of priority reports, which is close to option (a) in D18 (every report gets a second reader). Two signals drive it, and they mostly fire together:
- `equity.level` listed in `uncertain_fields` (43 of 100 medium records);
- `level_confidence: medium` (44 records).

The flags are only mildly selective: errors are spread across the articles roughly in proportion to how many are routed. Alternatives tried on the same data (medium as primary, priority segment):

| Trigger | Routed | Errors caught |
|---|---|---|
| current (any signal) | 46/54 | 19/20 |
| ignore `equity.level` when it is the only flag | 41/54 | 17/20 |
| only a method field in `uncertain_fields` | 41/54 | 17/20 |
| two or more `uncertain_fields`, or confidence low | 30/54 | 15/20 |

**The other-segment trigger is tight.** It routes a third of articles and catches half of the errors. The errors it misses are single-field disagreements, mostly `outcome_types`, `analytic_depth` and `groupwise` boundary calls in short reports. Applying the priority rule there instead would route 29/46 and catch 8/10.

**Readers flag uncertainty too readily for routing to save much.** With `medium` as the primary read there are only 30 errors in 100 articles, and the current rule would buy 61 second reads to catch 24 of them. Fixing the brief gaps below (most disagreements are brief gaps or ambiguity, not misreading) may cut errors more cheaply than second reads. If routing is kept, "two or more uncertain fields, or confidence low" for priority reports is a reasonable middle ground (56% routed, 75% of errors caught in this sample).

## How readers used the new fields (brief v4)
Counts per 100 records, low / medium.

| Field | low | medium |
|---|---|---|
| Records with any `groupwise` entry | 35 | 40 |
| `groupwise` entries | 68 | 82 |
| – assessment: side by side | 41 | 48 |
| – assessment: per-group vs referent only | 13 | 19 |
| – assessment: omnibus test | 13 | 14 |
| – assessment: interaction | 1 | 1 |
| – `resolves_to_finer` true | 18 | 24 |
| Records with a test by CI inversion | 12 | 13 |
| – of these, `method_stated` true | 5 | 8 |
| Records reporting CIs (`CIs` or `CIs and tests`) | 38 | 39 |
| – of these, `method_stated` true | 20 | 22 |
| `ci_overlap` true | 4 | 4 |
| `uncertain_fields` empty | 37 | 25 |
| `uncertain_fields`: 1 / 2 / 3+ entries | 26 / 28 / 9 | 30 / 31 / 14 |
| Equity `level_confidence` high / medium / low | 55 / 41 / 4 | 55 / 44 / 1 |
| `alternative_reading` on a procedure | 0 | 2 |

- **Fields listed in `uncertain_fields`** (low / medium): `equity.level` 42 / 43, `groupwise` 21 / 22, `comparison` 15 / 18, `analytic_depth` 9 / 17, `procedures` 6 / 10, `inference` 6 / 8, `study_design` 7 / 8, `outcome_types` 0 / 6, `equity.dimensions` 2 / 2, `regression_fields` 1 / 2. Medium flags more and states its doubts more specifically.
- **Condition roles** (low / medium mentions): outcome 107 / 118, vaccine target 35 / 40, population 26 / 24, exposure 11 / 6. Most records name one or two conditions; 2 / 3 records name none.
- **The CI method is often unstated:** in about half of the records that report CIs, and in most low-arm inversion tests.

## Condition grouping trial (step 6)
- **Terms:** 124 unique lowercased `conditions[].v` terms from `out/p2_medium/` (188 mentions), mapped by one Haiku agent (75.8K tokens, 98 s) into `conditions_map.tsv`.
- **Mapped: 121 of 124 (98%).** 99 terms got one group and 22 got two. No group outside the rubric's list was used.
- **Mapped to none (3):** two wordings of "serious adverse events and severe reactogenicity" (vaccine safety outcomes) and "urgent care or retail health clinic visits" (a care setting, not a condition).
- **Ambiguous (24 rows carry a note):**
  - open rubric questions: asthma → other chronic (provisional); hepatitis B and hepatitis C → other chronic (provisional); early introduction of complementary foods → health behaviors (the analogue of the breastfeeding question);
  - generic syndromes: acute respiratory illness, influenza-like illness, acute flaccid paralysis;
  - care-experience terms: discrimination or mistreatment during maternity care;
  - a screening activity: mammography screening → cancer;
  - a drug-induced hepatitis from TB treatment.
- **Rubric questions raised:**
  1. Settle asthma/COPD and viral hepatitis.
  2. Hepatitis B as a `vaccine target` conflicts with the open hepatitis question. Which rule wins?
  3. Should vaccine safety outcomes and care-setting terms stay ungrouped?
  4. Readers write the same concept in different wordings (two reactogenicity rows), so the map would benefit from a light normalization step before lookup.
- Most frequent groups: other acute 29, injury/poisoning 20, vaccine-preventable (other) 16, substance use 15, maternal/reproductive 10.

## Recurring brief issues
From reader handbacks (column `notes` of `p2_run_log.tsv`) and adjudicators (`reports/p2_brief_issues.md`, 50 articles).

1. **Typo.** §1 says "Two known false-hit patterns" but lists four. About 35 of 52 readers reported it.
2. **`data_focus` cannot be flagged.** It is not allowed in `uncertain_fields`, yet "limited" vs "yes" was the commonest unflaggable doubt. Cases: small case series (7–40 cases), ACIP or guidance reports that cite or pool estimates, progress reports, counts that are not health outcomes.
3. **"Limited" records.** It is unclear whether `outcome_types`, `analytic_depth` and `comparison` should be filled, or describe only cited evidence (6+ readers; several adjudicator gaps).
4. **Groupwise.**
   - The assessment list has no value for all-pairs tests without a referent (common in NCHS QuickStats).
   - `resolves_to_finer` is unclear under "per-group vs referent only".
   - Does case-series composition by an SPD count as "side by side"?
   - How should groups be counted when an SPD is crossed with a clinical variable?
5. **Equity level 2 cap vs levels 3/4.**
   - An SDOH used as the *outcome* (e.g. problems paying medical bills).
   - "Disproportionately affected" framing with named determinants but only side-by-side values.
   - SVI as a stratifier.
   - A shortfall from a target for an SES-defined population (Medicaid) with no SPD compared.
   - MSM outside an HIV setting.
   - Prisoners, and country income group, as SES.
6. **`has_determinants`.** Recommendation vs explanation ("need to promote access"), pandemic disruptions and their reversal, environmental exposures, cultural practice in a case report. One reader set it for an analysed exposure and for a controlled bias.
7. **Unstated survey weighting** (NHIS or BRFSS estimates that never say "weighted"). Readers split; the brief should say whether to record it.
8. **Comparison framework precedence** when a fixed referent (WHO or Healthy People target) coexists with pair or group contrasts.
9. **Inference edge cases.**
   - A methods line that states a nonoverlapping-CI rule when no claim compares CIs.
   - CIs alongside p-values.
   - A colloquial "significantly" with no test anywhere.
   - Bayesian P(Rt > 1) thresholds: test or quantization?
   - Whether a CI used as an inversion test gives "tests" or "CIs and tests".
10. **Outcome types.**
    - The "counts without denominator = count/rate" rule applied to the case total that frames a report's percentages (4 of 5 disagreements in one adjudicator's set).
    - Medians of intervals: continuous or event-time?
    - A mean of a 0–3 composite.
11. **Schema details not in the brief.**
    - No `abstract` evidence `loc` (readers used summary or text).
    - `determinants_of_health` and `equity_action` must be `{v, ev}` objects, and null `v` must be written as `{"v": null, "ev": []}`.
12. **Missing vocabulary.** No study design for awareness-month notices, systematic reviews or GRADE syntheses, or an evidence review inside guidance. Also open: Pearson correlation, spline terms for covariates (also raised in pilot 1), and imputation.
13. **Methods named only in table notes are easy to miss.** This produced the single largest error (mm7332e1, a hazard-ratio model).

## What went wrong
1. **Stale checkout.** The local branch was behind a force-updated `origin/mmwr-index-eval`, which carried the pilot-2 runbook. I reset to the remote before starting; nothing local was lost.
2. **Two reader tool calls were blocked** by a permission check as "PII data handling" (see above). No article was affected, but one reader read only part of the second worked example.
3. **The Haiku condition-grouping agent created an empty cloud session.** It called `create_session` with no parameters by mistake (`session_015FxjrkYBT5osSKqhrK7oeB`, idle, 0 tokens used). It was not archived and is left for the owner to remove.
4. **Cost by segment is not measurable.** The 2023–24 batches mix priority and non-priority articles, so per-article cost by segment cannot be separated. A future runbook could batch by segment.
5. **The disagreement view misses within-record inconsistencies.** It does not compare `comparison.across` or dimension roles. An adjudicator found B giving equity level 2 with no SPD in `comparison.across` (mm7320a3), which the brief's level ≥ 2 check should catch; `validate.py` could enforce it.
6. **Packet problems.**
   - mm6953a1 is marked retracted or corrected.
   - mm7240a3 has a garbled Male row in its table CSV.
   - mm7240a1 and mm7322a3 have results in supplementary tables or figures not in the packet.
   - Most 1999–2001 packets have no `tables/` (OCR only); mm5034a3's OCR "Table 2" is really Figure 1.
   - mm7305a4's "RR" values are odds ratios; both readers caught it.
7. **Many interim commits.** A session stop hook required committing after every turn, so the branch has many "in progress" commits before the final one.
