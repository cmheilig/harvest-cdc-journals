# MMWR index cloud pilot: run report

Run date 2026-10-11. Orchestrated by a Claude Code cloud session following `RUN_CLOUD_PILOT.md`. Raw figures are in `run_log.tsv`, `reports/scores.tsv`, `reports/agreement.tsv` and `reports/brief_issues.md`.

## Models used
| Role | Model id (step 0 check; also self-reported in every record) |
|---|---|
| Structural reader R1 | `claude-haiku-5-5` (100/100 records) |
| Judgment reader R3, full (arm A) and review (arm B) | `claude-opus-5-5` (50 `out/opus/` + 50 `out/merged/` records) |
| Adjudicator | `claude-opus-5-5` |

Both are 5.5 models, so step 0 passed.

## Articles done, refused, failed
- **Done: 100/100.** Arm A has 50 Haiku and 50 Opus full records. Arm B has 50 Haiku records, 50 Opus reviews and 50 merged records. All 200 scored records pass `tools/validate.py`.
- **Refused: 0.** No subagent reported a refusal.
- **Failed: 0 permanently.** In arm B, two merged records (`mm7302a2`, `mm7330a2`, both BR10) first failed validation because the Opus review edits contained evidence quotes over 25 words. Per step 3, I re-ran the reviewer once for each article alone, and both re-runs passed.
- **Adjudicated: 81/100** (every article with at least one disagreement), in 17 Opus subagents with none refused. 49 of the 81 are arm A and 32 are arm B.
- **Budget guard:** the arm-A total was 3.78M tokens (3.68M for readers alone), under the 6M limit, so arm B proceeded. The whole run logged 9.38M.

## Tokens and time per role, arm and stratum
Means per article, from `run_log.tsv`. Strata: R = 2023–24 and O = 1999–2001.

| Role | Arm | Stratum | Agents | Articles | Tokens/article | Seconds/article | Tool uses/article |
|---|---|---|---|---|---|---|---|
| structural (Haiku) | A | R | 10 | 39 | 43,924 | 80.9 | 6.2 |
| structural (Haiku) | A | O | 3 | 11 | 35,605 | 58.1 | 4.0 |
| structural (Haiku) | B | R | 10 | 39 | 42,835 | 75.3 | 6.4 |
| structural (Haiku) | B | O | 3 | 11 | 31,249 | 77.8 | 4.5 |
| judgment full (Opus) | A | R | 10 | 39 | 32,841 | 56.8 | 4.8 |
| judgment full (Opus) | A | O | 3 | 11 | 26,396 | 42.6 | 3.7 |
| judgment review (Opus) | B | R | 10 (+2 re-runs) | 39 | 30,647 (32,937 with re-runs) | 36.7 | 4.4 |
| judgment review (Opus) | B | O | 3 | 11 | 25,297 | 26.4 | 3.5 |
| adjudicator (Opus) | – | – | 17 | 81 | 24,138 | 32.4 | 3.9 |

**Caveat on the token metric.** `subagent_tokens` is the per-agent figure the harness reports. It behaves like the agent's final context size, not cumulative billed tokens. For example, a one-line model-check agent scored about 52K, and a four-article batch about 100–200K. Read it as a *relative* effort indicator, not a cost. About 52K of each agent's figure is fixed overhead (system prompt and tools), which adds about 13K per article in a four-article batch. Cumulative, cache-aware billed usage was not available to the orchestrator.

**Unexpected result:** Haiku's figures were *higher* than Opus's (about 43K vs 33K per article in the R stratum). Haiku made more tool calls (6.2 vs 4.8 per article) and re-checked more, so its context grew larger. Whether Haiku is still cheaper depends on per-token price, which this metric cannot settle.

## Error rates by role, arm and stratum
From `reports/scores.tsv`. These are field-level error rates over adjudicated articles: n = field verdicts, 27 normalized fields per article.

| Arm | Stratum | Structural (Haiku) | Judgment (Opus full / merged) | n |
|---|---|---|---|---|
| A | R (2023–24) | 5.8% | 1.7% | 1,026 |
| A | O (1999–2001) | 7.4% | 0.7% | 297 |
| A | all | 6.2% | 1.4% | 1,323 |
| B | R (2023–24) | 4.8% | 0.3% | 702 |
| B | O (1999–2001) | 4.9% | 0.0% | 162 |
| B | all | 4.9% | 0.2% | 864 |

Fields with the highest error rates: Haiku's `equity_dimensions` errors are 37% in arm A and 47% in arm B. Opus's are 10% in arm A and 3% in arm B. Haiku's next highest are A `comparison_scale` (18%) and B `data_source_type` (19%). Haiku's `analytic_depth` errors in A-O are 36% (n=11).

Agreed errors (both readers wrong, flagged by an adjudicator): 5 in total. They are in mm4823a2 (procedures), mm7305a4 (procedures: the article's "RR" values are actually crude ORs), mm7241e1 (analytic_depth, arguable), mm4834a2 (unadjusted_results) and mm7225a3 (data_source_type). Adjudicators rated the readers' justification good 121 times and adequate 41 times, never weak.

## The 10 fields with the most disagreement
Articles where the two readers disagreed (from `reports/agreement.tsv`):

| Field | Arm A | Arm B | Total |
|---|---|---|---|
| equity_dimensions | 24 | 19 | 43 |
| analytic_depth | 11 | 9 | 20 |
| data_source_type | 13 | 7 | 20 |
| outcome_types | 13 | 3 | 16 |
| comparison_framework | 11 | 5 | 16 |
| study_design | 11 | 3 | 14 |
| comparison_scale | 9 | 4 | 13 |
| has_determinants | 8 | 3 | 11 |
| procedure_classes | 8 | 1 | 9 |
| condition_group | 4 | 3 | 7 |

`equity_level` disagreed in only 5 articles (4 in A, 1 in B). `has_regression`, `has_standardization`, `has_weighting`, `rf_adjusted_results` and `rf_interaction_modeled` never disagreed. The core method fields are stable.

Adjudicator verdicts over 200 disputed fields: ambiguity 57, reader error 53, brief gap 52, omission 38, view issue 0. More than half the disagreements (ambiguity plus brief gap, 109 of 200) come from the brief rather than from misreading.

## Recurring `brief_issues`
The full list is in `reports/brief_issues.md` (67 articles). Readers' handbacks raised the same themes.
1. **Equity level 2 cap vs level 3.** The brief caps "values only set side by side" at level 2, but worked example `mm6925a1` gives 3 for case shares against population shares. Readers and adjudicators met this repeatedly (mm7343a3, mm7346a2, mm7235e1, mm4935a2). "Quantified" and "interpreted" are undefined, and so is whether a contrast cited from earlier work counts.
2. **"Age and sex are recorded always" is applied unevenly.** This includes population-defining ages (adults, ≥60, per 100,000 aged <15), dimensions named only in limitations or recommendations, nativity, and composite stratifiers (VFC eligibility).
3. **The scope of `determinants_of_health`.** Open cases: pandemic service disruptions, individual risk factors (homelessness, incarceration), a social determinant used as the outcome, and drug regulation or market availability.
4. **The `equity_action` "progress against a target" clause.** Does it need a disparity target, or does any target count (UNAIDS 95%, Healthy People, WHO 85%)? Raised by at least four readers and adjudicators.
5. **`data_source_type` categories.** The "WHO/international → international surveillance" shortcut misfits domestic WHO labs, non-US national programs and cohorts. Other open cases: non-US national surveys (PHIA), a sentinel network's own chart abstraction, linked indices (SVI), and questionnaires collected inside surveillance.
6. **`analytic_depth` stratified vs "series over time by group".** Readers also accept authors' use of "stratified" for two-way cross-tabs, and count case-confirmation steps, lab cutpoints (SNP thresholds, paired-sera rise) and reference standards as comparisons.
7. **`comparison_scale` when a test is reported but no difference or ratio is computed.** The cap at level 2 implies null; readers often put "absolute".
8. **`data_focus` "limited" vs "yes" for ACIP recommendation and evidence-summary reports** that tabulate cited estimates with CIs.
9. **Missing vocabulary.** There is no `study_design` value for field investigations of medication errors, non-randomized field trials, or surveillance-indicator evaluation. Other gaps: no multinomial family; no outcome type for cost outcomes or model-derived parameters (doubling time); no `setting` value for single sites (a cruise ship), US-plus-foreign case mixes, or multinational trial data; no `vaccine_outcome_type` rule for provider-attitude surveys, supply metrics or breakthrough clusters.
10. **Outcome types:** whether descriptive medians (age, incubation) make an outcome "continuous", and whether counts without denominators are count/rate or binary/categorical.

## Arm A vs arm B
**Quality.** Arm B's merged judgment record scored better than arm A's full Opus record: 0.2% vs 1.4% field error. Arm B's Haiku record also scored better: 4.9% vs 6.2%. Arm B had far fewer disagreements: 32 of 50 articles and 62 fields, against 49 of 50 articles and 138 fields in arm A.

**But this comparison favours B by design.** The merged record *inherits* every Haiku value the reviewer did not change. Inherited values agree with Haiku automatically and count as correct unless an adjudicator flags them in `agreed_errors`, and adjudicators mostly examine disputed fields. Arm B's 18 fully agreeing articles were not adjudicated or scored at all. So B's low error rate partly reflects errors that both records share and no one checked, rather than independent confirmation. In arm A, two independent readers expose more errors. **Conclusion:** on what was measured, B is at least as good as A for the judgment record. A fair test would audit a sample of arm-B articles with a blind, independent full read before adopting B.

**Judgment-reader effort per article.**

| Stratum | Arm A full read | Arm B review | Difference |
|---|---|---|---|
| R | 32,841 tokens, 56.8 s | 30,647 tokens, 36.7 s | −7% tokens, −35% time |
| O | 26,396 tokens, 42.6 s | 25,297 tokens, 26.4 s | −4% tokens, −38% time |

In this metric the saving is small, because review mode still reads the brief, the examples, the full packet *and* the Haiku record. Wall time and tool calls fall more clearly. Billed tokens would also scale with turns, so they probably fall more than the context metric shows, but this run cannot measure that. Review mode made few changes: 0–4 per article, median 1. Three reviews used `agree_data_focus_none`. Arm B also needed 2 re-runs for over-long quotes.

## O vs R strata
| Measure | O (1999–2001) | R (2023–24) |
|---|---|---|
| Haiku tokens/article (A / B) | 35.6K / 31.2K | 43.9K / 42.8K |
| Opus tokens/article (A full / B review) | 26.4K / 25.3K | 32.8K / 30.6K |
| Haiku error (A / B) | 7.4% / 4.9% | 5.8% / 4.8% |
| Opus error (A / B) | 0.7% / 0.0% | 1.7% / 0.3% |

The older reports took **less** effort: about 15–25% fewer tokens per article and fewer tool calls. They are shorter, and most O-stratum packets have no `tables/` folder. Error is similar: Haiku is slightly worse on O in arm A (7.4% vs 5.8%; analytic_depth 36% on n=11), and Opus is slightly better. O has only 11 articles per arm, so these differences are within noise. Packet quality is the O-stratum risk. Several tables exist only as OCR inside `article.txt` (e.g. mm5016a4, where "≥65" became "265"; mm4917a4; mm4935a2), so readers could not verify the numbers.

## What went wrong
1. **Review mode only parse-checks.** The arm-B review prompt asks only that the JSON parses, not that the merged record validates. Two reviews produced over-long quotes, which made two merged records fail; one re-run each fixed them. For the re-runs I added one factual sentence to the runbook prompt, saying why the earlier attempt failed. Otherwise the prompt was unchanged. Several reviewers validated a scratch merge on their own initiative; the prompt should require it.
2. **Subagents share one scratchpad.** Parallel agents overwrote each other's helper scripts twice. In one case an Opus arm-A reader ran a quote-check script that another agent had rewritten to open `out/haiku/`. The reader reports it failed before reading anything, and switched to its own script. Independence appears intact but was not enforced. Future prompts should give each agent its own scratch subfolder.
3. **Reviewers ran `merge_review.py`.** Two reviewers ran it, once as `--help`; the script ignores arguments and does a full merge into `out/merged/`. This is harmless, because the merge is deterministic and I re-ran it after every batch, but the script should reject unknown arguments.
4. **The merge leaves stale notes.** Merged records keep Haiku's notes even when the review changed the field they describe (mm7219a1, mm7305a4).
5. **Validator gaps.** `validate.py` accepts evidence `loc` values outside the brief's list (it accepted "abstract"), and accepts `level_alternative: null`, which the brief may intend to forbid.
6. **Off-list reads.** One Opus reader opened the top of `tools/validate.py` (schema code only), and one Haiku reader listed folder names in `out/` without opening files. Neither touched the other reader's records.
7. **Sample contents.** The sample includes corrected or retracted items: mm6953a1 is marked retracted or corrected, mm7305a4 is "Corrected after publication", and mm5042a4 has an erratum not in the packet. Readers logged many article-internal numeric inconsistencies in `view_issues`; adjudicators assigned no `view issue` verdicts.
8. **Token metric.** As noted above, the per-agent token figure is not billed usage, so the cost comparison between arms and models is only indicative.
9. **Process.** A stop hook in this environment requires a commit at every pause, so the branch holds many intermediate "checkpoint" commits before the final one. Nothing was merged to the main branch.
