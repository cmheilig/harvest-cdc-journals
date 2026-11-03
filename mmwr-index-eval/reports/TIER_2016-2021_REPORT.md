# The 2016–2021 full-read tier: gate report (2026-11-03)

*Claude's report, for the owner's review.*

**What was read.** The full-read tier for 2016–2021 (vols 65–70), as staged by `tools/surface_rules.py stage-tier`.
- It holds the articles the surface rules mark as an equity 3–4 candidate or as using two or more method families.
- 446 articles are in it; 24 more tier articles were read in the older slice and were not read again.
- List and reasons: `reports/tier-2016-2021.tsv`.
- Brief v5.2, prefix `30d57882dbf0`, Opus 5.5 at effort medium, Message Batches, under the volume key `tier-2016-2021`.
- 2022–2024 (vols 71–73) already had full production reads, so the tier for 2016–2024 is complete with this run.

## Gate (PRODUCTION_PLAN.md §4.2)
| Gate | Result | Status |
|---|---|---|
| Valid record for every article, or a logged failure (≤ 1%) | 446 of 446 valid: 404 on the first read, 42 after one fix-up | pass |
| Audit field error ≤ 1.5% | 4 of 621 scored fields (0.64%): 2 of 138 equity, 2 of 483 method. The second reads had 7 (1.13%). | pass |
| Groupwise check | 81 groupwise looks (flagged, plus a random 10%): 5 of 243 fields wrong | pass |
| Cost per article within 25% of the estimate | $0.148 against $0.134 at submit (+10.5%) | pass |
| Condition grouping pass | 274 new terms grouped (`conditions_map.tsv`: 958 terms, 657 canonical); nothing missing in the tier | pass |

**Cost.** The reads and fix-ups cost $65.95, and the audit's second reads $3.50. Platform spend in both ledgers is now about **$201.82**.

The tier runs above the production rate ($0.124–0.130 per article) because it is selected for method-rich reports, which are longer.

## Audit (§5)
- **Random second reads:** 23 articles (5%); 18 had at least one difference, 35 fields in all. Adjudicated by 4 Opus subagents under `ADJUDICATOR_BRIEF.md`; their verdicts are in `audit/tier-2016-2021/adjudication/`.
- **Most differences fall outside the scored fields: equity dimension roles.** One reader records a secondary role of a dimension and the other does not. Typical cases:
  - age or sex as a covariate through age standardization or a model's covariate list;
  - race as a covariate through survey oversampling;
  - a weighting variable recorded unevenly.

  The production read and the second read each missed some, and none changes an equity level.
- **The 4 scored production errors** were in study design, inference, and two equity fields. Examples:
  - mm6939a1: a before/after aim stated, so `before/after or trend` belongs in the design;
  - mm6802a3: CIs named in the methods only as a testing device, so they are not reported CIs.
- **Groupwise looks: 81 articles, 243 fields, 17 Opus subagents.** 5 fields were wrong:
  - mm7041a2: a rural-urban contrast resting on a stated pairwise-test rule, so per-group vs referent only rather than side by side;
  - mm6519a2: a CI-overlap rule that also covers the race claims;
  - mm655051e1 and mm6942a1: race left out, or results that sit only in an online supplement;
  - mm6929e2: a single Black-vs-rest test, logged as a brief gap.
- **The mechanical groupwise flag is mostly a false alarm.** Nearly every "side by side, but the record has a test" flag pointed to a test of something else:
  - a Table 1 composition chi-square;
  - period-to-period tests within each SPD level (rule 6);
  - trend tests across calendar years;
  - a pairwise test on a collapsed pair.

  Several adjudicators proposed the same fix: `audit_sample.py groupwise` should check that the test's margin is the SPD of the flagged entry before flagging.
- **`uncertain_fields`:** 440 of 446 records flag something, most often `equity.level` (353), `groupwise` (204), `comparison` (150) and `procedures` (110). As in vols 73–71, the flags are too common to separate errors.
- **Process note:** one adjudicator printed one packet's `disagreements.json` and `mechanical_flags.json` through Bash once before switching to the Read tool, against the repository's convention. Nothing else departed from the prompt.

## Brief issues raised (`audit/tier-2016-2021/brief_issues.md`, 49 lines)
These recur, and are candidates for a future brief version (none is applied here):
1. **Roles.** The brief does not define stratifier versus primary comparison, and has no role for a weighting variable. It does not say whether age as a direct-standardization variable is a covariate.
2. **Blanket test statements.** A single methods line ("differences were tested using chi-square tests" or a survey Wald F) under highest/lowest claims across three or more groups: is it omnibus, per-group vs referent only, or side by side when no result is reported for a dimension?
3. **Single level-vs-rest tests** (Black vs all others): per-group vs referent, or a collapsed pair?
4. **Area indexes.** Do the subthemes of the SVI count as SES or as the dimension each measures (age, disability, nativity)? Is each indicator's tertile its own groupwise entry?
5. **Supplements.** Groupwise results or stratified values that appear only in an online supplement.
6. **Basis for proxy race** (infant race from the mother's birth certificate; employer records).
7. **Smaller items.**
   - Whether an "Unknown" level counts toward `n_groups`.
   - Overlapping county groups.
   - School grade as the age SPD.
   - County urbanization of event location as rurality.
   - A `data_source_type` value for CDC program monitoring data (NHM&E).

**One script bug:** `compare.norm`'s `comparison_spd` does not recognize "country of birth" as the nativity SPD (mm6950a5). It affects only the audit's comparison field, not the records.

## Condition grouping
274 tier terms were missing from the map, mostly older-era conditions and behaviors. They were grouped under `CONDITION_RUBRIC.md` and the map's precedents:
- screening, testing and PrEP map to the condition behind them;
- overdoses map to `injury/poisoning|substance use`;
- diet, sleep and physical activity map to `health behaviors`;
- occupational lung disease maps to `respiratory - noninfectious|environmental`;
- all-cause death, general health status and social determinants (housing insecurity) map to no group.

Two terms already in the map kept their existing groups rather than new ones: coccidioidomycosis (`other acute`) and noise-induced hearing loss (`environmental|other chronic`). The filled terms are in `audit/tier-2016-2021/conditions_missing.tsv`.
