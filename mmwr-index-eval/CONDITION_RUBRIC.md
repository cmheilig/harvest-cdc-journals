# Condition rubric (for the grouping pass, not for readers)

From brief v4, readers record `conditions` as free text (`{"v", "role", "ev"}`), named as the article names them. This rubric maps those terms to groups in a **separate, cheap pass**: a lookup table, or a small model call over the unique terms. The rubric can change at any time and be re-applied to the whole corpus without re-reading articles (BRIEF_V4_DECISIONS D19).

## Groups
COVID · influenza · mpox · RSV · HIV · TB · viral hepatitis · STI · vaccine-preventable (other) · respiratory - infectious · respiratory - noninfectious · vaccine safety · diabetes · cardiovascular · cancer · obesity/nutrition · maternal/reproductive · mental health · substance use · tobacco · injury/poisoning · environmental · health behaviors · other acute · other chronic.

A term may map to more than one group. A term that fits none maps to none.

## Rules carried from the brief (D15 and earlier)
- Tobacco use (smoking, smokeless) → tobacco. Alcohol, vaping and other drug use → substance use.
- Physical activity, diet and sleep → health behaviors.
- A `vaccine target` entry maps to its disease group (e.g. mpox → mpox; measles → vaccine-preventable (other)).
- The `role` is kept alongside the group; grouping does not change it.
- A condition with its own group (COVID, influenza, RSV, TB, a vaccine-preventable disease) maps to that group only. `respiratory - infectious` is for respiratory infections without their own group (e.g. acute respiratory illness, pneumonia of unspecified cause, Legionnaires' disease).
- Asthma, COPD and other noninfectious respiratory disease → respiratory - noninfectious.
- Viral hepatitis (A–E) → viral hepatitis, including as a `vaccine target`. Non-viral hepatitis (e.g. drug-induced) does not.
- Breastfeeding and other infant feeding → maternal/reproductive.
- A term that names several conditions (e.g. a list of vaccine-preventable diseases) maps to each of their groups.
- **Vaccine safety** (owner decision 2026-10-16): adverse events following immunization (reactogenicity, serious adverse events) map to `vaccine safety`. A named event (e.g. Guillain-Barré syndrome, myocarditis) keeps its own group and gains `vaccine safety` at join time when the record's `vaccine_outcome_type` is safety and the condition's role is `outcome` (`tools/group_conditions.py`).
- **Health care use is not a condition** (owner decision 2026-10-16). Readers record the condition that drives a visit, test or screening, or nothing. Older terms of this kind are mapped by that condition (HIV testing → HIV; mammography screening → cancer) or to no group.

## Owner decisions (2026-10-14, after pilot 2)
- Asthma and COPD: split TB into its own group; asthma and COPD go to a new `respiratory - noninfectious` group. A companion `respiratory - infectious` group takes respiratory infections that have no group of their own.
- Viral hepatitis (B, C): a new group, `viral hepatitis`. This also settles hepatitis B as a vaccine target.
- Breastfeeding: maternal/reproductive (applied to other infant feeding, e.g. early introduction of complementary foods).

`conditions_map.tsv` was re-applied to these rules on 2026-10-14 (11 rows changed); `reports/PILOT2_REPORT.md` describes the first pass, before these decisions.

## Normalization (owner decision 2026-10-16)
The grouping pass first writes a `canonical` term for each reader term, then groups the canonical term, so one concept in several wordings gets one row of meaning. Canonical terms are lowercase; use the condition's full name (RSV → respiratory syncytial virus; Norwalk-like → norovirus); merge reorderings ("A and B" = "B and A") and singular/plural; and drop health care use wording ("and hospitalization"). Keep clinically distinct terms apart (stages, severity, invasive vs non-invasive disease). All terms with one canonical term carry the same groups.

## Output of the pass
One table, `conditions_map.tsv`: `term` (lowercased, as written by readers) · `canonical` · `groups` (pipe-separated, sorted) · `note`. Records are joined to it by term with `python3 tools/group_conditions.py OUT.tsv RECORD.json ...`, which also applies the vaccine-safety join rule and lists terms the map lacks. The pilot-2 map has 124 terms and 112 canonical terms.
