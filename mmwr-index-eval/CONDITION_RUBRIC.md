# Condition rubric (for the grouping pass, not for readers)

From brief v4, readers record `conditions` as free text (`{"v", "role", "ev"}`), named as the article names them. This rubric maps those terms to groups in a **separate, cheap pass**: a lookup table, or a small model call over the unique terms. The rubric can change at any time and be re-applied to the whole corpus without re-reading articles (BRIEF_V4_DECISIONS D19).

## Groups
COVID · influenza · mpox · RSV · HIV · TB/respiratory · STI · vaccine-preventable (other) · diabetes · cardiovascular · cancer · obesity/nutrition · maternal/reproductive · mental health · substance use · tobacco · injury/poisoning · environmental · health behaviors · other acute · other chronic.

A term may map to more than one group. A term that fits none maps to none.

## Rules carried from the brief (D15 and earlier)
- Tobacco use (smoking, smokeless) → tobacco. Alcohol, vaping and other drug use → substance use.
- Physical activity, diet and sleep → health behaviors.
- A `vaccine target` entry maps to its disease group (e.g. mpox → mpox; measles → vaccine-preventable (other)).
- The `role` is kept alongside the group; grouping does not change it.

## Open questions for the owner
- Asthma and COPD: TB/respiratory, or other chronic? (raised in the arm-B audit)
- Viral hepatitis (B, C): STI, other chronic, or a new group?
- Breastfeeding: maternal/reproductive, obesity/nutrition, or health behaviors?

## Output of the pass
One table, `conditions_map.tsv`: `term` (lowercased, as written by readers) · `groups` (pipe-separated) · `note`. Records are then joined to it by term.
