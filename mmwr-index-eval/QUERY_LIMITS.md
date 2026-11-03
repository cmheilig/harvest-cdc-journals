# What the query tool answers less well (vols 73–71)

`tools/query.py` searches 761 records whose audited field error is 0.37% (`reports/VOL71_REPORT.md`). Most queries can be taken at face value. The ones below fall on fields with known gray zones, on free-text fields, or on things the index does not record. For these, read the quotes printed with each match before choosing a report. Counts are from vols 73–71.

## 1. Combinations are per article, not per result
Filters combine **per article**. For example, `--proc weighting --gw omnibus --gw-across race` returns articles that have survey weighting *and* an omnibus test across race groups. It does not check that the test was run on the weighted estimates. The quotes usually settle it.

The exception is the `--gw*` flags: together they describe a single groupwise entry.

## 2. Groupwise edges
- **Rare assessments:**
  - trend test across SPD groups: 1 article;
  - model joint test: 3;
  - interaction: 2.

  Gradients across ordered groups (age, income, education) are mostly recorded as `omnibus test` or `per-group vs referent only`. To find them, use `--gw omnibus,trend --gw-ordered` (the `weighting-ordered-spd` query), not `--gw trend` alone.
- **Cases the audits flagged** (`reports/VOL7{3,2,1}_REPORT.md`, brief issues):
  - a CI-overlap rule stated once under "highest" claims. This was fixed in brief v5.2, but vol 73 was read with v5.1, so some of the `side by side` entries in its 42 articles that have one may really be `per-group vs referent only`;
  - a 2-group SPD contrast compared across levels of another SPD;
  - one tested pair next to an untested third group;
  - a ranking of increases with no per-group data;
  - target groups that mix age bands with a clinical group;
  - rurality of schools or clinics (units that stand for people).
- **"Reader unsure" is common on groupwise entries.** Of the 21 `weighting-omnibus-race` matches, 3 remain under `--confident`.

## 3. Equity level boundaries
- **Few articles reach the top levels:** 14 are at level 3 and 37 at level 4.
- **Level 3 vs 4 is the usual boundary.** Of the 37 level-4 readings, 26 have medium or low confidence and 36 give level 3 as the alternative. The question is usually whether the determinants are specific or generic. The 2/3 cap is the other boundary: values only set side by side, or significance marks only. The tool prints the level's confidence, its alternative and the reader's reason. Use them when the exact level matters.
- **Of the 51 articles at level 3 or higher, 12 have no uncertainty flag on the level.** `--equity-min 3` is a good first screen, not a final list.
- **Level 0 vs 1:** an SPD seen only as a descriptor (e.g. homelessness) was the one equity error in vol 73's audit.

## 4. Comparison framework
- **Fixed referent vs pair** is ambiguous when the referent is a historical baseline or a notification threshold (vol 72 audit).
- **The leading framework** follows the headline claim. A report can also compare with a target that it does not lead with.
- `--referent .` finds every article with a named referent, whatever its framework.
- Of the 10 `nftf-fixed-referent` matches, 2 remain under `--confident`.

## 5. Tests that are not named
- **46 articles** have an `unnamed test`: "significantly", or p-values, with no test named.
- **QuickStats never name their test in these volumes.** So "QuickStats with a trend test" cannot be told apart from a period-to-period comparison. The `qs-trend` query uses a trend design plus a test as a proxy (8 articles).
- **Procedure names are free text.** Write the NAME part of `--proc CLASS:NAME` as a regex with alternatives, e.g. `test:'chi|χ'` or `test:'rao|wald|survey'`. `python3 tools/query.py facets proc-name` lists the names.

## 6. Best-effort fields
`--cgroup`, `--condition`, `--source`, `--source-named`, `--setting` and `--vaccine` describe a report, but readers were not held to them as strictly. They were never sent to a second reader.
- **Conditions:**
  - they are the readers' free text, grouped afterward through `conditions_map.tsv`;
  - a term can belong to two groups (e.g. self-harm is in both `injury/poisoning` and `mental health`);
  - the role matters: `--crole outcome` leaves out conditions that only define the population or the exposure.
- **`vaccine_outcome_type`** can be set from VE figures that an ACIP summary only cites.
- **Data sources:** registries that a sentinel network consults to verify vaccination status are classified inconsistently.

## 7. Not in the index
The index does not record:
- sample sizes, effect sizes, or p-values as numbers;
- software, except where quoted as evidence;
- whether a method suits the data.

`--text` searches only the title and the reader's public health question, not the full text. `comparison.across` is free text, so `--across` takes a regex (e.g. `'race|ethnic'`). For SPDs, `--dim race:stratifier` or `--dim "race:primary comparison"` is more reliable.

## 8. Coverage and versions
- **Volumes:** 73–71 only, the report family plus QuickStats. The 85 holdout articles in these types are not indexed.
- **Brief versions:** vol 73 was read with brief v5.1, vols 72–71 with v5.2. The difference is the groupwise CI-overlap rule (§2).
- **Years** come from each article's date line. Two vol 73 articles are dated 2025.
- **Uncertainty flags:** 709 of 761 records flag at least one field as uncertain. The audit found these flags too common to separate errors. `--confident` is therefore a strict screen, not an error filter.
