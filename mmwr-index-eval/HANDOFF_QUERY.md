# Handoff: the query tool over vols 73–71 (2026-10-17)

**Who reads this.** A Claude Code session that builds the query tool. Production of the index is paused after vols 73–71, so the owner can test whether the index answers teaching questions before vols 70–65 are read (`reports/VOL71_REPORT.md`, J2).

**State in one paragraph.**
- 761 articles are indexed: the report family plus QuickStats in vols 73–71 (2022–2024), holdout omitted.
- One record per article is in `out/prod/<vol>/mm*.json`, each valid under `tools/validate.py`.
- The audit found 4 wrong fields in 1,080 scored (0.37%).
- Vol 73 was read with brief v5.1; vols 72 and 71 with v5.2 (tag `brief-v5.2`).
- Platform spend so far is $108.64 of a $120 line. The query tool needs no API calls.

## 1. Ask the owner first
The owner is a public health instructor who selects MMWR reports for teaching by **how they make claims about data**. Before building anything:
1. Ask what questions the tool should answer, and offer examples:
   - full reports that use survey weighting and an omnibus test across race groups;
   - QuickStats with a trend test;
   - equity level 3 or higher on mental health conditions;
   - case-control designs with regression;
   - Notes from the Field with a fixed-referent comparison.
2. Propose the form:
   - a Python CLI in `tools/`;
   - a static HTML page (filters over one JSON file);
   - or both.

   Agree on one before building.

## 2. Inputs (read-only)
| Path | What it gives |
|---|---|
| `out/prod/{73,72,71}/mm*.json` | the records; field meanings in `READER_BRIEF.md` §4 |
| `prod_packets/<vol>/frame.tsv` | `article_type` per id. Git-ignored; rebuild with `python3 tools/build_packets.py vol VOL` (it reads the extraction under `~/cdc-corpora`, read-only) |
| `conditions_map.tsv`, `tools/group_conditions.py` | free-text conditions mapped to the 25 groups of `CONDITION_RUBRIC.md`; the joined rows are in `audit/<vol>/conditions.tsv` |
| `tools/compare.py` `norm()` | a flattened view of a record's method and equity fields, a likely basis for filters |
| `reports/cost_by_article.tsv` | per-article characteristics: data focus, equity level, procedures, regression, groupwise, study design |

Each record's `ev` quotes are the evidence to show beside a match. The article's title, date and URL are in the packet's `article.txt` header, or in the corpus CSV.

## 3. Done looks like
- A tool in `tools/` that answers the owner's example queries. For each match it returns the id, title, date, URL and the evidence quotes behind each matching value.
- An allow rule for it in `../.claude/settings.json`, in the same form as the existing ones; Bash cannot write that file, so use the editor. Also a row in `README.md`.
- A short note on queries the index cannot yet answer well:
  - fields with known gray zones: groupwise edges, equity level boundaries, comparison framework with baselines or thresholds (the brief issues in `reports/VOL7{3,2,1}_REPORT.md`);
  - best-effort fields: conditions, data sources, setting.

## 4. Conventions
- `../CLAUDE.md`:
  - the source is public-domain US government work;
  - read packets and records with the Read tool, not by dumping them through Bash;
  - use Bash only for the repository's own scripts; a helper used more than once goes in `tools/`.
- A blank `Decision:` line, or a blank decision cell, means pending; only an explicit accept counts.
- Commit messages and PRs carry no model ids. Work on your own branch from `main`; open a PR when the owner approves.
- Do not modify `out/prod/`, `audit/`, `prod_ledger.tsv` or the pilot outputs. They are the production record.
