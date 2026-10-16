# harvest-cdc-journals

## Source material
The articles here are from CDC's journals (MMWR and others) and are published US government works in the public domain (17 U.S.C. §105). Case descriptions in them (age, sex, race, place, clinical course) come from published, de-identified public health reports, not from private records. Quoting and indexing them is the purpose of this repository.

## Working in mmwr-index-eval/
- Read packets, examples, briefs and records with the Read tool (use offset/limit for long files). Do not dump them through Bash (`cat`, `head`, Python one-liners, paging overflow files): such commands get flagged as personal-data handling and blocked.
- Use Bash only for the repository's own scripts, e.g. `python3 tools/validate.py <file>`. If a helper is needed more than once, add it to `tools/` rather than writing it ad hoc.
- Runbooks (`RUN_*.md`) and briefs (`READER_BRIEF.md`, `ADJUDICATOR_BRIEF.md`) govern subagent work; decisions are recorded in `BRIEF_V4_DECISIONS.md`.
