# Production: one volume, end to end

Follows `PRODUCTION_PLAN.md` §3–§5, with the frame decisions in `reports/LOCAL_ASSESSMENT.md` (L2, L3, L5, L6). Run from `mmwr-index-eval/`. Every command prints counts and ids only. API calls need `ANTHROPIC_API_KEY` (the project workspace key) in the shell that runs them.

## 1. Read the volume (Claude API, Message Batches)
1. `python3 tools/build_packets.py vol VOL`: packets and `frame.tsv` (holdout omitted).
2. `python3 tools/api_read.py submit VOL`: warms the cache, then submits one batch. It refuses when the uncached estimate exceeds the remaining budget.
3. `python3 tools/api_read.py poll` until the batch has ended (most within an hour, at most 24 hours), then `python3 tools/api_read.py collect VOL`.
4. `python3 tools/api_read.py fixup VOL`, then poll and collect again. This gives one fix-up turn to invalid or unparsed reads; add `--sync` for a handful.
5. `python3 tools/api_read.py retry VOL`: one synchronous retry, with server-side fallbacks, for refused, truncated or errored reads. Then run `fixup VOL --sync` for any retry that came back invalid.
6. `python3 tools/api_read.py status VOL`. Gate (plan §4.2): valid records for at least 99% of packets; cost per article within 25% of the estimate.
7. The owner appends `out/prod/VOL/exposure_rows.tsv` to the extraction's `exposure_log.tsv`.

## 2. Audit (plan §5)
1. `python3 tools/audit_sample.py draw VOL`: runs the groupwise check and draws three strata:
   - 5% random, at least 10 (second read);
   - every fallback-served read (second read);
   - a groupwise look at flagged records plus 10% of the rest (no second read).
2. `python3 tools/api_read.py submit VOL-audit`, then `poll`, `collect VOL-audit`, `fixup VOL-audit --sync`. Second reads use the same environment and effort as production.
3. `python3 tools/audit_sample.py compare VOL`: blinded packets in `audit/VOL/adj_packet/`.
4. Adjudicate every packet whose `disagreements.json` is not empty, 5 articles per Opus subagent (default effort), with the prompt below. Groupwise looks always have a list.
5. `python3 tools/audit_sample.py score VOL`. Gate: field error at most 1.5%, and the groupwise looks reviewed.

## 3. Conditions and close
1. `python3 tools/group_conditions.py audit/VOL/conditions.tsv out/prod/VOL/*.json --missing audit/VOL/conditions_missing.tsv`.
2. Grouping pass:
   - fill `canonical` and `groups` in the missing file, following `CONDITION_RUBRIC.md`;
   - append those rows (term, canonical, groups, note) to `conditions_map.tsv`;
   - re-run step 1 until nothing is missing.
3. Report the gate results and the actual cost against the estimate. Commit on the production branch (no model ids in messages). Stop for the owner before the next volume.

## Adjudicator prompt (Claude Code subagent, model opus)
```
You are an adjudicator in the production audit of the MMWR index (volume <VOL>). Working directory: mmwr-index-eval/.
1. Read ADJUDICATOR_BRIEF.md in full, then READER_BRIEF.md (version 5.1) in full. Judge every value under brief v5.1.
2. Your articles: <IDS>. For each, read audit/<VOL>/adj_packet/<id>/ (in place of adj_packet/<id>/) and the article in prod_packets/<VOL>/<id>/ (article.txt and tables/*.csv).
   - With reader_A.json and reader_B.json: disagreements.json gives, per field, the value of each label. Decide each field; several labels may be right if the field is genuinely ambiguous.
   - With reader_A.json only (a groupwise look): disagreements.json gives A's groupwise values, and mechanical_flags.json what a script flagged. Judge each listed field: "correct": ["A"] if A's value is right, otherwise "correct": [] with your verdict.
   Write audit/<VOL>/adjudication/<id>.json in the brief's format and check it parses. Leave agreed_errors empty.
3. Do not open audit/<VOL>/mapping.json, audit/<VOL>/sample.tsv, any *_mapping.json, out/, prod_ledger.tsv or reports/.
4. Read files with the Read tool; do not print packet or record contents through Bash.
5. If a request is refused, retry with a smaller piece of text; report anything you could not complete.
Reply in at most 6 lines: ids written; counts of verdict categories; the most important pattern.
```
