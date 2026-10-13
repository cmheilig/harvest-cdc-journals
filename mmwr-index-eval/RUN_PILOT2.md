# Pilot 2 (instructions for the orchestrating Claude Code session)

**Run this in a fresh session.** You orchestrate; readers and adjudicators are subagents. Work only inside `mmwr-index-eval/`. Do not modify `READER_BRIEF.md`, `ADJUDICATOR_BRIEF.md`, `examples/`, `packets/`, `tools/` or pilot 1's outputs (`out/haiku/`, `out/opus/`, `out/opus_review/`, `out/merged/`, `out/opus_audit/`, `adj_packet*/`, `adjudication/`, `adjudication_audit/`, `reports/` files that already exist). If something is broken, stop and report.

## What is being tested
1. **Brief v4** works: records validate, and readers use the new fields (`groupwise`, `uncertain_fields`, `conditions`, tests by CI inversion, `ci_overlap`).
2. **Effort** (BRIEF_V4_DECISIONS D20): every article is read twice by Opus, once at effort `low` and once at `medium` (Opus 5.5's default). The owner has asked for these two effort levels. Does `low` hold quality, and what does it save?
3. **Routing** (D18): if one read were the only primary read, how many articles would `uncertain_fields` and equity confidence send to a second reader, and what share of that read's errors sit in the routed articles? The other effort arm stands in for the second reader.
4. **Condition grouping** (D19): a cheap pass maps the free-text conditions to `CONDITION_RUBRIC.md`.

## Sample
`p2_sample.tsv` and `p2_batches.json`, made by `python3 tools/pilot2.py sample all`: the 100 pilot-1 articles (the only packets available) in 26 batches. 54 are in the **priority segment** (full reports published 2016–2024; here all from 2023–24). To halve the cost, the owner may instead run `python3 tools/pilot2.py sample priority` (54 articles, 14 batches) before step 1. Use whichever sample files exist when you start.

## Budget guard
Expected total about 6–8M tokens in the harness's per-agent metric for the full sample (about half for the priority sample). After step 2, sum the tokens in `p2_run_log.tsv`. If the total passes **9M** (full sample) or **5M** (priority sample), stop and report before adjudicating.

## Steps
0. **Check the model and effort.** Spawn one tiny subagent with model `opus` and effort `low`. It reports its exact model id. Record it in `p2_run_log.tsv` (columns as `run_log.tsv`: batch, role, model, status, subagent_tokens, tool_uses, duration_s, refusals, notes; batch `check`). If it is not a 5.5 model, or the effort parameter is rejected, stop and report.
1. **Reads.** For each batch, spawn two readers (model `opus`) with the reader prompt below: one with effort `low` (`<ARM>` = low) and one with effort `medium` (`<ARM>` = medium). Run up to 10 at a time. Give each its own scratch folder (session scratchpad, `p2/<BATCH>-<ARM>/`). After each one, append a row to `p2_run_log.tsv` (role `p2-low` or `p2-medium`).
2. **Check.** Run `python3 tools/validate.py out/p2_low/*.json out/p2_medium/*.json`. Re-run a failing article once, alone, at the same effort. If it fails again, leave it and note it. Then apply the budget guard.
3. **Compare.** Run `python3 tools/pilot2.py compare`. It writes `adj_packet_p2/`, `p2_mapping.json` and `reports/p2_agreement.tsv`. Best-effort fields (conditions, data sources, setting, vaccine outcome) are not sent to adjudication.
4. **Adjudicate** every article whose `adj_packet_p2/<id>/disagreements.json` is not empty, 5 per Opus subagent (default effort), with the adjudicator prompt below. Log each in `p2_run_log.tsv` (role `p2-adjudicator`).
5. **Score.** Run `python3 tools/pilot2.py score`. It writes `reports/p2_scores.tsv`, `reports/p2_routing.tsv` and `reports/p2_brief_issues.md`.
6. **Condition grouping trial.** Collect the unique `conditions[].v` terms from `out/p2_medium/` (lowercased) into a scratch file. Spawn one subagent with model `haiku`: it reads `CONDITION_RUBRIC.md` and the term list, and writes `conditions_map.tsv` (term, groups, note) as the rubric specifies. Log it (role `p2-conditions`).
7. **Report.** Write `reports/PILOT2_REPORT.md`:
   - models and effort levels used; articles done, refused, failed; first-pass validation failures, by field;
   - tokens, time and tool uses per article by effort arm and segment (from `p2_run_log.tsv`);
   - error rates for low vs medium on method and equity fields, by segment and stratum (from `reports/p2_scores.tsv`), and a recommendation on effort with the small-sample caveat;
   - routing (from `reports/p2_routing.tsv`): share of articles routed in each segment, share of errors in routed articles, and whether the trigger looks too loose or too tight (the trigger is `routed()` in `tools/pilot2.py`);
   - how readers used the new fields: share of records with `groupwise` entries and their assessment types; tests recorded by CI inversion, and how often the CI method was stated; `ci_overlap` flags; the distribution of `uncertain_fields`; condition roles;
   - the condition grouping trial: number of unique terms, share mapped, ambiguous terms, and any rubric questions;
   - recurring `brief_issues`; anything that went wrong.
8. **Commit** everything under `mmwr-index-eval/` to the working branch with a short message. Do not merge to the main branch.

**Refusals.** Record them, retry once alone, then leave the article and report it. Never edit a reader's or adjudicator's record yourself.

## Reader prompt (model opus; effort low or medium)
```
You are an MMWR index reader in pilot 2 (brief v4). Working directory: mmwr-index-eval/ in this repository.
1. Read READER_BRIEF.md in full. It is your contract.
2. Read at least two worked examples in examples/ (suggested: mm6925a1.json and mm7223a3.json).
3. Your batch (<BATCH>): <IDS>.
   For each article, one at a time: read packets/<id>/article.txt, prepass.json and tables/*.csv; write the full record to out/p2_<ARM>/<id>.json with reader.role "judgment"; run `python3 tools/validate.py out/p2_<ARM>/<id>.json` (that one file only) and fix until OK. Save each record before reading the next article.
   Give equity.level_confidence and equity.level_alternative, and list uncertain_fields honestly (empty when nothing is uncertain).
4. Read only the files named above. Do not open anything else under out/ (write only your own records), adj_packet*/, adjudication*/, *_mapping.json, reports/, or other .md files.
5. Put any scratch files only in <SCRATCH>; do not read or write other scratch folders.
6. If a request is refused or an article cannot be completed, skip it and continue; report it.
7. reader.model = your exact model id from your system prompt; reader.reader_id = "<ARM>".
Reply in at most 5 lines: ids written; ids skipped and why; your model id; anything unclear in the brief.
```

## Adjudicator prompt (model opus)
```
You are an adjudicator in pilot 2 of the MMWR index. Working directory: mmwr-index-eval/.
1. Read ADJUDICATOR_BRIEF.md in full, then READER_BRIEF.md in full.
2. Your articles: <IDS>. For each: read adj_packet_p2/<id>/ (in place of adj_packet/<id>/) and packets/<id>/; write adjudication_p2/<id>.json in the brief's format and check it parses. Also check the high-value fields both readers agree on (procedures and regression families, study design, comparison, groupwise, equity level and dimensions) and list any value both got wrong in agreed_errors.
3. Do not open adj_mapping.json, p2_mapping.json, audit_mapping.json, out/ or reports/.
4. Put any scratch files only in <SCRATCH>; do not read or write other scratch folders.
5. If a request is refused, retry with a smaller piece of text; report anything you could not complete.
Reply in at most 6 lines: ids written; counts of verdict categories; number of agreed errors; the most important pattern.
```
