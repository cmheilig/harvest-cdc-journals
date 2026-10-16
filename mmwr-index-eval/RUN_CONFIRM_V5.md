# Brief v5 confirmation run (instructions for the orchestrating Claude Code session)

**Status: approved by the owner and run 2026-10-16; results in `reports/CONFIRM_V5_REPORT.md`.** Cost note: the owner had about $64 of cloud credit left, so the orchestrator also stops if the whole run passes 4M tokens (about $38 at pilot 2's rate).

You orchestrate; readers and adjudicators are subagents. Work only inside `mmwr-index-eval/`, on branch `mmwr-index-v5`. Read files with the Read tool; use Bash only for the repository's scripts (see `CLAUDE.md`). Do not modify `READER_BRIEF.md`, `ADJUDICATOR_BRIEF.md`, `examples/`, `packets/`, `tools/` or any earlier run's outputs (`out/haiku/`, `out/opus*/`, `out/merged/`, `out/p2_*/`, `adj_packet*/` other than `adj_packet_cv5/`, `adjudication/`, `adjudication_audit/`, `adjudication_p2/`, existing `reports/` files). If something is broken, stop and report.

## What is being tested
Brief v5 (`BRIEF_V5_DECISIONS.md`, V1–V18) was written to settle the rule boundaries behind pilot 2's disputes. One v5 read per article, at the production effort (`medium`, D20), is scored against pilot 2's reference values: the adjudicated verdict where pilot 2's two reads disagreed, the agreed value otherwise. A v5 value that differs goes to a v5 adjudicator, blinded as A/B, who rules **under brief v5**: the pilot-2 reference can be wrong under v5 by design (e.g. CI overlap assertions are now tests). Questions:
1. Do v5 reads land on defensible values where pilot 2 disputed? What is the v5 field error rate? Baseline: pilot 2's medium reads score 1.5% against the same reference on these articles (27 of 1,782 fields; `tools/confirm_v5.py` self-test).
2. Do readers use the new rules and fields (`data_focus` in `uncertain_fields`, `pairwise (all pairs)`, `review or synthesis`, `basis`, "CI overlap assertion")?
3. How many articles does the V17 routing placeholder send to a second reader?
4. Do the conditions join cleanly to `conditions_map.tsv`?

## Sample
`cv5_sample.tsv` and `cv5_batches.json`, made by `python3 tools/confirm_v5.py sample`: the 66 articles cited in `BRIEF_V5_DECISIONS.md`, in 17 batches of up to 4, batched by segment (V5P = priority, V5O = other). 40 are in the priority segment; 54 were adjudicated in pilot 2.

## Budget guard
Expected about 2.1M tokens for the reads and 0.5–1M for adjudication in the harness's per-agent metric (about 40% of pilot 2's 7.7M). After step 2, sum the tokens in `cv5_run_log.tsv`. If the reads pass **3M**, stop and report before adjudicating.

## Steps
0. **Check the model.** Spawn one tiny subagent with model `opus` and effort `medium`; it reports its exact model id. Log it with `python3 tools/runlog.py cv5_run_log.tsv check model-check <tokens> <tool_uses> <duration_ms> 0 "model id check" --model <id>`. If it is not a 5.5 model, stop and report.
1. **Reads.** For each batch, spawn one reader (model `opus`, effort `medium`) with the reader prompt below. Run up to 10 at a time. Give each its own scratch folder (session scratchpad, `cv5/<BATCH>/`). After each one, log it: `python3 tools/runlog.py cv5_run_log.tsv <BATCH> cv5-reader <tokens> <tool_uses> <duration_ms> <refusals> "<notes>" --ids <comma-separated ids> --outdir out/v5`.
2. **Check.** Run `python3 tools/validate.py out/v5/*.json`. Re-run a failing article once, alone. If it fails again, leave it and note it. Then apply the budget guard.
3. **Compare.** Run `python3 tools/confirm_v5.py compare`. It writes `adj_packet_cv5/`, `cv5_mapping.json` and `reports/cv5_match.tsv`.
4. **Adjudicate** every article whose `adj_packet_cv5/<id>/disagreements.json` is not empty, 5 per Opus subagent (default effort), with the adjudicator prompt below. Log each (role `cv5-adjudicator`).
5. **Score.** Run `python3 tools/confirm_v5.py score` (`reports/cv5_scores.tsv`, `reports/cv5_brief_issues.md`) and `python3 tools/group_conditions.py reports/cv5_conditions.tsv out/v5/*.json` (it lists terms missing from the map).
6. **Report.** Write `reports/CONFIRM_V5_REPORT.md`:
   - model and effort; articles done, refused, failed; first-pass validation failures, by check;
   - tokens, time and tool uses per article (from `cv5_run_log.tsv`), against pilot 2's medium arm (30.9K tokens, 39 s, 4.3 tool uses per article);
   - v5 error rate by segment and group, against the 1.5% baseline; how the differing fields resolved (v5 right where the pilot-2 reference was wrong under v5; both defensible; v5 wrong), by adjudicator category;
   - for each V item, whether its cited articles still dispute the field it targets;
   - use of the new rules and fields; the `uncertain_fields` distribution; routing share by segment under V17;
   - condition terms missing from the map;
   - recurring `brief_issues`; anything that went wrong.
7. **Commit** everything under `mmwr-index-eval/` to `mmwr-index-v5` with a short message. Do not merge to the main branch.

**Refusals.** Record them, retry once alone, then leave the article and report it. Never edit a reader's or adjudicator's record yourself.

## Reader prompt (model opus; effort medium)
```
You are an MMWR index reader in the brief v5 confirmation run. Working directory: mmwr-index-eval/ in this repository.
1. Read READER_BRIEF.md (version 5) in full. It is your contract.
2. Read at least two worked examples in examples/ (suggested: mm6925a1.json and mm7223a3.json).
3. Your batch (<BATCH>): <IDS>.
   For each article, one at a time: read packets/<id>/article.txt, prepass.json and tables/*.csv, and every table and figure note; write the full record to out/v5/<id>.json with reader.role "judgment"; run `python3 tools/validate.py out/v5/<id>.json` (that one file only) and fix until OK. Save each record before reading the next article.
   Give equity.level_confidence and equity.level_alternative, and list uncertain_fields honestly (empty when nothing is uncertain).
4. Read only the files named above. Do not open anything else under out/ (write only your own records), adj_packet*/, adjudication*/, *_mapping.json, reports/, or other .md files.
5. Read files with the Read tool (offset/limit for long ones); use Bash only for `python3 tools/validate.py`. Do not print packet or record contents through Bash.
6. Put any scratch files only in <SCRATCH>; do not read or write other scratch folders.
7. If a request is refused or an article cannot be completed, skip it and continue; report it.
8. reader.model = your exact model id from your system prompt; reader.reader_id = "v5".
Reply in at most 5 lines: ids written; ids skipped and why; your model id; anything unclear in the brief.
```

## Adjudicator prompt (model opus)
```
You are an adjudicator in the brief v5 confirmation run of the MMWR index. Working directory: mmwr-index-eval/.
1. Read ADJUDICATOR_BRIEF.md in full, then READER_BRIEF.md (version 5) in full. Judge every value under brief v5.
2. Your articles: <IDS>. For each: read adj_packet_cv5/<id>/ (in place of adj_packet/<id>/): disagreements.json gives, per field, the value of label A and label B; one label's full record is in the folder. Read packets/<id>/ and decide each field under brief v5. Several labels may be right if the field is genuinely ambiguous. Write adjudication_cv5/<id>.json in the brief's format and check it parses. Leave agreed_errors empty.
3. Do not open adj_mapping.json, p2_mapping.json, cv5_mapping.json, audit_mapping.json, out/, adjudication_p2/ or reports/.
4. Read files with the Read tool; do not print packet or record contents through Bash.
5. Put any scratch files only in <SCRATCH>; do not read or write other scratch folders.
6. If a request is refused, retry with a smaller piece of text; report anything you could not complete.
Reply in at most 6 lines: ids written; counts of verdict categories; the most important pattern.
```
