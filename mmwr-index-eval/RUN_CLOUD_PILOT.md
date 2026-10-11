# Running the cloud pilot (instructions for the orchestrating Claude Code session)

You orchestrate. Readers and adjudicators are subagents. Work only inside `mmwr-index-eval/`. Do not modify `packets/`, `examples/`, the briefs or `tools/` (if something is broken, stop and report).

## What is being tested
1. **Two readers per article:**
   - a **structural** reader: Haiku, which reads presence or absence and is cheap;
   - a **judgment** reader: Opus, which handles judgment and gives its confidence on the equity level.
2. **Two arms, 50 articles each:**
   - **Arm A:** both readers write full records independently.
   - **Arm B:** Haiku writes the full record; Opus then reviews it in review mode (`READER_BRIEF.md` §6) and writes only its changes plus the equity block. Is B as good as A, and how much cheaper?
3. **Effort by epoch.** Batches are homogeneous by stratum: `R` = 2023–2024 and `O` = 1999–2001. Record tokens and time per batch, so the effort profile of the oldest reports can be compared with recent ones.

## Steps
0. **Check the models.** Spawn one tiny subagent with model `haiku` and one with model `opus`. Each reports its exact model id from its system prompt. Record both in `run_log.tsv` (batch `check`). If either is not a 5.5 model, stop and report.
1. **Arm A** (batches `AR01`–`AR10` and `AO01`–`AO03`; ids in `batches.json`).
   - For each batch, spawn reader R1 (model haiku, role structural) and reader R3 (model opus, role judgment), using the prompts below.
   - Run up to 10 subagents at a time.
   - After each subagent, append a row to `run_log.tsv`: batch, role, model, status (`ok n/n`), the subagent's reported tokens, tool uses, duration, refusals.
2. **Arm B** (batches `BR01`–`BR10` and `BO01`–`BO03`).
   - For each batch, first R1 (haiku, structural, full records).
   - When that batch is finished, run R3 (opus, judgment, **review mode**) on the same batch.
   - Then run `python3 tools/merge_review.py`.
3. **Check.** Run `python3 tools/validate.py out/haiku/*.json out/opus/*.json out/merged/*.json`. Re-run any reader whose record fails, for that article only.
4. **Compare.** Run `python3 tools/compare.py`. It writes `reports/agreement.tsv`, `reports/between_readers.tsv` and `adj_packet/`.
5. **Adjudicate.** Every article with at least one disagreement is adjudicated by an Opus subagent, 5 articles per subagent, using the adjudicator prompt below. Only adjudicate an article after both of its readers have finished.
6. **Score.** Run `python3 tools/score.py`.
7. **Report.** Write `reports/RUN_REPORT.md`:
   - models used;
   - articles done, refused or failed;
   - tokens and time per role, arm and stratum (from `run_log.tsv`: means per article);
   - error rates by role, arm and stratum (from `reports/scores.tsv`);
   - the 10 fields with the most disagreement;
   - recurring `brief_issues`;
   - arm A vs arm B: quality and judgment-reader tokens per article;
   - O vs R strata: tokens per article and error;
   - anything that went wrong.
8. **Commit** everything under `mmwr-index-eval/` to the working branch, with a short message. Do not merge to the main branch.

**Refusals.** If a subagent reports a refusal or cannot finish an article, record it in `run_log.tsv`. Retry that article once, alone. If it fails again, leave it and note it in the report. Never edit a reader's record yourself.

**Budget guard.** After arm A, sum the tokens in `run_log.tsv`. If the arm-A total exceeds 6M tokens, stop and report before arm B.

## Reader prompt: arm A, structural (R1, model haiku)
```
You are reader R1 (role structural) in the MMWR index cloud pilot. Working directory: mmwr-index-eval/ in this repository.
1. Read READER_BRIEF.md in full. It is your contract.
2. Read at least two worked examples in examples/ (suggested: mm6925a1.json and mm7223a3.json).
3. Your batch (<BATCH>): <IDS>.
   For each article, one at a time: read packets/<id>/article.txt, prepass.json and tables/*.csv; write the full record to out/haiku/<id>.json with reader.role "structural"; run `python3 tools/validate.py out/haiku/<id>.json` (that one file only) and fix until OK. Save each record before reading the next article.
4. Read only the files named above. Do not open out/opus/, out/opus_review/, out/merged/, adj_packet/, adjudication/ or reports/.
5. If a request is refused or an article cannot be completed, skip it and continue; report it.
6. reader.model = your exact model id from your system prompt; reader.reader_id = "R1".
Reply in at most 5 lines: ids written; ids skipped and why; your model id; anything unclear.
```

## Reader prompt: arm A, judgment (R3, model opus)
The same as the structural prompt, with these changes:
- reader R3, role judgment;
- write to `out/opus/`;
- `reader.role` "judgment";
- you **must** give `equity.level_confidence` and `equity.level_alternative`;
- do not open `out/haiku/`.

## Reader prompt: arm B, review (R3, model opus)
```
You are reader R3 (role judgment, review mode) in the MMWR index cloud pilot. Working directory: mmwr-index-eval/.
1. Read READER_BRIEF.md in full, especially §6 (review mode). Read at least two examples in examples/.
2. Your batch (<BATCH>): <IDS>. For each article: read packets/<id>/ and the structural record out/haiku/<id>.json; write out/opus_review/<id>.json as §6 specifies (changes only for fields you would set differently, plus the complete equity object with level_confidence and level_alternative). Check that it parses: python3 -c "import json,sys; json.load(open(sys.argv[1]))" out/opus_review/<id>.json
3. Do not open out/opus/, out/merged/, adj_packet/, adjudication/ or reports/.
4. If a request is refused or an article cannot be completed, skip it and continue; report it.
Reply in at most 5 lines: ids written; number of changes per article; ids skipped and why; your model id.
```

## Adjudicator prompt (model opus)
```
You are an adjudicator in the MMWR index cloud pilot. Working directory: mmwr-index-eval/.
1. Read ADJUDICATOR_BRIEF.md in full, then READER_BRIEF.md in full.
2. Your articles: <IDS>. For each: read adj_packet/<id>/ and packets/<id>/; write adjudication/<id>.json in the brief's format and check it parses.
3. Do not open adj_mapping.json, out/ or reports/.
4. If a request is refused, retry with a smaller piece of text; report anything you could not complete.
Reply in at most 6 lines: ids written; counts of verdict categories; the most important pattern.
```
