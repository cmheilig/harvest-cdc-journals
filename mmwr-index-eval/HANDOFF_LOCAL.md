# Handoff to a disk-local session (2026-10-16)

**Who reads this.** A Claude Code session on the owner's own computer. It takes over from the cloud session that ran the pilots. That session had only this repository. Your disk also has the packet builder and other resources that never reached the repository. Your first job is to **assess and report**, then build what `PRODUCTION_PLAN.md` describes.

**State in one paragraph.**
- The pilot is closed. Brief v5.1 and validator v2.3 are final for production.
- The confirmation run scored 0.8% field error against a 1.5% baseline, with 0 refusals in 66 articles (`reports/CONFIRM_V5_REPORT.md`).
- The owner accepted `PRODUCTION_PLAN.md`, P1–P5:
  - production on the Claude API, Message Batches;
  - gated by volume, starting at vol 73 and going down to 65;
  - a 5% random audit plus a groupwise check, with no second-reader routing (V19).
- **The owner's spending note:** spend through the platform up to about **$100** in all, then stop and reassess. At that point, either move to in-session work on regular usage, or space batches further apart.
- Batches are expected to take a day or two each.

## 1. Sync
1. `git fetch origin`.
   - If PR #4 (`mmwr-index-v5` → `main`) is merged, work from `main` and tag the merge commit `brief-v5.1`.
   - If it is not merged, ask the owner before branching from `mmwr-index-v5`.
2. Make production commits on a new branch (suggested: `mmwr-index-prod`). Open PRs to `main` and squash-merge them.
3. Your local working tree may hold files the cloud never saw: the packet builder, notebooks, local outputs. Do not delete or overwrite anything untracked without asking.

## 2. Read, in this order (keep context lean)
1. `../CLAUDE.md`: public-domain status of the source, the Read-tool rule, and the convention that a blank `Decision:` line means pending.
2. `README.md`: the file map.
3. `PRODUCTION_PLAN.md`, in full: the plan you are executing, with its decisions table.
4. `BRIEF_V5_DECISIONS.md`, V19 and V20 only.
5. `reports/CONFIRM_V5_REPORT.md`, the summary sections only.
6. `READER_BRIEF.md` and `examples/mm6925a1.json` and `examples/mm7223a3.json`: only when you build the API prompt. They are the reader's contract; you do not need them to assess.

Do not read the pilot outputs (`out/`, `adj_packet*/`, `adjudication*/`, `*_mapping.json`) unless a task needs them. They are frozen records of the pilots; never modify them.

## 3. Assess first (report to the owner before building)
1. **Packet builder.** Find the code that wrote `packets/<id>/` (`article.txt`, `tables/*.csv`, `prepass.json`).
   - The packets entered the repository through PR #2 from the owner's disk. `pycode/` (the harvest pipeline) does not write them.
   - Rebuild the 100 pilot packets with it and diff them against `packets/`. Report identical, differing (how), or not found.
2. **Corpus coverage for vols 65–73.**
   - `python3 tools/estimate_cost.py frame` counts English MMWR Weekly articles by volume from `../csv-output/cdc_corpus_df.csv`. Vols 73–65 hold 3,291 articles.
   - Check that every one has the HTML the builder needs. The repository has only the 2024 mirror zips in `../html-mirrors/`.
3. **Frame definition.** `level == "article"` may include errata, notices and announcements that need no index record. Find how the pilot samples (`sample.tsv`, `p2_sample.tsv`, column `article_type`) were drawn; the method is not recorded in the repository. Propose what is in scope, with counts per type. This changes the cost.
4. **API access.**
   - Is the Python `anthropic` SDK installed, and which version?
   - Credentials: `ANTHROPIC_API_KEY`, or `ant auth status`.
   - Is there a Console workspace for this project with a spend limit (plan §3.7)? The owner sets this; you only check.
5. **Tooling runs.** `python3 tools/validate.py examples/*.json` should print 7 OK. `python3 tools/estimate_cost.py cost` should run.

Write the findings to `reports/LOCAL_ASSESSMENT.md`. Each item gets one of: found / works, problem (what), or decision needed. Then stop for the owner.

## 4. Build (after the owner's go-ahead)
Section 3 of `PRODUCTION_PLAN.md` is the design. In order:
1. `tools/build_packets.py`: the builder from 3.1, checked in, reproducing the pilot packets.
2. `tools/api_read.py`:
   - subcommands `submit VOL`, `poll`, `collect`, `fixup`;
   - it writes `out/prod/<vol>/<id>.json` and `prod_ledger.tsv`, and prints counts and ids only, never record text;
   - before submitting, it estimates the volume's cost and refuses if the estimate exceeds the remaining budget.
3. `tools/audit_sample.py`: the 5% audit, generalized from `tools/confirm_v5.py` (blinded A/B packets, scoring) and `tools/audit.py`; plus the groupwise mechanical check.
4. Extend `tools/group_conditions.py` so missing terms go to a file for the grouping pass.
5. Add each new tool to `../.claude/settings.json` allow rules, in the same form as the existing ones.

**API facts.** Verify these with the `claude-api` skill before coding; do not work from memory.
- **Model and thinking:** `claude-opus-5-5`, with `output_config={"effort": "medium"}`. Adaptive thinking is always on; do not send `thinking`. `max_tokens` about 32000.
- **Request:** a 1-hour cache breakpoint on the fixed prefix (API preamble, brief, examples). The packet goes inline in the user turn.
- **Batches:** one batch per volume, `custom_id` = article id.
- **Fix-ups:** send the model's earlier response back unchanged (append-only).
- **Refusals and fallbacks:** check `stop_reason` for refusals. Check whether batches accept server-side `fallbacks`; record the served model either way.
- **Pricing:** $4 / $20 per 1M input / output tokens, $0.20 for cache reads, half price in batches.

**Calibrate first.** Before the first batch, run `messages.count_tokens` on 5 packets. Run 3 synchronous reads of articles already in `out/v5/` and compare them with `compare.norm`. This tests the API preamble, the one part of the reader's framing that changed. Then update the constants in `tools/estimate_cost.py`.

## 5. First production run, within the owner's ~$100
1. Vol 73 (236 articles; about $25 estimated, batch with caching). Collect the results, fix up the failures, run the audit (12 articles) and the groupwise check, then the condition grouping pass. Report the gate results (plan §4) and the **actual cost against the estimate**.
2. With the owner's go-ahead, continue with vols 72, 71, … while the total stays under about $100. Vols 73–71 come to about $93 estimated, before the audit. At the limit, stop and report so the owner can reassess.

## 6. Conventions that carry over
- **Decisions:** a blank `Decision:` line, or a blank table cell, means pending. Only an explicit accept counts.
- **Brief changes:** only between volumes, with a new tag. Re-read only what the change affects; the ledger says which.
- **Handling records:** read packets and records with the Read tool. Do not print their contents through Bash. Helpers used more than once go in `tools/`.
- **Commit messages and PRs:** no model ids in either.
- **Cloud-only items that do not apply to you:**
  - the stop hook that forced interim commits;
  - session scratchpad paths in old run logs;
  - GitHub access through MCP tools only.

## 7. Open items carried from the pilot
- About a dozen gray zones in the brief, each seen once (plan §1.4). The audit watches for them.
- Earlier volumes (64 and below): a 20-article calibration slice comes first (plan §4.4). The owner will decide later whether, and when, to index them.
