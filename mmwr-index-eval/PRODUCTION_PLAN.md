# Production plan (accepted 2026-10-16)

Entry point for the session that builds and runs production. It assumes brief v5.1 (`READER_BRIEF.md`), validator v2.3 (`tools/validate.py`) and the decisions in `BRIEF_V4_DECISIONS.md` and `BRIEF_V5_DECISIONS.md`. The owner accepted P1–P5 (table at the end). Build and run from a disk-local session: start with `HANDOFF_LOCAL.md`.

## 1. Close out the pilot
1. **Merge the v5.1 PR** (squash). Tag the merge commit `brief-v5.1`. Every production record names that tag (see 3.4), so later brief changes never mix silently with earlier reads.
2. **Confirm V20** in `BRIEF_V5_DECISIONS.md`. *Done: accepted by the owner.*
3. **No further confirmation run.** v5.1 changes wording at rule boundaries, not the record shape. Pilot 2 and the confirmation run already settled effort, the brief and routing. The first production gate (vol 73, 4.2) is the check.
4. **Remaining brief gray zones** from `reports/cv5_brief_issues.md` that v5.1 did not address, each seen once:
   - ecologic regression and `nonindependence`;
   - case breakdown by reporting source;
   - "Trends" in a title;
   - continuous-outcome rule collision;
   - outcome types for cited quantities;
   - non-SPD determinants;
   - surveillance inside an outbreak investigation;
   - reporting-form fields as descriptors;
   - QI investigations;
   - descriptive program measures;
   - the V9(i) note citing mm7332e1.

   They stay open and the audit watches for them. A brief edit happens only between volumes and makes a new tag.
5. **Condition map.** Add the 23 terms that `reports/cv5_conditions.tsv` reports as missing. *Done 2026-10-16: the map has 147 terms.* In production this is a grouping pass after each volume.
6. **Packet builder.** `packets/` was built on the owner's disk, outside this repository, and arrived through PR #2. No builder is checked in. Find it, or rewrite it, as `tools/build_packets.py` (article.txt in reading order with references omitted, `tables/*.csv`, `prepass.json`). Check it reproduces the 100 pilot packets byte for byte (or with documented differences) before it builds production packets. **This is the largest open item.**

## 2. Shape of production (any environment)
The unit of work is one article. Every environment does the same four things, so moving between them is a change of driver, not of method:
1. **Input:** `packets/<id>/`, built by `tools/build_packets.py`.
2. **Contract:** `READER_BRIEF.md` plus the two worked examples (`examples/mm6925a1.json`, `examples/mm7223a3.json`), at tag `brief-v5.1`.
3. **Output:** `out/prod/<vol>/<id>.json`. It must pass `tools/validate.py`; a failure gets one fix-up turn that shows the validator's messages.
4. **Ledger:** `prod_ledger.tsv`, one row per attempt:
   - id, volume, environment, model as served, effort, brief tag;
   - input, output and cache tokens, cost;
   - status (ok, fixed, failed, refused).

   `tools/runlog.py` is the model; the ledger replaces the per-run logs.

**Environments.**
- **(a) Claude API, Message Batches.** Primary; cheapest; see section 3.
- **(b) Claude Code subagents**, cloud or local. The pilot method: `RUN_*.md` runbooks, readers that open files themselves.
- **(c) Others** (Managed Agents, Bedrock, Vertex). Same request shape as (a); see the provider notes in section 3.

**Before switching environments:**
- Run a 20-article equivalence slice, re-reading articles already done.
- Compare with `compare.norm`. Expect field differences no higher than the pilot's two-reader rate.
- Adjudicate the differences only if that bar fails.

The main thing that changes is the reader's framing. In (b) the reader opens files and runs the validator itself; in (a) and (c) the packet is inline and the driver validates. So each environment has its own short preamble on top of the same brief. The slice tests that preamble.

## 3. API driver (environment a)
1. **Model and effort.**
   - `model="claude-opus-5-5"` and `output_config={"effort": "medium"}` (D20). Medium is the default on this model, but set it anyway.
   - Thinking is adaptive and cannot be disabled; leave `thinking` unset.
   - `max_tokens=32000`: thinking counts toward it; the record averages about 2K tokens.
2. **Request.**
   - `system`: the API preamble, then the brief, then the two examples. Put `cache_control: {"type": "ephemeral", "ttl": "1h"}` on the last fixed block.
   - `user`: the packet. Label each file (`article.txt`, `prepass.json`, `tables/t1.csv`, …) so table and figure notes can be cited as in the brief.
   - The preamble replaces the brief's tool instructions: the packet is inline, output only the JSON record, `reader.model` is filled by the driver. Keep it byte-stable: any change to the prefix invalidates the cache.
3. **Output and validation.**
   - Parse the JSON, then run `validate.py`.
   - Structured outputs (`output_config.format` with a JSON schema) are optional. The validator's cross-field checks cannot be expressed in a schema anyway, and a large conditional schema may exceed structured-output limits. Start without; add it only if parse failures are common.
   - **Fix-up:** a follow-up request with the original messages, the assistant's response passed back unchanged (thinking blocks included: append-only), and a user turn listing the validator errors. Fix-ups go out as a small second batch, or synchronously.
4. **Provenance.** The driver writes these fields; the model does not:
   - `reader.model` from `response.model`;
   - `reader.reader_id = "prod"`;
   - the brief tag;
   - the batch and request ids.
5. **Batches.**
   - One batch per volume, `custom_id` = article id. Results come back in any order, so key by id.
   - Up to 100,000 requests or 256 MB per batch. Most finish within an hour; the maximum is 24 hours. 50% off every token class.
   - Cache hits inside a batch are best effort. Warm the cache with one ordinary request just before submitting. The estimate below gives both the cached and the uncached figure.
6. **Refusals.**
   - Check `stop_reason`; a `"refusal"` is logged, then retried once alone.
   - Server-side fallbacks (`fallbacks: "default"`, beta `server-side-fallback-2026-07-01`) can answer with a different model. Check whether batches accept it.
   - If fallbacks are used, the ledger records the served model, and the audit samples fallback reads as their own stratum.
   - The pilot readers produced no record-level refusals, and the source is public-domain government text, so refusals should be rare.
7. **Cost control.**
   - A separate Console workspace for this project, with a monthly spend limit.
   - The driver refuses to submit a volume whose estimate exceeds the remaining budget line in the ledger.
8. **Provider notes for (c).**
   - Bedrock (Mantle client) names the model `anthropic.claude-opus-5-5`; Vertex uses the bare id.
   - Pricing and batch support differ by provider; check before switching.

**Tooling to add to `tools/`:**
- `build_packets.py`;
- `api_read.py` (submit, poll, collect, fixup, with the ledger);
- `audit_sample.py`, generalizing `confirm_v5.py` and `audit.py`;
- an extended `group_conditions.py` that writes missing terms for the grouping pass.

**Estimate.** From `python3 tools/estimate_cost.py cost [VOL ...]`. Sizes are character-based guesses: about 18K fixed prefix, 7K packet, 2K record, and an assumed 6K thinking. Replace them with `count_tokens` and the vol 73 actuals.

| Scope | Articles | Batch + cache | Batch, no cache hits |
|---|---|---|---|
| vols 73–65 | 3,291 | ~$345 | ~$435 |
| vols 64–31 | 9,637 | ~$1,010 | ~$1,270 |

Add about 10% for the audit (section 5). For comparison, the cloud confirmation run cost about $0.58 per article, adjudication included. The pilot packets lean older, and recent articles are longer, so expect the vol 73 actuals above the estimate.

## 4. Volume gating
1. **Order:** vol 73, then 72 … 65, one volume per batch. Then pause and decide whether to go on to 64 … 31 and at what pace.
2. **A volume passes its gate when all of these hold:**
   - every article has a valid record, or a logged failure or refusal (at most 1%);
   - the audit error (section 5) is at or below the confirmation run's level (field error ≤ 1.5%) and the groupwise check passes;
   - cost per article is within 25% of the running estimate;
   - the condition grouping pass for the volume is done.
3. **When a volume fails:** stop. Diagnose. If the brief needs a change, make it as a new tag. Re-read only the fields or articles affected (the ledger shows which).
4. **Relaxing the gates for earlier volumes.**
   - After 65, gate every three volumes instead of every one, and audit the same 5%.
   - Before vol 64, run a 20-article calibration slice. Older reports often lack the Summary box (the framework rule's fallback applies), have fewer tables and different article types. The pilot's 1999–2001 stratum is the only evidence so far.
   - Earlier years can be deferred indefinitely without touching the work already done.

## 5. Quality assurance (V19)
1. **Random audit.** Per volume, 5% of articles, at least 10.
   - A second read in the same environment and at the same effort.
   - `compare.norm` differences go to an adjudicator, blinded as A/B: `ADJUDICATOR_BRIEF.md`, packets made as in `confirm_v5.py`.
   - Field error is scored as in `reports/cv5_scores.tsv`.
2. **Groupwise check.** Groupwise entries were the main v5 error (most of the 14 wrong fields).
   - Every record with a non-empty groupwise entry gets a mechanical check: levels named in the article tables, an SPD present, rule-2 tests.
   - A further random 10% of those get an adjudicator's look.
3. **`uncertain_fields`.** Kept for analysis; it does not route a second read (V19). Report its distribution per volume, and compare the audit error on flagged and unflagged records.

## 6. Session hygiene
- Build the tooling in a fresh session that starts from this file; do not continue the pilot session. Each volume's run can be its own session, since the ledger carries the state.
- Commit after each volume. Squash-merge PRs; the cloud stop hook makes interim commits.
- Read packets and records with the Read tool, as `CLAUDE.md` says. The API driver keeps record content out of terminal output; it prints counts and ids only.

## Decisions
| Item | Proposal | Decision |
|---|---|---|
| P1 | Close-out steps 1–6 above | accept |
| P2 | API Message Batches as primary environment; equivalence slice on any switch | accept |
| P3 | Gate per volume for 73–65 as in section 4; relax afterward | accept |
| P4 | Audit 5% per volume (min 10) plus the groupwise check | accept |
| P5 | Budget line for vols 73–65: $500 including audit | accept |

Notes: I've revised and committed V20. I plan to use batch API for batches, which tend to take a day or 2. I plan to continue until  projected platform spending totals about $100, and I will reassess at that time. I will consider whether to switch to in-session work (regular usage tokens) or to space API batches further apart in time.
