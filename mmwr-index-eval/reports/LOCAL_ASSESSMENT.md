# Local assessment (2026-10-16)

`HANDOFF_LOCAL.md` §3, done from the owner's disk on branch `mmwr-index-prod` (from `main` = `af44748`, tag `brief-v5.1`). Nothing was built or sent to the API. Only counts, ids and file names were printed; no packet or record text.

"The extraction" below means the owner's extraction build, `~/cdc-corpora/_test/mmwr-extract-v2` (its own git repository, clean at `bdf1f54`). It was only read.

| Item | Status |
|---|---|
| 1. Packet builder | **found / works**: rebuild is byte-identical |
| 2. Corpus coverage, vols 65–73 | **found / works**: every in-scope article has its sources; **decision needed** on the sealed holdout |
| 3. Frame definition | **decision needed**: what is in scope (proposal below) |
| 4. API access | SDK **found**; credentials **problem** (none yet, as expected); workspace **owner to check** |
| 5. Tooling runs | **works** |

## 1. Packet builder: found / works
- **What it is.** `stage6b/build_packets.py` in the extraction, 27 lines. It does not parse HTML. It copies from the extraction's frozen outputs:
  - `article.txt` = a 4-line header (`id`, `url`, `date`, `article_type`) plus the `text` field of `views/index_text/<year>.jsonl`;
  - `prepass.json` = that article's line of `stage6b/prepass.jsonl`, written with `indent=1`; that file comes from `python -m mmwrx.index_prepass build`;
  - `tables/*.csv` = copies of `views/tables/<year>/<id>_T*.csv`.
- **The checked-in script is not the one that built `packets/`.** It reads the stage 6b sample (80 ids plus 7 examples) and writes to `stage6b/packet/`. The cloud packets were built at 21:30 on 2026-10-10 into `stage6c/package/mmwr-index-eval/packets/`, from `stage6c/cloud_sample.tsv`, by a variant that was never saved. `diff -r` shows the repository's `packets/` is identical to that package.
- **Rebuild.** I ran the same logic on the 100 ids in `sample.tsv` into a scratch directory. The result is **byte-identical: 293 of 293 files** (100 `article.txt`, 100 `prepass.json`, 93 table CSVs), with no missing ids.
  - The one detail to keep: the builder makes an empty `tables/` directory when an article has no tables (42 packets). Git does not track empty directories, so a fresh clone lacks them. This is harmless.
- **The inputs are pinned.** `python -m mmwrx.manifest --check` in the extraction reports `manifest matches`, so documents, views and table directories are unchanged since they were frozen. `prepass.jsonl` is not in the manifest; it is git-ignored and can be regenerated.
- **For `tools/build_packets.py` (build step 1):** port this copier, reading the extraction read-only, with the extraction root as a parameter. Check it against `packets/` as above.
  - Ids must allow double issues: 59 articles in vols 65–73 have ids like `mm695152a1` (six digits).
  - Reimplementing from HTML is not needed: the extraction already did that and verified it (its Stages 4–5).

## 2. Corpus coverage, vols 65–73: found / works
- **The builder needs extraction views, not HTML.** The 2024 zips in `../html-mirrors/` are not used.
  - The views came from `~/cdc-corpora/_test/json_out_20251016/` (the 2025 release) and the mirror.
  - The extraction's metadata CSV (`~/cdc-corpora/_test/cdc_corpus_df.csv`) has the same SHA-256 as `../csv-output/cdc_corpus_df.csv`.
- **Matching the frame (`estimate_cost.py frame`) to the extraction:**

| Vol | Frame | In documents | In views and prepass | Not in views |
|---|---|---|---|---|
| 73 | 236 | 236 | 231 | 5 |
| 72 | 316 | 316 | 300 | 16 |
| 71 | 336 | 336 | 321 | 15 |
| 70 | 397 | 397 | 367 | 30 |
| 69 | 447 | 447 | 419 | 28 |
| 68 | 315 | 315 | 295 | 20 |
| 67 | 380 | 380 | 358 | 22 |
| 66 | 424 | 424 | 381 | 43 |
| 65 | 440 | 440 | 412 | 28 |
| **All** | **3,291** | **3,291** | **3,084** | **207** |

  All 207 articles left out of the views are errata. The extraction excludes errata from its views by design, and they need no index record. **Every non-erratum article has a packet source.**
- **Decision needed: the extraction's sealed holdout.** The extraction sealed 837 ids (10%) for verifying the extraction (`DECISIONS.md` 2026-10-04). It used 90 of them in Stage 5 and kept the rest "for a possible second round" (`STAGE5_PLAN.md` §1).
  - **267 of the 2,887 articles proposed in §3 are holdout ids** (27 in vol 73).
  - Reading them in production exposes them: their text goes to the API, and their records go into this public repository.
  - The pilot samples kept clear of the holdout (`holdout_guard()`); production would be the first to touch it.
  - Options:
    - **(a)** read them with their volumes and give up a second extraction round on those ids;
    - **(b)** leave them out of the volume batches and read them later as one batch, once the owner closes the holdout. Nothing else waits on this.
  - Recommendation: (b), since it costs nothing now and keeps the choice open.
- **Bookkeeping (owner).** The 100 pilot ids are not yet in the extraction's `exposure_log.tsv`. `stage6c/DEPLOY_TOMORROW.md` says to log them once the run starts. Production reads will need the same logging. That file is in the extraction, which this session treats as read-only, so the owner decides who writes it.

## 3. Frame definition: decision needed
- **How the pilot sample was drawn.** `stage6c/draw_cloud_sample.py` in the extraction, seed `20261012`. It drew from ids that were unexposed and outside the holdout, with fixed quotas per extraction `article_type`:
  - 2023–24 (78): `full_report` 54, `notes_from_the_field` 14, `quickstats` 8, `vital_signs` 2;
  - 1999–2001 (22): `report` 18, `notice_to_readers` 2, `update` 2.

  `p2_sample.tsv` is the same 100 ids, and `cv5_sample.tsv` is 66 of them. The `article_type` column in all three is the extraction's classification (`article_types.tsv`), not a column of the corpus CSV.
- **Evidence from the pilot records:** both `notice_to_readers` articles came out `data_focus: none` in pilot 2. Every other type that was sampled came out `yes` or `limited`, except 1 of 54 `full_report`.
- **Types in vols 65–73 (articles in the views), with a proposal:**

| Type | 73 | 72 | 71 | 70 | 69 | 68 | 67 | 66 | 65 | Total | In pilot | Proposal |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| full_report | 145 | 175 | 228 | 281 | 307 | 157 | 198 | 189 | 205 | 1,885 | yes | in |
| notes_from_the_field | 45 | 56 | 36 | 26 | 42 | 52 | 66 | 52 | 66 | 441 | yes | in |
| quickstats | 28 | 52 | 51 | 54 | 52 | 51 | 50 | 51 | 51 | 440 | yes | in |
| vital_signs | 4 | 4 | 5 | 2 | 5 | 11 | 9 | 10 | 11 | 61 | yes | in |
| outbreak_report | 7 | 9 | 1 | 2 | 7 | 4 | 7 | 3 | 8 | 48 | no | in (report family) |
| grand_rounds | 0 | 0 | 0 | 0 | 0 | 0 | 2 | 5 | 5 | 12 | no | in (report family) |
| announcement | 0 | 0 | 0 | 0 | 0 | 0 | 1 | 51 | 45 | 97 | no | out (notice family) |
| cover_box | 0 | 0 | 0 | 0 | 3 | 19 | 18 | 14 | 14 | 68 | no | out (notice family) |
| notice_to_readers | 2 | 1 | 0 | 0 | 1 | 1 | 4 | 6 | 7 | 22 | yes, as `none` | out |
| correction_republication | 0 | 1 | 0 | 2 | 2 | 0 | 2 | 0 | 0 | 7 | no | out (correction family) |
| retraction | 0 | 1 | 0 | 0 | 0 | 0 | 1 | 0 | 0 | 2 | no | out |
| expression_of_concern | 0 | 1 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 1 | no | out |
| erratum (not in views) | 5 | 16 | 15 | 30 | 28 | 20 | 22 | 43 | 28 | 207 | no | out |
| **Proposed in scope** | **229** | **296** | **321** | **365** | **413** | **275** | **332** | **310** | **346** | **2,887** | | |
| of which holdout | 27 | 29 | 29 | 34 | 34 | 19 | 37 | 27 | 31 | 267 | | see §2 |

- **Effect on cost** at the current estimate of $0.105 per article (batch with caching):
  - the whole frame, 3,291 articles: ~$345;
  - the proposed scope, 2,887 articles: **~$303**;
  - the proposed scope without the holdout, 2,620 articles: ~$275.
  - Vols 73–71 in scope come to 846 articles, ~$89 (761 articles, ~$80, without the holdout). This is before the audit and before calibration with `count_tokens`.
- **Points to decide:**
  1. Which types are in scope. The proposal is the 6 report-family and quickstats types.
  2. Whether the two types the pilot never saw (`outbreak_report` 48, `grand_rounds` 12) need anything extra. The proposal: no extra run, but the audit samples at least one of each in any volume that has them.
  3. Whether excluded types get a stub record (id, type, "not indexed") so the index accounts for every article. The proposal: yes, written by the driver at no API cost.

## 4. API access
- **SDK: found.** `anthropic` 0.103.0 is installed in the system `python3` (3.13.7, `/Library/Frameworks/...`). The extraction's `.venv` does not have it, and does not need it. Whether this version supports `output_config`, batches, `count_tokens` and server-side fallbacks is to be checked with the `claude-api` skill at build time.
- **Credentials: problem (expected).** `ANTHROPIC_API_KEY` is not set, and the `ant` CLI is not installed. The owner will mint a key at the build step.
  - Pass the key as an environment variable. This session's sandbox blocks reads of any file under `~` whose name contains `api-key` or `apikey`, so a key file with such a name would not be readable by the driver.
  - `ANTHROPIC_BASE_URL` is set in this shell to the default `https://api.anthropic.com`, inherited from the session. It is harmless.
- **Console workspace and spend limit (plan §3.7): owner to check.** This cannot be seen without a key. The suggested setting is a workspace for this project with a monthly limit near the $100 spending note, and the key minted inside that workspace.

## 5. Tooling runs: works
- `python3 tools/validate.py examples/*.json`: 7 OK.
- `python3 tools/estimate_cost.py cost`:
  - inputs: fixed prefix ~18.4K tokens, packet mean ~7.0K (max ~42.3K), record ~2.1K, thinking assumed 6K;
  - result: $0.105 per article, batch with caching; vols 65–73 at $345.
  - It counts the whole frame, errata included. Once the scope is decided, `frame()` should count the scope instead. The vol 73 figure falls from 236 articles to 229, or 202 without the holdout.

## Decisions for the owner
| Item | Proposal | Decision |
|---|---|---|
| L1 | Build `tools/build_packets.py` as a port of the extraction's copier, reading the extraction read-only | owner: accept. *Done: `tools/build_packets.py check` gives 293 of 293 files identical* |
| L2 | Scope = full_report, notes_from_the_field, quickstats, vital_signs, outbreak_report, grand_rounds (2,887 in vols 65–73); others get a driver-written stub | owner: revised, see below |
| L3 | Holdout ids (267 in scope) held out of the volume batches until the owner closes the extraction holdout | owner: accept (holdout omitted initially) |
| L4 | Who logs production and pilot exposure in the extraction's `exposure_log.tsv` | owner: Claude may log (as `DEPLOY_TOMORROW.md` implies) |
| L5 | QuickStats in vols 73–71: the extraction classes QuickStats as its own family, not the report family | owner: include QuickStats in vols 73–71 (761 articles, holdout omitted) |

## Owner's response (2026-10-16)
The owner's original plan was to index only the 1,885 full reports in vols 65–73. The pilot widened the net to test feasibility. The other types were evaluated for feasibility only, not for their use to learners. The sequence:
1. **Vols 73–71: the whole report family.** That is `full_report`, `notes_from_the_field`, `vital_signs` and `outbreak_report` (these volumes have no `grand_rounds`), plus `quickstats` (L5).
2. **Evaluate whether vols 65–70 are limited to full reports**, using the vol 73–71 records as evidence.
3. **Implement the frame and sequence for vols 65–70** on that basis.
4. **After vols 65–73 are complete**, decide whether and how to refine the frame for earlier volumes. Before vol 65, the source has no internal metadata that tells full reports from other report types; this extraction labels them all `report` or `update`.

**Counts, holdout omitted** (holdout count in brackets), at $0.105 per article before calibration:

| Vol | full_report | notes_from_the_field | vital_signs | outbreak_report | Report family | + quickstats |
|---|---|---|---|---|---|---|
| 73 | 130 [15] | 40 [5] | 1 [3] | 5 [2] | 176 | 202 |
| 72 | 159 [16] | 52 [4] | 4 [0] | 8 [1] | 223 | 267 |
| 71 | 206 [22] | 33 [3] | 5 [0] | 1 [0] | 245 | 292 |
| **73–71** | | | | | **644, ~$68** | **761, ~$80** |

For comparison, full reports only in vols 65–70 come to 1,212 articles with the holdout omitted (125 more are holdout ids): ~$127.

**Table trimming (L6, measured 2026-10-16).** Of the 761 vol 73–71 packets, 507 have table CSVs. By characters:
- `article.txt` is 72.7% of packet input. It already renders each table's labels and text cells; the numbers of mostly numeric tables are left out.
- `prepass.json` is 8.3%.
- The table CSVs are 19.1%. Their numeric data cells (not header, stub or spanned, by the extraction's cell flags) are 5.8%.
- The largest 10% of CSVs hold 36% of all CSV characters.

At batch prices the whole packet input for vols 73–71 is about $16 of the ~$60 estimate; output, thinking included, is about $34. Blanking numeric data cells would save about $1–1.5; dropping the CSVs entirely, about $3.

| Item | Proposal | Decision |
|---|---|---|
| L6 | No table trimming for vols 73–71. Revisit before vols 70–65 only if the vol 73 actuals show input above about a third of cost. Any trimmed format would be built from the extraction's grid flags (header, stub, value kind), not the CSV, and tested on a 20-article slice | owner: accept (no trimming for vols 73–71) |

**Exposure log (L4).** The append to the extraction's `exposure_log.tsv` (100 pilot ids, in the file's `id, date, reader, purpose` format) was blocked by this session's permission classifier, since the extraction is a separate repository. *Done 2026-10-16: the owner ran the append; all 100 ids are logged. The change is not committed in the extraction.* Production reads will need the same logging; `api_read.py submit` can write the ids to a local file for the owner to append.
