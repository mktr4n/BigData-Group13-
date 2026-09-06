# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A CS4010 Big Data coursework project (group 13). It loads the Norwegian business
register (Brønnøysundregistrene, `enheter_alle.json`, ~1.17M entities) into
MongoDB, enriches it with annual accounts fetched one-by-one from the
Regnskapsregisteret REST API, mirrors both collections to Parquet and NDJSON,
benchmarks query engines and storage formats across those mirrors, and builds a
curated single-file Parquet table for PowerBI.

All work happens in Jupyter notebooks running **inside the container**. There is
no host-side Python environment, no package manifest, no test suite and no
linter. Correctness is enforced by `assert`s and measured verification cells
inside the notebooks themselves.

The deliverable is a PDF report of at most 25 pages plus this code folder.
`Assignment_brief.md` (local only, gitignored) holds the full brief: what the
report must cover, the grading criteria, and the course's scoping suggestions
across data, processing, architecture, analysis, evaluation and academic
contribution. Read it before proposing new work — it is the standard the project
is measured against, and its closing point is that depth beats breadth, so an
added technology has to earn its place.

## Working preferences

- **Never guess.** If something has not been verified, say so explicitly and say
  what was actually checked. Distinguish a spot check from a full check.
- **Ask when there are real options** or when something is genuinely unclear.
  Number every question, and number sub-questions too, so an answer can be tied
  back to what it answers.
- **Terminal commands are Windows CMD.** Code and commands must be copy-paste
  ready: no stray characters, no shell continuations CMD will not accept, lightly
  commented, efficient.
- Keep explanations to the point.
- Say so when work falls short of academic standards or lacks a proper reference,
  rather than presenting it as finished.

## Environment and commands

Host is Windows 11 with WSL2-backed Docker Desktop; the project folder is
Dropbox-synced (which has consequences for Spark writes — see below).
Everything runs through Docker Compose (`group13_mongodb` + `group13_jupyter`),
compose project `group_13`, network `group_13_net`, volume `group_13_mongo_data`:

```
docker compose up -d --build            :: build the image and start both containers
docker compose up -d --build jupyter    :: after editing jupyter/Dockerfile or spark-defaults.conf
docker compose restart mongodb          :: clear the WiredTiger cache between benchmark variants

:: One-time data load (mongoimport APPENDS - never run twice against a populated collection)
docker exec group13_mongodb mongoimport --db companiesdb --collection companies --file /import/enheter_alle.json --jsonArray
docker exec group13_mongodb mongorestore --gzip --archive=/import/financial_data.archive.gz --drop

:: Sanity check the load: databases, collections, counts, sample fields
docker exec group13_jupyter python /home/jovyan/work/discover_mongo.py
```

| Service | Address |
|---|---|
| JupyterLab | http://localhost:8889/lab?token=group13 (token is fixed in the Dockerfile) |
| Spark UI | http://localhost:4041 (only while a job runs) |
| MongoDB from host | `mongodb://localhost:27018` |
| MongoDB from a container | `mongodb://mongodb:27017` |

Host ports are shifted (27018/8889/4041) so the stack can coexist with another
MongoDB or Jupyter instance; container-internal ports are standard.

Path mapping: host `./notebooks` → `/home/jovyan/work`, host `./data` →
`/home/jovyan/data` (and → `/import` in the MongoDB container). Notebook code
always uses the container paths.

**`mongosh` is not on the PATH in the MongoDB image** — only `mongoimport`,
`mongorestore` and the other tools. Anything a shell session would have done goes
through `pymongo` from the Jupyter container instead.

**Do not touch `../ML_assignment_test`.** A separate older stack lives there
(containers `cs4010_*`, volume `group_assignment_mongo_data`, host ports
27017/8888/4040). It belongs to the solo assignment, and it is the reason this
stack's host ports are shifted by one.

## Pipeline order

Notebooks are not independent; each consumes what the previous one wrote.

1. `Fetch_all_financial_data.ipynb` — needs `companies` populated. Calls the
   Regnskapsregisteret API per organisation number into `financial_data`
   (`_id` = organisasjonsnummer). Hours-long, resumable (the work list is
   recomputed as a set difference every run), safe to interrupt. Only HTTP 200
   (`success`) and 404 (`no_data`) are written; anything else stays eligible for
   retry. `MAX_WORKERS = 5` against a public government API — do not raise it.
2. `Analyse_data.ipynb` — MongoDB-side analysis plus the full profiling pass that
   `schemas.py` is derived from. Its first cell creates the
   `companies.organisasjonsnummer` index, so run that one first.
3. `Export_to_parquet.ipynb` / `Export_to_ndjson.ipynb` — write the file mirrors
   under `data/parquet/` and `data/ndjson/` using the schemas from `schemas.py`.
4. `Benchmark_engines.ipynb` — five variants (A1, A2 MongoDB server-side; B
   Spark + connector; C Spark + Parquet; D Spark + raw JSON/NDJSON) × three
   workloads (W1 selective join, W3 unindexed predicate, W4 wide read plus
   aggregation). Writes `data/benchmark_results.json`.
5. `Build_analytics.ipynb` — flattens the Parquet mirror into
   `data/parquet/analytics_company_financials.parquet` (one row per registered
   entity, left join, financial columns null where nothing was filed) plus a
   thread-count scalability experiment. Writes `data/analytics_build_summary.json`.

`Diagnose_variant_a.ipynb` and `Diagnose_balance_and_layout.ipynb` are
investigations spun out of the benchmark and the build; they write their own
`data/diagnos*.json` and are deliberately kept separate from the deliverables.

## Architecture and invariants

**`notebooks/schemas.py` is the single source of truth.** Both exports and the
benchmark import `COMPANIES_SCHEMA` and `FINANCIAL_SCHEMA` from it, so all
variants provably read the same columns and Spark never runs an inference pass —
which would land as a cost on the JSON variant only, and which sampling makes
unsafe here (`naeringskode3` occurs in 1,576 of 1,171,373 records). Read its
module docstring before touching it. Three API field names are misspelled at
source (`regnkapsprinsipper`, `sumInnskuttEgenkaptial`, `omloepsmidler`) and are
reproduced verbatim — "fixing" any of them resolves the column to null everywhere.

**Editing `schemas.py` invalidates the mirrors.** The export notebooks skip work
when a cheap source signature (document count, plus newest `fetched_at` for
`financial_data`) is unchanged, and a schema change does not move that signature.
Set `FORCE_REFRESH = True` for the first run after any schema edit.

**Spark JVM configuration lives in `jupyter/spark-defaults.conf`, never in a
notebook.** `spark.driver.memory 8g`, `spark.master local[4]`, the Mongo
connector coordinate and the connection URIs are applied at JVM launch, so every
kernel is configured identically. Changing them means editing that file and
rebuilding the image; notebooks read the values back off `SparkConf` rather than
restating them. The one deliberate exception is the scalability experiment in
`Build_analytics.ipynb`, which stops the session and rebuilds it per thread count.

**Spark must never commit directly onto `data/`.** That bind mount points at a
Dropbox-synced Windows folder. Spark's committer writes under `_temporary` and
renames files into place, and those renames fail intermittently because Dropbox
holds files it is uploading — `java.io.IOException: Could not rename ...`. The
failure is destructive: `mode("overwrite")` deletes the previous output first, so
a failed write leaves nothing. Ordinary file writes into the folder are fine;
only the rename dance fails.

So every Spark write is staged on container-local disk and copied across
afterwards, through `notebooks/staged_write.py` — `write_staged(df, target,
"parquet"|"json")` for a directory of part files, `write_staged_file(df.coalesce(1),
target)` for the single analytics file PowerBI reads. Publishing removes the
previous output file by file rather than with `rmtree`, because directory removal
is the other operation that fails on this mount, and a stale part file left behind
would be read back as data. With this in place Dropbox no longer has to be paused.
A new Spark write that lands anywhere under `data/` should go through this module
rather than calling `df.write` on the target directly.

**Spark 4 runs with ANSI mode on.** Casting a malformed string to DATE raises
`CAST_INVALID_INPUT` rather than returning null, so register date columns use
`try_cast` and a probe cell counts what that nulls.

**`financial_data.data` is an array holding exactly one element** in every
populated record (verified over the full collection). Code takes `data[0]`
directly; do not add an `explode` — the join is one-to-one by design.

## Conventions to preserve when editing notebooks

- **Nothing is a stored constant.** Verification cells measure both sides in the
  same run (Parquet row count against a live MongoDB count, non-null frequencies
  against the source), so the checks keep working as the corpus grows. The one
  snapshot literal, `EXPECTED_ROWS` in the benchmark, exists to fail loudly if
  the collections moved under a timing run.
- **Every figure a report might cite is persisted to `data/*.json`** by the cell
  that measured it — not assembled at the end, not left as cell output.
- **Benchmark method:** one discarded warm-up run, then `REPEATS = 3` timed runs,
  median reported. Results from every completed variant are compared against the
  first completed one before any timing is quoted: group keys and integer counts
  exactly, float sums at 1e-9 relative tolerance. Two MongoDB/Spark semantic
  differences are reconciled explicitly rather than absorbed by that tolerance —
  a `$group` `_id` sub-field whose source path is missing is omitted by MongoDB
  but null in Spark (use `$ifNull` / `.get()`), and `$sum` over an all-missing
  group is `0` in MongoDB but `null` in Spark (coalesce the Spark side).
- Use `SELECTED_VARIANTS` in `Benchmark_engines.ipynb` to run a subset in its own
  kernel when isolating cache contention between variants.
- Markdown cells carry the reasoning — why a formulation was chosen, what a
  limitation is, what a number means. Keep that standard: a change that alters a
  method or a figure should update the surrounding prose in the same edit.
- `notebooks/.ipynb_checkpoints/` holds stale Jupyter autosaves. Ignore them.

## Data files and git

`.gitignore` excludes the bulk data (`data/*.json`, `*.gz`, `data/ndjson/`,
`data/parquet/*`) and then re-includes the small result artefacts by name:
`benchmark_results.json`, `analytics_build_summary.json`, the profile and
diagnostic JSONs, and `analytics_company_financials.parquet`. A newly persisted
result needs its own negation line or it will silently go uncommitted.

`enheter_alle.json` and `financial_data.archive.gz` are not in the repo and must
be placed in `data/` by hand. The register snapshot is dated **2026-08-25**:
1,171,373 companies, of which 431,581 (36.8%) are AS. `financial_data` is not a
snapshot — it grows whenever the fetch notebook is re-run, so quote it with a
date. **As of 2026-09-04 it holds 1,170,292 documents, 444,646 of them with a
filed statement** (`fetch_status = "success"`); older figures in the notebooks
and docs (1,170,290 / 444,644) are earlier states of the same collection, not
errors.

`Articles/` (course reading, with plain-text extractions under
`Articles/extracted/`) and `Assignment_brief.md` are gitignored as local
reference material.

## Established findings — do not re-derive

Verified over the full data, not sampled. Treat these as settled and cite them
rather than re-running the check:

- **A persistent set of roughly 1,081 organisation numbers (0.09%) returns HTTP
  500.** Reproduced across different days and at a throttled 1 request/second,
  with no `Retry-After` header, spread across 16 legal forms and unexplained by
  entity type. They are never written and are retried on every run. The set is
  persistent but **not fixed**: the shortfall measured 1,083 on 2026-08-31,
  1,082 on 2026-09-01 and 1,081 on 2026-09-04, and is 1,081 in the committed
  benchmark run, so a few do eventually succeed. Do not describe it as
  deterministic without that caveat.
  **Decision taken: accept and document, do not work around.**
- **ENK and Forening are near-100% `no_data`** — those forms are not required to
  file accounts, so the gap is a legal fact, not a fetch failure.
- **AS entities with no data are overwhelmingly founded 2024 or later**, i.e. not
  yet due to file.
- **The Regnskapsregisteret `?år=` parameter is ignored.** The API always returns
  the most recent filing, so `data` holds exactly one period per company and
  time-series analysis is out of scope for the data as fetched.

## Longer-form documentation

- `Walkthrough of files.md` — setup narrative, dataset description, technology
  rationale. Its benchmark section predates the current notebook (it describes
  three variants and a single workload).
- `notebooks/README_benchmark_section.md` — current benchmark methodology: why
  schemas are hardcoded, variant and workload definitions, correctness rules,
  known limitations. `Benchmark_engines.ipynb` is the authority where the two
  disagree; it splits variant A into A1/A2 to separate query formulation from
  engine choice.
- `Assignment_brief.md` — the course brief: deliverable, report requirements,
  grading criteria, and the scoping suggestions the project is assessed against.
  Gitignored: it is course material, not part of the deliverable.

`README.md` is still only prerequisites and setup steps; it has no analysis
section yet, and the report's code folder is expected to ship with one.
