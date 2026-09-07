# CS4010 Big Data — Group 13

Pipeline for loading the Norwegian business register (Brønnøysundregistrene)
into MongoDB and enriching it with financial statement data from the
Regnskapsregisteret API.

## Folder structure
```
group_13/
├── data/
│   ├── enheter_alle.json        <- the dataset (2.0 GB, not included in submission)
│   ├── parquet/                 <- written by Export_to_parquet.ipynb
│   ├── ndjson/                  <- written by Export_to_ndjson.ipynb
│   └── *.json                   <- recorded results: benchmark, build summary, profiles
├── jupyter/
│   ├── Dockerfile
│   └── spark-defaults.conf
├── notebooks/
│   ├── discover_mongo.py
│   ├── schemas.py               <- Spark schemas shared by the exports and the benchmark
│   ├── staged_write.py          <- writes Spark output without committing on the synced mount
│   ├── Fetch_all_financial_data.ipynb
│   ├── Analyse_data.ipynb
│   ├── Export_to_parquet.ipynb
│   ├── Export_to_ndjson.ipynb
│   ├── Benchmark_engines.ipynb
│   ├── Build_analytics.ipynb
│   ├── Diagnose_variant_a.ipynb
│   ├── Diagnose_balance_and_layout.ipynb
│   └── README_benchmark_section.md
├── docker-compose.yml
├── README.md
└── Walkthrough of files.md
```

`data/` and `notebooks/` are bind-mounted into the containers, so the paths
matter. Docker will create them if missing, but the dataset must be placed in
`data/` manually.

### About the dataset

`enheter_alle.json` is a bulk export of all registered Norwegian entities from
Brønnøysundregistrene. It is a single JSON array of 1,171,373 objects.

Because the register changes daily, a re-download will produce a different
snapshot with a different record count. The figures in this README refer to the
snapshot dated **2026-08-25**, containing **1,171,373 records**.

---

## Build and start the containers

From the `group_13` folder:

```
docker compose up -d --build
```

This builds one image and starts two containers:

| Container | Image | Purpose |
|---|---|---|
| `group13_mongodb` | `mongodb/mongodb-community-server:latest` | Database |
| `group13_jupyter` | built from `jupyter/Dockerfile` | JupyterLab, PySpark, analysis |

### Access points

| Service | URL / address |
|---|---|
| JupyterLab | http://localhost:8889/lab?token=group13 |
| Spark UI | http://localhost:4041 (only while a Spark job is running) |
| MongoDB (from host) | `mongodb://localhost:27018` |
| MongoDB (from containers) | `mongodb://mongodb:27017` |

Host ports are deliberately non-default (27018 / 8889 / 4041) so this stack can
run alongside another MongoDB or Jupyter instance. The ports *inside* the
containers are standard, so `mongodb://mongodb:27017` is what the notebooks use.

The Jupyter token is fixed to `group13` so no token needs to be read from the
container logs.

### Spark configuration

Spark settings live in `jupyter/spark-defaults.conf`, which the Dockerfile
copies into `$SPARK_HOME/conf/`. They are applied when the JVM launches, so
every notebook gets an identically configured session and no notebook contains
JVM configuration of its own. To change a setting, edit that file and rebuild.

| Setting | Value | Reason |
|---|---|---|
| `spark.driver.memory` | `8g` | The base image sets `SPARK_OPTS` with `-Xmx4096M`, but nothing applies that variable to the python3 kernel, so the driver would start at Spark's 1 GB default. In local mode the driver is also the executor. |
| `spark.master` | `local[4]` | `local[*]` gave one task per core (12 on this host), each holding its own MongoDB cursor batch in the same heap. |
| `spark.jars.packages` | `mongo-spark-connector_2.13:11.1.0` | Matches Spark 4.2.0 and Scala 2.13. Resolved from Maven Central on first use and cached in `~/.ivy2` inside the container. |
| `spark.mongodb.read/write.connection.uri` | `mongodb://mongodb:27017` | Service name on the compose network. |
| `spark.sql.session.timeZone` | `UTC` | Timestamp handling independent of host locale. |

---

## Load the register data into MongoDB

##Create the collection and index
docker exec group13_jupyter python -c "import pymongo; pymongo.MongoClient('mongodb://mongodb:27017/')['companiesdb']['companies'].create_index('organisasjonsnummer', unique=True); print('unique index ready')"

##Import data from JSON (first and subsequent runs)
docker exec group13_mongodb mongoimport --db companiesdb --collection companies --file /import/enheter_alle.json --jsonArray --mode merge --upsertFields organisasjonsnummer > data\ingest_2026-08-25.log 2>&1

`/import` is the container-side mount of the host `data/` folder, so no file
copying is needed.

The `--jsonArray` flag is required because the file is a single JSON array
rather than newline-delimited JSON.

Each notebook also creates the index it depends on, in
code, so the measured configuration is reproducible:

| Index | Created by |
|---|---|
| `companies.organisasjonsnummer` | `Analyse_data.ipynb`, first cell |
| `companies.organisasjonsform.kode` | `Benchmark_engines.ipynb`, setup cell |

The benchmark's setup cell creates **both**, so it can be run against a fresh
import without `Analyse_data.ipynb` having gone first. Without them it would
silently measure a different access path than the one reported, which is the
subject of the A1/A2 comparison.

Both calls are idempotent, and the benchmark records the full index inventory of
both collections in its results file, because which formulations are
index-served depends on it.

## Fetch the financial statement data

Open JupyterLab at http://localhost:8889/lab?token=group13 and run
`Fetch_all_financial_data.ipynb`.

**This step requires `companies` to be populated first.** The notebook builds
its work list by reading every `organisasjonsnummer` out of `companies`. Run
against an empty collection, it will fetch nothing and exit without error.

### What it does

For each organisation number it calls
`https://data.brreg.no/regnskapsregisteret/regnskap/{orgnr}` and writes the
result to the `financial_data` collection, using the organisation number as the
document `_id`.

Outcomes are recorded as:

| `fetch_status` | HTTP | Meaning |
|---|---|---|
| `success` | 200 | Accounts returned and stored in the `data` field |
| `no_data` | 404 | Confirmed: no accounts filed for this entity |

Anything else — a network failure, or any other status code — is deliberately
*not* written, leaving that organisation number eligible for retry.

### Configuration

Set at the top of the cell:

| Setting | Default | Notes |
|---|---|---|
| `BATCH_LIMIT` | `None` | `None` processes everything outstanding; set a number to cap the run |
| `MAX_WORKERS` | `5` | Concurrent requests. This is a public government API — do not raise this casually |
| `REQUEST_TIMEOUT` | `15` | Seconds before abandoning a single request |
| `PROGRESS_EVERY` | `100` | Progress line frequency |

### Runtime and resumability

This is a long-running step: roughly 1.17 million sequential HTTP requests
against an external API. **The full run takes several hours.**

The notebook is resumable and safe to interrupt. It computes its work list as
the set difference between `companies` and `financial_data` on every run, so
the Jupyter stop button can be used at any point and re-running the cell
continues from where it left off. Nothing already fetched is re-requested.

### Known limitation

A persistent set of roughly 1,081 organisation numbers (0.09% of the register)
returns **HTTP 500** from the Regnskapsregisteret API. This was reproduced
across separate runs on different days, and at a deliberately throttled rate of
1 request per second, which rules out client-side rate limiting — the responses
carry no `Retry-After` header. The failures are spread across 16 different legal
forms and are not explained by entity type.

Because the fetch script classifies any non-200/404 response as transient, these
records are never written and are retried on every subsequent run. The practical
consequence is that `financial_data` sits at roughly 99.9% of the register and
every further run reports a similar number of skips.

The set is persistent but **not fixed**, and the report should not call it
deterministic without saying so. The shortfall measured 1,083 on 2026-08-31,
1,082 on 2026-09-01 and 1,081 on 2026-09-04, and was still 1,081 in the
benchmark run recorded in `data/benchmark_results.json` — so a small number do
eventually succeed on a later attempt, while the great majority do not. Counts
quoted anywhere in this repository are therefore a state of the collection on a
date, not a constant:

| Date | `financial_data` | of which filed (`success`) | Shortfall |
|---|---|---|---|
| 2026-08-31 (variant-A diagnostic) | 1,170,290 | 444,644 | 1,083 |
| 2026-09-01 | 1,170,291 | — | 1,082 |
| 2026-09-04 | 1,170,292 | 444,646 | 1,081 |
| 2026-09-05 (exports, benchmark, analytics build) | 1,170,292 | 444,646 | 1,081 |

`companies` does not move: it is a bulk import of the 2026-08-25 snapshot,
1,171,373 records, of which 431,581 (36.8%) are AS.

---

## Analysis

Run `Analyse_data.ipynb`. Cells are independent of one another
and can be run in any order, but the first cell should be run first because it
creates the index on `companies.organisasjonsnummer`. Without that index, every
join against the register falls back to a full collection scan.

The notebook covers:

- Fetch coverage and outcome breakdown by `organisasjonsform`
- Distribution of fiscal years across the filings returned by the API
- Comparison of entities whose latest filing is recent (2025 or later) against
  those whose latest filing is older, on insolvency and liquidation flags
- Cross-check of the API's reported fiscal year against Brreg's own
  `sisteInnsendteAarsregnskap` field
- Founding-year breakdown of AS entities with no financial data

### Note on fiscal periods

The Regnskapsregisteret API returns only the latest available filing, so each
document's `data` field normally holds a single accounting period. Where the
analysis reduces a company to one year, it takes the most recent one
explicitly. The fiscal-year cell reports how many companies carry more than one
period, so the assumption can be checked rather than assumed.

---

## Export to Parquet and NDJSON

Run `Export_to_parquet.ipynb`, which writes both collections to Parquet under
`data/parquet/`, and `Export_to_ndjson.ipynb`, which writes `financial_data` to
`data/ndjson/`. Together they give the benchmark its file-based sources.

`companies` needs no NDJSON export: the bulk download `enheter_alle.json` already
is the file, and the profiling pass confirmed the raw file and the MongoDB
collection hold an identical set of 64 top-level fields. `financial_data` has no
file equivalent, because it was assembled from ~1.17M individual API calls.

**Both exports stage their output.** The target folders are inside a
Dropbox-synced directory, and Spark's committer renames files into place rather
than writing them there — renames that fail intermittently while Dropbox holds
the files it is uploading:

```
java.io.IOException: Could not rename
file:/home/jovyan/data/parquet/financial_data/_temporary/0/_temporary/attempt_...
to file:/home/jovyan/data/parquet/financial_data/_temporary/0/task_...
```

The failure is destructive, because `mode("overwrite")` deletes the previous
export before writing: one such run left no `financial_data` mirror at all.
Spark therefore writes to container-local `/tmp` and `notebooks/staged_write.py`
copies the finished files onto the mount, replacing the previous export file by
file. Plain file writes are what a sync client is built for, so no pausing is
needed.

Both exports read from MongoDB, not from `enheter_alle.json`, so the variants
provably see identical content. The NDJSON export is therefore a reconstruction
rather than the original ingestion path — a genuinely file-native pipeline would
have written each API response to disk as it arrived, which would also have
produced ~1.17M small files. The report states this rather than implying the
file variant is independent of the database.

### Full-width schemas

Both collections are written at **full width**, using the explicit schemas in
`notebooks/schemas.py` — every observed field, including the nested `data`
statement blob. An earlier version exported a fourteen-column subset, which
banked Parquet's column-pruning advantage at export time rather than measuring it
at query time; exporting at full width moves pruning inside the query, where it
is the thing being measured.

The schemas are hardcoded rather than inferred. Inference on JSON requires a full
scan before any query runs, which would land as a cost on the JSON variant alone,
and sampled inference is demonstrably unsafe on this data — `totalresultat`
appears in 54% of filings but in none of a ten-record sample, and `naeringskode3`
in 1,576 of 1,171,373 records. Pinning the schema also skips the connector's
inference pass, which matters because reading whole documents without one
exhausted the Spark driver heap during BSON decoding.

Three field names from the Regnskapsregisteret API are misspelled at source and
are reproduced verbatim in the schema — `regnkapsprinsipper`,
`sumInnskuttEgenkaptial` and `omloepsmidler`. Correcting any of them resolves the
column to null across every record.

### Change detection

A full re-export takes minutes, so it is skipped when the source is unchanged.
The signature is deliberately cheap to compute:

- `companies` — document count. The collection is bulk-imported and carries no
  per-document modification timestamp, so a count is the only signal available
  without scanning every document. A same-size replacement would go undetected;
  set `FORCE_REFRESH = True` after any re-import.
- `financial_data` — count plus the newest `fetched_at`, which together catch
  both appended and rewritten records.

This is a staleness check, not incremental loading. When the signature differs,
the whole collection is rewritten. The metadata file is written only after a
successful export, so an interrupted run stays marked stale rather than falsely
current.

---

## Engine benchmark

Run `Benchmark_engines.ipynb` after both exports. It answers three questions
five ways each. A **variant** is the storage and execution path; a **workload**
is the question.

| Variant | Execution | Companies source | Financial source |
|---|---|---|---|
| A1 | `mongod`, server-side | MongoDB | MongoDB |
| A2 | `mongod`, server-side | MongoDB | MongoDB |
| B | Spark JVM, `local[4]` | MongoDB via connector | MongoDB via connector |
| C | Spark JVM, `local[4]` | Parquet | Parquet |
| D | Spark JVM, `local[4]` | raw `enheter_alle.json` | NDJSON export |

A1 and A2 are the same engine, data, hardware and session, and differ only in how
the aggregation is written. **Query formulation is an experimental dimension in
its own right here**, because measurement showed it outweighing engine choice.
A1 drives from `financial_data`, `$lookup`s into `companies` and filters to AS
after the join. A2 matches AS first through `organisasjonsform.kode_1`, cutting
the driving set to 431,581, then probes `financial_data._id` — the primary index.

Neither MongoDB variant is a Python join. pymongo sends the pipeline to `mongod`,
which executes it; Python only deserialises the result. The comparison is between
a database engine and a distributed framework, not between languages.

| Workload | Question |
|---|---|
| W1 | Selective join: join on organisation number, keep AS, count by `fetch_status`. Two output rows. |
| W3 | Unindexed predicate, no join: count `konkurs = true` by legal form. No index on `konkurs`, so MongoDB must scan. |
| W4 | Wide read plus aggregation: revenue, operating profit, equity and debt for AS filers, grouped by industry code and municipality. 45,033 groups. |

Each variant runs once untimed to warm caches and let the JVM JIT-compile, then
three timed runs, and the median is reported rather than the mean so a single GC
pause does not dominate. Results are compared across every variant that
completed, against the first of them, before any timing is quoted.

### Results

Measured on 12 cores, 8 GB Spark driver heap, `local[4]`, Spark 4.2.0, connector
11.1.0, MongoDB 8.3.8. Medians in seconds, from `data/benchmark_results.json`.
All five variants agreed on all three workloads.

| Variant | W1 | W3 | W4 |
|---|---|---|---|
| A1: MongoDB, financial-first | 42.00 | 0.43 (single formulation) | 20.39 |
| A2: MongoDB, AS-first | **7.38** | — | 17.37 |
| B: Spark + connector | 10.75 | 1.50 | 19.30 |
| C: Spark + Parquet | **2.71** | 1.39 | **3.69** |
| D: Spark + JSON | 35.99 | 31.16 | 44.78 |

**Formulation against engine.** On W1 the two MongoDB formulations differ by
5.7×, against 2.7× between the best MongoDB and the best Spark result. How the
query was written mattered more than which engine ran it. Quoting the naive
A1-against-Parquet ratio alone would attribute to the engine what the formulation
caused. On W4 the formulation effect nearly vanishes (1.17×), because the
aggregation, not the join order, dominates.

**Where each wins.** MongoDB takes W3 outright, 0.43 s against Spark's best of
1.39 s: a single-collection scan with no join is what a database is for, and
Spark pays JVM and shuffle overhead for nothing. Parquet takes W1 and W4, and
its margin widens as the read gets wider — 2.7× on W1, 4.7× on W4 — because it
reads only the columns asked for and does no BSON decoding.

**Variant D is slow everywhere, and asymmetrically so.** `enheter_alle.json` is a
single pretty-printed array of 2.00 GB, which Spark can read only with
`multiLine=true`; that mode is not splittable, so one thread parses the whole
file regardless of `local[4]`. The financial side is NDJSON and does read in
parallel. Applying a narrow projection to a JSON read does not avoid the I/O
either: the parser tokenises every byte and then discards what it was not asked
for. Column pruning is close to worthless in row-oriented text, which is why the
gap widens on wide-schema workloads rather than narrowing.

### Correctness check

W1 totals 431,452 AS entities against 431,581 in the register, and the
difference of 129 is exactly the number of AS entities among the organisation
numbers that return HTTP 500 and were therefore never written to
`financial_data`.

Both figures come from `data/diagnose_variant_a.json` (2026-08-31), where
`financial_data` held 1,170,290 documents and the shortfall was 1,083 — not from
the benchmark run in the table above, which sat two documents later at 1,170,292.
Neither of those two is an AS with a filing, because AS `success` is 403,782 in
both that diagnostic and the 2026-09-05 analytics build, so the benchmark run's
own W1 total is between 431,452 and 431,454. It was not recorded and cannot be
recovered from the committed artefacts.

Group keys and integer counts are compared exactly; sums of floating-point
columns with a relative tolerance of 1e-9, because `mongod` and Spark's shuffle
accumulate in different orders. Two genuine semantic differences are reconciled
explicitly rather than absorbed by that tolerance: a `$group` `_id` sub-field
whose source path is missing is omitted by MongoDB but null in Spark, and `$sum`
over an all-missing group is `0` in MongoDB but `null` in Spark.

### Caveats

- **The timings are not independent measurements.** They run sequentially against
  one MongoDB instance and share its cache. For cold-cache numbers, run each
  variant in its own kernel through `SELECTED_VARIANTS`, with
  `docker compose restart mongodb` between them.
- **The MongoDB variants use indexes Spark cannot.** The primary index on
  `financial_data._id` and the secondary index on
  `companies.organisasjonsform.kode` are the database's native optimisations.
  Letting each engine use its own strengths is the intended methodology, not an
  oversight.
- **Variant D's NDJSON side is a reconstruction**, exported from MongoDB rather
  than captured during ingestion, and its write is repartitioned to one split per
  core. That is a deliberate advantage handed to the file variant; the raw
  `companies` file gets no such help because it arrives as one unsplittable array.
- **Single-node.** `local[4]` measures intra-node parallelism, not distribution
  across nodes, and the results should not be extrapolated to a cluster.

---

## Build the analytics table

Run `Build_analytics.ipynb` after the Parquet export. It flattens the single
annual statement out of `data[0]`, joins the register attributes, derives the
operating margin, and writes
`data/parquet/analytics_company_financials.parquet` — 1,171,373 rows, 61
columns, 91 MB, one row per registered entity. This is the file PowerBI
connects to. Figures below are from `data/analytics_build_summary.json`, which
the notebook writes as it measures.

**Every entity is kept, not only filers.** The join is a left join, so the
726,728 entities with no statement stay in the table with null financial columns
and `operating_margin_status = 'no_filing'`. An inner join would have been
simpler but would have deleted exactly the rows that make the coverage gap
visible, and the near-total absence of filings outside AS is itself a result:

| Legal form | Entities | With accounts | Coverage |
|---|---|---|---|
| BRL (housing co-ops) | 10,244 | 9,995 | 97.6% |
| AS | 431,581 | 403,782 | 93.6% |
| ESEK | 33,212 | 10,324 | 31.1% |
| FLI (associations) | 128,677 | 2,810 | 2.2% |
| ENK (sole proprietorships) | 461,154 | 3,266 | 0.7% |
| UTLA | 28,118 | 0 | 0.0% |

**The margin carries a reason when it is undefined.** `operating_margin_pct` is
`(driftsresultat / sumDriftsinntekter) × 100`, stored unrounded and unbounded,
and null whenever the ratio would be undefined or misleading —
`operating_margin_status` records why. Of 444,645 filings: 293,155 `computed`,
91,026 `revenue_missing`, 59,206 `revenue_zero`, 875 `revenue_negative`, 383
`income_missing`. A third of all filed statements therefore report no operating
revenue at all, which the diagnostics notebook investigates rather than leaving
as a footnote.

**Ratios must be recomputed from components, never averaged.** The per-company
median margin is 6.53% while the pooled margin — `SUM(operating_income) /
SUM(revenue)` — is 5.51%, and the distribution runs from -192% at the 5th
percentile to +79% at the 95th. Averaging the stored ratio across a group gives
neither figure. The numerator and denominator are kept as columns so PowerBI can
divide sums; equity and current ratios are deliberately *not* stored for the
same reason.

**Currency is flagged, not converted.** 443,461 of 444,645 filings are in NOK;
the rest span twelve currencies. Summing them would add unlike units, so
`currency_comparable` gates money aggregates while leaving every row in the
table. The effect is small but measured rather than assumed: the pooled margin
is 5.51% over all rows against 5.31% over NOK rows only, a 0.20 percentage-point
difference over 1.94% of total revenue.

### Verification

The table is re-read from disk rather than trusted in memory. Row count matches,
no row contradicts its own status, and the stored margin agrees with a
recomputation from its two components to a worst relative error of 0.0.

The balance sheet identity is the sharper check: total assets must equal equity
plus liabilities in every filing, which is an accounting constraint rather than
an assumption about this dataset. 15,462 of 444,645 filings (3.48%) violate it
at a 0.5 NOK threshold — but the deltas are whole numbers with a median of 1
NOK, because the Regnskapsregisteret reports whole kroner and two independently
rounded subtotals can differ by one as a matter of arithmetic. At a relative
threshold of 0.1% only 1,761 filings (0.40%) remain, and the rate rises with the
accounting regime, from 0.39% under the ordinary Norwegian rules to 1.80% under
IFRS, whose balance sheet does not map onto the template the API populates.
`Diagnose_balance_and_layout.ipynb` decomposes the remainder, including 369
filings that report zero assets against a multi-billion funding side.

Date parsing is checked before the build rather than after: 0 unparseable values
across all three cast date columns. Spark 4 runs with ANSI mode on, where a
malformed string cast to DATE aborts the job, so the build uses `try_cast` and
this cell counts what that would null.

### Scalability

The same `build` function, run at increasing thread counts on a 12-core host with
the driver heap held constant at 8 GB, one discarded warm-up and three timed runs
each, ending in a write so shuffle and output cost are included.

| Setting | Median | Speedup | Efficiency |
|---|---|---|---|
| `local[1]` | 20.52 s | 1.00× | 1.00 |
| `local[2]` | 15.45 s | 1.33× | 0.66 |
| `local[4]` | 14.26 s | 1.44× | 0.36 |
| `local[8]` | 13.51 s | 1.52× | 0.19 |
| `local[12]` | 15.27 s | 1.34× | 0.11 |

Speedup saturates around 1.5× and then regresses. The serial fraction dominates:
the `coalesce(1)` write is single-threaded by construction, and at 12 threads the
executor threads contend with the driver on the same machine. This is the
practical form of Amdahl's law on a single node, and it is a more useful result
than a scaling curve that was never pushed far enough to bend.

---

## Technology notes

**MongoDB** — the database earns its place on the *enrichment* half of the
pipeline rather than on the bulk load, and it is worth separating the two.

`financial_data` is not a file that was downloaded. It is 1.17M individual API
responses accumulated over hours, and `Fetch_all_financial_data.ipynb` depends on
three properties a columnar file cannot provide. It recomputes its work list on
every run as the set difference between `companies` and `financial_data`, which
requires a keyed store that can be read back mid-pipeline; Parquet has no primary
key and no point lookup. It writes each response as it arrives, so an interrupted
run loses nothing already stored — the file-native equivalent is either 1.17M
tiny files, which is a pathological input for distributed processing, or a buffer
that is lost on interruption. And `_id = organisasjonsnummer` makes a re-run
structurally incapable of duplicating a record, which `mongoimport` against the
bulk file conspicuously is not.

The schema argument runs the same way. Parquet requires the schema before the
first write, but `schemas.py` was *derived* from a full profiling pass over the
already-landed corpus — that is how `totalresultat` (54% of filings, yet absent
from a ten-record sample), `naeringskode3` (1,576 records of 1,171,373) and the
three source-side misspellings were found at all. Landing the data in a
schema-on-read store first is what made the schema knowable. The nesting and
heterogeneity of the register — `vedtektsfestetFormaal` an array of strings,
`kapital` and `organisasjonsform` subdocuments, many fields absent on any given
record — is accepted directly for the same reason, though on its own that is the
weaker argument: Parquet handles nested structs and sparse columns perfectly
well.

For `companies` alone the database is not load-bearing. It is a static 2.0 GB
download that a single Spark job could convert to Parquet directly; its role here
is to host the join partner the enrichment probes against, and to supply variants
A and B of the benchmark. That the benchmark then shows `mongod` completing a
431k × 1.17M join in 7.38 s when the pipeline is written to exploit its indexes,
and winning W3 outright, is a result of the comparison rather than the reason the
collection is in a database.

**PySpark** — provided by the `quay.io/jupyter/pyspark-notebook` base image.
Used here to test whether distributed processing pays off at this scale. The
benchmark shows that it does not when reading from MongoDB, and does when
reading columnar files, which is a more useful result than assuming either.
`Build_analytics.ipynb` extends this with a thread-count experiment, running the
same build at `local[1]` through `local[12]` to measure how far the speedup
tracks the ideal line.

**Parquet** — columnar storage. Fastest variant on both join workloads, by 2.7×
on the selective join and 4.7× on the wide aggregation, from reading only the
required columns and avoiding per-document BSON deserialisation entirely. It does
not win everywhere: MongoDB takes the single-collection scan, which is the more
useful result than a blanket claim either way.

**Docker Compose** — makes the environment reproducible on any machine with
Docker installed, with no host-level Python, Java, or MongoDB installation.

### Python dependencies

The base image already provides PySpark, pandas, pyarrow, scikit-learn, scipy,
and matplotlib. The Dockerfile adds only:

| Package | Used for |
|---|---|
| `pymongo` | MongoDB access from notebooks and scripts |
| `requests` | Regnskapsregisteret REST API calls |
