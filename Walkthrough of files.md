# CS4010 Big Data — Group 13

Pipeline for loading the Norwegian business register (Brønnøysundregistrene)
into MongoDB and enriching it with financial statement data from the
Regnskapsregisteret API.

## Folder structure
```
group_13/
├── data/
│   └── enheter_alle.json        <- the dataset (2.0 GB, not included in submission)
├── jupyter/
│   ├── Dockerfile
│   └── spark-defaults.conf
├── notebooks/
│   ├── discover_mongo.py
│   ├── Fetch_all_financial_data.ipynb
│   ├── Analyse_data.ipynb
│   ├── Export_to_parquet.ipynb
│   └── Benchmark_engines.ipynb
├── docker-compose.yml
└── README.md
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

**Run this once, against an empty collection.** `mongoimport` appends rather
than replaces — running it twice produces duplicate documents.

```
docker exec group13_mongodb mongoimport --db companiesdb --collection companies --file /import/enheter_alle.json --jsonArray
```

`/import` is the container-side mount of the host `data/` folder, so no file
copying is needed.

The `--jsonArray` flag is required because the file is a single JSON array
rather than newline-delimited JSON.

Indexes are not created here. Each notebook creates the index it depends on, in
code, so the measured configuration is reproducible:

| Index | Created by |
|---|---|
| `companies.organisasjonsnummer` | `Analyse_data.ipynb`, first cell |
| `companies.organisasjonsform.kode` | `Benchmark_engines.ipynb`, variant A cell |

Both calls are idempotent.

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

A fixed set of approximately 1,083 organisation numbers (0.09% of the register)
returns **HTTP 500** from the Regnskapsregisteret API deterministically. This
was reproduced across separate runs on different days, and at a deliberately
throttled rate of 1 request per second, which rules out client-side rate
limiting — the responses carry no `Retry-After` header. The failures are spread
across 16 different legal forms and are not explained by entity type.

Because the fetch script classifies any non-200/404 response as transient,
these records are never written and are retried on every subsequent run. The
practical consequence is that `financial_data` converges to approximately
1,170,290 of 1,171,373 records (99.9%) and every further run reports the same
~1,083 skips.

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

## Export to Parquet

Run `Export_to_parquet.ipynb`. It writes both collections to Parquet under
`data/parquet/`, for use as a third data source in the engine benchmark.

**Pause Dropbox before running.** The output folder is inside a synced
directory, and Dropbox locking files mid-write breaks Spark writes.

The export reads from MongoDB, not from `enheter_alle.json`. `financial_data`
exists only in MongoDB — it was fetched from the API — so half the dataset has
no file equivalent, and reading both from one source guarantees the benchmark
variants see identical content.

### Projected schemas

Both collections are written with explicit schemas rather than in full.
`companies` keeps the fields the analysis uses; `financial_data` omits the
nested `data` statement blob, which no analysis reads.

This was necessary as well as convenient: reading whole documents exhausted the
Spark driver heap during BSON decoding. An explicit schema skips the
connector's inference pass and limits each decoded document to the listed
fields.

It is also a limitation of the benchmark. The Parquet copies are narrower than
the MongoDB collections, so part of Parquet's column-pruning advantage is
realised at export time rather than at query time. The benchmark applies the
same schemas when reading from MongoDB, so the variants compare like with like
on the same columns.

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

Run `Benchmark_engines.ipynb` after the Parquet export. It times the same
question three ways: join `companies` to `financial_data` on the organisation
number, keep AS entities, count by `fetch_status`.

| Variant | Where the join runs | Source |
|---|---|---|
| A | `mongod`, server-side | MongoDB collections |
| B | Spark JVM, `local[4]` | MongoDB via connector |
| C | Spark JVM, `local[4]` | Parquet files |

Variant A is not a Python join. pymongo sends the pipeline to `mongod`, which
executes it; Python only deserialises the small result. The comparison is
between a database engine and a distributed framework, not between languages.

Each variant runs once untimed to warm caches and let the JVM JIT-compile, then
three timed runs. The median is reported rather than the mean, so a single GC
pause or scheduling hiccup does not dominate. All three results are compared
for equality before any timing is reported.

### Results

Measured on 12 cores, 8 GB Spark driver heap, `local[4]`, Spark 4.2.0,
connector 11.1.0.

| Variant | Median | vs fastest |
|---|---|---|
| C: Spark + Parquet | 2.13 s | 1.0× |
| A: MongoDB `$lookup` (AS-first) | 6.81 s | 3.2× |
| B: Spark + connector | 8.87 s | 4.2× |

An earlier formulation of variant A ran at **49.35 s** — 7.2× slower than the
version above. It differed in two ways: it was driven from `financial_data`
(1,170,290 documents) rather than from the AS subset of `companies` (431,581),
and it joined against a secondary field rather than against `financial_data._id`,
which is the primary index. `$lookup` is a nested-loop join, so both changes
matter.

That difference is the main finding. Measured against the naive pipeline, Spark
appeared three times faster than MongoDB. Measured against a correctly written
one, MongoDB is faster than Spark reading from MongoDB, and what remains is a
storage-format result rather than an engine result: Parquet wins because it
reads two columns from a columnar file with no BSON decoding.

Variant B is slowest because it pays MongoDB's read cost and Spark's shuffle
cost without benefiting from either engine's strengths.

### Correctness check

The result totals 431,452 AS entities against 431,581 in the register. The
difference of 129 is exactly the number of AS entities among the ~1,083
organisation numbers that return HTTP 500 and were therefore never written to
`financial_data`.

### Caveats

- **The three timings are not independent measurements.** They run sequentially
  against one MongoDB instance and share its cache. Between two runs of the
  benchmark, variants B and C changed timing (B from 16.00 s to 8.87 s) although
  their code did not — most plausibly because the lighter variant A left more of
  the WiredTiger cache intact for the variants that follow. This was not
  verified. For cold-cache numbers, run each variant in its own kernel with
  `docker compose restart mongodb` between them.
- **Variant A uses indexes that Spark cannot.** The primary index on
  `financial_data._id` and the secondary index on
  `companies.organisasjonsform.kode` are the database's native optimisations.
  Letting each engine use its own strengths is the intended methodology, not an
  oversight.
- The Parquet dataset is projected, as described in the export section above.

---

## Technology notes

**MongoDB** — the register data is deeply nested and heterogeneous. Fields such
as `vedtektsfestetFormaal` are arrays of strings, `kapital` and
`organisasjonsform` are subdocuments, and many fields are absent on any given
record. A document store accepts this shape directly, with no schema design or
flattening step before the data can be queried. The benchmark also shows it
completing a 431k × 1.17M join in under seven seconds when the pipeline is
written to exploit its indexes.

**PySpark** — provided by the `quay.io/jupyter/pyspark-notebook` base image.
Used here to test whether distributed processing pays off at this scale. The
benchmark shows that it does not when reading from MongoDB, and does when
reading columnar files, which is a more useful result than assuming either.

**Parquet** — columnar storage. Fastest of the three variants by a factor of
three, from reading only the required columns and avoiding per-document BSON
deserialisation entirely.

**Docker Compose** — makes the environment reproducible on any machine with
Docker installed, with no host-level Python, Java, or MongoDB installation.

### Python dependencies

The base image already provides PySpark, pandas, pyarrow, scikit-learn, scipy,
and matplotlib. The Dockerfile adds only:

| Package | Used for |
|---|---|
| `pymongo` | MongoDB access from notebooks and scripts |
| `requests` | Regnskapsregisteret REST API calls |
