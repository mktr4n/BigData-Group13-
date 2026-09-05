# Group 13 — CS4010 Big Data

An end-to-end pipeline over the Norwegian business register
(Brønnøysundregistrene), enriched with annual accounts from the
Regnskapsregisteret API, mirrored to Parquet and NDJSON, benchmarked across
query engines and storage formats, and reduced to a curated table for PowerBI.

Everything runs in Docker: MongoDB for storage, JupyterLab with PySpark for
processing. No host-level Python, Java or MongoDB installation is needed.

## Prerequisites

- Docker Desktop
- Git
- ~15 GB free disk (2.0 GB source file, MongoDB volume, Parquet and NDJSON mirrors)

## 1. Get the data

Clone the repository, then place the two dataset files in `data/`:

- `enheter_alle.json` — the bulk register export, a single JSON array of
  ~1.17M entities (~2.0 GB). If you downloaded it compressed, extract it here.
- `financial_data.archive.gz` — a `mongodump` archive of the fetched annual
  accounts. Leave it compressed; `mongorestore` reads it as-is.

The dataset is not in the repository. `data/` is bind-mounted into both
containers, so the files must be in that folder before the load steps.

## 2. Start the stack

```
docker compose up -d --build
```

| Service | Address |
|---|---|
| JupyterLab | http://localhost:8889/lab?token=group13 |
| Spark UI | http://localhost:4041 (only while a job runs) |
| MongoDB, from the host | `mongodb://localhost:27018` |
| MongoDB, from a container | `mongodb://mongodb:27017` |

Host ports are shifted from the defaults so the stack can run alongside another
MongoDB or Jupyter instance. The Jupyter token is fixed to `group13`, so no
token needs to be read out of the container logs.

## 3. Load both collections

Host `data/` is visible as `/import` inside the MongoDB container, so no file
copying is needed.

```
docker exec group13_mongodb mongoimport --db companiesdb --collection companies --file /import/enheter_alle.json --jsonArray
docker exec group13_mongodb mongorestore --gzip --archive=/import/financial_data.archive.gz --drop
```

`--jsonArray` is required because the register file is one JSON array rather
than newline-delimited JSON.

**Run `mongoimport` once, against an empty collection.** It appends rather than
replaces, so running it twice produces duplicate documents. `mongorestore
--drop` is safe to repeat.

Verify the load — databases, collections, document counts and the top-level
fields of a sample document:

```
docker exec group13_jupyter python /home/jovyan/work/discover_mongo.py
```

Expect `companiesdb` with `companies` (1,171,373 documents in the 2026-08-25
snapshot, 431,581 of them AS) and `financial_data` (~1.17M).

## 4. Run the notebooks

Open http://localhost:8889/lab?token=group13. The notebooks form a chain — each
one consumes what the previous one wrote.

| Order | Notebook | Purpose |
|---|---|---|
| 1 | `Fetch_all_financial_data.ipynb` | Fetches annual accounts from the Regnskapsregisteret API into `financial_data`. Only needed to extend or refresh the data; the provided archive already contains the result. |
| 2 | `Analyse_data.ipynb` | Coverage, fiscal years, insolvency and liquidation flags, and the full profiling pass the schemas are derived from. Its first cell creates the `organisasjonsnummer` index — run that first. |
| 3 | `Export_to_parquet.ipynb` | Writes both collections to `data/parquet/`. |
| 3 | `Export_to_ndjson.ipynb` | Writes `financial_data` to `data/ndjson/`. |
| 4 | `Benchmark_engines.ipynb` | Five variants × three workloads, timed and cross-checked. Writes `data/benchmark_results.json`. |
| 5 | `Build_analytics.ipynb` | Builds the curated PowerBI table and runs the thread-count scalability experiment. Writes `data/analytics_build_summary.json`. |

`Diagnose_variant_a.ipynb` and `Diagnose_balance_and_layout.ipynb` are
investigations into specific results and are not part of the main chain.

`notebooks/schemas.py` holds the Spark schemas both exports and the benchmark
import, so every variant provably reads the same columns.

If the project folder is synced by Dropbox or a similar client, no action is
needed: Spark writes to container-local disk and the finished files are copied
into `data/`. Committing directly into a synced folder fails intermittently,
because the committer renames files a sync client may be holding open. See
`notebooks/staged_write.py`.

## 5. Outputs

| File | Written by |
|---|---|
| `data/parquet/analytics_company_financials.parquet` | `Build_analytics.ipynb` — one row per registered entity; this is what PowerBI connects to |
| `data/benchmark_results.json` | `Benchmark_engines.ipynb` — configuration, timings, agreement checks |
| `data/analytics_build_summary.json` | `Build_analytics.ipynb` — input profile, verification, scalability |
| `data/profile_*.json` | `Analyse_data.ipynb` — field-level profile of both sources |
| `data/diagnos*.json`, `data/w4_industry_municipality.json` | the diagnostic notebooks and workload W4 |

In PowerBI: Get Data → Parquet → `data/parquet/analytics_company_financials.parquet`.
Ratios must be computed from summed components
(`DIVIDE(SUM(operating_income), SUM(revenue))`), not by averaging the stored
per-company ratio — the two differ substantially given the skew.

## Configuration

Spark settings live in `jupyter/spark-defaults.conf` and are applied when the
JVM launches, so every notebook session is configured identically and no
notebook contains JVM configuration of its own. Changing a setting means
editing that file and rebuilding:

```
docker compose up -d --build jupyter
```

| Setting | Value |
|---|---|
| `spark.driver.memory` | `8g` |
| `spark.master` | `local[4]` |
| `spark.jars.packages` | `mongo-spark-connector_2.13:11.1.0` |
| `spark.sql.session.timeZone` | `UTC` |

The image adds only `pymongo` and `requests` to
`quay.io/jupyter/pyspark-notebook`, which already provides PySpark, pandas,
pyarrow, scikit-learn, scipy and matplotlib.

## Further documentation

- `Walkthrough of files.md` — full walkthrough: dataset, load, fetch, analysis,
  export, benchmark, and the technology rationale.
- `notebooks/README_benchmark_section.md` — benchmark methodology: why the
  schemas are hardcoded, the variant and workload definitions, correctness
  rules, and known limitations.
