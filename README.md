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

| Service                   | Address                                       |
| ------------------------- | --------------------------------------------- |
| JupyterLab                | http://localhost:8889/lab?token=group13       |
| Spark UI                  | http://localhost:4041 (only while a job runs) |
| MongoDB, from the host    | `mongodb://localhost:27018`                   |
| MongoDB, from a container | `mongodb://mongodb:27017`                     |

Host ports are shifted from the defaults so the stack can run alongside another
MongoDB or Jupyter instance. The Jupyter token is fixed to `group13`, so no
token needs to be read out of the container logs.

## 3. Load both collections

Host `data/` is visible as `/import` inside the MongoDB container, so no file
copying is needed.

Create the collection and its unique index first. The index is what makes the
import an upsert rather than an append:

```
docker exec group13_jupyter python -c "import pymongo; pymongo.MongoClient('mongodb://mongodb:27017/')['companiesdb']['companies'].create_index('organisasjonsnummer', unique=True); print('unique index ready')"
```

Then import. This command is safe to re-run: `--mode merge` with
`--upsertFields organisasjonsnummer` updates the top-level fields of companies
already present, inserts companies new to the file, and leaves companies that
have dropped out of the export in place. The redirect keeps an audit trail of
each load:

```
docker exec group13_mongodb mongoimport --db companiesdb --collection companies --file /import/enheter_alle.json --jsonArray --mode merge --upsertFields organisasjonsnummer > data\ingest_2026-08-25.log 2>&1
docker exec group13_mongodb mongorestore --gzip --archive=/import/financial_data.archive.gz --drop
```

`--jsonArray` is required because the register file is a single JSON array
rather than newline-delimited JSON. `mongorestore --drop` replaces
`financial_data` outright and is likewise safe to repeat.

Verify the load — databases, collections, document counts and the top-level
fields of a sample document:

```
docker exec group13_jupyter python /home/jovyan/work/discover_mongo.py
```

Expect `companiesdb` with `companies` (1,171,373 documents in the 2026-08-25
snapshot, 431,581 of them AS) and `financial_data` (~1.17M).

## 4. Run the notebooks

### Unattended, in one command

```
run_pipeline.cmd
```

Runs the chain in dependency order — analysis, both exports, the benchmark, the
curated build — saving each notebook with fresh outputs and stopping at the
first failure, so a later stage cannot read a half-written mirror. A transcript
is appended to `data\pipeline_run.log`.

```
run_pipeline.cmd --list                 :: the stages, and which are on by default
run_pipeline.cmd --dry-run              :: print the plan without running it
run_pipeline.cmd --include-fetch        :: prepend the hours-long API fetch
run_pipeline.cmd --include-diagnostics  :: append the two investigation notebooks
run_pipeline.cmd --only parquet ndjson  :: just these stages
run_pipeline.cmd --from benchmark       :: resume after fixing a failure
```

Two stages are worth knowing about before running the whole chain. `benchmark`
re-times every variant, so its results supersede any timing previously quoted;
`analytics` rebuilds the curated table, so if the fetch has run since the last
build, every downstream figure moves with it. `--only` and `--from` exist so
neither has to be run by accident.

### Or interactively

Open http://localhost:8889/lab?token=group13 and run the core path in this order:

1. `Analyse_data.ipynb` — creates the company index and profiles the sources.
2. `Export_to_parquet.ipynb` — writes the MongoDB collections to Parquet.
3. `Import_ssb_population.ipynb` — downloads municipality population from SSB table 06913.
4. `Build_analytics.ipynb` — joins SSB by `kommunenummer` and builds the curated PowerBI table.
5. `Analyse_geography.ipynb` — compares company type, filing coverage, distress, financial performance, and industry mix across population bands.

The provided financial archive already contains the API results. Run
`Fetch_all_financial_data.ipynb` only when extending or refreshing that data;
run `Export_to_ndjson.ipynb` and `Benchmark_engines.ipynb` separately when you
want to reproduce the storage-format benchmark.

`Analyse_data.ipynb` must be run before changing `schemas.py`, because the
explicit schemas are derived from its full profiling pass. After a schema or
source-data change, rerun the affected export before building analytics.

`Diagnose_variant_a.ipynb` and `Diagnose_balance_and_layout.ipynb` are
investigations into specific results and are not part of the main chain.

Four modules are shared by the notebooks rather than copied into them:

| Module                      | Holds                                                                                                            |
| --------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| `notebooks/schemas.py`      | the Spark schemas both exports and the benchmark import, so every variant provably reads the same columns        |
| `notebooks/bootstrap.py`    | container paths, the Spark session, and the JVM readout, so every notebook documents the environment identically |
| `notebooks/mirrors.py`      | the source-signature bookkeeping that decides whether a mirror needs re-exporting                                |
| `notebooks/staged_write.py` | the staged write that keeps Spark from committing onto the synced mount                                          |

No notebook configures the JVM. Driver memory, thread count, the connector
package and the connection URIs are applied at JVM launch from
`jupyter/spark-defaults.conf`; `bootstrap.py` only reads them back.

If the project folder is synced by Dropbox or a similar client, no action is
needed: Spark writes to container-local disk and the finished files are copied
into `data/`. Committing directly into a synced folder fails intermittently,
because the committer renames files a sync client may be holding open. See
`notebooks/staged_write.py`.

## 5. Outputs

| File                                                       | Written by                                                                                |
| ---------------------------------------------------------- | ----------------------------------------------------------------------------------------- |
| `data/parquet/ssb_population_2026.parquet`                 | `Import_ssb_population.ipynb` — municipality population dimension from SSB table 06913    |
| `data/parquet/analytics_company_financials.parquet`        | `Build_analytics.ipynb` — one row per registered entity; this is what PowerBI connects to |
| `data/geography_analysis.json`                             | `Analyse_geography.ipynb` — persisted population-band findings                            |
| `data/benchmark_results.json`                              | `Benchmark_engines.ipynb` — configuration, timings, agreement checks                      |
| `data/analytics_build_summary.json`                        | `Build_analytics.ipynb` — input profile, verification, scalability                        |
| `data/profile_*.json`                                      | `Analyse_data.ipynb` — field-level profile of both sources                                |
| `data/diagnos*.json`, `data/w4_industry_municipality.json` | the diagnostic notebooks and workload W4                                                  |

Only the JSON artefacts in that table are committed. Both Parquet outputs are
derived and are not in the repository, so a fresh clone builds them first:
`run_pipeline.cmd --only ssb analytics`.

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

| Setting                      | Value                               |
| ---------------------------- | ----------------------------------- |
| `spark.driver.memory`        | `6g`                                |
| `spark.master`               | `local[4]`                          |
| `spark.jars.packages`        | `mongo-spark-connector_2.13:11.1.0` |
| `spark.sql.session.timeZone` | `UTC`                               |

The image adds only `pymongo` and `requests` to
`quay.io/jupyter/pyspark-notebook`, which already provides PySpark, pandas,
pyarrow, scikit-learn, scipy and matplotlib.

## Further documentation

- `Walkthrough of files.md` — full walkthrough: dataset, load, fetch, analysis,
  export, benchmark, and the technology rationale.
- `notebooks/README_benchmark_section.md` — benchmark methodology: why the
  schemas are hardcoded, the variant and workload definitions, correctness
  rules, and known limitations.
