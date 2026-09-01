# Storage format comparison

## Why schemas are hardcoded

`notebooks/schemas.py` defines the Spark schemas for both collections as literal
`StructType` declarations rather than letting Spark infer them.

Inference was rejected for three reasons.

**It would corrupt the measurement.** Schema inference on JSON requires a full
scan of the source before any query runs. That cost lands on the JSON variant
and on nothing else, so the format comparison would measure inference overhead
rather than read performance. Parquet carries its schema in the file footer and
needs no such pass, and the MongoDB connector samples documents. Pinning the
schema removes the difference and guarantees all four variants read the same
columns.

**Sampled inference is demonstrably unsafe on this data.** The schemas were
derived from a full profiling pass over all 1,171,373 raw records and all
1,170,290 financial documents (`Analyse_data.ipynb`), not from a sample. The
profile found `totalresultat` in 241,560 filings — 54% of those with accounts —
yet it appears in none of the ten records in `financial_test_10.json`. In the
company register, `naeringskode3` occurs in 1,576 records out of 1,171,373, and
`tvangsopplostPgaManglendeDagligLederDato` in exactly one. A sampling-based
inference would omit these columns silently.

**It makes the schema reviewable.** A hardcoded schema is a versioned artefact a
reader can check against the profile output. An inferred one is a side effect of
whichever records the sampler happened to see.

The cost is maintenance: a change upstream at Brønnøysundregistrene requires
re-running the profiler and editing the file. The profiler exists for that
reason, and the export notebooks assert expected row counts and non-null
frequencies so a drift fails loudly rather than producing quietly wrong results.

## Source-side spelling

Three field names from the Regnskapsregisteret API are misspelled at source and
are reproduced verbatim in the schema:

| In the API | Apparently intended |
|---|---|
| `regnkapsprinsipper` | `regnskapsprinsipper` |
| `sumInnskuttEgenkaptial` | `sumInnskuttEgenkapital` |
| `omloepsmidler` | `omløpsmidler` |

The third is a deliberate ASCII transliteration rather than an error, but the
first two are typographical. Correcting any of them in the schema resolves the
column to null across every record. This was found by profiling, not by reading
the API documentation, and is a concrete argument for deriving schemas from the
data rather than from a specification.

## Excluded fields

`links`, `organisasjonsform.links` and `foretaksformIHjemlandet.links` are
omitted. Each is an empty array in 1,171,373 of 1,171,373 records. Every other
field is retained, including `historiskeNavn` (populated in 219,869 records,
18.8%) and `paategninger` (2,702 records, 0.23%).

An earlier iteration of this project exported a fourteen-column subset. That
made Parquet's column-pruning advantage a property of the export rather than of
the query, which the comparison could not then attribute correctly. Exporting at
full width moves pruning inside the query, where it is the thing being measured.

## Variants

| Variant | Execution | Companies | Financial statements |
|---|---|---|---|
| A | `mongod`, server-side | MongoDB collection | MongoDB collection |
| B | Spark, `local[4]` | MongoDB via connector | MongoDB via connector |
| C | Spark, `local[4]` | Parquet | Parquet |
| D | Spark, `local[4]` | raw `enheter_alle.json` | NDJSON export |

Variant D is deliberately asymmetric. `enheter_alle.json` is a single
pretty-printed JSON array of 2.00 GB (2,001,085,762 bytes), which Spark can read only with
`multiLine=true`; that mode is not splittable, so one thread parses the whole
file irrespective of `local[4]`. The financial side is NDJSON and reads in
parallel across partitions. This is not an artefact of the setup — it is the
real difference between a bulk download and an incremental fetch, and the
benchmark records per-side read timings so the two effects can be separated
rather than reported as one blended number.

An important mechanism behind the results: applying a narrow projection to a
JSON read does not avoid I/O. The parser still tokenises every byte of every
record and then discards the fields it was not asked for. Parquet skips the
bytes entirely. Column pruning is therefore close to worthless in row-oriented
text, which is why the gap widens on wide-schema workloads rather than
narrowing.

### Provenance of the NDJSON file

`data/ndjson/financial_data` is exported from MongoDB, not captured during
ingestion. A genuinely file-native pipeline would have written each
Regnskapsregisteret response to disk as it arrived and never involved MongoDB at
all. That pipeline would also have produced roughly 1.17M small files, which is
a pathological input for distributed processing. Re-fetching to demonstrate this
is impractical at 1 request per second. The export is therefore equivalent in
content to what a file-native ingestion would have produced, but not equivalent
in provenance, and the report states this rather than implying the JSON variant
is independent of the database.

## Workloads

**W1 — selective join, two output rows.** Join financial data to companies on
`organisasjonsnummer`, restrict to AS, count by `fetch_status`. Retained from
the earlier narrow-schema benchmark so the two can be compared.

**W3 — unindexed predicate, no join.** Count `konkurs = true` grouped by legal
form. No index exists on `konkurs`, so variant A performs a full collection scan
here, where W1 was served through the `organisasjonsnummer` index. W3 tests
whether A's advantage in W1 generalises or was a property of a well-indexed
access path. The absence of a join isolates companies-side read cost.

**W4 — wide read plus aggregation.** Operating revenue, operating profit, equity
and debt for AS companies with a filed statement, grouped by industry code and
municipality. Reads deeply nested columns from both sources and produces tens of
thousands of groups, moving the bottleneck from scan to shuffle. Its output is
retained as `data/w4_industry_municipality.json` and feeds the analysis, so the
workload is not purely synthetic.

## Correctness

Timings from variants that disagree are worthless, so every workload is checked
across all variants that completed, against the first of them as reference.
An earlier version used variant A unconditionally as the reference, which meant
that when A failed on W4 the other three were reported as disagreeing when in
fact they had never been compared to anything. Group keys and integer counts
are compared exactly. Sums of floating-point columns are compared with a
relative tolerance of 1e-9, because `mongod` and Spark's shuffle accumulate in
different orders and bit-identical results are neither achievable nor the
property of interest.

Two engine semantics differ and are reconciled explicitly rather than absorbed
by the tolerance. A `$group` `_id` sub-field whose source path is missing is
omitted from the MongoDB result document rather than stored as null, so those
keys are read with `.get()`; `naeringskode1` is absent in 35,727 companies and
`forretningsadresse.kommunenummer` in 62,388. And `$sum` over a group where
every value is missing returns `0` in MongoDB but `null` in Spark, so the Spark
aggregate coalesces to `0.0`; `sumDriftsinntekter` is absent in 91,026 filings,
so all-missing groups do occur. Neither is a tolerance question — they are
genuine semantic differences that would otherwise be misreported as
disagreements.

The float tolerance itself is precautionary. Simulated reassociation of 300,000 mixed-magnitude
NOK values did not produce any measurable divergence in IEEE 754 double
arithmetic, so in practice the comparison may well be exact; the tolerance
exists so that a legitimate ordering difference cannot be misreported as a
correctness failure.

## Known limitations

- **No time series.** `data` holds exactly one filing per company in all 444,644
  populated records, confirmed over the full collection. The Regnskapsregisteret
  `?år=` parameter is ignored by the API, which always returns the most recent
  filing. The only variation in `regnskapsperiode` is between companies with
  different fiscal years, not within a company over time. Trend analysis is
  therefore out of scope for this dataset as fetched.
- **1,083 organisation numbers (0.09%)** return HTTP 500 deterministically and
  are absent from `financial_data`. Reconciles exactly:
  1,171,373 − 1,170,290 = 1,083.
- **Variant A's W1 timing is not stable between runs.** The same pipeline
  measured 7.13s in an earlier run and 45.97s in the current one. The cause is
  under investigation in `Diagnose_variant_a.ipynb`; candidates are page-cache
  contention from the Spark reads, the addition of `allowDiskUse=True`, and
  collection growth. A cold MongoDB cache is ruled out, since W3 scans all
  1,171,373 company documents in 0.44s in the same session. The read-only
  timing cell has been moved to the end of the benchmark to remove contention
  as a variable, but this is not yet a demonstrated cause and the report should
  state the conditions of each measurement rather than presenting either figure
  alone.
- **Single-node.** All variants run on one machine. `local[4]` measures
  intra-node parallelism, not distribution across nodes, and results should not
  be extrapolated to a cluster without stating that.
