"""
Shared environment bootstrap for the group 13 notebooks.

Why this exists
---------------
Five notebooks opened with the same twenty lines: the same container paths, the
same `SparkSession.builder...getOrCreate()`, the same readback of the connector
coordinate and the Mongo URI off `SparkConf`, and the same block of print
statements. Copied that many times the block had already begun to drift - one
notebook carried an `ANALYTICS_FILE` pointing at `data/` rather than
`data/parquet/`, two carried constants they never used, and the printed
description differed between them, so two runs of the same stack did not
document themselves the same way.

None of that is JVM configuration. Driver memory, thread count, the connector
package and the connection URIs still live in `jupyter/spark-defaults.conf` and
are applied when the JVM launches; this module only names the paths and reads
the settings back, so a notebook still cannot drift from the environment a
grader gets.

The one deliberate caller-side exception is the scalability experiment in
`Build_analytics.ipynb`, which stops the session and rebuilds it per thread
count. It calls `start_spark` with its own master override rather than going
around this module.
"""

import os

# pyspark is imported inside the functions that need it, not at module level.
# Two notebooks here never open a Spark session - Analyse_data works entirely
# through pymongo, and Diagnose_variant_a times MongoDB on its own - and
# neither should fail to import its paths because Spark is unavailable.

# --------------------------------------------------------------------------
# Container paths. Host ./data is bind-mounted at /home/jovyan/data and host
# ./notebooks at /home/jovyan/work, so notebook code always uses these.
# --------------------------------------------------------------------------
MONGO_DB = "companiesdb"

DATA_DIR = "/home/jovyan/data"
PARQUET_DIR = os.path.join(DATA_DIR, "parquet")
NDJSON_DIR = os.path.join(DATA_DIR, "ndjson")
RAW_COMPANIES = os.path.join(DATA_DIR, "enheter_alle.json")

# Single file rather than a part-file directory, so PowerBI's Parquet connector
# takes a plain path and needs no Folder/Combine step.
ANALYTICS_FILE = os.path.join(PARQUET_DIR, "analytics_company_financials.parquet")

# Regnskapsregisteret, one statement per organisation number. Named here because
# the fetch notebook calls it and two analysis cells print it back as the URL a
# reader can check a company against; three copies of a URL drift.
BASE_URL = "https://data.brreg.no/regnskapsregisteret/regnskap/"


def start_spark(app_name, master=None, describe=True):
    """
    Open (or attach to) the Spark session and describe the JVM it actually got.

    `master` is only for the scalability experiment, which needs a new session
    per thread count. Leave it None everywhere else so the value from
    spark-defaults.conf applies and every notebook is configured identically.
    """
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(app_name)
    if master is not None:
        builder = builder.master(master)
    spark = builder.getOrCreate()
    if describe:
        describe_session(spark)
    return spark


def session_settings(spark):
    """
    The JVM settings a timing is only reproducible against, read back from the
    running session rather than restated from the config file.
    """
    conf = spark.sparkContext.getConf()
    return {
        "spark_version": spark.version,
        "spark_master": spark.sparkContext.master,
        "driver_max_heap_gb": (
            spark._jvm.java.lang.Runtime.getRuntime().maxMemory() / 1024 ** 3),
        "default_parallelism": spark.sparkContext.defaultParallelism,
        "cpu_cores": os.cpu_count(),
        "connector": conf.get("spark.jars.packages"),
        "mongo_uri": conf.get("spark.mongodb.read.connection.uri"),
    }


def describe_session(spark, **extra):
    """Print the session settings, plus anything the caller wants recorded."""
    settings = session_settings(spark)
    settings.update(extra)
    for key, value in settings.items():
        if key == "driver_max_heap_gb":
            print("%-20s %.1f GB" % (key, value))
        else:
            print("%-20s %s" % (key, value))
    return settings


def mongo_client(spark=None, uri=None):
    """
    A pymongo handle on the same MongoDB the Spark session is pointed at.

    Taking the URI off SparkConf rather than hardcoding it means the notebooks
    cannot end up reading one MongoDB through Spark and a different one through
    pymongo. `uri` is the fallback for the notebooks that never open a session.
    """
    from pymongo import MongoClient

    if uri is None:
        uri = (spark.sparkContext.getConf().get("spark.mongodb.read.connection.uri")
               if spark is not None else "mongodb://mongodb:27017")
    return MongoClient(uri)


def mongo_db(spark=None, uri=None, database=MONGO_DB):
    """The `companiesdb` database handle. See `mongo_client` for the URI rule."""
    return mongo_client(spark=spark, uri=uri)[database]
