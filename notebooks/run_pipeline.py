"""
Run the notebook pipeline end to end, unattended.

Why this exists
---------------
The notebooks are a chain: each one consumes what the previous one wrote, and
running them by hand means opening five tabs in the right order and watching for
the one that fails. This runs them in dependency order in a single command, saves
each notebook with its fresh outputs, and stops at the first failure so a later
stage cannot read a half-written mirror.

Usage (from the host, Windows CMD)
----------------------------------
    run_pipeline.cmd                     :: the standard chain
    run_pipeline.cmd --list              :: show the stages and exit
    run_pipeline.cmd --include-fetch     :: add the hours-long API fetch first
    run_pipeline.cmd --only parquet ndjson   :: run named stages only
    run_pipeline.cmd --from benchmark    :: resume from a stage onwards
    run_pipeline.cmd --dry-run           :: print what would run

Or directly inside the container:

    docker exec -w /home/jovyan/work group13_jupyter python run_pipeline.py

What it does not do
-------------------
Nothing here decides *whether* work is needed. The export notebooks skip
themselves when the source signature is unchanged, and the fetch notebook
recomputes its own work list. This only sequences them.

Two stages are expensive and change published figures, so read before running
the whole chain:

  * `benchmark` re-times every variant. New timings supersede whatever the
    report quotes, so the report's benchmark table has to be updated after it.
  * `analytics` rebuilds `analytics_company_financials.parquet` from the current
    mirror. If the fetch notebook has run since the last build, every figure in
    the analysis chapter and every PowerBI visual moves with it.

`--only` and `--from` exist so neither has to be run by accident.
"""

import argparse
import os
import sys
import time
from datetime import datetime, timezone

# nbconvert's kernel inherits this process's environment. The base image adds
# Spark to PYTHONPATH from a login profile that `docker exec python ...` never
# runs, so pyspark would be missing. Set it here rather than making every caller
# remember -e PYTHONPATH=...
_SPARK_HOME = os.environ.get("SPARK_HOME", "/usr/local/spark")
for _entry in (os.path.join(_SPARK_HOME, "python"),
               *sorted(_p for _p in __import__("glob").glob(
                   os.path.join(_SPARK_HOME, "python", "lib", "py4j-*-src.zip")))):
    if _entry not in os.environ.get("PYTHONPATH", "").split(os.pathsep):
        os.environ["PYTHONPATH"] = os.pathsep.join(
            [_entry] + [p for p in os.environ.get("PYTHONPATH", "").split(os.pathsep) if p])
    if _entry not in sys.path:
        sys.path.insert(0, _entry)

import nbformat                                          # noqa: E402
from nbclient import NotebookClient                      # noqa: E402
from nbclient.exceptions import CellExecutionError       # noqa: E402

WORK_DIR = os.path.dirname(os.path.abspath(__file__))

# (name, notebook, default-on, one-line description). Order is the dependency
# order documented in README.md - each stage consumes what the previous wrote.
STAGES = [
    ("fetch", "Fetch_all_financial_data.ipynb", False,
     "Fetch annual accounts from the Regnskapsregisteret API (hours; resumable)"),
    ("analyse", "Analyse_data.ipynb", True,
     "MongoDB-side analysis and the full profiling pass schemas.py derives from"),
    ("parquet", "Export_to_parquet.ipynb", True,
     "Mirror both collections to data/parquet/"),
    ("ndjson", "Export_to_ndjson.ipynb", True,
     "Mirror financial_data to data/ndjson/"),
    ("benchmark", "Benchmark_engines.ipynb", True,
     "Five variants x three workloads, timed - REPLACES data/benchmark_results.json"),
    ("analytics", "Build_analytics.ipynb", True,
     "Build the curated PowerBI table - REPLACES the analysis chapter's figures"),
    ("diag-variant-a", "Diagnose_variant_a.ipynb", False,
     "Investigation into the variant A formulation gap"),
    ("diag-balance", "Diagnose_balance_and_layout.ipynb", False,
     "Investigation into balance-sheet identity violations"),
]

# Generous, and per notebook rather than per cell: the fetch notebook is the
# only one that can legitimately run for hours, and it is off by default.
DEFAULT_TIMEOUT = 7200
FETCH_TIMEOUT = 86400


def wait_for_mongo(log, attempts=30, delay=2):
    """
    Block until MongoDB answers, so a run started right after `docker compose up`
    does not fail in the first notebook's first cell. `restart: unless-stopped`
    means the container is up well before the server is accepting connections.
    """
    from pymongo import MongoClient
    from pymongo.errors import PyMongoError

    for attempt in range(1, attempts + 1):
        try:
            MongoClient("mongodb://mongodb:27017/",
                        serverSelectionTimeoutMS=2000).admin.command("ping")
            return True
        except PyMongoError:
            if attempt == 1:
                log("Waiting for MongoDB ...")
            time.sleep(delay)
    log("MongoDB did not answer after %ds - is the mongodb container healthy?"
        % (attempts * delay))
    return False


class Tee:
    """Write to the console and the log file at once, so an unattended run leaves a record."""

    def __init__(self, path):
        self.handle = open(path, "a", encoding="utf-8") if path else None

    def __call__(self, message=""):
        print(message, flush=True)
        if self.handle:
            self.handle.write(message + "\n")
            self.handle.flush()

    def close(self):
        if self.handle:
            self.handle.close()


def run_notebook(path, timeout, log):
    """Execute one notebook in place. Returns (ok, seconds, error_text)."""
    notebook = nbformat.read(path, as_version=4)
    client = NotebookClient(
        notebook,
        timeout=timeout,
        kernel_name="python3",
        # Cell errors abort this notebook rather than being recorded and
        # skipped: a stage that failed halfway has not produced what the next
        # stage reads.
        allow_errors=False,
        resources={"metadata": {"path": WORK_DIR}},
    )
    started = time.time()
    try:
        client.execute()
        error = None
    except CellExecutionError as exc:
        error = str(exc)
    except Exception as exc:                              # noqa: BLE001
        error = "%s: %s" % (type(exc).__name__, exc)
    elapsed = time.time() - started

    # Saved either way. A failed run's outputs are the evidence of what broke,
    # and discarding them would mean re-running to find out.
    nbformat.write(notebook, path)
    return error is None, elapsed, error


def parse_args(stage_names):
    parser = argparse.ArgumentParser(
        description="Run the group 13 notebook pipeline in dependency order.")
    parser.add_argument("--list", action="store_true", help="show the stages and exit")
    parser.add_argument("--dry-run", action="store_true", help="print the plan without running")
    parser.add_argument("--include-fetch", action="store_true",
                        help="also run the API fetch (hours; resumable)")
    parser.add_argument("--include-diagnostics", action="store_true",
                        help="also run the two investigation notebooks")
    parser.add_argument("--only", nargs="+", metavar="STAGE", choices=stage_names,
                        help="run only these stages, in pipeline order")
    parser.add_argument("--skip", nargs="+", metavar="STAGE", choices=stage_names,
                        default=[], help="skip these stages")
    parser.add_argument("--from", dest="from_stage", metavar="STAGE", choices=stage_names,
                        help="start at this stage and run everything after it")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT,
                        help="seconds per notebook (default %d)" % DEFAULT_TIMEOUT)
    parser.add_argument("--continue-on-error", action="store_true",
                        help="keep going after a failure instead of stopping")
    parser.add_argument("--log", default="/home/jovyan/data/pipeline_run.log",
                        help="append a transcript here ('' to disable)")
    return parser.parse_args()


def select(args, stage_names):
    """Work out which stages to run, always in pipeline order."""
    if args.only:
        chosen = [s for s in STAGES if s[0] in set(args.only)]
    else:
        chosen = [s for s in STAGES if s[2]]
        if args.include_fetch:
            chosen = [s for s in STAGES if s[2] or s[0] == "fetch"]
        if args.include_diagnostics:
            names = {s[0] for s in chosen} | {"diag-variant-a", "diag-balance"}
            chosen = [s for s in STAGES if s[0] in names]
    if args.from_stage:
        start = stage_names.index(args.from_stage)
        chosen = [s for s in chosen if stage_names.index(s[0]) >= start]
    return [s for s in chosen if s[0] not in set(args.skip)]


def main():
    stage_names = [s[0] for s in STAGES]
    args = parse_args(stage_names)

    if args.list:
        print("%-16s %-38s %-8s %s" % ("STAGE", "NOTEBOOK", "DEFAULT", "DOES"))
        for name, notebook, default, description in STAGES:
            print("%-16s %-38s %-8s %s"
                  % (name, notebook, "on" if default else "off", description))
        return 0

    plan = select(args, stage_names)
    if not plan:
        print("Nothing selected.")
        return 1

    log = Tee(args.log or None)
    log("")
    log("=" * 78)
    log("Pipeline run started %s" % datetime.now(timezone.utc).isoformat(timespec="seconds"))
    log("Stages: %s" % ", ".join(name for name, _, _, _ in plan))
    log("=" * 78)

    if args.dry_run:
        for name, notebook, _, description in plan:
            log("  would run %-16s %-38s %s" % (name, notebook, description))
        log.close()
        return 0

    if not wait_for_mongo(log):
        log.close()
        return 1

    results = []
    failed = False
    total_started = time.time()

    for index, (name, notebook, _, description) in enumerate(plan, start=1):
        path = os.path.join(WORK_DIR, notebook)
        if not os.path.exists(path):
            log("[%d/%d] %-16s MISSING %s" % (index, len(plan), name, path))
            results.append((name, "missing", 0.0))
            failed = True
            if not args.continue_on_error:
                break
            continue

        timeout = FETCH_TIMEOUT if name == "fetch" else args.timeout
        log("")
        log("[%d/%d] %s - %s" % (index, len(plan), name, description))
        log("        %s (timeout %ds)" % (notebook, timeout))

        ok, elapsed, error = run_notebook(path, timeout, log)
        results.append((name, "ok" if ok else "FAILED", elapsed))

        if ok:
            log("        done in %s" % human(elapsed))
        else:
            failed = True
            log("        FAILED after %s" % human(elapsed))
            # Last few lines only: the full traceback is saved in the notebook.
            for line in (error or "").strip().splitlines()[-12:]:
                log("        | %s" % line)
            if not args.continue_on_error:
                log("")
                log("Stopping. Later stages read what this one writes; fix it and")
                log("resume with:  run_pipeline.cmd --from %s" % name)
                break

    log("")
    log("-" * 78)
    for name, status, elapsed in results:
        log("  %-16s %-8s %s" % (name, status, human(elapsed)))
    log("  %-16s %-8s %s" % ("TOTAL", "", human(time.time() - total_started)))
    log("-" * 78)
    log("Finished %s" % datetime.now(timezone.utc).isoformat(timespec="seconds"))
    log.close()
    return 1 if failed else 0


def human(seconds):
    if seconds < 60:
        return "%.1fs" % seconds
    minutes, seconds = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    return "%dh %02dm %02ds" % (hours, minutes, seconds) if hours else "%dm %02ds" % (minutes, seconds)


if __name__ == "__main__":
    sys.exit(main())
