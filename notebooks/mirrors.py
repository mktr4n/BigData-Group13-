"""
Source-signature bookkeeping for the Parquet and NDJSON mirrors.

Why this exists
---------------
Both export notebooks answer the same question - "has the source moved since the
last export?" - and both recorded the answer in a `_export_metadata.json` beside
the mirror. They did it with two separate implementations: the Parquet notebook
with a function covering both collections, the NDJSON notebook with the same
logic inlined for one. The two had already diverged in shape, one nesting the
signature per collection and the other storing a bare dict, and a third reader
(the benchmark's mirror-staleness check) had to guess which it was looking at.

That divergence caused a real defect. The Parquet notebook wrote `rows_written`
for the collections it had just exported, so after a run where `companies` was
unchanged and skipped, `rows_written` held only `financial_data`. The benchmark
then read no row count for `companies` and reported

    parquet/companies          no export metadata - run the export notebook

for a mirror that was present and current, and recorded `in_sync: false` for it
in `benchmark_results.json`. `save_metadata` below carries forward the entries
for skipped collections, so "skipped because unchanged" and "never exported"
stop looking alike.

What the signature is
---------------------
Deliberately cheap: a document count for every collection, plus the newest
`fetched_at` for `financial_data`, which is what moves when the fetch notebook
re-asks a company that previously answered 404 without changing the count. It is
a staleness hint, not a checksum - it cannot see a schema change, which is why
the export notebooks keep a `FORCE_REFRESH` switch for the first run after
`schemas.py` is edited.
"""

import json
from datetime import datetime, timezone

# Collections whose signature includes the newest fetched_at, not just a count.
_TIMESTAMPED = {"financial_data": "fetched_at"}


def source_signature(db, collections):
    """Cheap per-collection fingerprint of the MongoDB side."""
    signature = {}
    for name in collections:
        entry = {"count": db[name].count_documents({})}
        field = _TIMESTAMPED.get(name)
        if field:
            newest = db[name].find_one(sort=[(field, -1)], projection={field: 1})
            value = newest.get(field) if newest else None
            entry["max_" + field] = value.isoformat() if value else None
        signature[name] = entry
    return signature


def load_metadata(path, collections):
    """
    Read a mirror's metadata, normalising the two shapes that exist on disk.

    The NDJSON export used to store the signature of its single collection as a
    bare `{"count": ..., "max_fetched_at": ...}`. Reshaping it here rather than
    forcing a re-export keeps a 705 MB write from being triggered by a
    refactor - the recorded facts are the same either way.
    """
    try:
        with open(path) as fh:
            meta = json.load(fh)
    except (OSError, ValueError):
        return {"signature": {}, "rows_written": {}, "exported_at": None}

    signature = meta.get("signature") or {}
    if signature and "count" in signature:
        signature = {collections[0]: signature}

    rows = meta.get("rows_written")
    if not isinstance(rows, dict):
        rows = {collections[0]: rows} if rows is not None else {}

    return {"signature": signature, "rows_written": rows,
            "exported_at": meta.get("exported_at")}


def stale_collections(current, previous, force=False):
    """Which collections need re-exporting. `force` overrides the comparison."""
    return {name: (force or current[name] != previous.get(name))
            for name in current}


def save_metadata(path, signature, rows_written, previous=None, wrote=True):
    """
    Record the export, carrying forward what this run did not touch.

    A collection that was skipped because it was unchanged keeps the row count
    and signature entry from the run that did write it. Without that, the next
    reader cannot tell a current mirror from a missing one.

    Called on every run, not only on runs that wrote something, so a mirror
    verified as current is recorded as current. `wrote` keeps `exported_at`
    meaning what it says: it advances only when files were actually written,
    while `checked_at` advances every time.
    """
    previous = previous or {}

    merged_rows = dict(previous.get("rows_written") or {})
    merged_rows.update(rows_written)

    merged_signature = dict(previous.get("signature") or {})
    merged_signature.update(signature)

    now = datetime.now(timezone.utc).isoformat()
    with open(path, "w") as fh:
        json.dump({"signature": merged_signature,
                   "exported_at": now if wrote else previous.get("exported_at"),
                   "checked_at": now,
                   "rows_written": merged_rows}, fh, indent=2)
    return merged_rows


def mirror_rows(metadata_path, collection):
    """
    Rows in one mirrored collection as of the last export, or None if unknown.

    Used by the benchmark to check that the file variants and the live-MongoDB
    variants are answering the same question over the same data.
    """
    meta = load_metadata(metadata_path, [collection])
    return meta["rows_written"].get(collection)


def report_status(mirrors, collection_counts):
    """
    Print and return the staleness of every mirror against the live collections.

    `mirrors` is a list of (label, metadata_path, [collections]). Warned rather
    than raised: running against slightly stale mirrors is a legitimate choice
    as long as it is recorded, so the status goes into the results file and a
    reader can see which it was.
    """
    status = {}
    for label, metadata_path, collections in mirrors:
        for collection in collections:
            key = "%s/%s" % (label, collection)
            rows = mirror_rows(metadata_path, collection)
            live = collection_counts[collection]
            status[key] = {"mirror_rows": rows, "mongodb_rows": live,
                           "in_sync": rows == live}
            if rows is None:
                print("%-26s never exported - run the export notebook" % key)
            elif rows == live:
                print("%-26s in sync (%d rows)" % (key, rows))
            else:
                print("%-26s STALE: mirror %d, MongoDB %d - re-run the export "
                      "before quoting these timings" % (key, rows, live))
    return status
