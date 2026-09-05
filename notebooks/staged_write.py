"""
Write Spark output onto the Dropbox-synced bind mount without letting Spark
commit there.

Why this exists
---------------
Spark's FileOutputCommitter does not write files where they belong. Each task
writes into `_temporary/0/_temporary/attempt_*` and the commit renames that into
`_temporary/0/task_*`, then into the output directory. On `data/`, which is a
bind mount onto a Dropbox-synced Windows folder, those renames fail
intermittently:

    java.io.IOException: Could not rename
    file:/home/jovyan/data/parquet/financial_data/_temporary/0/_temporary/attempt_...
    to file:/home/jovyan/data/parquet/financial_data/_temporary/0/task_...

Dropbox holds handles on files it is uploading, and a rename of a file it has
open fails. The failure is not recoverable in place, and it is destructive:
`mode("overwrite")` deletes the previous export before writing, so a failed
write leaves no export at all.

Writing plain files into the folder does work - that is what Dropbox is built
for. Only the rename dance fails. So Spark writes to container-local disk, where
Dropbox cannot see it, and the finished files are copied across afterwards.

Two shapes are published: `write_staged` for a directory of part files, which is
what the exports produce, and `write_staged_file` for the single analytics file
PowerBI reads, where a part-file directory would force a Folder/Combine step.

The cost is one extra pass over the data on local disk. At these sizes that is
seconds, against an export that cannot be relied on to finish.
"""

import glob
import os
import shutil

# Container-local scratch: inside the image, not on any mounted volume.
STAGING_ROOT = "/tmp/group13_export_staging"


def publish(staging, target):
    """
    Copy a finished Spark output directory onto the mount.

    Returns (files_copied, bytes_copied).
    """
    if not os.path.exists(os.path.join(staging, "_SUCCESS")):
        raise RuntimeError("%s has no _SUCCESS marker: the Spark write did not "
                           "finish, so there is nothing safe to publish" % staging)

    os.makedirs(target, exist_ok=True)

    # The previous export is removed file by file rather than with rmtree.
    # Removing the directory itself fails intermittently on this mount even when
    # writing into it succeeds, and a stale part file left behind would be read
    # back as data - Spark reads every part-* in the directory.
    for name in os.listdir(target):
        path = os.path.join(target, name)
        if os.path.isdir(path):
            shutil.rmtree(path)
        else:
            os.remove(path)

    files = bytes_copied = 0
    for name in sorted(os.listdir(staging)):
        src = os.path.join(staging, name)
        if not os.path.isfile(src):
            continue
        shutil.copyfile(src, os.path.join(target, name))
        files += 1
        bytes_copied += os.path.getsize(src)
    return files, bytes_copied


def write_staged(df, target, fmt, **options):
    """
    Write `df` to `target` through container-local staging.

    `fmt` is the DataFrameWriter method to call, "parquet" or "json". Any
    remaining keyword arguments are passed to the writer as options.
    """
    staging = os.path.join(STAGING_ROOT, os.path.basename(target.rstrip("/")))
    shutil.rmtree(staging, ignore_errors=True)
    os.makedirs(STAGING_ROOT, exist_ok=True)

    writer = df.write.mode("overwrite")
    if options:
        writer = writer.options(**options)
    getattr(writer, fmt)(staging)

    try:
        files, size = publish(staging, target)
    finally:
        # Staging is scratch and the container has a finite disk; keep it clean
        # whether or not the copy succeeded.
        shutil.rmtree(staging, ignore_errors=True)

    print("  staged write: %d files, %.2f GB copied to %s"
          % (files, size / 1024 ** 3, target))
    return files


def write_staged_file(df, target, fmt="parquet"):
    """
    Write `df` as a single file at `target` on the mount.

    Used where a reader wants a plain file path rather than a part-file
    directory - PowerBI's Parquet connector takes a file and would otherwise
    need a Folder/Combine step.

    The caller coalesces. How many partitions the data is reduced to is a
    property of the data and of what the write costs, not of this mechanism, so
    it stays visible at the call site; this only checks that the result really
    was a single part file before publishing it under the expected name.

    Returns the size written, in bytes.
    """
    staging = os.path.join(STAGING_ROOT, os.path.basename(target) + ".staging")
    shutil.rmtree(staging, ignore_errors=True)
    os.makedirs(STAGING_ROOT, exist_ok=True)

    getattr(df.write.mode("overwrite"), fmt)(staging)

    try:
        parts = sorted(glob.glob(os.path.join(staging, "part-*")))
        if len(parts) != 1:
            raise RuntimeError("expected exactly one part file in %s, got %d: "
                               "coalesce(1) before calling this" % (staging, len(parts)))
        if os.path.exists(target):
            os.remove(target)
        # copyfile rather than move: staging and the mount are different
        # filesystems, and a copy leaves the source intact if the destination
        # write is interrupted.
        shutil.copyfile(parts[0], target)
        size = os.path.getsize(target)
    finally:
        shutil.rmtree(staging, ignore_errors=True)

    print("  staged write: 1 file, %.1f MB copied to %s" % (size / 1024 ** 2, target))
    return size
