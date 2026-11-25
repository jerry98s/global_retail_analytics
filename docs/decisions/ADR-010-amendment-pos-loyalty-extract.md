# ADR-010 amendment: POS loyalty extract

**Status:** Accepted
**Date:** 2026-11-25
**Amends:** [ADR-010](./ADR-010-spark-graphframes-identity.md)

---

## Context

The hourly identity job was reading the full POS Parquet. It uses one column,
`loyalty_id`, and only the distinct non-blank values become graph nodes.
The input size logged on S3 was also zero, because the driver summed a local
directory that does not exist there.

A single cumulative loyalty file would have to be rebuilt and rolled back
whenever a POS day failed or was restated. Corrected POS files are delivered
by an operator, so the extract has to come from the partition that is
actually in Bronze.

## Decision

The daily POS step writes one distinct loyalty-id file per requested date
under a new run directory, then replaces one manifest. The manifest is
replaced only after every requested date has been written, and it keeps
every previously committed date this run did not touch:

```
bronze/pos_loyalty_ids/run=<id>/dt=<YYYY-MM-DD>/part-00000.parquet
bronze/pos_loyalty_ids/manifest
```

`ingestion/batch/extract_pos_loyalty_ids.py` reads the POS files for the run
dates (the business date, or every date in a restatement). A requested POS
partition that is missing fails the extract, deletes that run's files, and
leaves the previous manifest in place. An empty POS file is still committed,
so a correction that removes a card drops it. A one-day run does not drop
the other committed dates.

The hourly identity job reads that manifest (`--pos-loyalty-path` is the
extract root) and does not read POS line items. It pins the manifest file
list before it writes anything. A POS date the manifest does not cover fails
the run, so the previous identity tables stay. dbt still reads
`bronze.pos_transactions` for monetary metrics.

The bytes logged for the identity stage are the sizes of the pinned files,
so an S3 input is counted.

## Consequences

- The hourly job's schema dependency on POS is the loyalty-id column of the
  extract, not the POS line-item schema.
- A restatement must commit the extract for the delivered dates before the
  next identity job. The warehouse DAG does this after generating POS, and
  uploads the manifest last. Local `-Task pos-parquet` does it, including
  when the generators are skipped.
- Successful run directories are left in place. A reader that already pinned
  a manifest can finish against those files. A failed run's directory is
  removed.
- The number of committed dates still grows by one per day, as POS does.
  Each file is the distinct ids for that day.
