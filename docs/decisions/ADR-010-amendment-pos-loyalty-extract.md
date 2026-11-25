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

The daily POS step writes one distinct loyalty-id file per POS date, beside
the POS partitions:

```
bronze/pos_loyalty_ids/data/dt=<YYYY-MM-DD>/part-00000.parquet
```

`ingestion/batch/extract_pos_loyalty_ids.py` reads the POS files for the run
dates (the business date, or every date in a restatement) and replaces that
date's loyalty file. A requested POS partition that is missing fails the
extract. An empty POS file writes an empty loyalty file, so a correction
that removes a card drops it.

The hourly identity job reads this extract (`--pos-loyalty-path`) and does
not read POS line items. When a POS date has no loyalty file, the job names
that date in a warning and resolves the dates that exist. Earlier dates stay
where they are, which is how a missed POS batch keeps the last validated
set. dbt still reads `bronze.pos_transactions` for monetary metrics.

The bytes logged for the identity stage are the Parquet bytes the Hadoop
filesystem reports, so an S3 input is counted.

## Consequences

- The hourly job's schema dependency on POS is the loyalty-id column of the
  extract, not the POS line-item schema.
- A restatement must run the extract over the delivered dates before the
  next identity job. The warehouse DAG does this after generating POS. Local
  `-Task pos-parquet` does it, including when the generators are skipped.
- The number of files still grows by one per day, as POS does. Each file is
  the distinct ids for that day.
