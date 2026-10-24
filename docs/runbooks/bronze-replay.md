# Raw Bronze rollout and replay

See [ADR-011](../decisions/ADR-011-replayable-bronze.md) for contracts and limits.
This is a code migration, not proof that an existing deployment has raw history.

## Rollout

1. Pause downstream dbt, GE, catalog refresh and dashboard readers. Stop the two
   old Bronze jobs; record their last offsets/checkpoints. Keep legacy tables.
2. Deploy the new job code plus `raw_bronze.py`. These jobs have new source/sink
   graphs: start fresh, do not restore incompatible old validation/dedup state.
   Choose stable `BRONZE_SOURCE_SYSTEM` per cluster/topic incarnation.
   Local Compose: set `$env:BRONZE_SOURCE_SYSTEM='retail-local-v2'` before
   recreating the Flink containers. Cloud CLI: pass
   `deploy_platform.ps1 -Action flink -BronzeSourceSystem retail-dev-v2`.
   MWAA: set the `bronze_source_system` Airflow Variable to the same value;
   it is forwarded to the YARN application master. Do not change it during
   ordinary restarts of the same source incarnation.
3. Start both raw jobs. Earliest retained Kafka offsets are recaptured into new
   `bronze.*_records` tables; wait for a successful Iceberg checkpoint commit.
   Confirm offset coverage against Kafka's earliest/end offsets. Kafka retention
   already lost is not recoverable from old filtered tables.
4. Package `canonical_events.py,event_contract.py,graph_logic.py` with the Spark
   job (`--py-files` is wired in Compose, deploy script and marketing DAG).
   Run Spark; require reconciliation counts and successful identity exports.
5. Cloud: regenerate/apply the Silver Spectrum DDL using the normal bootstrap
   procedure. New `silver.*_events` locations use `consumer_current`, not Iceberg
   `data/`. Legacy Bronze Spectrum tables can remain for historical inspection.
6. Local: run dbt/quality after Spark. Seed-only mode remains independent; do not
   switch to fixtures to mask a missing raw or canonical dataset.
7. Resume downstream schedules only after audits pass. Observe raw source lag,
   quarantine reasons, duplicate rates and raw-to-canonical reconciliation.

Local sequence (Docker must be running):

```powershell
.\scripts\local\run_local_stack.ps1 -Task up
.\scripts\local\run_local_stack.ps1 -Task topics
.\scripts\local\run_local_stack.ps1 -Task flink
.\scripts\local\run_local_stack.ps1 -Task simulate
.\scripts\local\run_local_stack.ps1 -Task pos-parquet
.\scripts\local\run_local_stack.ps1 -Task spark
.\scripts\local\run_local_stack.ps1 -Task dbt
.\scripts\local\run_local_stack.ps1 -Task quality
```

## Replay without Kafka

Stop concurrent Spark writers and pause consumers, including the catalog DAG.
Keep Bronze tables and files intact. The Spark job reads committed Iceberg
snapshots, not Kafka and not globbed Bronze data files. Rerunning `-Task spark`
rebuilds canonical events plus identity from the current Bronze snapshot.

For events only, submit the same deployed Spark job and runtime packages with:

```text
spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3 \
  --py-files graph_logic.py,canonical_events.py,event_contract.py \
  identity_resolution_job.py \
  --bronze-warehouse <bronze-warehouse-uri> \
  --silver-warehouse <silver-warehouse-uri> --canonical-only
```

The example assumes the job directory is the working directory; supply full
paths otherwise. Cloud submissions need the same S3 filesystem/auth configuration
as normal EMR jobs. Preserve Spark logs containing each `bronze_snapshot_id` and
reconciliation counts. A fixed historical snapshot/isolated output override is
not exposed by this CLI: current committed snapshots are pinned per stream.
Do not expire those snapshots during a long replay.

Use Iceberg-aware Spark SQL to inspect:

```sql
SELECT error_reason, count(*) FROM iceberg_silver.silver.clickstream_quarantine
GROUP BY error_reason;
SELECT duplicate_kind, count(*) FROM iceberg_silver.silver.clickstream_duplicates
GROUP BY duplicate_kind;
```

An unchanged snapshot and policy must yield the same canonical event IDs and
winning Kafka coordinates. A new policy can change membership; replay replaces
exports so removed rows do not remain in downstream scans. Rerun failed exports
fully while consumers stay paused. Do not treat directory existence as a
successful replay signal.

## Gold and live inventory limits

Canonical replay is not automatic historical Gold repair. dbt incrementals have
lookback windows; old corrected data requires an owner-scoped WAP rebuild:
clone live to pending, rebuild affected facts/intermediates beyond their normal
lookback, test/GE audit, then publish only that owner's tables. Preserve existing
SCD2 product history; never indiscriminately full-refresh all models or publish
seed dimensions. Use existing WAP runbooks for the audited publish procedure.

Do not overwrite live `silver.inventory_hourly` from replay while Flink owns it.
Compare canonical inventory against that fast-path output first; an historical
rollup rebuild and writer handoff are separate operations, not implemented by
`--canonical-only`. Bronze remains available for that future reconciliation.

## Tests

```powershell
.\.venv\Scripts\python.exe -m pytest tests/unit -q
$env:PYTHONPATH=(Get-Location).Path
spark-submit --master local[2] --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3 tests/integration/verify_bronze_replay.py --iceberg
```

The Spark regression needs the repository on `PYTHONPATH`; CI configures it.
It checks invalid bytes/tombstones, old timestamps, both duplicate classes,
deterministic winners, count conservation and replacement exports. A live Kafka
to Flink to Iceberg smoke run remains required before production rollout.
