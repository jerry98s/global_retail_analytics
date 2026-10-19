"""Execute the real PySpark/GraphFrames identity path on a tiny graph.

This is intentionally a standalone spark-submit smoke test rather than a
pytest unit test. It verifies that the DataFrame implementation matches the
plain-Python rules, including normalization of blank/whitespace identifiers.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from pyspark.sql import SparkSession

from spark.identity_resolution import graph_logic as reference
from spark.identity_resolution.identity_resolution_job import (
    assign_resolution,
    build_edges,
    build_vertices,
    write_consumer_exports,
)


CLICKSTREAM_ROWS = [
    {
        "client_id": " c-anchor ",
        "customer_id": " L1001 ",
        "event_time": "2026-08-30T00:00:00Z",
    },
    {
        "client_id": "c-shared",
        "customer_id": "C-01",
        "event_time": "2026-08-30T00:00:01Z",
    },
    {
        "client_id": "c-shared",
        "customer_id": "C-02",
        "event_time": "2026-08-30T00:00:02Z",
    },
    {
        "client_id": "c-empty-customer",
        "customer_id": " ",
        "event_time": "2026-08-30T00:00:03Z",
    },
    {
        "client_id": "",
        "customer_id": "C-orphan",
        "event_time": "2026-08-30T00:00:04Z",
    },
]

POS_ROWS = [{"loyalty_id": " L1001 "}, {"loyalty_id": " "}]


def _reference_rows() -> dict[tuple[str, str], tuple]:
    _, rows, _ = reference.resolve_identity_graph(
        CLICKSTREAM_ROWS,
        POS_ROWS,
        threshold=2,
    )
    return {
        (row.identifier_type, row.identifier_value): (
            row.customer_key,
            round(row.confidence_score, 4),
            row.resolution_method,
            row.is_public_device,
            row.component_rep_node,
            row.component_rep_type,
        )
        for row in rows
    }


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="spark-identity-checkpoint-") as tmp:
        spark = (
            SparkSession.builder.master("local[2]")
            .appName("verify-spark-identity")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        try:
            spark.sparkContext.setCheckpointDir(str(Path(tmp).resolve()))
            clickstream = spark.createDataFrame(CLICKSTREAM_ROWS)
            pos = spark.createDataFrame(POS_ROWS)
            edges, public_devices = build_edges(clickstream, pos, threshold=2)
            vertices = build_vertices(clickstream, pos)
            actual_frame = assign_resolution(
                spark,
                vertices,
                edges,
                public_devices,
            )
            actual = {
                (row.identifier_type, row.identifier_value): (
                    row.customer_key,
                    round(float(row.confidence_score), 4),
                    row.resolution_method,
                    row.is_public_device,
                    row.component_rep_node,
                    row.component_rep_type,
                )
                for row in actual_frame.collect()
            }

            # Consumers never glob an Iceberg table's data directory. Verify
            # the dedicated plain-Parquet handoff is replace-only across runs.
            consumer_root = (Path(tmp) / "consumer-current").resolve().as_uri()
            write_consumer_exports(actual_frame, edges, consumer_root)
            second_resolution = actual_frame.filter("identifier_value = 'L1001'")
            second_edges = edges.limit(1)
            write_consumer_exports(second_resolution, second_edges, consumer_root)
            exported_resolution = spark.read.parquet(
                f"{consumer_root}/identity_resolution"
            )
            exported_edges = spark.read.parquet(f"{consumer_root}/identity_edges")
            if exported_resolution.count() != 1 or exported_edges.count() != 1:
                raise SystemExit("consumer_current export retained superseded run files")
            if exported_resolution.first().identifier_value != "L1001":
                raise SystemExit("consumer_current export did not expose the latest run")
        finally:
            spark.stop()

    expected = _reference_rows()
    if actual != expected:
        missing = expected.keys() - actual.keys()
        extra = actual.keys() - expected.keys()
        mismatched = {
            key: (expected[key], actual[key])
            for key in expected.keys() & actual.keys()
            if expected[key] != actual[key]
        }
        raise SystemExit(
            "Spark/reference identity mismatch: "
            f"missing={sorted(missing)}, extra={sorted(extra)}, "
            f"mismatched={mismatched}"
        )

    forbidden = {("client_id", ""), ("customer_id", ""), ("loyalty_id", "")}
    if forbidden & actual.keys():
        raise SystemExit(f"Blank graph nodes survived normalization: {forbidden & actual.keys()}")

    print(f"PASS: Spark GraphFrames matches reference logic ({len(actual)} identifiers).")


if __name__ == "__main__":
    main()
