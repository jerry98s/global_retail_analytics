"""
SCD Type 2 correctness test suite for dim_product.
Tests set-level temporal properties that column-level GE checks cannot catch.

Four failure modes tested:
  1. OVERLAP   — two versions active for the same period
  2. GAP       — no version covers a specific date range
  3. PHANTOM   — consecutive versions with identical record_hash
  4. ORPHAN    — product_key in fact tables with no dim_product match
"""

import pandas as pd
import pytest

pytestmark = pytest.mark.integration


class TestSCD2NoOverlap:
    """No two versions of the same product should cover the same date."""

    def test_no_overlapping_versions(self, dim_product_df: pd.DataFrame):
        violations = []

        for product_id, group in dim_product_df.groupby("product_id"):
            rows = group.sort_values("effective_from").to_dict("records")

            for i in range(len(rows)):
                for j in range(i + 1, len(rows)):
                    a, b = rows[i], rows[j]
                    a_end = pd.Timestamp(a["effective_to"] or "2099-12-31")
                    b_end = pd.Timestamp(b["effective_to"] or "2099-12-31")
                    a_start = pd.Timestamp(a["effective_from"])
                    b_start = pd.Timestamp(b["effective_from"])

                    if a_start < b_end and b_start < a_end:
                        violations.append({
                            "product_id": product_id,
                            "key_a": a["product_key"],
                            "key_b": b["product_key"],
                            "a_range": f"{a['effective_from']} → {a['effective_to']}",
                            "b_range": f"{b['effective_from']} → {b['effective_to']}",
                        })

        assert len(violations) == 0, (
            f"{len(violations)} overlapping version pairs found:\n"
            + pd.DataFrame(violations).to_string()
        )


class TestSCD2NoGap:
    """Versions for the same product must be contiguous."""

    def test_no_temporal_gaps(self, dim_product_df: pd.DataFrame):
        violations = []

        for product_id, group in dim_product_df.groupby("product_id"):
            rows = group.sort_values("effective_from").to_dict("records")
            if len(rows) < 2:
                continue

            for i in range(len(rows) - 1):
                current_end = rows[i]["effective_to"]
                next_start  = rows[i + 1]["effective_from"]

                if current_end is None:
                    continue  # current record — overlap check handles this

                if pd.Timestamp(current_end) != pd.Timestamp(next_start):
                    violations.append({
                        "product_id":    product_id,
                        "gap_after_key": rows[i]["product_key"],
                        "version_ends":  current_end,
                        "next_starts":   next_start,
                        "gap_days": (
                            pd.Timestamp(next_start)
                            - pd.Timestamp(current_end)
                        ).days
                    })

        assert len(violations) == 0, (
            f"{len(violations)} temporal gaps found:\n"
            + pd.DataFrame(violations).to_string()
        )


class TestSCD2NoPhantom:
    """Consecutive versions must not have identical record_hash values."""

    def test_no_phantom_versions(self, dim_product_df: pd.DataFrame):
        violations = []

        for product_id, group in dim_product_df.groupby("product_id"):
            rows = group.sort_values("effective_from").to_dict("records")

            for i in range(len(rows) - 1):
                if rows[i]["record_hash"] == rows[i + 1]["record_hash"]:
                    violations.append({
                        "product_id":  product_id,
                        "key_a":       rows[i]["product_key"],
                        "key_b":       rows[i + 1]["product_key"],
                        "hash_prefix": rows[i]["record_hash"][:16],
                    })

        assert len(violations) == 0, (
            f"{len(violations)} phantom version pairs found "
            f"(consecutive identical hashes):\n"
            + pd.DataFrame(violations).to_string()
        )


class TestSCD2CurrentRecord:
    """Every active product must have exactly one current record."""

    def test_exactly_one_current_per_product(
        self, dim_product_df: pd.DataFrame
    ):
        current = dim_product_df[dim_product_df["is_current"]]
        counts  = current.groupby("product_id").size()

        zero_current  = dim_product_df[
            ~dim_product_df["product_id"].isin(current["product_id"])
        ]["product_id"].unique()
        multi_current = counts[counts > 1].index.tolist()

        errors = []
        if len(zero_current) > 0:
            errors.append(
                f"Products with NO current record "
                f"(ETL mid-transaction failure?): {list(zero_current[:5])}"
            )
        if len(multi_current) > 0:
            errors.append(
                f"Products with MULTIPLE current records "
                f"(ETL ran twice?): {multi_current[:5]}"
            )

        assert not errors, "\n".join(errors)

    def test_effective_date_ordering(self, dim_product_df: pd.DataFrame):
        """effective_to must always be strictly after effective_from."""
        closed = dim_product_df[dim_product_df["effective_to"].notna()].copy()
        closed["ef"] = pd.to_datetime(closed["effective_from"])
        closed["et"] = pd.to_datetime(closed["effective_to"])
        bad = closed[closed["et"] <= closed["ef"]]

        assert len(bad) == 0, (
            f"{len(bad)} rows where effective_to <= effective_from:\n"
            + bad[["product_key", "product_id",
                   "effective_from", "effective_to"]].to_string()
        )


class TestSCD2NoOrphanFactReferences:
    """Fact table product keys must exist in dim_product."""

    def test_no_orphan_product_keys_in_fact_sales(
        self,
        fact_sales_sample_df: pd.DataFrame,
        dim_product_df: pd.DataFrame,
    ):
        fact_keys = set(
            fact_sales_sample_df["product_key"].dropna().astype(int).tolist()
        )
        dim_keys = set(dim_product_df["product_key"].dropna().astype(int).tolist())
        missing = sorted(fact_keys - dim_keys)
        assert not missing, (
            "Orphan product_key values found in fact_sales with no dim_product match: "
            f"{missing[:20]}"
        )
