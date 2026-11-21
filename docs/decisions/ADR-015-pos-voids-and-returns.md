# ADR-015: POS Voids Versus Returns

**Status:** Accepted (voids, the returns source, `fact_returns` and the summary implemented)
**Date:** 2026-11-21
**Author:** Data platform team
**Related:** [ADR-009](ADR-009-write-audit-publish.md)

---

## Context

POS Bronze carried one flag, `is_voided`, drawn at random per line. It could
not say whether one item or the whole purchase was cancelled, and the platform
had no way to record a refund. Finance needs three operations kept apart:

| Operation | Example | Expected result |
|---|---|---|
| Line void | One item cancelled before checkout | Keep the line; not a sale |
| Transaction void | Whole purchase cancelled before it completes | Keep every line; none is a sale |
| Return | Item brought back days later | Keep the sale; record a separate refund |

## Decision

1. **Status columns on the existing line grain.** POS Bronze keeps one row
   per `(transaction_id, line_item_number)` and adds `line_status`
   (`ACTIVE`, `VOIDED`), `transaction_status` (`COMPLETED`, `VOIDED`, repeated
   on every line), `voided_at`, `void_reason` and `source_updated_at`.
2. **`is_voided` is derived, not trusted.** `stg_pos_transactions` sets it to
   line OR transaction voided. Bronze written before these columns maps its
   old flag to a line void, and a NULL flag stays NULL so the publish blocks.
3. **Voided lines stay in `fact_sales`** with their recorded amounts.
   Recognized sales are the rows where `is_voided` is false; a transaction
   with no recognized line is not counted. A blocking test checks that the
   lines of a transaction agree on its status and that void audit fields are
   present exactly on voided lines.
4. **A return is a new transaction, not an edit.** Returns land as their own
   daily Parquet source, `bronze.pos_returns`, keyed by
   `(return_id, return_line_number)` and partitioned by `return_date`; an
   empty day is a zero-row file. `finance.fact_returns` holds one row per
   return line, referencing the original line. The original sale is never
   changed. A refund counts on its return processing date and store.
   `is_refund_recognized` is true only for a completed return of a
   non-voided sale line. A return of a voided line is kept as `VOIDED_SALE`
   with no refund. A return naming an unknown line or the wrong product, or
   recognized refunds that exceed the line's quantity or net revenue across
   all days, block the finance publish. Exact duplicate deliveries collapse to
   one row; conflicting rows with the same key block. A return matches only
   a sale line sold within `pos_return_window_days` (default 90) on or
   before its own return date, so a full build, a replay and a one-day
   rebuild judge it the same way. An explicit restatement clears and reloads
   the whole returns range, and also rebuilds every later return day (up to
   the window) that refunds a restated sale line, so a sale voided or
   corrected afterwards re-judges its refunds; `sales_daily_store` is
   re-aggregated over the same span. Every requested returns date must have a
   delivered file: the loader and the DAG fail on a missing one instead of
   reading it as a day without returns.
5. **One summary, combined after aggregation.** `summary.sales_daily_store`
   adds `return_transaction_count`, `return_amount` and
   `net_sales_after_returns`. Sales and returns are each aggregated to
   `(date_key, store_key)` first and then full-outer-joined. This is the one
   summary table built from two facts; it never joins them line to line. The
   existing `transaction_count` stays the completed-sales count, and
   `net_revenue` stays sales after discounts, before returns. A store-day
   with refunds and no sales keeps a row with zero sales measures. The
   blocking reconciliation test checks returns source to fact, fact to
   summary, and net sales after returns at exact decimal amounts.
6. **Returns do not move inventory.** A refunded item may be scrapped, so
   financial returns never add stock in this release.

## Consequences

- Rows already in `fact_sales` keep NULL statuses until their day is
  reprocessed (lookback or explicit restatement); their `is_voided` is still
  correct.
- The daily DAG copies the POS partitions in the returns generator's lookback
  from S3, runs the generator on local files, uploads the day's file to
  `pos_returns_bronze_s3_path`, and registers its Spectrum partition. Before
  porting to `main`: Spectrum's `bronze.pos_transactions` needs the new
  columns, `bronze.pos_returns` needs an external table, and the Airflow
  Variable `pos_returns_bronze_s3_path` must be seeded.
- Existing live `finance.fact_sales` and `summary.sales_daily_store` gain
  their new columns at publish: WAP adds columns that only pending has to
  live inside the publish transaction (ADR-009), and still aborts when
  pending lacks a live column. On the first run after rollout,
  `sales_daily_store` sees no `return_amount` in its pending clone and
  re-aggregates every day from both facts, so no older row keeps NULL return
  measures.
- Generated POS keeps its existing lines for a given date; void timestamps and
  transaction voids come from a separate random stream.

## Alternatives considered

- **Keep one `is_voided` flag.** Cannot separate a cancelled item from a
  cancelled purchase. Rejected.
- **Separate header and line tables.** Closer to a real POS, but the
  simulated source has no header feed. Deferred to a real integration.
- **Subtract refunds from the original sale row.** Rewrites closed periods and
  loses the audit trail. Rejected.
- **A separate `summary.returns_daily_store`.** Keeps one fact per summary,
  but pushes net sales into every consumer. Rejected in favour of decision 5.
