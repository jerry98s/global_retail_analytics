"""
SCD2 product refresh DAG.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from metadata_airflow import (
    dbt_bash_with_metadata,
    on_dag_failure,
    on_dag_start,
    on_dag_success,
)

DAG_DOC_MD = """
## catalog_bihourly_product_scd2_refresh

Refreshes `marketing.dim_product` (the only SCD Type 2 dimension in the
platform, per `docs/data-model/dimensional-model.md`).

### Purpose
Re-processes the dim_product dbt model every two hours so that upstream
product catalog changes (price, category, brand, status) are captured as
new SCD2 rows with `valid_from = now()` and the prior row is closed out
with `valid_to = now()` and `is_current = false`.

### Why separate from warehouse_daily_batch_pipeline
- The product catalog changes intra-day (merchandising team), and dashboards
  on `fact_sales` rely on the SCD2 join (`is_current = true`) for *current*
  pricing. A 24h delay would make today's margin reports stale.
- The cost is one dbt run of a single model (~30s) every 2h — negligible
  vs. the cost of stale product attributes on Gold marts.

### Idempotency
`dbt run --select dim_product` uses `incremental_strategy='delete+insert'`
with `unique_key='product_key'` for SCD2 close-out rows. Re-running for the
same effective_from timestamp will close + re-open the same SCD2 row, so
operators should only force-rerun this DAG if the upstream catalog source
has actually changed. There is no full-refresh needed.

### Recovery
If `dbt_test_dim_product` fails:
1. Inspect the failing test (often `dbt_utils.unique_combination_of_columns`
   on `(product_id, valid_from)` or the `valid_to >= valid_from` assertion).
2. Roll back the upstream catalog source if a bad write happened.
3. Re-run the DAG — dbt's incremental model will issue corrective SCD2 rows
   (a new row with `valid_from = now()` for the correct attributes; the
   erroneous row remains in history with `valid_to = now()`).
4. If the bad row has propagated to `fact_sales` (its `product_key` FK now
   points at the erroneous SCD2 row), re-run `warehouse_daily_batch_pipeline` for the
   affected date partition.

### Related
- `docs/decisions/ADR-005-scd2-on-dim_product-only.md`
- `transformation/dbt_project/models/marts/marketing/dim_product.sql`
"""

with DAG(
    dag_id="catalog_bihourly_product_scd2_refresh",
    default_args={
        "owner": "data-platform",
        "start_date": datetime(2024, 1, 1),
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
        "email_on_failure": True,
        "email": ["data-platform@company.com"],
    },
    schedule_interval="0 */2 * * *",
    catchup=False,
    tags=["dbt", "scd2"],
    doc_md=DAG_DOC_MD,
    on_success_callback=on_dag_success,
    on_failure_callback=on_dag_failure,
) as dag:
    metadata_start = PythonOperator(
        task_id="metadata_start",
        python_callable=on_dag_start,
    )
    refresh_dim_product = BashOperator(
        task_id="dbt_run_dim_product",
        bash_command=dbt_bash_with_metadata(
            "aws s3 sync s3://{{ var.value.artifacts_bucket }}/mwaa/dbt_project /tmp/dbt_project && "
            "aws s3 sync s3://{{ var.value.artifacts_bucket }}/mwaa/scripts /tmp/scripts && "
            "cp /tmp/dbt_project/profiles.yml.example /tmp/dbt_project/profiles.yml && "
            "cd /tmp/dbt_project && dbt deps && "
            "dbt run --select dim_product --target prod"
        ),
    )

    test_dim_product = BashOperator(
        task_id="dbt_test_dim_product",
        bash_command=dbt_bash_with_metadata(
            "cd /tmp/dbt_project && "
            "dbt test --select dim_product --target prod"
        ),
    )

    metadata_start >> refresh_dim_product >> test_dim_product
