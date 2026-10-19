"""MWAA Airflow plugins package.

Houses Python callables imported by DAGs under ``orchestration/airflow/dags/``.
MWAA auto-loads modules from this directory; the ``dags/`` files import them
directly (e.g. ``from row_count_reconciliation import reconcile_gold_row_counts``).

Keeping callables here (rather than inline in DAG files) lets unit tests import
them without pulling in Airflow's runtime — see ``tests/unit/`` for any future
contract tests.
"""
