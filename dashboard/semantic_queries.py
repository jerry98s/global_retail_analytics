"""MetricFlow-compiled SQL for the dashboard. Generated file, do not edit.

Regenerate after changing transformation/dbt_project/models/semantic/*.yml
or DASHBOARD_QUERIES:
    dbt parse  (in transformation/dbt_project)
    python scripts/common/generate_semantic_queries.py
CI runs the generator with --check.
"""

QUERIES: dict[str, dict[str, str]] = {
    'duckdb': {
        'sales_by_day': """
SELECT
  metric_time__day
  , SUM(__net_revenue) AS net_revenue
  , SUM(__units_sold) AS units_sold
  , SUM(__transaction_count) AS transaction_count
FROM (
  SELECT
    DATE_TRUNC('day', cast(substring(cast(date_key as varchar), 1, 4) || '-' || substring(cast(date_key as varchar), 5, 2) || '-' || substring(cast(date_key as varchar), 7, 2) as date)) AS metric_time__day
    , net_revenue AS __net_revenue
    , units_sold AS __units_sold
    , transaction_count AS __transaction_count
  FROM "summary"."sales_daily_store" sales_daily_store_src_10000
) subq_3
GROUP BY
  metric_time__day
ORDER BY metric_time__day
""",
        'sales_by_store': """
SELECT
  store_src_10000.store_name AS store__store_name
  , SUM(sales_daily_store_src_10000.net_revenue) AS net_revenue
FROM "summary"."sales_daily_store" sales_daily_store_src_10000
LEFT OUTER JOIN
  "finance"."dim_store" store_src_10000
ON
  sales_daily_store_src_10000.store_key = store_src_10000.store_key
GROUP BY
  store_src_10000.store_name
ORDER BY net_revenue DESC
LIMIT 20
""",
        'inventory_by_day': """
WITH sma_10001_cte AS (
  SELECT
    DATE_TRUNC('day', cast(substring(cast(snapshot_date_key as varchar), 1, 4) || '-' || substring(cast(snapshot_date_key as varchar), 5, 2) || '-' || substring(cast(snapshot_date_key as varchar), 7, 2) as date)) AS snapshot_date__day
    , DATE_TRUNC('day', cast(substring(cast(snapshot_date_key as varchar), 1, 4) || '-' || substring(cast(snapshot_date_key as varchar), 5, 2) || '-' || substring(cast(snapshot_date_key as varchar), 7, 2) as date)) AS metric_time__day
    , ending_on_hand_qty AS __ending_on_hand_qty
    , ending_available_qty AS __ending_available_qty
    , stockout_hour_count AS __stockout_hour_count
  FROM "summary"."inventory_position_daily" inventory_position_daily_src_10000
)

SELECT
  COALESCE(subq_8.metric_time__day, subq_13.metric_time__day) AS metric_time__day
  , MAX(subq_8.ending_on_hand_qty) AS ending_on_hand_qty
  , MAX(subq_8.ending_available_qty) AS ending_available_qty
  , MAX(subq_13.stockout_hour_count) AS stockout_hour_count
FROM (
  SELECT
    subq_2.metric_time__day AS metric_time__day
    , SUM(subq_2.__ending_on_hand_qty) AS ending_on_hand_qty
    , SUM(subq_2.__ending_available_qty) AS ending_available_qty
  FROM (
    SELECT
      snapshot_date__day
      , metric_time__day
      , __ending_on_hand_qty
      , __ending_available_qty
    FROM sma_10001_cte
  ) subq_2
  INNER JOIN (
    SELECT
      metric_time__day
      , MAX(snapshot_date__day) AS snapshot_date__day__complete
    FROM sma_10001_cte
    GROUP BY
      metric_time__day
  ) subq_4
  ON
    subq_2.snapshot_date__day = subq_4.snapshot_date__day__complete
  GROUP BY
    subq_2.metric_time__day
) subq_8
FULL OUTER JOIN (
  SELECT
    metric_time__day
    , SUM(__stockout_hour_count) AS stockout_hour_count
  FROM sma_10001_cte
  GROUP BY
    metric_time__day
) subq_13
ON
  subq_8.metric_time__day = subq_13.metric_time__day
GROUP BY
  COALESCE(subq_8.metric_time__day, subq_13.metric_time__day)
ORDER BY metric_time__day
""",
        'inventory_current': """
SELECT
  SUM(subq_2.__ending_on_hand_qty) AS ending_on_hand_qty
  , SUM(subq_2.__ending_available_qty) AS ending_available_qty
FROM (
  SELECT
    DATE_TRUNC('day', cast(substring(cast(snapshot_date_key as varchar), 1, 4) || '-' || substring(cast(snapshot_date_key as varchar), 5, 2) || '-' || substring(cast(snapshot_date_key as varchar), 7, 2) as date)) AS snapshot_date__day
    , ending_on_hand_qty AS __ending_on_hand_qty
    , ending_available_qty AS __ending_available_qty
  FROM "summary"."inventory_position_daily" inventory_position_daily_src_10000
) subq_2
INNER JOIN (
  SELECT
    MAX(DATE_TRUNC('day', cast(substring(cast(snapshot_date_key as varchar), 1, 4) || '-' || substring(cast(snapshot_date_key as varchar), 5, 2) || '-' || substring(cast(snapshot_date_key as varchar), 7, 2) as date))) AS snapshot_date__day__complete
  FROM "summary"."inventory_position_daily" inventory_position_daily_src_10000
) subq_4
ON
  subq_2.snapshot_date__day = subq_4.snapshot_date__day__complete
""",
        'inventory_by_store': """
SELECT
  subq_5.store__store_name AS store__store_name
  , SUM(subq_5.__ending_on_hand_qty) AS ending_on_hand_qty
FROM (
  SELECT
    subq_1.snapshot_date__day AS snapshot_date__day
    , store_src_10000.store_name AS store__store_name
    , subq_1.__ending_on_hand_qty AS __ending_on_hand_qty
  FROM (
    SELECT
      DATE_TRUNC('day', cast(substring(cast(snapshot_date_key as varchar), 1, 4) || '-' || substring(cast(snapshot_date_key as varchar), 5, 2) || '-' || substring(cast(snapshot_date_key as varchar), 7, 2) as date)) AS snapshot_date__day
      , store_key AS store
      , ending_on_hand_qty AS __ending_on_hand_qty
    FROM "summary"."inventory_position_daily" inventory_position_daily_src_10000
  ) subq_1
  LEFT OUTER JOIN
    "finance"."dim_store" store_src_10000
  ON
    subq_1.store = store_src_10000.store_key
) subq_5
INNER JOIN (
  SELECT
    MAX(subq_1.snapshot_date__day) AS snapshot_date__day__complete
  FROM (
    SELECT
      DATE_TRUNC('day', cast(substring(cast(snapshot_date_key as varchar), 1, 4) || '-' || substring(cast(snapshot_date_key as varchar), 5, 2) || '-' || substring(cast(snapshot_date_key as varchar), 7, 2) as date)) AS snapshot_date__day
      , store_key AS store
    FROM "summary"."inventory_position_daily" inventory_position_daily_src_10000
  ) subq_1
  LEFT OUTER JOIN
    "finance"."dim_store" store_src_10000
  ON
    subq_1.store = store_src_10000.store_key
) subq_7
ON
  subq_5.snapshot_date__day = subq_7.snapshot_date__day__complete
GROUP BY
  subq_5.store__store_name
ORDER BY ending_on_hand_qty DESC
LIMIT 20
""",
        'session_funnel': """
WITH sma_10003_cte AS (
  SELECT
    platform AS platform_day__platform
    , session_count AS __session_count
    , converted_sessions AS __converted_sessions
    , product_view_count AS __product_view_count
    , add_to_cart_count AS __add_to_cart_count
  FROM "summary"."sessions_daily_platform" sessions_daily_platform_src_10000
)

SELECT
  COALESCE(subq_5.platform_day__platform, subq_10.platform_day__platform) AS platform_day__platform
  , MAX(subq_5.session_count) AS session_count
  , MAX(subq_5.product_view_count) AS product_view_count
  , MAX(subq_5.add_to_cart_count) AS add_to_cart_count
  , MAX(subq_5.converted_sessions) AS converted_sessions
  , MAX(CAST(subq_10.converted_sessions AS DOUBLE) / CAST(NULLIF(subq_10.session_count, 0) AS DOUBLE)) AS conversion_rate
FROM (
  SELECT
    platform_day__platform
    , SUM(__session_count) AS session_count
    , SUM(__product_view_count) AS product_view_count
    , SUM(__add_to_cart_count) AS add_to_cart_count
    , SUM(__converted_sessions) AS converted_sessions
  FROM sma_10003_cte
  GROUP BY
    platform_day__platform
) subq_5
FULL OUTER JOIN (
  SELECT
    platform_day__platform
    , SUM(__converted_sessions) AS converted_sessions
    , SUM(__session_count) AS session_count
  FROM sma_10003_cte
  GROUP BY
    platform_day__platform
) subq_10
ON
  subq_5.platform_day__platform = subq_10.platform_day__platform
GROUP BY
  COALESCE(subq_5.platform_day__platform, subq_10.platform_day__platform)
ORDER BY session_count DESC
""",
    },
    'redshift': {
        'sales_by_day': """
SELECT
  metric_time__day
  , SUM(__net_revenue) AS net_revenue
  , SUM(__units_sold) AS units_sold
  , SUM(__transaction_count) AS transaction_count
FROM (
  SELECT
    DATE_TRUNC('day', cast(substring(cast(date_key as varchar), 1, 4) || '-' || substring(cast(date_key as varchar), 5, 2) || '-' || substring(cast(date_key as varchar), 7, 2) as date)) AS metric_time__day
    , net_revenue AS __net_revenue
    , units_sold AS __units_sold
    , transaction_count AS __transaction_count
  FROM "summary"."sales_daily_store" sales_daily_store_src_10000
) subq_3
GROUP BY
  metric_time__day
ORDER BY metric_time__day
""",
        'sales_by_store': """
SELECT
  store_src_10000.store_name AS store__store_name
  , SUM(sales_daily_store_src_10000.net_revenue) AS net_revenue
FROM "summary"."sales_daily_store" sales_daily_store_src_10000
LEFT OUTER JOIN
  "finance"."dim_store" store_src_10000
ON
  sales_daily_store_src_10000.store_key = store_src_10000.store_key
GROUP BY
  store_src_10000.store_name
ORDER BY net_revenue DESC
LIMIT 20
""",
        'inventory_by_day': """
WITH sma_10001_cte AS (
  SELECT
    DATE_TRUNC('day', cast(substring(cast(snapshot_date_key as varchar), 1, 4) || '-' || substring(cast(snapshot_date_key as varchar), 5, 2) || '-' || substring(cast(snapshot_date_key as varchar), 7, 2) as date)) AS snapshot_date__day
    , DATE_TRUNC('day', cast(substring(cast(snapshot_date_key as varchar), 1, 4) || '-' || substring(cast(snapshot_date_key as varchar), 5, 2) || '-' || substring(cast(snapshot_date_key as varchar), 7, 2) as date)) AS metric_time__day
    , ending_on_hand_qty AS __ending_on_hand_qty
    , ending_available_qty AS __ending_available_qty
    , stockout_hour_count AS __stockout_hour_count
  FROM "summary"."inventory_position_daily" inventory_position_daily_src_10000
)

SELECT
  COALESCE(subq_8.metric_time__day, subq_13.metric_time__day) AS metric_time__day
  , MAX(subq_8.ending_on_hand_qty) AS ending_on_hand_qty
  , MAX(subq_8.ending_available_qty) AS ending_available_qty
  , MAX(subq_13.stockout_hour_count) AS stockout_hour_count
FROM (
  SELECT
    subq_2.metric_time__day AS metric_time__day
    , SUM(subq_2.__ending_on_hand_qty) AS ending_on_hand_qty
    , SUM(subq_2.__ending_available_qty) AS ending_available_qty
  FROM (
    SELECT
      snapshot_date__day
      , metric_time__day
      , __ending_on_hand_qty
      , __ending_available_qty
    FROM sma_10001_cte
  ) subq_2
  INNER JOIN (
    SELECT
      metric_time__day
      , MAX(snapshot_date__day) AS snapshot_date__day__complete
    FROM sma_10001_cte
    GROUP BY
      metric_time__day
  ) subq_4
  ON
    subq_2.snapshot_date__day = subq_4.snapshot_date__day__complete
  GROUP BY
    subq_2.metric_time__day
) subq_8
FULL OUTER JOIN (
  SELECT
    metric_time__day
    , SUM(__stockout_hour_count) AS stockout_hour_count
  FROM sma_10001_cte
  GROUP BY
    metric_time__day
) subq_13
ON
  subq_8.metric_time__day = subq_13.metric_time__day
GROUP BY
  COALESCE(subq_8.metric_time__day, subq_13.metric_time__day)
ORDER BY metric_time__day
""",
        'inventory_current': """
SELECT
  SUM(subq_2.__ending_on_hand_qty) AS ending_on_hand_qty
  , SUM(subq_2.__ending_available_qty) AS ending_available_qty
FROM (
  SELECT
    DATE_TRUNC('day', cast(substring(cast(snapshot_date_key as varchar), 1, 4) || '-' || substring(cast(snapshot_date_key as varchar), 5, 2) || '-' || substring(cast(snapshot_date_key as varchar), 7, 2) as date)) AS snapshot_date__day
    , ending_on_hand_qty AS __ending_on_hand_qty
    , ending_available_qty AS __ending_available_qty
  FROM "summary"."inventory_position_daily" inventory_position_daily_src_10000
) subq_2
INNER JOIN (
  SELECT
    MAX(DATE_TRUNC('day', cast(substring(cast(snapshot_date_key as varchar), 1, 4) || '-' || substring(cast(snapshot_date_key as varchar), 5, 2) || '-' || substring(cast(snapshot_date_key as varchar), 7, 2) as date))) AS snapshot_date__day__complete
  FROM "summary"."inventory_position_daily" inventory_position_daily_src_10000
) subq_4
ON
  subq_2.snapshot_date__day = subq_4.snapshot_date__day__complete
""",
        'inventory_by_store': """
SELECT
  subq_5.store__store_name AS store__store_name
  , SUM(subq_5.__ending_on_hand_qty) AS ending_on_hand_qty
FROM (
  SELECT
    subq_1.snapshot_date__day AS snapshot_date__day
    , store_src_10000.store_name AS store__store_name
    , subq_1.__ending_on_hand_qty AS __ending_on_hand_qty
  FROM (
    SELECT
      DATE_TRUNC('day', cast(substring(cast(snapshot_date_key as varchar), 1, 4) || '-' || substring(cast(snapshot_date_key as varchar), 5, 2) || '-' || substring(cast(snapshot_date_key as varchar), 7, 2) as date)) AS snapshot_date__day
      , store_key AS store
      , ending_on_hand_qty AS __ending_on_hand_qty
    FROM "summary"."inventory_position_daily" inventory_position_daily_src_10000
  ) subq_1
  LEFT OUTER JOIN
    "finance"."dim_store" store_src_10000
  ON
    subq_1.store = store_src_10000.store_key
) subq_5
INNER JOIN (
  SELECT
    MAX(subq_1.snapshot_date__day) AS snapshot_date__day__complete
  FROM (
    SELECT
      DATE_TRUNC('day', cast(substring(cast(snapshot_date_key as varchar), 1, 4) || '-' || substring(cast(snapshot_date_key as varchar), 5, 2) || '-' || substring(cast(snapshot_date_key as varchar), 7, 2) as date)) AS snapshot_date__day
      , store_key AS store
    FROM "summary"."inventory_position_daily" inventory_position_daily_src_10000
  ) subq_1
  LEFT OUTER JOIN
    "finance"."dim_store" store_src_10000
  ON
    subq_1.store = store_src_10000.store_key
) subq_7
ON
  subq_5.snapshot_date__day = subq_7.snapshot_date__day__complete
GROUP BY
  subq_5.store__store_name
ORDER BY ending_on_hand_qty DESC
LIMIT 20
""",
        'session_funnel': """
WITH sma_10003_cte AS (
  SELECT
    platform AS platform_day__platform
    , session_count AS __session_count
    , converted_sessions AS __converted_sessions
    , product_view_count AS __product_view_count
    , add_to_cart_count AS __add_to_cart_count
  FROM "summary"."sessions_daily_platform" sessions_daily_platform_src_10000
)

SELECT
  COALESCE(subq_5.platform_day__platform, subq_10.platform_day__platform) AS platform_day__platform
  , MAX(subq_5.session_count) AS session_count
  , MAX(subq_5.product_view_count) AS product_view_count
  , MAX(subq_5.add_to_cart_count) AS add_to_cart_count
  , MAX(subq_5.converted_sessions) AS converted_sessions
  , MAX(CAST(subq_10.converted_sessions AS DOUBLE PRECISION) / CAST(NULLIF(subq_10.session_count, 0) AS DOUBLE PRECISION)) AS conversion_rate
FROM (
  SELECT
    platform_day__platform
    , SUM(__session_count) AS session_count
    , SUM(__product_view_count) AS product_view_count
    , SUM(__add_to_cart_count) AS add_to_cart_count
    , SUM(__converted_sessions) AS converted_sessions
  FROM sma_10003_cte
  GROUP BY
    platform_day__platform
) subq_5
FULL OUTER JOIN (
  SELECT
    platform_day__platform
    , SUM(__converted_sessions) AS converted_sessions
    , SUM(__session_count) AS session_count
  FROM sma_10003_cte
  GROUP BY
    platform_day__platform
) subq_10
ON
  subq_5.platform_day__platform = subq_10.platform_day__platform
GROUP BY
  COALESCE(subq_5.platform_day__platform, subq_10.platform_day__platform)
ORDER BY session_count DESC
""",
    },
}
