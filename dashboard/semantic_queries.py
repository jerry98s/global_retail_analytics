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
    DATE_TRUNC('day', cast(substring(cast(snapshot_date_key as varchar), 1, 4) || '-' || substring(cast(snapshot_date_key as varchar), 5, 2) || '-' || substring(cast(snapshot_date_key as varchar), 7, 2) as date)) AS position_date__day
    , DATE_TRUNC('day', cast(substring(cast(snapshot_date_key as varchar), 1, 4) || '-' || substring(cast(snapshot_date_key as varchar), 5, 2) || '-' || substring(cast(snapshot_date_key as varchar), 7, 2) as date)) AS metric_time__day
    , ending_on_hand_qty AS __inventory_on_hand
    , ending_available_qty AS __inventory_available
    , stockout_hour_count AS __stockout_hours
  FROM "summary"."inventory_position_daily" inventory_position_daily_src_10000
)

SELECT
  COALESCE(subq_8.metric_time__day, subq_13.metric_time__day) AS metric_time__day
  , MAX(subq_8.inventory_on_hand) AS inventory_on_hand
  , MAX(subq_8.inventory_available) AS inventory_available
  , MAX(subq_13.stockout_hours) AS stockout_hours
FROM (
  SELECT
    subq_2.metric_time__day AS metric_time__day
    , SUM(subq_2.__inventory_on_hand) AS inventory_on_hand
    , SUM(subq_2.__inventory_available) AS inventory_available
  FROM (
    SELECT
      position_date__day
      , metric_time__day
      , __inventory_on_hand
      , __inventory_available
    FROM sma_10001_cte
  ) subq_2
  INNER JOIN (
    SELECT
      metric_time__day
      , MAX(position_date__day) AS position_date__day__complete
    FROM sma_10001_cte
    GROUP BY
      metric_time__day
  ) subq_4
  ON
    subq_2.position_date__day = subq_4.position_date__day__complete
  GROUP BY
    subq_2.metric_time__day
) subq_8
FULL OUTER JOIN (
  SELECT
    metric_time__day
    , SUM(__stockout_hours) AS stockout_hours
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
  SUM(subq_2.__inventory_on_hand) AS inventory_on_hand
  , SUM(subq_2.__inventory_available) AS inventory_available
FROM (
  SELECT
    DATE_TRUNC('day', cast(substring(cast(snapshot_date_key as varchar), 1, 4) || '-' || substring(cast(snapshot_date_key as varchar), 5, 2) || '-' || substring(cast(snapshot_date_key as varchar), 7, 2) as date)) AS position_date__day
    , ending_on_hand_qty AS __inventory_on_hand
    , ending_available_qty AS __inventory_available
  FROM "summary"."inventory_position_daily" inventory_position_daily_src_10000
) subq_2
INNER JOIN (
  SELECT
    MAX(DATE_TRUNC('day', cast(substring(cast(snapshot_date_key as varchar), 1, 4) || '-' || substring(cast(snapshot_date_key as varchar), 5, 2) || '-' || substring(cast(snapshot_date_key as varchar), 7, 2) as date))) AS position_date__day__complete
  FROM "summary"."inventory_position_daily" inventory_position_daily_src_10000
) subq_4
ON
  subq_2.position_date__day = subq_4.position_date__day__complete
""",
        'inventory_by_store': """
SELECT
  subq_5.store__store_name AS store__store_name
  , SUM(subq_5.__inventory_on_hand) AS inventory_on_hand
FROM (
  SELECT
    subq_1.position_date__day AS position_date__day
    , store_src_10000.store_name AS store__store_name
    , subq_1.__inventory_on_hand AS __inventory_on_hand
  FROM (
    SELECT
      DATE_TRUNC('day', cast(substring(cast(snapshot_date_key as varchar), 1, 4) || '-' || substring(cast(snapshot_date_key as varchar), 5, 2) || '-' || substring(cast(snapshot_date_key as varchar), 7, 2) as date)) AS position_date__day
      , store_key AS store
      , ending_on_hand_qty AS __inventory_on_hand
    FROM "summary"."inventory_position_daily" inventory_position_daily_src_10000
  ) subq_1
  LEFT OUTER JOIN
    "finance"."dim_store" store_src_10000
  ON
    subq_1.store = store_src_10000.store_key
) subq_5
INNER JOIN (
  SELECT
    MAX(subq_1.position_date__day) AS position_date__day__complete
  FROM (
    SELECT
      DATE_TRUNC('day', cast(substring(cast(snapshot_date_key as varchar), 1, 4) || '-' || substring(cast(snapshot_date_key as varchar), 5, 2) || '-' || substring(cast(snapshot_date_key as varchar), 7, 2) as date)) AS position_date__day
      , store_key AS store
    FROM "summary"."inventory_position_daily" inventory_position_daily_src_10000
  ) subq_1
  LEFT OUTER JOIN
    "finance"."dim_store" store_src_10000
  ON
    subq_1.store = store_src_10000.store_key
) subq_7
ON
  subq_5.position_date__day = subq_7.position_date__day__complete
GROUP BY
  subq_5.store__store_name
ORDER BY inventory_on_hand DESC
LIMIT 20
""",
        'session_funnel': """
WITH sma_10003_cte AS (
  SELECT
    platform AS platform_day__platform
    , session_count AS __sessions
    , converted_sessions AS __converted_sessions
    , product_view_count AS __product_views
    , add_to_cart_count AS __add_to_carts
  FROM "summary"."sessions_daily_platform" sessions_daily_platform_src_10000
)

SELECT
  COALESCE(subq_5.platform_day__platform, subq_10.platform_day__platform) AS platform_day__platform
  , MAX(subq_5.sessions) AS sessions
  , MAX(subq_5.product_views) AS product_views
  , MAX(subq_5.add_to_carts) AS add_to_carts
  , MAX(subq_5.converted_sessions) AS converted_sessions
  , MAX(CAST(subq_10.converted_sessions AS DOUBLE) / CAST(NULLIF(subq_10.sessions, 0) AS DOUBLE)) AS session_conversion_rate
FROM (
  SELECT
    platform_day__platform
    , SUM(__sessions) AS sessions
    , SUM(__product_views) AS product_views
    , SUM(__add_to_carts) AS add_to_carts
    , SUM(__converted_sessions) AS converted_sessions
  FROM sma_10003_cte
  GROUP BY
    platform_day__platform
) subq_5
FULL OUTER JOIN (
  SELECT
    platform_day__platform
    , SUM(__converted_sessions) AS converted_sessions
    , SUM(__sessions) AS sessions
  FROM sma_10003_cte
  GROUP BY
    platform_day__platform
) subq_10
ON
  subq_5.platform_day__platform = subq_10.platform_day__platform
GROUP BY
  COALESCE(subq_5.platform_day__platform, subq_10.platform_day__platform)
ORDER BY sessions DESC
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
    DATE_TRUNC('day', cast(substring(cast(snapshot_date_key as varchar), 1, 4) || '-' || substring(cast(snapshot_date_key as varchar), 5, 2) || '-' || substring(cast(snapshot_date_key as varchar), 7, 2) as date)) AS position_date__day
    , DATE_TRUNC('day', cast(substring(cast(snapshot_date_key as varchar), 1, 4) || '-' || substring(cast(snapshot_date_key as varchar), 5, 2) || '-' || substring(cast(snapshot_date_key as varchar), 7, 2) as date)) AS metric_time__day
    , ending_on_hand_qty AS __inventory_on_hand
    , ending_available_qty AS __inventory_available
    , stockout_hour_count AS __stockout_hours
  FROM "summary"."inventory_position_daily" inventory_position_daily_src_10000
)

SELECT
  COALESCE(subq_8.metric_time__day, subq_13.metric_time__day) AS metric_time__day
  , MAX(subq_8.inventory_on_hand) AS inventory_on_hand
  , MAX(subq_8.inventory_available) AS inventory_available
  , MAX(subq_13.stockout_hours) AS stockout_hours
FROM (
  SELECT
    subq_2.metric_time__day AS metric_time__day
    , SUM(subq_2.__inventory_on_hand) AS inventory_on_hand
    , SUM(subq_2.__inventory_available) AS inventory_available
  FROM (
    SELECT
      position_date__day
      , metric_time__day
      , __inventory_on_hand
      , __inventory_available
    FROM sma_10001_cte
  ) subq_2
  INNER JOIN (
    SELECT
      metric_time__day
      , MAX(position_date__day) AS position_date__day__complete
    FROM sma_10001_cte
    GROUP BY
      metric_time__day
  ) subq_4
  ON
    subq_2.position_date__day = subq_4.position_date__day__complete
  GROUP BY
    subq_2.metric_time__day
) subq_8
FULL OUTER JOIN (
  SELECT
    metric_time__day
    , SUM(__stockout_hours) AS stockout_hours
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
  SUM(subq_2.__inventory_on_hand) AS inventory_on_hand
  , SUM(subq_2.__inventory_available) AS inventory_available
FROM (
  SELECT
    DATE_TRUNC('day', cast(substring(cast(snapshot_date_key as varchar), 1, 4) || '-' || substring(cast(snapshot_date_key as varchar), 5, 2) || '-' || substring(cast(snapshot_date_key as varchar), 7, 2) as date)) AS position_date__day
    , ending_on_hand_qty AS __inventory_on_hand
    , ending_available_qty AS __inventory_available
  FROM "summary"."inventory_position_daily" inventory_position_daily_src_10000
) subq_2
INNER JOIN (
  SELECT
    MAX(DATE_TRUNC('day', cast(substring(cast(snapshot_date_key as varchar), 1, 4) || '-' || substring(cast(snapshot_date_key as varchar), 5, 2) || '-' || substring(cast(snapshot_date_key as varchar), 7, 2) as date))) AS position_date__day__complete
  FROM "summary"."inventory_position_daily" inventory_position_daily_src_10000
) subq_4
ON
  subq_2.position_date__day = subq_4.position_date__day__complete
""",
        'inventory_by_store': """
SELECT
  subq_5.store__store_name AS store__store_name
  , SUM(subq_5.__inventory_on_hand) AS inventory_on_hand
FROM (
  SELECT
    subq_1.position_date__day AS position_date__day
    , store_src_10000.store_name AS store__store_name
    , subq_1.__inventory_on_hand AS __inventory_on_hand
  FROM (
    SELECT
      DATE_TRUNC('day', cast(substring(cast(snapshot_date_key as varchar), 1, 4) || '-' || substring(cast(snapshot_date_key as varchar), 5, 2) || '-' || substring(cast(snapshot_date_key as varchar), 7, 2) as date)) AS position_date__day
      , store_key AS store
      , ending_on_hand_qty AS __inventory_on_hand
    FROM "summary"."inventory_position_daily" inventory_position_daily_src_10000
  ) subq_1
  LEFT OUTER JOIN
    "finance"."dim_store" store_src_10000
  ON
    subq_1.store = store_src_10000.store_key
) subq_5
INNER JOIN (
  SELECT
    MAX(subq_1.position_date__day) AS position_date__day__complete
  FROM (
    SELECT
      DATE_TRUNC('day', cast(substring(cast(snapshot_date_key as varchar), 1, 4) || '-' || substring(cast(snapshot_date_key as varchar), 5, 2) || '-' || substring(cast(snapshot_date_key as varchar), 7, 2) as date)) AS position_date__day
      , store_key AS store
    FROM "summary"."inventory_position_daily" inventory_position_daily_src_10000
  ) subq_1
  LEFT OUTER JOIN
    "finance"."dim_store" store_src_10000
  ON
    subq_1.store = store_src_10000.store_key
) subq_7
ON
  subq_5.position_date__day = subq_7.position_date__day__complete
GROUP BY
  subq_5.store__store_name
ORDER BY inventory_on_hand DESC
LIMIT 20
""",
        'session_funnel': """
WITH sma_10003_cte AS (
  SELECT
    platform AS platform_day__platform
    , session_count AS __sessions
    , converted_sessions AS __converted_sessions
    , product_view_count AS __product_views
    , add_to_cart_count AS __add_to_carts
  FROM "summary"."sessions_daily_platform" sessions_daily_platform_src_10000
)

SELECT
  COALESCE(subq_5.platform_day__platform, subq_10.platform_day__platform) AS platform_day__platform
  , MAX(subq_5.sessions) AS sessions
  , MAX(subq_5.product_views) AS product_views
  , MAX(subq_5.add_to_carts) AS add_to_carts
  , MAX(subq_5.converted_sessions) AS converted_sessions
  , MAX(CAST(subq_10.converted_sessions AS DOUBLE PRECISION) / CAST(NULLIF(subq_10.sessions, 0) AS DOUBLE PRECISION)) AS session_conversion_rate
FROM (
  SELECT
    platform_day__platform
    , SUM(__sessions) AS sessions
    , SUM(__product_views) AS product_views
    , SUM(__add_to_carts) AS add_to_carts
    , SUM(__converted_sessions) AS converted_sessions
  FROM sma_10003_cte
  GROUP BY
    platform_day__platform
) subq_5
FULL OUTER JOIN (
  SELECT
    platform_day__platform
    , SUM(__converted_sessions) AS converted_sessions
    , SUM(__sessions) AS sessions
  FROM sma_10003_cte
  GROUP BY
    platform_day__platform
) subq_10
ON
  subq_5.platform_day__platform = subq_10.platform_day__platform
GROUP BY
  COALESCE(subq_5.platform_day__platform, subq_10.platform_day__platform)
ORDER BY sessions DESC
""",
    },
}
