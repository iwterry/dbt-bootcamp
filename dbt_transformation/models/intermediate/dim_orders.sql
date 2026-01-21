{{
  config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={
      "field": "partition_timestamp",
      "data_type": "TIMESTAMP",
      "granularity": "DAY"
    }
  )
}}

WITH orders AS (
  SELECT
    order_id,
    order_date,
    shipped_date,
    paid_date,
    status_id,
    partition_timestamp  
  FROM {{ ref('stg1_orders') }}
  {% if is_incremental() %}
  WHERE DATE(partition_timestamp) >= DATE(_dbt_max_partition)
  {% endif %}
)
SELECT
  o.order_id,
  o.order_date,
  o.shipped_date,
  o.paid_date,
  os.order_status,
  o.partition_timestamp,
  CURRENT_TIMESTAMP() AS insertion_timestamp
FROM orders AS o
JOIN {{ ref('stg1_order_status') }} AS os
ON o.status_id = os.status_id