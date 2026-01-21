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

SELECT
  od.sales_id,
  od.order_id,
  od.product_id,
  o.customer_id,
  o.employee_id,
  o.shipper_id,
  od.quantity,
  od.unit_price,
  od.discount,
  od.date_allocated,
  od.partition_timestamp,
  CURRENT_TIMESTAMP() AS insertion_timestamp
FROM {{ ref('stg1_order_details') }} AS od
JOIN {{ ref('stg1_orders') }} AS o
ON od.order_id = o.order_id
{% if is_incremental() %}
WHERE DATE(od.partition_timestamp) >= DATE(_dbt_max_partition)
{% endif %}