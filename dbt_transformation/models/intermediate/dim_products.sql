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

WITH products AS (
  SELECT
    p.product_id,
    p.product_code,
    p.product_name,
    p.product_desc,
    CAST(sid AS INT64) AS supplier_id,
    p.discontinued,
    p.product_category,
    p.standard_cost,
    p.partition_timestamp
  FROM {{ ref('stg1_products')  }} AS p,
    UNNEST(SPLIT(p.supplier_ids, ';')) AS sid
    {% if is_incremental() %}
    WHERE DATE(p.partition_timestamp) >= DATE(_dbt_max_partition)
    {% endif %}
),
suppliers AS (
  SELECT
    supplier_id,
    supplier_company_name,
    ROW_NUMBER() OVER(PARTITION BY supplier_id ORDER BY partition_timestamp ASC) AS row_num
  FROM {{ ref('stg1_suppliers') }}
)
SELECT
  p.product_id,
  p.product_code,
  p.product_name,
  p.product_desc,
  STRING_AGG(s.supplier_company_name) AS supplier_company_name,
  p.discontinued,
  p.product_category,
  p.standard_cost,
  p.partition_timestamp,
  CURRENT_TIMESTAMP() AS insertion_timestamp
FROM products AS p
JOIN suppliers AS s
ON p.supplier_id = s.supplier_id AND s.row_num = 1
GROUP BY 
  p.product_id,
  p.product_code,
  p.product_name,
  p.product_desc,
  p.discontinued,
  p.product_category,
  p.standard_cost,
  p.partition_timestamp