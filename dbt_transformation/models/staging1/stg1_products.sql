SELECT
  id AS product_id,
  supplier_ids,
  product_code,
  product_name,
  description AS product_desc,
  category AS product_category,
  discontinued,
  standard_cost,
  partition_timestamp
FROM {{ source('dl_northwind', 'products') }}