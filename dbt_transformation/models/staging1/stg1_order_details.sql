SELECT
  id AS sales_id,
  order_id,
  product_id,
  quantity,
  unit_price,
  discount,
  date_allocated,
  partition_timestamp
FROM {{ source('dl_northwind', 'order_details') }}