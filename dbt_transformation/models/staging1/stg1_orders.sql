SELECT
  id AS order_id,
  employee_id,
  customer_id,
  shipper_id,
  order_date,
  paid_date,
  shipped_date,
  status_id,
  partition_timestamp
FROM {{ source('dl_northwind', 'orders') }}