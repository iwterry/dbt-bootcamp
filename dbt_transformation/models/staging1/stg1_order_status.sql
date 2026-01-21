SELECT
  id AS status_id,
  status_name AS order_status
FROM {{ source('dl_northwind', 'orders_status') }}