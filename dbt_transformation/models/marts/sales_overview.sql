WITH fact_sales AS (
  SELECT
    order_id,
    product_id,
    customer_id,
    employee_id,
    shipper_id,
    ROW_NUMBER() OVER(PARTITION BY sales_id ORDER BY partition_timestamp ASC) AS row_num
  FROM {{ ref('fact_sales') }}
),
dim_orders AS (
  SELECT
    order_id,
    order_date,
    shipped_date,
    paid_date,
    order_status,
    ROW_NUMBER() OVER(PARTITION BY order_id ORDER BY partition_timestamp ASC) AS row_num
  FROM {{ ref('dim_orders') }}
),
dim_products AS (
  SELECT
    product_id,
    product_name,
    product_desc,
    product_category,
    standard_cost,
    supplier_company_name,
    ROW_NUMBER() OVER(PARTITION BY product_id ORDER BY partition_timestamp ASC) AS row_num
  FROM {{ ref('dim_products') }}
),
dim_customers AS (
  SELECT
    customer_id,
    customer_first_name,
    customer_company_name
  FROM {{ ref('dim_customers') }}
),
dim_employees AS (
  SELECT
    employee_id,
    employee_first_name,
    employee_company_name,
    ROW_NUMBER() OVER(PARTITION BY employee_id ORDER BY partition_timestamp ASC) AS row_num
  FROM {{ ref('dim_employees') }}
),
dim_shippers AS (
  SELECT
    shipper_id,
    shipper_first_name,
    shipper_company_name,
    ROW_NUMBER() OVER(PARTITION BY shipper_id ORDER BY partition_timestamp ASC) AS row_num
  FROM {{ ref('dim_shippers') }}
)
SELECT
  fs.order_id,
  o.order_date,
  o.shipped_date,
  o.paid_date,
  o.order_status,
  fs.product_id,
  p.product_name,
  p.product_desc,
  p.product_category,
  p.standard_cost,
  p.supplier_company_name,
  fs.customer_id,
  c.customer_first_name,
  c.customer_company_name,
  fs.employee_id,
  e.employee_first_name,
  e.employee_company_name,
  fs.shipper_id,
  s.shipper_first_name,
  s.shipper_company_name,
  CURRENT_TIMESTAMP() AS insertion_timestamp
FROM fact_sales AS fs
  JOIN dim_orders AS o
  ON fs.order_id = o.order_id
  JOIN dim_products AS p
  ON fs.product_id = p.product_id
  JOIN dim_customers AS c
  ON fs.customer_id = c.customer_id
  JOIN dim_employees AS e
  ON fs.employee_id = e.employee_id
  JOIN dim_shippers AS s
  ON fs.shipper_id = s.shipper_id
WHERE
  fs.row_num = 1 AND
  o.row_num = 1 AND
  p.row_num = 1 AND
  e.row_num = 1 AND
  s.row_num = 1
