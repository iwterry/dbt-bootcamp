SELECT
  customer_id,
  customer_company_name,
  customer_last_name,
  customer_first_name,
  customer_email,
  customer_job,
  customer_phone,
  customer_address,
  customer_city,
  customer_postal_code,
  customer_country,
  partition_timestamp,
  CURRENT_TIMESTAMP() AS insertion_timestamp
FROM {{ ref('stg1_customers') }}