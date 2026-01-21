SELECT
  id AS customer_id,
  company AS customer_company_name,
  last_name AS customer_last_name,
  first_name AS customer_first_name,
  email_address AS customer_email,
  job_title AS customer_job,
  mobile_phone AS customer_phone,
  address AS customer_address,
  city AS customer_city,
  zip_postal_code AS customer_postal_code,
  country_region AS customer_country,
  partition_timestamp,
  CURRENT_TIMESTAMP() AS insertion_timestamp
FROM {{ ref('customers_snapshot') }}
WHERE dbt_valid_to IS NULL