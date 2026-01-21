SELECT
  id AS supplier_id,
  company AS supplier_company_name,
  last_name AS supplier_last_name,
  first_name AS supplier_first_name,
  email_address AS supplier_email,
  job_title AS supplier_job,
  mobile_phone AS supplier_phone,
  address AS supplier_address,
  city AS supplier_city,
  zip_postal_code AS supplier_postal_code,
  country_region AS supplier_country,
  web_page AS supplier_web_page,
  partition_timestamp
FROM {{ source('dl_northwind', 'suppliers') }}