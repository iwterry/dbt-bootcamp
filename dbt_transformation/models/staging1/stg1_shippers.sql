SELECT
  id AS shipper_id,
  company AS shipper_company_name,
  last_name AS shipper_last_name,
  first_name AS shipper_first_name,
  email_address AS shipper_email,
  job_title AS shipper_job,
  mobile_phone AS shipper_phone,
  address AS shipper_address,
  city AS shipper_city,
  zip_postal_code AS shipper_postal_code,
  country_region AS shipper_country,
  web_page AS shipper_web_page,
  partition_timestamp
FROM {{ source('dl_northwind', 'shippers') }}
