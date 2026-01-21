SELECT
  id AS employee_id,
  company AS employee_company_name,
  last_name AS employee_last_name,
  first_name AS employee_first_name,
  email_address AS employee_email,
  job_title AS employee_job,
  mobile_phone AS employee_phone,
  address AS employee_address,
  city AS employee_city,
  zip_postal_code AS employee_postal_code,
  country_region AS employee_country,
  partition_timestamp
FROM {{ source('dl_northwind', 'employees') }}
