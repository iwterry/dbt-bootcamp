{{
  config(
    materialized='table',
    database='dbt-masterclass-483907',
    schema='materializations',
    alias='customers_table',
    tags='materializations'
  )
}}

SELECT
  id,
  company,
  last_name,
  first_name,
  job_title,
  partition_timestamp,
  insertion_timestamp
FROM {{ ref('2_customers') }}