{{
  config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={
      "field": "partition_timestamp",
      "data_type": "TIMESTAMP",
      "granularity": "DAY"
    }
  )
}}

SELECT
  employee_id,
  employee_company_name,
  employee_last_name,
  employee_first_name,
  employee_email,
  employee_job,
  employee_phone,
  employee_address,
  employee_city,
  employee_postal_code,
  employee_country,
  partition_timestamp,
  CURRENT_TIMESTAMP() AS insertion_timestamp
FROM {{ ref('stg1_employees') }}
{% if is_incremental() %}
WHERE DATE(partition_timestamp) >= DATE(_dbt_max_partition)
{% endif %}