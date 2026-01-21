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
  shipper_id,
  shipper_company_name,
  shipper_last_name,
  shipper_first_name,
  shipper_email,
  shipper_job,
  shipper_phone,
  shipper_address,
  shipper_city,
  shipper_postal_code,
  shipper_country,
  partition_timestamp,
  CURRENT_TIMESTAMP() AS insertion_timestamp
FROM {{ ref('stg1_shippers') }}
{% if is_incremental() %}
WHERE DATE(partition_timestamp) >= DATE(_dbt_max_partition)
{% endif %}