{{
  config(
    materialized='incremental',
    database='dbt-masterclass-483907',
    schema='materializations',
    alias='customers_merge',
    tags='materializations',
    incremental_strategy='merge',
    partition_by={
      "field":"partition_timestamp",
      "data_type":"TIMESTAMP",
      "granularity":"DAY"
    }
  )
}}

SELECT
  id,
  company,
  last_name,
  first_name,
  job_title,
  partition_timestamp,
  CURRENT_TIMESTAMP() AS insertion_timestamp
FROM {{ source('dl_northwind', 'customer_new') }}
{% if is_incremental() %}
WHERE DATE(partition_timestamp) >= {{ get_max_partition() }}
{% endif %}