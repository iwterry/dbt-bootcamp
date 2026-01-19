{{
  config(
    materialized='incremental',
    database='dbt-masterclass-483907',
    schema='materializations',
    alias='customers_merge_on_schema_change',
    tags='materializations',
    incremental_strategy='merge',
    unique_key='id',
    on_schema_change='sync_all_columns',
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
  registration_date,
  job_title,
  partition_timestamp,
  CURRENT_TIMESTAMP() AS insertion_timestamp
FROM {{ source('dl_northwind', 'customer_new') }}
{% if is_incremental() %}
WHERE DATE(partition_timestamp) >= {{ get_max_partition() }}
{% endif %}