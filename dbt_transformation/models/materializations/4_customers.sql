{{
  config(
    materialized='materialized_view',
    database='dbt-masterclass-483907',
    schema='materializations',
    alias='customers_met_view',
    tags='materializations',
    on_configuration_change='apply',
    enable_refresh=True,
    refresh_interval_minutes=30,
    max_staleness='INTERVAL 30 MINUTE'
  )
}}

SELECT
  id,
  company,
  last_name,
  first_name,
  job_title,
  partition_timestamp
--  CURRENT_TIMESTAMP() AS insertion_timestamp
FROM {{ source('dl_northwind', 'customer_new') }}