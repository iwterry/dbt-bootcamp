{% snapshot customers_snapshot %}

{{
  config(
    target_schema='snapshots',
    unique_key='id',
    strategy='timestamp',
    updated_at='partition_timestamp'
  )
}}

SELECT *
FROM {{ source('dl_northwind', 'customer') }}

{% endsnapshot %}