WITH invalid_discontinued_status AS (
  SELECT
    product_id,
    discontinued
  FROM {{ ref('stg1_products') }}
  WHERE discontinued NOT IN (0, 1)
)
SELECT *
FROM invalid_discontinued_status