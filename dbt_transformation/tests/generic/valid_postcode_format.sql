{% test valid_postcode_format(model, column_name, field) %}
  WITH invalid_postcodes AS (
    SELECT
    {{ column_name }},
    {{ field }}
  FROM {{ model }}
  WHERE
    ({{ field }} = 'USA' AND NOT REGEXP_CONTAINS(CAST({{ column_name }} AS STRING), r'^\d{5}(-\d{4})?$')) OR
    ({{ field }} = 'CA' AND NOT REGEXP_CONTAINS(CAST({{ column_name }} AS STRING), r'^[A-Za-z]\d[A-Za-z] \d[A-Za-z]\d$'))
  )
  SELECT *
  FROM invalid_postcodes
{% endtest %}