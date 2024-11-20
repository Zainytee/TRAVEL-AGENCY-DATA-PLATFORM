
-- Use the `ref` function to select from other models

{{ config(
    materialized='table'
) }}

SELECT 
    country_name,
    currency_code,
    currency_name,
    currency_symbol
FROM {{ source('staging_country_data', 'country_data') }}
