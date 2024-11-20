{{ config(
    materialized='table'
) }}

SELECT 
    dim_countries.country_name,
    dim_countries.region,
    dim_countries.sub_region,
    SUM(dim_countries.population) AS total_population,
    COUNT(*) AS country_count,
    MAX(dim_countries.area) AS total_area  -- Using MAX because there should only be one area per country
FROM {{ ref('dim_countries') }} AS dim_countries
GROUP BY dim_countries.country_name, dim_countries.region, dim_countries.sub_region
