
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(
    materialized='table' 
) }}


WITH source_country_data AS (
    SELECT 
        country_name,
        country_code,
        region,
        sub_region,
        languages,
        continents,
        official_name,
        population,
        area
    FROM {{ source('staging_country_data', 'country_data') }}
)

SELECT DISTINCT
    country_name,
        country_code,
        region,
        sub_region,
        languages,
        continents,
        official_name,
        population,
        area
FROM source_country_data


/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
