{{ config(materialized='table') }}

-- Base: latest valid snapshot records
WITH current_geography AS (
    SELECT
        geography_id,
        city,
        state_province_name,
        state_province_code,
        postal_code,
        customer_country,
        country_region_code,
        ip_address_locator
    FROM {{ ref('geography_snapshot') }}
    WHERE dbt_valid_to IS NULL
)

-- Add 'Unknown' row using macro
SELECT * FROM (
    {{ add_unknown_row(
        model_relation="current_geography",
        unknown_key_name="geography_id",
        unknown_values=[
            "-1 AS geography_id",
            "'Unknown' AS city",
            "'Unknown' AS state_province_name",
            "'Unknown' AS state_province_code",
            "'Unknown' AS postal_code",
            "'Unknown' AS customer_country",
            "'Unknown' AS country_region_code",
            "'Unknown' AS ip_address_locator"
        ]
    ) }}
)
