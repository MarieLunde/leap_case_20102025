{{ config(materialized="table") }}

-- Base: latest valid snapshot records
with
    current_geography as (
        select
            row_number() over (order by geography_id) as sk_geography,  -- surrogate key
            geography_id,
            city,
            state_province_name,
            state_province_code,
            postal_code,
            customer_country,
            country_region_code,
            ip_address_locator
        from {{ ref("geography_snapshot") }}
        where dbt_valid_to is null
    )

-- Add 'Unknown' row using macro
select *
from
    (
        {{
            add_unknown_row(
                model_relation="current_geography",
                unknown_key_name="geography_id",
                unknown_values=[
                    "-1 AS sk_geography",
                    "-1 AS geography_id",
                    "'Unknown' AS city",
                    "'Unknown' AS state_province_name",
                    "'Unknown' AS state_province_code",
                    "'Unknown' AS postal_code",
                    "'Unknown' AS customer_country",
                    "'Unknown' AS country_region_code",
                    "'Unknown' AS ip_address_locator",
                ],
            )
        }}
    )
