{{ config(materialized="view") }}

select
    cast(geographykey as number) as geography_id,
    cast(city as string) as city,
    cast(stateprovincecode as string) as state_province_code,
    cast(stateprovincename as string) as state_province_name,
    cast(postalcode as string) as postal_code,
    cast(countryregioncode as string) as country_region_code,
    cast(customer_country as string) as customer_country,
    cast(ipaddresslocator as string) as ip_address_locator

from {{ source('bike_sales', "3_GEOGRAPHY") }}
