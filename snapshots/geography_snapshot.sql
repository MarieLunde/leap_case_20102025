{% snapshot geography_snapshot %}

{{
    config(
        target_schema='SNAPSHOTS',     
        unique_key='geography_id',
        strategy='check',              
        check_cols=[
            'city',
            'state_province_name',
            'state_province_code',
            'postal_code',
            'customer_country',
            'country_region_code',
            'ip_address_locator'
        ]
    )
}}

SELECT
    geography_id,
    city,
    state_province_name,
    state_province_code,
    postal_code,
    customer_country,
    country_region_code,
    ip_address_locator
FROM {{ ref('stg_geography') }}

{% endsnapshot %}
