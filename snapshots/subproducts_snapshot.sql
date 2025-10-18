{% snapshot subproducts_snapshot %}

{{
    config(
        target_database="BIKE_SALES_CASE",
        target_schema='SNAPSHOTS',
        unique_key=['product_subcategory_id', 'product_category_id'],
        strategy='check',
        check_cols=[
            'product_subcategory_name',
            'product_subcategory_name_fr',
            'product_subcategory_name_sp'
        ]
    )
}}

SELECT
    product_subcategory_id,
    product_category_id,
    product_subcategory_name,
    product_subcategory_name_fr,
    product_subcategory_name_sp
FROM {{ ref('stg_subproducts') }}

{% endsnapshot %}
