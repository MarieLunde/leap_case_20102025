{% snapshot product_snapshot %}
    {{
        config(
            target_database="PC_DBT_DB",
            target_schema="SNAPSHOTS",
            unique_key="product_id",
            strategy="check",
            check_cols=[
                "product_name",
                "standard_cost",
                "list_price",
                "dealer_price",
                "color",
                "weight",
                "status",
                "end_date",
            ],
        )
    }}

    select
        product_id,
        product_subcategory_id,
        product_name,
        standard_cost,
        color,
        safety_stock_level,
        list_price,
        size_range,
        weight,
        days_to_manufacture,
        product_line,
        dealer_price,
        class,
        model_name,
        description,
        start_date,
        end_date,
        status,
        current_stock
    from {{ ref("stg_products") }}

{% endsnapshot %}
