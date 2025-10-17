{{ config(materialized='table') }}

-- Base: latest valid snapshot records
WITH current_product AS (
    SELECT
        row_number() OVER (ORDER BY product_id) AS sk_product,  -- surrogate key
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
    FROM {{ ref('product_snapshot') }}
    WHERE dbt_valid_to IS NULL
)

-- Add 'Unknown' row using macro
SELECT * FROM (
    {{ add_unknown_row(
        model_relation="current_product",
        unknown_key_name="product_id",
        unknown_values=[
            "-1 AS sk_product",
            "-1 AS product_id",
            "-1 AS product_subcategory_id",
            "'Unknown Product' AS product_name",
            "0.0 AS standard_cost",
            "'Unknown' AS color",
            "0 AS safety_stock_level",
            "0.0 AS list_price",
            "'Unknown' AS size_range",
            "0.0 AS weight",
            "NULL AS days_to_manufacture",
            "'Unknown' AS product_line",
            "0.0 AS dealer_price",
            "'Unknown' AS class",
            "'Unknown Model' AS model_name",
            "'Unknown Description' AS description",
            "NULL AS start_date",
            "NULL AS end_date",
            "'Unknown' AS status",
            "0 AS current_stock"
        ]
    ) }}
)
