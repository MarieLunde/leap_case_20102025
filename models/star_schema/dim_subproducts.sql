{{ config(materialized='table') }}

WITH current_subcategory AS (
    SELECT *
    FROM {{ ref('subproducts_snapshot') }}
    WHERE dbt_valid_to IS NULL
),
sk_subcategory AS (
    SELECT
        row_number() OVER (ORDER BY product_subcategory_id, product_category_id) AS sk_subproduct,  -- surrogate key
        product_subcategory_id,
        product_category_id,
        product_subcategory_name,
        product_subcategory_name_fr,
        product_subcategory_name_sp
    FROM current_subcategory
)

{{ add_unknown_row(
    model_relation="sk_subcategory",
    unknown_key_name="sk_subproduct",
    unknown_values=[
        "-1 AS sk_subproduct",
        "-1 AS product_subcategory_id",
        "-1 AS product_category_id",
        "'Unknown' AS product_subcategory_name",
        "'Unknown' AS product_subcategory_name_fr",
        "'Unknown' AS product_subcategory_name_sp"
    ]
) }}
