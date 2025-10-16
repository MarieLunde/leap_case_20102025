{{ config(materialized='view') }}

SELECT
    CAST(PRODUCTSUBCATEGORYKEY AS NUMBER)   AS product_subcategory_key,
    CAST(PRODUCTCATEGORYKEY AS NUMBER)      AS product_category_key,
    CAST(PRODUCT_SUBCATEGORY AS STRING)     AS product_subcategory_name,
    CAST(FR_PRODUCT_SUBCATEGORY AS STRING)  AS product_subcategory_name_fr,
    CAST(SP_PRODUCT_SUBCATEGORY AS STRING)  AS product_subcategory_name_sp

FROM {{ source('bike_sales', "5_PRODUCTSUBCATEGORY") }}
