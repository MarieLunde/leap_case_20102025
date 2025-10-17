{{ config(materialized='view') }}

SELECT
    CAST(PRODUCTSUBCATEGORYKEY AS NUMBER)   AS product_subcategory_id,
    CAST(PRODUCTCATEGORYKEY AS NUMBER)      AS product_category_id, 
    CAST(PRODUCT_SUBCATEGORY AS STRING)     AS product_subcategory_name,
    CAST(FR_PRODUCT_SUBCATEGORY AS STRING)  AS product_subcategory_name_fr,
    CAST(SP_PRODUCT_SUBCATEGORY AS STRING)  AS product_subcategory_name_sp

FROM {{ source('bike_sales', "5_PRODUCTSUBCATEGORY") }}
