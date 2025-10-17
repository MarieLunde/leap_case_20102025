{{ config(materialized="view") }}

SELECT
    CAST(PRODUCTKEY AS NUMBER)             AS product_id,
    CAST(PRODUCTSUBCATEGORYKEY AS NUMBER)  AS product_subcategory_id,
    CAST(PRODUCTNAME AS STRING)            AS product_name,
    CAST(STANDARDCOST AS FLOAT)            AS standard_cost,
    CAST(COLOR AS STRING)                  AS color,
    CAST(SAFETYSTOCKLEVEL AS NUMBER)       AS safety_stock_level,
    CAST(LISTPRICE AS FLOAT)               AS list_price,
    CAST(SIZERANGE AS STRING)              AS size_range,
    CAST(WEIGHT AS FLOAT)                  AS weight,
    CAST(DAYSTOMANUFACTURE AS NUMBER)      AS days_to_manufacture,
    CAST(PRODUCTLINE AS STRING)            AS product_line,
    CAST(DEALERPRICE AS FLOAT)             AS dealer_price,
    CAST(CLASS AS STRING)                  AS class,
    CAST(MODELNAME AS STRING)              AS model_name,
    CAST(DESCRIPTION AS STRING)            AS description,
    TRY_TO_DATE(STARTDATE, 'YYYY-MM-DD')   AS start_date,
    TRY_TO_DATE(ENDDATE, 'YYYY-MM-DD')     AS end_date,
    CAST(STATUS AS STRING)                 AS status,
    CAST(CURRENTSTOCK AS NUMBER)           AS current_stock

FROM {{ source('bike_sales', '11_PRODUCTS') }}
