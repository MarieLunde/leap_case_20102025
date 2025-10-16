{{ config(materialized="view") }}

SELECT
    CAST(CLASS AS STRING)                AS class,
    CAST(COLOR AS STRING)                AS color,
    CAST(CURRENTSTOCK AS NUMBER)         AS current_stock,
    CAST(DAYSTOMANUFACTURE AS NUMBER)    AS days_to_manufacture,
    CAST(DEALERPRICE AS FLOAT)           AS dealer_price,
    CAST(DESCRIPTION AS STRING)          AS description,
    TRY_TO_DATE(STARTDATE, 'YYYY-MM-DD') AS start_date,
    TRY_TO_DATE(ENDDATE, 'YYYY-MM-DD')   AS end_date,
    CAST(LISTPRICE AS FLOAT)             AS list_price,
    CAST(MODELNAME AS STRING)            AS model_name,
    CAST(PRODUCTKEY AS NUMBER)           AS product_key,
    CAST(PRODUCTLINE AS STRING)          AS product_line,
    CAST(PRODUCTNAME AS STRING)          AS product_name,
    CAST(PRODUCTSUBCATEGORYKEY AS NUMBER) AS product_subcategory_key,
    CAST(SAFETYSTOCKLEVEL AS NUMBER)     AS safety_stock_level,
    CAST(SIZERANGE AS STRING)            AS size_range,
    CAST(STANDARDCOST AS FLOAT)          AS standard_cost,
    CAST(STATUS AS STRING)               AS status,
    CAST(WEIGHT AS FLOAT)                AS weight

FROM {{ source('bike_sales', '11_PRODUCTS') }}