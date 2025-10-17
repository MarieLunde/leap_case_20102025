{{ config(materialized='incremental', unique_key='sale_id') }}

SELECT
    -- Keys
    row_number() OVER (ORDER BY sale_id) AS sk_sales,  -- surrogate key
    sale_id,
    sales_order_number,
    customer_id,
    product_id,
    sales_territory_id,
    order_date_id,

    -- numbers
    order_quantity,
    unit_price,
    unit_price_discount_pct,
    discount_amount,
    extended_amount,
    product_standard_cost,
    total_product_cost,
    freight_cost,
    tax_amount,
    sales_amount

FROM {{ ref('stg_sales') }}
