{{ config(materialized="table") }}

SELECT
    -- Primary keys and foreign keys
    sale_id,
    sales_order_number,
    customer_id,
    product_id,
    sales_territory_id,
    order_date_id,

    -- Dates and timestamps
    order_date,
    order_year,
    order_timestamp,

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
