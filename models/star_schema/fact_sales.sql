{{ config(
    materialized='incremental',
    unique_key='sale_id',
    incremental_strategy='merge'
) }}

WITH source AS (
    SELECT
        sale_id,
        sales_order_number,
        customer_id,
        product_id,
        sales_territory_id,
        order_date_id,
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
),

joined AS (
    SELECT
        -- Surrogate Key for fact table
        ROW_NUMBER() OVER (ORDER BY s.sale_id) AS sk_sales,

        -- Business keys
        s.sale_id,
        s.sales_order_number,

        -- Foreign keys (from dimension surrogate keys)
        COALESCE(dc.sk_customer, -1) AS sk_customer,
        COALESCE(dp.sk_product, -1) AS sk_product,
        COALESCE(dt.sk_date, -1) AS sk_order_date,

        -- Measures
        s.order_quantity,
        s.unit_price,
        s.unit_price_discount_pct,
        s.discount_amount,
        s.extended_amount,
        s.product_standard_cost,
        s.total_product_cost,
        s.freight_cost,
        s.tax_amount,
        s.sales_amount,

        -- Derived measures
        (s.sales_amount - s.discount_amount) AS net_sales_amount,
        (s.sales_amount - s.total_product_cost) AS gross_profit,
        CASE 
            WHEN s.sales_amount > 0 THEN 
                ((s.sales_amount - s.total_product_cost) / s.sales_amount) * 100 
            ELSE NULL 
        END AS profit_margin_pct,
        CASE 
            WHEN s.sales_amount > 0 THEN 
                (s.tax_amount / s.sales_amount) * 100 
            ELSE NULL 
        END AS tax_pct,
        (s.total_product_cost + s.freight_cost + s.tax_amount) AS total_cost_with_freight_tax,
        CASE 
            WHEN s.order_quantity > 0 THEN 
                (s.sales_amount / s.order_quantity)
            ELSE NULL 
        END AS revenue_per_unit

    FROM source s
    LEFT JOIN {{ ref('dim_customers') }} dc
        ON s.customer_id = dc.customer_id
    LEFT JOIN {{ ref('dim_products') }} dp
        ON s.product_id = dp.product_id
    LEFT JOIN {{ ref('dim_date') }} dt
        ON s.order_date_id = dt.date_id
)

SELECT *
FROM joined

{% if is_incremental() %}
-- Only insert new records that are not already in the target table
WHERE sale_id NOT IN (SELECT sale_id FROM {{ this }})
{% endif %}
