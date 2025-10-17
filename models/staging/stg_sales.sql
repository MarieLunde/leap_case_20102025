{{ config(materialized="view") }}

select
    cast(customerkey as number) as customer_id,
    cast(discountamount as float) as discount_amount,
    cast(extendedamount as float) as extended_amount,
    cast(freight as float) as freight_cost,

    cast(orderdate as date) as order_date,
    cast(orderdatekey as number) as order_date_id,
    cast(orderyear as number) as order_year,
    cast(ordertimestamp as timestamp_ntz) as order_timestamp,

    cast(orderquantity as number) as order_quantity,
    cast(productkey as number) as product_id,
    cast(productstandardcost as float) as product_standard_cost,
    cast(recordkey as number) as sale_id,

    cast(salesamount as float) as sales_amount,
    cast(salesordernumber as string) as sales_order_number,
    cast(salesterritorykey as number) as sales_territory_id,
    cast(taxamt as float) as tax_amount,
    cast(totalproductcost as float) as total_product_cost,
    cast(unitprice as float) as unit_price,
    cast(unitpricediscountpct as float) as unit_price_discount_pct

from {{ source('bike_sales', "1_SALES") }}
