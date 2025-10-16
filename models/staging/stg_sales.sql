{{ config(materialized="view") }}

select
    cast(customerkey as number) as customer_key,
    cast(discountamount as float) as discount_amount,
    cast(extendedamount as float) as extended_amount,
    cast(freight as float) as freight_cost,

    cast(orderdate as date) as order_date,
    cast(orderdatekey as number) as order_date_key,
    cast(orderyear as number) as order_year,
    cast(ordertimestamp as timestamp_ntz) as order_timestamp,

    cast(orderquantity as number) as order_quantity,
    cast(productkey as number) as product_key,
    cast(productstandardcost as float) as product_standard_cost,
    cast(recordkey as number) as record_key,
    cast(regionmonthid as string) as region_month_id,

    cast(salesamount as float) as sales_amount,
    cast(salesorderlinenumber as number) as sales_order_line_number,
    cast(salesordernumber as string) as sales_order_number,
    cast(salesterritorykey as number) as sales_territory_key,
    cast(taxamt as float) as tax_amount,
    cast(totalproductcost as float) as total_product_cost,
    cast(unitprice as float) as unit_price,
    cast(unitpricediscountpct as float) as unit_price_discount_pct

from {{ source('bike_sales', "1_SALES") }}
