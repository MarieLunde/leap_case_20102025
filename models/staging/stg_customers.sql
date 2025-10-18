{{ config(materialized="view") }}

select
    cast(customerkey as number) as customer_id,
    cast(name as string) as customer_name,
    cast(gender as string) as gender,
    cast(maritalstatus as string) as marital_status,
    cast(occupation as string) as occupation,

    -- Demographics
    cast(age as number) as age,
    try_to_date(birthdate, 'YYYY-MM-DD') as birth_date,
    cast(houseownerflag as boolean) as is_house_owner,
    cast(numbercarsowned as number) as number_cars_owned,
    try_to_number(numberchildrenathome) as number_children_at_home,
    -- try_to_number(yearlyincome) as yearly_income, Contains only null values

    -- Geography and contact
    cast(geographykey as number) as geography_id,
    cast(addressline1 as string) as address_line_1,
    cast(addressline2 as string) as address_line_2,
    cast(phone as string) as phone_number,

    -- Customer behavior and status
    try_to_date(datefirstpurchase, 'YYYY-MM-DD') as date_first_purchase,
    cast(currentstatus as string) as current_status

from {{ source('bike_sales', "2_CUSTOMERS") }}
