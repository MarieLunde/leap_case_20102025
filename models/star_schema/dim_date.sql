{{ config(materialized = 'table') }}

-- Define your date range
-- Min and max dates (2021-07-31	2018-07-01)
{% set start_date = '2018-07-01' %}
{% set end_date = '2021-12-31' %}

-- Use dbt_utils' date_spine macro to generate continuous daily dates
with date_spine as (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="to_date('" ~ start_date ~ "')",
        end_date="to_date('" ~ end_date ~ "')"
    ) }}

),

date_dimension as (

    select
        row_number() OVER (ORDER BY date_day) AS sk_date,
        cast(to_char(date_day, 'YYYYMMDD') as number) as date_id,  -- numeric surrogate key
        date_day as date,
        extract(year from date_day) as year,
        extract(quarter from date_day) as quarter,
        extract(month from date_day) as month,
        to_char(date_day, 'Month') as month_name,
        extract(day from date_day) as day,
        to_char(date_day, 'DY') as day_name,
        dayofweek(date_day) as day_of_week,
        week(date_day) as week_of_year,
        to_char(date_day, 'YYYY-MM-DD') as date_string,
        case
            when dayofweek(date_day) in (6, 7) then true
            else false
        end as is_weekend
    from date_spine
)

select * from (
    {{ add_unknown_row(
        model_relation="date_dimension",
        unknown_key_name="sk_date",
        unknown_values=[
            "-1 AS sk_date",
            "-1 AS date_id",
            "NULL AS date",
            "NULL AS year",
            "NULL AS quarter",
            "NULL AS month",
            "'Unknown' AS month_name",
            "NULL AS day",
            "'Unknown' AS day_name",
            "NULL AS day_of_week",
            "NULL AS week_of_year",
            "'Unknown' AS date_string",
            "NULL AS is_weekend"
        ]
    ) }}
)
