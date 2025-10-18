{% snapshot customers_snapshot %}

{{
    config(
        target_database="BIKE_SALES_CASE",
        target_schema='SNAPSHOTS',
        unique_key='customer_id',
        strategy='check',
        check_cols=[
            'customer_name',
            'gender',
            'marital_status',
            'occupation',
            'age',
            'birth_date',
            'is_house_owner',
            'number_cars_owned',
            'number_children_at_home',
            'geography_id',
            'address_line_1',
            'address_line_2',
            'phone_number',
            'date_first_purchase',
            'current_status'
        ]
    )
}}

SELECT
    customer_id,
    customer_name,
    gender,
    marital_status,
    occupation,
    age,
    birth_date,
    is_house_owner,
    number_cars_owned,
    number_children_at_home,
    geography_id,
    address_line_1,
    address_line_2,
    phone_number,
    date_first_purchase,
    current_status
FROM {{ ref('stg_customers') }}

{% endsnapshot %}
