{{ config(materialized='table') }}

-- Base: latest valid snapshot records
WITH current_customer AS (
    SELECT
        row_number() OVER (ORDER BY customer_id) AS sk_customer,  -- surrogate key
        customer_id,
        customer_name,
        geography_id,
        gender,
        marital_status,
        occupation,
        age,
        birth_date,
        is_house_owner,
        number_cars_owned,
        number_children_at_home,
        address_line_1,
        address_line_2,
        phone_number,
        date_first_purchase,
        current_status
    FROM {{ ref('customers_snapshot') }}
    WHERE dbt_valid_to IS NULL
),

-- Join to Geography dimension using surrogate keys
customer_with_geo AS (
    SELECT
        c.sk_customer,
        c.customer_id,
        c.customer_name,
        COALESCE(g.sk_geography, -1) AS sk_geography,              -- Surrogate key from geography dimension
        c.gender,
        c.marital_status,
        c.occupation,
        c.age,
        c.birth_date,
        c.is_house_owner,
        c.number_cars_owned,
        c.number_children_at_home,
        c.address_line_1,
        c.address_line_2,
        c.phone_number,
        c.date_first_purchase,
        c.current_status
    FROM current_customer c
    LEFT JOIN {{ ref('dim_geography') }} g
        ON c.geography_id = g.geography_id
)

-- Add 'Unknown' row using macro
SELECT * FROM (
    {{ add_unknown_row(
        model_relation="customer_with_geo",
        unknown_key_name="sk_customer",
        unknown_values=[
            "-1 AS sk_customer",
            "-1 AS customer_id",
            "'Unknown Customer' AS customer_name",
            "-1 AS sk_geography",
            "'Unknown' AS gender",
            "'Unknown' AS marital_status",
            "'Unknown' AS occupation",
            "-1 AS age",
            "NULL AS birth_date",
            "false AS is_house_owner",
            "0 AS number_cars_owned",
            "0 AS number_children_at_home",
            "'Unknown' AS address_line_1",
            "'Unknown' AS address_line_2",
            "'Unknown' AS phone_number",
            "NULL AS date_first_purchase",
            "'Unknown' AS current_status"
        ]
    ) }}
)
