{{ config(materialized='table') }}

-- Base: latest valid snapshot records
WITH current_customer AS (
    SELECT
        row_number() OVER (ORDER BY customer_id) AS sk_customer,  -- surrogate key
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
        geography_key,
        address_line_1,
        address_line_2,
        phone_number,
        date_first_purchase,
        current_status
    FROM {{ ref('customers_snapshot') }}
    WHERE dbt_valid_to IS NULL
)

-- Add 'Unknown' row using macro
SELECT * FROM (
    {{ add_unknown_row(
        model_relation="current_customer",
        unknown_key_name="customer_id",
        unknown_values=[
            "-1 AS sk_customer",
            "-1 AS customer_id",
            "'Unknown Customer' AS customer_name",
            "'Unknown' AS gender",
            "'Unknown' AS marital_status",
            "'Unknown' AS occupation",
            "-1 AS age",
            "NULL AS birth_date",
            "false AS is_house_owner",
            "0 AS number_cars_owned",
            "0 AS number_children_at_home",
            "-1 AS geography_key",
            "'Unknown' AS address_line_1",
            "'Unknown' AS address_line_2",
            "'Unknown' AS phone_number",
            "NULL AS date_first_purchase",
            "'Unknown' AS current_status"
        ]
    ) }}
)
