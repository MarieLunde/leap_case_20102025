-- Fail if sales references a non-existent customer
select fs.customer_id
from {{ ref('fact_sales') }} fs
left join {{ ref('dim_customers') }} dc
  on fs.customer_id = dc.customer_id
where dc.customer_id is null
