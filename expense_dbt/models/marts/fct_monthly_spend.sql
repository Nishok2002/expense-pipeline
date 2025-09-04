{{ config(materialized='table') }}

with base as (
  select
    date_trunc('month', txn_date) as month,
    case when amount >= 0 then 'Income' else coalesce(category,'Other') end as category_norm,
    amount
  from {{ ref('stg_transactions') }}
)

select
  month::date,
  category_norm as category,
  round(sum(amount),2) as total_amount
from base
group by 1,2
order by 1,2
