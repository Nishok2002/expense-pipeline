{{ config(materialized='view') }}

with src as (
  select
    id,
    txn_date,
    merchant,
    description,
    amount,
    category,
    src_file,
    load_ts
  from {{ source('raw','transactions') }}
)

select
  id,
  txn_date,
  coalesce(merchant, 'Unknown') as merchant,
  description,
  amount,
  category,
  src_file,
  load_ts
from src
