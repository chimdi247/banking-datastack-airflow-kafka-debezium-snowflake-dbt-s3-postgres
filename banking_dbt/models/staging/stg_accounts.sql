{{ config(materialized='view') }}

-- One row per account: the most recent change event wins (balance updates replace older versions).
select
    v:id::string            as account_id,
    v:customer_id::string   as customer_id,
    v:account_type::string  as account_type,
    v:balance::float        as balance,
    v:currency::string      as currency,
    v:created_at::timestamp as created_at,
    current_timestamp       as load_timestamp
from {{ source('raw', 'accounts') }}
qualify row_number() over (
    partition by v:id::string
    order by v:_ts_ms::number desc nulls last, v:created_at::timestamp desc
) = 1
