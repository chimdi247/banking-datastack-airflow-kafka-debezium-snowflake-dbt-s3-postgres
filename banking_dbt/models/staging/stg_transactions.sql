{{ config(materialized='view') }}

-- De-duplicated on transaction_id: the CDC pipeline is at-least-once, and a duplicate key
-- in the source of fact_transactions' incremental MERGE would make the MERGE fail.
select
    v:id::string                 as transaction_id,
    v:account_id::string         as account_id,
    v:amount::float              as amount,
    v:txn_type::string           as transaction_type,
    v:related_account_id::string as related_account_id,
    v:status::string             as status,
    v:created_at::timestamp      as transaction_time,
    current_timestamp            as load_timestamp
from {{ source('raw', 'transactions') }}
qualify row_number() over (
    partition by v:id::string
    order by v:_ts_ms::number desc nulls last, v:created_at::timestamp desc
) = 1
