{{ config(materialized='view') }}

-- One row per customer: the most recent change event wins (_ts_ms is the Debezium event time).
select
    v:id::string            as customer_id,
    v:first_name::string    as first_name,
    v:last_name::string     as last_name,
    v:email::string         as email,
    v:created_at::timestamp as created_at,
    current_timestamp       as load_timestamp
from {{ source('raw', 'customers') }}
qualify row_number() over (
    partition by v:id::string
    order by v:_ts_ms::number desc nulls last, v:created_at::timestamp desc
) = 1
