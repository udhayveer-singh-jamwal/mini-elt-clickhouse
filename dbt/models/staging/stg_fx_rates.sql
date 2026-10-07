{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree(ingested_at)',
    order_by=['base_currency', 'quote_currency', 'rate_date'],
    incremental_strategy='append'
) }}

select
    base_currency,
    kv.1 as quote_currency,
    kv.2 as rate,
    JSONExtractString(raw_response, 'date')::Date as rate_date,
    ingested_at
from {{ source('bronze', 'fx_raw_events') }}
array join JSONExtractKeysAndValues(raw_response, 'rates', 'Float64') as kv
{% if is_incremental() %}
where ingested_at > (select max(ingested_at) from {{ this }})
{% endif %}
