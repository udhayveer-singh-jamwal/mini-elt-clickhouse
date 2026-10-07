{{ config(
    materialized='table',
    engine='ReplacingMergeTree',
    order_by=['base_currency', 'quote_currency', 'rate_date']
) }}

with ranked as (
    select
        base_currency,
        quote_currency,
        rate_date,
        rate,
        if(row_number() over (partition by base_currency, quote_currency order by rate_date) = 1,
           NULL,
           lagInFrame(rate) over (partition by base_currency, quote_currency order by rate_date)
        ) as prev_day_rate
    from {{ ref('stg_fx_rates') }}
)
select
    base_currency,
    quote_currency,
    rate_date,
    rate,
    prev_day_rate,
    case
        when prev_day_rate is null or prev_day_rate = 0 then NULL
        else round((rate - prev_day_rate) / prev_day_rate * 100, 4)
    end as day_over_day_pct
from ranked
