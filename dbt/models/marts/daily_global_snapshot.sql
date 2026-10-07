{{ config(
    materialized='table',
    engine='ReplacingMergeTree',
    order_by=['snapshot_date', 'city']
) }}

select
    toDate(w.observed_hour) as snapshot_date,
    w.city,
    avg(w.avg_temperature_c) as avg_temperature_c,
    anyIf(f_inr.rate, f_inr.quote_currency = 'INR') as usd_inr_rate,
    anyIf(f_usd.rate, f_usd.quote_currency = 'USD') as eur_usd_rate
from {{ ref('weather_hourly_summary') }} w
left join {{ ref('stg_fx_rates') }} f_inr
    on toDate(w.observed_hour) = f_inr.rate_date
   and f_inr.quote_currency = 'INR'
left join {{ ref('stg_fx_rates') }} f_usd
    on toDate(w.observed_hour) = f_usd.rate_date
   and f_usd.quote_currency = 'USD'
group by snapshot_date, w.city
