{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree(ingested_at)',
    order_by=['city', 'observed_at'],
    incremental_strategy='append'
) }}

select
    city,
    latitude,
    longitude,
    parseDateTimeBestEffort(JSONExtractString(raw_response, 'current', 'time')) as observed_at,
    JSONExtractFloat(raw_response, 'current', 'temperature_2m') as temperature_c,
    JSONExtractFloat(raw_response, 'current', 'relative_humidity_2m') as humidity_pct,
    JSONExtractFloat(raw_response, 'current', 'apparent_temperature') as apparent_temperature_c,
    JSONExtractFloat(raw_response, 'current', 'precipitation') as precipitation_mm,
    JSONExtractUInt(raw_response, 'current', 'weather_code') as weather_code,
    JSONExtractFloat(raw_response, 'current', 'wind_speed_10m') as wind_speed_kmh,
    ingested_at
from {{ source('bronze', 'weather_raw_events') }}
where JSONHas(raw_response, 'current', 'time')
  and JSONHas(raw_response, 'current', 'temperature_2m')
{% if is_incremental() %}
  and ingested_at > (select max(ingested_at) from {{ this }})
{% endif %}
