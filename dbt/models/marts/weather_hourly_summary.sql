{{ config(
    materialized='table',
    engine='SummingMergeTree',
    order_by=['city', 'observed_hour']
) }}

select
    city,
    toStartOfHour(observed_at) as observed_hour,
    avg(temperature_c) as avg_temperature_c,
    min(temperature_c) as min_temperature_c,
    max(temperature_c) as max_temperature_c,
    avg(humidity_pct) as avg_humidity_pct,
    avg(wind_speed_kmh) as avg_wind_speed_kmh,
    count() as reading_count
from {{ ref('stg_weather_readings') }}
group by city, observed_hour
