-- =====================================================================
-- ClickHouse Initialization Script for Mini ELT Project
-- Medallion Architecture: Bronze -> Silver -> Gold
-- =====================================================================

-- ---------------------------------------------------------------------
-- 1. BRONZE LAYER (Raw Ingestion)
-- ---------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS bronze;

-- Real-time Open-Meteo Weather Events (polled per city)
CREATE TABLE IF NOT EXISTS bronze.weather_raw_events (
    city String,
    latitude Float64,
    longitude Float64,
    ingested_at DateTime DEFAULT now(),
    raw_response String
) ENGINE = MergeTree 
ORDER BY (city, ingested_at);

-- Batch Frankfurter FX Events (daily base EUR)
CREATE TABLE IF NOT EXISTS bronze.fx_raw_events (
    base_currency String,
    ingested_at DateTime DEFAULT now(),
    raw_response String
) ENGINE = MergeTree 
ORDER BY (base_currency, ingested_at);

-- ---------------------------------------------------------------------
-- 2. SILVER LAYER (Parsed, Cleaned, Deduplicated)
-- ---------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS silver;

-- Weather Readings (ReplacingMergeTree dedupes by city + observed_at)
CREATE TABLE IF NOT EXISTS silver.weather_readings (
    city String,
    latitude Float64,
    longitude Float64,
    observed_at DateTime,
    temperature_c Float64,
    humidity_pct Float64,
    apparent_temperature_c Float64,
    precipitation_mm Float64,
    weather_code UInt16,
    wind_speed_kmh Float64,
    ingested_at DateTime
) ENGINE = ReplacingMergeTree(ingested_at) 
ORDER BY (city, observed_at);

-- FX Rates (ReplacingMergeTree dedupes by base + quote + date)
CREATE TABLE IF NOT EXISTS silver.fx_rates (
    base_currency String,
    quote_currency String,
    rate Float64,
    rate_date Date,
    ingested_at DateTime
) ENGINE = ReplacingMergeTree(ingested_at) 
ORDER BY (base_currency, quote_currency, rate_date);

-- ---------------------------------------------------------------------
-- 3. GOLD LAYER (Aggregations and Business Marts)
-- ---------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS gold;

-- Weather Hourly Summary (SummingMergeTree for fast hourly rollups)
CREATE TABLE IF NOT EXISTS gold.weather_hourly_summary (
    city String,
    observed_hour DateTime,
    avg_temperature_c Float64,
    min_temperature_c Float64,
    max_temperature_c Float64,
    avg_humidity_pct Float64,
    avg_wind_speed_kmh Float64,
    reading_count UInt32
) ENGINE = SummingMergeTree 
ORDER BY (city, observed_hour);

-- FX Daily Summary with Day-over-Day percentage shift
CREATE TABLE IF NOT EXISTS gold.fx_daily_summary (
    base_currency String,
    quote_currency String,
    rate_date Date,
    rate Float64,
    prev_day_rate Nullable(Float64),
    day_over_day_pct Nullable(Float64)
) ENGINE = ReplacingMergeTree 
ORDER BY (base_currency, quote_currency, rate_date);

-- Cross-source Snapshot joining weather & currency markets by date
CREATE TABLE IF NOT EXISTS gold.daily_global_snapshot (
    snapshot_date Date,
    city String,
    avg_temperature_c Float64,
    usd_inr_rate Nullable(Float64),
    eur_usd_rate Nullable(Float64)
) ENGINE = ReplacingMergeTree 
ORDER BY (snapshot_date, city);
