#!/usr/bin/env python3
"""
Real-time Weather Stream Daemon (NiFi Architecture Twin)
Executes the identical polling and ClickHouse HTTP parameterized insertion logic
as defined in the NiFi flow specification:
- Polls Open-Meteo API every 60 seconds for 5 cities:
    1. Jammu    (32.7266, 74.857)
    2. Delhi    (28.6139, 77.2090)
    3. Mumbai   (19.0760, 72.8777)
    4. London   (51.5074, -0.1278)
    5. New York (40.7128, -74.0060)
- Ingests into ClickHouse bronze.weather_raw_events via HTTP parameterized query
"""

import os
import sys
import time
import urllib.parse
import urllib.request
import json
import logging

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = os.environ.get("CLICKHOUSE_PORT", "8123")
POLL_INTERVAL_SEC = int(os.environ.get("POLL_INTERVAL_SEC", "60"))

CITIES = [
    {"city": "Jammu", "latitude": 32.7266, "longitude": 74.8570},
    {"city": "Delhi", "latitude": 28.6139, "longitude": 77.2090},
    {"city": "Mumbai", "latitude": 19.0760, "longitude": 72.8777},
    {"city": "London", "latitude": 51.5074, "longitude": -0.1278},
    {"city": "New York", "latitude": 40.7128, "longitude": -74.0060},
]

def ensure_bronze_table():
    query = """
    CREATE DATABASE IF NOT EXISTS bronze;
    CREATE TABLE IF NOT EXISTS bronze.weather_raw_events (
        city String,
        latitude Float64,
        longitude Float64,
        ingested_at DateTime DEFAULT now(),
        raw_response String
    ) ENGINE = MergeTree 
    ORDER BY (city, ingested_at);
    """
    url = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/"
    req = urllib.request.Request(url, data=query.encode('utf-8'), method='POST')
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            logging.info("Verified bronze.weather_raw_events table in ClickHouse")
    except Exception as e:
        logging.warning(f"Could not initialize ClickHouse table via HTTP (daemon will retry on insert): {e}")

def fetch_and_post_city_weather(city_info):
    city = city_info["city"]
    lat = city_info["latitude"]
    lon = city_info["longitude"]

    api_url = (
        f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}"
        "&current=temperature_2m,relative_humidity_2m,apparent_temperature,precipitation,weather_code,wind_speed_10m"
        "&timezone=Asia%2FKolkata"
    )

    try:
        req = urllib.request.Request(api_url, headers={"User-Agent": "MiniELT-Weather/1.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            raw_response = resp.read().decode('utf-8')
            parsed = json.loads(raw_response)
            current = parsed.get("current", {})
            temp = current.get("temperature_2m")
            obs_time = current.get("time")
            logging.info(f"[{city}] Weather fetched: Temp={temp}°C, Time={obs_time}")

        # Construct ClickHouse HTTP parameterized query matching NiFi InvokeHTTP
        base_query = (
            "INSERT INTO bronze.weather_raw_events "
            "SELECT {city:String}, {latitude:Float64}, {longitude:Float64}, now(), {raw_response:String}"
        )
        params = {
            "query": base_query,
            "param_city": city,
            "param_latitude": str(lat),
            "param_longitude": str(lon),
            "param_raw_response": raw_response
        }
        encoded_query_string = urllib.parse.urlencode(params)
        ch_url = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/?{encoded_query_string}"

        post_req = urllib.request.Request(ch_url, data=b"", method='POST')
        with urllib.request.urlopen(post_req, timeout=10) as ch_resp:
            if ch_resp.status == 200:
                logging.info(f"[{city}] Ingested successfully into bronze.weather_raw_events")
            else:
                logging.error(f"[{city}] ClickHouse responded with status {ch_resp.status}")

    except Exception as e:
        logging.error(f"[{city}] Error fetching/ingesting weather: {e}")

def poll_all_cities_once():
    logging.info(f"Starting weather polling cycle for {len(CITIES)} cities...")
    for city_info in CITIES:
        fetch_and_post_city_weather(city_info)
    logging.info("Completed weather polling cycle.")

def run_loop():
    logging.info("Starting Weather Streaming Daemon...")
    ensure_bronze_table()
    while True:
        poll_all_cities_once()
        logging.info(f"Sleeping for {POLL_INTERVAL_SEC} seconds until next poll...")
        time.sleep(POLL_INTERVAL_SEC)

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--once":
        ensure_bronze_table()
        poll_all_cities_once()
    else:
        run_loop()
