#!/usr/bin/env python3
"""
Automated NiFi Flow Provisioner via NiFi REST API
Creates and configures the 5-city real-time weather polling pipelines in Apache NiFi:
Cities:
  1. Jammu     (lat: 32.7266, lon: 74.857)
  2. Delhi     (lat: 28.6139, lon: 77.2090)
  3. Mumbai    (lat: 19.0760, lon: 72.8777)
  4. London    (lat: 51.5074, lon: -0.1278)
  5. New York  (lat: 40.7128, lon: -74.0060)

Each pipeline chain:
  GenerateFlowFile (60s) -> UpdateAttribute -> InvokeHTTP (Open-Meteo) -> ExtractText -> InvokeHTTP (ClickHouse POST)
"""

import os
import sys
import time
import requests

NIFI_BASE_URL = os.environ.get("NIFI_URL", "http://localhost:8080/nifi-api")
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = os.environ.get("CLICKHOUSE_PORT", "8123")

CITIES = [
    {"city": "Jammu", "latitude": "32.7266", "longitude": "74.8570"},
    {"city": "Delhi", "latitude": "28.6139", "longitude": "77.2090"},
    {"city": "Mumbai", "latitude": "19.0760", "longitude": "72.8777"},
    {"city": "London", "latitude": "51.5074", "longitude": "-0.1278"},
    {"city": "New York", "latitude": "40.7128", "longitude": "-74.0060"},
]

def wait_for_nifi(max_retries=60, delay=5):
    print(f"Waiting for NiFi to become responsive at {NIFI_BASE_URL}...")
    for i in range(max_retries):
        try:
            res = requests.get(f"{NIFI_BASE_URL}/flow/about", timeout=5)
            if res.status_code == 200:
                print("NiFi is online and ready!")
                return True
        except Exception:
            pass
        time.sleep(delay)
        print(f"Still waiting for NiFi... ({i+1}/{max_retries})")
    print("NiFi connection timed out.")
    return False

def get_root_process_group_id():
    res = requests.get(f"{NIFI_BASE_URL}/process-groups/root")
    res.raise_for_status()
    return res.json()["id"]

def create_processor(pg_id, proc_type, name, x, y, config):
    payload = {
        "revision": {"version": 0},
        "component": {
            "type": proc_type,
            "name": name,
            "position": {"x": x, "y": y},
            "config": config
        }
    }
    res = requests.post(f"{NIFI_BASE_URL}/process-groups/{pg_id}/processors", json=payload)
    res.raise_for_status()
    return res.json()

def create_connection(pg_id, source_id, dest_id, relationships, name=""):
    payload = {
        "revision": {"version": 0},
        "component": {
            "name": name,
            "source": {"id": source_id, "type": "PROCESSOR", "groupId": pg_id},
            "destination": {"id": dest_id, "type": "PROCESSOR", "groupId": pg_id},
            "selectedRelationships": relationships
        }
    }
    res = requests.post(f"{NIFI_BASE_URL}/process-groups/{pg_id}/connections", json=payload)
    res.raise_for_status()
    return res.json()

def build_city_pipeline(pg_id, city_info, y_offset):
    city = city_info["city"]
    lat = city_info["latitude"]
    lon = city_info["longitude"]

    print(f"Creating pipeline chain for {city} (lat: {lat}, lon: {lon})...")

    # 1. GenerateFlowFile (Timer-driven 60s)
    gen_proc = create_processor(
        pg_id,
        "org.apache.nifi.processors.standard.GenerateFlowFile",
        f"GenerateFlowFile - {city}",
        100, y_offset,
        {
            "schedulingStrategy": "TIMER_DRIVEN",
            "schedulingPeriod": "60 sec",
            "properties": {
                "File Size": "0B",
                "Batch Size": "1",
                "Data Format": "Text",
                "Unique FlowFiles": "false"
            }
        }
    )
    gen_id = gen_proc["id"]

    # 2. UpdateAttribute (Set city, lat, lon)
    update_proc = create_processor(
        pg_id,
        "org.apache.nifi.processors.attributes.UpdateAttribute",
        f"UpdateAttribute - {city}",
        450, y_offset,
        {
            "properties": {
                "city": city,
                "latitude": lat,
                "longitude": lon
            }
        }
    )
    update_id = update_proc["id"]
    create_connection(pg_id, gen_id, update_id, ["success"])

    # 3. InvokeHTTP (Open-Meteo GET)
    meteo_url = (
        "https://api.open-meteo.com/v1/forecast"
        f"?latitude={lat}&longitude={lon}"
        "&current=temperature_2m,relative_humidity_2m,apparent_temperature,precipitation,weather_code,wind_speed_10m"
        "&timezone=Asia%2FKolkata"
    )
    http_get_proc = create_processor(
        pg_id,
        "org.apache.nifi.processors.standard.InvokeHTTP",
        f"InvokeHTTP GET Open-Meteo - {city}",
        800, y_offset,
        {
            "properties": {
                "HTTP Method": "GET",
                "Remote URL": meteo_url,
                "Content-Type": "application/json"
            },
            "autoTerminatedRelationships": ["Retry", "No Retry", "Failure", "Original"]
        }
    )
    get_id = http_get_proc["id"]
    create_connection(pg_id, update_id, get_id, ["success"])

    # 4. ExtractText (Extract raw JSON into raw_response_text)
    extract_proc = create_processor(
        pg_id,
        "org.apache.nifi.processors.standard.ExtractText",
        f"ExtractText - {city}",
        1150, y_offset,
        {
            "properties": {
                "raw_response_text": r"(?s)(.*)",
                "Character Set": "UTF-8"
            },
            "autoTerminatedRelationships": ["unmatched"]
        }
    )
    extract_id = extract_proc["id"]
    create_connection(pg_id, get_id, extract_id, ["Response"])

    # 5. InvokeHTTP (POST to ClickHouse HTTP interface with parameterized query)
    ch_query_url = (
        f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/?query="
        "INSERT%20INTO%20bronze.weather_raw_events%20SELECT%20"
        "%7Bcity:String%7D,%20%7Blatitude:Float64%7D,%20%7Blongitude:Float64%7D,%20now(),%20%7Braw_response:String%7D"
        "&param_city=${city:urlEncode()}"
        "&param_latitude=${latitude:urlEncode()}"
        "&param_longitude=${longitude:urlEncode()}"
        "&param_raw_response=${raw_response_text:urlEncode()}"
    )
    http_post_proc = create_processor(
        pg_id,
        "org.apache.nifi.processors.standard.InvokeHTTP",
        f"InvokeHTTP POST ClickHouse - {city}",
        1500, y_offset,
        {
            "properties": {
                "HTTP Method": "POST",
                "Remote URL": ch_query_url,
                "Content-Type": "text/plain"
            },
            "autoTerminatedRelationships": ["Response", "Retry", "No Retry", "Failure", "Original"]
        }
    )
    post_id = http_post_proc["id"]
    create_connection(pg_id, extract_id, post_id, ["matched"])

def start_all_processors(pg_id):
    print("Starting all processors in NiFi process group...")
    res = requests.put(
        f"{NIFI_BASE_URL}/flow/process-groups/{pg_id}",
        json={"id": pg_id, "state": "RUNNING"}
    )
    if res.status_code in [200, 202]:
        print("All NiFi processors successfully started!")
    else:
        print(f"Warning: Failed to start processors: {res.text}")

def main():
    if not wait_for_nifi():
        sys.exit(1)
    root_id = get_root_process_group_id()
    print(f"Root process group ID: {root_id}")

    # Create dedicated sub-process group for Mini ELT Weather
    sub_pg = requests.post(
        f"{NIFI_BASE_URL}/process-groups/{root_id}/process-groups",
        json={
            "revision": {"version": 0},
            "component": {
                "name": "Mini-ELT-Weather-Ingestion",
                "position": {"x": 300, "y": 100}
            }
        }
    ).json()
    elt_pg_id = sub_pg["id"]
    print(f"Created process group 'Mini-ELT-Weather-Ingestion' ID: {elt_pg_id}")

    y_pos = 100
    for city_info in CITIES:
        build_city_pipeline(elt_pg_id, city_info, y_pos)
        y_pos += 220

    start_all_processors(elt_pg_id)
    print("\n✅ NiFi Weather Ingestion Flow successfully configured and running!")

if __name__ == "__main__":
    main()
