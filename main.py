"""
Cold weather ETL Pipeline
Fetches hourly weather for given cities from Open-Meteo,
calculates relevant metrics and loads into postgres
"""

import yaml
import pandas as pd
from src.extract import extract_all
from src.transform import transform_data
from src.load import *
from psycopg2.errors import UndefinedTable

def load_locations_cfg(config_path: str = "config/locations.yaml") -> list[dict]:
    with open(config_path) as f:
        return yaml.safe_load(f) or []

def ensure_schema(conn):
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM locations LIMIT 1")
    except UndefinedTable:
        conn.rollback()
        print("applying schema")
        with conn.cursor() as cur:
            with open("migrations/001_create_schema.sql") as f:
                cur.execute(f.read())
        conn.commit()
        print("schema applied")

    
def main():
    print("...STARTING...")

    #load
    locations = load_locations_cfg()
    if not locations:
        print("No locations provided in config.yaml")
        return
        
    cities = [location["city"] for location in locations]
    print(f"Monitoring {len(cities)} cities: {', '.join(cities)}")


    print("Extracting from Open-Meteo")
    raw_data = extract_all(locations)
    print(f"Extracted {len(raw_data)} hourly records")

    if not raw_data:
        print("No data received")
        return
    
    print("Transforming data...")
    df = transform_data(raw_data)
    print(f"Transformed: {len(df)} rows")

    #db
    try:
        with get_conn() as conn:
            ensure_schema(conn)
            load_locations(conn, df)
            load_raw_weather(conn, df)
            load_derived_metrics(conn, df)
        print("ETL completed")
    except Exception as e:
        print("load failed: {e}")
        raise

if __name__ == "__main__":
    main()
