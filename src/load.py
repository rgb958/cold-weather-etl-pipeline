import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv
from psycopg2.extras import execute_values
from typing import List

load_dotenv()

def get_db_url() -> str:
    return (
        f"postgresql://"
        f"{os.getenv('POSTGRES_USER')}:"
        f"{os.getenv('POSTGRES_PASSWORD')}@"
        f"{os.getenv('POSTGRES_HOST', 'localhost')}:"
        f"{os.getenv('POSTGRES_PORT', '5433')}/"
        f"{os.getenv('POSTGRES_DB')}"
    )

def get_conn():
    db_url = get_db_url()
    return psycopg2.connect(db_url)

def load_locations(conn, df: pd.DataFrame):
    locations = df[["city","latitude","longitude"]].drop_duplicates().round({'latitude': 4,'longitude': 4})
    insert_sql = """
    INSERT INTO locations (city, latitude, longitude)
    VALUES %s
    ON CONFLICT (city) DO NOTHING
    """

    values = [tuple(row) for row in locations.itertuples(index=False, name=None)]
    with conn.cursor() as cur:
        execute_values(cur, insert_sql, list(values))
    conn.commit()
    print(f"Loaded {len(values)} locations")

def load_raw_weather(conn, df: pd.DataFrame):
    #city -> location_id
    with conn.cursor() as cur:
        cur.execute("SELECT city, id FROM locations")
        city_to_id = {row[0]: row[1] for row in cur.fetchall()}
    
    df = df.copy()
    df["location_id"] = df["city"].map(city_to_id)

    if df["location_id"].isna().any():
        print("warning: some cities not found in locations table (probably because load_locations didnt run first)")
        df = df.dropna(subset=["locations_id"])

    insert_sql = """
    INSERT INTO raw_weather
        (location_id, timestamp, temperature, relative_humidity, wind_speed_10m, precipitation)
    VALUES %s
    ON CONFLICT (location_id, timestamp) DO NOTHING
    """

    values = df[[
        "location_id", "timestamp", "temperature",
        "relative_humidity", "wind_speed_10m", "precipitation"
    ]].itertuples(index=False, name=None)

    with conn.cursor() as cur:
        execute_values(cur, insert_sql, list(values))
    conn.commit()
    print(f"Loaded {len(df)} raw weather rows")

def load_derived_metrics(conn, df: pd.DataFrame):
    required_cols = ["wind_chill","frostbite_risk","snowfall_cm","city","timestamp"]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        print(f"missing cols: {missing}")
        return
    
    df_metrics = df[required_cols].dropna(subset=["wind_chill"])
    
    if df_metrics.empty:
        print("No derived metrics to load")
        return

    insert_sql = """
    INSERT INTO derived_metrics (raw_weather_id, wind_chill, frostbite_risk, snowfall_cm)
    SELECT rw.id, %s, %s, %s
    FROM raw_weather rw
    JOIN locations l ON rw.location_id = l.id
    WHERE l.city = %s AND rw.timestamp = %s
    ON CONFLICT (raw_weather_id) DO NOTHING
    """

    values = df_metrics.itertuples(index = False, name = None)
    
    count = 0
    with conn.cursor() as cur:
        for wind_chill, frostbite_risk, snowfall_cm, city, ts in values:
            cur.execute(insert_sql, (wind_chill, frostbite_risk, snowfall_cm, city, ts))
            if cur.rowcount > 0:
                count += 1
    conn.commit()
    print(f"loaded derived metrics for {count} rows (out of: {len(df_metrics)} attempted)")
    