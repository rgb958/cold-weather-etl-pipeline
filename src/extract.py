import requests
import json
import hashlib
from typing import List, Dict
from datetime import datetime
from pathlib import Path


CACHE_DIR = Path("cache")
CACHE_DIR.mkdir(exist_ok=True)
CACHE_TTL_SECONDS = 3600
def _get_cache_key(lat: float, lon: float, params: dict) -> Path:
    key_str = f"{lat:.4f}_{lon:.4f}_{str(sorted(params.items()))}"
    filename = hashlib.md5(key_str.encode()).hexdigest() + ".json"
    return CACHE_DIR / filename

def _is_cache_valid(cache_file: Path) -> bool:
    if not cache_file.exists():
        return False
    age_seconds = (datetime.now() - datetime.fromtimestamp(cache_file.stat().st_mtime)).total_seconds()
    return age_seconds < CACHE_TTL_SECONDS




def fetch_weather(lat: float, lon: float, city: str) -> List[Dict]:
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation",
        "timezone": "auto",
        "forcast_days": 1,
    }

    cache_file = _get_cache_key(lat, lon, params)
    
    if _is_cache_valid(cache_file):
        print(f"Cache hit {city}")
        data = json.loads(cache_file.read_text())
    else:
        print(f"Fetching data for {city}")
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        cache_file.write_text(json.dumps(data))

    hourly = data["hourly"]
    rows = []
    for i in range(len(hourly["time"])):
        rows.append({
            "city": city,
            "latitude": lat,
            "longitude": lon,
            "timestamp": hourly["time"][i],
            "temperature": hourly["temperature_2m"][i],
            "relative_humidity": hourly["relative_humidity_2m"][i],
            "wind_speed_10m": hourly["wind_speed_10m"][i],
            "precipitation": hourly["precipitation"][i],
        })
    return rows

def extract_all(locations: List[Dict]) -> List[Dict]:
    all_rows = []
    for location in locations:
        rows = fetch_weather(location["latitude"], location["longitude"], location["city"])
        all_rows.extend(rows)
    return all_rows

