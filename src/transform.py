import pandas as pd
import numpy as np
from datetime import datetime


#Wind chill summary: https://en.wikipedia.org/wiki/Wind_chill
#TL:DR
#Wind chill attempts to model the sensation of cold produced by the wind on exposed skin 
#for a given ambient air temperature in climates where wind increases heat loss from the body.
#Appropriate when air temperature is < 10 celsius and wind >~ 5 km/h

#frostbite risk time for exposed skin based on wind chill. https://www.weather.gov/bou/windchill

#Snowfall approximated with precipitation and increased fluff for cold temps.

def wind_chill(air_temperature_celsius: np.ndarray, wind_speed_kmh: np.ndarray) -> np.ndarray:
    v_pow = wind_speed_kmh ** 0.16
    wind_chill_calc = (
        13.12
        +0.6215*air_temperature_celsius
        -11.37*v_pow
        +0.3965 *air_temperature_celsius*v_pow
    )
    
    mask = (air_temperature_celsius <= 10) & (wind_speed_kmh > 4.8)
    return np.where(mask, np.round(wind_chill_calc, 1), air_temperature_celsius)

def calculate_frostbite_risk(wind_chill: np.ndarray) -> pd.Series:
    conditions = [
        (wind_chill >= -10),
        (wind_chill < -10) & (wind_chill >= -25),
        (wind_chill < -25) & (wind_chill >= -35),
        (wind_chill < -35) & (wind_chill >= -48),
        (wind_chill < -48)
    ]

    choices = [
        ">30 minutes (low risk)",
        "10-30 minutes",
        "5-10 minutes",
        "5 minutes or less",
        "<5 minutes"
    ]
    return np.select(conditions, choices, default="No risk")

def estimate_snowfall(precip: np.ndarray, temp: np.ndarray) -> np.ndarray:
    snow_mask = (temp <= 2.0) & (precip > 0)
    ratio = np.where(temp < -10, 15.0, 10.0)
    snowfall_cm = precip * ratio / 10.0
    return np.where(snow_mask, np.round(snowfall_cm, 1), 0.0) 


def transform_data(raw_rows: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(raw_rows)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc = True)
    #missing vales
    df = df.replace({None: np.nan})

    df["wind_chill"] = wind_chill(df["temperature"].values, df["wind_speed_10m"].values)
    df["frostbite_risk"] = calculate_frostbite_risk(df["wind_chill"].values)
    df["snowfall_cm"] = estimate_snowfall(df["precipitation"].values, df["temperature"].values)
    return df
