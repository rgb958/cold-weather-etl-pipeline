import streamlit as st
import pandas as pd
import plotly.express as px
import psycopg2
from dotenv import load_dotenv
import os


load_dotenv()

def get_conn():
    return psycopg2.connect(
        host = os.getenv("POSTGRES_HOST", "localhost"),
        port = int(os.getenv("POSTGRES_PORT", "5433")),
        dbname = os.getenv("POSTGRES_DB"),
        user = os.getenv("POSTGRES_USER"),
        password = os.getenv("POSTGRES_PASSWORD")
    )
st.set_page_config(page_title="Cold Weather Dashboard", layout="wide")
st.title("Cold Weather Dashboard")

@st.cache_data(ttl=3600)
def load_data():
    conn = get_conn()
    query = """
    SELECT 
        l.city,
        rw.timestamp AT TIME ZONE 'UTC' AS timestamp,
        rw.temperature,
        rw.wind_speed_10m,
        rw.precipitation,
        COALESCE(d.wind_chill, rw.temperature) AS wind_chill,
        COALESCE(d.frostbite_risk, 'No Risk') AS frostbite_risk,
        COALESCE(d.snowfall_cm, 0.0) AS snowfall_cm
        
    FROM raw_weather rw
    JOIN locations l ON rw.location_id = l.id
    LEFT JOIN derived_metrics d ON d.raw_weather_id = rw.id
    ORDER BY rw.timestamp DESC
    """
    df = pd.read_sql(query, conn)
    conn.close()
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df

df = load_data()

st.sidebar.header("Filters")
cities = sorted(df["city"].unique())
selected_city = st.sidebar.selectbox("City", cities, index=0)

date_range = st.sidebar.date_input(
    "Date range",
    value = (df["timestamp"].dt.date.min(), df["timestamp"].dt.date.max())
)

mask =  (df["city"] == selected_city) & \
        (df["timestamp"].dt.date >= date_range[0]) & \
        (df["timestamp"].dt.date <= date_range[1])
df_filtered = df[mask].sort_values("timestamp")


st.header(f"Current conditions in {selected_city}")
if df_filtered.empty:
    st.warning("No data for selected filters.")
    st.stop()

latest = df_filtered.iloc[-1]
risk = latest.get('frostbite_risk', 'No risk')
risk_color = (
    "inverse" if "extreme" in risk.lower() or "<5" in risk else
    "inverse" if "5 minutes or less" in risk else
    "normal"
)

col1, col2, col3, col4, col5, col6 = st.columns(6)
col1.metric("Temperature", f"{latest['temperature']:.1f} °C")
col2.metric("Wind Chill", f"{latest['wind_chill']:.1f} °C")
col3.metric("Frostbite Risk", risk, delta_color=risk_color)
col4.metric("Wind Speed", f"{latest['wind_speed_10m']:.1f} km/h")
col5.metric("Precipitation", f"{latest['precipitation']:.1f} mm")
col6.metric("Estimated Snowfall", f"{latest['snowfall_cm']:.1f} cm")


st.subheader("Temperature and Wind Chill")
fig_temp = px.line(df_filtered, x = "timestamp", y = ["temperature", "wind_chill"],
                    title="Temperature vs Wind Chill",
                    color_discrete_sequence=["#00BFFF", "#1E90FF"])
st.plotly_chart(fig_temp, width='content')

st.subheader("Frostbite Risk Over Time")
fig_risk = px.scatter(df_filtered, x="timestamp", y="wind_chill", color="frostbite_risk",
                        title="Frostbite Risk Categories",
                        color_discrete_map={
                            "No_risk":"#90EE90",
                            ">30 minutes (low risk)": "#98FB98",
                            "10-30 minutes": "#FFD700",
                            "5-10 minutes": "#FFA500",
                            "5 minutes or less": "#FF4500",
                            "<5 minutes (extreme danger)": "#DC143C"
                        })
st.plotly_chart(fig_risk, width='content')

st.subheader("Estimated Snowfall")
fig_snow = px.bar(df_filtered.set_index("timestamp")["snowfall_cm"].resample("D").sum().reset_index(),
                    x = "timestamp", y = "snowfall_cm", title = "Daily Estimated Snowfall (cm)")
st.plotly_chart(fig_snow, width='content')
st.caption("Data from Open-Meteo")

st.subheader("Wind Speed")
fig_wind = px.bar(
    df_filtered.set_index("timestamp")["wind_speed_10m"].resample("D").mean().reset_index(),
    x = "timestamp",
    y = "wind_speed_10m",
    title = f"Daily Average Wind Speed in {selected_city}"
)
st.plotly_chart(fig_wind, width='content')
st.caption("Data from Open-Meteo")

