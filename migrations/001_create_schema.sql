CREATE TABLE IF NOT EXISTS locations (
    id SERIAL PRIMARY KEY,
    city TEXT UNIQUE NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL
);

CREATE TABLE IF NOT EXISTS raw_weather (
    id BIGSERIAL PRIMARY KEY,
    location_id INTEGER NOT NULL REFERENCES locations(id),
    timestamp TIMESTAMPTZ NOT NULL,
    temperature REAL,
    relative_humidity REAL,
    wind_speed_10m REAL,
    precipitation REAL,
    CONSTRAINT unique_location_timestamp UNIQUE (location_id, timestamp)
);

CREATE TABLE IF NOT EXISTS derived_metrics (
    id BIGSERIAL PRIMARY KEY,
    raw_weather_id BIGINT NOT NULL UNIQUE REFERENCES raw_weather(id),
    wind_chill REAL,
    frostbite_risk TEXT,
    snowfall_cm REAL DEFAULT 0.0
);

CREATE INDEX IF NOT EXISTS idx_raw_weather_timestamp 
    ON raw_weather(timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_raw_weather_location_timestamp
    ON raw_weather(location_id, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_derived_raw_id 
    ON derived_metrics(raw_weather_id);
