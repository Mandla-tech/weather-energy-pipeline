
-- Raw landing tables, the ELT pattern, raw data stored intact first

CREATE TABLE IF NOT EXISTS raw_weather (
    id               SERIAL PRIMARY KEY,
    ingested_at      TIMESTAMP DEFAULT NOW(),
    city             TEXT,
    country          TEXT,
    weather_date     DATE,
    temp_celsius     NUMERIC(5,2),
    feels_like       NUMERIC(5,2),
    humidity_pct     INTEGER,
    wind_speed_ms    NUMERIC(6,2),
    weather_main     TEXT,
    weather_desc     TEXT,
    precipitation_mm NUMERIC(6,2),
    raw_payload      JSONB,
    -- Unique constraint enables ON CONFLICT upsert (idempotency)
    CONSTRAINT uq_weather_date_city UNIQUE (weather_date, city)
);

CREATE TABLE IF NOT EXISTS raw_energy (
    id           SERIAL PRIMARY KEY,
    ingested_at  TIMESTAMP DEFAULT NOW(),
    energy_date  DATE,
    series_name  TEXT,
    price_value  NUMERIC(12,4),
    price_unit   TEXT,
    source_api   TEXT,
    raw_payload  JSONB,
    -- Unique constraint enables ON CONFLICT upsert (idempotency)
    CONSTRAINT uq_energy_date_series UNIQUE (energy_date, series_name)
);


-- Pipeline health — captures every validation failure
-- This table feeds the 'Pipeline Alerts' panel in Tableau


CREATE TABLE IF NOT EXISTS pipeline_alerts (
    id            SERIAL PRIMARY KEY,
    alerted_at    TIMESTAMP DEFAULT NOW(),
    source        TEXT,
    error_message TEXT,
    raw_response  TEXT
);

-- Indexes for fast joins and dashboard queries
CREATE INDEX IF NOT EXISTS idx_weather_date ON raw_weather(weather_date);
CREATE INDEX IF NOT EXISTS idx_energy_date  ON raw_energy(energy_date);
CREATE INDEX IF NOT EXISTS idx_alerts_at    ON pipeline_alerts(alerted_at);
