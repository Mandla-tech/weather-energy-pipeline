# Johannesburg Weather vs Ethereum Energy Prices
### Automated Daily Data Engineering Pipeline

[![Pipeline Status](https://img.shields.io/badge/Pipeline-Active-brightgreen)]()
[![Airflow](https://img.shields.io/badge/Orchestration-Airflow%202.9-017CEE)]()
[![dbt](https://img.shields.io/badge/Transform-dbt%201.11-FF694B)]()
[![PostgreSQL](https://img.shields.io/badge/Database-PostgreSQL%2015-336791)]()
[![Docker](https://img.shields.io/badge/Infrastructure-Docker-2496ED)]()
[![Tableau](https://img.shields.io/badge/Dashboard-Tableau%20Public-E97627)]()

---

##  Author

** Mandla Moyo**  
Data Engineer  
🔗 LinkedIn: https://www.linkedin.com/in/mandla-m/
🐙 GitHub: https://www.github.com/Mandla-tech
📊 Dashboard: [Live Tableau Public Dashboard](https://public.tableau.com/app/profile/mandla.moyo/viz/JohannesburgWeatherVsEthereumEnergyPrices/Dashboard1)

---

## The Business Problem

Energy procurement decisions are weather-sensitive. Businesses that
consume large amounts of electricity, such as data centres, manufacturers,
and mining operations make purchasing and hedging decisions based
on energy price forecasts where weather is a primary input variable.

**The manual process this pipeline replaces:**
A data analyst downloads a weather report every morning and opens a
spreadsheet, manually enters temperature and rainfall data then pulls
energy prices from a separate source, pastes them together, and
builds a chart. This takes 30–45 minutes per day and introduces
human error at every step.

**What this pipeline does instead:**
Every morning at 08h00 South African time, an automated pipeline pulls live
Johannesburg weather data and Ethereum energy commodity prices,
validates them against a data contract and stores the raw payload in
a file lake, loads clean data into PostgreSQL, transforms it using
dbt, and keeps a Tableau dashboard current and automatically.

> **Business impact:** Eliminated 30–45 minutes of daily manual
> data collection. Removed human error from the ingestion process.
> Guaranteed data freshness through automated scheduling.
> Built a reliable audit trail through raw payload preservation.

---

## Architecture
````
┌─────────────────────────────────────────────────────────────┐
│                     DATA SOURCES                            │
│                                                             │
│  OpenWeatherMap API    Open-Meteo API    CoinGecko API      │
│  (current conditions)  (precipitation)  (ETH price)        │
└──────────────┬─────────────────┬──────────────┬────────────┘
│                 │              │
└────────┬────────┘              │
↓                       ↓
┌─────────────────────────────────────────────────────────────┐
│                   EXTRACT & VALIDATE                        │
│                                                             │
│         weather_extractor.py    energy_extractor.py        │
│                                                             │
│         ✓ Data contract validation before DB write         │
│         ✓ Halt pipeline on bad data                        │
│         ✓ Log failures to pipeline_alerts table            │
└──────────────────────────┬──────────────────────────────────┘
↓
┌─────────────────────────────────────────────────────────────┐
│                    FILE LAKE (local)                        │
│                                                             │
│         data/lake/YYYY-MM-DD/weather_openweathermap.json   │
│         data/lake/YYYY-MM-DD/weather_open_meteo.json       │
│         data/lake/YYYY-MM-DD/energy_coingecko.json         │
│                                                             │
│         Raw JSON preserved intact before any processing    │
│         Production equivalent: AWS S3 / Google GCS         │
└──────────────────────────┬──────────────────────────────────┘
↓
┌─────────────────────────────────────────────────────────────┐
│                 WAREHOUSE (PostgreSQL 15)                   │
│                                                             │
│         raw_weather    raw_energy    pipeline_alerts        │
│                                                             │
│         ✓ Idempotent upserts — ON CONFLICT DO UPDATE       │
│         ✓ raw_payload JSONB — full API response stored     │
│         ✓ ELT pattern — transform after load               │
└──────────────────────────┬──────────────────────────────────┘
↓
┌─────────────────────────────────────────────────────────────┐
│                  ORCHESTRATION (Airflow)                    │
│                                                             │
│         DAG: weather_energy_pipeline                       │
│         Schedule: 0 6 * * * (08:00 SAST daily)            │
│                                                             │
│         extract_weather ──→ extract_energy                 │
│                                                             │
│         ✓ Retry logic — 2 retries, 5 min delay            │
│         ✓ Task dependency enforcement                      │
│         ✓ Full run history and log visibility              │
└──────────────────────────┬──────────────────────────────────┘
↓
┌─────────────────────────────────────────────────────────────┐
│               TRANSFORMATION (dbt Core)                    │
│                                                             │
│    raw_weather ──→ stg_weather ──┐                         │
│                                  ├──→ mart_weather_energy  │
│    raw_energy  ──→ stg_energy  ──┘                         │
│                                                             │
│         ✓ 11 automated data quality tests                  │
│         ✓ rainy_day flag (precipitation ≥ 1mm)             │
│         ✓ rainfall_category dimension                      │
│         ✓ Day-over-day price change calculations           │
└──────────────────────────┬──────────────────────────────────┘
↓
┌─────────────────────────────────────────────────────────────┐
│                 VISUALISATION (Tableau Public)              │
│                                                             │
│         Chart 1: ETH Price vs Rainfall over time           │
│         Chart 2: Avg ETH Price by Rainfall Category        │
│         Chart 3: Pipeline Health & Daily Summary           │
│                                                             
│  🔗 Live Dashboard → https://public.tableau.com/app/profile/mandla.moyo/viz/JohannesburgWeatherVsEthereumEnergyPrices/Dashboard1?publish=yes                 │
└─────────────────────────────────────────────────────────────┘
````

---

## 🛠️ Tech Stack

| Layer | Tool | Version | Purpose |
|---|---|---|---|
| Language | Python | 3.12 | Extraction, validation, loading |
| Orchestration | Apache Airflow | 2.9.2 | Scheduling, retry logic, monitoring |
| Database | PostgreSQL | 15 | Raw and transformed data storage |
| Transformation | dbt Core | 1.11 | Versioned, tested SQL models |
| Containerisation | Docker + Compose | Latest | Reproducible local infrastructure |
| Visualisation | Tableau Public | Latest | Interactive dashboard |
| Version Control | Git + GitHub | — | Code versioning, portfolio hosting |

**All tools are free and open source.**

---

## 📊 Data Sources

| Source | Data | Key Fields | Cost |
|---|---|---|---|
| OpenWeatherMap API | Current conditions | temp, humidity, wind, weather_main | Free tier |
| Open-Meteo API | Daily precipitation | precipitation_mm | Free, no key |
| CoinGecko API | Ethereum price | price_usd | Free, no key |

**Why Ethereum as an energy proxy?**
The Ethereum network's computing infrastructure consumes electricity
as its primary operational cost. When global electricity prices rise,
the cost of running Ethereum infrastructure rises with it, making
ETH price a freely available, real-time signal of global energy
economics. In a production environment with data subscriptions, this
feed would be replaced with a direct energy price API. The pipeline
architecture supports swapping the data source without changing any
other component.

---

## Key Engineering Decisions

### ELT over ETL
Raw data is loaded to PostgreSQL before transformation. The
`raw_payload JSON` column preserves the complete original API
response. If transformation logic changes, data can be reprocessed
from the raw stored payload without having to call the API again.
This guarantees data integrity at every stage.

### Idempotency
Every database write uses `ON CONFLICT DO UPDATE`. Running the
pipeline twice on the same day produces exactly one row not two.
This is a reliability pattern that makes pipelines
safe to retry without data corruption.


### File Lake Pattern
Raw JSON is saved to `data/lake/YYYY-MM-DD/` before loading to
the database. This simulates the AWS S3 / GCS landing zone pattern
used in production cloud ELT architectures. Raw data is always
preserved at its original landing point.

### Infrastructure as Code
The entire local stack of PostgreSQL, Airflow webserver, Airflow
scheduler is defined in `docker-compose.yml`. This command
reproduces the complete environment on any machine:
```bash
docker compose up -d
```

### dbt for Transformations
SQL transformations are versioned, tested, and documented using
dbt Core. 11 automated data quality tests run after every model
build. The mart layer adds business logic, a `rainy_day` flag,
`rainfall_category`, and day to day price change calculations together with
on top of clean staging models.

---

## 📁 Project Structure
````
weather-energy-pipeline/
│
├── docker-compose.yml          # Full stack infrastructure definition
├── requirements.txt            # Pinned Python dependencies
├── .env                        # Secrets, not committed
├── .gitignore                 
│
├── init_db/
│   └── 01_create_tables.sql    # Schema runs automatically on first boot
│
├── extractors/
│   ├── db_loader.py            # Database layer
│   ├── weather_extractor.py    # OpenWeatherMap + Open-Meteo pipeline
│   └── energy_extractor.py     # CoinGecko Ethereum price pipeline
│
├── dags/
│   └── weather_energy_pipeline.py  # Airflow DAG — schedule and task graph
│
├── dbt/
│   └── weather_energy/
│       ├── models/
│       │   ├── staging/
│       │   │   ├── stg_weather.sql     # Cleaned weather model
│       │   │   ├── stg_energy.sql      # Cleaned energy model
│       │   │   └── schema.yml          # Tests and documentation
│       │   └── marts/
│       │       ├── mart_weather_energy.sql  # Joined analytical mart
│       │       └── schema.yml               # Tests and documentation
│       └── dbt_project.yml
│
├── scripts/
│   └── backfill_historical.py  # One-time historical data loader
│
└── data/
└── lake/
└── YYYY-MM-DD/         # Raw JSON landing zone by date
├── weather_openweathermap.json
├── weather_open_meteo.json
└── energy_coingecko.json
````

---

## How to Run Locally

### Prerequisites
- Docker Desktop (with WSL2 integration if on Windows)
- Python 3.12+
- Git

### Step 1 — Clone the repository
```bash
git clone https://github.com/[YOUR GITHUB USERNAME HERE]/weather-energy-pipeline.git
cd weather-energy-pipeline
```

### Step 2 — Create your environment file
```bash
cp .env.example .env
# Edit .env and add your API keys
nano .env
```

### Step 3 — Set up the Python environment
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Step 4 — Start the infrastructure
```bash
# Fix folder permissions for Airflow
sudo chown -R 50000:0 logs/
mkdir -p logs/scheduler

# Start all services
docker compose up -d
```

### Step 5 — Access the Airflow UI
URL:      http://localhost:8080
Username: admin
Password: admin

Trigger the `weather_energy_pipeline` DAG manually for the first run.

##
# Step 6 — Run dbt transformations
```bash
cd dbt/weather_energy
dbt run
dbt test
```

### Step 7 — Verify data in PostgreSQL
```bash
docker exec -it pipeline_postgres psql \
  -U pipeline_user \
  -d weather_energy \
  -c "SELECT * FROM analytics.mart_weather_energy ORDER BY weather_date;"
```

---

## Environment Variables

Create a `.env` file in the project root. A template is provided
in `.env.example`. 

| Variable | Description |
|---|---|
| `OPENWEATHER_API_KEY` | OpenWeatherMap API key (free at openweathermap.org) |
| `WEATHER_CITY` | Target city — set to `Johannesburg` |
| `WEATHER_LAT` | Latitude — `-26.2041` for Johannesburg |
| `WEATHER_LON` | Longitude — `28.0473` for Johannesburg |
| `POSTGRES_USER` | PostgreSQL username |
| `POSTGRES_PASSWORD` | PostgreSQL password |
| `POSTGRES_DB` | Database name — `weather_energy` |
| `POSTGRES_HOST` | Host — `postgres` inside Docker, `localhost` for dbt |
| `POSTGRES_PORT` | Port — `5432` |
| `AIRFLOW_UID` | Airflow container user — `50000` |

---

## 🧪 Data Quality

This pipeline implements data quality at three levels:

**Level 1 — Extraction validation**
Every API response is validated against a data contract before
any data is written anywhere. Checks include field presence,
null values, and value range guards. Failures halt the pipeline
and write to `pipeline_alerts`.

**Level 2 — dbt model tests**
11 automated tests run after every dbt build:
- `not_null` on all critical columns
- `unique` on all date columns
- `accepted_values` on `rainfall_category`

**Level 3 — Pipeline alerts table**
Every validation failure is logged to `pipeline_alerts` with
source, error message, timestamp, and raw response. This table
feeds the Pipeline Health panel in the dashboard providing a
full operational audit trail.

---

## dbt Lineage

raw_weather ──→ stg_weather ──┐
├──→ mart_weather_energy
raw_energy  ──→ stg_energy  ──┘

| Model | Type | Description |
|---|---|---|
| `stg_weather` | View | Cleans raw weather — renames, casts, coalesces nulls |
| `stg_energy` | View | Cleans raw energy — filters to Ethereum series |
| `mart_weather_energy` | Table | Joined mart with rainy_day flag and price metrics |

---

## Analysis — What the Data Shows

The pipeline is menat to answer one core question:

> **Does Johannesburg rainfall correlate with Ethereum energy prices?**

Key derived columns in `mart_weather_energy`:

| Column | Definition | Purpose |
|---|---|---|
| `rainy_day` | `precipitation_mm >= 1.0` | Boolean — meteorological rainy day definition |
| `rainfall_category` | Dry / Trace / Light / Moderate / Heavy | Ordinal dimension for dashboard filtering |
| `eth_price_change_usd` | Day to day USD change | Trend analysis |
| `eth_price_change_pct` | Day to day % change | Normalised trend comparison |

*Note: Statistically meaningful correlation requires a minimum of
90 days of observations. The pipeline collects one row per day
automatically patterns will emerge over time.*

---


## Production Architecture

This portfolio project runs entirely locally at zero cost.
Every component has a direct production cloud equivalent:

**FinOps principle applied:** Right-sized infrastructure for the
data volume. At 365 rows per year, a local PostgreSQL instance is
the correct tool. At 365 million rows, the architecture migrates
to a columnar warehouse (BigQuery / Snowflake) without changing
the extraction or orchestration layers.

---

## Future Improvements

- [ ] Replacing Ethereum proxy with direct Eskom / energy price API
- [ ] Add GitHub Actions CI — run dbt tests automatically on
      every push to main
- [ ] Migrate file lake to AWS S3 using boto3 — zero code change
      in extractors, only `save_to_lake()` function updated
- [ ] Add Great Expectations for richer data contract validation
- [ ] Implement incremental dbt models for efficiency at scale
- [ ] Add Slack alerting alongside email on pipeline failure
- [ ] Extend/scale to multiple South African cities for regional analysis

---

## Licence

MIT Licence — free to use, adapt, and build upon with attribution.

---

*Built with purpose for my project, Every architectural decision has a production
equivalent.*
