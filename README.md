# Weather Data Pipeline with Airflow & Astronomer Cloud

## Overview

This project utilizes **Astro CLI** and **Astronomer Cloud** to create a configureless DAG deployment.  
The main goal of this pipeline is to:

- **Fetch** weather forecast data from the [Open-Meteo API](https://open-meteo.com/).  
  Since the API provides a rolling 3-day forecast, we only extract **today** and **tomorrow** data.
- **Load** the extracted data into **PostgreSQL** for storage and further analysis.
- Maintain **two tables**:
  - `weather_forecast` — stores the forecast for tomorrow (for future use, like user-facing weather prediction).
  - `weather_observation` — stores actual weather data for today (to compare forecast accuracy over time).

---

## Data Flow

1. **Extract**:  
   Fetch hourly weather metrics (temperature, precipitation, wind speed, humidity, cloud cover, weather code).
   
2. **Transform**:  
   Filter API response to only keep relevant dates (today/tomorrow) and map weather codes to descriptions.

3. **Load**:  
   Insert data into PostgreSQL tables with `ON CONFLICT` upserts to prevent duplication.

---

## Tech Stack

- **Apache Airflow** (via Astronomer Cloud)
- **Astro CLI**
- **PostgreSQL**
- **Python** (with `requests`, `pendulum`, `psycopg2`)

---