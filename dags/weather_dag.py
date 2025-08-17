from airflow.sdk.definitions.asset import Asset
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_batch
import pendulum
import requests


weather_code_map = {
    0: "Clear sky", 1: "Mainly clear", 2: "Partly cloudy", 3: "Overcast",
    45: "Fog", 48: "Depositing rime fog",
    51: "Light drizzle", 53: "Moderate drizzle", 55: "Dense drizzle",
    61: "Slight rain", 63: "Moderate rain", 65: "Heavy rain",
    80: "Slight rain showers", 81: "Moderate rain showers", 82: "Violent rain showers",
    95: "Thunderstorm", 96: "Thunderstorm w/ slight hail", 99: "Thunderstorm w/ heavy hail",
}

url = "https://api.open-meteo.com/v1/forecast?latitude=-6.25&longitude=106.75&timezone=Asia%2FJakarta&hourly=temperature_2m,weathercode,precipitation,windspeed_10m,relativehumidity_2m,cloudcover"


@dag(
    start_date=pendulum.datetime(2025, 8, 17, tz="Asia/Jakarta"),
    schedule="5 23 * * *", # 23:05 WIB == 16:05 UTC
    catchup=False, # Tells Airflow: “don’t go back and fill in missing runs from the past"
    default_args={"owner": "Astro", "retries": 3},
    tags=["weather"],
)
def weather_dag():

    @task(outlets=[Asset("daily_weather_data")])
    def fetch_tomorrow_weather():
        response = requests.get(url)
        data = response.json()

        hours = data["hourly"]["time"]
        temperatures = data["hourly"]["temperature_2m"]
        weathercodes = data["hourly"]["weathercode"]
        precipitations = data["hourly"]["precipitation"]
        windspeed = data["hourly"]["windspeed_10m"]
        humidities = data["hourly"]["relativehumidity_2m"]
        cloudcovers = data["hourly"]["cloudcover"]

        tz = "Asia/Jakarta"
        today = pendulum.now(tz).date()
        tomorrow = today.add(days=1)

        rows = []
        for t, temp, code, p, w, h, c in zip(hours, temperatures, weathercodes, precipitations, windspeed, humidities, cloudcovers):
            dt = pendulum.parse(t, tz=tz) # to convert to tz/jkt time
            if dt.date() == tomorrow:
                rows.append({
                    "ts": t,
                    "temperature_c": temp,
                    "weather_code": int(code),
                    "weather_desc": weather_code_map.get(int(code), "Unknown"),
                    "precip_mm" : p,
                    "windspeed_10m" : w,
                    "relative_humidity" : h,
                    "cloudcover" : c,
                })
        return rows

    @task
    def load_tomorrow_weather(rows):
        if not rows:
            return "No rows to load"
        
        hook = PostgresHook(postgres_conn_id="conn_with_supabase")
        conn = hook.get_conn()
        cur = conn.cursor()

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS weather_forecast(
                ts timestamptz PRIMARY KEY,
                temperature_c numeric,
                weather_code int,
                weather_desc text,
                precip_mm numeric,
                windspeed_10m numeric,
                relative_humidity numeric,
                cloudcover numeric,
                collected_at timestamptz DEFAULT now()
            );
            """
        )
        tuples = [
            (r["ts"],
             r["temperature_c"],
             r["weather_code"],
             r["weather_desc"],
             r["precip_mm"],
             r["windspeed_10m"],
             r["relative_humidity"],
             r["cloudcover"]
            )
            for r in rows
        ]
        execute_batch(cur, """
            INSERT INTO weather_forecast (ts, temperature_c, weather_code, weather_desc, precip_mm, windspeed_10m, relative_humidity, cloudcover)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (ts) DO UPDATE SET
              temperature_c=EXCLUDED.temperature_c,
              weather_code=EXCLUDED.weather_code,
              weather_desc=EXCLUDED.weather_desc,
              precip_mm=EXCLUDED.precip_mm,
              windspeed_10m=EXCLUDED.windspeed_10m,
              relative_humidity=EXCLUDED.relative_humidity,
              cloudcover=EXCLUDED.cloudcover;
        """,
        tuples)

        conn.commit()
        cur.close()
        conn.close()
        return f"Upserted {len(rows)} rows"

    @task
    def fetch_today_weather():
        response = requests.get(url)
        data = response.json()

        hours = data["hourly"]["time"]
        temperatures = data["hourly"]["temperature_2m"]
        weathercodes = data["hourly"]["weathercode"]
        precipitations = data["hourly"]["precipitation"]
        windspeed = data["hourly"]["windspeed_10m"]
        humidities = data["hourly"]["relativehumidity_2m"]
        cloudcovers = data["hourly"]["cloudcover"]

        tz = "Asia/Jakarta"
        today = pendulum.now(tz).date()

        rows = []
        for t, temp, code, p, w, h, c in zip(hours, temperatures, weathercodes, precipitations, windspeed, humidities, cloudcovers):
            dt = pendulum.parse(t, tz=tz) # to convert to tz/jkt time
            if dt.date() == today:
                rows.append({
                    "ts": t,
                    "temperature_c": temp,
                    "weather_code": int(code),
                    "weather_desc": weather_code_map.get(int(code), "Unknown"),
                    "precip_mm" : p,
                    "windspeed_10m" : w,
                    "relative_humidity" : h,
                    "cloudcover" : c,
                })
        return rows    

    @task
    def load_today_weather(rows):
        if not rows:
            return "No rows to load"
        
        hook = PostgresHook(postgres_conn_id="conn_with_supabase")
        conn = hook.get_conn()
        cur = conn.cursor()

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS weather_observation(
                ts timestamptz PRIMARY KEY,
                temperature_c numeric,
                weather_code int,
                weather_desc text,
                precip_mm numeric,
                windspeed_10m numeric,
                relative_humidity numeric,
                cloudcover numeric,
                collected_at timestamptz DEFAULT now()
            );
            """
        )
        tuples = [
            (r["ts"],
             r["temperature_c"],
             r["weather_code"],
             r["weather_desc"],
             r["precip_mm"],
             r["windspeed_10m"],
             r["relative_humidity"],
             r["cloudcover"]
            )
            for r in rows
        ]
        execute_batch(cur, """
            INSERT INTO weather_observation (ts, temperature_c, weather_code, weather_desc, precip_mm, windspeed_10m, relative_humidity, cloudcover)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (ts) DO UPDATE SET
              temperature_c=EXCLUDED.temperature_c,
              weather_code=EXCLUDED.weather_code,
              weather_desc=EXCLUDED.weather_desc,
              precip_mm=EXCLUDED.precip_mm,
              windspeed_10m=EXCLUDED.windspeed_10m,
              relative_humidity=EXCLUDED.relative_humidity,
              cloudcover=EXCLUDED.cloudcover;
        """,
        tuples)

        conn.commit()
        cur.close()
        conn.close()
        return f"Upserted {len(rows)} rows"

    # wire the tasks
    load_tomorrow_weather(fetch_tomorrow_weather())
    load_today_weather(fetch_today_weather())

# Instantiate the DAG
weather_dag()