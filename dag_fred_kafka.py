from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import pandas as pd
from confluent_kafka import Producer
import json

FRED_API_KEY = "704fdac2c75a836581cb74008d766882"

SERIES_LIST = {
    "DFF": "Federal Funds Rate",
    "CPIAUCSL": "Consumer Price Index",
    "UNRATE": "Unemployment Rate"
}

START_DATE = "2026-01-01"

def fetch_and_send():

    def fetch_fred_series(series_id, series_name):
        url = "https://api.stlouisfed.org/fred/series/observations"
        params = {
            "series_id": series_id,
            "api_key": FRED_API_KEY,
            "file_type": "json",
            "observation_start": START_DATE
        }

        response = requests.get(url, params=params)
        data = response.json()
        observations = data.get("observations", [])

        df = pd.DataFrame(observations)

        if df.empty:
            return df

        df["date"] = pd.to_datetime(df["date"])
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df["series_id"] = series_id
        df["series_name"] = series_name

        return df[["series_id", "series_name", "date", "value"]]

    all_data = pd.concat(
        [fetch_fred_series(k, v) for k, v in SERIES_LIST.items()],
        ignore_index=True
    )

    conf = {
        'bootstrap.servers': 'pkc-921jm.us-east-2.aws.confluent.cloud:9092',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': 'XGXDR43CWRIHV5HG',
        'sasl.password': 'TON_SECRET_ICI'
    }

    producer = Producer(conf)

    topic = "fred_test"

    for _, row in all_data.iterrows():
        producer.produce(topic, json.dumps(row.to_dict()))

    producer.flush()

    print("✅ FRED data sent to Kafka")

with DAG(
    dag_id="fred_kafka_pipeline",
    start_date=datetime(2024,1,1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    task1 = PythonOperator(
        task_id="fetch_and_send_fred",
        python_callable=fetch_and_send,
    )