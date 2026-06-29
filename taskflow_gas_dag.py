import os
import json
import http.client
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from datetime import datetime, timedelta
from airflow.decorators import dag, task

load_dotenv()

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

@dag(
    dag_id='taskflow_gas_etl',
    default_args=default_args,
    schedule=timedelta(minutes=25),
    catchup=False,
    tags=['gas_prices', 'etl']
)
def gas_prices_etl_pipeline():

    @task()
    def extract_gas_prices() -> str:
        
        conn = http.client.HTTPSConnection("api.collectapi.com")
        api_key = os.getenv("API_KEY")
        headers = {
            'content-type': "application/json",
            'authorization': f"apikey {api_key}"
        }
        conn.request("GET", "/gasPrice/stateUsaPrice?state=WA", headers=headers)
        res = conn.getresponse()
        data = res.read().decode("utf-8")
        return data

    @task()
    def transform_gas_prices(raw_data: str) -> list:
        
        data = json.loads(raw_data)
        cities = data["result"]["cities"]

        df = pd.DataFrame(cities)
        df.rename(columns={"name": "city_name", "midGrade": "mid_grade"}, inplace=True)
        df.drop(columns=["lowername"], inplace=True)
        
        return df.to_dict(orient="records")

    @task()
    def load_gas_prices(city_records: list):
        
        cities_df = pd.DataFrame(city_records)

        db_user = os.getenv("DB_USER")
        db_password = os.getenv("DB_PASSWORD")
        db_host = os.getenv("DB_HOST")
        db_port = os.getenv("DB_PORT")
        db_name = os.getenv("DB_NAME")

        engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")
        cities_df.to_sql("gasprices", con=engine, if_exists="replace", index=False)
        print("ETL process is complete!")

    #TaskFlow dependencies
    raw_data = extract_gas_prices()
    cleaned_data = transform_gas_prices(raw_data)
    load_gas_prices(cleaned_data)

gas_prices_etl = gas_prices_etl_pipeline()
