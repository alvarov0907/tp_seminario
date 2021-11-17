"""
TP SEMINARIO. 
Alumnos: Alvaro Vanina y Ariel Belossi

Get all Market-Binance values and ingest in postgres.
----------------------------------

1) Create table if not exists binance_market

2) Get all values of Binance Markets Symbols.

3) Update new values each hour searching in binance_market and make the ingestion in table in postgres.

P.D. We are taking some params from params.py like the keys and
     the connection to access postgress and the default_args for the dags.
----------------------------------
"""

#!/usr/bin/python3
from time import sleep
import requests
import urllib.parse
import json
import pandas as pd
import sys
import subprocess
import logging
from datetime import timedelta
from datetime import datetime
from pandas.io.json import json_normalize
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from pathlib import Path


from params import (
    SYMBOLS,
    schema,
    engine,
    default_args,
)

STORE_DIR = Path(__file__).resolve().parent / 'tmp-files' / 'random-num'
Path.mkdir(STORE_DIR, exist_ok=True, parents=True)

SQL_CREATE_TABLE_BINANCE_MARKET = f""" CREATE TABLE IF NOT EXISTS binance_market(
symbol TEXT,
priceChange	TEXT,
priceChangePercent TEXT,
weightedAvgPrice TEXT,
prevClosePrice TEXT,
lastPrice TEXT,
lastQty	TEXT,
bidPrice TEXT,
bidQty TEXT,
askPrice TEXT,
askQty TEXT,
openPrice TEXT,
highPrice TEXT,
lowPrice TEXT,
volume TEXT,
quoteVolume TEXT,
openTime INT,
closeTime INT,
firstId	INT,
lastId	INT,
count INT
) ;
"""

def _ingestion_binance_market():
    r = requests.get('https://api.binance.com/api/v1/ticker/24hr')
    if r.status_code == 200:
        result = r.json()
        result = json_normalize(result)
        df = pd.DataFrame(result)
    #print(str(STORE_DIR)+str('/binance.csv'))
    df.to_csv(str(STORE_DIR)+str('/binance.csv'),index=False) #Escribo dataframe a csv en la carpeta tmp-files/random-run
    with engine.begin():
        try:
            df.to_sql("binance_market", con=engine, schema=schema, if_exists="replace", index=False)
        except Exception as e:
            pass
    logging.info(f"It was wrote to postgres binance_market succesfully.")

# dag definition
with DAG(
    dag_id="tp_seminario_binance_market_hourly",
    description="Get Binance Market hourly.",
    start_date=datetime(2021, 11, 16),
    schedule_interval="@hourly", # “At every minute.”
    catchup=False,
    default_args=default_args,
    tags=["Binance", "Market" , "hourly"],
    dagrun_timeout=timedelta(seconds=3600),
    doc_md=__doc__
) as dag:
    # tasks
    start_execution = DummyOperator(task_id="start_execution")
    end_execution = DummyOperator(task_id="end_execution")

    create_table_binance_market = PostgresOperator(
        task_id='create_table_binance_market',
        sql=SQL_CREATE_TABLE_BINANCE_MARKET,
        postgres_conn_id='my_postgres_connection_id',
    )

    ingestion_binance_market = PythonOperator(
        task_id="ingestion_binance_market",
        python_callable=_ingestion_binance_market,
    )


    start_execution >> create_table_binance_market >> ingestion_binance_market >> end_execution
