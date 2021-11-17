"""
TP SEMINARIO. 
Alumnos: Alvaro Vanina y Ariel Belossi

Get scalping signals in binance in temporality of 5 minutes.
----------------------------------

1) Create table binance_market if not exists in schema workshop in postgres

2) Calculate Scalping Binance for symbols in the list in file params.py
   This task "_get_scalping_data" gives us the values to calculate:
   
   If ema13 > ema34 and volumen_mayor_media == True --> Buy Signal
   
   If ema13 < ema34 and volumen_mayor_media == True --> Sell Signal

3) Append new values in table binance_scalping and then we can alert to buy or sell.

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

SQL_CREATE_TABLE_BINANCE_SCALPING = f""" CREATE TABLE IF NOT EXISTS binance_scalping(
symbol text,
ema13 decimal, 
ema34 decimal, 
ema13_1 decimal, 
ema34_1 decimal, 
volumen decimal, 
volumen_mayor_media bool, 
cruce_alcista bool, 
cruce_bajista bool, 
lastprice decimal,
datetime timestamp,
execution_date_dag timestamp
) ;
"""

def checkMarketHigh (candles):
    a = float(candles.iloc[-1]['crossing_ema_high'])
    b = float(candles.iloc[-2]['crossing_ema_high'])
    return (a>b)  # Si la anterior es menor y la actual mayor, entonces cruzó

def checkMarketLow (candles):
    a = float(candles.iloc[-1]['crossing_ema_low'])
    b = float(candles.iloc[-2]['crossing_ema_low'])
    return (a>b)  # Si la anterior es menor y la actual mayor, entonces cruzó
    
def _get_scalping_data(symbol,tick,exec_date):
    sleep(5)
    print(symbol,tick)
    url = "https://api.binance.com/api/v1/klines?symbol="+str(symbol)+"&interval="+str(tick)
    print(url)
    r = requests.get(url)
    #print(r.status_code)
    #print(r.json)
    if r.status_code == 200:
        data = r.json()
        labels = ['OT', 'open', 'high', 'low', 'close', 'V', 'CT', 'VQ', 'NT', 'T1', 'T2', "_"]
        df = pd.DataFrame.from_records(data, columns=labels)
        df.to_csv('ariel.csv')
        df[['open','high', 'low', 'close', 'V']] = df[['open','high', 'low', 'close', 'V']].apply(pd.to_numeric)
        klines = df[['open','high', 'low', 'close', 'V']]
        klines['ema_13'] = klines['close'].ewm(span=13).mean() #Calcula ema a 13
        klines['ema_34'] = klines['close'].ewm(span=34).mean() # Calcula ema a 34
        klines['crossing_ema_high']  = klines["ema_13"] > klines["ema_34"] # Booleano que dice si es mayor 13 que 34
        klines['crossing_ema_low']  = klines["ema_13"] < klines["ema_34"] # Booleano que dice si es menor 13 que 34

        # Definicion de Valores que devuelvo
        cruce_alcista = isinstance(klines, pd.DataFrame) and checkMarketHigh(klines)
        cruce_bajista = isinstance(klines, pd.DataFrame) and checkMarketLow(klines)
        ema13 = klines.iloc[-1]['ema_13']
        ema13_1 = klines.iloc[-2]['ema_13']
        ema34 = klines.iloc[-1]['ema_34']
        ema34_1 = klines.iloc[-2]['ema_34']
        lastprice = "{:.8f}".format(klines.iloc[-1]['close'])
        volumen = float(klines.iloc[-1]['V'])


        volumenbase = 0
        try:
            for i in range (1,11):
                volumenbase += float(klines.iloc[-i]['V'])
            volumen_media = float(volumenbase/10)
            volumen_mayor_media = float(volumen) > float(volumen_media)
        except Exception as e:
            print(e)

        print('volumen: ', volumen, '  volumen_media: ', volumen_media,)

        columns = ['symbol','ema13' , 'ema34' , 'ema13_1' , 'ema34_1' , 'volumen' , 'volumen_mayor_media' , 'cruce_alcista' , 'cruce_bajista' , 'lastprice' , 'datetime' , 'execution_date_dag']
        df = pd.DataFrame(columns=columns)
        results = [symbol,ema13 , ema34 , ema13_1 , ema34_1 , volumen, volumen_mayor_media , cruce_alcista , cruce_bajista, float(lastprice),datetime.now().strftime("%m/%d/%Y, %H:%M:%S"),exec_date]
        df.loc[0] = results
        #print(df.dtypes)
        #print(df)
        with engine.begin():
            try:
                df.to_sql("binance_scalping", con=engine, schema=schema, if_exists="append", index=False)
            except Exception as e:
                pass
        logging.info(f"It was wrote to postgres binance_scalping succesfully.")

# dag definition
with DAG(
    dag_id="tp_seminario_binance_scalping_minute",
    description="Get Binance Scalping data for 5 min. Temporality",
    start_date=datetime(2021, 11, 16),
    schedule_interval="* * * * *", # “At every minute.”
    catchup=False,
    default_args=default_args,
    tags=["Binance", "minute"],
    dagrun_timeout=timedelta(seconds=3600),
    doc_md=__doc__
) as dag:
    # tasks
    start_execution = DummyOperator(task_id="start_execution")
    end_execution = DummyOperator(task_id="end_execution")

    create_table_binance_scalping = PostgresOperator(
        task_id='create_table_binance_scalping',
        sql=SQL_CREATE_TABLE_BINANCE_SCALPING,
        postgres_conn_id='my_postgres_connection_id',
    )

    # Create several task in loop
    ingestion_tasks = []

    for symbol in SYMBOLS:
        task_id = f"ingestion_{symbol}"
        task_op_kwargs = {
            "symbol": f"{symbol}",
            "tick": "15m",
            "exec_date": "{{ds}}",
        }
        ingestion_task = PythonOperator(
            python_callable=_get_scalping_data,
            task_id=task_id,
            op_kwargs=task_op_kwargs,
        )
        logging.info(f"Dynamicall task: {ingestion_task}")
        ingestion_tasks.append(ingestion_task)


    start_execution >> create_table_binance_scalping >> ingestion_tasks >> end_execution
