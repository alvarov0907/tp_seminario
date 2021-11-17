#!/usr/bin/python3
from sqlalchemy import create_engine
from datetime import datetime, timedelta

SYMBOLS = [
    'BTCUSDT','ETHUSDT','LTCUSDT','LINKUSDT','DOTUSDT','BNBUSDT','SHIBUSDT','MANAUSDT',
    'SANDUSDT','XRPUSDT','GALAUSDT','SOLUSDT','TRXUSDT','LTCUSDT','FILUSDT','ADAUSDT',
    'DOGEUSDT','AVAXUSDT','LINKUSDT','SXPUSDT','PORTOUSDT','FTMUSDT','CHZUSDT','OMGUSDT',
    'IOXTXUSDT','LRCUSDT','WAPXUSDT','MATICUSDT','WINUSDT','ETCUSDT','CHRUSDT','EOSUSDT',
    'ALGOUSDT','NEARUSDT','MITHUSDT','SLPUSDT','ATOMUSDT','ICPUSDT'
    ]
    
user = "workshop"
password = "w0rkzh0p"
host = "postgres"
port = "5432"
database = "workshop"
schema = "workshop"
engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")

default_args = {
    "depends_on_past": False,
    "max_active_runs": 1,
    "owner": "TP_SEMINARIO",
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "wait_for_downstream": False,
}