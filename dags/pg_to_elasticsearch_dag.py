import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pandas as pd
import psycopg2
from elasticsearch import Elasticsearch
import os


# define helper functions
def query_pg(db_name, host, query, output_filepath):
    conn_string = f"dbname={db_name} host={host}"
    conn = psycopg2.connect(conn_string)
    df = pd.read_sql(query, conn)
    df.to_csv(output_filepath)
    print("---------- Postgres Data Saved ----------")


def insert_es(host, ssl_assert_fingerprint, username, password, input_filepath):
    # establish an Elasticsearch connection
    es = Elasticsearch(
        {host},
        ssl_assert_fingerprint=ssl_assert_fingerprint,
        http_auth=(username, password),
    )
    df = pd.read_csv(input_filepath)
    for i, r in df.iterrows():
        doc = r.to_json()
        res = es.index(index="frompostgres", body=doc)
        print(res)


# set defaults
default_args = {
    "owner": "srmarshall",
    "start_date": dt.datetime(2024, 7, 12),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# create dag
with DAG(
    "pg_to_elasticsearch_dag",
    default_args=default_args,
    schedule_interval=timedelta(minutes=5),
) as dag:
    getData = PythonOperator(
        task_id="QueryPostgreSQL",
        python_callable=query_pg,
        op_kwargs={
            "db_name": "airflow",
            "host": "localhost",
            "query": "select name, city from users",
            "output_filepath": "/Users/srmarshall/Desktop/code/personal/de-with-python/data/pg_csv.csv",
        },
    )

    insertData = PythonOperator(
        task_id="InsertElasticSearch",
        python_callable=insert_es,
        op_kwargs={
            "host": "https://localhost:9200",
            "ssl_assert_fingerprint": os.getenv("ES_FINGERPRINT"),
            "username": "elastic",
            "password": os.getenv("ES_PASSWORD"),
            "input_filepath": "/Users/srmarshall/Desktop/code/personal/de-with-python/data/pg_csv.csv",
        },
    )

    getData >> insertData
