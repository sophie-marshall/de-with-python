import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pandas as pd

# set default args
default_args = {
    "owner": "srmarshall",
    "start_date": dt.datetime(2024, 7, 15),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# helper functions
def clean_data(input_filepath, output_filepath):
    df = pd.read_csv(input_filepath)
    df.drop(columns=["region_id"], inplace=True)
    df.columns = [x.lower() for x in df.columns]
    df["started_at"] = pd.to_datetime(df["started_at"], format="%m/%d/%Y %H:%M")
    df.to_csv(output_filepath)


def filter_data(input_filepath, output_filepath):
    df = pd.read_csv(input_filepath)
    start_date = "2019-05-23"
    end_date = "2019-06-03"
    filtered_df = df[(df["started_at"] > start_date) & (df["started_at"] < end_date)]
    filtered_df.to_csv(output_filepath)


# create date
with DAG(
    "clean_scooter_df_dag",
    default_args=default_args,
    schedule_interval=timedelta(minutes=5),
) as dag:
    cleanData = PythonOperator(
        task_id="CleanRawData",
        python_callable=clean_data,
        op_kwargs={
            "input_filepath": "/Users/srmarshall/Desktop/code/personal/de-with-python/data/scooter_data.csv",
            "output_filepath": "/Users/srmarshall/Desktop/code/personal/de-with-python/data/clean_scooter_data.csv",
        },
    )

    filterData = PythonOperator(
        task_id="FilterData",
        python_callable=filter_data,
        op_kwargs={
            "input_filepath": "/Users/srmarshall/Desktop/code/personal/de-with-python/data/clean_scooter_data.csv",
            "output_filepath": "/Users/srmarshall/Desktop/code/personal/de-with-python/data/clean_filtered_scooter_data.csv",
        },
    )

    copyFile = BashOperator(
        task_id="copyToDesktop",
        bash_command="cp /Users/srmarshall/Desktop/code/personal/de-with-python/data/clean_filtered_scooter_data.csv /Users/srmarshall/Desktop/",
    )

    cleanData >> filterData >> copyFile
