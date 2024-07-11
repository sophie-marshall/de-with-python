import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pandas as pd


# pipeline hlpers
def csv_to_json(input_filepath, output_filepath):
    df = pd.read_csv(input_filepath)
    for i, r in df.iterrows():
        print(r["name"])
    df.to_json(output_filepath, orient="records")


# set default args for
default_args = {
    "owner": "srmarshall",
    "start_date": dt.datetime(2024, 7, 11),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# create dag
with DAG(
    "csv_to_json_dag",
    default_args=default_args,
    schedule_interval=timedelta(minutes=5),
) as dag:
    # create bash operator to confirm DAG is running
    print_starting = BashOperator(
        task_id="starting", bash_command="echo 'Reading CSV...'"
    )

    # use python operator to call function
    CSVtoJSON = PythonOperator(
        task_id="convertCSVtoJSON",
        python_callable=csv_to_json,
        op_kwargs={
            "input_filepath": "../data/faker_csv.csv",
            "output_filepath": "../data/faker_json_from_airflow.json",
        },
    )

    # connect tasks by specifying up and downstream tasks
    print_starting >> CSVtoJSON
