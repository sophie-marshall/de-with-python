import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from elasticsearch import Elasticsearch
import requests
import os

# test


## ----- HELPERS ----- ##
def query_scf_api(
    place_url: str, per_page: str = "100", get_historical_data: bool = False
) -> list:
    """
    Gather data from See Click Fix API for a specified location

    Args:
        - place_url (str): Parameter specifying the city/location you want information about
        - per_page (str): How many results shoudl the API return
        - get_historical_data (bool): Should the API look backwards and get past data?

    Returns:
        - issues (list): List of issues returned by the SCF API
    """
    # set request params
    params = {"place_url": place_url, "per_page": per_page}
    # if we wnat historical data, add a new param to look backwards
    if get_historical_data:
        params["status"] = "Archived"
    # send request
    res = requests.get(url="https://seeclickfix.com/api/v2/issues", params=params)
    # grab data if request is successfull
    if res.status_code == 200:
        data = res.json()
        issues = data["issues"]
        # get total pages and current page values
        curr = data["metadata"]["pagination"]["page"]
        total = data["metadata"]["pagination"]["pages"]
        print(f"Available Pages: {total}\n")
        # while there are still pages left continue gathering data
        while curr <= total:
            print(f"Current Page: {curr}")
            if data["metadata"]["pagination"]["next_page_url"]:
                res = requests.get(
                    url=data["metadata"]["pagination"]["next_page_url"], params=params
                )
                data = res.json()
                issues.extend(data["issues"])
                curr = data["metadata"]["pagination"]["page"]
            else:
                break
        return {"body": issues, "statusCode": 200}
    else:
        return {
            "body": None,
            "statusCode": res.status_code,
            "message": "Error getting API data",
        }


def combine_results(current_data: list, historical_data: list) -> list:
    """
    Combine current data with historical data

    Args:
        - current_data (list): List of items returned by SCF API for current events
        - historical_data (list): List of items returned by SCF API for historical events

    Returns:
        - JSON style response with combined results
    """
    try:
        combined_results = current_data + historical_data
        return {"body": combined_results, "statusCode": 200, "message": "success!"}
    except Exception as e:
        return {"body": None, "statusCode": 400, "message": e}


def upsert_to_elasticsearch(
    host: str,
    ssl_assert_fingerprint: str,
    username: str,
    password: str,
    index: str,
    data: list,
) -> dict:
    """
    Upsert collected data to Elasticsearch index

    Args:
        - host (str): Elasticsearch host address
        - ssl_assert_fingerprint (str): Fingerprint for Elasticsearch services
        - username (str): Elasticsearch username
        - password (str): Elasticsearch password
        - index (str): Name of Elasticsearch index to upsert data to
        - data (list): List of JSON entries to upsert to Elasticsearch

    Returns:
        - JSON response indicating status of operation and associated messaging
    """
    # establish elasticsearch connection
    es = Elasticsearch(
        {host},
        ssl_assert_fingerprint=ssl_assert_fingerprint,
        http_auth=(username, password),
    )
    # upsert provided data to elasticsearch
    try:
        for item in data:
            res = es.index(index=index, id=item["id"], body=item)
        return {"statusCode": 200, "message": "success!"}
    except Exception as e:
        return {"statusCode": 400, "message": str(e)}


## ----- DAG ----- ##
default_args = {
    "owner": "srmarshall",
    "start_date": datetime.datetime(2024, 7, 17),
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
}

with DAG(
    dag_id="upsert_to_scf_index", default_args=default_args, schedule="@daily"
) as dag:

    getCurrentData = PythonOperator(
        task_id="GetCurrentData",
        python_callable=query_scf_api,
        op_kwargs={"place_url": "downtown_district-of-columbia"},
    )

    getHistoricalData = PythonOperator(
        task_id="GetHistoricalData",
        python_callable=query_scf_api,
        op_kwargs={
            "place_url": "downtown_district-of-columbia",
            "get_historical_data": True,
        },
    )

    combineResults = PythonOperator(
        task_id="CombineResults",
        python_callable=combine_results,
        op_kwargs={
            "current_data": "{{ ti.xcom_pull(task_ids='GetCurrentData')['body'] }}",
            "historical_data": "{{ ti.xcom_pull(task_ids='GetHistoricalData')['body'] }}",
        },
    )

    upsertToElasticsearch = PythonOperator(
        task_id="UpsertToElasticsearch",
        python_callable=upsert_to_elasticsearch,
        op_kwargs={
            "host": "https://localhost:9200",
            "ssl_assert_fingerprint": os.getenv("ES_FINGERPRINT"),
            "username": "elastic",
            "password": os.getenv("ES_PASSWORD"),
            "index": "scf-downtown-district-of-columbia",
            "data": "{{ ti.xcom_pull(task_ids='CombineResults')['body'] }}",
        },
    )

    # set dependencies
    [getCurrentData, getHistoricalData] >> combineResults
    combineResults >> upsertToElasticsearch
