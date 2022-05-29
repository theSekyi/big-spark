from asyncio import Task

from sqlalchemy import table
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

S3_BUCKET = "s3://bigspark.challenge.data/tpcds_data_5g"

local_workflow = DAG(
    "LocalIngestionDag",
    schedule_interval="@daily",
    start_date=datetime(2021, 5, 1),
)

with local_workflow:
    download_data_task = BashOperator(task_id="download-data", bash_command=f"ls .")
