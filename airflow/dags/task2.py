# Imorting libraries for 1st pipeline
import os
import random
from airflow import DAG
from airflow.operators.python import PythonOperator
from bs4 import BeautifulSoup
import requests
from datetime import datetime, timedelta
import zipfile

# Importing libraries necessary for 2nd pipeline
import apache_beam as beam
import geopandas as gpd
from airflow.sensors.filesystem import FileSensor

download_dir = '/tmp'
start_year = 2022
end_year = 2023


dag2 = DAG(
    'dag2_analytics',
    default_args={
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 2),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),},
    description='DAG for analytics ',
    # Need to change it to be activated every minute
    schedule_interval=None,  # Set to None for manual triggering
)

wait_for_zip = FileSensor(
    task_id='wait_for_zip',
    fs_conn_id='fs_default',
    filepath='path/to/zipfile.zip',
    timeout=5,
    poke_interval=5,
    mode='poke',
    dag=dag2,
)