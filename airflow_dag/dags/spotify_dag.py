from airflow import DAG
from airflow.decorators import task

from datetime import datetime
import pandas as pd
import gcsfs
import json


BUCKET = "ghin-data-engineering-project-bucket"
SONGS_PATH = f"gs://{BUCKET}/spotify/mysql_export/songs_export.json"
USERS_PATH = f"gs://{BUCKET}/spotify/mysql_export/users_export.json"
STREAMS_BASE_PATH = f"gs://{BUCKET}/topics/streams_play/year=2025/month=06/day=20/hour=*/*.json"

with DAG (
    dag_id = "spotify_dag",
    schedule = "@once",
    start_date = datetime(2025, 6, 23, 3, 0),
    catchup=False,
    tags=["portofolio"]
) as dag:
    
    # Task 1: read data from GCS bucket
    @task
    def read_gcs_files():
        fs = gcsfs.GCSFileSystem(token="cloud")
        # Songs
        with fs.open(SONGS_PATH, 'r') as f:
            songs_data = json.load(f)

        # Users
        with fs.open(USERS_PATH, 'r') as f:
            users_data = json.load(f)

        # Streams - gabungkan dari semua folder hour
        stream_files = fs.glob(STREAMS_BASE_PATH)
        streams_data = []
        for file in stream_files:
            with fs.open(file, 'r') as f:
                streams_data.extend(json.load(f))  
        
        print(f"Read data from GCS was done successfully!")

        return {
            "songs": songs_data,
            "users": users_data,
            "streams": streams_data,
        }
    
    # Set Dependencies
    fetched_data = read_gcs_files()
