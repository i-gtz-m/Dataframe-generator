from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python import PythonOperator

from google.cloud import storage

PROJECT = os.environ.get("GCP_PROJECT")
BUCKET = os.environ.get("GCS_BUCKET")
LOCAL_DIR = os.environ.get("LOCAL_INPUT_DIR")

def upload_folder_to_gcs():
    client = storage.Client(project=PROJECT)
    bucket = client.get_bucket(BUCKET)
    for root, _, files in os.walk(LOCAL_DIR):
        for fname in files:
            if fname.endswith((".parquet", ".csv")):
                local_path = os.path.join(root, fname)
                rel = os.path.relpath(local_path, LOCAL_DIR).replace("\\", "/")
                blob = bucket.blob(f"raw/{rel}")
                blob.upload_from_filename(local_path)
                print(f"Uploaded {local_path} -> gs://{BUCKET}/raw/{rel}")

default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="local_to_gcs_dataset",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["gcs","onedrive","synthetic"]
) as dag:

    t_upload = PythonOperator(
        task_id="upload_folder_to_gcs",
        python_callable=upload_folder_to_gcs
    )

    t_upload
