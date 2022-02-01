import os 
import logging
import pyarrow.csv as pv
import pyarrow.parquet as pq

from airflow import DAG 
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from google.cloud import storage

PROJECT_ID = "dtc-de-bootcamp"
BUCKET = "dtc_data_lake_dtc-de-bootcamp"

dataset_file = "fhv_tripdata_{{ data_interval_start.strftime(\'%Y-%m\') }}.csv"
dataset_url = f"https://nyc-tlc.s3.amazonaws.com/trip+data/{dataset_file}"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
parquet_file = dataset_file.replace(".csv", ".parquet")

def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))

# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

default_args = {
    "owner": "airflow",
    "start_date": datetime(2019,1,1),
    "end_date": datetime(2019,12,30),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id = "data_ingestion_fhv_trips_gcs",
    schedule_interval = "0 10 2 * *",
    default_args = default_args,
    catchup = True,
    max_active_runs = 3,
    tags = ['dtc-de: homework_week_2'],
) as dag:

    download_fhv_data = BashOperator(
        task_id = "download_fhv_data",
        bash_command = f"curl -sSLf {dataset_url} > {path_to_local_home}/{dataset_file}"
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{path_to_local_home}/{dataset_file}",
        },
    )

    upload_to_gcs_task = PythonOperator(
        task_id="upload_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{parquet_file}",
            "local_file": f"{path_to_local_home}/{parquet_file}",
        },
    )

    remove_file_task =  BashOperator(
        task_id="remove_csv_file",
        bash_command=f"rm {path_to_local_home}/{dataset_file} {path_to_local_home}/{parquet_file}"
    )

    download_fhv_data >> format_to_parquet_task >> upload_to_gcs_task >> remove_file_task