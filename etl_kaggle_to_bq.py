import os
import pandas as pd
import re
import zipfile
from pathlib import Path

import subprocess
import logging

from google.cloud import bigquery

from prefect import flow,task
from prefect_gcp import GcpCredentials
from prefect_gcp.bigquery import BigQueryWarehouse
from prefect_gcp.cloud_storage import GcsBucket

os.environ['KAGGLE_USERNAME'] = "***********" # username from the json file
os.environ['KAGGLE_KEY'] = "***********************" # key from the json file

@task(log_prints = True)
def fetch(dataset_url:str) -> Path:
    """Fetching Data From Kaggle With Kaggle api"""
    path = Path(f"dataset/flight-data.zip")
    if not os.path.isfile(path) :
        download_dataset = os.system(
            f"mkdir dataset;\
            cd dataset;\
            mkdir csvs;\
            mkdir parquets;\
            kaggle datasets download -d {dataset_url}"
            )
    return path

@task(log_prints = True)
def extract_local(zip_path:Path) -> list:
    """Extracting zip file csv`s and return their paths"""
    csv_files = []
    csv_directory  = f"dataset/csvs/"
    with zipfile.ZipFile(zip_path,"r") as zip_ref:
        csvs = Path(csv_directory)
        zip_ref.extractall(csvs)
    for file_name in os.listdir(csv_directory):
        if file_name.endswith('.csv'):
            csv_files.append(Path(f"{csv_directory}/{file_name}"))
    return csv_files

@task(log_prints = True)
def clean(df : pd.DataFrame) -> pd.DataFrame:
    """Change columns dtypes in the dataframes to strings"""
    for column in df.columns:
        if df[column].dtype == "object":
            df[column] = df[column].astype('string')
            df[column].fillna("unknown", inplace=True)
    return df

@task(log_prints = True)
def csv_to_parquet(df : pd.DataFrame, file_name: str) -> Path:
    """Save dataframes into parquet files and return their paths"""
    path = Path(f"dataset/parquets/{file_name}.parquet")
    df.to_parquet(path)
    return path

@task(log_prints = True , retries = 3 , timeout_seconds=5000)
def write_to_gcs(path:Path) -> str:
    """Upload Parquet files from its path to gcs buckets"""
    gcp_bucket_block = GcsBucket.load("flights-gcs")
    gcp_bucket_block.upload_from_path(
        from_path = path,
        to_path = f"flights/{path}"
    )
    return path.name # File Name

@task(log_prints = True)
def create_external_bq_table(project_id , dataset_id , table_id , parquet_file) -> str:
    """Create external table for teams in BigQuery based on .parquet file in bucket and returns the table name"""
    gcp_credentials = GcpCredentials.load("flights-gcp-creds")
    table_name = f"{project_id}.{dataset_id}.{table_id}_external"
    with BigQueryWarehouse(gcp_credentials=gcp_credentials) as warehouse:
        warehouse.execute(
            f"""
            CREATE OR REPLACE EXTERNAL TABLE `{table_name}`
            OPTIONS (
                format = 'parquet',
                uris = ['gs://de_flights/flights/dataset/parquets/{parquet_file}']
            );
            """
        )
    return table_name

@task(log_prints = True)
def create_materialized_bq_table(project_id , dataset_id , table_id , external_table_name) -> None:
    gcp_credentials = GcpCredentials.load("flights-gcp-creds")
    with BigQueryWarehouse(gcp_credentials=gcp_credentials) as warehouse:
        warehouse.execute(
            f"""
            CREATE OR REPLACE TABLE {project_id}.{dataset_id}.{table_id} AS
            SELECT *
            FROM {external_table_name};
            """
        )


@task(log_prints = True)
def upload_spark_job(file_name: str) -> None:
    """Upload spark job to cloud bucket, to be run by dataproc"""
    gcp_bucket_block = GcsBucket.load("flights-gcs")
    gcp_bucket_block.upload_from_path(
        from_path = f"./spark_jobs/{file_name}" ,
        to_path = f"flights/spark_jobs/{file_name}"
    )
    return


@task(log_prints = True)
def trigger_spark_job(file_name: str) -> None:
    """Trigger dataproc to run our pyspark job. The end result should be a table in bigquery ready to be visualized"""
    result = subprocess.run(
        [
            "gcloud", "dataproc", "jobs", "submit", "pyspark",
            f"--cluster=flights-cluster", 
            f"--region=europe-west1",
            "--jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar",
            f"gs://de_flights/flights/spark_jobs/{file_name}"
            
        ]
    )
    logging.info(f"Command return code: {result.returncode} - Hopefully it is 0!")

@flow(name="Kaggle To GCS Flow")
def etl_kaggle_gcs_bq(csv_path : Path) -> None :
    """the full cycle of a single file from reading it to be uploaded in gcs"""
    df = pd.read_csv(csv_path)
    clean_df = clean(df)
    # Using stem attribute of Path module,extracted the file name.
    # It works for python 3.4 and above.
    # the filename without the final extension
    parquet_path = csv_to_parquet(clean_df,file_name = csv_path.stem)
    parquet_file = write_to_gcs(parquet_path) 

    # regex to Remove all special characters, punctuation and spaces from string
    # .split('.')[0] to remove the extention from file name
    project_id = 'resolute-choir-403411'
    dataset_id = 'flights'
    table_id = re.sub('[^A-Za-z0-9]+', '',parquet_file.split('.')[0]) 

    external_table_name = create_external_bq_table(project_id,dataset_id,table_id,parquet_file)
    create_materialized_bq_table(project_id,dataset_id,table_id,external_table_name)


@flow(name = "Parent Flow")
def etl_parent_flow(kaggle_dataset_url: str):
    flights_zip = fetch(kaggle_dataset_url)
    csv_files = extract_local(flights_zip)
    for csv_path in csv_files :
        etl_kaggle_gcs_bq(csv_path)
    spark_job_file_name = 'fact_flights_job.py'
    upload_spark_job(spark_job_file_name)
    trigger_spark_job(spark_job_file_name)


if __name__ == '__main__':
    kaggle_dataset = "salikhussaini49/flight-data"
    etl_parent_flow(kaggle_dataset)


