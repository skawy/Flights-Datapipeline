import os
import pandas as pd
import zipfile
from pathlib import Path
from prefect import flow,task

from prefect_gcp.cloud_storage import GcsBucket

os.environ['KAGGLE_USERNAME'] = "*********" # username from the json file
os.environ['KAGGLE_KEY'] = "*****************" # key from the json file

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
    return df

@task(log_prints = True)
def csv_to_parquet(df : pd.DataFrame, file_name: str) -> Path:
    """Save dataframes into parquet files and return their paths"""
    path = Path(f"dataset/parquets/{file_name}.parquet")
    df.to_parquet(path)
    return path

@task(log_prints = True , retries = 3 , timeout_seconds=5000)
def write_gcs(path:Path) -> None:
    """Upload Parquet files from its path to gcs buckets"""
    gcp_bucket_block = GcsBucket.load("flights-gcs")
    gcp_bucket_block.upload_from_path(
        from_path = path,
        to_path = f"flights/{path}"
    )


@flow(name="Kaggle To GCS Flow")
def etl_kaggle_to_gcs(csv_path : Path) -> None :
    """the full cycle of a single file from reading it to be uploaded in gcs"""
    df = pd.read_csv(csv_path)
    clean_df = clean(df)
    # Using stem attribute of Path module,extracted the file name.
    # It works for python 3.4 and above.
    parquet_path = csv_to_parquet(clean_df,file_name = csv_path.stem)
    write_gcs(parquet_path)

@flow(name = "Parent Flow")
def etl_parent_flow(kaggle_dataset_url: str):
    flights_zip = fetch(kaggle_dataset_url)
    csv_files = extract_local(flights_zip)
    for csv_path in csv_files :
        etl_kaggle_to_gcs(csv_path)

if __name__ == '__main__':
    kaggle_dataset = "salikhussaini49/flight-data"
    etl_parent_flow(kaggle_dataset)


