import os
import pandas as pd
import re
import zipfile
from pathlib import Path
from prefect import flow,task

from google.cloud import bigquery
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket

os.environ['KAGGLE_USERNAME'] = "***********" # username from the json file
os.environ['KAGGLE_KEY'] = "********************" # key from the json file

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
def write_to_gcs(path:Path) -> pd.DataFrame:
    """Upload Parquet files from its path to gcs buckets"""
    gcp_bucket_block = GcsBucket.load("flights-gcs")
    gcp_bucket_block.upload_from_path(
        from_path = path,
        to_path = f"flights/{path}"
    )
    return pd.read_parquet(path)

@task(log_prints = True)
def create_bq_table(project_id : str , dataset_id : str, table_id :str , df: pd.DataFrame) -> pd.DataFrame :
    """"Creating Big Query Table From This CSV Schema In Case Its Not Existed"""
    client = bigquery.Client()  # Construct a BigQuery client object.
    # The Next Part To check if there is a big query table with the same name
    dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
    table_ref = bigquery.TableReference(dataset_ref, table_id)
    try: # In Case Its Created Before We Just Return False
        client.get_table(table_ref)
        print("Its Created Before then I will just append the data\n")
        return df
    except :
        print("Its Not Created Before Then I Will Create this table\n")
    # Then If Its not exist then we will create new one from here
    types_mapping = {'object': 'STRING', 'string': 'STRING' ,'int64':'INTEGER' }
    schema = []
    for column in df.columns: # Creating Schema Dynamically from the dataframe columns
        column_type = str(df[column].dtype)
        bq_type = types_mapping[column_type]
        schema.append(bigquery.SchemaField(column, bq_type, mode="REQUIRED"))
    table = bigquery.Table(f"{project_id}.{dataset_id}.{table_id}", schema=schema)
    table = client.create_table(table)  # Make an API request.
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )
    return df

@task(log_prints = True )
def write_to_bq(destination:str, project_id:str, df: pd.DataFrame) -> None:
    """Write Our DataFrame to BiqQuery"""
    gcp_credentials_block = GcpCredentials.load("flights-gcp-creds")
    df.to_gbq(
        destination_table= destination,
        project_id=project_id,
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )

@flow(name="Kaggle To GCS Flow")
def etl_kaggle_gcs_bq(csv_path : Path) -> None :
    """the full cycle of a single file from reading it to be uploaded in gcs"""
    df = pd.read_csv(csv_path)
    clean_df = clean(df)
    # Using stem attribute of Path module,extracted the file name.
    # It works for python 3.4 and above.
    parquet_path = csv_to_parquet(clean_df,file_name = csv_path.stem)
    #I called it gcs_df In case iam getting the data from gcs in the write_to_gcs function
    # but here I already have the parquet file then iam just using it
    gcs_df = write_to_gcs(parquet_path) 
    project_id = 'resolute-choir-403411'
    dataset_id = 'flights'
    # regex  to Remove all special characters, punctuation and spaces from string
    file_no_extention = csv_path.stem.split('.')[0]
    table_id = re.sub('[^A-Za-z0-9]+', '',file_no_extention)
    gcs_df =create_bq_table(project_id,dataset_id,table_id,gcs_df)
    write_to_bq(
        destination = f"{dataset_id}.{table_id}",
        project_id = project_id ,
        df = gcs_df
    )
    
@flow(name = "Parent Flow")
def etl_parent_flow(kaggle_dataset_url: str):
    flights_zip = fetch(kaggle_dataset_url)
    csv_files = extract_local(flights_zip)
    for csv_path in csv_files :
        etl_kaggle_gcs_bq(csv_path)

if __name__ == '__main__':
    kaggle_dataset = "salikhussaini49/flight-data"
    etl_parent_flow(kaggle_dataset)


