import os
import pandas as pd
import re
from prefect import flow,task

from google.cloud import bigquery
from prefect_gcp import GcpCredentials
from prefect_gcp.bigquery import BigQueryWarehouse
from prefect_gcp.cloud_storage import GcsBucket


# In case You want to create external table from parquet file in your data lake
# Iam createing internal table with feeding the dataset localy
# but in case you want in your own pipeline to not download the parequet and read it 
# then its better option to easier to create the table with this code
@task(log_prints = True)
def create_external_bq_table(project_id , table_id , gcs_parquet_path) -> None:
    """Create external table for teams in BigQuery based on .parquet file in bucket"""
    gcp_credentials = GcpCredentials.load("flights-gcp-creds")
    with BigQueryWarehouse(gcp_credentials=gcp_credentials) as warehouse:
        warehouse.execute(
            f"""
            CREATE OR REPLACE EXTERNAL TABLE `{project_id}.{table_id}`
            OPTIONS (
                format = 'parquuet',
                uris = ['{gcs_parquet_path}']
            );
            """
        )
# But I Write A Code To edit the schema manually and write internal table
# The Schema is just by mapping df types to big query types and then create a table with it
# In Case there is not a table already created before
# With Inserting The Row From pandas Df.bq 
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

# And To Insert Data into it i can use

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


# =========================================================================
# Using stem attribute of Path module,extracted the file name.
# It works for python 3.4 and above.
# the filename without the final extension
# This Code is to get valid table name from giving path
# aregex  to Remove all special characters, punctuation and spaces from string

file_no_extention = csv_path.stem
table_id = re.sub('[^A-Za-z0-9]+', '',file_no_extention)

#Path.stem is equivalent to path.name with no extention
# and we can remove extention by using 
Path.name.split('.')[0]

# =========================================================================

