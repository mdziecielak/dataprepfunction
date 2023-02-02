import numpy as np
import pandas as pd
import os
from google.cloud import storage
from tempfile import NamedTemporaryFile
from google.cloud import bigquery


project_id = os.environ.get('GCP_PROJECT')


def data_processing(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    file = event
    client = storage.Client()

    #definition of variable for reading csv data to dataframe
    ##definition of source files
    source_bucket = client.get_bucket(file['bucket'])
    source_blob = source_bucket.get_blob(file['name'])
    source_file_name = file['name']
    source_bucket_name = file['bucket']
    ##definition of export tables
    dataset_id= "cloud_statistic_house"
    table_id="longterm_spending"
    full_table_path = f"{project_id}.{dataset_id}.{table_id}"
   

    #build uri new data files for reading
    uri = f"gs://{source_bucket_name}/{source_file_name}"

    #reading data

    md=pd.read_csv(uri)

    #preparing data in pandas dataframe 
    file_name_split=source_file_name.split("-")
    month= file_name_split[2]
    year= file_name_split[1]

    cmd=md.dropna(subset=['Provider'])
    cmd['Month']=month
    cmd['Year']=year


    #definition of output i bq
    header = ["Provider", "Resource ID", "Monthly cost", "Application name", "Month", "Year"]
    
    print(cmd)