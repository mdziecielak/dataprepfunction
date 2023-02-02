import logging
import os
 
import pandas as pd
from google.cloud import bigquery

project_id = os.environ.get('GCP_PROJECT')

dataset_id= "cloud_statistic_house"
table_id="longterm_spending"
full_table_path = f"{project_id}.{dataset_id}.{table_id}"


client = bigquery.Client()
dataset_ref = client.dataset(dataset_id, project=project_id)
job_config = bigquery.LoadJobConfig()
job_config.autodetect = True
job_config.write_disposition = 'WRITE_APPEND'
client.load_table_from_dataframe