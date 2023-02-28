#adding important library

import pandas as pd
import os
from google.cloud import bigquery

##definition of env data (GPC project and )

   

def bq_batch_transform_loader(event, context): 
    
    #default definition function wihich will be lunch after load niew file
    project_id = os.environ.get('gpc_project') #for testing <- imho to "hand" definition
    dataset_id= "cloud_statistic_house" #to set up after create dataset (must be exsits, before run function!)
    table_id="longterm_spending" #to set up after create table (must be exsits, before run function!)
    full_table_path = f"{project_id}.{dataset_id}.{table_id}"
    file=event
    source_file_name = file['name']

    #data to prep
    header_name = ["id_wizyty", "status_wizyty" ,"wizytujacy", "wspowizytujacy", "wizytowany", "data_wizyty", "data_modyfikacji", "spolka", "rodzaj_pracy", "obszar", "manager_obszaru", "poziom1", "poziom2", "poziom3", "instalacje", "komentarz", "id_spostrzezenia", "typ_spostrzezenia", "stan", "opis_spostrzezenia", "instalacja", "kategoria1", "kategoria2", "kategoria3", "kategoria4", "dotyczy_wykonawcy", "akcja_wymagana", "opis_akcji", "weryfikacja_wymagana", "po_terminie", "przypisany", "czas_do_realizacji", "data_realizacji", "prawdopodobienstwo_wystapienia", "skutki", "ryzyko", "standard_ryzyka"]
    dataframe=pd.read_csv(source_file_name, header=None, skiprows=1, names=header_name, index_col=None)
    print(dataframe)

    print(f"BQ Batch Transform Loader Processing file: {file['name']} and load XX raw to table YYY in {project_id}.") #output of function - will be usefull to read info of loader in logs in next chapter
    
    #definition of BQ load
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
    # Specify a (partial) schema. All columns are always written to the
    # table. The schema is used to assist in data type definitions.
    schema=[
        # Specify the type of columns whose type cannot be auto-detected. For
        # example the "title" column uses pandas dtype "object", so its
        # data type is ambiguous.
        bigquery.SchemaField("Provider", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("Resource_ID", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("Monthly_cost", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("Application_name", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("Month", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("Year", bigquery.enums.SqlTypeNames.STRING),
    ],
    # Optionally, set the write disposition. BigQuery appends loaded rows
    # to an existing table by default, but with WRITE_TRUNCATE write
    # disposition it replaces the table with the loaded data.
    #write_disposition="WRITE_TRUNCATE"
    )





#test data
event={'name': "/Users/maciejdziecielak/vcode/githubproject/dataexamples/whb_01-02-2023.csv"}
bq_batch_transform_loader(event, "gcs")