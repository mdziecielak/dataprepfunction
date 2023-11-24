#adding important library

import pandas as pd
import os
from google.cloud import bigquery

##definition of env data 

   

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

    #print(f"BQ Batch Transform Loader Processing file: {file['name']} and load XX raw to table YYY in {project_id}.") #output of function - will be usefull to read info of loader in logs in next chapter
    print(len(header_name))

    #definition of BQ load
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
    # Specify a (partial) schema. All columns are always written to the
    # table. The schema is used to assist in data type definitions.
    schema=[
        # Specify the type of columns whose type cannot be auto-detected. For
        # example the "title" column uses pandas dtype "object", so its
        # data type is ambiguous.
        bigquery.SchemaField(header_name[0], bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField(header_name[1], bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField(header_name[2], bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField(header_name[3], bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField(header_name[4], bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField(header_name[5], bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField(header_name[6], bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField(header_name[7], bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField(header_name[8], bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField(header_name[9], bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField(header_name[10], bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField(header_name[11], bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField(header_name[12], bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField(header_name[13], bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField(header_name[14], bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField(header_name[15], bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField(header_name[16], bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField(header_name[17], bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField(header_name[18], bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField(header_name[19], bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField(header_name[20], bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField(header_name[21], bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField(header_name[22], bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField(header_name[23], bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField(header_name[24], bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField(header_name[25], bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField(header_name[26], bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField(header_name[27], bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField(header_name[28], bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField(header_name[29], bigquery.enums.SqlTypeNames.STRING),
         bigquery.SchemaField(header_name[30], bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField(header_name[31], bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField(header_name[32], bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField(header_name[33], bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField(header_name[34], bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField(header_name[35], bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField(header_name[36], bigquery.enums.SqlTypeNames.STRING),
        
    ],
    # Optionally, set the write disposition. BigQuery appends loaded rows
    # to an existing table by default, but with WRITE_TRUNCATE write
    # disposition it replaces the table with the loaded data.
    write_disposition="WRITE_TRUNCATE",
    )


job = bigquery.Client().load_table_from_dataframe(
    dataframe, full_table_path, job_config=job_config
)  # Make an API request.
job.result()  # Wait for the job to complete.


#test data
event={'name': "/whb_01-02-2023.csv"}
bq_batch_transform_loader(event, "gcs")