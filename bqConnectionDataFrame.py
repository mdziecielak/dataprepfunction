import datetime

from google.cloud import bigquery
import pandas as pd
import pytz
import os
from google.cloud import storage
from tempfile import NamedTemporaryFile

# Construct a BigQuery client object.
client = bigquery.Client()

# TODO(developer): Set table_id to the ID of the table to create.
# table_id = "your-project.your_dataset.your_table_name"

header_name = ["Provider", "Resource_ID" ,"Monthly_cost", "Application_name", "Month", "Year"]
dataframe =pd.read_csv("gs://pl-ist-global-finopsdata-kpi/tobqimport/klarity_monthly_202212.csv", names=header_name, header=None, index_col=None)

#dataframe = upload.to_ (index=False)





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
    write_disposition="WRITE_TRUNCATE",
)

job = client.load_table_from_dataframe(
    dataframe, "pl-ist-global-finopsdata.cloud_statistic_house.long_term", job_config=job_config
)  # Make an API request.
job.result()  # Wait for the job to complete.

table = client.get_table("pl-ist-global-finopsdata.cloud_statistic_house.long_term")  # Make an API request.
print(
    "Loaded {} rows and {} columns to {}".format(
        table.num_rows, len(table.schema), "pl-ist-global-finopsdata.cloud_statistic_house.long_term"
    )
)