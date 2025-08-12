# Import Packages
from google.cloud import bigquery


# Initialize variables

PROJECT_ID = 'cidat-10040-int-445d'
DATASET_ID = 'ez_cb_positions_data_interactive'
TABLE_NAME = 'positions_source_dda'


query = """
    SELECT

    Many_Columns

    FROM
        `cidat-prd-dda9.ez_cb_positions_data.ent_positions_dda` d

    WHERE
        d.business_effective_date BETWEEN date_sub(current_date, INTERVAL 750 DAY) AND current_date
        AND COALESCE(b.business_level_2,'Retail') != 'Retail'
        AND (b.business_level_2 != 'Commercial' or (b.business_level_2 = 'Commercial' and c.customer_type_report = 'Commercial'))
    """


# Construct BigQuery objects
client = bigquery.Client()
table_id = '{}.{}.{}'.format(PROJECT_ID, DATASET_ID, TABLE_NAME)


# Upload Dataframe data to BQ Table
job_config = bigquery.QueryJobConfig(write_disposition="WRITE_TRUNCATE",destination=table_id)

job = client.query(query, job_config=job_config)
job.result()  # Wait for the job to complete.

print("Query Uploaded")


table = client.get_table(table_id)  # Make an API request.
print(
    "Loaded {} rows and {} columns to {}".format(
        table.num_rows, len(table.schema), table_id
    )
)