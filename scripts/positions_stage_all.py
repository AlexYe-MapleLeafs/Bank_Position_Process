#Import Packages
from google.cloud import bigquery


#Initialize variables

PROJECT_ID = 'cidat-10040-int-445d'
DATASET_ID = 'ez_cb_positions_data_interactive'
TABLE_NAME = 'positions_source_all'


query = """
    SELECT * FROM `cidat-10040-int-445d.ez_cb_positions_data_interactive.positions_source_dda`
    UNION ALL
    SELECT * FROM `cidat-10040-int-445d.ez_cb_positions_data_interactive.positions_source_ufdhs`
    UNION ALL
    SELECT * FROM `cidat-10040-int-445d.ez_cb_positions_data_interactive.positions_source_uftas`
    UNION ALL
    SELECT * FROM `cidat-10040-int-445d.ez_cb_positions_data_interactive.positions_source_oll`
    UNION ALL
    SELECT * FROM `cidat-10040-int-445d.ez_cb_positions_data_interactive.positions_source_fal`
    UNION ALL
    SELECT * FROM `cidat-10040-int-445d.ez_cb_positions_data_interactive.positions_source_tls`
--    UNION ALL
--    SELECT * FROM `cidat-10040-int-445d.ez_cb_positions_data_interactive.positions_source_wssmm`
    UNION ALL
    SELECT * FROM `cidat-10040-int-445d.ez_cb_positions_data_interactive.positions_source_infolease`
    UNION ALL
    SELECT * FROM `cidat-10040-int-445d.ez_cb_positions_data_interactive.positions_source_mtroy`
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
