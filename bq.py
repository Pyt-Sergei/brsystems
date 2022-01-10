from google.cloud import bigquery
from google.oauth2 import service_account
import google.api_core

from main import df


credentials = service_account.Credentials.from_service_account_file(
    'my-first-project-335519-ab0d42878c02.json'
)
client = bigquery.Client(credentials=credentials)

table_id = "bitrix.leads_bq"

schema = [
    {'name': 'STATUS_DESCRIPTION', 'type': 'STRING'},
    {'name': 'ORIGINATOR_ID', 'type': 'INTEGER'},
    {'name': 'ORIGIN_ID', 'type': 'INTEGER'},
    {'name': 'ADDRESS', 'type': 'STRING'},
    {'name': 'ADDRESS_2', 'type': 'STRING'},
    {'name': 'ADDRESS_POSTAL_CODE', 'type': 'STRING'},
    {'name': 'ADDRESS_COUNTRY_CODE', 'type': 'STRING'},
    {'name': 'ADDRESS_REGION', 'type': 'STRING'},
    {'name': 'UTM_SOURCE', 'type': 'STRING'},
    {'name': 'UTM_MEDIUM', 'type': 'STRING'},
    {'name': 'UTM_CAMPAIGN', 'type': 'STRING'},
    {'name': 'UTM_CONTENT', 'type': 'STRING'},
    {'name': 'UTM_TERM', 'type': 'STRING'},
]
job_config = bigquery.LoadJobConfig(
    write_disposition='WRITE_TRUNCATE', schema=schema)


try:
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
except google.api_core.exceptions.NotFound:
    dataset_id = table_id.split('.')[0]
    dataset = bigquery.Dataset("%s.%s" % (credentials.project_id, dataset_id))
    client.create_dataset(dataset)

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)


job.result()
