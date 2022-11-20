import os
from datetime import datetime
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from operators.JiraToGCSOperator import JiraToGCSOperator
from operators.AirtableToGCSOperator import AirtableToGCSOperator
from scripts.transform_assessment_data import transform_assessment_data
from scripts.transform_service_data import transform_service_data
from scripts.transform_jira_airtable_facts import transform_assessor_fact
from scripts.transform_assessor_data import transform_assessor_data
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from scripts.config import config

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET")
AIRTABLE_BASE = os.environ.get("AIRTABLE_BASE")
AIRTABLE_TABLE = os.environ.get("AIRTABLE_TABLE")
AIRTABLE_KEY = os.environ.get("AIRTABLE_KEY")

dfa_jira_airtable_pipeline = DAG(
  dag_id='dfa_jira_airtable_pipeline',
  schedule_interval=None,
  start_date=datetime(2022, 6, 10),
  tags=['dfa', 'jira', 'airtable', 'bigquery']
)

with dfa_jira_airtable_pipeline:
  with TaskGroup(group_id="extract_jira_data") as extract_jira_data:
    assessments = JiraToGCSOperator(
      task_id = 'assessments',
      bucket_name=config.BUCKET_NAME,
      object_name=f'{config.PATH_TO_RAW}jira/assessments',
      endpoint = 'rest/api/3/search',
      fields = config.ASSESSMENT_FIELDS,
      gcp_conn_id='GOOGLE_CLOUD_DEFAULT',
      headers = {"Content-Type": "application/json"},
      http_conn_id = 'JIRA_API',
      jqlQuery = 'project = DA AND issuetype = Assessment ORDER BY created DESC'
    )

    services = JiraToGCSOperator(
      task_id = 'services',
      bucket_name = config.BUCKET_NAME,
      object_name = f'{config.PATH_TO_RAW}jira/services',
      endpoint = 'rest/api/3/search',
      fields = config.SERVICE_FIELDS,
      gcp_conn_id = 'GOOGLE_CLOUD_DEFAULT',
      headers = {"Content-Type": "application/json"},
      http_conn_id = 'JIRA_API',
      jqlQuery = 'project = DA AND type = "Digital Service" ORDER BY created DESC'
    )

    assessors = JiraToGCSOperator(
      task_id = 'assessors',
      bucket_name = config.BUCKET_NAME,
      object_name = f'{config.PATH_TO_RAW}jira/assessors',
      endpoint = 'rest/api/3/search',
      fields = config.ASSESSOR_FIELDS,
      gcp_conn_id = 'GOOGLE_CLOUD_DEFAULT',
      headers = {"Content-Type": "application/json"},
      http_conn_id = 'JIRA_API',
      jqlQuery = 'project = DA AND issuetype = Assessment ORDER BY created DESC'
    )

  with TaskGroup(group_id="extract_airtable_data") as extract_airtable_data:
    assessors = AirtableToGCSOperator(
      task_id = 'assessors',
      bucket_name = config.BUCKET_NAME,
      object_name = f'{config.PATH_TO_RAW}airtable/assessors',
      endpoint = f'/v0/{AIRTABLE_BASE}/{AIRTABLE_TABLE}?api_key={AIRTABLE_KEY}',
      gcp_conn_id = 'GOOGLE_CLOUD_DEFAULT',
      headers = {"Content-Type": "application/json"},
      http_conn_id = 'AIRTABLE_API'
    )

  with TaskGroup(group_id="transform_jira_data") as transform_jira_data:
    assessments = PythonOperator(
      task_id='assessments',
      python_callable=transform_assessment_data,
      op_kwargs = {
        'gcp_conn_id': 'GOOGLE_CLOUD_DEFAULT',
        'bucket_name': config.BUCKET_NAME,
        'object_name': f'{config.PATH_TO_STAGE}jira/assessments'
      },
      templates_dict = {
        'load_bucket_key': "{{ ti.xcom_pull(key='gcs_object_key', task_ids='extract_jira_data.assessments') }}",
      },
    )

    services = PythonOperator(
      task_id='services',
      python_callable=transform_service_data,
      op_kwargs = {
        'gcp_conn_id': 'GOOGLE_CLOUD_DEFAULT',
        'bucket_name': config.BUCKET_NAME,
        'object_name': f'{config.PATH_TO_STAGE}jira/services'
      },
      templates_dict = {
        'load_bucket_key': "{{ ti.xcom_pull(key='gcs_object_key', task_ids='extract_jira_data.services') }}",
      },
    )

    assessors = PythonOperator(
      task_id='assessor_facts',
      python_callable=transform_assessor_fact,
      op_kwargs = {
        'gcp_conn_id': 'GOOGLE_CLOUD_DEFAULT',
        'bucket_name': config.BUCKET_NAME,
        'object_name': f'{config.PATH_TO_STAGE}jira/assessors'
      },
      templates_dict = {
        'load_bucket_key': "{{ ti.xcom_pull(key='gcs_object_key', task_ids='extract_jira_data.assessors') }}",
      },
    )

  with TaskGroup(group_id="transform_airtable_data") as transform_airtable_data:
    assessors = PythonOperator(
      task_id='assessors',
      python_callable=transform_assessor_data,
      op_kwargs = {
        'gcp_conn_id': 'GOOGLE_CLOUD_DEFAULT',
        'bucket_name': config.BUCKET_NAME,
        'object_name': f'{config.PATH_TO_STAGE}airtable/assessors'
      },
      templates_dict = {
        'load_bucket_key': "{{ ti.xcom_pull(key='gcs_object_key', task_ids='extract_airtable_data.assessors') }}",
      },
  )

  with TaskGroup(group_id='load_jira_to_bigquery') as load_jira_to_bigquery:
    load_assessments = GCSToBigQueryOperator(
      task_id='load_assessments',
      bucket=config.BUCKET_NAME,
      source_objects=['stage/jira/assessments.parquet'],
      source_format = "PARQUET",
      destination_project_dataset_table=f'{BIGQUERY_DATASET}.assessments',
      write_disposition='WRITE_TRUNCATE',
    )

    load_services = GCSToBigQueryOperator(
      task_id='load_services',
      bucket=config.BUCKET_NAME,
      source_objects=['stage/jira/services.parquet'],
      source_format = "PARQUET",
      destination_project_dataset_table=f'{BIGQUERY_DATASET}.services',
      write_disposition='WRITE_TRUNCATE',
    )

    load_assessors = GCSToBigQueryOperator(
      task_id='load_assessors',
      bucket=config.BUCKET_NAME,
      source_objects=['stage/jira/assessors.parquet'],
      source_format = "PARQUET",
      destination_project_dataset_table=f'{BIGQUERY_DATASET}.assessor_facts',
      write_disposition='WRITE_TRUNCATE',
    )

  with TaskGroup(group_id='load_airtable_to_bigquery') as load_airtable_to_bigquery:
    load_assessments = GCSToBigQueryOperator(
      task_id='load_assessors',
      bucket=config.BUCKET_NAME,
      source_objects=['stage/airtable/assessors.parquet'],
      source_format = "PARQUET",
      destination_project_dataset_table=f'{BIGQUERY_DATASET}.assessors',
      write_disposition='WRITE_TRUNCATE',
    )

  begin = DummyOperator(task_id='begin')
  end = DummyOperator(task_id="end", trigger_rule="none_failed_min_one_success")


  begin >> extract_jira_data >> transform_jira_data >> load_jira_to_bigquery >> end
  begin >> extract_airtable_data >> transform_airtable_data >> load_airtable_to_bigquery >> end
