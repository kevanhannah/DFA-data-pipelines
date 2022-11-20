from airflow.models import BaseOperator
from airflow.providers.http.hooks.http import HttpHook
# from requests.auth import HTTPBasicAuth
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import json
from datetime import timedelta
from ratelimit import limits, sleep_and_retry

class AirtableToGCSOperator(BaseOperator):
  def __init__(
    self,
    bucket_name,
    object_name,
    endpoint,
    gcp_conn_id,
    headers,
    http_conn_id,
    **kwargs):
    super().__init__(**kwargs)
    self.bucket_name = bucket_name
    self.object_name = object_name
    self.endpoint = endpoint
    self.gcp_conn_id = gcp_conn_id
    self.headers = headers or {}
    self.http_conn_id = http_conn_id

  @staticmethod
  # Airtable API calls allow you to request 100 records at a time
  @sleep_and_retry
  @limits(calls=5, period=timedelta(seconds=1).total_seconds())
  def get_airtable_records(self, http):
    records = []
    offset = None

    # Initial record load
    response = http.run(
      endpoint=self.endpoint, headers=self.headers
    )

    r_json = json.loads(response.text)
    records.extend(r_json['records'])

    # Check for additional records on the next page
    if 'offset' in r_json:
      offset = r_json['offset']

    # Pagination loop
    while offset is not None:
      next_response = http.run(
        endpoint=f'{self.endpoint}&offset={offset}', headers=self.headers
      )
      next_json_response = json.loads(next_response.text)
      records.extend(next_json_response['records'])
      print(json.dumps(next_json_response, indent=4))

      if 'offset' in next_json_response:
        offset = next_json_response['offset']
      else:
        offset = None

    return records

  def execute(self, context):
    http = HttpHook(
      method='GET', http_conn_id=self.http_conn_id
    )

    # Pull data from Jira
    airtable_data = self.get_airtable_records(self, http)

    # Save assessment data to GCS
    save_object_key = f'{self.object_name}.json'
    gcs_hook = GCSHook(gcp_conn_id=self.gcp_conn_id)
    gcs_hook.upload(
      bucket_name=self.bucket_name,
      object_name=save_object_key,
      data=json.dumps(airtable_data, indent=4),
    )

    # Push GCS key
    context["ti"].xcom_push(key='gcs_object_key', value=save_object_key)