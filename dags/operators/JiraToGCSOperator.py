from airflow.models import BaseOperator
from airflow.providers.http.hooks.http import HttpHook
from requests.auth import HTTPBasicAuth
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import json

class JiraToGCSOperator(BaseOperator):
  template_fields = ['jqlQuery']

  def __init__(
    self,
    bucket_name,
    object_name,
    endpoint,
    fields,
    gcp_conn_id,
    headers,
    http_conn_id,
    jqlQuery,
    **kwargs):
    super().__init__(**kwargs)
    self.bucket_name = bucket_name
    self.object_name = object_name
    self.endpoint = endpoint
    self.fields = fields
    self.gcp_conn_id = gcp_conn_id
    self.headers = headers or {}
    self.http_conn_id = http_conn_id
    self.jqlQuery = jqlQuery

  @staticmethod
  # Jira API calls allow you to request 100 issues at a time
  def get_jira_issues(self, http):
    start = 0
    max = 100
    issues = []

    requestBody = {
      'jql': self.jqlQuery,
      'fields': self.fields,
      'startAt': start,
      'maxResults': max
      }

    while True:
      requestBody = {
        'jql': self.jqlQuery,
        'fields': self.fields,
        'startAt': start,
        'maxResults': max
      }

      response = http.run(
        endpoint=self.endpoint, data=json.dumps(requestBody, indent=4), headers=self.headers
      )

      r_json = json.loads(response.text)
      issues.extend(r_json['issues'])
      start += max

      if start > r_json['total']:
        return issues

  def execute(self, context):
    http = HttpHook(
      method='POST', http_conn_id=self.http_conn_id, auth_type=HTTPBasicAuth
    )

    # Pull data from Jira
    jira_data = self.get_jira_issues(self, http)

    # Save assessment data to GCS
    save_object_key = f'{self.object_name}.json'
    gcs_hook = GCSHook(gcp_conn_id=self.gcp_conn_id)
    gcs_hook.upload(
      bucket_name=self.bucket_name,
      object_name=save_object_key,
      data=json.dumps(jira_data, indent=4),
    )

    # Push GCS key
    context["ti"].xcom_push(key='gcs_object_key', value=save_object_key)