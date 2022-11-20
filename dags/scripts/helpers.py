from airflow.providers.google.cloud.hooks.gcs import GCSHook

def get_json_from_gcs(gcp_conn_id, bucket_name, object_name):
  import json

  gcs_hook = GCSHook(gcp_conn_id=gcp_conn_id)
  file_content = gcs_hook.download(bucket_name=bucket_name, object_name=object_name)
  json_data = json.loads(file_content)
  return json_data

def save_pandas_object_gcs(gcp_conn_id, bucket_name, object_name, pd_dataframe=None):
  
  if pd_dataframe is None:
      return None

  data = pd_dataframe.to_parquet()

  print(
    f'Saving pandas object:\nGCP CONN ID: {gcp_conn_id}\nBUCKET: {bucket_name}\nOBJECT: {object_name}\n'
  )

  # Save data
  gcs_hook = GCSHook(gcp_conn_id=gcp_conn_id)
  gcs_hook.upload(
    bucket_name=bucket_name,
    object_name=f'{object_name}.parquet',
    data=data
  )