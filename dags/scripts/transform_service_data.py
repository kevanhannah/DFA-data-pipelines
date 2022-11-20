from scripts.helpers import get_json_from_gcs, save_pandas_object_gcs

def transform_service_data(**kwargs):
  import pandas as pd
  # Get JSON from raw bucket/key
  json_data = get_json_from_gcs(
    kwargs['gcp_conn_id'],
    kwargs['bucket_name'],
    kwargs['templates_dict']['load_bucket_key'],
  )

  # Parse JSON into dataframe
  services = pd.json_normalize(json_data, ['fields', ['customfield_10029']], ['key', ['fields', 'summary'], ['fields', 'customfield_13398'], ['fields', 'customfield_13399'], ['fields', 'status', 'name']])

  # Log number records at start
  print(f'Dataframe loaded with {services.shape[0]} rows and {services.shape[1]} columns')

  service_field_map = {
    'key': 'digital_service_key',
    'fields.summary': 'description',
    'fields.status.name': 'status',
    'value': 'ministry', # Ministry
    'fields.customfield_13398': 'request_date', # Request date
    'fields.customfield_13399': 'dismissal_date' # Dismissal date
  }

  column_order = [
    'digital_service_key',
    'ministry',
    'description',
    'status',
    'request_date',
    'dismissal_date'
  ]

  services.drop(['self', 'id'], axis=1, inplace=True)

  services.rename(columns=service_field_map, errors='raise', inplace=True)

  # Set date column type to datetime
  services['request_date'] = pd.to_datetime(services['request_date'])
  services['dismissal_date'] = pd.to_datetime(services['dismissal_date'])

  # Reorder columns
  services = services[column_order]

  # Sort the final table
  services = services.sort_values(by=['request_date', 'ministry'], ascending=[False, True])
  services = services.reset_index(drop=True)

  save_pandas_object_gcs(
    kwargs['gcp_conn_id'], kwargs['bucket_name'], kwargs['object_name'], services
  )