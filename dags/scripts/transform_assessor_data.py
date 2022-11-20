from scripts.helpers import get_json_from_gcs, save_pandas_object_gcs

def transform_assessor_data(**kwargs):
  import pandas as pd
  # Get JSON from raw bucket/key
  json_data = get_json_from_gcs(
    kwargs['gcp_conn_id'],
    kwargs['bucket_name'],
    kwargs['templates_dict']['load_bucket_key'],
  )

  # Parse JSON into dataframe
  assessors = pd.json_normalize(json_data)

  # Log number records at start
  print(f'Dataframe loaded with {assessors.shape[0]} rows and {assessors.shape[1]} columns')

  # Sort the assessors alphabetically
  assessors = assessors.sort_values('fields.Name')

  # Get a list of all disciplines and add each to the dataframe as columns of booleans
  disciplined_assessors = assessors[assessors['fields.Disciplines'].notna()].reset_index() # First, filter only for assessors that have a discipline (sometimes they have none recorded)
  pivot_disciplines = disciplined_assessors.reset_index().explode('fields.Disciplines').pivot_table(index='index', values='id', columns='fields.Disciplines', aggfunc='any', fill_value=False).reset_index().rename_axis(None, axis=1) # Second, pivot the disciplines field to get the boolean columns
  assessors = assessors.merge(pivot_disciplines, left_index=True, right_on='index', how='left').reset_index() # Third, merge the pivot table back into the list of assessors
  assessors.loc[:, 'Accessibility':'UX'] = assessors.loc[:, 'Accessibility':'UX'].fillna(False) # Fourth, fill the assessors with no set disciplines as False

  assessors_field_map = {
    'id': 'airtable_id',
    'createdTime': 'created_time',
    'fields.Ministry': 'ministry',
    'fields.Status': 'status',
    'fields.Type': 'type',
    'fields.Email': 'email',
    'fields.First Name': 'first_name',
    'fields.Last Name': 'last_name',
    'fields.Name': 'full_name',
    'fields.Last Modified': 'last_modified_time',
    'fields.Division': 'division',
    'Accessibility': 'accessibility_assessor',
    'Content': 'content_assessor',
    'Data': 'data_assessor',
    'Lead': 'lead_assessor',
    'Lean': 'lean_assessor',
    'Measurement': 'measurement_assessor',
    'Policy': 'policy_assessor',
    'Product': 'product_assessor',
    'Tech': 'tech_assessor',
    'UX': 'ux_assessor'
  }

  keep_columns = [
    'email',
    'airtable_id',
    'first_name',
    'last_name',
    'ministry',
    'division',
    'status',
    'type',
    'accessibility_assessor',
    'content_assessor',
    'data_assessor',
    'lead_assessor',
    'lean_assessor',
    'measurement_assessor',
    'policy_assessor',
    'product_assessor',
    'tech_assessor',
    'ux_assessor',
    'created_time',
    'last_modified_time'
  ]

  # Rename columns using assessors_field_map
  assessors.rename(columns=assessors_field_map, errors='raise', inplace=True)

  # Keep only the columns that we need and reorder them
  assessors = assessors[keep_columns]

  # Format the email column to lowercase
  assessors['email'] = assessors['email'].str.lower()

  # Set datetime columns
  assessors['created_time'] = pd.to_datetime(assessors['created_time'])
  assessors['last_modified_time'] = pd.to_datetime(assessors['last_modified_time'])

  # Handle incompletes
  assessors.loc[:,'ministry':'division'] = assessors.loc[:,'ministry':'division'].fillna('n/a')

  # Sort the final table
  assessors = assessors.sort_values(by=['first_name', 'last_name'], ascending=[True, True])
  assessors = assessors.reset_index(drop=True)

  save_pandas_object_gcs(
    kwargs['gcp_conn_id'], kwargs['bucket_name'], kwargs['object_name'], assessors
  )