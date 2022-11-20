from scripts.helpers import get_json_from_gcs, save_pandas_object_gcs

def transform_assessor_fact(**kwargs):
  import pandas as pd
  import numpy as np

  # Get JSON from raw bucket/key
  json_data = get_json_from_gcs(
    kwargs['gcp_conn_id'],
    kwargs['bucket_name'],
    kwargs['templates_dict']['load_bucket_key'],
  )

  # Parse JSON into dataframe
  df = pd.json_normalize(
    json_data,
    record_path=['fields', 'customfield_13403'], # Lead assessor
    meta=[
      'key',
      ['fields', 'customfield_13408'], # Digital Service Key
    ],
    errors='ignore'
  )

  assessor_field_map = {
    'value': 'lead_assessor',
    'fields.customfield_13408': 'digital_service_key',
  }

  column_order = [
    'key',
    'digital_service_key',
    'lead_assessor'
  ]

  df.rename(columns=assessor_field_map, errors='raise', inplace=True)
  df.drop(['self', 'id'], axis=1, inplace=True)
  df = df[column_order]

  assessor_types = [
    {
      'field': 'customfield_13404',
      'label': 'product_assessors'
    },
    {
      'field': 'customfield_13405',
      'label': 'ux_assessors'
    },
    {
      'field': 'customfield_13406',
      'label': 'tech_assessors'
    },
    {
      'field': 'customfield_13437',
      'label': 'data_assessors'
    },
    {
      'field': 'customfield_13402',
      'label': 'content_assessors'
    },
    {
      'field': 'customfield_13422',
      'label': 'accessibility_assessors'
    },
    {
      'field': 'customfield_13409',
      'label': 'policy_assessors'
    },
    {
      'field': 'customfield_13417',
      'label': 'lean_assessors'
    }
  ]

  for type in assessor_types:
    type_assessors = pd.json_normalize(
      json_data,
      record_path=['fields', type['field']],
      meta=['key'],
      errors='ignore'
    )

    if (type_assessors.shape[0] > 1):
      type_assessors.drop(['self', 'id'], axis=1, inplace=True)
      type_assessors.rename(columns={'value': type['label']}, errors='raise', inplace=True)
      df = pd.merge(df, type_assessors, how='left', on='key')

  df_columns = df.iloc[:, 2:].columns.values.tolist()

  for column in df_columns:
    duplicates = df.duplicated(subset=['key', column])
    df[column].loc[duplicates] = np.nan

  assessor_fact = df.melt(id_vars=['key'], value_vars=['lead_assessor', 'product_assessors',	'ux_assessors',	'tech_assessors',	'content_assessors',	'accessibility_assessors',	'policy_assessors',	'lean_assessors']).dropna(subset=['value'])

  assessor_fact.loc[(assessor_fact.variable == 'lead_assessor'), 'variable'] = 'Lead'
  assessor_fact.loc[(assessor_fact.variable == 'product_assessors'), 'variable'] = 'Product'
  assessor_fact.loc[(assessor_fact.variable == 'ux_assessors'), 'variable'] = 'UX'
  assessor_fact.loc[(assessor_fact.variable == 'tech_assessors'), 'variable'] = 'Tech'
  assessor_fact.loc[(assessor_fact.variable == 'data_assessors'), 'variable'] = 'Data'
  assessor_fact.loc[(assessor_fact.variable == 'content_assessors'), 'variable'] = 'Content'
  assessor_fact.loc[(assessor_fact.variable == 'accessibility_assessors'), 'variable'] = 'Accessibility'
  assessor_fact.loc[(assessor_fact.variable == 'policy_assessors'), 'variable'] = 'Policy'
  assessor_fact.loc[(assessor_fact.variable == 'lean_assessors'), 'variable'] = 'Lean'

  assessor_fact = assessor_fact.reset_index(drop=True)

  assessor_fact['variable'] = assessor_fact['variable'].astype('category')

  assessor_fact = assessor_fact.rename(columns={
    'key': 'assessment_key',
    'variable':'role',
    'value':'name',
  })

  save_pandas_object_gcs(
    kwargs['gcp_conn_id'], kwargs['bucket_name'], kwargs['object_name'], assessor_fact
  )