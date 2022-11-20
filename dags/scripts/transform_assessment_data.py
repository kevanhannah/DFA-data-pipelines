from scripts.helpers import get_json_from_gcs, save_pandas_object_gcs

def transform_assessment_data(**kwargs):
  import pandas as pd
  # Get JSON from raw bucket/key
  json_data = get_json_from_gcs(
    kwargs['gcp_conn_id'],
    kwargs['bucket_name'],
    kwargs['templates_dict']['load_bucket_key'],
  )

  # Parse JSON into dataframe
  assessments = pd.json_normalize(json_data)

  # Log number records at start
  print(f'Dataframe loaded with {assessments.shape[0]} rows and {assessments.shape[1]} columns')

  keep_columns = [
    'key',
    'fields.status.name',
    'fields.parent.key', # Digital Service Key
    'fields.customfield_13390.value', # Phase
    'fields.customfield_13389.value', # Type
    'fields.customfield_13401.value', # Result
    'fields.customfield_13421.value', # Requires reassessment
    'fields.customfield_13411', # Booking date
    'fields.customfield_13416', # Closing date
    'fields.customfield_13407', # Assessment date
    'fields.customfield_13414', # Material due date
    'fields.customfield_13412', # Pre-chat date
    'fields.customfield_13163.value', # DSS:Accessible
    'fields.customfield_13164.value', # DSS:Agile
    'fields.customfield_13159.value', # DSS:Consistent
    'fields.customfield_13272.value', # DSS:Data
    'fields.customfield_13160.value', # DSS:Design
    'fields.customfield_13167.value', # DSS:Encourage
    'fields.customfield_13161.value', # DSS:First Time
    'fields.customfield_13169.value', # DSS:Measurre
    'fields.customfield_13170.value', # DSS:Ministerr
    'fields.customfield_13165.value', # DSS:Open
    'fields.customfield_13166.value', # DSS:Privacy
    'fields.customfield_13168.value', # DSS:Support
    'fields.customfield_13158.value', # DSS:Team
    'fields.customfield_13162.value', # DSS:Test
    'fields.customfield_13157.value' # DSS:Understand
  ]

  assessment_field_map = {
    'key': 'assessment_key',
    'fields.status.name': 'status',
    'fields.parent.key': 'digital_service_key',
    'fields.customfield_13390.value': 'phase',
    'fields.customfield_13389.value': 'type',
    'fields.customfield_13401.value': 'result',
    'fields.customfield_13421.value': 'reassessment',
    'fields.customfield_13411': 'booking_date',
    'fields.customfield_13416': 'closing_date',
    'fields.customfield_13407': 'assessment_date',
    'fields.customfield_13414': 'materials_due_date',
    'fields.customfield_13412': 'pre_chat_date',
    'fields.customfield_13163.value': 'dss_accessible',
    'fields.customfield_13164.value': 'dss_agile',
    'fields.customfield_13159.value': 'dss_consistent',
    'fields.customfield_13272.value': 'dss_data',
    'fields.customfield_13160.value': 'dss_design',
    'fields.customfield_13167.value': 'dss_encourage',
    'fields.customfield_13161.value': 'dss_ensure',
    'fields.customfield_13169.value': 'dss_measure',
    'fields.customfield_13170.value': 'dss_minister',
    'fields.customfield_13165.value': 'dss_open',
    'fields.customfield_13166.value': 'dss_privacy',
    'fields.customfield_13168.value': 'dss_support',
    'fields.customfield_13158.value': 'dss_team',
    'fields.customfield_13162.value': 'dss_test',
    'fields.customfield_13157.value': 'dss_understand'
  }

  column_order = [
    'assessment_key',
    'digital_service_key',
    'phase',
    'type',
    'status',
    'result',
    'reassessment',
    'booking_date',
    'closing_date',
    'assessment_date',
    'materials_due_date',
    'pre_chat_date',
    'dss_accessible',
    'dss_agile',
    'dss_consistent',
    'dss_data',
    'dss_design',
    'dss_encourage',
    'dss_ensure',
    'dss_measure',
    'dss_minister',
    'dss_open',
    'dss_privacy',
    'dss_support',
    'dss_team',
    'dss_test',
    'dss_understand'
  ]

  # Keep only the columns that we need
  assessments = assessments[keep_columns]

  # Rename columns uusing assessment_fields_map
  assessments.rename(columns=assessment_field_map, errors='raise', inplace=True)

  # Reorder columns
  assessments = assessments[column_order]

  # Set datetime columns
  assessments['booking_date'] = pd.to_datetime(assessments['booking_date'])
  assessments['closing_date'] = pd.to_datetime(assessments['closing_date'])
  assessments['assessment_date'] = pd.to_datetime(assessments['assessment_date'])
  assessments['materials_due_date'] = pd.to_datetime(assessments['materials_due_date'])
  assessments['pre_chat_date'] = pd.to_datetime(assessments['pre_chat_date'])

  # Handle reassessment results
  def set_reassessment_bool(str):
    if str == 'Yes':
      return True
    else:
      return False

  assessments['reassessment'] = assessments['reassessment'].apply(set_reassessment_bool)

  # Handle incompletes
  incomplete_assessments = assessments['status'] != 'Complete'
  assessments['result'] = assessments['result'].mask(incomplete_assessments, 'Not complete')

  assessments['dss_minister'] = assessments['dss_minister'].fillna('Not applicable')
  assessments['dss_encourage'] = assessments['dss_encourage'].fillna('Not applicable')
  assessments.loc[:,'dss_accessible':'dss_understand'] = assessments.loc[:,'dss_accessible':'dss_understand'].fillna('Not complete')

  # Sort the final table
  assessments = assessments.sort_values(by=['assessment_date', 'phase'], ascending=[False, True])
  assessments = assessments.reset_index(drop=True)

  save_pandas_object_gcs(
    kwargs['gcp_conn_id'], kwargs['bucket_name'], kwargs['object_name'], assessments
  )