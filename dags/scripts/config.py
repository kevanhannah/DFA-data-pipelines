class config:
  BUCKET_NAME = 'data_lake_dfa_dashboards'
  PATH_TO_RAW = 'raw/'
  PATH_TO_STAGE = 'stage/'
  ASSESSMENT_FIELDS = [
    'status',
    'parent',
    'customfield_13390', # Phase
    'customfield_13389', # Type
    'customfield_13401', # Result
    'customfield_13421', # Requires reassessment
    'customfield_13411', # Booking date
    'customfield_13416', # Closing date
    'customfield_13407', # Assessment date
    'customfield_13414', # Material due date
    'customfield_13412', # Pre-chat date
    'customfield_13163', # DSS:Accessible
    'customfield_13164', # DSS:Agile
    'customfield_13159', # DSS:Consistent
    'customfield_13272', # DSS:Data
    'customfield_13160', # DSS:Design
    'customfield_13167', # DSS:Encourage
    'customfield_13161', # DSS:First Time
    'customfield_13169', # DSS:Measurre
    'customfield_13170', # DSS:Minister
    'customfield_13165', # DSS:Open
    'customfield_13166', # DSS:Privacy
    'customfield_13168', # DSS:Support
    'customfield_13158', # DSS:Team
    'customfield_13162', # DSS:Test
    'customfield_13157' # DSS:Understand
  ]
  ASSESSOR_FIELDS = [
    'customfield_13408', # Digital Service Key
    'customfield_13403', # Lead assessors
    'customfield_13404', # Product assessors
    'customfield_13405', # UX assessors
    'customfield_13406', # Tech assessors
    'customfield_13437', # Data assessors
    'customfield_13402', # Content assessors
    'customfield_13422', # Accessibility assessors
    'customfield_13409', # Policy assessors
    'customfield_13417', # Lean assessors
  ]
  SERVICE_FIELDS = [
    'key',
    'summary',
    'status',
    'customfield_10029', # Ministry
    'customfield_13398', # Request date
    'customfield_13399' # Dismissal date
  ]