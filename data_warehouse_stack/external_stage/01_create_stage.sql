CREATE OR REPLACE STAGE my_stage
  URL = 's3://your-bucket/path/'
  STORAGE_INTEGRATION = my_s3_integration;
