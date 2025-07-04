BEGIN
  -- Create fact_claims if it doesn't exist
  IF NOT EXISTS (
    SELECT * FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_NAME = 'FACT_CLAIMS'
      AND TABLE_SCHEMA = CURRENT_SCHEMA()
  ) THEN
    CREATE TABLE FACT_CLAIMS (
      id INT,
      member_id INT,
      provider_id INT
    );
  END IF;

  -- Create dim_member if it doesn't exist
  IF NOT EXISTS (
    SELECT * FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_NAME = 'DIM_MEMBER'
      AND TABLE_SCHEMA = CURRENT_SCHEMA()
  ) THEN
    CREATE TABLE DIM_MEMBER (
      member_id INT,
      name STRING
    );
  END IF;

  -- Create dim_provider if it doesn't exist
  IF NOT EXISTS (
    SELECT * FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_NAME = 'DIM_PROVIDER'
      AND TABLE_SCHEMA = CURRENT_SCHEMA()
  ) THEN
    CREATE TABLE DIM_PROVIDER (
      provider_id INT,
      name STRING
    );
  END IF;
END;
