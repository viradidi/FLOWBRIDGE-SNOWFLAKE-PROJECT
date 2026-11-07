
----------------------------------------------------------------------
-- FLOWBRIDGE SUPPLY CHAIN DATA PROJECT
-- Script : 02_bronze/bronze.sql
-- Purpose:
--   1. Azure Blob Storage integration
--   2. Azure Queue notification integration
--   3. JSON file format
--   4. External stage
--   5. Bronze RAW_ORDERS table
--   6. Snowpipe auto-ingestion
--   7. Historical file ingestion
--   8. Pipeline diagnostics
--
-- Azure Storage Account : storageflowbridge
-- Azure Blob Container  : supply-chain-raw-dev
-- Azure Queue           : flowbridge-supply-chain-queue
-- Database              : FLOWBRIDGE_DEV_DB
-- Schema                : BRONZE_SCH
-- Warehouse             : FLOWBRIDGE_PIPELINE_WH
--
-- Run account-level commands as ACCOUNTADMIN.
-- Run pipeline commands as SYSADMIN.
----------------------------------------------------------------------


----------------------------------------------------------------------
-- PHASE 0: ENVIRONMENT
----------------------------------------------------------------------

USE ROLE ACCOUNTADMIN;

USE DATABASE FLOWBRIDGE_DEV_DB;


----------------------------------------------------------------------
-- PHASE 1: AZURE BLOB STORAGE INTEGRATION
----------------------------------------------------------------------

CREATE STORAGE INTEGRATION IF NOT EXISTS FLOWBRIDGE_ADLS_INTEGRATION
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = AZURE
    ENABLED = TRUE
    AZURE_TENANT_ID = '<AZURE_TENANT_ID>'
    STORAGE_ALLOWED_LOCATIONS = (
        'azure://storageflowbridge.blob.core.windows.net/supply-chain-raw-dev/',
        'azure://storageflowbridge.blob.core.windows.net/supply-chain-raw-prod/'
    );

-- Retrieve Azure identity and consent details.
-- Replace <AZURE_TENANT_ID> with the target Azure tenant before deployment.
DESC STORAGE INTEGRATION FLOWBRIDGE_ADLS_INTEGRATION;

-- Grant access to the execution role.
GRANT USAGE ON INTEGRATION FLOWBRIDGE_ADLS_INTEGRATION
    TO ROLE SYSADMIN;


----------------------------------------------------------------------
-- PHASE 2: AZURE QUEUE NOTIFICATION INTEGRATION
----------------------------------------------------------------------

-- IMPORTANT:
-- Confirm this is the actual Azure QUEUE URL,
-- not the Blob container URL.
--
-- Expected format:
-- https://storage-account.queue.core.windows.net/queue-name

CREATE NOTIFICATION INTEGRATION IF NOT EXISTS
    FLOWBRIDGE_AZURE_NOTIFICATIONS_INT
    ENABLED = TRUE
    TYPE = QUEUE
    NOTIFICATION_PROVIDER = AZURE_STORAGE_QUEUE
    AZURE_STORAGE_QUEUE_PRIMARY_URI =
        'https://storageflowbridge.queue.core.windows.net/flowbridge-supply-chain-queue'
    AZURE_TENANT_ID = '<AZURE_TENANT_ID>';

-- Retrieve the Azure consent URL and application name.
DESC NOTIFICATION INTEGRATION
    FLOWBRIDGE_AZURE_NOTIFICATIONS_INT;

-- Allow SYSADMIN to use the notification integration.
GRANT USAGE ON INTEGRATION
    FLOWBRIDGE_AZURE_NOTIFICATIONS_INT
    TO ROLE SYSADMIN;


----------------------------------------------------------------------
-- PHASE 3: PIPELINE INFRASTRUCTURE
----------------------------------------------------------------------

USE ROLE SYSADMIN;

USE SECONDARY ROLES ALL;

USE WAREHOUSE FLOWBRIDGE_PIPELINE_WH;

USE DATABASE FLOWBRIDGE_DEV_DB;

CREATE SCHEMA IF NOT EXISTS BRONZE_SCH;

USE SCHEMA BRONZE_SCH;


----------------------------------------------------------------------
-- PHASE 4: JSON FILE FORMAT
----------------------------------------------------------------------

CREATE FILE FORMAT IF NOT EXISTS JSON_FILE_FORMAT
    TYPE = JSON
    STRIP_OUTER_ARRAY = TRUE
    COMMENT = 'Flowbridge Bronze JSON ingestion format';

DESC FILE FORMAT JSON_FILE_FORMAT;


----------------------------------------------------------------------
-- PHASE 5: EXTERNAL AZURE STAGE
----------------------------------------------------------------------

CREATE STAGE IF NOT EXISTS ADLS_RAW_STAGE
    URL = 'azure://storageflowbridge.blob.core.windows.net/supply-chain-raw-dev/'
    STORAGE_INTEGRATION = FLOWBRIDGE_ADLS_INTEGRATION
    FILE_FORMAT = JSON_FILE_FORMAT
    COMMENT = 'Flowbridge development Azure raw data stage';

-- Verify that Snowflake can list the Azure files.
LIST @ADLS_RAW_STAGE;


----------------------------------------------------------------------
-- PHASE 6: BRONZE RAW ORDERS TABLE
----------------------------------------------------------------------

CREATE TRANSIENT TABLE IF NOT EXISTS RAW_ORDERS (
    RAW_DATA VARIANT,
    INGESTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FILE_NAME STRING,
    FILE_ROW_NUMBER NUMBER,
    LOAD_ID STRING DEFAULT UUID_STRING()
)
COMMENT = 'Flowbridge Bronze layer raw supply chain orders';


----------------------------------------------------------------------
-- PHASE 7: SNOWPIPE AUTO-INGESTION
----------------------------------------------------------------------

CREATE PIPE IF NOT EXISTS SUPPLY_CHAIN_PIPE
    AUTO_INGEST = TRUE
    INTEGRATION = 'FLOWBRIDGE_AZURE_NOTIFICATIONS_INT'
    COMMENT = 'Flowbridge Azure Blob to Snowflake automatic ingestion'
AS
COPY INTO FLOWBRIDGE_DEV_DB.BRONZE_SCH.RAW_ORDERS
(
    RAW_DATA,
    FILE_NAME,
    FILE_ROW_NUMBER
)
FROM
(
    SELECT
        $1,
        METADATA$FILENAME,
        METADATA$FILE_ROW_NUMBER
    FROM @FLOWBRIDGE_DEV_DB.BRONZE_SCH.ADLS_RAW_STAGE
)
FILE_FORMAT = (
    FORMAT_NAME = 'FLOWBRIDGE_DEV_DB.BRONZE_SCH.JSON_FILE_FORMAT'
);


----------------------------------------------------------------------
-- PHASE 8: PIPE CONFIGURATION
----------------------------------------------------------------------

-- Confirm pipe definition.
SHOW PIPES LIKE 'SUPPLY_CHAIN_PIPE'
    IN SCHEMA FLOWBRIDGE_DEV_DB.BRONZE_SCH;

-- Check operational status.
SELECT PARSE_JSON(
    SYSTEM$PIPE_STATUS(
        'FLOWBRIDGE_DEV_DB.BRONZE_SCH.SUPPLY_CHAIN_PIPE'
    )
) AS PIPE_STATUS;


----------------------------------------------------------------------
-- PHASE 9: HISTORICAL FILE INGESTION
----------------------------------------------------------------------

-- Refresh the pipe to discover existing staged files.
-- Snowpipe REFRESH only discovers files within its supported
-- refresh window (up to the previous 7 days).

ALTER PIPE FLOWBRIDGE_DEV_DB.BRONZE_SCH.SUPPLY_CHAIN_PIPE
    REFRESH;


----------------------------------------------------------------------
-- PHASE 10: DATA VALIDATION
----------------------------------------------------------------------

-- Check whether records have arrived.
SELECT *
FROM FLOWBRIDGE_DEV_DB.BRONZE_SCH.RAW_ORDERS
ORDER BY INGESTED_AT DESC
LIMIT 20;


-- Count ingested records.
SELECT
    COUNT(*) AS TOTAL_INGESTED_RECORDS,
    COUNT(DISTINCT FILE_NAME) AS TOTAL_FILES_LOADED
FROM FLOWBRIDGE_DEV_DB.BRONZE_SCH.RAW_ORDERS;


-- Check ingestion by file.
SELECT
    FILE_NAME,
    COUNT(*) AS ROWS_INGESTED,
    MIN(INGESTED_AT) AS FIRST_INGESTION,
    MAX(INGESTED_AT) AS LAST_INGESTION
FROM FLOWBRIDGE_DEV_DB.BRONZE_SCH.RAW_ORDERS
GROUP BY FILE_NAME
ORDER BY LAST_INGESTION DESC;


----------------------------------------------------------------------
-- PHASE 11: COPY HISTORY AND ERROR DIAGNOSTICS
----------------------------------------------------------------------

SELECT
    FILE_NAME,
    STATUS,
    ROW_COUNT,
    ROW_PARSED,
    FIRST_ERROR_MESSAGE,
    LAST_LOAD_TIME
FROM TABLE(
    FLOWBRIDGE_DEV_DB.INFORMATION_SCHEMA.COPY_HISTORY(
        TABLE_NAME => 'RAW_ORDERS',
        START_TIME => DATEADD(HOUR, -24, CURRENT_TIMESTAMP())
    )
)
ORDER BY LAST_LOAD_TIME DESC;


----------------------------------------------------------------------
-- PHASE 12: FINAL PIPELINE STATUS
----------------------------------------------------------------------

SELECT
    SYSTEM$PIPE_STATUS(
        'FLOWBRIDGE_DEV_DB.BRONZE_SCH.SUPPLY_CHAIN_PIPE'
    ) AS FINAL_PIPE_STATUS;


----------------------------------------------------------------------
-- END OF FLOWBRIDGE BRONZE PIPELINE SCRIPT
----------------------------------------------------------------------

select * from bronze_sch.raw_orders;

-- Check ingestion history
select * 
    from table(information_schema.copy_history(
        table_name => 'raw_orders',
        start_time => dateadd(hours,-1,current_timestamp())
    ));
