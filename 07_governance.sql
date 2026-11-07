-- ENVIRONMENT SETUP
USE ROLE accountadmin;

-- Dedicated least-privilege role for dashboard consumers.
CREATE ROLE IF NOT EXISTS FLOWBRIDGE_ANALYTICS_ROLE;
GRANT USAGE ON WAREHOUSE FLOWBRIDGE_ANALYTICS_WH TO ROLE FLOWBRIDGE_ANALYTICS_ROLE;
GRANT USAGE ON DATABASE FLOWBRIDGE_DEV_DB TO ROLE FLOWBRIDGE_ANALYTICS_ROLE;
GRANT USAGE ON SCHEMA FLOWBRIDGE_DEV_DB.SERVING_SCH TO ROLE FLOWBRIDGE_ANALYTICS_ROLE;
GRANT SELECT ON ALL VIEWS IN SCHEMA FLOWBRIDGE_DEV_DB.SERVING_SCH TO ROLE FLOWBRIDGE_ANALYTICS_ROLE;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA FLOWBRIDGE_DEV_DB.SERVING_SCH TO ROLE FLOWBRIDGE_ANALYTICS_ROLE;

USE DATABASE flowbridge_dev_db;
USE WAREHOUSE flowbridge_pipeline_wh;

-- STEP 1 - EMAIL NOTIFICATION INTEGRATION
CREATE NOTIFICATION INTEGRATION IF NOT EXISTS email_notification_int
    TYPE = EMAIL
    ENABLED = TRUE
    COMMENT = 'Email Notification Integration for pipeline health alerts';

-- Verify the intended recipient in Snowsight before enabling this alert.
-- Replace the explicit placeholder in the alert action below with a verified
-- Snowflake account-user email during deployment. Never commit personal addresses.


CREATE OR REPLACE ALERT bronze_sch.pipeline_health_alert
    WAREHOUSE = flowbridge_pipeline_wh
    SCHEDULE = '5 minute'
IF (EXISTS (
    -- --------- Bronze - Snowpipe Failure ---------
    SELECT 1
    FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
        table_name => 'RAW_ORDERS',
        start_time => DATEADD(hour, -1, CURRENT_TIMESTAMP())
    ))
    WHERE status = 'Load Failed'

    UNION ALL

    -- --------- Silver - 2 tasks failure ---------
    SELECT 1 
    FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
        scheduled_time_range_start => DATEADD(hour, -1, CURRENT_TIMESTAMP())
    ))
    WHERE state = 'FAILED'
      AND database_name = 'flowbridge_dev_db'

    UNION ALL

    -- --------- Gold - DT's Failure ---------
    SELECT 1 
    FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY())
    WHERE schema_name = 'GOLD_SCH'
      AND database_name = 'flowbridge_dev_db'
      AND state = 'FAILED'
      AND refresh_start_time > DATEADD(minute, -5, CURRENT_TIMESTAMP())
))
THEN CALL SYSTEM$SEND_EMAIL(
    'email_notification_int',
    '<VERIFIED_ACCOUNT_USER_EMAIL>',
    'Pipeline Alert! - Flowbridge Project!',
    'Something went wrong in the pipeline. Check Bronze/Silver/Gold Layers for failures. Login to snowsight -> monitoring -> History / Copy History'
);


alter alert bronze_sch.pipeline_health_alert resume;


show alerts in database flowbridge_dev_db;

-- Check alert history
SELECT 
    * 
FROM 
    TABLE(INFORMATION_SCHEMA.ALERT_HISTORY(
        scheduled_time_range_start => DATEADD(hour, -1, CURRENT_TIMESTAMP())
    ))
ORDER BY 
    scheduled_time DESC
LIMIT 10;


show resource monitors;
