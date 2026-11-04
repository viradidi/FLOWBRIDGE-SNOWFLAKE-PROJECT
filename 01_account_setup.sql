use role accountadmin;

create database if not exists flowbridge_dev_db;

create database if not exists flowbridge_prod_db;

CREATE SCHEMA IF NOT EXISTS flowbridge_dev_db.bronze_sch
  COMMENT = 'Raw Ingestion Layer';

CREATE SCHEMA IF NOT EXISTS flowbridge_dev_db.silver_sch
  COMMENT = 'Transformation Layer';

CREATE SCHEMA IF NOT EXISTS flowbridge_dev_db.gold_sch
  COMMENT = 'Aggregation Layer';

CREATE SCHEMA IF NOT EXISTS flowbridge_dev_db.serving_sch
  COMMENT = 'Serving Layer';


CREATE SCHEMA IF NOT EXISTS flowbridge_prod_db.bronze_sch
  COMMENT = 'Raw Ingestion Layer';

CREATE SCHEMA IF NOT EXISTS flowbridge_prod_db.silver_sch
  COMMENT = 'Transformation Layer';

CREATE SCHEMA IF NOT EXISTS flowbridge_prod_db.gold_sch
  COMMENT = 'Aggregation Layer';

CREATE SCHEMA IF NOT EXISTS flowbridge_prod_db.serving_sch
  COMMENT = 'Serving Layer';


CREATE WAREHOUSE IF NOT EXISTS flowbridge_pipeline_wh
  WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  COMMENT = 'Pipeline workloads - Ingestion + transformation';


  CREATE WAREHOUSE IF NOT EXISTS flowbridge_analytics_wh
  WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  COMMENT = 'Pipeline workloads - streamlit + data sharing';




CREATE OR REPLACE RESOURCE MONITOR flowbridge_pipeline_rm
  WITH CREDIT_QUOTA = 20
       FREQUENCY = MONTHLY
       START_TIMESTAMP = IMMEDIATELY
  TRIGGERS
       ON 75 PERCENT DO NOTIFY
       ON 90 PERCENT DO NOTIFY
       ON 100 PERCENT DO SUSPEND;



CREATE OR REPLACE RESOURCE MONITOR flowbridge_analytics_rm
  WITH CREDIT_QUOTA = 20
       FREQUENCY = MONTHLY
       START_TIMESTAMP = IMMEDIATELY
  TRIGGERS
       ON 75 PERCENT DO NOTIFY
       ON 90 PERCENT DO NOTIFY
       ON 100 PERCENT DO SUSPEND;




-- ASSIGN MONITORS TO WAREHOUSES
alter warehouse flowbridge_pipeline_wh set resource_monitor = flowbridge_pipeline_rm;
alter warehouse flowbridge_analytics_wh set resource_monitor = flowbridge_analytics_rm;

---------------------- GRANT PRIVILEGES TO SYSADMIN ----------------------
use role accountadmin;
--------------------------- VERIFY ---------------------------
grant execute task on account to role sysadmin;

grant usage on warehouse flowbridge_pipeline_wh to role sysadmin;
grant usage on warehouse flowbridge_analytics_wh to role sysadmin;

grant all privileges on database flowbridge_dev_db to role sysadmin;
grant all privileges on database flowbridge_prod_db to role sysadmin;



grant all privileges on all schemas
  in database flowbridge_dev_db to role sysadmin;

grant all privileges on all schemas
  in database flowbridge_prod_db to role sysadmin;



show databases like 'flowbridge_%';

show warehouses like 'flowbridge_%';

show resource monitors;

