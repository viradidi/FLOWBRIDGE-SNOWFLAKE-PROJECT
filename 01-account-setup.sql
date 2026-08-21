use role accountadmin;
create database if not exists flowbridge_dev_db
   comment = "flowbridge supply chain - development database";

create database if not exists flowbridge_prod_db
   comment = "flowbridge supply chain - production database";


create schema if not exists flowbridge_dev_db.bronze_sch
   comment = "raw ingestion layer";


create schema if not exists flowbridge_dev_db.silver_sch
   comment = "transformation layer";

create schema if not exists flowbridge_dev_db.gold_sch
   comment = "aggregation layer";
create schema if not exists flowbridge_dev_db.serving_sch
   comment = "serving layer";



create schema if not exists flowbridge_prod_db.bronze_sch
   comment = "raw ingestion layer";


create schema if not exists flowbridge_prod_db.silver_sch
   comment = "transformation layer";

create schema if not exists flowbridge_prod_db.gold_sch
   comment = "aggregation layer";
create schema if not exists flowbridge_prod_db.serving_sch
   comment = "serving layer";


create warehouse if not exists flowbridge_pipeline_wh
   warehouse_size = 'x-small'
   auto_suspend = 60
   auto_resume = true
   comment = "pipeline_workloads = ingestion + transformation";


create or replace warehouse  flowbridge_analytics_wh
   warehouse_size = 'x-small'
   auto_suspend = 60
   auto_resume = true
   comment = "pipeline_workloads = streamlit + datasharing";



create or replace resource monitor flowbridge_pipeline_rm
  with credit_quota = 20
  frequency = monthly
  start_timestamp = immediately
    triggers
      on 75 percent do notify
      on 90 percent do notify
      on 100 percent do suspend;


create or replace resource monitor flowbridge_analytics_rm
  with credit_quota = 20
  frequency = monthly
  start_timestamp = immediately
    triggers
      on 75 percent do notify
      on 90 percent do notify
      on 100 percent do suspend;


alter warehouse flowbridge_pipeline_wh set resource_monitor = flowbridge_pipeline_rm;
alter warehouse flowbridge_analytics_wh set resource_monitor = flowbridge_analytics_rm;


use role accountadmin;

grant execute task on account to role sysadmin;

grant usage on warehouse flowbridge_pipeline_wh to role sysadmin;
grant usage on warehouse flowbridge_analytics_wh to role sysadmin;

grant all privileges on database flowbridge_dev_db to role sysadmin;
grant all privileges on database flowbridge_prod_db to role sysadmin;

grant all privileges on all schemas
  in database flowbridge_dev_db to role sysadmin;

grant all privileges on all schemas
  in database flowbridge_prod_db to role sysadmin;


show databases like 'flowbridge_%'


show warehouses like 'flowbridge_%'

show resource monitors;