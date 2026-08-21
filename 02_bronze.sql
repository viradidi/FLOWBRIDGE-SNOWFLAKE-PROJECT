use database flowbridge_dev_db;
use role accountadmin;

CREATE STORAGE INTEGRATION IF NOT EXISTS flowbridge_adls_integration
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'AZURE'
    ENABLED = TRUE
    AZURE_TENANT_ID = 'dc78956f-8c91-443d-bc5f-b2fa60afcac3'
    STORAGE_ALLOWED_LOCATIONS = (
        'azure://flowbrigeadls.blob.core.windows.net/supply-chain-raw-dev/',
        'azure://flowbrigeadls.blob.core.windows.net/supply-chain-raw-prod/'
    );

desc integration flowbridge_adls_integration;

grant usage on integration flowbridge_adls_integration to role sysadmin;

-- # create or replace notification integration flow_bridge_azure_notifications_int
--  enabled = true
 -- type = queue
 -- notification_provider = azure_storage_queue
 -- azure_storage_queue_primary_url = 'https://flowbrigeadls.queue.core.windows.net/flowbridge-supply-chain-queue'
 -- azure_tenant_id = 'dc78956f-8c91-443d-bc5f-b2fa60afcac3';# 


CREATE OR REPLACE NOTIFICATION INTEGRATION flow_bridge_azure_notifications_int
    ENABLED = TRUE
    TYPE = QUEUE
    NOTIFICATION_PROVIDER = AZURE_STORAGE_QUEUE
    AZURE_STORAGE_QUEUE_PRIMARY_URI =
        'https://flowbrigeadls.queue.core.windows.net/flowbridge-supply-chain-queue'
    AZURE_TENANT_ID =
        'dc78956f-8c91-443d-bc5f-b2fa60afcac3';

DESC NOTIFICATION INTEGRATION flow_bridge_azure_notifications_int;

grant usage on integration  flow_bridge_azure_notifications_int to role sysadmin;

use role sysadmin;
use database flowbridge_dev_db;
use schema flowbridge_dev_db.bronze_sch;
use warehouse flowbridge_pipeline_wh;

create file format if not exists bronze_sch.json_file_format
 type = 'json'
 strip_outer_array = true -- []
 comment = 'json file format for flowbridge project';

describe file format bronze_sch.json_file_format;

create stage if not exists bronze_sch.adls_raw_stage
   url = 'azure://flowbrigeadls.blob.core.windows.net/supply-chain-raw-dev/'
   storage_integration = flowbridge_adls_integration
   file_format = bronze_sch.json_file_format
   comment = 'External storage - ADLS gen2 dev container';

list @bronze_sch.adls_raw_stage;




create or replace transient table bronze_sch.raw_orders(
raw_data variant,
ingested_at timestamp_ntz default CURRENT_TIMESTAMP(),
file_name STRING,
file_row_number number,
load_id string default UUID_STRING()

)COMMENT = 'BRONZE_LAYER - RAW JSON SUPPLY CHAIN ORDER FOR FLOWBRIDGE';


CREATE PIPE IF NOT EXISTS bronze_sch.supply_chain_pipe
auto_ingest = true
integration = flow_bridge_azure_notifications_int
comment = 'Snowpipe - auto ingest json files from adls gen2'
as 
copy into bronze_sch.raw_orders(
raw_data,
file_name,
file_row_number

)
from (
select $1,
metadata$filename,
metadata$file_row_number

from @bronze_sch.adls_raw_stage

) file_format = (format_name = 'bronze_sch.json_file_format');

show pipes;

select system$pipe_status('bronze_sch.supply_chain_pipe');

alter pipe bronze_sch.supply_chain_pipe refresh;

select * from bronze_sch.raw_orders;

select count(*) from bronze_sch.raw_orders;

SELECT *
FROM TABLE(
    INFORMATION_SCHEMA.COPY_HISTORY(
        TABLE_NAME => 'RAW_ORDERS',
        START_TIME => DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
    )
);
