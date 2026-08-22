use role accountadmin;
use database flowbridge_dev_db;
use warehouse flowbridge_pipeline_wh;


create notification integration if not exists email_notification_int
    type = email
    enabled = true
    comment = 'Email Notification Integration for pipeline health alerts';



-- Test email - verify integration is working
SELECT SYSTEM$START_USER_EMAIL_VERIFICATION('MOSOTAAA');

call system$send_email(
    'email_notification_int',
    'limbaga776@gmail.com',
    'Flowbridge Project - DataToCrunch',
    'DatatoCrunch - Alerts are ready!'
);
SELECT SYSTEM$START_USER_EMAIL_VERIFICATION('MOSOTAAA');

SHOW USERS LIKE 'MOSOTAAA';


SELECT SYSTEM$START_USER_EMAIL_VERIFICATION('MOSOTAAA');

CALL SYSTEM$SEND_EMAIL(
    'EMAIL_NOTIFICATION_INT',
    'limbaga776@gmail.com',
    'Flowbridge Project - DataToCrunch',
    'DatatoCrunch - Alerts are ready!'
);

show integrations;



create or replace alert bronze_sch.pipeline_health_alert
    warehouse = flowbridge_pipeline_wh
    schedule = '5 minute'
if(exists(
    --------- Bronze - Snowpipe Failure ---------
    select 1
    from table(information_schema.copy_history(
        table_name = 'RAW_ORDERS',
        start_time = dateadd(hour,-1,current_timestamp())
    ))
    where status = 'Load Failed'

    union all

    --------- Silver - Tasks Failure ---------
    select 1 
    from table(information_schema.task_history(
        scheduled_time_range_start = dateadd(hour,-1,current_timestamp())
    ))
    where state = 'FAILED'
    and database_name = 'FLOWBRIDGE_DEV_DB'

    union all

    --------- Gold - DT's Failure ---------
    select 1 
    from table(information_schema.dynamic_table_refresh_history())
    where schema_name = 'GOLD_SCH'
    AND database_name = 'FLOWBRIDGE_DEV_DB'
    and state = 'FAILED'
    and refresh_start_time > dateadd(minute,-5,current_timestamp())
))
then call system$send_email(
    'email_notification_int',
    'limbaga776@gmail.com',
    'Pipeline Alert! - Flowbridge Project',
    'Something went wrong in the pipeline. Check Bronze/Silver/Gold layers for failures. Login to snowsight -> monitoring -> History / Copy History'
);


-- Activate alert - alerts are SUSPENDED by default
alter alert bronze_sch.pipeline_health_alert resume;

show alerts in database flowbridge_dev_db;


-- Check alert history
select * from table(information_schema.alert_history(
    scheduled_time_range_start => dateadd(hour,-1,current_timestamp())
))
order by scheduled_time desc
limit 10;


show alerts in database flowbridge_dev_db;

show resource monitors;

