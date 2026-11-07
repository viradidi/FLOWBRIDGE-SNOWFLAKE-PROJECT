
----------------------------------------------------------------------
-- FLOWBRIDGE - SILVER TO STAR SCHEMA
-- Script : 03_silver/silver_to_star.sql
-- Purpose:
--   1. Create surrogate key sequences
--   2. Create dimension tables
--   3. Create fact table
--   4. Create stream on STG_ORDERS
--   5. Create procedure to populate dimensions and fact table
--   6. Create task to execute procedure every minute
--
-- Run as: SYSADMIN
-- Database: FLOWBRIDGE_DEV_DB
-- Schema: SILVER_SCH
----------------------------------------------------------------------


---------------------------- ENVIRONMENT SETTING --------------------

USE ROLE SYSADMIN;
USE DATABASE FLOWBRIDGE_DEV_DB;
USE SCHEMA SILVER_SCH;
USE WAREHOUSE FLOWBRIDGE_PIPELINE_WH;


----------------------------------------------------------------------
-- STEP 1 - CREATE SURROGATE KEY SEQUENCES
----------------------------------------------------------------------

CREATE SEQUENCE IF NOT EXISTS silver_sch.dim_customer_sk_seq
    START = 1
    INCREMENT = 1;

CREATE SEQUENCE IF NOT EXISTS silver_sch.dim_product_sk_seq
    START = 1
    INCREMENT = 1;

CREATE SEQUENCE IF NOT EXISTS silver_sch.dim_supplier_sk_seq
    START = 1
    INCREMENT = 1;

CREATE SEQUENCE IF NOT EXISTS silver_sch.dim_warehouse_sk_seq
    START = 1
    INCREMENT = 1;

CREATE SEQUENCE IF NOT EXISTS silver_sch.dim_shipment_sk_seq
    START = 1
    INCREMENT = 1;

CREATE SEQUENCE IF NOT EXISTS silver_sch.fact_orders_sk_seq
    START = 1
    INCREMENT = 1;


----------------------------------------------------------------------
-- STEP 2 - CREATE DIM_CUSTOMER
----------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS silver_sch.dim_customer (

    customer_sk NUMBER DEFAULT silver_sch.dim_customer_sk_seq.NEXTVAL,
    customer_id STRING NOT NULL,
    customer_name STRING NOT NULL,
    customer_region STRING NOT NULL,
    customer_segment STRING NOT NULL,

    is_active BOOLEAN DEFAULT TRUE,

    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()

)
COMMENT = 'Customer Dimension - star schema - surrogate key via sequence';


----------------------------------------------------------------------
-- STEP 3 - CREATE DIM_PRODUCT
----------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS silver_sch.dim_product (

    product_sk NUMBER DEFAULT silver_sch.dim_product_sk_seq.NEXTVAL,
    product_id STRING NOT NULL,
    product_name STRING NOT NULL,
    category STRING NOT NULL,
    unit_price NUMBER(18,2) NOT NULL,

    is_active BOOLEAN DEFAULT TRUE,

    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()

)
COMMENT = 'Product Dimension - star schema - surrogate key via sequence';


----------------------------------------------------------------------
-- STEP 4 - CREATE DIM_SUPPLIER
----------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS silver_sch.dim_supplier (

    supplier_sk NUMBER DEFAULT silver_sch.dim_supplier_sk_seq.NEXTVAL,
    supplier_id STRING NOT NULL,
    supplier_name STRING NOT NULL,
    supplier_country STRING NOT NULL,
    lead_time_days NUMBER NOT NULL,
    performance_score FLOAT NOT NULL,

    is_active BOOLEAN DEFAULT TRUE,

    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()

)
COMMENT = 'Supplier Dimension - star schema - surrogate key via sequence';


----------------------------------------------------------------------
-- STEP 5 - CREATE DIM_WAREHOUSE
----------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS silver_sch.dim_warehouse (

    warehouse_sk NUMBER DEFAULT silver_sch.dim_warehouse_sk_seq.NEXTVAL,
    warehouse_id STRING NOT NULL,
    warehouse_location STRING NOT NULL,

    is_active BOOLEAN DEFAULT TRUE,

    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()

)
COMMENT = 'Warehouse Dimension - star schema - surrogate key via sequence';


----------------------------------------------------------------------
-- STEP 6 - CREATE DIM_SHIPMENT
----------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS silver_sch.dim_shipment (

    shipment_sk NUMBER DEFAULT silver_sch.dim_shipment_sk_seq.NEXTVAL,
    shipment_id STRING NOT NULL,
    carrier STRING NOT NULL,
    ship_date TIMESTAMP_NTZ,
    estimated_delivery DATE,

    is_active BOOLEAN DEFAULT TRUE,

    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()

)
COMMENT = 'Shipment Dimension - star schema - surrogate key via sequence';


-- Stable unknown shipment member for orders that have not shipped.
INSERT INTO silver_sch.dim_shipment (shipment_sk, shipment_id, carrier, is_active)
SELECT -1, 'UNKNOWN', 'UNKNOWN', TRUE
WHERE NOT EXISTS (
    SELECT 1 FROM silver_sch.dim_shipment
    WHERE shipment_sk = -1 OR shipment_id = 'UNKNOWN'
);


----------------------------------------------------------------------
-- STEP 7 - CREATE FACT_ORDERS
----------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS silver_sch.fact_orders (

    order_sk NUMBER DEFAULT silver_sch.fact_orders_sk_seq.NEXTVAL,
    order_id STRING NOT NULL,

    customer_sk NUMBER NOT NULL,
    product_sk NUMBER NOT NULL,
    supplier_sk NUMBER NOT NULL,
    warehouse_sk NUMBER NOT NULL,
    shipment_sk NUMBER NOT NULL,

    order_date TIMESTAMP_NTZ NOT NULL,
    order_status STRING NOT NULL,
    payment_status STRING NOT NULL,

    quantity NUMBER NOT NULL,
    unit_price NUMBER(18,2) NOT NULL,
    total_amount NUMBER(18,2) NOT NULL,
    delay_days NUMBER,
    inventory_level NUMBER,

    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    ingested_at TIMESTAMP_NTZ

)
COMMENT = 'Fact Table - one row per order. 5 dimension keys and measures';


----------------------------------------------------------------------
-- STEP 8 - CREATE STREAM ON STG_ORDERS
----------------------------------------------------------------------

CREATE STREAM IF NOT EXISTS silver_sch.stg_orders_stream
    ON TABLE silver_sch.stg_orders
    SHOW_INITIAL_ROWS = TRUE
    COMMENT = 'Stream on STG_ORDERS for Silver to Star Schema processing';


SHOW STREAMS;


----------------------------------------------------------------------
-- STEP 9 - CREATE SILVER_TO_STAR PROCEDURE
----------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE silver_sch.silver_to_star()

RETURNS STRING
LANGUAGE SQL

AS
$$
BEGIN

    ------------------------------------------------------------------
    -- PART 1 - CREATE TEMPORARY TABLE FROM STAGING STREAM
    ------------------------------------------------------------------

    -- Buffer before the transaction; stream-consuming INSERT and downstream
    -- MERGEs commit together so failed transforms do not lose stream changes.
    CREATE OR REPLACE TEMPORARY TABLE silver_sch.tmp_stream_data
        LIKE silver_sch.stg_orders;

    BEGIN TRANSACTION;

    INSERT INTO silver_sch.tmp_stream_data (ORDER_ID, ORDER_DATE, ORDER_STATUS, CUSTOMER_ID, CUSTOMER_NAME, CUSTOMER_REGION, CUSTOMER_SEGMENT, SUPPLIER_ID, SUPPLIER_NAME, SUPPLIER_COUNTRY, LEAD_TIME_DAYS, PERFORMANCE_SCORE, SHIPMENT_ID, CARRIER, SHIP_DATE, ESTIMATED_DELIVERY, DELAY_DAYS, PRODUCT_ID, PRODUCT_NAME, CATEGORY, QUANTITY, UNIT_PRICE, TOTAL_AMOUNT, PAYMENT_STATUS, WAREHOUSE_ID, WAREHOUSE_LOCATION, INVENTORY_LEVEL, FILE_NAME, FILE_ROW_NUMBER, INGESTED_AT, TRANSFORMED_AT)
    SELECT ORDER_ID, ORDER_DATE, ORDER_STATUS, CUSTOMER_ID, CUSTOMER_NAME, CUSTOMER_REGION, CUSTOMER_SEGMENT, SUPPLIER_ID, SUPPLIER_NAME, SUPPLIER_COUNTRY, LEAD_TIME_DAYS, PERFORMANCE_SCORE, SHIPMENT_ID, CARRIER, SHIP_DATE, ESTIMATED_DELIVERY, DELAY_DAYS, PRODUCT_ID, PRODUCT_NAME, CATEGORY, QUANTITY, UNIT_PRICE, TOTAL_AMOUNT, PAYMENT_STATUS, WAREHOUSE_ID, WAREHOUSE_LOCATION, INVENTORY_LEVEL, FILE_NAME, FILE_ROW_NUMBER, INGESTED_AT, TRANSFORMED_AT
    FROM silver_sch.stg_orders_stream
    WHERE METADATA$ACTION = 'INSERT';


    ------------------------------------------------------------------
    -- PART 2 - MERGE INTO DIM_CUSTOMER (SCD TYPE 1)
    ------------------------------------------------------------------

    MERGE INTO silver_sch.dim_customer AS tgt

    USING (

        SELECT
            UPPER(customer_id)      AS customer_id,
            UPPER(customer_name)    AS customer_name,
            UPPER(customer_region)  AS customer_region,
            UPPER(customer_segment) AS customer_segment

        FROM silver_sch.tmp_stream_data

        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY UPPER(customer_id)
            ORDER BY order_date DESC, ingested_at DESC, file_name DESC, file_row_number DESC
        ) = 1

    ) AS src

    ON tgt.customer_id = src.customer_id

    WHEN MATCHED THEN UPDATE SET

        tgt.customer_name    = src.customer_name,
        tgt.customer_region  = src.customer_region,
        tgt.customer_segment = src.customer_segment,
        tgt.is_active        = TRUE,
        tgt.updated_at       = CURRENT_TIMESTAMP()

    WHEN NOT MATCHED THEN INSERT (

        customer_id,
        customer_name,
        customer_region,
        customer_segment

    )

    VALUES (

        src.customer_id,
        src.customer_name,
        src.customer_region,
        src.customer_segment

    );


    ------------------------------------------------------------------
    -- PART 3 - MERGE INTO DIM_PRODUCT (SCD TYPE 1)
    ------------------------------------------------------------------

    MERGE INTO silver_sch.dim_product AS tgt

    USING (

        SELECT
            UPPER(product_id)   AS product_id,
            UPPER(product_name) AS product_name,
            UPPER(category)     AS category,
            unit_price

        FROM silver_sch.tmp_stream_data

        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY UPPER(product_id)
            ORDER BY order_date DESC, ingested_at DESC, file_name DESC, file_row_number DESC
        ) = 1

    ) AS src

    ON tgt.product_id = src.product_id

    WHEN MATCHED THEN UPDATE SET

        tgt.product_name = src.product_name,
        tgt.category     = src.category,
        tgt.unit_price   = src.unit_price,
        tgt.is_active    = TRUE,
        tgt.updated_at   = CURRENT_TIMESTAMP()

    WHEN NOT MATCHED THEN INSERT (

        product_id,
        product_name,
        category,
        unit_price

    )

    VALUES (

        src.product_id,
        src.product_name,
        src.category,
        src.unit_price

    );


    ------------------------------------------------------------------
    -- PART 4 - MERGE INTO DIM_SUPPLIER (SCD TYPE 1)
    ------------------------------------------------------------------

    MERGE INTO silver_sch.dim_supplier AS tgt

    USING (

        SELECT
            UPPER(supplier_id)      AS supplier_id,
            UPPER(supplier_name)    AS supplier_name,
            UPPER(supplier_country) AS supplier_country,
            lead_time_days,
            performance_score

        FROM silver_sch.tmp_stream_data

        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY UPPER(supplier_id)
            ORDER BY order_date DESC, ingested_at DESC, file_name DESC, file_row_number DESC
        ) = 1

    ) AS src

    ON tgt.supplier_id = src.supplier_id

    WHEN MATCHED THEN UPDATE SET

        tgt.supplier_name     = src.supplier_name,
        tgt.supplier_country  = src.supplier_country,
        tgt.lead_time_days    = src.lead_time_days,
        tgt.performance_score = src.performance_score,
        tgt.is_active         = TRUE,
        tgt.updated_at        = CURRENT_TIMESTAMP()

    WHEN NOT MATCHED THEN INSERT (

        supplier_id,
        supplier_name,
        supplier_country,
        lead_time_days,
        performance_score

    )

    VALUES (

        src.supplier_id,
        src.supplier_name,
        src.supplier_country,
        src.lead_time_days,
        src.performance_score

    );


    ------------------------------------------------------------------
    -- PART 5 - MERGE INTO DIM_WAREHOUSE (SCD TYPE 1)
    ------------------------------------------------------------------

    MERGE INTO silver_sch.dim_warehouse AS tgt

    USING (

        SELECT
            UPPER(warehouse_id)       AS warehouse_id,
            UPPER(warehouse_location) AS warehouse_location

        FROM silver_sch.tmp_stream_data

        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY UPPER(warehouse_id)
            ORDER BY order_date DESC, ingested_at DESC, file_name DESC, file_row_number DESC
        ) = 1

    ) AS src

    ON tgt.warehouse_id = src.warehouse_id

    WHEN MATCHED THEN UPDATE SET

        tgt.warehouse_location = src.warehouse_location,
        tgt.is_active          = TRUE,
        tgt.updated_at         = CURRENT_TIMESTAMP()

    WHEN NOT MATCHED THEN INSERT (

        warehouse_id,
        warehouse_location

    )

    VALUES (

        src.warehouse_id,
        src.warehouse_location

    );


    ------------------------------------------------------------------
    -- PART 6 - MERGE INTO DIM_SHIPMENT (SCD TYPE 1)
    ------------------------------------------------------------------

    MERGE INTO silver_sch.dim_shipment AS tgt

    USING (

        SELECT
            UPPER(shipment_id) AS shipment_id,
            UPPER(carrier)     AS carrier,
            ship_date,
            estimated_delivery

        FROM silver_sch.tmp_stream_data

        WHERE UPPER(shipment_id) IS NOT NULL
          AND UPPER(shipment_id) <> 'UNKNOWN'

        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY UPPER(shipment_id)
            ORDER BY order_date DESC, ingested_at DESC, file_name DESC, file_row_number DESC
        ) = 1

    ) AS src

    ON tgt.shipment_id = src.shipment_id

    WHEN MATCHED THEN UPDATE SET

        tgt.carrier            = src.carrier,
        tgt.ship_date          = src.ship_date,
        tgt.estimated_delivery = src.estimated_delivery,
        tgt.is_active          = TRUE,
        tgt.updated_at         = CURRENT_TIMESTAMP()

    WHEN NOT MATCHED THEN INSERT (

        shipment_id,
        carrier,
        ship_date,
        estimated_delivery

    )

    VALUES (

        src.shipment_id,
        src.carrier,
        src.ship_date,
        src.estimated_delivery

    );


    ------------------------------------------------------------------
    -- PART 7 - MERGE INTO FACT_ORDERS
    ------------------------------------------------------------------

    MERGE INTO silver_sch.fact_orders AS tgt

    USING (

        SELECT

            UPPER(s.order_id) AS order_id,

            dc.customer_sk,
            dp.product_sk,
            ds.supplier_sk,
            dw.warehouse_sk,

            COALESCE(dsh.shipment_sk, -1) AS shipment_sk,

            s.order_date,

            UPPER(s.order_status)   AS order_status,
            UPPER(s.payment_status) AS payment_status,

            s.quantity,
            s.unit_price,
            s.total_amount,
            s.delay_days,
            s.inventory_level,
            s.ingested_at

        FROM silver_sch.tmp_stream_data s

        INNER JOIN silver_sch.dim_customer dc
            ON dc.customer_id = UPPER(s.customer_id)

        INNER JOIN silver_sch.dim_product dp
            ON dp.product_id = UPPER(s.product_id)

        INNER JOIN silver_sch.dim_supplier ds
            ON ds.supplier_id = UPPER(s.supplier_id)

        INNER JOIN silver_sch.dim_warehouse dw
            ON dw.warehouse_id = UPPER(s.warehouse_id)

        LEFT JOIN silver_sch.dim_shipment dsh
            ON dsh.shipment_id = UPPER(s.shipment_id)

        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY UPPER(s.order_id)
            ORDER BY s.order_date DESC, s.ingested_at DESC, s.file_name DESC, s.file_row_number DESC
        ) = 1

    ) AS src

    ON tgt.order_id = src.order_id

    WHEN MATCHED THEN UPDATE SET

        tgt.customer_sk     = src.customer_sk,
        tgt.product_sk      = src.product_sk,
        tgt.supplier_sk     = src.supplier_sk,
        tgt.warehouse_sk    = src.warehouse_sk,
        tgt.shipment_sk     = src.shipment_sk,

        tgt.order_date      = src.order_date,
        tgt.order_status    = src.order_status,
        tgt.payment_status  = src.payment_status,

        tgt.quantity        = src.quantity,
        tgt.unit_price      = src.unit_price,
        tgt.total_amount    = src.total_amount,
        tgt.delay_days      = src.delay_days,
        tgt.inventory_level = src.inventory_level,

        tgt.ingested_at     = src.ingested_at,
        tgt.updated_at      = CURRENT_TIMESTAMP()

    WHEN NOT MATCHED THEN INSERT (

        order_id,

        customer_sk,
        product_sk,
        supplier_sk,
        warehouse_sk,
        shipment_sk,

        order_date,
        order_status,
        payment_status,

        quantity,
        unit_price,
        total_amount,
        delay_days,
        inventory_level,

        ingested_at

    )

    VALUES (

        src.order_id,

        src.customer_sk,
        src.product_sk,
        src.supplier_sk,
        src.warehouse_sk,
        src.shipment_sk,

        src.order_date,
        src.order_status,
        src.payment_status,

        src.quantity,
        src.unit_price,
        src.total_amount,
        src.delay_days,
        src.inventory_level,

        src.ingested_at

    );


    ------------------------------------------------------------------
    -- PART 8 - CLEANUP TEMPORARY TABLE
    ------------------------------------------------------------------

    COMMIT;

    DROP TABLE IF EXISTS silver_sch.tmp_stream_data;


    ------------------------------------------------------------------
    -- PART 9 - RETURN SUCCESS MESSAGE
    ------------------------------------------------------------------

    RETURN 'silver_to_star completed successfully';

END;
$$;


----------------------------------------------------------------------
-- STEP 10 - CREATE TASK: SILVER_TO_STAR_TASK
-- Purpose: Execute silver_to_star every minute when stream has data
----------------------------------------------------------------------

CREATE OR REPLACE TASK silver_sch.silver_to_star_task

    WAREHOUSE = flowbridge_pipeline_wh

    SCHEDULE = '1 minute'

    COMMENT = 'Task - calls silver_to_star when stg_orders_stream has data'

    WHEN SYSTEM$STREAM_HAS_DATA(
        'FLOWBRIDGE_DEV_DB.SILVER_SCH.STG_ORDERS_STREAM'
    )

AS

    CALL silver_sch.silver_to_star();


----------------------------------------------------------------------
-- STEP 11 - RESUME TASK
----------------------------------------------------------------------

ALTER TASK silver_sch.silver_to_star_task RESUME;


----------------------------------------------------------------------
-- STEP 12 - CHECK TASK STATUS
----------------------------------------------------------------------

SHOW TASKS;


----------------------------------------------------------------------
-- STEP 13 - CHECK STREAM STATUS
----------------------------------------------------------------------

SELECT SYSTEM$STREAM_HAS_DATA(
    'FLOWBRIDGE_DEV_DB.SILVER_SCH.STG_ORDERS_STREAM'
) AS STREAM_HAS_DATA;


----------------------------------------------------------------------
-- STEP 14 - CHECK ALL DIMENSION AND FACT TABLE COUNTS
----------------------------------------------------------------------

SELECT 'DIM_CUSTOMER' AS TABLE_NAME, COUNT(*) AS TOTAL_RECORDS
FROM silver_sch.dim_customer

UNION ALL

SELECT 'DIM_PRODUCT', COUNT(*)
FROM silver_sch.dim_product

UNION ALL

SELECT 'DIM_SUPPLIER', COUNT(*)
FROM silver_sch.dim_supplier

UNION ALL

SELECT 'DIM_WAREHOUSE', COUNT(*)
FROM silver_sch.dim_warehouse

UNION ALL

SELECT 'DIM_SHIPMENT', COUNT(*)
FROM silver_sch.dim_shipment

UNION ALL

SELECT 'FACT_ORDERS', COUNT(*)
FROM silver_sch.fact_orders;


----------------------------------------------------------------------
-- STEP 15 - CHECK TASK EXECUTION HISTORY
----------------------------------------------------------------------

SELECT
    NAME,
    STATE,
    SCHEDULED_TIME,
    COMPLETED_TIME,
    ERROR_MESSAGE

FROM TABLE(
    INFORMATION_SCHEMA.TASK_HISTORY(
        SCHEDULED_TIME_RANGE_START =>
            DATEADD('HOUR', -2, CURRENT_TIMESTAMP())
    )
)

WHERE NAME = 'SILVER_TO_STAR_TASK'

ORDER BY SCHEDULED_TIME DESC;


----------------------------------------------------------------------
-- OPTIONAL: MANUAL EXECUTION
-- Only run this if you want to execute the procedure manually.
-- Do not run it unnecessarily while the scheduled task is running.
----------------------------------------------------------------------

-- CALL silver_sch.silver_to_star();


----------------------------------------------------------------------
-- END OF SCRIPT
----------------------------------------------------------------------


-- Check all table counts
select 'DIM_CUSTOMER' , count(*) from silver_sch.dim_customer
union all
select 'DIM_PRODUCT'  , count(*) from silver_sch.dim_product
union all
select 'DIM_SUPPLIER' , count(*) from silver_sch.dim_supplier
union all
select 'DIM_WAREHOUSE', count(*) from silver_sch.dim_warehouse
union all
select 'DIM_SHIPMENT' , count(*) from silver_sch.dim_shipment
union all
select 'FACT_ORDERS'  , count(*) from silver_sch.fact_orders;


USE ROLE SYSADMIN;
USE DATABASE FLOWBRIDGE_DEV_DB;
USE SCHEMA SILVER_SCH;

SELECT
    COUNT(*) AS TOTAL_STAGING_RECORDS
FROM SILVER_SCH.STG_ORDERS;

SELECT
    SYSTEM$STREAM_HAS_DATA(
        'FLOWBRIDGE_DEV_DB.SILVER_SCH.STG_ORDERS_STREAM'
    ) AS STREAM_HAS_DATA;




    SELECT
    NAME,
    STATE,
    SCHEDULED_TIME,
    COMPLETED_TIME,
    ERROR_MESSAGE
FROM TABLE(
    INFORMATION_SCHEMA.TASK_HISTORY(
        SCHEDULED_TIME_RANGE_START =>
            DATEADD('HOUR', -24, CURRENT_TIMESTAMP())
    )
)
WHERE NAME = 'SILVER_TO_STAR_TASK'
ORDER BY SCHEDULED_TIME DESC;


SHOW TASKS LIKE 'SILVER_TO_STAR_TASK'
IN SCHEMA SILVER_SCH;



SHOW PROCEDURES LIKE 'SILVER_TO_STAR'
IN SCHEMA FLOWBRIDGE_DEV_DB.SILVER_SCH;

USE DATABASE FLOWBRIDGE_DEV_DB;

CALL SILVER_SCH.SILVER_TO_STAR();


USE ROLE SYSADMIN;
USE DATABASE FLOWBRIDGE_DEV_DB;
USE SCHEMA SILVER_SCH;

SELECT 'DIM_CUSTOMER' AS TABLE_NAME, COUNT(*) AS TOTAL_RECORDS
FROM DIM_CUSTOMER

UNION ALL

SELECT 'DIM_PRODUCT', COUNT(*)
FROM DIM_PRODUCT

UNION ALL

SELECT 'DIM_SUPPLIER', COUNT(*)
FROM DIM_SUPPLIER

UNION ALL

SELECT 'DIM_WAREHOUSE', COUNT(*)
FROM DIM_WAREHOUSE

UNION ALL

SELECT 'DIM_SHIPMENT', COUNT(*)
FROM DIM_SHIPMENT

UNION ALL

SELECT 'FACT_ORDERS', COUNT(*)
FROM FACT_ORDERS

UNION ALL

SELECT 'STG_ORDERS', COUNT(*)
FROM STG_ORDERS;


select * from silver_sch.dim_customer;

select * from silver_sch.dim_product;

select * from silver_sch.dim_supplier;

select * from silver_sch.dim_warehouse;

select * from silver_sch.dim_shipment;

