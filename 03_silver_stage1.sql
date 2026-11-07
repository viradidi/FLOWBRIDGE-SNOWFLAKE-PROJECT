-- ============================================================================
-- SESSION ENVIRONMENT SETUP
-- ============================================================================

SET ENV = 'DEV';
SET DB = 'FLOWBRIDGE_' || $ENV || '_DB';

USE DATABASE IDENTIFIER($DB);
USE ROLE SYSADMIN;
USE SCHEMA SILVER_SCH;
USE WAREHOUSE FLOWBRIDGE_PIPELINE_WH;


-- ============================================================================
-- STEP 1 - TARGET LAYER STRUCTURES
-- ============================================================================

-- Dead Letter Table Definition
------------------------------------------------------------

CREATE TRANSIENT TABLE IF NOT EXISTS SILVER_SCH.DEAD_LETTER (
    RAW_DATA VARIANT,
    ERROR_REASON STRING,
    FILE_NAME STRING,
    FILE_ROW_NUMBER NUMBER,
    REJECTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Dead Letter Table - rejected/bad records from bronze';


-- Permanent Staging Orders Table Definition
-- Flattened JSON Data
------------------------------------------------------------

CREATE TABLE IF NOT EXISTS SILVER_SCH.STG_ORDERS (

    -- Order Fields
    ORDER_ID          STRING COMMENT 'Unique order identifier',
    ORDER_DATE        TIMESTAMP_NTZ COMMENT 'Order timestamp',
    ORDER_STATUS      STRING COMMENT 'Current order status',

    -- Customer fields
    CUSTOMER_ID       STRING COMMENT 'Customer identifier',
    CUSTOMER_NAME     STRING COMMENT 'Customer name',
    CUSTOMER_REGION   STRING COMMENT 'Customer region',
    CUSTOMER_SEGMENT  STRING COMMENT 'Customer segment',

    -- Supplier fields
    SUPPLIER_ID       STRING COMMENT 'Supplier identifier',
    SUPPLIER_NAME     STRING COMMENT 'Supplier name',
    SUPPLIER_COUNTRY  STRING COMMENT 'Supplier country',
    LEAD_TIME_DAYS    NUMBER COMMENT 'Supplier lead time in days',
    PERFORMANCE_SCORE FLOAT COMMENT 'Supplier performance score',

    -- Shipment fields
    SHIPMENT_ID        STRING COMMENT 'Shipment identifier',
    CARRIER            STRING COMMENT 'Shipping carrier',
    SHIP_DATE          TIMESTAMP_NTZ COMMENT 'Shipment date',
    ESTIMATED_DELIVERY DATE COMMENT 'Estimated delivery date',
    DELAY_DAYS         NUMBER COMMENT 'Number of delay days',

    -- Product fields
    PRODUCT_ID   STRING COMMENT 'Product identifier',
    PRODUCT_NAME STRING COMMENT 'Product name',
    CATEGORY     STRING COMMENT 'Product category',
    QUANTITY     NUMBER COMMENT 'Order quantity',
    UNIT_PRICE   FLOAT COMMENT 'Unit price',

    -- Financial fields
    TOTAL_AMOUNT   NUMBER(18,2) COMMENT 'Total order amount',
    PAYMENT_STATUS STRING COMMENT 'Payment status',

    -- Warehouse fields
    WAREHOUSE_ID       STRING COMMENT 'Warehouse identifier',
    WAREHOUSE_LOCATION STRING COMMENT 'Warehouse location',
    INVENTORY_LEVEL    NUMBER COMMENT 'Current inventory level',

    -- Metadata
    FILE_NAME       STRING COMMENT 'Source file name',
    FILE_ROW_NUMBER NUMBER COMMENT 'Row number in source file',
    INGESTED_AT     TIMESTAMP_NTZ COMMENT 'When record was ingested',
    TRANSFORMED_AT  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        COMMENT 'When record was transformed'

)
COMMENT = 'Silver Stage 1 - flattened and cleaned supply chain orders';


-- ============================================================================
-- STEP 2 - CHANGE DATA CAPTURE (CDC) STREAM
-- ============================================================================

CREATE STREAM IF NOT EXISTS SILVER_SCH.RAW_ORDERS_STREAM
    ON TABLE FLOWBRIDGE_DEV_DB.BRONZE_SCH.RAW_ORDERS
    APPEND_ONLY = TRUE
    SHOW_INITIAL_ROWS = TRUE
    COMMENT = 'Stream on raw_orders - captures new data for silver processing';


-- Verify stream created

SHOW STREAMS IN SCHEMA SILVER_SCH;


-- ============================================================================
-- STEP 3 - RUNTIME PIPELINE (ROUTING STORED PROCEDURE)
-- ============================================================================

CREATE OR REPLACE PROCEDURE SILVER_SCH.SP_BRONZE_TO_SILVER()

RETURNS STRING
LANGUAGE SQL

COMMENT = 'SP - routes bad records to dead_letter, merges clean records to stg_orders - will be called by task'

AS
$$

DECLARE

    V_PROCESSED NUMBER DEFAULT 0;
    V_REJECTED NUMBER DEFAULT 0;

BEGIN


    -- ========================================================================
    -- PART 0 - BUFFER STREAM INTO TEMP TABLE
    -- ========================================================================

    -- Create the buffer before the transaction. Its INSERT consumes the stream;
    -- the stream offset advances only when all downstream DML commits.
    CREATE OR REPLACE TEMPORARY TABLE STREAM_BUFFER
        LIKE FLOWBRIDGE_DEV_DB.BRONZE_SCH.RAW_ORDERS;

    BEGIN TRANSACTION;

    INSERT INTO STREAM_BUFFER (
        RAW_DATA, INGESTED_AT, FILE_NAME, FILE_ROW_NUMBER, LOAD_ID
    )
    SELECT RAW_DATA, INGESTED_AT, FILE_NAME, FILE_ROW_NUMBER, LOAD_ID
    FROM SILVER_SCH.RAW_ORDERS_STREAM
    WHERE METADATA$ACTION = 'INSERT';

    SELECT COUNT(*) INTO :V_PROCESSED FROM STREAM_BUFFER;


    -- ========================================================================
    -- PART 1 - ROUTE BAD RECORDS TO DEAD LETTER
    -- ========================================================================

    INSERT INTO SILVER_SCH.DEAD_LETTER (

        RAW_DATA,
        ERROR_REASON,
        FILE_NAME,
        FILE_ROW_NUMBER

    )

    SELECT

        S.RAW_DATA,

        CASE

            -- Order level validations

            WHEN NULLIF(TRIM(S.RAW_DATA:order_id::STRING), '') IS NULL
                THEN 'Missing order_id'

            WHEN TRY_TO_TIMESTAMP_NTZ(
                    S.RAW_DATA:order_date::STRING
                 ) IS NULL

                 AND TRY_TO_DATE(
                    S.RAW_DATA:order_date::STRING,
                    'DD-MM-YYYY'
                 ) IS NULL

                THEN 'Invalid or missing order_date'

            WHEN NULLIF(UPPER(TRIM(S.RAW_DATA:order_status::STRING)), '') IS NULL
                 OR UPPER(TRIM(S.RAW_DATA:order_status::STRING)) NOT IN (
                    'PENDING',
                    'PROCESSING',
                    'SHIPPED',
                    'IN TRANSIT',
                    'DELIVERED',
                    'CANCELLED'
                )

                THEN 'Invalid order_status: '
                     || COALESCE(
                         S.RAW_DATA:order_status::STRING,
                         'NULL'
                     )


            -- Customer level validations

            WHEN NULLIF(
                    TRIM(S.RAW_DATA:customer.customer_id::STRING),
                    ''
                 ) IS NULL

                THEN 'Missing customer_id'


            -- Supplier level validations

            WHEN NULLIF(
                    TRIM(S.RAW_DATA:supplier.supplier_id::STRING),
                    ''
                 ) IS NULL

                THEN 'Missing supplier_id'

            WHEN TRY_TO_DOUBLE(
                    S.RAW_DATA:supplier.performance_score::STRING
                 ) IS NULL
                THEN 'Missing or invalid performance_score'

            WHEN TRY_TO_DOUBLE(
                    S.RAW_DATA:supplier.performance_score::STRING
                 ) > 100

                THEN 'Invalid performance_score > 100: '
                     || S.RAW_DATA:supplier.performance_score::STRING

            WHEN TRY_TO_DOUBLE(
                    S.RAW_DATA:supplier.performance_score::STRING
                 ) < 0

                THEN 'Invalid performance_score < 0: '
                     || S.RAW_DATA:supplier.performance_score::STRING

            WHEN TRY_TO_NUMBER(
                    S.RAW_DATA:supplier.lead_time_days::STRING
                 ) IS NULL
                 OR TRY_TO_NUMBER(
                    S.RAW_DATA:supplier.lead_time_days::STRING
                 ) < 0

                THEN 'Invalid lead_time_days < 0: '
                     || S.RAW_DATA:supplier.lead_time_days::STRING


            WHEN NULLIF(TRIM(S.RAW_DATA:shipment.delay_days::STRING), '') IS NOT NULL
                 AND TRY_TO_NUMBER(S.RAW_DATA:shipment.delay_days::STRING) IS NULL
                THEN 'Invalid shipment delay_days'

            WHEN NULLIF(TRIM(S.RAW_DATA:shipment.delay_days::STRING), '') IS NOT NULL
                 AND TRY_TO_NUMBER(S.RAW_DATA:shipment.delay_days::STRING) IS NULL
                THEN 'Invalid shipment delay_days'

            -- This order-grain model accepts exactly one product item per order.
            WHEN ARRAY_SIZE(S.RAW_DATA:items) IS NULL
                 OR ARRAY_SIZE(S.RAW_DATA:items) <> 1
                THEN 'Expected exactly one item in items array for this order-grain model'

            -- Product level validations

            WHEN NULLIF(
                    TRIM(S.RAW_DATA:items[0].product_id::STRING),
                    ''
                 ) IS NULL

                THEN 'Missing product_id'

            WHEN TRY_TO_NUMBER(
                    S.RAW_DATA:items[0].quantity::STRING
                 ) IS NULL

                 OR TRY_TO_NUMBER(
                    S.RAW_DATA:items[0].quantity::STRING
                 ) <= 0

                THEN 'Invalid quantity: '
                     || COALESCE(
                         S.RAW_DATA:items[0].quantity::STRING,
                         'NULL'
                     )

            WHEN TRY_TO_DECIMAL(S.RAW_DATA:items[0].unit_price::STRING, 18, 2) IS NULL

                 OR TRY_TO_DECIMAL(S.RAW_DATA:items[0].unit_price::STRING, 18, 2) < 0

                THEN 'Invalid unit_price: '
                     || COALESCE(
                         S.RAW_DATA:items[0].unit_price::STRING,
                         'NULL'
                     )


            -- Financial level validations

            WHEN TRY_TO_DECIMAL(S.RAW_DATA:financials.total_amount::STRING, 18, 2) IS NULL

                 OR TRY_TO_DECIMAL(S.RAW_DATA:financials.total_amount::STRING, 18, 2) < 0

                THEN 'Invalid total_amount: '
                     || COALESCE(
                         S.RAW_DATA:financials.total_amount::STRING,
                         'NULL'
                     )


            -- Warehouse level validations

            WHEN NULLIF(TRIM(S.RAW_DATA:warehouse.inventory_level::STRING), '') IS NOT NULL
                 AND TRY_TO_NUMBER(S.RAW_DATA:warehouse.inventory_level::STRING) IS NULL
                THEN 'Invalid inventory_level value'

            WHEN NULLIF(TRIM(S.RAW_DATA:warehouse.inventory_level::STRING), '') IS NOT NULL
                 AND TRY_TO_NUMBER(S.RAW_DATA:warehouse.inventory_level::STRING) IS NULL
                THEN 'Invalid inventory_level value'

            WHEN TRY_TO_NUMBER(
                    S.RAW_DATA:warehouse.inventory_level::STRING
                 ) < 0

                THEN 'Invalid inventory_level < 0: '
                     || S.RAW_DATA:warehouse.inventory_level::STRING


            ELSE 'Unknown'

        END AS ERROR_REASON,

        S.FILE_NAME,
        S.FILE_ROW_NUMBER

    FROM STREAM_BUFFER AS S

    WHERE

        -- Order level

        NULLIF(TRIM(S.RAW_DATA:order_id::STRING), '') IS NULL

        OR (
            TRY_TO_TIMESTAMP_NTZ(
                S.RAW_DATA:order_date::STRING
            ) IS NULL

            AND TRY_TO_DATE(
                S.RAW_DATA:order_date::STRING,
                'DD-MM-YYYY'
            ) IS NULL
        )

        OR NULLIF(UPPER(TRIM(S.RAW_DATA:order_status::STRING)), '') IS NULL
        OR UPPER(TRIM(S.RAW_DATA:order_status::STRING)) NOT IN (
                'PENDING',
                'PROCESSING',
                'SHIPPED',
                'IN TRANSIT',
                'DELIVERED',
                'CANCELLED'
            )


        -- Customer level

        OR NULLIF(
            TRIM(S.RAW_DATA:customer.customer_id::STRING),
            ''
        ) IS NULL


        -- Supplier level

        OR NULLIF(
            TRIM(S.RAW_DATA:supplier.supplier_id::STRING),
            ''
        ) IS NULL

        OR TRY_TO_DOUBLE(
            S.RAW_DATA:supplier.performance_score::STRING
        ) IS NULL
        OR TRY_TO_DOUBLE(
            S.RAW_DATA:supplier.performance_score::STRING
        ) > 100

        OR TRY_TO_DOUBLE(
            S.RAW_DATA:supplier.performance_score::STRING
        ) < 0

        OR TRY_TO_NUMBER(
            S.RAW_DATA:supplier.lead_time_days::STRING
        ) IS NULL
        OR TRY_TO_NUMBER(
            S.RAW_DATA:supplier.lead_time_days::STRING
        ) < 0


        -- Product level

        OR ARRAY_SIZE(S.RAW_DATA:items) IS NULL
        OR ARRAY_SIZE(S.RAW_DATA:items) <> 1
        OR NULLIF(
            TRIM(S.RAW_DATA:items[0].product_id::STRING),
            ''
        ) IS NULL

        OR TRY_TO_NUMBER(
            S.RAW_DATA:items[0].quantity::STRING
        ) IS NULL

        OR TRY_TO_NUMBER(
            S.RAW_DATA:items[0].quantity::STRING
        ) <= 0

        OR TRY_TO_DECIMAL(S.RAW_DATA:items[0].unit_price::STRING, 18, 2) IS NULL

        OR TRY_TO_DECIMAL(S.RAW_DATA:items[0].unit_price::STRING, 18, 2) < 0


        -- Financial level

        OR TRY_TO_DECIMAL(S.RAW_DATA:financials.total_amount::STRING, 18, 2) IS NULL

        OR TRY_TO_DECIMAL(S.RAW_DATA:financials.total_amount::STRING, 18, 2) < 0


        -- Shipment / warehouse numeric fields
        OR (
            NULLIF(TRIM(S.RAW_DATA:shipment.delay_days::STRING), '') IS NOT NULL
            AND TRY_TO_NUMBER(S.RAW_DATA:shipment.delay_days::STRING) IS NULL
        )
        OR (
            NULLIF(TRIM(S.RAW_DATA:warehouse.inventory_level::STRING), '') IS NOT NULL
            AND TRY_TO_NUMBER(S.RAW_DATA:warehouse.inventory_level::STRING) IS NULL
        )
        OR TRY_TO_NUMBER(
            S.RAW_DATA:warehouse.inventory_level::STRING
        ) < 0;


    -- ========================================================================
    -- PART 2 - MERGE CLEAN RECORDS INTO STAGING TABLE
    -- ========================================================================

    MERGE INTO SILVER_SCH.STG_ORDERS AS TGT

    USING (

        SELECT

            -- Order fields

            UPPER(TRIM(S.RAW_DATA:order_id::STRING)) AS ORDER_ID,

            TRY_TO_TIMESTAMP_NTZ(

                CASE

                    WHEN S.RAW_DATA:order_date::STRING LIKE '__-__-____'

                    THEN TO_VARCHAR(
                        TO_DATE(
                            S.RAW_DATA:order_date::STRING,
                            'DD-MM-YYYY'
                        ),
                        'YYYY-MM-DD'
                    ) || 'T00:00:00'

                    ELSE S.RAW_DATA:order_date::STRING

                END

            ) AS ORDER_DATE,

            UPPER(TRIM(
                S.RAW_DATA:order_status::STRING
            )) AS ORDER_STATUS,


            -- Customer fields

            UPPER(TRIM(
                S.RAW_DATA:customer.customer_id::STRING
            )) AS CUSTOMER_ID,

            COALESCE(
                UPPER(TRIM(
                    S.RAW_DATA:customer.customer_name::STRING
                )),
                'UNKNOWN'
            ) AS CUSTOMER_NAME,

            COALESCE(
                UPPER(TRIM(
                    S.RAW_DATA:customer.region::STRING
                )),
                'UNKNOWN'
            ) AS CUSTOMER_REGION,

            COALESCE(
                UPPER(TRIM(
                    S.RAW_DATA:customer.segment::STRING
                )),
                'UNKNOWN'
            ) AS CUSTOMER_SEGMENT,


            -- Supplier fields

            UPPER(TRIM(
                S.RAW_DATA:supplier.supplier_id::STRING
            )) AS SUPPLIER_ID,

            COALESCE(
                UPPER(TRIM(
                    S.RAW_DATA:supplier.supplier_name::STRING
                )),
                'UNKNOWN'
            ) AS SUPPLIER_NAME,

            COALESCE(
                UPPER(TRIM(
                    S.RAW_DATA:supplier.country::STRING
                )),
                'UNKNOWN'
            ) AS SUPPLIER_COUNTRY,

            TRY_TO_NUMBER(S.RAW_DATA:supplier.lead_time_days::STRING) AS LEAD_TIME_DAYS,

            TRY_TO_DOUBLE(S.RAW_DATA:supplier.performance_score::STRING) AS PERFORMANCE_SCORE,


            -- Shipment fields

            COALESCE(
                UPPER(TRIM(
                    S.RAW_DATA:shipment.shipment_id::STRING
                )),
                'UNKNOWN'
            ) AS SHIPMENT_ID,

            COALESCE(
                UPPER(TRIM(
                    S.RAW_DATA:shipment.carrier::STRING
                )),
                'UNKNOWN'
            ) AS CARRIER,

            TRY_TO_TIMESTAMP_NTZ(
                S.RAW_DATA:shipment.ship_date::STRING
            ) AS SHIP_DATE,

            TRY_TO_DATE(
                S.RAW_DATA:shipment.estimated_delivery::STRING
            ) AS ESTIMATED_DELIVERY,

            -- Keep early deliveries negative and unknown delays NULL.
            TRY_TO_NUMBER(S.RAW_DATA:shipment.delay_days::STRING) AS DELAY_DAYS,


            -- Product fields (items[0] -> first array element)

            UPPER(TRIM(
                S.RAW_DATA:items[0].product_id::STRING
            )) AS PRODUCT_ID,

            COALESCE(
                UPPER(TRIM(
                    S.RAW_DATA:items[0].product_name::STRING
                )),
                'UNKNOWN'
            ) AS PRODUCT_NAME,

            COALESCE(
                UPPER(TRIM(
                    S.RAW_DATA:items[0].category::STRING
                )),
                'UNKNOWN'
            ) AS CATEGORY,

            TRY_TO_NUMBER(
                S.RAW_DATA:items[0].quantity::STRING
            ) AS QUANTITY,

            TRY_TO_DECIMAL(S.RAW_DATA:items[0].unit_price::STRING, 18, 2) AS UNIT_PRICE,


            -- Financial fields

            TRY_TO_DECIMAL(S.RAW_DATA:financials.total_amount::STRING, 18, 2) AS TOTAL_AMOUNT,

            COALESCE(
                UPPER(TRIM(
                    S.RAW_DATA:financials.payment_status::STRING
                )),
                'UNKNOWN'
            ) AS PAYMENT_STATUS,


            -- Warehouse fields

            COALESCE(
                UPPER(TRIM(
                    S.RAW_DATA:warehouse.warehouse_id::STRING
                )),
                'UNKNOWN'
            ) AS WAREHOUSE_ID,

            COALESCE(
                UPPER(TRIM(
                    S.RAW_DATA:warehouse.warehouse_location::STRING
                )),
                'UNKNOWN'
            ) AS WAREHOUSE_LOCATION,

            TRY_TO_NUMBER(S.RAW_DATA:warehouse.inventory_level::STRING) AS INVENTORY_LEVEL,


            -- Metadata

            S.FILE_NAME,
            S.FILE_ROW_NUMBER,
            S.INGESTED_AT

        FROM STREAM_BUFFER AS S

        WHERE

            S.RAW_DATA:order_id::STRING IS NOT NULL

            AND (
                TRY_TO_TIMESTAMP_NTZ(
                    S.RAW_DATA:order_date::STRING
                ) IS NOT NULL

                OR TRY_TO_DATE(
                    S.RAW_DATA:order_date::STRING,
                    'DD-MM-YYYY'
                ) IS NOT NULL
            )

            AND NULLIF(UPPER(TRIM(S.RAW_DATA:order_status::STRING)), '') IS NOT NULL
            AND UPPER(TRIM(
                S.RAW_DATA:order_status::STRING
            )) IN (
                'PENDING',
                'PROCESSING',
                'SHIPPED',
                'IN TRANSIT',
                'DELIVERED',
                'CANCELLED'
            )

            AND NULLIF(TRIM(S.RAW_DATA:customer.customer_id::STRING), '') IS NOT NULL

            AND NULLIF(TRIM(S.RAW_DATA:supplier.supplier_id::STRING), '') IS NOT NULL

            AND TRY_TO_DOUBLE(
                S.RAW_DATA:supplier.performance_score::STRING
            ) BETWEEN 0 AND 100

            AND TRY_TO_NUMBER(
                S.RAW_DATA:supplier.lead_time_days::STRING
            ) IS NOT NULL
            AND TRY_TO_NUMBER(
                S.RAW_DATA:supplier.lead_time_days::STRING
            ) >= 0

            AND ARRAY_SIZE(S.RAW_DATA:items) = 1
            AND NULLIF(TRIM(S.RAW_DATA:items[0].product_id::STRING), '') IS NOT NULL

            AND TRY_TO_NUMBER(
                S.RAW_DATA:items[0].quantity::STRING
            ) IS NOT NULL

            AND TRY_TO_NUMBER(
                S.RAW_DATA:items[0].quantity::STRING
            ) > 0

            AND TRY_TO_DECIMAL(S.RAW_DATA:items[0].unit_price::STRING, 18, 2) IS NOT NULL

            AND TRY_TO_DECIMAL(S.RAW_DATA:items[0].unit_price::STRING, 18, 2) >= 0

            AND TRY_TO_DECIMAL(S.RAW_DATA:financials.total_amount::STRING, 18, 2) IS NOT NULL

            AND TRY_TO_DECIMAL(S.RAW_DATA:financials.total_amount::STRING, 18, 2) >= 0

            AND (
                NULLIF(TRIM(S.RAW_DATA:warehouse.inventory_level::STRING), '') IS NULL
                OR (
                    TRY_TO_NUMBER(S.RAW_DATA:warehouse.inventory_level::STRING) IS NOT NULL
                    AND TRY_TO_NUMBER(S.RAW_DATA:warehouse.inventory_level::STRING) >= 0
                )
            )
            AND (
                NULLIF(TRIM(S.RAW_DATA:shipment.delay_days::STRING), '') IS NULL
                OR TRY_TO_NUMBER(S.RAW_DATA:shipment.delay_days::STRING) IS NOT NULL
            )

        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY UPPER(TRIM(S.RAW_DATA:order_id::STRING))
            ORDER BY S.INGESTED_AT DESC, S.FILE_NAME DESC, S.FILE_ROW_NUMBER DESC
        ) = 1

    ) AS SRC

    ON TGT.ORDER_ID = SRC.ORDER_ID


    -- ========================================================================
    -- UPDATE MATCHED RECORDS
    -- ========================================================================

    WHEN MATCHED THEN UPDATE SET

        TGT.ORDER_DATE          = SRC.ORDER_DATE,
        TGT.ORDER_STATUS        = SRC.ORDER_STATUS,
        TGT.CUSTOMER_ID         = SRC.CUSTOMER_ID,
        TGT.CUSTOMER_NAME       = SRC.CUSTOMER_NAME,
        TGT.CUSTOMER_REGION     = SRC.CUSTOMER_REGION,
        TGT.CUSTOMER_SEGMENT    = SRC.CUSTOMER_SEGMENT,
        TGT.SUPPLIER_ID         = SRC.SUPPLIER_ID,
        TGT.SUPPLIER_NAME       = SRC.SUPPLIER_NAME,
        TGT.SUPPLIER_COUNTRY    = SRC.SUPPLIER_COUNTRY,
        TGT.LEAD_TIME_DAYS      = SRC.LEAD_TIME_DAYS,
        TGT.PERFORMANCE_SCORE   = SRC.PERFORMANCE_SCORE,
        TGT.SHIPMENT_ID         = SRC.SHIPMENT_ID,
        TGT.CARRIER             = SRC.CARRIER,
        TGT.SHIP_DATE           = SRC.SHIP_DATE,
        TGT.ESTIMATED_DELIVERY  = SRC.ESTIMATED_DELIVERY,
        TGT.DELAY_DAYS          = SRC.DELAY_DAYS,
        TGT.PRODUCT_ID          = SRC.PRODUCT_ID,
        TGT.PRODUCT_NAME        = SRC.PRODUCT_NAME,
        TGT.CATEGORY            = SRC.CATEGORY,
        TGT.QUANTITY            = SRC.QUANTITY,
        TGT.UNIT_PRICE          = SRC.UNIT_PRICE,
        TGT.TOTAL_AMOUNT        = SRC.TOTAL_AMOUNT,
        TGT.PAYMENT_STATUS      = SRC.PAYMENT_STATUS,
        TGT.WAREHOUSE_ID        = SRC.WAREHOUSE_ID,
        TGT.WAREHOUSE_LOCATION  = SRC.WAREHOUSE_LOCATION,
        TGT.INVENTORY_LEVEL     = SRC.INVENTORY_LEVEL,
        TGT.FILE_NAME           = SRC.FILE_NAME,
        TGT.FILE_ROW_NUMBER     = SRC.FILE_ROW_NUMBER,
        TGT.INGESTED_AT         = SRC.INGESTED_AT,
        TGT.TRANSFORMED_AT      = CURRENT_TIMESTAMP()


    -- ========================================================================
    -- INSERT NEW RECORDS
    -- ========================================================================

    WHEN NOT MATCHED THEN INSERT (

        ORDER_ID,
        ORDER_DATE,
        ORDER_STATUS,

        CUSTOMER_ID,
        CUSTOMER_NAME,
        CUSTOMER_REGION,
        CUSTOMER_SEGMENT,

        SUPPLIER_ID,
        SUPPLIER_NAME,
        SUPPLIER_COUNTRY,
        LEAD_TIME_DAYS,
        PERFORMANCE_SCORE,

        SHIPMENT_ID,
        CARRIER,
        SHIP_DATE,
        ESTIMATED_DELIVERY,
        DELAY_DAYS,

        PRODUCT_ID,
        PRODUCT_NAME,
        CATEGORY,
        QUANTITY,
        UNIT_PRICE,

        TOTAL_AMOUNT,
        PAYMENT_STATUS,

        WAREHOUSE_ID,
        WAREHOUSE_LOCATION,
        INVENTORY_LEVEL,

        FILE_NAME,
        FILE_ROW_NUMBER,
        INGESTED_AT

    )

    VALUES (

        SRC.ORDER_ID,
        SRC.ORDER_DATE,
        SRC.ORDER_STATUS,

        SRC.CUSTOMER_ID,
        SRC.CUSTOMER_NAME,
        SRC.CUSTOMER_REGION,
        SRC.CUSTOMER_SEGMENT,

        SRC.SUPPLIER_ID,
        SRC.SUPPLIER_NAME,
        SRC.SUPPLIER_COUNTRY,
        SRC.LEAD_TIME_DAYS,
        SRC.PERFORMANCE_SCORE,

        SRC.SHIPMENT_ID,
        SRC.CARRIER,
        SRC.SHIP_DATE,
        SRC.ESTIMATED_DELIVERY,
        SRC.DELAY_DAYS,

        SRC.PRODUCT_ID,
        SRC.PRODUCT_NAME,
        SRC.CATEGORY,
        SRC.QUANTITY,
        SRC.UNIT_PRICE,

        SRC.TOTAL_AMOUNT,
        SRC.PAYMENT_STATUS,

        SRC.WAREHOUSE_ID,
        SRC.WAREHOUSE_LOCATION,
        SRC.INVENTORY_LEVEL,

        SRC.FILE_NAME,
        SRC.FILE_ROW_NUMBER,
        SRC.INGESTED_AT

    );


    -- ========================================================================
    -- PART 3 - CLEANUP
    -- ========================================================================

    COMMIT;

    DROP TABLE IF EXISTS STREAM_BUFFER;


    -- ========================================================================
    -- PART 4 - RETURN EXECUTION RESULT
    -- ========================================================================

    RETURN
        'sp_bronze_to_silver completed successfully. Records processed: '
        || V_PROCESSED;


END;

$$;


-- ============================================================================
-- STEP 4 - VERIFY PROCEDURE
-- ============================================================================

SHOW PROCEDURES IN SCHEMA SILVER_SCH;


-- ============================================================================
-- STEP 5 - EXECUTE PROCEDURE
-- ============================================================================

CALL SILVER_SCH.SP_BRONZE_TO_SILVER();


-- ============================================================================
-- STEP 6 - VERIFY SILVER STAGING DATA
-- ============================================================================

SELECT *
FROM SILVER_SCH.STG_ORDERS
ORDER BY ORDER_DATE DESC;


-- ============================================================================
-- STEP 7 - VERIFY DEAD LETTER RECORDS
-- ============================================================================

SELECT *
FROM SILVER_SCH.DEAD_LETTER
ORDER BY REJECTED_AT DESC;


-- ============================================================================
-- STEP 8 - VALIDATION SUMMARY
-- ============================================================================

SELECT

    (SELECT COUNT(*)
     FROM SILVER_SCH.STG_ORDERS) AS SILVER_RECORDS,

    (SELECT COUNT(*)
     FROM SILVER_SCH.DEAD_LETTER) AS REJECTED_RECORDS,

    (SELECT COUNT(DISTINCT ORDER_ID)
     FROM SILVER_SCH.STG_ORDERS) AS UNIQUE_ORDERS;


-- ============================================================================
-- END OF SILVER LAYER SCRIPT
-- ============================================================================



CREATE OR REPLACE TASK silver_sch.bronze_to_silver_task
    WAREHOUSE = flowbridge_pipeline_wh
    SCHEDULE = '1 minute'
    COMMENT = 'Task - calls sp_bronze_to_silver every minute when raw_orders_stream has data'
WHEN SYSTEM$STREAM_HAS_DATA('silver_sch.raw_orders_stream')
AS
    CALL silver_sch.sp_bronze_to_silver();



-- STEP 6 - RESUME TASK
------------------------------------------------------------
alter task silver_sch.bronze_to_silver_task resume;

show tasks;
--
-- STEP 7 - TEST STORED PROCEDURE DIRECTLY

call silver_sch.sp_bronze_to_silver();

select system$stream_has_data('silver_sch.raw_orders_stream');


SELECT COUNT(*) AS BRONZE_RECORD_COUNT
FROM FLOWBRIDGE_DEV_DB.BRONZE_SCH.RAW_ORDERS;

SHOW STREAMS IN SCHEMA FLOWBRIDGE_DEV_DB.SILVER_SCH;

DESCRIBE STREAM FLOWBRIDGE_DEV_DB.SILVER_SCH.RAW_ORDERS_STREAM;

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
WHERE NAME = 'BRONZE_TO_SILVER_TASK'
ORDER BY SCHEDULED_TIME DESC;

select count(*) as stg_orders_count from silver_sch.stg_orders
union all
select count(*) as stg_orders_count from silver_sch.dead_letter;



select * from silver_sch.dead_letter;

select * from silver_sch.stg_orders;
