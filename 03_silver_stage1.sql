-- ============================================================
-- FLOWBRIDGE SUPPLY CHAIN
-- SNOWFLAKE BRONZE -> SILVER PIPELINE
-- ============================================================


-- ============================================================
-- 1. ENVIRONMENT
-- ============================================================

SET env = 'DEV';

SET db = 'FLOWBRIDGE_' || $env || '_DB';


-- ============================================================
-- 2. ROLE / DATABASE / SCHEMA / WAREHOUSE
-- ============================================================

USE ROLE SYSADMIN;

USE DATABASE IDENTIFIER($db);

USE SCHEMA SILVER_SCH;

USE WAREHOUSE FLOWBRIDGE_PIPELINE_WH;


-- ============================================================
-- 3. AZURE STORAGE INTEGRATION
-- ============================================================

CREATE STORAGE INTEGRATION IF NOT EXISTS FLOWBRIDGE_ADLS_INTEGRATION
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'AZURE'
    ENABLED = TRUE
    AZURE_TENANT_ID = 'dc78956f-8c91-443d-bc5f-b2fa60afcac3'
    STORAGE_ALLOWED_LOCATIONS = (
        'azure://flowbrigeadls.blob.core.windows.net/supply-chain-raw-dev/',
        'azure://flowbrigeadls.blob.core.windows.net/supply-chain-raw-prod/'
    );


-- ============================================================
-- 4. AZURE NOTIFICATION INTEGRATION
-- ============================================================

CREATE OR REPLACE NOTIFICATION INTEGRATION FLOW_BRIDGE_AZURE_NOTIFICATIONS_INT
    ENABLED = TRUE
    TYPE = QUEUE
    NOTIFICATION_PROVIDER = AZURE_STORAGE_QUEUE
    AZURE_STORAGE_QUEUE_PRIMARY_URI =
        'https://flowbrigeadls.queue.core.windows.net/flowbridge-supply-chain-queue'
    AZURE_TENANT_ID = 'dc78956f-8c91-443d-bc5f-b2fa60afcac3';


-- ============================================================
-- 5. COPY HISTORY CHECK
-- ============================================================

SELECT *
FROM TABLE(
    INFORMATION_SCHEMA.COPY_HISTORY(
        TABLE_NAME => 'RAW_ORDERS',
        START_TIME => DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
    )
);


-- ============================================================
-- 6. DEAD LETTER TABLE
-- ============================================================

CREATE OR REPLACE TRANSIENT TABLE SILVER_SCH.DEAD_LETTER
(
    RAW_DATA        VARIANT,
    ERROR_REASON    STRING,
    FILE_NAME       STRING,
    FILE_ROW_NUMBER NUMBER,
    REJECTED_AT     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Dead Letter Table for rejected or invalid Bronze records';


-- ============================================================
-- 7. SILVER STAGING TABLE
-- ============================================================

CREATE OR REPLACE TABLE SILVER_SCH.STG_ORDERS
(
    -- --------------------------------------------------------
    -- Order fields
    -- --------------------------------------------------------

    ORDER_ID           STRING
        COMMENT 'Unique order identifier',

    ORDER_DATE         TIMESTAMP_NTZ
        COMMENT 'Order timestamp',

    ORDER_STATUS       STRING
        COMMENT 'Current order status',


    -- --------------------------------------------------------
    -- Customer fields
    -- --------------------------------------------------------

    CUSTOMER_ID        STRING
        COMMENT 'Customer identifier',

    CUSTOMER_NAME      STRING
        COMMENT 'Customer name',

    CUSTOMER_REGION    STRING
        COMMENT 'Customer region',

    CUSTOMER_SEGMENT   STRING
        COMMENT 'Customer segment',


    -- --------------------------------------------------------
    -- Supplier fields
    -- --------------------------------------------------------

    SUPPLIER_ID        STRING
        COMMENT 'Supplier identifier',

    SUPPLIER_NAME      STRING
        COMMENT 'Supplier name',

    SUPPLIER_COUNTRY   STRING
        COMMENT 'Supplier country',

    LEAD_TIME_DAYS     NUMBER
        COMMENT 'Supplier lead time in days',

    PERFORMANCE_SCORE  FLOAT
        COMMENT 'Supplier performance score',


    -- --------------------------------------------------------
    -- Shipment fields
    -- --------------------------------------------------------

    SHIPMENT_ID        STRING
        COMMENT 'Shipment identifier',

    CARRIER            STRING
        COMMENT 'Shipping carrier',

    SHIP_DATE          TIMESTAMP_NTZ
        COMMENT 'Shipment date',

    ESTIMATED_DELIVERY DATE
        COMMENT 'Estimated delivery date',

    DELAY_DAYS         NUMBER
        COMMENT 'Number of delay days',


    -- --------------------------------------------------------
    -- Product fields
    -- --------------------------------------------------------

    PRODUCT_ID         STRING
        COMMENT 'Product identifier',

    PRODUCT_NAME       STRING
        COMMENT 'Product name',

    CATEGORY           STRING
        COMMENT 'Product category',

    QUANTITY            NUMBER
        COMMENT 'Order quantity',

    UNIT_PRICE          FLOAT
        COMMENT 'Unit price',


    -- --------------------------------------------------------
    -- Financial fields
    -- --------------------------------------------------------

    TOTAL_AMOUNT        FLOAT
        COMMENT 'Total order amount',

    PAYMENT_STATUS      STRING
        COMMENT 'Payment status',


    -- --------------------------------------------------------
    -- Warehouse fields
    -- --------------------------------------------------------

    WAREHOUSE_ID        STRING
        COMMENT 'Warehouse identifier',

    WAREHOUSE_LOCATION  STRING
        COMMENT 'Warehouse location',

    INVENTORY_LEVEL     NUMBER
        COMMENT 'Current inventory level',


    -- --------------------------------------------------------
    -- Metadata
    -- --------------------------------------------------------

    FILE_NAME            STRING
        COMMENT 'Source file name',

    FILE_ROW_NUMBER      NUMBER
        COMMENT 'Row number in source file',

    INGESTED_AT          TIMESTAMP_NTZ
        COMMENT 'When record was ingested',

    TRANSFORMED_AT       TIMESTAMP_NTZ
        DEFAULT CURRENT_TIMESTAMP()
        COMMENT 'When record was transformed'
)
COMMENT = 'Silver Stage 1 - flattened and cleaned supply chain orders';


-- ============================================================
-- 8. BRONZE -> SILVER STREAM
-- ============================================================

CREATE OR REPLACE STREAM SILVER_SCH.RAW_ORDERS_STREAM
ON TABLE FLOWBRIDGE_DEV_DB.BRONZE_SCH.RAW_ORDERS
APPEND_ONLY = TRUE
SHOW_INITIAL_ROWS = TRUE
COMMENT = 'Stream on Bronze raw orders capturing new records for Silver processing';


-- ============================================================
-- 9. CHECK STREAM
-- ============================================================

SHOW STREAMS;


-- ============================================================
-- 10. STORED PROCEDURE
--     BRONZE -> SILVER
-- ============================================================

CREATE OR REPLACE PROCEDURE SILVER_SCH.SP_BRONZE_TO_SILVER()
RETURNS STRING
LANGUAGE SQL
COMMENT = 'Routes invalid records to DEAD_LETTER and merges valid records into STG_ORDERS'
AS
$$
BEGIN

    -- ========================================================
    -- STEP 1
    -- Capture INSERT records from Bronze stream
    -- ========================================================

    CREATE OR REPLACE TEMPORARY TABLE STREAM_BUFFER AS
    SELECT *
    FROM SILVER_SCH.RAW_ORDERS_STREAM
    WHERE METADATA$ACTION = 'INSERT';


    -- ========================================================
    -- STEP 2
    -- Route invalid records to DEAD_LETTER
    -- ========================================================

    INSERT INTO SILVER_SCH.DEAD_LETTER
    (
        RAW_DATA,
        ERROR_REASON,
        FILE_NAME,
        FILE_ROW_NUMBER
    )

    SELECT
        S.RAW_DATA,

        CASE

            -- ------------------------------------------------
            -- ORDER VALIDATIONS
            -- ------------------------------------------------

            WHEN S.RAW_DATA:order_id::STRING IS NULL
                THEN 'Missing order_id'

            WHEN
                TRY_TO_TIMESTAMP_NTZ(
                    S.RAW_DATA:order_date::STRING
                ) IS NULL
                AND NOT REGEXP_LIKE(
                    S.RAW_DATA:order_date::STRING,
                    '^[0-9]{2}-[0-9]{2}-[0-9]{4}$'
                )
                THEN 'Invalid or missing order_date'

            WHEN UPPER(
                    TRIM(
                        S.RAW_DATA:order_status::STRING
                    )
                 ) NOT IN
                 (
                    'PENDING',
                    'PROCESSING',
                    'SHIPPED',
                    'IN TRANSIT',
                    'DELIVERED',
                    'CANCELLED'
                 )
                THEN
                    'Invalid order_status: '
                    ||
                    COALESCE(
                        S.RAW_DATA:order_status::STRING,
                        'NULL'
                    )


            -- ------------------------------------------------
            -- CUSTOMER VALIDATIONS
            -- ------------------------------------------------

            WHEN S.RAW_DATA:customer.customer_id::STRING IS NULL
                THEN 'Missing customer_id'


            -- ------------------------------------------------
            -- SUPPLIER VALIDATIONS
            -- ------------------------------------------------

            WHEN S.RAW_DATA:supplier.supplier_id::STRING IS NULL
                THEN 'Missing supplier_id'

            WHEN S.RAW_DATA:supplier.performance_score::FLOAT > 100
                THEN
                    'Invalid performance_score > 100: '
                    ||
                    S.RAW_DATA:supplier.performance_score::STRING

            WHEN S.RAW_DATA:supplier.performance_score::FLOAT < 0
                THEN
                    'Invalid performance_score < 0: '
                    ||
                    S.RAW_DATA:supplier.performance_score::STRING

            WHEN S.RAW_DATA:supplier.lead_time_days::NUMBER < 0
                THEN
                    'Invalid lead_time_days < 0: '
                    ||
                    S.RAW_DATA:supplier.lead_time_days::STRING


            -- ------------------------------------------------
            -- PRODUCT VALIDATIONS
            -- ------------------------------------------------

            WHEN S.RAW_DATA:items[0].product_id::STRING IS NULL
                THEN 'Missing product_id'

            WHEN
                S.RAW_DATA:items[0].quantity::NUMBER IS NULL
                OR
                S.RAW_DATA:items[0].quantity::NUMBER <= 0
                THEN
                    'Invalid quantity: '
                    ||
                    COALESCE(
                        S.RAW_DATA:items[0].quantity::STRING,
                        'NULL'
                    )

            WHEN
                S.RAW_DATA:items[0].unit_price::FLOAT IS NULL
                OR
                S.RAW_DATA:items[0].unit_price::FLOAT < 0
                THEN
                    'Invalid unit_price: '
                    ||
                    COALESCE(
                        S.RAW_DATA:items[0].unit_price::STRING,
                        'NULL'
                    )


            -- ------------------------------------------------
            -- FINANCIAL VALIDATIONS
            -- ------------------------------------------------

            WHEN
                S.RAW_DATA:financials.total_amount::FLOAT IS NULL
                OR
                S.RAW_DATA:financials.total_amount::FLOAT < 0
                THEN
                    'Invalid total_amount: '
                    ||
                    COALESCE(
                        S.RAW_DATA:financials.total_amount::STRING,
                        'NULL'
                    )


            -- ------------------------------------------------
            -- WAREHOUSE VALIDATIONS
            -- ------------------------------------------------

            WHEN
                S.RAW_DATA:warehouse.inventory_level::NUMBER < 0
                THEN
                    'Invalid inventory_level < 0: '
                    ||
                    S.RAW_DATA:warehouse.inventory_level::STRING

            ELSE 'Unknown'

        END AS ERROR_REASON,

        S.FILE_NAME,
        S.FILE_ROW_NUMBER

    FROM STREAM_BUFFER AS S

    WHERE

        -- ----------------------------------------------------
        -- ORDER
        -- ----------------------------------------------------

        S.RAW_DATA:order_id::STRING IS NULL

        OR
        (
            TRY_TO_TIMESTAMP_NTZ(
                S.RAW_DATA:order_date::STRING
            ) IS NULL

            AND NOT REGEXP_LIKE(
                S.RAW_DATA:order_date::STRING,
                '^[0-9]{2}-[0-9]{2}-[0-9]{4}$'
            )
        )

        OR UPPER(
            TRIM(
                S.RAW_DATA:order_status::STRING
            )
        ) NOT IN
        (
            'PENDING',
            'PROCESSING',
            'SHIPPED',
            'IN TRANSIT',
            'DELIVERED',
            'CANCELLED'
        )


        -- ----------------------------------------------------
        -- CUSTOMER
        -- ----------------------------------------------------

        OR S.RAW_DATA:customer.customer_id::STRING IS NULL


        -- ----------------------------------------------------
        -- SUPPLIER
        -- ----------------------------------------------------

        OR S.RAW_DATA:supplier.supplier_id::STRING IS NULL

        OR S.RAW_DATA:supplier.performance_score::FLOAT > 100

        OR S.RAW_DATA:supplier.performance_score::FLOAT < 0

        OR S.RAW_DATA:supplier.lead_time_days::NUMBER < 0


        -- ----------------------------------------------------
        -- PRODUCT
        -- ----------------------------------------------------

        OR S.RAW_DATA:items[0].product_id::STRING IS NULL

        OR S.RAW_DATA:items[0].quantity::NUMBER IS NULL

        OR S.RAW_DATA:items[0].quantity::NUMBER <= 0

        OR S.RAW_DATA:items[0].unit_price::FLOAT IS NULL

        OR S.RAW_DATA:items[0].unit_price::FLOAT < 0


        -- ----------------------------------------------------
        -- FINANCIAL
        -- ----------------------------------------------------

        OR S.RAW_DATA:financials.total_amount::FLOAT IS NULL

        OR S.RAW_DATA:financials.total_amount::FLOAT < 0


        -- ----------------------------------------------------
        -- WAREHOUSE
        -- ----------------------------------------------------

        OR S.RAW_DATA:warehouse.inventory_level::NUMBER < 0;


    -- ========================================================
    -- STEP 3
    -- Merge valid records into STG_ORDERS
    -- ========================================================

    MERGE INTO SILVER_SCH.STG_ORDERS AS TGT

    USING
    (
        SELECT

            -- ------------------------------------------------
            -- ORDER
            -- ------------------------------------------------

            UPPER(
                TRIM(
                    S.RAW_DATA:order_id::STRING
                )
            ) AS ORDER_ID,


            TRY_TO_TIMESTAMP_NTZ(
                CASE

                    WHEN REGEXP_LIKE(
                        S.RAW_DATA:order_date::STRING,
                        '^[0-9]{2}-[0-9]{2}-[0-9]{4}$'
                    )

                    THEN
                        TO_VARCHAR(
                            TO_DATE(
                                S.RAW_DATA:order_date::STRING,
                                'DD-MM-YYYY'
                            ),
                            'YYYY-MM-DD'
                        )
                        || ' 00:00:00'

                    ELSE
                        S.RAW_DATA:order_date::STRING

                END
            ) AS ORDER_DATE,


            UPPER(
                TRIM(
                    S.RAW_DATA:order_status::STRING
                )
            ) AS ORDER_STATUS,


            -- ------------------------------------------------
            -- CUSTOMER
            -- ------------------------------------------------

            UPPER(
                TRIM(
                    S.RAW_DATA:customer.customer_id::STRING
                )
            ) AS CUSTOMER_ID,

            COALESCE(
                UPPER(
                    TRIM(
                        S.RAW_DATA:customer.customer_name::STRING
                    )
                ),
                'UNKNOWN'
            ) AS CUSTOMER_NAME,

            COALESCE(
                UPPER(
                    TRIM(
                        S.RAW_DATA:customer.region::STRING
                    )
                ),
                'UNKNOWN'
            ) AS CUSTOMER_REGION,

            COALESCE(
                UPPER(
                    TRIM(
                        S.RAW_DATA:customer.segment::STRING
                    )
                ),
                'UNKNOWN'
            ) AS CUSTOMER_SEGMENT,


            -- ------------------------------------------------
            -- SUPPLIER
            -- ------------------------------------------------

            UPPER(
                TRIM(
                    S.RAW_DATA:supplier.supplier_id::STRING
                )
            ) AS SUPPLIER_ID,

            COALESCE(
                UPPER(
                    TRIM(
                        S.RAW_DATA:supplier.supplier_name::STRING
                    )
                ),
                'UNKNOWN'
            ) AS SUPPLIER_NAME,

            COALESCE(
                UPPER(
                    TRIM(
                        S.RAW_DATA:supplier.supplier_country::STRING
                    )
                ),
                'UNKNOWN'
            ) AS SUPPLIER_COUNTRY,

            COALESCE(
                S.RAW_DATA:supplier.lead_time_days::NUMBER,
                0
            ) AS LEAD_TIME_DAYS,

            COALESCE(
                S.RAW_DATA:supplier.performance_score::FLOAT,
                0
            ) AS PERFORMANCE_SCORE,


            -- ------------------------------------------------
            -- SHIPMENT
            -- ------------------------------------------------

            COALESCE(
                UPPER(
                    TRIM(
                        S.RAW_DATA:shipment.shipment_id::STRING
                    )
                ),
                'UNKNOWN'
            ) AS SHIPMENT_ID,

            COALESCE(
                UPPER(
                    TRIM(
                        S.RAW_DATA:shipment.carrier::STRING
                    )
                ),
                'UNKNOWN'
            ) AS CARRIER,

            TRY_TO_TIMESTAMP_NTZ(
                S.RAW_DATA:shipment.ship_date::STRING
            ) AS SHIP_DATE,

            TRY_TO_DATE(
                S.RAW_DATA:shipment.estimated_delivery::STRING
            ) AS ESTIMATED_DELIVERY,

            GREATEST(
                COALESCE(
                    S.RAW_DATA:shipment.delay_days::NUMBER,
                    0
                ),
                0
            ) AS DELAY_DAYS,


            -- ------------------------------------------------
            -- PRODUCT
            -- ------------------------------------------------

            UPPER(
                TRIM(
                    S.RAW_DATA:items[0].product_id::STRING
                )
            ) AS PRODUCT_ID,

            COALESCE(
                UPPER(
                    TRIM(
                        S.RAW_DATA:items[0].product_name::STRING
                    )
                ),
                'UNKNOWN'
            ) AS PRODUCT_NAME,

            COALESCE(
                UPPER(
                    TRIM(
                        S.RAW_DATA:items[0].category::STRING
                    )
                ),
                'UNKNOWN'
            ) AS CATEGORY,

            S.RAW_DATA:items[0].quantity::NUMBER
                AS QUANTITY,

            S.RAW_DATA:items[0].unit_price::FLOAT
                AS UNIT_PRICE,


            -- ------------------------------------------------
            -- FINANCIAL
            -- ------------------------------------------------

            S.RAW_DATA:financials.total_amount::FLOAT
                AS TOTAL_AMOUNT,

            COALESCE(
                UPPER(
                    TRIM(
                        S.RAW_DATA:financials.payment_status::STRING
                    )
                ),
                'UNKNOWN'
            ) AS PAYMENT_STATUS,


            -- ------------------------------------------------
            -- WAREHOUSE
            -- ------------------------------------------------

            COALESCE(
                UPPER(
                    TRIM(
                        S.RAW_DATA:warehouse.warehouse_id::STRING
                    )
                ),
                'UNKNOWN'
            ) AS WAREHOUSE_ID,

            COALESCE(
                UPPER(
                    TRIM(
                        S.RAW_DATA:warehouse.warehouse_location::STRING
                    )
                ),
                'UNKNOWN'
            ) AS WAREHOUSE_LOCATION,

            COALESCE(
                S.RAW_DATA:warehouse.inventory_level::NUMBER,
                0
            ) AS INVENTORY_LEVEL,


            -- ------------------------------------------------
            -- METADATA
            -- ------------------------------------------------

            S.FILE_NAME,

            S.FILE_ROW_NUMBER,

            S.INGESTED_AT

        FROM STREAM_BUFFER AS S

        WHERE

            -- ------------------------------------------------
            -- ORDER VALIDATION
            -- ------------------------------------------------

            S.RAW_DATA:order_id::STRING IS NOT NULL

            AND
            (
                TRY_TO_TIMESTAMP_NTZ(
                    S.RAW_DATA:order_date::STRING
                ) IS NOT NULL

                OR REGEXP_LIKE(
                    S.RAW_DATA:order_date::STRING,
                    '^[0-9]{2}-[0-9]{2}-[0-9]{4}$'
                )
            )

            AND UPPER(
                TRIM(
                    S.RAW_DATA:order_status::STRING
                )
            ) IN
            (
                'PENDING',
                'PROCESSING',
                'SHIPPED',
                'IN TRANSIT',
                'DELIVERED',
                'CANCELLED'
            )


            -- ------------------------------------------------
            -- CUSTOMER VALIDATION
            -- ------------------------------------------------

            AND S.RAW_DATA:customer.customer_id::STRING IS NOT NULL


            -- ------------------------------------------------
            -- SUPPLIER VALIDATION
            -- ------------------------------------------------

            AND S.RAW_DATA:supplier.supplier_id::STRING IS NOT NULL

            AND S.RAW_DATA:supplier.performance_score::FLOAT
                BETWEEN 0 AND 100

            AND
            (
                S.RAW_DATA:supplier.lead_time_days::NUMBER IS NULL

                OR
                S.RAW_DATA:supplier.lead_time_days::NUMBER >= 0
            )


            -- ------------------------------------------------
            -- PRODUCT VALIDATION
            -- ------------------------------------------------

            AND S.RAW_DATA:items[0].product_id::STRING IS NOT NULL

            AND S.RAW_DATA:items[0].quantity::NUMBER IS NOT NULL

            AND S.RAW_DATA:items[0].quantity::NUMBER > 0

            AND S.RAW_DATA:items[0].unit_price::FLOAT IS NOT NULL

            AND S.RAW_DATA:items[0].unit_price::FLOAT >= 0


            -- ------------------------------------------------
            -- FINANCIAL VALIDATION
            -- ------------------------------------------------

            AND S.RAW_DATA:financials.total_amount::FLOAT IS NOT NULL

            AND S.RAW_DATA:financials.total_amount::FLOAT >= 0


            -- ------------------------------------------------
            -- WAREHOUSE VALIDATION
            -- ------------------------------------------------

            AND
            (
                S.RAW_DATA:warehouse.inventory_level::NUMBER IS NULL

                OR
                S.RAW_DATA:warehouse.inventory_level::NUMBER >= 0
            )

    ) AS SRC

    ON TGT.ORDER_ID = SRC.ORDER_ID


    -- ========================================================
    -- STEP 4
    -- Update existing records
    -- ========================================================

    WHEN MATCHED THEN

        UPDATE SET

            TGT.ORDER_STATUS =
                SRC.ORDER_STATUS,

            TGT.CARRIER =
                SRC.CARRIER,

            TGT.SHIP_DATE =
                SRC.SHIP_DATE,

            TGT.ESTIMATED_DELIVERY =
                SRC.ESTIMATED_DELIVERY,

            TGT.DELAY_DAYS =
                SRC.DELAY_DAYS,

            TGT.INVENTORY_LEVEL =
                SRC.INVENTORY_LEVEL,

            TGT.PAYMENT_STATUS =
                SRC.PAYMENT_STATUS,

            TGT.CUSTOMER_NAME =
                SRC.CUSTOMER_NAME,

            TGT.CUSTOMER_REGION =
                SRC.CUSTOMER_REGION,

            TGT.SUPPLIER_NAME =
                SRC.SUPPLIER_NAME,

            TGT.PERFORMANCE_SCORE =
                SRC.PERFORMANCE_SCORE,

            TGT.WAREHOUSE_ID =
                SRC.WAREHOUSE_ID,

            TGT.WAREHOUSE_LOCATION =
                SRC.WAREHOUSE_LOCATION,

            TGT.TRANSFORMED_AT =
                CURRENT_TIMESTAMP()


    -- ========================================================
    -- STEP 5
    -- Insert new records
    -- ========================================================

    WHEN NOT MATCHED THEN

        INSERT
        (
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

        VALUES
        (
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


    -- ========================================================
    -- STEP 6
    -- Cleanup
    -- ========================================================

    DROP TABLE IF EXISTS STREAM_BUFFER;


    -- ========================================================
    -- STEP 7
    -- Return status
    -- ========================================================

    RETURN 'sp_bronze_to_silver completed successfully';

END;
$$;


-- ============================================================
-- 11. VERIFY PROCEDURE
-- ============================================================

SHOW PROCEDURES LIKE 'SP_BRONZE_TO_SILVER';


-- ============================================================
-- 12. CREATE BRONZE -> SILVER TASK
-- ============================================================

CREATE OR REPLACE TASK SILVER_SCH.BRONZE_TO_SILVER_TASK
    WAREHOUSE = FLOWBRIDGE_PIPELINE_WH
    SCHEDULE = '1 MINUTE'
    COMMENT = 'Calls SP_BRONZE_TO_SILVER every minute when raw orders stream has data'
    WHEN SYSTEM$STREAM_HAS_DATA(
        'SILVER_SCH.RAW_ORDERS_STREAM'
    )
AS
    CALL SILVER_SCH.SP_BRONZE_TO_SILVER();


-- ============================================================
-- 13. RESUME TASK
-- ============================================================

ALTER TASK SILVER_SCH.BRONZE_TO_SILVER_TASK
RESUME;


-- ============================================================
-- 14. VERIFY TASK
-- ============================================================

SHOW TASKS;


-- ============================================================
-- 15. MANUAL TEST
-- ============================================================

CALL SILVER_SCH.SP_BRONZE_TO_SILVER();


-- ============================================================
-- 16. VERIFY SILVER DATA
-- ============================================================

SELECT *
FROM SILVER_SCH.STG_ORDERS
ORDER BY TRANSFORMED_AT DESC;


-- ============================================================
-- 17. VERIFY REJECTED RECORDS
-- ============================================================

SELECT *
FROM SILVER_SCH.DEAD_LETTER
ORDER BY REJECTED_AT DESC;

select count(*) as stg_orders_count from silver_sch.stg_orders
union all
select count(*) as stg_orders_count from silver_sch.dead_letter;

SELECT
    (SELECT COUNT(*) FROM SILVER_SCH.STG_ORDERS) AS VALID_RECORDS,
    (SELECT COUNT(*) FROM SILVER_SCH.DEAD_LETTER) AS REJECTED_RECORDS,
    (
        SELECT COUNT(*)
        FROM SILVER_SCH.STG_ORDERS
    )
    +
    (
        SELECT COUNT(*)
        FROM SILVER_SCH.DEAD_LETTER
    ) AS TOTAL_RECORDS;




  select * from dead_letter;