-- ============================================================
-- FLOWBRIDGE PRODUCTION DEPLOYMENT
-- ============================================================

USE ROLE ACCOUNTADMIN;

-- ============================================================
-- 1. CLONE DEV → PROD
-- ============================================================

CREATE OR REPLACE SCHEMA FLOWBRIDGE_PROD_DB.BRONZE_SCH
CLONE FLOWBRIDGE_DEV_DB.BRONZE_SCH;

CREATE OR REPLACE SCHEMA FLOWBRIDGE_PROD_DB.SILVER_SCH
CLONE FLOWBRIDGE_DEV_DB.SILVER_SCH;

CREATE OR REPLACE SCHEMA FLOWBRIDGE_PROD_DB.GOLD_SCH
CLONE FLOWBRIDGE_DEV_DB.GOLD_SCH;

CREATE OR REPLACE SCHEMA FLOWBRIDGE_PROD_DB.SERVING_SCH
CLONE FLOWBRIDGE_DEV_DB.SERVING_SCH;


-- ============================================================
-- 2. TRANSFER SCHEMA OWNERSHIP
-- ============================================================

GRANT OWNERSHIP ON SCHEMA FLOWBRIDGE_PROD_DB.BRONZE_SCH
TO ROLE SYSADMIN COPY CURRENT GRANTS;

GRANT OWNERSHIP ON SCHEMA FLOWBRIDGE_PROD_DB.SILVER_SCH
TO ROLE SYSADMIN COPY CURRENT GRANTS;

GRANT OWNERSHIP ON SCHEMA FLOWBRIDGE_PROD_DB.GOLD_SCH
TO ROLE SYSADMIN COPY CURRENT GRANTS;

GRANT OWNERSHIP ON SCHEMA FLOWBRIDGE_PROD_DB.SERVING_SCH
TO ROLE SYSADMIN COPY CURRENT GRANTS;


-- ============================================================
-- 3. PRODUCTION ENVIRONMENT
-- ============================================================

USE ROLE SYSADMIN;

USE DATABASE FLOWBRIDGE_PROD_DB;

USE SCHEMA BRONZE_SCH;

USE WAREHOUSE FLOWBRIDGE_PIPELINE_WH;


-- ============================================================
-- 4. PRODUCTION EXTERNAL STAGE
-- ============================================================

CREATE OR REPLACE STAGE
    FLOWBRIDGE_PROD_DB.BRONZE_SCH.ADLS_RAW_STAGE_PROD

    URL =
        'azure://flowbrigeadls.blob.core.windows.net/supply-chain-raw-prod/'

    STORAGE_INTEGRATION =
        FLOWBRIDGE_ADLS_INTEGRATION

    FILE_FORMAT =
        FLOWBRIDGE_PROD_DB.BRONZE_SCH.JSON_FILE_FORMAT

    COMMENT =
        'External Stage - ADLS Gen2 Prod container';


-- ============================================================
-- 5. PRODUCTION SNOWPIPE
-- ============================================================

CREATE OR REPLACE PIPE
    FLOWBRIDGE_PROD_DB.BRONZE_SCH.SUPPLY_CHAIN_PIPE_PROD

    AUTO_INGEST = TRUE

    INTEGRATION =
        FLOW_BRIDGE_AZURE_NOTIFICATIONS_INT

    COMMENT =
        'Snowpipe - auto ingest JSON files from ADLS Gen2'

AS

COPY INTO FLOWBRIDGE_PROD_DB.BRONZE_SCH.RAW_ORDERS
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

    FROM
        @FLOWBRIDGE_PROD_DB.BRONZE_SCH.ADLS_RAW_STAGE_PROD
)

FILE_FORMAT =
(
    FORMAT_NAME =
        'FLOWBRIDGE_PROD_DB.BRONZE_SCH.JSON_FILE_FORMAT'
);


-- ============================================================
-- 6. PRODUCTION STREAMS
-- ============================================================

CREATE OR REPLACE STREAM
    FLOWBRIDGE_PROD_DB.SILVER_SCH.RAW_ORDERS_STREAM

ON TABLE
    FLOWBRIDGE_PROD_DB.BRONZE_SCH.RAW_ORDERS

APPEND_ONLY = TRUE

SHOW_INITIAL_ROWS = TRUE

COMMENT =
    'Stream on raw_orders - captures new data';


CREATE OR REPLACE STREAM
    FLOWBRIDGE_PROD_DB.SILVER_SCH.STG_ORDERS_STREAM

ON TABLE
    FLOWBRIDGE_PROD_DB.SILVER_SCH.STG_ORDERS

SHOW_INITIAL_ROWS = TRUE

COMMENT =
    'Stream on stg_orders';


-- ============================================================
-- 7. BRONZE → SILVER PROCEDURE
-- ============================================================

CREATE OR REPLACE PROCEDURE
    FLOWBRIDGE_PROD_DB.SILVER_SCH.SP_BRONZE_TO_SILVER()

RETURNS STRING

LANGUAGE SQL

COMMENT =
    'Routes invalid records to dead_letter and merges valid records into stg_orders'

AS

$$

BEGIN

    -- --------------------------------------------------------
    -- BUFFER STREAM
    -- --------------------------------------------------------

    CREATE OR REPLACE TEMPORARY TABLE
        STREAM_BUFFER
    AS

    SELECT *
    FROM FLOWBRIDGE_PROD_DB.SILVER_SCH.RAW_ORDERS_STREAM

    WHERE METADATA$ACTION = 'INSERT';


    -- --------------------------------------------------------
    -- DEAD LETTER
    -- --------------------------------------------------------

    INSERT INTO FLOWBRIDGE_PROD_DB.SILVER_SCH.DEAD_LETTER
    (
        RAW_DATA,
        ERROR_REASON,
        FILE_NAME,
        FILE_ROW_NUMBER
    )

    SELECT

        S.RAW_DATA,

        CASE

            WHEN S.RAW_DATA:order_id::STRING IS NULL
                THEN 'Missing order_id'

            WHEN TRY_TO_TIMESTAMP_NTZ(
                    S.RAW_DATA:order_date::STRING
                 ) IS NULL

                 AND S.RAW_DATA:order_date::STRING
                     NOT LIKE '__-__-____'

                THEN 'Invalid or missing order_date'

            WHEN UPPER(
                    TRIM(
                        S.RAW_DATA:order_status::STRING
                    )
                 )

                 NOT IN
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

            WHEN S.RAW_DATA:customer.customer_id::STRING IS NULL
                THEN 'Missing customer_id'

            WHEN S.RAW_DATA:supplier.supplier_id::STRING IS NULL
                THEN 'Missing supplier_id'

            WHEN S.RAW_DATA:supplier.performance_score::FLOAT > 100
                THEN 'Invalid performance_score > 100'

            WHEN S.RAW_DATA:supplier.performance_score::FLOAT < 0
                THEN 'Invalid performance_score < 0'

            WHEN S.RAW_DATA:supplier.lead_time_days::NUMBER < 0
                THEN 'Invalid lead_time_days < 0'

            WHEN S.RAW_DATA:items[0].product_id::STRING IS NULL
                THEN 'Missing product_id'

            WHEN S.RAW_DATA:items[0].quantity::NUMBER IS NULL
                 OR S.RAW_DATA:items[0].quantity::NUMBER <= 0

                THEN 'Invalid quantity'

            WHEN S.RAW_DATA:items[0].unit_price::FLOAT IS NULL
                 OR S.RAW_DATA:items[0].unit_price::FLOAT < 0

                THEN 'Invalid unit_price'

            WHEN S.RAW_DATA:financials.total_amount::FLOAT IS NULL
                 OR S.RAW_DATA:financials.total_amount::FLOAT < 0

                THEN 'Invalid total_amount'

            WHEN S.RAW_DATA:warehouse.inventory_level::NUMBER < 0
                THEN 'Invalid inventory_level'

            ELSE 'Unknown'

        END AS ERROR_REASON,

        S.FILE_NAME,

        S.FILE_ROW_NUMBER

    FROM STREAM_BUFFER S

    WHERE

        S.RAW_DATA:order_id::STRING IS NULL

        OR

        (
            TRY_TO_TIMESTAMP_NTZ(
                S.RAW_DATA:order_date::STRING
            ) IS NULL

            AND S.RAW_DATA:order_date::STRING
                NOT LIKE '__-__-____'
        )

        OR

        UPPER(
            TRIM(
                S.RAW_DATA:order_status::STRING
            )
        )

        NOT IN
        (
            'PENDING',
            'PROCESSING',
            'SHIPPED',
            'IN TRANSIT',
            'DELIVERED',
            'CANCELLED'
        )

        OR S.RAW_DATA:customer.customer_id::STRING IS NULL

        OR S.RAW_DATA:supplier.supplier_id::STRING IS NULL

        OR S.RAW_DATA:supplier.performance_score::FLOAT NOT BETWEEN 0 AND 100

        OR S.RAW_DATA:supplier.lead_time_days::NUMBER < 0

        OR S.RAW_DATA:items[0].product_id::STRING IS NULL

        OR S.RAW_DATA:items[0].quantity::NUMBER IS NULL

        OR S.RAW_DATA:items[0].quantity::NUMBER <= 0

        OR S.RAW_DATA:items[0].unit_price::FLOAT IS NULL

        OR S.RAW_DATA:items[0].unit_price::FLOAT < 0

        OR S.RAW_DATA:financials.total_amount::FLOAT IS NULL

        OR S.RAW_DATA:financials.total_amount::FLOAT < 0

        OR S.RAW_DATA:warehouse.inventory_level::NUMBER < 0;


    -- --------------------------------------------------------
    -- VALID RECORDS → STG_ORDERS
    -- --------------------------------------------------------

    MERGE INTO
        FLOWBRIDGE_PROD_DB.SILVER_SCH.STG_ORDERS TGT

    USING
    (

        SELECT

            UPPER(TRIM(
                S.RAW_DATA:order_id::STRING
            )) AS ORDER_ID,

            TRY_TO_TIMESTAMP_NTZ(

                CASE

                    WHEN S.RAW_DATA:order_date::STRING
                         LIKE '__-__-____'

                    THEN
                        TO_VARCHAR(
                            TO_DATE(
                                S.RAW_DATA:order_date::STRING,
                                'DD-MM-YYYY'
                            ),
                            'YYYY-MM-DD'
                        ) || 'T00:00:00'

                    ELSE
                        S.RAW_DATA:order_date::STRING

                END

            ) AS ORDER_DATE,

            UPPER(TRIM(
                S.RAW_DATA:order_status::STRING
            )) AS ORDER_STATUS,

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

            COALESCE(
                S.RAW_DATA:supplier.lead_time_days::NUMBER,
                0
            ) AS LEAD_TIME_DAYS,

            COALESCE(
                S.RAW_DATA:supplier.performance_score::FLOAT,
                0
            ) AS PERFORMANCE_SCORE,

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

            TRY_TO_TIMESTAMP_NTZ(
                S.RAW_DATA:shipment.estimated_delivery::STRING
            ) AS ESTIMATED_DELIVERY,

            GREATEST(
                COALESCE(
                    S.RAW_DATA:shipment.delay_days::NUMBER,
                    0
                ),
                0
            ) AS DELAY_DAYS,

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

            S.RAW_DATA:items[0].quantity::NUMBER AS QUANTITY,

            S.RAW_DATA:items[0].unit_price::FLOAT AS UNIT_PRICE,

            S.RAW_DATA:financials.total_amount::FLOAT AS TOTAL_AMOUNT,

            COALESCE(
                UPPER(TRIM(
                    S.RAW_DATA:financials.payment_status::STRING
                )),
                'UNKNOWN'
            ) AS PAYMENT_STATUS,

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

            COALESCE(
                S.RAW_DATA:warehouse.inventory_level::NUMBER,
                0
            ) AS INVENTORY_LEVEL,

            S.FILE_NAME,

            S.FILE_ROW_NUMBER,

            S.INGESTED_AT

        FROM STREAM_BUFFER S

        WHERE
            S.RAW_DATA:order_id::STRING IS NOT NULL

            AND
            (
                TRY_TO_TIMESTAMP_NTZ(
                    S.RAW_DATA:order_date::STRING
                ) IS NOT NULL

                OR
                S.RAW_DATA:order_date::STRING
                    LIKE '__-__-____'
            )

            AND UPPER(TRIM(
                S.RAW_DATA:order_status::STRING
            ))
            IN
            (
                'PENDING',
                'PROCESSING',
                'SHIPPED',
                'IN TRANSIT',
                'DELIVERED',
                'CANCELLED'
            )

            AND S.RAW_DATA:customer.customer_id::STRING IS NOT NULL

            AND S.RAW_DATA:supplier.supplier_id::STRING IS NOT NULL

            AND S.RAW_DATA:supplier.performance_score::FLOAT
                BETWEEN 0 AND 100

            AND
            (
                S.RAW_DATA:supplier.lead_time_days::NUMBER IS NULL
                OR
                S.RAW_DATA:supplier.lead_time_days::NUMBER >= 0
            )

            AND S.RAW_DATA:items[0].product_id::STRING IS NOT NULL

            AND S.RAW_DATA:items[0].quantity::NUMBER > 0

            AND S.RAW_DATA:items[0].unit_price::FLOAT >= 0

            AND S.RAW_DATA:financials.total_amount::FLOAT >= 0

            AND
            (
                S.RAW_DATA:warehouse.inventory_level::NUMBER IS NULL
                OR
                S.RAW_DATA:warehouse.inventory_level::NUMBER >= 0
            )

    ) SRC

    ON TGT.ORDER_ID = SRC.ORDER_ID

    WHEN MATCHED THEN UPDATE SET

        TGT.ORDER_STATUS       = SRC.ORDER_STATUS,
        TGT.CARRIER            = SRC.CARRIER,
        TGT.SHIP_DATE          = SRC.SHIP_DATE,
        TGT.ESTIMATED_DELIVERY = SRC.ESTIMATED_DELIVERY,
        TGT.DELAY_DAYS         = SRC.DELAY_DAYS,
        TGT.INVENTORY_LEVEL    = SRC.INVENTORY_LEVEL,
        TGT.PAYMENT_STATUS     = SRC.PAYMENT_STATUS,
        TGT.CUSTOMER_NAME      = SRC.CUSTOMER_NAME,
        TGT.CUSTOMER_REGION    = SRC.CUSTOMER_REGION,
        TGT.SUPPLIER_NAME      = SRC.SUPPLIER_NAME,
        TGT.PERFORMANCE_SCORE  = SRC.PERFORMANCE_SCORE,
        TGT.WAREHOUSE_ID       = SRC.WAREHOUSE_ID,
        TGT.WAREHOUSE_LOCATION = SRC.WAREHOUSE_LOCATION,
        TGT.TRANSFORMED_AT     = CURRENT_TIMESTAMP()

    WHEN NOT MATCHED THEN INSERT
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


    DROP TABLE IF EXISTS STREAM_BUFFER;

    RETURN 'SP_BRONZE_TO_SILVER completed successfully';

END;

$$;


-- ============================================================
-- 8. SILVER → STAR PROCEDURE
-- ============================================================

CREATE OR REPLACE PROCEDURE
    FLOWBRIDGE_PROD_DB.SILVER_SCH.SILVER_TO_STAR()

RETURNS STRING

LANGUAGE SQL

AS

$$

BEGIN

    CREATE OR REPLACE TEMPORARY TABLE
        TMP_STREAM_DATA
    AS

    SELECT *
    FROM FLOWBRIDGE_PROD_DB.SILVER_SCH.STG_ORDERS_STREAM

    WHERE METADATA$ACTION = 'INSERT';


    -- --------------------------------------------------------
    -- CUSTOMER
    -- --------------------------------------------------------

    MERGE INTO
        FLOWBRIDGE_PROD_DB.SILVER_SCH.DIM_CUSTOMER TGT

    USING
    (
        SELECT
            UPPER(CUSTOMER_ID) AS CUSTOMER_ID,
            UPPER(CUSTOMER_NAME) AS CUSTOMER_NAME,
            UPPER(CUSTOMER_REGION) AS CUSTOMER_REGION,
            UPPER(CUSTOMER_SEGMENT) AS CUSTOMER_SEGMENT

        FROM TMP_STREAM_DATA

        QUALIFY ROW_NUMBER() OVER
        (
            PARTITION BY UPPER(CUSTOMER_ID)
            ORDER BY ORDER_DATE DESC
        ) = 1

    ) SRC

    ON TGT.CUSTOMER_ID = SRC.CUSTOMER_ID

    WHEN MATCHED THEN UPDATE SET
        TGT.CUSTOMER_NAME = SRC.CUSTOMER_NAME,
        TGT.CUSTOMER_REGION = SRC.CUSTOMER_REGION,
        TGT.CUSTOMER_SEGMENT = SRC.CUSTOMER_SEGMENT,
        TGT.IS_ACTIVE = TRUE,
        TGT.UPDATED_AT = CURRENT_TIMESTAMP()

    WHEN NOT MATCHED THEN INSERT
    (
        CUSTOMER_ID,
        CUSTOMER_NAME,
        CUSTOMER_REGION,
        CUSTOMER_SEGMENT
    )

    VALUES
    (
        SRC.CUSTOMER_ID,
        SRC.CUSTOMER_NAME,
        SRC.CUSTOMER_REGION,
        SRC.CUSTOMER_SEGMENT
    );


    -- --------------------------------------------------------
    -- PRODUCT
    -- --------------------------------------------------------

    MERGE INTO
        FLOWBRIDGE_PROD_DB.SILVER_SCH.DIM_PRODUCT TGT

    USING
    (
        SELECT
            UPPER(PRODUCT_ID) AS PRODUCT_ID,
            UPPER(PRODUCT_NAME) AS PRODUCT_NAME,
            UPPER(CATEGORY) AS CATEGORY,
            UNIT_PRICE

        FROM TMP_STREAM_DATA

        QUALIFY ROW_NUMBER() OVER
        (
            PARTITION BY UPPER(PRODUCT_ID)
            ORDER BY ORDER_DATE DESC
        ) = 1

    ) SRC

    ON TGT.PRODUCT_ID = SRC.PRODUCT_ID

    WHEN MATCHED THEN UPDATE SET
        TGT.PRODUCT_NAME = SRC.PRODUCT_NAME,
        TGT.CATEGORY = SRC.CATEGORY,
        TGT.UNIT_PRICE = SRC.UNIT_PRICE,
        TGT.IS_ACTIVE = TRUE,
        TGT.UPDATED_AT = CURRENT_TIMESTAMP()

    WHEN NOT MATCHED THEN INSERT
    (
        PRODUCT_ID,
        PRODUCT_NAME,
        CATEGORY,
        UNIT_PRICE
    )

    VALUES
    (
        SRC.PRODUCT_ID,
        SRC.PRODUCT_NAME,
        SRC.CATEGORY,
        SRC.UNIT_PRICE
    );


    -- --------------------------------------------------------
    -- SUPPLIER
    -- --------------------------------------------------------

    MERGE INTO
        FLOWBRIDGE_PROD_DB.SILVER_SCH.DIM_SUPPLIER TGT

    USING
    (
        SELECT
            UPPER(SUPPLIER_ID) AS SUPPLIER_ID,
            UPPER(SUPPLIER_NAME) AS SUPPLIER_NAME,
            UPPER(SUPPLIER_COUNTRY) AS SUPPLIER_COUNTRY,
            LEAD_TIME_DAYS,
            PERFORMANCE_SCORE

        FROM TMP_STREAM_DATA

        QUALIFY ROW_NUMBER() OVER
        (
            PARTITION BY UPPER(SUPPLIER_ID)
            ORDER BY ORDER_DATE DESC
        ) = 1

    ) SRC

    ON TGT.SUPPLIER_ID = SRC.SUPPLIER_ID

    WHEN MATCHED THEN UPDATE SET
        TGT.SUPPLIER_NAME = SRC.SUPPLIER_NAME,
        TGT.SUPPLIER_COUNTRY = SRC.SUPPLIER_COUNTRY,
        TGT.LEAD_TIME_DAYS = SRC.LEAD_TIME_DAYS,
        TGT.PERFORMANCE_SCORE = SRC.PERFORMANCE_SCORE,
        TGT.IS_ACTIVE = TRUE,
        TGT.UPDATED_AT = CURRENT_TIMESTAMP()

    WHEN NOT MATCHED THEN INSERT
    (
        SUPPLIER_ID,
        SUPPLIER_NAME,
        SUPPLIER_COUNTRY,
        LEAD_TIME_DAYS,
        PERFORMANCE_SCORE
    )

    VALUES
    (
        SRC.SUPPLIER_ID,
        SRC.SUPPLIER_NAME,
        SRC.SUPPLIER_COUNTRY,
        SRC.LEAD_TIME_DAYS,
        SRC.PERFORMANCE_SCORE
    );


    -- --------------------------------------------------------
    -- WAREHOUSE
    -- --------------------------------------------------------

    MERGE INTO
        FLOWBRIDGE_PROD_DB.SILVER_SCH.DIM_WAREHOUSE TGT

    USING
    (
        SELECT
            UPPER(WAREHOUSE_ID) AS WAREHOUSE_ID,
            UPPER(WAREHOUSE_LOCATION) AS WAREHOUSE_LOCATION

        FROM TMP_STREAM_DATA

        QUALIFY ROW_NUMBER() OVER
        (
            PARTITION BY UPPER(WAREHOUSE_ID)
            ORDER BY ORDER_DATE DESC
        ) = 1

    ) SRC

    ON TGT.WAREHOUSE_ID = SRC.WAREHOUSE_ID

    WHEN MATCHED THEN UPDATE SET
        TGT.WAREHOUSE_LOCATION = SRC.WAREHOUSE_LOCATION,
        TGT.IS_ACTIVE = TRUE,
        TGT.UPDATED_AT = CURRENT_TIMESTAMP()

    WHEN NOT MATCHED THEN INSERT
    (
        WAREHOUSE_ID,
        WAREHOUSE_LOCATION
    )

    VALUES
    (
        SRC.WAREHOUSE_ID,
        SRC.WAREHOUSE_LOCATION
    );


    -- --------------------------------------------------------
    -- SHIPMENT
    -- --------------------------------------------------------

    MERGE INTO
        FLOWBRIDGE_PROD_DB.SILVER_SCH.DIM_SHIPMENT TGT

    USING
    (
        SELECT
            UPPER(SHIPMENT_ID) AS SHIPMENT_ID,
            UPPER(CARRIER) AS CARRIER,
            SHIP_DATE,
            ESTIMATED_DELIVERY

        FROM TMP_STREAM_DATA

        WHERE UPPER(SHIPMENT_ID) != 'UNKNOWN'

        QUALIFY ROW_NUMBER() OVER
        (
            PARTITION BY UPPER(SHIPMENT_ID)
            ORDER BY ORDER_DATE DESC
        ) = 1

    ) SRC

    ON TGT.SHIPMENT_ID = SRC.SHIPMENT_ID

    WHEN MATCHED THEN UPDATE SET
        TGT.CARRIER = SRC.CARRIER,
        TGT.SHIP_DATE = SRC.SHIP_DATE,
        TGT.ESTIMATED_DELIVERY = SRC.ESTIMATED_DELIVERY,
        TGT.IS_ACTIVE = TRUE,
        TGT.UPDATED_AT = CURRENT_TIMESTAMP()

    WHEN NOT MATCHED THEN INSERT
    (
        SHIPMENT_ID,
        CARRIER,
        SHIP_DATE,
        ESTIMATED_DELIVERY
    )

    VALUES
    (
        SRC.SHIPMENT_ID,
        SRC.CARRIER,
        SRC.SHIP_DATE,
        SRC.ESTIMATED_DELIVERY
    );


    -- --------------------------------------------------------
    -- FACT ORDERS
    -- IMPORTANT: WAREHOUSE_SK
    -- --------------------------------------------------------

    MERGE INTO
        FLOWBRIDGE_PROD_DB.SILVER_SCH.FACT_ORDERS TGT

    USING
    (
        SELECT

            UPPER(S.ORDER_ID) AS ORDER_ID,

            DC.CUSTOMER_SK,

            DP.PRODUCT_SK,

            DS.SUPPLIER_SK,

            DW.WAREHOUSE_SK,

            COALESCE(
                DSH.SHIPMENT_SK,
                -1
            ) AS SHIPMENT_SK,

            S.ORDER_DATE,

            UPPER(S.ORDER_STATUS) AS ORDER_STATUS,

            UPPER(S.PAYMENT_STATUS) AS PAYMENT_STATUS,

            S.QUANTITY,

            S.UNIT_PRICE,

            S.TOTAL_AMOUNT,

            S.DELAY_DAYS,

            S.INVENTORY_LEVEL,

            S.INGESTED_AT

        FROM TMP_STREAM_DATA S

        INNER JOIN
            FLOWBRIDGE_PROD_DB.SILVER_SCH.DIM_CUSTOMER DC
            ON DC.CUSTOMER_ID = UPPER(S.CUSTOMER_ID)

        INNER JOIN
            FLOWBRIDGE_PROD_DB.SILVER_SCH.DIM_PRODUCT DP
            ON DP.PRODUCT_ID = UPPER(S.PRODUCT_ID)

        INNER JOIN
            FLOWBRIDGE_PROD_DB.SILVER_SCH.DIM_SUPPLIER DS
            ON DS.SUPPLIER_ID = UPPER(S.SUPPLIER_ID)

        INNER JOIN
            FLOWBRIDGE_PROD_DB.SILVER_SCH.DIM_WAREHOUSE DW
            ON DW.WAREHOUSE_ID = UPPER(S.WAREHOUSE_ID)

        LEFT JOIN
            FLOWBRIDGE_PROD_DB.SILVER_SCH.DIM_SHIPMENT DSH
            ON DSH.SHIPMENT_ID = UPPER(S.SHIPMENT_ID)

        QUALIFY ROW_NUMBER() OVER
        (
            PARTITION BY UPPER(S.ORDER_ID)
            ORDER BY S.ORDER_DATE DESC
        ) = 1

    ) SRC

    ON TGT.ORDER_ID = SRC.ORDER_ID

    WHEN MATCHED THEN UPDATE SET

        TGT.CUSTOMER_SK = SRC.CUSTOMER_SK,
        TGT.PRODUCT_SK = SRC.PRODUCT_SK,
        TGT.SUPPLIER_SK = SRC.SUPPLIER_SK,
        TGT.WAREHOUSE_SK = SRC.WAREHOUSE_SK,
        TGT.SHIPMENT_SK = SRC.SHIPMENT_SK,
        TGT.ORDER_DATE = SRC.ORDER_DATE,
        TGT.ORDER_STATUS = SRC.ORDER_STATUS,
        TGT.PAYMENT_STATUS = SRC.PAYMENT_STATUS,
        TGT.QUANTITY = SRC.QUANTITY,
        TGT.UNIT_PRICE = SRC.UNIT_PRICE,
        TGT.TOTAL_AMOUNT = SRC.TOTAL_AMOUNT,
        TGT.DELAY_DAYS = SRC.DELAY_DAYS,
        TGT.INVENTORY_LEVEL = SRC.INVENTORY_LEVEL,
        TGT.INGESTED_AT = SRC.INGESTED_AT,
        TGT.UPDATED_AT = CURRENT_TIMESTAMP()

    WHEN NOT MATCHED THEN INSERT
    (
        ORDER_ID,
        CUSTOMER_SK,
        PRODUCT_SK,
        SUPPLIER_SK,
        WAREHOUSE_SK,
        SHIPMENT_SK,
        ORDER_DATE,
        ORDER_STATUS,
        PAYMENT_STATUS,
        QUANTITY,
        UNIT_PRICE,
        TOTAL_AMOUNT,
        DELAY_DAYS,
        INVENTORY_LEVEL,
        INGESTED_AT
    )

    VALUES
    (
        SRC.ORDER_ID,
        SRC.CUSTOMER_SK,
        SRC.PRODUCT_SK,
        SRC.SUPPLIER_SK,
        SRC.WAREHOUSE_SK,
        SRC.SHIPMENT_SK,
        SRC.ORDER_DATE,
        SRC.ORDER_STATUS,
        SRC.PAYMENT_STATUS,
        SRC.QUANTITY,
        SRC.UNIT_PRICE,
        SRC.TOTAL_AMOUNT,
        SRC.DELAY_DAYS,
        SRC.INVENTORY_LEVEL,
        SRC.INGESTED_AT
    );


    DROP TABLE IF EXISTS TMP_STREAM_DATA;

    RETURN 'SILVER_TO_STAR completed successfully';

END;

$$;


-- ============================================================
-- 9. PIPELINE TASKS
-- ============================================================

CREATE OR REPLACE TASK
    FLOWBRIDGE_PROD_DB.SILVER_SCH.BRONZE_TO_SILVER_TASK

    WAREHOUSE = FLOWBRIDGE_PIPELINE_WH

    SCHEDULE = '1 minute'

WHEN SYSTEM$STREAM_HAS_DATA(
    'FLOWBRIDGE_PROD_DB.SILVER_SCH.RAW_ORDERS_STREAM'
)

AS

CALL FLOWBRIDGE_PROD_DB.SILVER_SCH.SP_BRONZE_TO_SILVER();


CREATE OR REPLACE TASK
    FLOWBRIDGE_PROD_DB.SILVER_SCH.SILVER_TO_STAR_TASK

    WAREHOUSE = FLOWBRIDGE_PIPELINE_WH

    SCHEDULE = '1 minute'

WHEN SYSTEM$STREAM_HAS_DATA(
    'FLOWBRIDGE_PROD_DB.SILVER_SCH.STG_ORDERS_STREAM'
)

AS

CALL FLOWBRIDGE_PROD_DB.SILVER_SCH.SILVER_TO_STAR();


-- ============================================================
-- 10. PIPELINE HEALTH ALERT
-- ============================================================

CREATE OR REPLACE ALERT
    FLOWBRIDGE_PROD_DB.BRONZE_SCH.PIPELINE_HEALTH_ALERT

    WAREHOUSE = FLOWBRIDGE_PIPELINE_WH

    SCHEDULE = '5 minute'

IF
(
    EXISTS
    (
        SELECT 1
        FROM TABLE
        (
            INFORMATION_SCHEMA.COPY_HISTORY
            (
                TABLE_NAME = 'RAW_ORDERS',
                START_TIME = DATEADD(
                    HOUR,
                    -1,
                    CURRENT_TIMESTAMP()
                )
            )
        )
        WHERE STATUS = 'Load Failed'

        UNION ALL

        SELECT 1
        FROM TABLE
        (
            INFORMATION_SCHEMA.TASK_HISTORY
            (
                SCHEDULED_TIME_RANGE_START =
                    DATEADD(
                        HOUR,
                        -1,
                        CURRENT_TIMESTAMP()
                    )
            )
        )
        WHERE STATE = 'FAILED'
          AND DATABASE_NAME = 'FLOWBRIDGE_PROD_DB'

        UNION ALL

        SELECT 1
        FROM TABLE
        (
            INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY()
        )
        WHERE SCHEMA_NAME = 'GOLD_SCH'
          AND DATABASE_NAME = 'FLOWBRIDGE_PROD_DB'
          AND STATE = 'FAILED'
          AND REFRESH_START_TIME >
              DATEADD(
                  MINUTE,
                  -5,
                  CURRENT_TIMESTAMP()
              )
    )
)

THEN

CALL SYSTEM$SEND_EMAIL
(
    'email_notification_int',
    'limbaga776@gmail.com',
    'Pipeline Alert - Flowbridge Project',
    'Pipeline failure detected. Check Snowflake monitoring history.'
);

ALTER ALERT
    FLOWBRIDGE_PROD_DB.BRONZE_SCH.PIPELINE_HEALTH_ALERT
RESUME;


-- ============================================================
-- 11. DATA SHARE
-- ============================================================

CREATE OR REPLACE SHARE
    FLOWBRIDGE_PROD_SHARE

COMMENT =
    'Flowbridge production serving data';


GRANT USAGE
ON DATABASE FLOWBRIDGE_PROD_DB
TO SHARE FLOWBRIDGE_PROD_SHARE;


GRANT USAGE
ON SCHEMA FLOWBRIDGE_PROD_DB.SERVING_SCH
TO SHARE FLOWBRIDGE_PROD_SHARE;


GRANT SELECT
ON VIEW FLOWBRIDGE_PROD_DB.SERVING_SCH.VW_ORDER_FULFILLMENT
TO SHARE FLOWBRIDGE_PROD_SHARE;


GRANT SELECT
ON VIEW FLOWBRIDGE_PROD_DB.SERVING_SCH.VW_SUPPLIER_PERFORMANCE
TO SHARE FLOWBRIDGE_PROD_SHARE;


GRANT SELECT
ON VIEW FLOWBRIDGE_PROD_DB.SERVING_SCH.VW_INVENTORY_TURNOVER
TO SHARE FLOWBRIDGE_PROD_SHARE;


GRANT SELECT
ON VIEW FLOWBRIDGE_PROD_DB.SERVING_SCH.VW_SHIPMENT_DELAYS
TO SHARE FLOWBRIDGE_PROD_SHARE;


-- ============================================================
-- 12. LOGISTICS PARTNER READER ACCOUNT
-- ============================================================

SET logistics_admin_name =
    'logistics_admin';

SET logistics_admin_password =
    'FlowbridgeLogistics2026!Secure';


CREATE MANAGED ACCOUNT IF NOT EXISTS
    FLOWBRIDGE_LOGISTICS_PARTNER

    ADMIN_NAME = $logistics_admin_name

    ADMIN_PASSWORD = $logistics_admin_password

    TYPE = READER

    COMMENT =
        'Reader Account for Flowbridge Logistics Partner - PROD KPI access';


-- ============================================================
-- 13. ADD LOGISTICS PARTNER TO SHARE
-- ============================================================

ALTER SHARE FLOWBRIDGE_PROD_SHARE
ADD ACCOUNTS = EE63260;


-- ============================================================
-- 14. INITIAL PIPELINE LOAD
-- ============================================================

GRANT OWNERSHIP
ON STREAM FLOWBRIDGE_PROD_DB.SILVER_SCH.RAW_ORDERS_STREAM
TO ROLE SYSADMIN
COPY CURRENT GRANTS;


CALL FLOWBRIDGE_PROD_DB.SILVER_SCH.SP_BRONZE_TO_SILVER();

CALL FLOWBRIDGE_PROD_DB.SILVER_SCH.SILVER_TO_STAR();


-- ============================================================
-- 15. RESUME PIPELINE
-- ============================================================

ALTER TASK
    FLOWBRIDGE_PROD_DB.SILVER_SCH.BRONZE_TO_SILVER_TASK
RESUME;


ALTER TASK
    FLOWBRIDGE_PROD_DB.SILVER_SCH.SILVER_TO_STAR_TASK
RESUME;


-- ============================================================
-- 16. RESUME GOLD DYNAMIC TABLES
-- ============================================================

ALTER DYNAMIC TABLE
    FLOWBRIDGE_PROD_DB.GOLD_SCH.AGG_BASE
RESUME;

ALTER DYNAMIC TABLE
    FLOWBRIDGE_PROD_DB.GOLD_SCH.AGG_INVENTORY_TURNOVER
RESUME;

ALTER DYNAMIC TABLE
    FLOWBRIDGE_PROD_DB.GOLD_SCH.AGG_ORDER_FULFILLMENT
RESUME;

ALTER DYNAMIC TABLE
    FLOWBRIDGE_PROD_DB.GOLD_SCH.AGG_SHIPMENT_DELAYS
RESUME;

ALTER DYNAMIC TABLE
    FLOWBRIDGE_PROD_DB.GOLD_SCH.AGG_SUPPLIER_PERFORMANCE
RESUME;


-- ============================================================
-- 17. FINAL VALIDATION
-- ============================================================

SELECT
    'RAW_ORDERS' AS TABLE_NAME,
    COUNT(*) AS ROW_COUNT
FROM FLOWBRIDGE_PROD_DB.BRONZE_SCH.RAW_ORDERS

UNION ALL

SELECT
    'STG_ORDERS',
    COUNT(*)
FROM FLOWBRIDGE_PROD_DB.SILVER_SCH.STG_ORDERS

UNION ALL

SELECT
    'DIM_CUSTOMER',
    COUNT(*)
FROM FLOWBRIDGE_PROD_DB.SILVER_SCH.DIM_CUSTOMER

UNION ALL

SELECT
    'DIM_PRODUCT',
    COUNT(*)
FROM FLOWBRIDGE_PROD_DB.SILVER_SCH.DIM_PRODUCT

UNION ALL

SELECT
    'DIM_SUPPLIER',
    COUNT(*)
FROM FLOWBRIDGE_PROD_DB.SILVER_SCH.DIM_SUPPLIER

UNION ALL

SELECT
    'DIM_WAREHOUSE',
    COUNT(*)
FROM FLOWBRIDGE_PROD_DB.SILVER_SCH.DIM_WAREHOUSE

UNION ALL

SELECT
    'DIM_SHIPMENT',
    COUNT(*)
FROM FLOWBRIDGE_PROD_DB.SILVER_SCH.DIM_SHIPMENT

UNION ALL

SELECT
    'FACT_ORDERS',
    COUNT(*)
FROM FLOWBRIDGE_PROD_DB.SILVER_SCH.FACT_ORDERS

UNION ALL

SELECT
    'DEAD_LETTER',
    COUNT(*)
FROM FLOWBRIDGE_PROD_DB.SILVER_SCH.DEAD_LETTER

UNION ALL

SELECT
    'AGG_BASE',
    COUNT(*)
FROM FLOWBRIDGE_PROD_DB.GOLD_SCH.AGG_BASE

UNION ALL

SELECT
    'AGG_INVENTORY_TURNOVER',
    COUNT(*)
FROM FLOWBRIDGE_PROD_DB.GOLD_SCH.AGG_INVENTORY_TURNOVER

UNION ALL

SELECT
    'AGG_ORDER_FULFILLMENT',
    COUNT(*)
FROM FLOWBRIDGE_PROD_DB.GOLD_SCH.AGG_ORDER_FULFILLMENT

UNION ALL

SELECT
    'AGG_SHIPMENT_DELAYS',
    COUNT(*)
FROM FLOWBRIDGE_PROD_DB.GOLD_SCH.AGG_SHIPMENT_DELAYS

UNION ALL

SELECT
    'AGG_SUPPLIER_PERFORMANCE',
    COUNT(*)
FROM FLOWBRIDGE_PROD_DB.GOLD_SCH.AGG_SUPPLIER_PERFORMANCE;


-- ============================================================
-- 18. FINAL OBJECT CHECKS
-- ============================================================

DESC TABLE
    FLOWBRIDGE_PROD_DB.SILVER_SCH.FACT_ORDERS;

SHOW MANAGED ACCOUNTS;

SHOW GRANTS ON SHARE FLOWBRIDGE_PROD_SHARE;

SHOW TASKS IN DATABASE FLOWBRIDGE_PROD_DB;

SHOW STREAMS IN DATABASE FLOWBRIDGE_PROD_DB;

SELECT SYSTEM$PIPE_STATUS(
    'FLOWBRIDGE_PROD_DB.BRONZE_SCH.SUPPLY_CHAIN_PIPE_PROD'
);

DESC TABLE FLOWBRIDGE_PROD_DB.SILVER_SCH.FACT_ORDERS;
ALTER TABLE FLOWBRIDGE_PROD_DB.SILVER_SCH.FACT_ORDERS
RENAME COLUMN WARHOUSE_SK TO WAREHOUSE_SK;


DESC TABLE FLOWBRIDGE_PROD_DB.SILVER_SCH.FACT_ORDERS;


CREATE OR REPLACE PROCEDURE
FLOWBRIDGE_PROD_DB.SILVER_SCH.SILVER_TO_STAR()
...






-- ==========================================
-- Step 1: Infrastructure Setup & Manual Load
-- ==========================================
-- Grant ownership of the stream to the SYSADMIN role
GRANT OWNERSHIP ON STREAM FLOWBRIDGE_PROD_DB.SILVER_SCH.RAW_ORDERS_STREAM 
  TO ROLE SYSADMIN COPY CURRENT GRANTS;

-- Manually call stored procedures to populate the tables initially
CALL FLOWBRIDGE_PROD_DB.SILVER_SCH.SP_BRONZE_TO_SILVER();
CALL FLOWBRIDGE_PROD_DB.SILVER_SCH.SILVER_TO_STAR();


-- ==========================================
-- Step 2: Resume Tasks & Dynamic Tables
-- ==========================================
-- Resume tasks for ongoing automated processing
ALTER TASK FLOWBRIDGE_PROD_DB.SILVER_SCH.BRONZE_TO_SILVER_TASK RESUME;
ALTER TASK FLOWBRIDGE_PROD_DB.SILVER_SCH.SILVER_TO_STAR_TASK RESUME;

-- Resume dynamic tables in the Gold schema to restart automatic refreshing
ALTER DYNAMIC TABLE FLOWBRIDGE_PROD_DB.GOLD_SCH.AGG_BASE RESUME;
ALTER DYNAMIC TABLE FLOWBRIDGE_PROD_DB.GOLD_SCH.AGG_INVENTORY_TURNOVER RESUME;
ALTER DYNAMIC TABLE FLOWBRIDGE_PROD_DB.GOLD_SCH.AGG_ORDER_FULFILLMENT RESUME;
ALTER DYNAMIC TABLE FLOWBRIDGE_PROD_DB.GOLD_SCH.AGG_SHIPMENT_DELAYS RESUME;
ALTER DYNAMIC TABLE FLOWBRIDGE_PROD_DB.GOLD_SCH.AGG_SUPPLIER_PERFORMANCE RESUME;


-- ==========================================
-- Step 3: Verify Row Counts
-- ==========================================
-- Consolidated query to check current data volume across all layers
SELECT 'RAW_ORDERS' AS TBL, COUNT(*) AS CNT FROM FLOWBRIDGE_PROD_DB.BRONZE_SCH.RAW_ORDERS
UNION ALL SELECT 'STG_ORDERS', COUNT(*) FROM FLOWBRIDGE_PROD_DB.SILVER_SCH.STG_ORDERS
UNION ALL SELECT 'DIM_CUSTOMER', COUNT(*) FROM FLOWBRIDGE_PROD_DB.SILVER_SCH.DIM_CUSTOMER
UNION ALL SELECT 'DIM_PRODUCT', COUNT(*) FROM FLOWBRIDGE_PROD_DB.SILVER_SCH.DIM_PRODUCT
UNION ALL SELECT 'DIM_SHIPMENT', COUNT(*) FROM FLOWBRIDGE_PROD_DB.SILVER_SCH.DIM_SHIPMENT
UNION ALL SELECT 'DIM_SUPPLIER', COUNT(*) FROM FLOWBRIDGE_PROD_DB.SILVER_SCH.DIM_SUPPLIER
UNION ALL SELECT 'DIM_WAREHOUSE', COUNT(*) FROM FLOWBRIDGE_PROD_DB.SILVER_SCH.DIM_WAREHOUSE
UNION ALL SELECT 'FACT_ORDERS', COUNT(*) FROM FLOWBRIDGE_PROD_DB.SILVER_SCH.FACT_ORDERS
UNION ALL SELECT 'DEAD_LETTER', COUNT(*) FROM FLOWBRIDGE_PROD_DB.SILVER_SCH.DEAD_LETTER
UNION ALL SELECT 'AGG_BASE', COUNT(*) FROM FLOWBRIDGE_PROD_DB.GOLD_SCH.AGG_BASE
UNION ALL SELECT 'AGG_INVENTORY_TURNOVER', COUNT(*) FROM FLOWBRIDGE_PROD_DB.GOLD_SCH.AGG_INVENTORY_TURNOVER
UNION ALL SELECT 'AGG_ORDER_FULFILLMENT', COUNT(*) FROM FLOWBRIDGE_PROD_DB.GOLD_SCH.AGG_ORDER_FULFILLMENT
UNION ALL SELECT 'AGG_SHIPMENT_DELAYS', COUNT(*) FROM FLOWBRIDGE_PROD_DB.GOLD_SCH.AGG_SHIPMENT_DELAYS
UNION ALL SELECT 'AGG_SUPPLIER_PERFORMANCE', COUNT(*) FROM FLOWBRIDGE_PROD_DB.GOLD_SCH.AGG_SUPPLIER_PERFORMANCE;


select count(*) from serving_sch.vw_inventory_turnover;


CREATE OR REPLACE PROCEDURE
    FLOWBRIDGE_PROD_DB.SILVER_SCH.SILVER_TO_STAR()

RETURNS STRING

LANGUAGE SQL

AS
$$

BEGIN

    CREATE OR REPLACE TEMPORARY TABLE
        FLOWBRIDGE_PROD_DB.SILVER_SCH.TMP_STREAM_DATA
    AS
    SELECT *
    FROM FLOWBRIDGE_PROD_DB.SILVER_SCH.STG_ORDERS_STREAM
    WHERE METADATA$ACTION = 'INSERT';


    -- =========================================================
    -- DIM CUSTOMER
    -- =========================================================

    MERGE INTO FLOWBRIDGE_PROD_DB.SILVER_SCH.DIM_CUSTOMER TGT

    USING
    (
        SELECT
            UPPER(CUSTOMER_ID) AS CUSTOMER_ID,
            UPPER(CUSTOMER_NAME) AS CUSTOMER_NAME,
            UPPER(CUSTOMER_REGION) AS CUSTOMER_REGION,
            UPPER(CUSTOMER_SEGMENT) AS CUSTOMER_SEGMENT

        FROM FLOWBRIDGE_PROD_DB.SILVER_SCH.TMP_STREAM_DATA

        QUALIFY ROW_NUMBER() OVER
        (
            PARTITION BY UPPER(CUSTOMER_ID)
            ORDER BY ORDER_DATE DESC
        ) = 1

    ) SRC

    ON TGT.CUSTOMER_ID = SRC.CUSTOMER_ID

    WHEN MATCHED THEN UPDATE SET
        TGT.CUSTOMER_NAME    = SRC.CUSTOMER_NAME,
        TGT.CUSTOMER_REGION  = SRC.CUSTOMER_REGION,
        TGT.CUSTOMER_SEGMENT = SRC.CUSTOMER_SEGMENT,
        TGT.IS_ACTIVE        = TRUE,
        TGT.UPDATED_AT       = CURRENT_TIMESTAMP()

    WHEN NOT MATCHED THEN INSERT
    (
        CUSTOMER_ID,
        CUSTOMER_NAME,
        CUSTOMER_REGION,
        CUSTOMER_SEGMENT
    )
    VALUES
    (
        SRC.CUSTOMER_ID,
        SRC.CUSTOMER_NAME,
        SRC.CUSTOMER_REGION,
        SRC.CUSTOMER_SEGMENT
    );


    -- =========================================================
    -- DIM PRODUCT
    -- =========================================================

    MERGE INTO FLOWBRIDGE_PROD_DB.SILVER_SCH.DIM_PRODUCT TGT

    USING
    (
        SELECT
            UPPER(PRODUCT_ID) AS PRODUCT_ID,
            UPPER(PRODUCT_NAME) AS PRODUCT_NAME,
            UPPER(CATEGORY) AS CATEGORY,
            UNIT_PRICE

        FROM FLOWBRIDGE_PROD_DB.SILVER_SCH.TMP_STREAM_DATA

        QUALIFY ROW_NUMBER() OVER
        (
            PARTITION BY UPPER(PRODUCT_ID)
            ORDER BY ORDER_DATE DESC
        ) = 1

    ) SRC

    ON TGT.PRODUCT_ID = SRC.PRODUCT_ID

    WHEN MATCHED THEN UPDATE SET
        TGT.PRODUCT_NAME = SRC.PRODUCT_NAME,
        TGT.CATEGORY     = SRC.CATEGORY,
        TGT.UNIT_PRICE   = SRC.UNIT_PRICE,
        TGT.IS_ACTIVE    = TRUE,
        TGT.UPDATED_AT   = CURRENT_TIMESTAMP()

    WHEN NOT MATCHED THEN INSERT
    (
        PRODUCT_ID,
        PRODUCT_NAME,
        CATEGORY,
        UNIT_PRICE
    )
    VALUES
    (
        SRC.PRODUCT_ID,
        SRC.PRODUCT_NAME,
        SRC.CATEGORY,
        SRC.UNIT_PRICE
    );


    -- =========================================================
    -- DIM SUPPLIER
    -- =========================================================

    MERGE INTO FLOWBRIDGE_PROD_DB.SILVER_SCH.DIM_SUPPLIER TGT

    USING
    (
        SELECT
            UPPER(SUPPLIER_ID) AS SUPPLIER_ID,
            UPPER(SUPPLIER_NAME) AS SUPPLIER_NAME,
            UPPER(SUPPLIER_COUNTRY) AS SUPPLIER_COUNTRY,
            LEAD_TIME_DAYS,
            PERFORMANCE_SCORE

        FROM FLOWBRIDGE_PROD_DB.SILVER_SCH.TMP_STREAM_DATA

        QUALIFY ROW_NUMBER() OVER
        (
            PARTITION BY UPPER(SUPPLIER_ID)
            ORDER BY ORDER_DATE DESC
        ) = 1

    ) SRC

    ON TGT.SUPPLIER_ID = SRC.SUPPLIER_ID

    WHEN MATCHED THEN UPDATE SET
        TGT.SUPPLIER_NAME      = SRC.SUPPLIER_NAME,
        TGT.SUPPLIER_COUNTRY   = SRC.SUPPLIER_COUNTRY,
        TGT.LEAD_TIME_DAYS    = SRC.LEAD_TIME_DAYS,
        TGT.PERFORMANCE_SCORE = SRC.PERFORMANCE_SCORE,
        TGT.IS_ACTIVE         = TRUE,
        TGT.UPDATED_AT        = CURRENT_TIMESTAMP()

    WHEN NOT MATCHED THEN INSERT
    (
        SUPPLIER_ID,
        SUPPLIER_NAME,
        SUPPLIER_COUNTRY,
        LEAD_TIME_DAYS,
        PERFORMANCE_SCORE
    )
    VALUES
    (
        SRC.SUPPLIER_ID,
        SRC.SUPPLIER_NAME,
        SRC.SUPPLIER_COUNTRY,
        SRC.LEAD_TIME_DAYS,
        SRC.PERFORMANCE_SCORE
    );


    -- =========================================================
    -- DIM WAREHOUSE
    -- =========================================================

    MERGE INTO FLOWBRIDGE_PROD_DB.SILVER_SCH.DIM_WAREHOUSE TGT

    USING
    (
        SELECT
            UPPER(WAREHOUSE_ID) AS WAREHOUSE_ID,
            UPPER(WAREHOUSE_LOCATION) AS WAREHOUSE_LOCATION

        FROM FLOWBRIDGE_PROD_DB.SILVER_SCH.TMP_STREAM_DATA

        QUALIFY ROW_NUMBER() OVER
        (
            PARTITION BY UPPER(WAREHOUSE_ID)
            ORDER BY ORDER_DATE DESC
        ) = 1

    ) SRC

    ON TGT.WAREHOUSE_ID = SRC.WAREHOUSE_ID

    WHEN MATCHED THEN UPDATE SET
        TGT.WAREHOUSE_LOCATION = SRC.WAREHOUSE_LOCATION,
        TGT.IS_ACTIVE          = TRUE,
        TGT.UPDATED_AT        = CURRENT_TIMESTAMP()

    WHEN NOT MATCHED THEN INSERT
    (
        WAREHOUSE_ID,
        WAREHOUSE_LOCATION
    )
    VALUES
    (
        SRC.WAREHOUSE_ID,
        SRC.WAREHOUSE_LOCATION
    );


    -- =========================================================
    -- DIM SHIPMENT
    -- =========================================================

    MERGE INTO FLOWBRIDGE_PROD_DB.SILVER_SCH.DIM_SHIPMENT TGT

    USING
    (
        SELECT
            UPPER(SHIPMENT_ID) AS SHIPMENT_ID,
            UPPER(CARRIER) AS CARRIER,
            SHIP_DATE,
            ESTIMATED_DELIVERY

        FROM FLOWBRIDGE_PROD_DB.SILVER_SCH.TMP_STREAM_DATA

        WHERE UPPER(SHIPMENT_ID) <> 'UNKNOWN'

        QUALIFY ROW_NUMBER() OVER
        (
            PARTITION BY UPPER(SHIPMENT_ID)
            ORDER BY ORDER_DATE DESC
        ) = 1

    ) SRC

    ON TGT.SHIPMENT_ID = SRC.SHIPMENT_ID

    WHEN MATCHED THEN UPDATE SET
        TGT.CARRIER            = SRC.CARRIER,
        TGT.SHIP_DATE          = SRC.SHIP_DATE,
        TGT.ESTIMATED_DELIVERY = SRC.ESTIMATED_DELIVERY,
        TGT.IS_ACTIVE          = TRUE,
        TGT.UPDATED_AT         = CURRENT_TIMESTAMP()

    WHEN NOT MATCHED THEN INSERT
    (
        SHIPMENT_ID,
        CARRIER,
        SHIP_DATE,
        ESTIMATED_DELIVERY
    )
    VALUES
    (
        SRC.SHIPMENT_ID,
        SRC.CARRIER,
        SRC.SHIP_DATE,
        SRC.ESTIMATED_DELIVERY
    );


    -- =========================================================
    -- FACT ORDERS
    -- =========================================================

    MERGE INTO FLOWBRIDGE_PROD_DB.SILVER_SCH.FACT_ORDERS TGT

    USING
    (
        SELECT

            UPPER(S.ORDER_ID) AS ORDER_ID,

            DC.CUSTOMER_SK,
            DP.PRODUCT_SK,
            DS.SUPPLIER_SK,
            DW.WAREHOUSE_SK,

            COALESCE(DSH.SHIPMENT_SK, -1) AS SHIPMENT_SK,

            S.ORDER_DATE,
            UPPER(S.ORDER_STATUS) AS ORDER_STATUS,
            UPPER(S.PAYMENT_STATUS) AS PAYMENT_STATUS,
            S.QUANTITY,
            S.UNIT_PRICE,
            S.TOTAL_AMOUNT,
            S.DELAY_DAYS,
            S.INVENTORY_LEVEL,
            S.INGESTED_AT

        FROM FLOWBRIDGE_PROD_DB.SILVER_SCH.TMP_STREAM_DATA S

        INNER JOIN FLOWBRIDGE_PROD_DB.SILVER_SCH.DIM_CUSTOMER DC
            ON DC.CUSTOMER_ID = UPPER(S.CUSTOMER_ID)

        INNER JOIN FLOWBRIDGE_PROD_DB.SILVER_SCH.DIM_PRODUCT DP
            ON DP.PRODUCT_ID = UPPER(S.PRODUCT_ID)

        INNER JOIN FLOWBRIDGE_PROD_DB.SILVER_SCH.DIM_SUPPLIER DS
            ON DS.SUPPLIER_ID = UPPER(S.SUPPLIER_ID)

        INNER JOIN FLOWBRIDGE_PROD_DB.SILVER_SCH.DIM_WAREHOUSE DW
            ON DW.WAREHOUSE_ID = UPPER(S.WAREHOUSE_ID)

        LEFT JOIN FLOWBRIDGE_PROD_DB.SILVER_SCH.DIM_SHIPMENT DSH
            ON DSH.SHIPMENT_ID = UPPER(S.SHIPMENT_ID)

        QUALIFY ROW_NUMBER() OVER
        (
            PARTITION BY UPPER(S.ORDER_ID)
            ORDER BY S.ORDER_DATE DESC
        ) = 1

    ) SRC

    ON TGT.ORDER_ID = SRC.ORDER_ID

    WHEN MATCHED THEN UPDATE SET

        TGT.CUSTOMER_SK     = SRC.CUSTOMER_SK,
        TGT.PRODUCT_SK      = SRC.PRODUCT_SK,
        TGT.SUPPLIER_SK     = SRC.SUPPLIER_SK,
        TGT.WAREHOUSE_SK    = SRC.WAREHOUSE_SK,
        TGT.SHIPMENT_SK     = SRC.SHIPMENT_SK,
        TGT.ORDER_DATE      = SRC.ORDER_DATE,
        TGT.ORDER_STATUS    = SRC.ORDER_STATUS,
        TGT.PAYMENT_STATUS  = SRC.PAYMENT_STATUS,
        TGT.QUANTITY        = SRC.QUANTITY,
        TGT.UNIT_PRICE      = SRC.UNIT_PRICE,
        TGT.TOTAL_AMOUNT    = SRC.TOTAL_AMOUNT,
        TGT.DELAY_DAYS      = SRC.DELAY_DAYS,
        TGT.INVENTORY_LEVEL = SRC.INVENTORY_LEVEL,
        TGT.INGESTED_AT     = SRC.INGESTED_AT,
        TGT.UPDATED_AT      = CURRENT_TIMESTAMP()

    WHEN NOT MATCHED THEN INSERT
    (
        ORDER_ID,
        CUSTOMER_SK,
        PRODUCT_SK,
        SUPPLIER_SK,
        WAREHOUSE_SK,
        SHIPMENT_SK,
        ORDER_DATE,
        ORDER_STATUS,
        PAYMENT_STATUS,
        QUANTITY,
        UNIT_PRICE,
        TOTAL_AMOUNT,
        DELAY_DAYS,
        INVENTORY_LEVEL,
        INGESTED_AT
    )
    VALUES
    (
        SRC.ORDER_ID,
        SRC.CUSTOMER_SK,
        SRC.PRODUCT_SK,
        SRC.SUPPLIER_SK,
        SRC.WAREHOUSE_SK,
        SRC.SHIPMENT_SK,
        SRC.ORDER_DATE,
        SRC.ORDER_STATUS,
        SRC.PAYMENT_STATUS,
        SRC.QUANTITY,
        SRC.UNIT_PRICE,
        SRC.TOTAL_AMOUNT,
        SRC.DELAY_DAYS,
        SRC.INVENTORY_LEVEL,
        SRC.INGESTED_AT
    );


    DROP TABLE IF EXISTS
        FLOWBRIDGE_PROD_DB.SILVER_SCH.TMP_STREAM_DATA;


    RETURN 'silver_to_star completed successfully';

END;

$$;