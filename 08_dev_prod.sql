-- Clone Bronze schema
CREATE OR REPLACE SCHEMA flowbridge_prod_db.bronze_sch
    CLONE flowbridge_dev_db.bronze_sch;

-- Clone Silver schema
-- Clone Silver schema
CREATE OR REPLACE SCHEMA flowbridge_prod_db.silver_sch
    CLONE flowbridge_dev_db.silver_sch;

-- Clone Gold schema
CREATE OR REPLACE SCHEMA flowbridge_prod_db.gold_sch
    CLONE flowbridge_dev_db.gold_sch;

-- Clone Serving schema
CREATE OR REPLACE SCHEMA flowbridge_prod_db.serving_sch
    CLONE flowbridge_dev_db.serving_sch;

show schemas in database flowbridge_dev_db;


-- Grant ownership of PROD schemas to SYSADMIN
GRANT OWNERSHIP ON SCHEMA flowbridge_prod_db.bronze_sch TO ROLE sysadmin COPY CURRENT GRANTS;
GRANT OWNERSHIP ON SCHEMA flowbridge_prod_db.silver_sch TO ROLE sysadmin COPY CURRENT GRANTS;
GRANT OWNERSHIP ON SCHEMA flowbridge_prod_db.gold_sch TO ROLE sysadmin COPY CURRENT GRANTS;
GRANT OWNERSHIP ON SCHEMA flowbridge_prod_db.serving_sch TO ROLE sysadmin COPY CURRENT GRANTS;


use role sysadmin;


-- Bronze
SELECT 'DEV' AS env, COUNT(*) AS raw_orders FROM flowbridge_dev_db.bronze_sch.raw_orders
UNION ALL
SELECT 'PROD' AS env, COUNT(*) AS raw_orders FROM flowbridge_prod_db.bronze_sch.raw_orders;

-- Silver
SELECT 'DEV' AS env, COUNT(*) AS stg_orders FROM flowbridge_dev_db.silver_sch.stg_orders
UNION ALL
SELECT 'PROD' AS env, COUNT(*) AS stg_orders FROM flowbridge_prod_db.silver_sch.stg_orders;

SELECT 'DEV' AS env, COUNT(*) AS fact_orders FROM flowbridge_dev_db.silver_sch.fact_orders
UNION ALL
SELECT 'PROD' AS env, COUNT(*) AS fact_orders FROM flowbridge_prod_db.silver_sch.fact_orders;

-- Gold
SELECT 'DEV' AS env, COUNT(*) AS agg_base FROM flowbridge_dev_db.gold_sch.agg_base
UNION ALL
SELECT 'PROD' AS env, COUNT(*) AS agg_base FROM flowbridge_prod_db.gold_sch.agg_base;

desc integration flowbridge_adls_integration;

USE ROLE SYSADMIN;

USE DATABASE FLOWBRIDGE_PROD_DB;
USE SCHEMA BRONZE_SCH;
USE WAREHOUSE FLOWBRIDGE_PIPELINE_WH;

-- Create PROD external stage
CREATE OR REPLACE STAGE BRONZE_SCH.ADLS_RAW_STAGE_PROD
    URL = 'azure://storageflowbridge.blob.core.windows.net/supply-chain-raw-prod/'
    STORAGE_INTEGRATION = FLOWBRIDGE_ADLS_INTEGRATION
    FILE_FORMAT = BRONZE_SCH.JSON_FILE_FORMAT
    COMMENT = 'External Stage - ADLS Gen2 Prod container';

-- Verify files
LIST @BRONZE_SCH.ADLS_RAW_STAGE_PROD;


-- Create PROD Snowpipe
create pipe if not exists bronze_sch.supply_chain_pipe_prod
  auto_ingest = true
  integration = flowbridge_azure_notifications_int
  comment = 'Snowpipe - auto ingest json files from ADLS Gen2'
as
  copy into flowbridge_prod_db.bronze_sch.raw_orders (
    raw_data,
    file_name,
    file_row_number
  )
from (
  select $1,
         metadata$filename,
         metadata$file_row_number
  from @flowbridge_prod_db.bronze_sch.adls_raw_stage_prod
) file_format = (format_name = 'bronze_sch.json_file_format');


drop pipe FLOWBRIDGE_PROD_DB.BRONZE_SCH.SUPPLY_CHAIN_PIPE;

select system$pipe_status('FLOWBRIDGE_PROD_DB.BRONZE_SCH.SUPPLY_CHAIN_PIPE_PROD');


-- Stream on PROD RAW_ORDERS
create or replace stream flowbridge_prod_db.silver_sch.raw_orders_stream
  on table flowbridge_prod_db.bronze_sch.raw_orders
  append_only = true
  show_initial_rows = true
  comment = 'Stream on raw_orders - captures new data for silver processing';

-- Stream on PROD STG_ORDERS
create or replace stream flowbridge_prod_db.silver_sch.stg_orders_stream
  on table flowbridge_prod_db.silver_sch.stg_orders
  show_initial_rows = true
  comment = 'Stream on stg_orders';

show streams in database flowbridge_prod_db;



CREATE OR REPLACE PROCEDURE flowbridge_prod_db.silver_sch.sp_bronze_to_silver()
RETURNS STRING
LANGUAGE SQL
COMMENT = 'SP - routes bad records to dead_letter, merges clean records to stg_orders - will be called by task'
AS
BEGIN

    ---------------------------------------------------------------------------
    -- PART 0 - BUFFER STREAM INTO TEMP TABLE
    -- Stream can only be consumed once per DML statement.
    -- We materialise it here so both DEAD LETTER and MERGE
    -- can read from the same snapshot.
    ---------------------------------------------------------------------------
    CREATE OR REPLACE TEMPORARY TABLE flowbridge_prod_db.silver_sch.stream_buffer AS
    SELECT *
    FROM flowbridge_prod_db.silver_sch.raw_orders_stream
    WHERE metadata$action = 'INSERT';


    ---------------------------------------------------------------------------
    -- PART 1 - ROUTE BAD RECORDS TO DEAD LETTER
    -- Only truly unrecoverable records are rejected.
    -- Validation is case-insensitive using UPPER(TRIM())
    -- order_date: reject ONLY when BOTH conditions fail
    --   (AND logic) - ISO format passes TRY_TO_TIMESTAMP_NTZ
    --   DD-MM-YYYY matches LIKE pattern
    -- order_status: UPPER(TRIM()) before IN check
    --   so "shipped" / "Shipped" / "SHIPPED" all pass
    ---------------------------------------------------------------------------
    INSERT INTO flowbridge_prod_db.silver_sch.dead_letter (
        raw_data,
        error_reason,
        file_name,
        file_row_number
    )
    SELECT
        s.raw_data,
        CASE
            -- Order level validations
            WHEN s.raw_data:order_id::STRING IS NULL
                THEN 'Missing order_id'

            WHEN TRY_TO_TIMESTAMP_NTZ(s.raw_data:order_date::STRING) IS NULL
                 AND s.raw_data:order_date::STRING NOT LIKE '__-__-____'
                THEN 'Invalid or missing order_date'

            WHEN UPPER(TRIM(s.raw_data:order_status::STRING)) NOT IN (
                     'PENDING', 'PROCESSING', 'SHIPPED', 
                     'IN TRANSIT', 'DELIVERED', 'CANCELLED'
                 )
                THEN 'Invalid order_status: ' || COALESCE(s.raw_data:order_status::STRING, 'NULL')

            -- Customer level validations
            WHEN s.raw_data:customer.customer_id::STRING IS NULL
                THEN 'Missing customer_id'

            -- Supplier level validations
            WHEN s.raw_data:supplier.supplier_id::STRING IS NULL
                THEN 'Missing supplier_id'

            WHEN s.raw_data:supplier.performance_score::FLOAT > 100
                THEN 'Invalid performance_score > 100: ' || s.raw_data:supplier.performance_score::STRING

            WHEN s.raw_data:supplier.performance_score::FLOAT < 0
                THEN 'Invalid performance_score < 0: ' || s.raw_data:supplier.performance_score::STRING

            WHEN s.raw_data:supplier.lead_time_days::NUMBER < 0
                THEN 'Invalid lead_time_days < 0: ' || s.raw_data:supplier.lead_time_days::STRING

            -- Product level validations
            WHEN s.raw_data:items.product_id::STRING IS NULL
                THEN 'Missing product_id'

            WHEN s.raw_data:items.quantity::NUMBER IS NULL
                 OR s.raw_data:items.quantity::NUMBER <= 0
                THEN 'Invalid quantity: ' || COALESCE(s.raw_data:items.quantity::STRING, 'NULL')

            WHEN s.raw_data:items.unit_price::FLOAT IS NULL
                 OR s.raw_data:items.unit_price::FLOAT < 0
                THEN 'Invalid unit_price: ' || COALESCE(s.raw_data:items.unit_price::STRING, 'NULL')

            -- Financial level validations
            WHEN s.raw_data:financials.total_amount::FLOAT IS NULL
                 OR s.raw_data:financials.total_amount::FLOAT < 0
                THEN 'Invalid total_amount: ' || COALESCE(s.raw_data:financials.total_amount::STRING, 'NULL')

            -- Warehouse level validations
            WHEN s.raw_data:warehouse.inventory_level::NUMBER < 0
                THEN 'Invalid inventory_level < 0: ' || s.raw_data:warehouse.inventory_level::STRING

            ELSE 'Unknown'
        END AS error_reason,
        s.file_name,
        s.file_row_number
    FROM flowbridge_prod_db.silver_sch.stream_buffer AS s
    WHERE (
        -- Order Level
        s.raw_data:order_id::STRING IS NULL
        OR (
            TRY_TO_TIMESTAMP_NTZ(s.raw_data:order_date::STRING) IS NULL
            AND s.raw_data:order_date::STRING NOT LIKE '__-__-____'
        )
        OR UPPER(TRIM(s.raw_data:order_status::STRING)) NOT IN (
            'PENDING', 'PROCESSING', 'SHIPPED', 
            'IN TRANSIT', 'DELIVERED', 'CANCELLED'
        )
        -- Customer Level
        OR s.raw_data:customer.customer_id::STRING IS NULL
        -- Supplier Level
        OR s.raw_data:supplier.supplier_id::STRING IS NULL
        OR s.raw_data:supplier.performance_score::FLOAT > 100
        OR s.raw_data:supplier.performance_score::FLOAT < 0
        OR s.raw_data:supplier.lead_time_days::NUMBER < 0
        -- Product Level
        OR s.raw_data:items.product_id::STRING IS NULL
        OR s.raw_data:items.quantity::NUMBER IS NULL
        OR s.raw_data:items.quantity::NUMBER <= 0
        OR s.raw_data:items.unit_price::FLOAT IS NULL
        OR s.raw_data:items.unit_price::FLOAT < 0
        -- Financial Level
        OR s.raw_data:financials.total_amount::FLOAT IS NULL
        OR s.raw_data:financials.total_amount::FLOAT < 0
        -- Warehouse Level
        OR s.raw_data:warehouse.inventory_level::NUMBER < 0
    );


    ---------------------------------------------------------------------------
    -- PART 2 - MERGE CLEAN RECORDS INTO STG_ORDERS
    -- WHEN MATCHED     -> UPDATE changed fields only
    -- WHEN NOT MATCHED -> INSERT new order
    ---------------------------------------------------------------------------
    MERGE INTO flowbridge_prod_db.silver_sch.stg_orders AS tgt
    USING (
        SELECT
            -- Order fields
            UPPER(TRIM(s.raw_data:order_id::STRING)) AS order_id,
            TRY_TO_TIMESTAMP_NTZ(
                CASE
                    WHEN s.raw_data:order_date::STRING LIKE '__-__-____'
                        THEN TO_VARCHAR(TO_DATE(s.raw_data:order_date::STRING, 'DD-MM-YYYY'), 'YYYY-MM-DD') || 'T00:00:00Z'
                    ELSE s.raw_data:order_date::STRING
                END
            ) AS order_date,
            UPPER(TRIM(s.raw_data:order_status::STRING)) AS order_status,

            -- Customer fields
            UPPER(TRIM(s.raw_data:customer.customer_id::STRING)) AS customer_id,
            COALESCE(UPPER(TRIM(s.raw_data:customer.customer_name::STRING)), 'UNKNOWN') AS customer_name,
            COALESCE(UPPER(TRIM(s.raw_data:customer.region::STRING)), 'UNKNOWN') AS customer_region,
            COALESCE(UPPER(TRIM(s.raw_data:customer.segment::STRING)), 'UNKNOWN') AS customer_segment,

            -- Supplier fields
            UPPER(TRIM(s.raw_data:supplier.supplier_id::STRING)) AS supplier_id,
            COALESCE(UPPER(TRIM(s.raw_data:supplier.supplier_name::STRING)), 'UNKNOWN') AS supplier_name,
            COALESCE(UPPER(TRIM(s.raw_data:supplier.country::STRING)), 'UNKNOWN') AS supplier_country,
            COALESCE(s.raw_data:supplier.lead_time_days::NUMBER, 0) AS lead_time_days,
            COALESCE(s.raw_data:supplier.performance_score::FLOAT, 0) AS performance_score,

            -- Shipment fields
            COALESCE(UPPER(TRIM(s.raw_data:shipment.shipment_id::STRING)), 'UNKNOWN') AS shipment_id,
            COALESCE(UPPER(TRIM(s.raw_data:shipment.carrier::STRING)), 'UNKNOWN') AS carrier,
            TRY_TO_TIMESTAMP_NTZ(s.raw_data:shipment.ship_date::STRING) AS ship_date,
            TRY_TO_DATE(s.raw_data:shipment.estimated_delivery::STRING) AS estimated_delivery,
            GREATEST(COALESCE(s.raw_data:shipment.delay_days::NUMBER, 0), 0) AS delay_days,

            -- Product fields (items array)
            UPPER(TRIM(s.raw_data:items.product_id::STRING)) AS product_id,
            COALESCE(UPPER(TRIM(s.raw_data:items.product_name::STRING)), 'UNKNOWN') AS product_name,
            COALESCE(UPPER(TRIM(s.raw_data:items.category::STRING)), 'UNKNOWN') AS category,
            s.raw_data:items.quantity::NUMBER AS quantity,
            s.raw_data:items.unit_price::FLOAT AS unit_price,

            -- Financial fields
            s.raw_data:financials.total_amount::FLOAT AS total_amount,
            COALESCE(UPPER(TRIM(s.raw_data:financials.payment_status::STRING)), 'UNKNOWN') AS payment_status,

            -- Warehouse fields
            COALESCE(UPPER(TRIM(s.raw_data:warehouse.warehouse_id::STRING)), 'UNKNOWN') AS warehouse_id,
            COALESCE(UPPER(TRIM(s.raw_data:warehouse.warehouse_location::STRING)), 'UNKNOWN') AS warehouse_location,
            COALESCE(s.raw_data:warehouse.inventory_level::NUMBER, 0) AS inventory_level,

            -- Metadata
            s.file_name,
            s.file_row_number,
            s.ingested_at
        FROM flowbridge_prod_db.silver_sch.stream_buffer AS s
        WHERE s.raw_data:order_id::STRING IS NOT NULL
          AND (
              TRY_TO_TIMESTAMP_NTZ(s.raw_data:order_date::STRING) IS NOT NULL
              OR s.raw_data:order_date::STRING LIKE '__-__-____'
          )
          AND UPPER(TRIM(s.raw_data:order_status::STRING)) IN (
              'PENDING', 'PROCESSING', 'SHIPPED', 
              'IN TRANSIT', 'DELIVERED', 'CANCELLED'
          )
          AND s.raw_data:customer.customer_id::STRING IS NOT NULL
          AND s.raw_data:supplier.supplier_id::STRING IS NOT NULL
          AND s.raw_data:supplier.performance_score::FLOAT BETWEEN 0 AND 100
          AND (
              s.raw_data:supplier.lead_time_days::NUMBER IS NULL
              OR s.raw_data:supplier.lead_time_days::NUMBER >= 0
          )
          AND s.raw_data:items.product_id::STRING IS NOT NULL
          AND s.raw_data:items.quantity::NUMBER IS NOT NULL
                  AND s.raw_data:items.quantity::NUMBER > 0
        AND s.raw_data:items.unit_price::FLOAT IS NOT NULL
        AND s.raw_data:items.unit_price::FLOAT >= 0
        AND s.raw_data:financials.total_amount::FLOAT IS NOT NULL
        AND s.raw_data:financials.total_amount::FLOAT >= 0
        AND (
            s.raw_data:warehouse.inventory_level::NUMBER IS NULL
            OR s.raw_data:warehouse.inventory_level::NUMBER >= 0
        )
) AS src
ON tgt.order_id = src.order_id

WHEN MATCHED THEN
    UPDATE SET
        tgt.order_status       = src.order_status,
        tgt.carrier            = src.carrier,
        tgt.ship_date          = src.ship_date,
        tgt.estimated_delivery = src.estimated_delivery,
        tgt.delay_days         = src.delay_days,
        tgt.inventory_level    = src.inventory_level,
        tgt.payment_status     = src.payment_status,
        tgt.customer_name      = src.customer_name,
        tgt.customer_region    = src.customer_region,
        tgt.supplier_name      = src.supplier_name,
        tgt.performance_score  = src.performance_score,
        tgt.warehouse_id       = src.warehouse_id,
        tgt.warehouse_location = src.warehouse_location,
        tgt.transformed_at     = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
    INSERT (
        order_id, 
        order_date, 
        order_status,
        customer_id, 
        customer_name, 
        customer_region, 
        customer_segment,
        supplier_id, 
        supplier_name, 
        supplier_country, 
        lead_time_days, 
        performance_score,
        shipment_id, 
        carrier, 
        ship_date, 
        estimated_delivery, 
        delay_days,
        product_id, 
        product_name, 
        category, 
        quantity, 
        unit_price,
        total_amount, 
        payment_status,
        warehouse_id, 
        warehouse_location, 
        inventory_level,
        file_name, 
        file_row_number, 
        ingested_at
    )
    VALUES (
        src.order_id, 
        src.order_date, 
        src.order_status,
        src.customer_id, 
        src.customer_name, 
        src.customer_region, 
        src.customer_segment,
        src.supplier_id, 
        src.supplier_name, 
        src.supplier_country, 
        src.lead_time_days, 
        src.performance_score,
        src.shipment_id, 
        src.carrier, 
        src.ship_date, 
        src.estimated_delivery, 
        src.delay_days,
        src.product_id, 
        src.product_name, 
        src.category, 
        src.quantity, 
        src.unit_price,
        src.total_amount, 
        src.payment_status,
        src.warehouse_id, 
        src.warehouse_location, 
        src.inventory_level,
        src.file_name, 
        src.file_row_number, 
        src.ingested_at
    );

DROP TABLE IF EXISTS flowbridge_prod_db.silver_sch.stream_buffer;

RETURN 'sp_bronze_to_silver completed successfully';

END;



CREATE OR REPLACE PROCEDURE flowbridge_prod_db.silver_sch.silver_to_star()
RETURNS STRING
LANGUAGE SQL
AS
BEGIN

    -- =========================================================================
    -- Part 1 - TEMP TABLE
    -- =========================================================================
    CREATE OR REPLACE TEMPORARY TABLE flowbridge_prod_db.silver_sch.tmp_stream_data AS
    SELECT * 
    FROM flowbridge_prod_db.silver_sch.stg_orders_stream
    WHERE metadata$action = 'INSERT';


    -- =========================================================================
    -- Part 2 - Merge into dim_customer ( SCD1 - Upsert )
    -- =========================================================================
    MERGE INTO flowbridge_prod_db.silver_sch.dim_customer AS tgt
    USING (
        SELECT 
            UPPER(customer_id)      AS customer_id,
            UPPER(customer_name)    AS customer_name,
            UPPER(customer_region)  AS customer_region,
            UPPER(customer_segment) AS customer_segment
        FROM flowbridge_prod_db.silver_sch.tmp_stream_data
        QUALIFY ROW_NUMBER() OVER (PARTITION BY UPPER(customer_id) ORDER BY order_date DESC) = 1
    ) AS src
    ON tgt.customer_id = src.customer_id
    
    WHEN MATCHED THEN 
        UPDATE SET
            tgt.customer_name   = src.customer_name,
            tgt.customer_region = src.customer_region,
            tgt.customer_segment = src.customer_segment,
            tgt.is_active       = TRUE,
            tgt.updated_at      = CURRENT_TIMESTAMP()
            
    WHEN NOT MATCHED THEN 
        INSERT (
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


    -- =========================================================================
    -- Part 3 - Merge into dim_product ( SCD1 - Upsert )
    -- =========================================================================
    MERGE INTO flowbridge_prod_db.silver_sch.dim_product AS tgt
    USING (
        SELECT
            UPPER(product_id)   AS product_id,
            UPPER(product_name) AS product_name,
            UPPER(category)     AS category,
            unit_price
        FROM flowbridge_prod_db.silver_sch.tmp_stream_data
        QUALIFY ROW_NUMBER() OVER (PARTITION BY UPPER(product_id) ORDER BY order_date DESC) = 1
    ) AS src
    ON tgt.product_id = src.product_id
    
    WHEN MATCHED THEN 
        UPDATE SET
            tgt.product_name = src.product_name,
            tgt.category     = src.category,
            tgt.unit_price   = src.unit_price,
            tgt.is_active    = TRUE,
            tgt.updated_at   = CURRENT_TIMESTAMP()
            
    WHEN NOT MATCHED THEN 
        INSERT (
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


    -- =========================================================================
    -- Part 4 - Merge into dim_supplier ( SCD1 - Upsert )
    -- =========================================================================
    MERGE INTO flowbridge_prod_db.silver_sch.dim_supplier AS tgt
    USING (
        SELECT
            UPPER(supplier_id)      AS supplier_id,
            UPPER(supplier_name)    AS supplier_name,
            UPPER(supplier_country) AS supplier_country,
            lead_time_days,
            performance_score
        FROM flowbridge_prod_db.silver_sch.tmp_stream_data
        QUALIFY ROW_NUMBER() OVER (PARTITION BY UPPER(supplier_id) ORDER BY order_date DESC) = 1
    ) AS src
    ON tgt.supplier_id = src.supplier_id
    
    WHEN MATCHED THEN 
        UPDATE SET
            tgt.supplier_name     = src.supplier_name,
            tgt.supplier_country  = src.supplier_country,
            tgt.lead_time_days    = src.lead_time_days,
            tgt.performance_score = src.performance_score,
            tgt.is_active         = TRUE,
            tgt.updated_at        = CURRENT_TIMESTAMP()
            
    WHEN NOT MATCHED THEN 
        INSERT (
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


    -- =========================================================================
    -- Part 5 - Merge into dim_warehouse ( SCD1 - Upsert )
    -- =========================================================================
    MERGE INTO flowbridge_prod_db.silver_sch.dim_warehouse AS tgt
    USING (
        SELECT
            UPPER(warehouse_id)       AS warehouse_id,
            UPPER(warehouse_location) AS warehouse_location
        FROM flowbridge_prod_db.silver_sch.tmp_stream_data
        QUALIFY ROW_NUMBER() OVER (PARTITION BY UPPER(warehouse_id) ORDER BY order_date DESC) = 1
    ) AS src
    ON tgt.warehouse_id = src.warehouse_id
    
    WHEN MATCHED THEN 
        UPDATE SET
            tgt.warehouse_location = src.warehouse_location,
            tgt.is_active          = TRUE,
            tgt.updated_at         = CURRENT_TIMESTAMP()
            
    WHEN NOT MATCHED THEN 
        INSERT (
            warehouse_id, 
            warehouse_location
        )
        VALUES (
            src.warehouse_id, 
            src.warehouse_location
        );


    -- =========================================================================
    -- Part 6 - Merge into dim_shipment ( SCD1 - Upsert )
    -- =========================================================================
    MERGE INTO flowbridge_prod_db.silver_sch.dim_shipment AS tgt
    USING (
        SELECT
            UPPER(shipment_id) AS shipment_id,
            UPPER(carrier)     AS carrier,
            ship_date,
            estimated_delivery
        FROM flowbridge_prod_db.silver_sch.tmp_stream_data
        WHERE UPPER(shipment_id) != 'UNKNOWN'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY UPPER(shipment_id) ORDER BY order_date DESC) = 1
    ) AS src
    ON tgt.shipment_id = src.shipment_id
    
    WHEN MATCHED THEN 
        UPDATE SET
            tgt.carrier            = src.carrier,
            tgt.ship_date          = src.ship_date,
            tgt.estimated_delivery = src.estimated_delivery,
            tgt.is_active          = TRUE,
            tgt.updated_at         = CURRENT_TIMESTAMP()
            
    WHEN NOT MATCHED THEN 
        INSERT (
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


    -- =========================================================================
    -- Part 7 - Merge into fact_orders
    -- =========================================================================
    MERGE INTO flowbridge_prod_db.silver_sch.fact_orders AS tgt
    USING (
        SELECT
            UPPER(s.order_id)               AS order_id,
            dc.customer_sk,
            dp.product_sk,
            ds.supplier_sk,
            dw.warehouse_sk,
            COALESCE(dsh.shipment_sk, -1)   AS shipment_sk,
            s.order_date,
            UPPER(s.order_status)           AS order_status,
            UPPER(s.payment_status)         AS payment_status,
            s.quantity,
            s.unit_price,
            s.total_amount,
            s.delay_days,
            s.inventory_level,
            s.ingested_at
        FROM flowbridge_prod_db.silver_sch.tmp_stream_data s
        INNER JOIN flowbridge_prod_db.silver_sch.dim_customer dc  ON dc.customer_id  = UPPER(s.customer_id)
        INNER JOIN flowbridge_prod_db.silver_sch.dim_product dp   ON dp.product_id   = UPPER(s.product_id)
        INNER JOIN flowbridge_prod_db.silver_sch.dim_supplier ds  ON ds.supplier_id  = UPPER(s.supplier_id)
        INNER JOIN flowbridge_prod_db.silver_sch.dim_warehouse dw ON dw.warehouse_id = UPPER(s.warehouse_id)
        LEFT JOIN flowbridge_prod_db.silver_sch.dim_shipment dsh  ON dsh.shipment_id = UPPER(s.shipment_id)
        QUALIFY ROW_NUMBER() OVER (PARTITION BY UPPER(s.order_id) ORDER BY s.order_date DESC) = 1
    ) AS src
    ON tgt.order_id = src.order_id
    
    WHEN MATCHED THEN 
        UPDATE SET
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
            
    WHEN NOT MATCHED THEN 
        INSERT (
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

    -- =========================================================================
    -- Cleanup temp table - no longer needed after all MERGEs
    -- =========================================================================
    DROP TABLE IF EXISTS flowbridge_prod_db.silver_sch.tmp_stream_data;

    RETURN 'sp_silver_to_star completed successfully';

END;



-- Bronze to Silver task
CREATE OR REPLACE TASK flowbridge_prod_db.silver_sch.bronze_to_silver_task
    WAREHOUSE = flowbridge_pipeline_wh
    SCHEDULE  = '1 minute'
    COMMENT   = 'Task - calls sp_bronze_to_silver every minute when raw_orders_stream has data'
WHEN SYSTEM$STREAM_HAS_DATA('silver_sch.raw_orders_stream')
AS
    CALL silver_sch.sp_bronze_to_silver();


            
-- Bronze to Silver task
CREATE OR REPLACE TASK flowbridge_prod_db.silver_sch.bronze_to_silver_task
    WAREHOUSE = flowbridge_pipeline_wh
    SCHEDULE  = '1 minute'
    COMMENT   = 'Task - calls sp_bronze_to_silver every minute when raw_orders_stream has data'
WHEN SYSTEM$STREAM_HAS_DATA('flowbridge_prod_db.silver_sch.raw_orders_stream')
AS
    CALL silver_sch.sp_bronze_to_silver();


-- Silver to Star task
CREATE OR REPLACE TASK flowbridge_prod_db.silver_sch.silver_to_star_task
    WAREHOUSE = flowbridge_pipeline_wh
    SCHEDULE  = '1 minute'
WHEN SYSTEM$STREAM_HAS_DATA('flowbridge_prod_db.silver_sch.stg_orders_stream')
AS
    CALL silver_sch.sp_silver_to_star();




show tasks in database flowbridge_prod_db;


-- =========================================================================
-- Recreate to point to PROD database
-- =========================================================================
USE ROLE accountadmin;

CREATE OR REPLACE ALERT flowbridge_prod_db.bronze_sch.pipeline_health_alert
    WAREHOUSE = flowbridge_pipeline_wh
    SCHEDULE  = '5 minute'
IF (EXISTS (
    
    -- --------- Bronze - Snowpipe Failure ---------
    SELECT 1
    FROM TABLE(information_schema.copy_history(
        table_name => 'RAW_ORDERS',
        start_time => DATEADD(hour, -1, CURRENT_TIMESTAMP())
    ))
    WHERE status = 'Load Failed'

    UNION ALL

    -- --------- Silver - 2 tasks failure ---------
    SELECT 1 
    FROM TABLE(information_schema.task_history(
        scheduled_time_range_start => DATEADD(hour, -1, CURRENT_TIMESTAMP())
    ))
    WHERE state = 'FAILED'
      AND database_name = 'FLOWBRIDGE_PROD_DB'

    UNION ALL

    -- --------- Gold - DT's Failure ---------
    SELECT 1 
    FROM TABLE(information_schema.dynamic_table_refresh_history())
    WHERE schema_name = 'GOLD_SCH'
      AND database_name = 'FLOWBRIDGE_PROD_DB'
      AND state = 'FAILED'
      AND refresh_start_time > DATEADD(minute, -5, CURRENT_TIMESTAMP())
))
THEN CALL system$send_email(
    'email_notification_int',
    'briankeya043@gmail.com',
    'Pipeline Alert! - Flowbridge Project!',
    'Something went wrong in the pipeline. Check Bronze/Silver/Gold Layers for failures. Login to Snowsight -> Monitoring -> History / Copy History'
);


ALTER ALERT flowbridge_prod_db.bronze_sch.pipeline_health_alert RESUME;

-- =========================================================================
-- STEP 8 - SWITCH DATA SHARING TO PROD
-- Purpose : Update share to use PROD serving layer
-- =========================================================================
USE ROLE accountadmin;

-- 1. Create the share object first
CREATE SHARE IF NOT EXISTS flowbridge_prod_share;

-- 2. Add PROD grants to the share
GRANT USAGE ON DATABASE flowbridge_prod_db 
    TO SHARE flowbridge_prod_share;

GRANT USAGE ON SCHEMA flowbridge_prod_db.serving_sch 
    TO SHARE flowbridge_prod_share;


GRANT SELECT ON VIEW flowbridge_prod_db.serving_sch.vw_order_fulfillment 
    TO SHARE flowbridge_prod_share;

GRANT SELECT ON VIEW flowbridge_prod_db.serving_sch.vw_supplier_performance 
    TO SHARE flowbridge_prod_share;

GRANT SELECT ON VIEW flowbridge_prod_db.serving_sch.vw_inventory_turnover 
    TO SHARE flowbridge_prod_share;

GRANT SELECT ON VIEW flowbridge_prod_db.serving_sch.vw_shipment_delays 
    TO SHARE flowbridge_prod_share;


desc share flowbridge_prod_share;

show grants on share flowbridge_prod_share;



-- ⚠️ Run these in your session BEFORE executing the CREATE below (do NOT save passwords in files):
SET reader_admin_name = 'flowbridge_admin';
SET reader_admin_password = 'YourStrongPassword123!';

CREATE MANAGED ACCOUNT IF NOT EXISTS flowbridge_partner_account
    ADMIN_NAME = $reader_admin_name
    ADMIN_PASSWORD = $reader_admin_password
    TYPE = READER
    COMMENT = 'Reader Account for Flowbridge Logistics Partners - access to PROD KPI only';

-- Get reader account details
SHOW MANAGED ACCOUNTS;

alter share flowbridge_prod_share
 add accounts = "IF53590";




GRANT OWNERSHIP ON STREAM FLOWBRIDGE_PROD_DB.SILVER_SCH.RAW_ORDERS_STREAM TO ROLE SYSADMIN COPY CURRENT GRANTS;

USE ROLE SYSADMIN;

CALL FLOWBRIDGE_PROD_DB.SILVER_SCH.SP_BRONZE_TO_SILVER();
CALL FLOWBRIDGE_PROD_DB.SILVER_SCH.SILVER_TO_STAR();

-- Step 2: Resume tasks for ongoing processing
ALTER TASK FLOWBRIDGE_PROD_DB.SILVER_SCH.BRONZE_TO_SILVER_TASK RESUME;
ALTER TASK FLOWBRIDGE_PROD_DB.SILVER_SCH.SILVER_TO_STAR_TASK RESUME;


-- Step 3: Verify row counts
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



