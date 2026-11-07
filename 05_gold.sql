-- -----------------------------------------------------------------------------
-- FLOWBRIDGE - GOLD LAYER
-- Purpose: Create Gold aggregation base dynamic table
-- -----------------------------------------------------------------------------

USE ROLE SYSADMIN;
USE WAREHOUSE FLOWBRIDGE_PIPELINE_WH;
USE DATABASE FLOWBRIDGE_DEV_DB;
USE SCHEMA GOLD_SCH;

-- Grant warehouse permissions
GRANT USAGE, OPERATE
ON WAREHOUSE FLOWBRIDGE_PIPELINE_WH
TO ROLE SYSADMIN;

-- -----------------------------------------------------------------------------
-- CREATE GOLD BASE DYNAMIC TABLE
-- -----------------------------------------------------------------------------

CREATE OR REPLACE DYNAMIC TABLE GOLD_SCH.AGG_BASE
    TARGET_LAG = DOWNSTREAM
    WAREHOUSE = FLOWBRIDGE_PIPELINE_WH
    COMMENT = 'Base Dynamic Table'
AS

SELECT
    -- Order fields
    f.ORDER_ID,
    f.ORDER_DATE,
    f.ORDER_STATUS,
    f.PAYMENT_STATUS,

    -- Measures
    f.QUANTITY,
    f.UNIT_PRICE,
    f.TOTAL_AMOUNT,
    f.DELAY_DAYS,
    f.INVENTORY_LEVEL,

    -- Pipeline metadata
    f.INGESTED_AT,

    -- Customer fields
    c.CUSTOMER_ID,
    c.CUSTOMER_NAME,
    c.CUSTOMER_REGION,
    c.CUSTOMER_SEGMENT,

    -- Product fields
    p.PRODUCT_ID,
    p.PRODUCT_NAME,
    p.CATEGORY,

    -- Supplier fields
    sp.SUPPLIER_ID,
    sp.SUPPLIER_NAME,
    sp.SUPPLIER_COUNTRY,
    sp.LEAD_TIME_DAYS,
    sp.PERFORMANCE_SCORE,

    -- Warehouse fields
    w.WAREHOUSE_ID,
    w.WAREHOUSE_LOCATION,

    -- Shipment fields
    sh.SHIPMENT_ID,
    sh.CARRIER,
    sh.SHIP_DATE,
    sh.ESTIMATED_DELIVERY

FROM SILVER_SCH.FACT_ORDERS AS f

JOIN SILVER_SCH.DIM_CUSTOMER AS c
    ON f.CUSTOMER_SK = c.CUSTOMER_SK

JOIN SILVER_SCH.DIM_PRODUCT AS p
    ON f.PRODUCT_SK = p.PRODUCT_SK

JOIN SILVER_SCH.DIM_SUPPLIER AS sp
    ON f.SUPPLIER_SK = sp.SUPPLIER_SK

JOIN SILVER_SCH.DIM_WAREHOUSE AS w
    ON f.WAREHOUSE_SK = w.WAREHOUSE_SK

LEFT JOIN SILVER_SCH.DIM_SHIPMENT AS sh
    ON f.SHIPMENT_SK = sh.SHIPMENT_SK;



select * from GOLD_SCH.AGG_BASE;

CREATE OR REPLACE DYNAMIC TABLE gold_sch.agg_order_fulfillment
    TARGET_LAG = '1 minute'
    WAREHOUSE = flowbridge_pipeline_wh
    COMMENT = 'Order fulfillment KPIs by customer region. DOWNSTREAM from AGG_BASE.'
AS
SELECT
    customer_region,
    customer_segment,
    
    -- Order counts
    COUNT(order_id) as total_orders,
    COUNT(CASE WHEN order_status = 'DELIVERED' THEN 1 END) as delivered_orders,
    COUNT(CASE WHEN order_status IN ('PENDING', 'PROCESSING') THEN 1 END) as pending_orders,
    COUNT(CASE WHEN order_status = 'CANCELLED' THEN 1 END) as cancelled_orders,
    COUNT(CASE WHEN order_status = 'IN TRANSIT' THEN 1 END) as in_transit_orders,
    
    -- Fulfillment rate - delivered / total * 100
    ROUND(
        COUNT(CASE WHEN order_status = 'DELIVERED' THEN 1 END) 
        / NULLIF(COUNT(order_id), 0) * 100, 
        2
    ) as fulfillment_rate_pct,
    
    -- Revenue KPIs
    ROUND(SUM(total_amount), 2) as total_revenue,
    ROUND(AVG(total_amount), 2) as avg_order_value,
    
    -- Payment breakdown
    COUNT(CASE WHEN payment_status = 'PAID' THEN 1 END) as paid_orders,
    COUNT(CASE WHEN payment_status = 'PENDING' THEN 1 END) as payment_pending_orders,
    COUNT(CASE WHEN payment_status = 'OVERDUE' THEN 1 END) as overdue_orders
FROM gold_sch.agg_base
GROUP BY 
    customer_region,
    customer_segment;

select * from gold_sch.agg_order_fulfillment;


CREATE OR REPLACE DYNAMIC TABLE gold_sch.agg_supplier_performance
    TARGET_LAG = '1 minute'
    WAREHOUSE = flowbridge_pipeline_wh
    COMMENT = 'Supplier performance KPIs. DOWNSTREAM from AGG_BASE.'
AS
SELECT
    supplier_id,
    supplier_name,
    supplier_country,
    
    -- Order volume
    COUNT(order_id) as total_orders,
    
    -- Performance metrics
    ROUND(AVG(performance_score), 2) as avg_performance_score,
    ROUND(AVG(lead_time_days), 2) as avg_lead_time_days,
    
    -- Revenue
    ROUND(SUM(total_amount), 2) as total_revenue,
    ROUND(AVG(total_amount), 2) as avg_order_value,
    
    -- Delay metrics
    COUNT(CASE WHEN delay_days <= 0 AND shipment_id IS NOT NULL AND shipment_id <> 'UNKNOWN' THEN 1 END) as on_time_orders,
    COUNT(CASE WHEN delay_days > 0 AND shipment_id IS NOT NULL AND shipment_id <> 'UNKNOWN' THEN 1 END) as delayed_orders,
    
    -- On time rate
    ROUND(
        COUNT(CASE WHEN delay_days <= 0 AND shipment_id IS NOT NULL AND shipment_id <> 'UNKNOWN' THEN 1 END)
        / NULLIF(COUNT(CASE WHEN delay_days IS NOT NULL AND shipment_id IS NOT NULL AND shipment_id <> 'UNKNOWN' THEN 1 END), 0) * 100, 
        2
    ) as on_time_rate_pct,
    
    -- Average delay when delayed
    ROUND(AVG(CASE WHEN delay_days > 0 THEN delay_days END), 2) as avg_delay_days
FROM gold_sch.agg_base
GROUP BY 
    supplier_id,
    supplier_name,
    supplier_country;


select * from gold_sch.agg_supplier_performance;


CREATE OR REPLACE DYNAMIC TABLE gold_sch.agg_inventory_turnover
    TARGET_LAG = '1 minute'
    WAREHOUSE = flowbridge_pipeline_wh
    COMMENT = 'Quantity-to-average-inventory movement proxy by warehouse and category. Leaf dynamic table drives dashboard freshness.'
AS
SELECT
    warehouse_id,
    warehouse_location,
    category,
    
    -- Order volume
    COUNT(order_id) as total_orders,
    SUM(quantity) as total_quantity_ordered,
    
    -- Inventory metrics
    ROUND(AVG(inventory_level), 2) as avg_inventory_level,
    MIN(inventory_level) as min_inventory_level,
    MAX(inventory_level) as max_inventory_level,
    
    -- Revenue
    ROUND(SUM(total_amount), 2) as total_revenue,
    
    -- Inventory movement proxy (not accounting COGS / average inventory).
    -- Higher values indicate more ordered units per average on-hand unit.
    ROUND(
        SUM(quantity) 
        / NULLIF(AVG(inventory_level), 0), 
        2
    ) as inventory_movement_proxy_ratio
FROM gold_sch.agg_base
GROUP BY 
    warehouse_id,
    warehouse_location,
    category;


select * from gold_sch.agg_inventory_turnover;


CREATE OR REPLACE DYNAMIC TABLE gold_sch.agg_shipment_delays
    TARGET_LAG = '1 minute'
    WAREHOUSE = flowbridge_pipeline_wh
    COMMENT = 'Shipment delay KPIs by carrier. DOWNSTREAM from AGG_BASE. Only shipped orders included.'
AS
SELECT
    carrier,
    customer_region,
    
    -- Shipment counts
    COUNT(order_id) as total_shipments,
    COUNT(CASE WHEN delay_days <= 0 THEN 1 END) as on_time_shipments,
    COUNT(CASE WHEN delay_days > 0 THEN 1 END) as delayed_shipments,
    
    -- On time rate
    ROUND(
        COUNT(CASE WHEN delay_days <= 0 AND shipment_id IS NOT NULL AND shipment_id <> 'UNKNOWN' THEN 1 END)
        / NULLIF(COUNT(CASE WHEN delay_days IS NOT NULL AND shipment_id IS NOT NULL AND shipment_id <> 'UNKNOWN' THEN 1 END), 0) * 100, 
        2
    ) as on_time_rate_pct,
    
    -- Delay metrics
    ROUND(AVG(delay_days), 2) as avg_delay_days,
    MAX(delay_days) as max_delay_days,
    
    -- Revenue
    ROUND(SUM(total_amount), 2) as total_revenue
FROM gold_sch.agg_base
-- Only include shipped orders
-- NULL SHIPMENT_ID = order not yet shipped
WHERE shipment_id IS NOT NULL
  AND shipment_id <> 'UNKNOWN'
GROUP BY 
    carrier,
    customer_region;


select * from gold_sch.agg_shipment_delays;


show dynamic tables in schema gold_sch;


SELECT 
    * 
FROM 
    TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY()) 
WHERE 
    schema_name = 'GOLD_SCH' 
ORDER BY 
    refresh_start_time DESC 
LIMIT 10;
