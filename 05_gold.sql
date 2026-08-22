use role sysadmin;
use warehouse flowbridge_pipeline_wh;
use database flowbridge_dev_db;
use schema gold_sch;

create or replace dynamic_table gold_sch.agg_base
lag = '1 minute'
warehouse = flowbridge_dev_db
comment = 'Base Dynamic Table'

grant usage, operate on warehouse flowbridge_pipeline_wh to role sysadmin;

create or replace dynamic table gold_sch.agg_base
    lag = '1 minute'
    warehouse = flowbridge_pipeline_wh
    comment = 'Base Dynamic Table'
as
select
-- Order fields ---------------------------------------------
    f.ORDER_ID,
    f.ORDER_DATE,
    f.ORDER_STATUS,
    f.PAYMENT_STATUS,
-- Measures -------------------------------------------------
    f.QUANTITY,
    f.UNIT_PRICE,
    f.TOTAL_AMOUNT,
    f.DELAY_DAYS,
    f.INVENTORY_LEVEL,
-- Pipeline metadata ----------------------------------------
    f.INGESTED_AT,
-- Customer fields ------------------------------------------
    c.CUSTOMER_ID,
    c.CUSTOMER_NAME,
    c.CUSTOMER_REGION,
    c.CUSTOMER_SEGMENT,
-- Product fields -------------------------------------------
    p.PRODUCT_ID,
    p.PRODUCT_NAME,
    p.CATEGORY,
-- Supplier fields ------------------------------------------
    sp.SUPPLIER_ID,
    sp.SUPPLIER_NAME,
    sp.SUPPLIER_COUNTRY,
    sp.LEAD_TIME_DAYS,
    sp.PERFORMANCE_SCORE,
-- Warehouse fields -----------------------------------------
    w.WAREHOUSE_ID,
    w.WAREHOUSE_LOCATION,
-- Shipment fields ------------------------------------------
-- LEFT JOIN - NULL if order not yet shipped
    sh.SHIPMENT_ID,
    sh.CARRIER,
    sh.SHIP_DATE,
    sh.ESTIMATED_DELIVERY
from silver_sch.fact_orders as f
JOIN      SILVER_SCH.DIM_CUSTOMER     c  ON f.CUSTOMER_SK  = c.CUSTOMER_SK
JOIN      SILVER_SCH.DIM_PRODUCT      p  ON f.PRODUCT_SK   = p.PRODUCT_SK
JOIN      SILVER_SCH.DIM_SUPPLIER     sp ON f.SUPPLIER_SK  = sp.SUPPLIER_SK
JOIN      SILVER_SCH.DIM_WAREHOUSE    w  ON f.WARHOUSE_SK  = w.WAREHOUSE_SK
LEFT JOIN SILVER_SCH.DIM_SHIPMENT     sh ON f.SHIPMENT_SK  = sh.SHIPMENT_SK;
