-- ENVIRONMENT
USE ROLE sysadmin;
USE DATABASE flowbridge_dev_db;
USE SCHEMA serving_sch;
USE WAREHOUSE flowbridge_analytics_wh;


CREATE OR REPLACE SECURE VIEW serving_sch.vw_order_fulfillment
    COMMENT = 'Secure View for GOLD_SCH.AGG_ORDER_FULFILLMENT (Dynamic Table)'
AS
SELECT
    customer_region,
    customer_segment,
    total_orders,
    delivered_orders,
    pending_orders,
    cancelled_orders,
    in_transit_orders,
    fulfillment_rate_pct,
    total_revenue,
    avg_order_value,
    paid_orders,
    payment_pending_orders,
    overdue_orders
FROM 
    GOLD_SCH.AGG_ORDER_FULFILLMENT;


CREATE OR REPLACE SECURE VIEW serving_sch.vw_supplier_performance
    COMMENT = 'Secure View for GOLD_SCH.AGG_SUPPLIER_PERFORMANCE'
AS
SELECT
    supplier_id,
    supplier_name,
    supplier_country,
    total_orders,
    avg_performance_score,
    avg_lead_time_days,
    total_revenue,
    avg_order_value,
    on_time_orders,
    delayed_orders,
    on_time_rate_pct,
    avg_delay_days
FROM 
    GOLD_SCH.AGG_SUPPLIER_PERFORMANCE;



CREATE OR REPLACE SECURE VIEW serving_sch.vw_inventory_turnover
    COMMENT = 'Secure View for GOLD_SCH.AGG_INVENTORY_TURNOVER'
AS
SELECT
    warehouse_id,
    warehouse_location,
    category,
    total_orders,
    total_quantity_ordered,
    avg_inventory_level,
    min_inventory_level,
    max_inventory_level,
    total_revenue,
    inventory_turnover_ratio
FROM 
    GOLD_SCH.AGG_INVENTORY_TURNOVER;



CREATE OR REPLACE SECURE VIEW serving_sch.vw_shipment_delays
    COMMENT = 'Secure View for GOLD_SCH.AGG_SHIPMENT_DELAYS'
AS
SELECT
    carrier,
    customer_region,
    total_shipments,
    on_time_shipments,
    delayed_shipments,
    on_time_rate_pct,
    avg_delay_days,
    max_delay_days,
    total_revenue
FROM 
    GOLD_SCH.AGG_SHIPMENT_DELAYS;


show views in schema serving_sch;

select * from serving_sch.vw_order_fulfillment;

select * from serving_sch.vw_supplier_performance;






