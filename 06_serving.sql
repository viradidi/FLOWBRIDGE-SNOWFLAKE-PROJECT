-- ----------------------------------------------------------------------------
-- ENVIRONMENT
-- ----------------------------------------------------------------------------
use role sysadmin;
use database flowbridge_dev_db;
use schema serving_sch;
use warehouse flowbridge_analytics_wh;

create or replace secure view serving_sch.vw_order_fulfillment
    comment = 'Secure View for GOLD_SCH.AGG_ORDER_FULFILLMENT (Dynamic Table)'
as
select
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
from GOLD_SCH.AGG_ORDER_FULFILLMENT;


create or replace secure view serving_sch.vw_supplier_performance
    comment = 'Secure View for GOLD_SCH.AGG_SUPPLIER_PERFORMANCE'
as
select
    supplier_id,
    supplier_name,
    supplier_country,
    total_orders,
    avg_performance_score,
    avg_lead_time_days,
    total_reveune,
    avg_order_value,
    on_time_orders,
    delayed_orders,
    on_time_rate_pct,
    avg_delay_days
from GOLD_SCH.AGG_SUPPLIER_PERFORMANCE;



create or replace secure view serving_sch.vw_inventory_turnover
    comment = 'Secure View for GOLD_SCH.AGG_INVENTORY_TURNOVER'
as
select
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
from GOLD_SCH.AGG_INVENTORY_TURNOVER;

create or replace secure view serving_sch.vw_shipment_delays
    comment = 'Secure View for GOLD_SCH.AGG_SHIPMENT_DELAYS'
as
select
    carrier,
    customer_region,
    total_shipments,
    on_time_shipments,
    delayed_shipments,
    on_time_rate_pct,
    avg_delay_days,
    max_delay_days,
    total_revenue
from GOLD_SCH.AGG_SHIPMENT_DELAYS;

show views in schema serving_sch;

select * from serving_sch.vw_shipment_delays;
