
import pandas as pd
import streamlit as st

from datetime import datetime, timedelta
from snowflake.snowpark.context import get_active_session


# ================================================================
# 1. PAGE CONFIGURATION
# ================================================================

st.set_page_config(
    page_title="FlowBridge Dashboard",
    page_icon="📦",
    layout="wide"
)

session = get_active_session()

DATABASE = "FLOWBRIDGE_PROD_DB"
TABLE = f"{DATABASE}.GOLD_SCH.AGG_BASE"

st.title("📦 FlowBridge Dashboard")
st.caption("Supply Chain Analytics and Performance Monitoring")


# ================================================================
# 2. REFRESH DATA
# ================================================================

if st.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()

st.divider()


# ================================================================
# 3. DATE FILTERS
# ================================================================

col_filter1, col_filter2 = st.columns(2)

with col_filter1:

    date_type = st.radio(
        "Filter By:",
        ["Order Date", "Ingestion Date"],
        horizontal=True
    )

with col_filter2:

    date_options = [
        "Today",
        "This Week",
        "Last 7 Days",
        "Last 14 Days",
        "Last 30 Days",
        "Last 90 Days",
        "YTD",
        "Last Year",
        "All Time"
    ]

    selected_period = st.selectbox(
        "Time Period:",
        date_options,
        index=2
    )


# ================================================================
# 4. CALCULATE DATE RANGE
# ================================================================

today = datetime.now().date()

start_date = None
end_date = None

if selected_period == "Today":

    start_date = today
    end_date = today

elif selected_period == "This Week":

    start_date = today - timedelta(days=today.weekday())
    end_date = today

elif selected_period == "Last 7 Days":

    start_date = today - timedelta(days=6)
    end_date = today

elif selected_period == "Last 14 Days":

    start_date = today - timedelta(days=13)
    end_date = today

elif selected_period == "Last 30 Days":

    start_date = today - timedelta(days=29)
    end_date = today

elif selected_period == "Last 90 Days":

    start_date = today - timedelta(days=89)
    end_date = today

elif selected_period == "YTD":

    start_date = today.replace(month=1, day=1)
    end_date = today

elif selected_period == "Last Year":

    start_date = today.replace(
        year=today.year - 1,
        month=1,
        day=1
    )

    end_date = today.replace(
        year=today.year - 1,
        month=12,
        day=31
    )


# ================================================================
# 5. STATUS AND REGION FILTERS
# ================================================================

all_statuses = [
    "PENDING",
    "PROCESSING",
    "SHIPPED",
    "IN TRANSIT",
    "DELIVERED",
    "CANCELLED"
]

all_regions = [
    "NORTH AMERICA",
    "EUROPE",
    "ASIA PACIFIC"
]


# Map dashboard labels to actual database values

status_mapping = {
    "PENDING": "PENDING",
    "PROCESSING": "PROCESSING",
    "SHIPPED": "SHIPPED",
    "IN TRANSIT": "IN_TRANSIT",
    "DELIVERED": "DELIVERED",
    "CANCELLED": "CANCELLED"
}


col_filter3, col_filter4 = st.columns(2)


# ---------------- ORDER STATUS ----------------

with col_filter3:

    status_box = st.selectbox(
        "Order Status:",
        ["ALL"] + all_statuses,
        index=0,
        key="status_mode"
    )

    if status_box == "ALL":

        selected_statuses = all_statuses

    else:

        extra_statuses = st.multiselect(
            "Add More Statuses:",
            [
                s for s in all_statuses
                if s != status_box
            ],
            key="extra_status"
        )

        selected_statuses = [
            status_box
        ] + extra_statuses


# ---------------- CUSTOMER REGION ----------------

with col_filter4:

    region_box = st.selectbox(
        "Customer Region:",
        ["ALL"] + all_regions,
        index=0,
        key="region_mode"
    )

    if region_box == "ALL":

        selected_regions = all_regions

    else:

        extra_regions = st.multiselect(
            "Add More Regions:",
            [
                r for r in all_regions
                if r != region_box
            ],
            key="extra_region"
        )

        selected_regions = [
            region_box
        ] + extra_regions


# ================================================================
# 6. BUILD SAFE SQL FILTERS
# ================================================================

# Select the actual date column based on the radio selection.

date_column_mapping = {
    "Order Date": "ORDER_DATE",
    "Ingestion Date": "INGESTION_DATE"
}

date_column = date_column_mapping[date_type]


def build_filters(
    date_column,
    start_date,
    end_date,
    status_box,
    selected_statuses,
    region_box,
    selected_regions
):

    conditions = []
    params = []

    # DATE FILTER

    if start_date is not None and end_date is not None:

        conditions.append(
            f"{date_column} >= ? AND {date_column} < ?"
        )

        params.extend([
            start_date,
            end_date + timedelta(days=1)
        ])

    # STATUS FILTER

    if status_box != "ALL" and selected_statuses:

        database_statuses = [
            status_mapping[s]
            for s in selected_statuses
        ]

        placeholders = ", ".join(
            ["?"] * len(database_statuses)
        )

        conditions.append(
            f"ORDER_STATUS IN ({placeholders})"
        )

        params.extend(database_statuses)

    # REGION FILTER

    if region_box != "ALL" and selected_regions:

        placeholders = ", ".join(
            ["?"] * len(selected_regions)
        )

        conditions.append(
            f"CUSTOMER_REGION IN ({placeholders})"
        )

        params.extend(selected_regions)

    # COMBINE FILTERS

    where_clause = ""

    if conditions:

        where_clause = "WHERE " + " AND ".join(conditions)

    return where_clause, tuple(params)


combined_filter, filter_params = build_filters(
    date_column,
    start_date,
    end_date,
    status_box,
    selected_statuses,
    region_box,
    selected_regions
)


# ================================================================
# 7. DATA LOADING FUNCTIONS
# ================================================================

@st.cache_data(ttl=60)
def load_fulfillment(where_clause, params):

    query = f"""
        SELECT
            CUSTOMER_REGION,
            CUSTOMER_SEGMENT,

            COUNT(*) AS TOTAL_ORDERS,

            COUNT(
                CASE WHEN ORDER_STATUS = 'DELIVERED'
                THEN 1 END
            ) AS DELIVERED_ORDERS,

            COUNT(
                CASE WHEN ORDER_STATUS = 'PENDING'
                THEN 1 END
            ) AS PENDING_ORDERS,

            COUNT(
                CASE WHEN ORDER_STATUS = 'CANCELLED'
                THEN 1 END
            ) AS CANCELLED_ORDERS,

            COUNT(
                CASE WHEN ORDER_STATUS = 'IN_TRANSIT'
                THEN 1 END
            ) AS IN_TRANSIT_ORDERS,

            ROUND(
                COUNT(
                    CASE WHEN ORDER_STATUS = 'DELIVERED'
                    THEN 1 END
                ) * 100.0 / NULLIF(COUNT(*), 0),
                2
            ) AS FULFILLMENT_RATE_PCT,

            SUM(TOTAL_AMOUNT) AS TOTAL_REVENUE,

            AVG(TOTAL_AMOUNT) AS AVG_ORDER_VALUE

        FROM {TABLE}

        {where_clause}

        GROUP BY
            CUSTOMER_REGION,
            CUSTOMER_SEGMENT
    """

    return session.sql(
        query,
        params=list(params)
    ).to_pandas()


@st.cache_data(ttl=60)
def load_suppliers(where_clause, params):

    query = f"""
        SELECT
            SUPPLIER_ID,
            SUPPLIER_NAME,
            SUPPLIER_COUNTRY,

            COUNT(*) AS TOTAL_ORDERS,

            AVG(PERFORMANCE_SCORE)
                AS AVG_PERFORMANCE_SCORE,

            AVG(LEAD_TIME_DAYS)
                AS AVG_LEAD_TIME_DAYS,

            SUM(TOTAL_AMOUNT) AS TOTAL_REVENUE,

            COUNT(
                CASE WHEN DELAY_DAYS <= 0
                THEN 1 END
            ) AS ON_TIME_ORDERS,

            COUNT(
                CASE WHEN DELAY_DAYS > 0
                THEN 1 END
            ) AS DELAYED_ORDERS,

            ROUND(
                COUNT(
                    CASE WHEN DELAY_DAYS <= 0
                    THEN 1 END
                ) * 100.0 / NULLIF(COUNT(*), 0),
                2
            ) AS ON_TIME_RATE_PCT,

            AVG(DELAY_DAYS) AS AVG_DELAY_DAYS

        FROM {TABLE}

        {where_clause}

        GROUP BY
            SUPPLIER_ID,
            SUPPLIER_NAME,
            SUPPLIER_COUNTRY
    """

    return session.sql(
        query,
        params=list(params)
    ).to_pandas()


@st.cache_data(ttl=60)
def load_inventory(where_clause, params):

    query = f"""
        SELECT
            WAREHOUSE_ID,
            WAREHOUSE_LOCATION,
            CATEGORY,

            COUNT(*) AS TOTAL_ORDERS,

            SUM(QUANTITY) AS TOTAL_QUANTITY_ORDERED,

            AVG(INVENTORY_LEVEL) AS AVG_INVENTORY_LEVEL,

            SUM(TOTAL_AMOUNT) AS TOTAL_REVENUE,

            ROUND(
                CASE
                    WHEN AVG(INVENTORY_LEVEL) > 0
                    THEN SUM(QUANTITY) / AVG(INVENTORY_LEVEL)
                    ELSE 0
                END,
                2
            ) AS INVENTORY_TURNOVER_RATIO

        FROM {TABLE}

        {where_clause}

        GROUP BY
            WAREHOUSE_ID,
            WAREHOUSE_LOCATION,
            CATEGORY
    """

    return session.sql(
        query,
        params=list(params)
    ).to_pandas()


@st.cache_data(ttl=60)
def load_shipments(where_clause, params):

    # Add carrier conditions without breaking existing filters.

    carrier_condition = """
        CARRIER IS NOT NULL
        AND CARRIER != 'UNKNOWN'
    """

    if where_clause:

        shipment_filter = (
            f"{where_clause} AND {carrier_condition}"
        )

    else:

        shipment_filter = f"WHERE {carrier_condition}"

    query = f"""
        SELECT
            CARRIER,
            CUSTOMER_REGION,

            COUNT(*) AS TOTAL_SHIPMENTS,

            COUNT(
                CASE WHEN DELAY_DAYS <= 0
                THEN 1 END
            ) AS ON_TIME_SHIPMENTS,

            COUNT(
                CASE WHEN DELAY_DAYS > 0
                THEN 1 END
            ) AS DELAYED_SHIPMENTS,

            ROUND(
                COUNT(
                    CASE WHEN DELAY_DAYS <= 0
                    THEN 1 END
                ) * 100.0 / NULLIF(COUNT(*), 0),
                2
            ) AS ON_TIME_RATE_PCT,

            AVG(DELAY_DAYS) AS AVG_DELAY_DAYS,

            SUM(TOTAL_AMOUNT) AS TOTAL_REVENUE

        FROM {TABLE}

        {shipment_filter}

        GROUP BY
            CARRIER,
            CUSTOMER_REGION
    """

    return session.sql(
        query,
        params=list(params)
    ).to_pandas()


@st.cache_data(ttl=60)
def load_kpis(where_clause, params):

    query = f"""
        SELECT

            COUNT(*) AS TOTAL_ORDERS,

            SUM(TOTAL_AMOUNT) AS TOTAL_REVENUE,

            ROUND(
                COUNT(
                    CASE WHEN ORDER_STATUS = 'DELIVERED'
                    THEN 1 END
                ) * 100.0 / NULLIF(COUNT(*), 0),
                1
            ) AS FULFILLMENT_RATE,

            ROUND(
                COUNT(
                    CASE WHEN DELAY_DAYS <= 0
                    THEN 1 END
                ) * 100.0 / NULLIF(COUNT(*), 0),
                1
            ) AS ON_TIME_RATE

        FROM {TABLE}

        {where_clause}
    """

    return session.sql(
        query,
        params=list(params)
    ).to_pandas()


@st.cache_data(ttl=60)
def load_time_series(where_clause, params):

    query = f"""
        SELECT

            DATE_TRUNC('DAY', ORDER_DATE) AS ORDER_DAY,

            COUNT(ORDER_ID) AS DAILY_ORDERS,

            SUM(TOTAL_AMOUNT) AS DAILY_REVENUE,

            AVG(DELAY_DAYS) AS AVG_DELAY

        FROM {TABLE}

        {where_clause}

        GROUP BY 1

        ORDER BY 1
    """

    return session.sql(
        query,
        params=list(params)
    ).to_pandas()


# ================================================================
# 8. DATA LOADING EXECUTION PIPELINE
# ================================================================

try:

    df_f = load_fulfillment(
        combined_filter,
        filter_params
    )

    df_s = load_suppliers(
        combined_filter,
        filter_params
    )

    df_i = load_inventory(
        combined_filter,
        filter_params
    )

    df_d = load_shipments(
        combined_filter,
        filter_params
    )

    df_ts = load_time_series(
        combined_filter,
        filter_params
    )

    df_kpi = load_kpis(
        combined_filter,
        filter_params
    )

except Exception as e:

    st.error("Unable to load dashboard data.")

    st.exception(e)

    st.stop()


# ================================================================
# 9. EMPTY DATA VALIDATION
# ================================================================

if (
    df_kpi.empty
    or pd.isna(df_kpi["TOTAL_ORDERS"].iloc[0])
    or df_kpi["TOTAL_ORDERS"].iloc[0] == 0
):

    st.warning(
        "No data available for the selected filters."
    )

    st.stop()


# ================================================================
# 10. KPI DASHBOARD
# ================================================================

total_orders = df_kpi["TOTAL_ORDERS"].iloc[0]

total_revenue = df_kpi["TOTAL_REVENUE"].iloc[0]

fulfillment_rate = df_kpi["FULFILLMENT_RATE"].iloc[0]

on_time_rate = df_kpi["ON_TIME_RATE"].iloc[0]


# Handle NULL values safely.

total_revenue = (
    0 if pd.isna(total_revenue)
    else total_revenue
)

fulfillment_rate = (
    0 if pd.isna(fulfillment_rate)
    else fulfillment_rate
)

on_time_rate = (
    0 if pd.isna(on_time_rate)
    else on_time_rate
)


st.subheader("📊 Overall Performance")

col1, col2, col3, col4 = st.columns(4)

col1.metric(
    "Total Orders",
    f"{int(total_orders):,}"
)

col2.metric(
    "Total Revenue",
    f"${total_revenue:,.2f}"
)

col3.metric(
    "Fulfillment Rate",
    f"{fulfillment_rate}%"
)

col4.metric(
    "On-Time Rate",
    f"{on_time_rate}%"
)

st.divider()


# ================================================================
# 11. ORDER AND REVENUE TRENDS
# ================================================================

st.subheader("📈 Order and Revenue Trends")

if not df_ts.empty:

    chart_col1, chart_col2 = st.columns(2)

    with chart_col1:

        st.markdown("**Daily Orders**")

        orders_chart = df_ts.set_index(
            "ORDER_DAY"
        )[["DAILY_ORDERS"]]

        st.line_chart(orders_chart)

    with chart_col2:

        st.markdown("**Daily Revenue**")

        revenue_chart = df_ts.set_index(
            "ORDER_DAY"
        )[["DAILY_REVENUE"]]

        st.line_chart(revenue_chart)

else:

    st.info("No time-series data available.")


st.divider()


# ================================================================
# 12. FULFILLMENT ANALYTICS
# ================================================================

st.subheader("📦 Fulfillment Analytics")

if not df_f.empty:

    fulfillment_col1, fulfillment_col2 = st.columns(2)

    with fulfillment_col1:

        st.markdown("**Orders by Customer Region**")

        region_chart = (
            df_f.groupby("CUSTOMER_REGION")[
                "TOTAL_ORDERS"
            ]
            .sum()
            .to_frame()
        )

        st.bar_chart(region_chart)

    with fulfillment_col2:

        st.markdown("**Fulfillment by Customer Segment**")

        segment_chart = (
            df_f.groupby("CUSTOMER_SEGMENT")[
                "FULFILLMENT_RATE_PCT"
            ]
            .mean()
            .to_frame()
        )

        st.bar_chart(segment_chart)

    with st.expander("View Fulfillment Details"):

        st.dataframe(
            df_f,
            use_container_width=True
        )

else:

    st.info("No fulfillment data available.")


st.divider()


# ================================================================
# 13. SUPPLIER PERFORMANCE
# ================================================================

st.subheader("🏭 Supplier Performance")

if not df_s.empty:

    supplier_col1, supplier_col2 = st.columns(2)

    with supplier_col1:

        st.markdown("**Supplier Revenue**")

        supplier_revenue = (
            df_s.groupby("SUPPLIER_NAME")[
                "TOTAL_REVENUE"
            ]
            .sum()
            .sort_values(ascending=False)
            .head(10)
            .to_frame()
        )

        st.bar_chart(supplier_revenue)

    with supplier_col2:

        st.markdown("**Supplier On-Time Performance**")

        supplier_performance = (
            df_s.groupby("SUPPLIER_NAME")[
                "ON_TIME_RATE_PCT"
            ]
            .mean()
            .sort_values(ascending=False)
            .head(10)
            .to_frame()
        )

        st.bar_chart(supplier_performance)

    with st.expander("View Supplier Details"):

        st.dataframe(
            df_s,
            use_container_width=True
        )

else:

    st.info("No supplier data available.")


st.divider()


# ================================================================
# 14. INVENTORY ANALYTICS
# ================================================================

st.subheader("🏬 Inventory Analytics")

if not df_i.empty:

    inventory_col1, inventory_col2 = st.columns(2)

    with inventory_col1:

        st.markdown("**Quantity Ordered by Category**")

        inventory_chart = (
            df_i.groupby("CATEGORY")[
                "TOTAL_QUANTITY_ORDERED"
            ]
            .sum()
            .to_frame()
        )

        st.bar_chart(inventory_chart)

    with inventory_col2:

        st.markdown("**Inventory Turnover by Category**")

        turnover_chart = (
            df_i.groupby("CATEGORY")[
                "INVENTORY_TURNOVER_RATIO"
            ]
            .mean()
            .to_frame()
        )

        st.bar_chart(turnover_chart)

    with st.expander("View Inventory Details"):

        st.dataframe(
            df_i,
            use_container_width=True
        )

else:

    st.info("No inventory data available.")


st.divider()


# ================================================================
# 15. SHIPMENT ANALYTICS
# ================================================================

st.subheader("🚚 Shipment Analytics")

if not df_d.empty:

    shipment_col1, shipment_col2 = st.columns(2)

    with shipment_col1:

        st.markdown("**Shipments by Carrier**")

        carrier_chart = (
            df_d.groupby("CARRIER")[
                "TOTAL_SHIPMENTS"
            ]
            .sum()
            .to_frame()
        )

        st.bar_chart(carrier_chart)

    with shipment_col2:

        st.markdown("**Carrier On-Time Rates**")

        carrier_performance = (
            df_d.groupby("CARRIER")[
                "ON_TIME_RATE_PCT"
            ]
            .mean()
            .to_frame()
        )

        st.bar_chart(carrier_performance)

    with st.expander("View Shipment Details"):

        st.dataframe(
            df_d,
            use_container_width=True
        )

else:

    st.info("No shipment data available.")


# ================================================================
# 16. DASHBOARD FOOTER
# ================================================================

st.divider()

st.caption(
    "FlowBridge Supply Chain Analytics | "
    "Powered by Snowflake and Streamlit | "
    "Data cache: 60 seconds"
)
