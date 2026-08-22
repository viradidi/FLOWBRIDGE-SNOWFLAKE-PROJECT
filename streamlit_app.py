import pandas as pd
import streamlit as st
from datetime import timedelta
from snowflake.snowpark.context import get_active_session


# ============================================================
# PAGE CONFIGURATION
# ============================================================

st.set_page_config(
    page_title="FlowBridge Dashboard",
    page_icon="📦",
    layout="wide"
)


# ============================================================
# SNOWFLAKE SESSION
# ============================================================

session = get_active_session()

database = "FLOWBRIDGE_PROD_DB"
source_table = f"{database}.GOLD_SCH.AGG_BASE"


# ============================================================
# PAGE TITLE
# ============================================================

st.title("FlowBridge Dashboard")

st.caption(
    "Order Fulfillment, Supplier, Inventory & Shipment Analytics"
)


# ============================================================
# REFRESH DATA
# ============================================================

if st.button("🔄 Refresh Data"):

    st.cache_data.clear()
    st.rerun()


st.divider()


# ============================================================
# FILTER SECTION
# ============================================================

st.subheader("Dashboard Filters")


# ============================================================
# DATE FILTERS
# ============================================================

col_filter1, col_filter2 = st.columns(2)


# ------------------------------------------------------------
# Date Type
# ------------------------------------------------------------

with col_filter1:

    date_type = st.radio(
        "Filter By:",
        ["Order Date", "Ingestion Date"],
        horizontal=True
    )

    # Actual columns from AGG_BASE:
    #
    # Order Date     -> ORDER_DATE
    # Ingestion Date -> INGESTED_AT

    if date_type == "Order Date":

        date_column = "ORDER_DATE"

    else:

        date_column = "INGESTED_AT"


# ------------------------------------------------------------
# Time Period
# ------------------------------------------------------------

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
        index=2,
        key="selected_period"
    )


# ============================================================
# DATE RANGE LOGIC
# ============================================================

today = session.sql(
    "SELECT CURRENT_DATE()"
).collect()[0][0]


if selected_period == "Today":

    start_date = today
    end_date = today + timedelta(days=1)


elif selected_period == "This Week":

    start_date = (
        today - timedelta(days=today.weekday())
    )

    end_date = today + timedelta(days=1)


elif selected_period == "Last 7 Days":

    start_date = today - timedelta(days=6)
    end_date = today + timedelta(days=1)


elif selected_period == "Last 14 Days":

    start_date = today - timedelta(days=13)
    end_date = today + timedelta(days=1)


elif selected_period == "Last 30 Days":

    start_date = today - timedelta(days=29)
    end_date = today + timedelta(days=1)


elif selected_period == "Last 90 Days":

    start_date = today - timedelta(days=89)
    end_date = today + timedelta(days=1)


elif selected_period == "YTD":

    start_date = today.replace(
        month=1,
        day=1
    )

    end_date = today + timedelta(days=1)


elif selected_period == "Last Year":

    start_date = today - timedelta(days=365)
    end_date = today + timedelta(days=1)


else:

    start_date = None
    end_date = None


# ============================================================
# MASTER DIMENSIONS
# ============================================================

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


# ============================================================
# STATUS AND REGION FILTERS
# ============================================================

col_filter3, col_filter4 = st.columns(2)


# ============================================================
# ORDER STATUS
# ============================================================

with col_filter3:

    status_mode = st.selectbox(
        "Order Status:",
        ["ALL"] + all_statuses,
        index=0,
        key="status_mode"
    )


    if status_mode == "ALL":

        selected_statuses = all_statuses


    else:

        extra_statuses = st.multiselect(
            "Add More Statuses:",
            [
                status
                for status in all_statuses
                if status != status_mode
            ],
            key="extra_status"
        )

        selected_statuses = [
            status_mode
        ] + extra_statuses


# ============================================================
# CUSTOMER REGION
# ============================================================

with col_filter4:

    region_mode = st.selectbox(
        "Customer Region:",
        ["ALL"] + all_regions,
        index=0,
        key="region_mode"
    )


    if region_mode == "ALL":

        selected_regions = all_regions


    else:

        extra_regions = st.multiselect(
            "Add More Regions:",
            [
                region
                for region in all_regions
                if region != region_mode
            ],
            key="extra_region"
        )

        selected_regions = [
            region_mode
        ] + extra_regions


# ============================================================
# BUILD SQL FILTERS
# ============================================================

filters = []


# ------------------------------------------------------------
# Date Filter
# ------------------------------------------------------------

if start_date is not None and end_date is not None:

    filters.append(
        f"{date_column} >= '{start_date}' "
        f"AND {date_column} < '{end_date}'"
    )


# ------------------------------------------------------------
# Status Filter
# ------------------------------------------------------------

if status_mode != "ALL":

    status_list = ", ".join(
        f"'{status}'"
        for status in selected_statuses
    )

    filters.append(
        f"ORDER_STATUS IN ({status_list})"
    )


# ------------------------------------------------------------
# Region Filter
# ------------------------------------------------------------

if region_mode != "ALL":

    region_list = ", ".join(
        f"'{region}'"
        for region in selected_regions
    )

    filters.append(
        f"CUSTOMER_REGION IN ({region_list})"
    )


# ============================================================
# COMBINE FILTERS
# ============================================================

if filters:

    combined_filter = (
        "WHERE "
        + " AND ".join(filters)
    )

else:

    combined_filter = ""


# ============================================================
# ACTIVE FILTER SUMMARY
# ============================================================

with st.expander("🔎 Active Filters", expanded=False):

    filter_col1, filter_col2, filter_col3 = st.columns(3)


    with filter_col1:

        st.write("**Date Column**")

        st.write(date_column)


    with filter_col2:

        st.write("**Time Period**")

        st.write(selected_period)


    with filter_col3:

        st.write("**Date Range**")

        if start_date is not None:

            display_end_date = (
                end_date - timedelta(days=1)
            )

            st.write(
                f"{start_date} → {display_end_date}"
            )

        else:

            st.write("All Time")


    st.write("**Order Status:**")

    if status_mode == "ALL":

        st.write("All Statuses")

    else:

        st.write(
            ", ".join(selected_statuses)
        )


    st.write("**Customer Region:**")

    if region_mode == "ALL":

        st.write("All Regions")

    else:

        st.write(
            ", ".join(selected_regions)
        )


# ============================================================
# GENERATED SQL FILTER
# ============================================================

with st.expander(
    "🧪 Generated SQL Filter",
    expanded=False
):

    if combined_filter:

        st.code(
            combined_filter,
            language="sql"
        )

    else:

        st.info(
            "No filters selected. All records will be included."
        )


st.divider()


# ============================================================
# STEP 1: CACHED DATA FUNCTIONS
# ============================================================


# ============================================================
# FULFILLMENT
# ============================================================

@st.cache_data(ttl=60)
def load_fulfillment(combined_filter):

    query = f"""
        SELECT
            CUSTOMER_REGION,
            CUSTOMER_SEGMENT,

            COUNT(*) AS TOTAL_ORDERS,

            COUNT(
                CASE
                    WHEN ORDER_STATUS = 'DELIVERED'
                    THEN 1
                END
            ) AS DELIVERED_ORDERS,

            COUNT(
                CASE
                    WHEN ORDER_STATUS = 'PENDING'
                    THEN 1
                END
            ) AS PENDING_ORDERS,

            COUNT(
                CASE
                    WHEN ORDER_STATUS = 'CANCELLED'
                    THEN 1
                END
            ) AS CANCELLED_ORDERS,

            COUNT(
                CASE
                    WHEN ORDER_STATUS = 'IN TRANSIT'
                    THEN 1
                END
            ) AS IN_TRANSIT_ORDERS,

            ROUND(
                COUNT(
                    CASE
                        WHEN ORDER_STATUS = 'DELIVERED'
                        THEN 1
                    END
                ) * 100.0
                / NULLIF(COUNT(*), 0),
                2
            ) AS FULFILLMENT_RATE_PCT,

            SUM(TOTAL_AMOUNT) AS TOTAL_REVENUE,

            AVG(TOTAL_AMOUNT) AS AVG_ORDER_VALUE

        FROM {source_table}

        {combined_filter}

        GROUP BY
            CUSTOMER_REGION,
            CUSTOMER_SEGMENT

        ORDER BY
            TOTAL_ORDERS DESC
    """

    return session.sql(query).to_pandas()


# ============================================================
# SUPPLIERS
# ============================================================

@st.cache_data(ttl=60)
def load_suppliers(combined_filter):

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

            SUM(TOTAL_AMOUNT)
                AS TOTAL_REVENUE,

            COUNT(
                CASE
                    WHEN DELAY_DAYS <= 0
                    THEN 1
                END
            ) AS ON_TIME_ORDERS,

            COUNT(
                CASE
                    WHEN DELAY_DAYS > 0
                    THEN 1
                END
            ) AS DELAYED_ORDERS,

            ROUND(
                COUNT(
                    CASE
                        WHEN DELAY_DAYS <= 0
                        THEN 1
                    END
                ) * 100.0
                / NULLIF(COUNT(*), 0),
                2
            ) AS ON_TIME_RATE_PCT,

            AVG(DELAY_DAYS)
                AS AVG_DELAY_DAYS

        FROM {source_table}

        {combined_filter}

        GROUP BY
            SUPPLIER_ID,
            SUPPLIER_NAME,
            SUPPLIER_COUNTRY

        ORDER BY
            TOTAL_REVENUE DESC
    """

    return session.sql(query).to_pandas()


# ============================================================
# INVENTORY
# ============================================================

@st.cache_data(ttl=60)
def load_inventory(combined_filter):

    query = f"""
        SELECT
            WAREHOUSE_ID,
            WAREHOUSE_LOCATION,
            CATEGORY,

            COUNT(*) AS TOTAL_ORDERS,

            SUM(QUANTITY)
                AS TOTAL_QUANTITY_ORDERED,

            AVG(INVENTORY_LEVEL)
                AS AVG_INVENTORY_LEVEL,

            SUM(TOTAL_AMOUNT)
                AS TOTAL_REVENUE,

            ROUND(
                CASE
                    WHEN AVG(INVENTORY_LEVEL) > 0
                    THEN
                        SUM(QUANTITY)
                        / AVG(INVENTORY_LEVEL)
                    ELSE 0
                END,
                2
            ) AS INVENTORY_TURNOVER_RATIO

        FROM {source_table}

        {combined_filter}

        GROUP BY
            WAREHOUSE_ID,
            WAREHOUSE_LOCATION,
            CATEGORY

        ORDER BY
            TOTAL_REVENUE DESC
    """

    return session.sql(query).to_pandas()


# ============================================================
# SHIPMENTS
# ============================================================

@st.cache_data(ttl=60)
def load_shipments(combined_filter):

    if combined_filter.strip():

        carrier_filter = """
            AND CARRIER IS NOT NULL
            AND CARRIER != 'UNKNOWN'
        """

    else:

        carrier_filter = """
            WHERE CARRIER IS NOT NULL
              AND CARRIER != 'UNKNOWN'
        """


    query = f"""
        SELECT
            CARRIER,
            CUSTOMER_REGION,

            COUNT(*) AS TOTAL_SHIPMENTS,

            COUNT(
                CASE
                    WHEN DELAY_DAYS <= 0
                    THEN 1
                END
            ) AS ON_TIME_SHIPMENTS,

            COUNT(
                CASE
                    WHEN DELAY_DAYS > 0
                    THEN 1
                END
            ) AS DELAYED_SHIPMENTS,

            ROUND(
                COUNT(
                    CASE
                        WHEN DELAY_DAYS <= 0
                        THEN 1
                    END
                ) * 100.0
                / NULLIF(COUNT(*), 0),
                2
            ) AS ON_TIME_RATE_PCT,

            AVG(DELAY_DAYS)
                AS AVG_DELAY_DAYS,

            SUM(TOTAL_AMOUNT)
                AS TOTAL_REVENUE

        FROM {source_table}

        {combined_filter}

        {carrier_filter}

        GROUP BY
            CARRIER,
            CUSTOMER_REGION

        ORDER BY
            TOTAL_SHIPMENTS DESC
    """

    return session.sql(query).to_pandas()


# ============================================================
# KPIs
# ============================================================

@st.cache_data(ttl=60)
def load_kpis(combined_filter):

    query = f"""
        SELECT

            COUNT(*) AS TOTAL_ORDERS,

            COALESCE(
                SUM(TOTAL_AMOUNT),
                0
            ) AS TOTAL_REVENUE,

            ROUND(
                COUNT(
                    CASE
                        WHEN ORDER_STATUS = 'DELIVERED'
                        THEN 1
                    END
                ) * 100.0
                / NULLIF(COUNT(*), 0),
                1
            ) AS FULFILLMENT_RATE,

            ROUND(
                COUNT(
                    CASE
                        WHEN DELAY_DAYS <= 0
                        THEN 1
                    END
                ) * 100.0
                / NULLIF(COUNT(*), 0),
                1
            ) AS ON_TIME_RATE

        FROM {source_table}

        {combined_filter}
    """

    return session.sql(query).to_pandas()


# ============================================================
# TIME SERIES
# ============================================================

@st.cache_data(ttl=60)
def load_time_series(combined_filter):

    query = f"""
        SELECT

            DATE_TRUNC(
                'DAY',
                ORDER_DATE
            ) AS ORDER_DAY,

            COUNT(ORDER_ID)
                AS DAILY_ORDERS,

            SUM(TOTAL_AMOUNT)
                AS DAILY_REVENUE,

            AVG(DELAY_DAYS)
                AS AVG_DELAY

        FROM {source_table}

        {combined_filter}

        GROUP BY 1

        ORDER BY 1
    """

    return session.sql(query).to_pandas()


# ============================================================
# STEP 2: EXECUTE DATA PIPELINE
# ============================================================

try:

    df_f = load_fulfillment(
        combined_filter
    )

    df_s = load_suppliers(
        combined_filter
    )

    df_i = load_inventory(
        combined_filter
    )

    df_d = load_shipments(
        combined_filter
    )

    df_ts = load_time_series(
        combined_filter
    )

    df_kpi = load_kpis(
        combined_filter
    )


except Exception as e:

    st.error(
        "Unable to load dashboard data from Snowflake."
    )

    with st.expander(
        "Show Snowflake Error"
    ):

        st.code(
            str(e),
            language="text"
        )

    st.stop()


# ============================================================
# STEP 3: SAFETY CHECK
# ============================================================

if df_kpi.empty:

    st.warning(
        "No data available for the selected filters."
    )

    st.stop()


total_orders = df_kpi[
    "TOTAL_ORDERS"
].iloc[0]


if pd.isna(total_orders):

    st.warning(
        "No data available for the selected filters."
    )

    st.stop()


if int(total_orders) == 0:

    st.warning(
        "No data available for the selected time period."
    )

    st.stop()


# ============================================================
# KPI VALUES
# ============================================================

total_orders = int(
    df_kpi["TOTAL_ORDERS"].iloc[0]
)


total_revenue = df_kpi[
    "TOTAL_REVENUE"
].iloc[0]


fulfillment_rate = df_kpi[
    "FULFILLMENT_RATE"
].iloc[0]


on_time_rate = df_kpi[
    "ON_TIME_RATE"
].iloc[0]


# ============================================================
# KPI DISPLAY
# ============================================================

st.subheader("Operational Performance")


col1, col2, col3, col4 = st.columns(4)


with col1:

    st.metric(
        "Total Orders",
        f"{total_orders:,}"
    )


with col2:

    st.metric(
        "Total Revenue",
        f"${total_revenue:,.0f}"
    )


with col3:

    st.metric(
        "Fulfillment Rate",
        f"{fulfillment_rate:.1f}%"
    )


with col4:

    st.metric(
        "On-Time Rate",
        f"{on_time_rate:.1f}%"
    )


st.divider()


# ============================================================
# MAIN ANALYTICS TABS
# ============================================================

tab1, tab2, tab3 = st.tabs(
    [
        "📈 Trends",
        "📊 Breakdown",
        "🏭 Suppliers"
    ]
)


# ============================================================
# TAB 1: OPERATIONAL TRENDS
# ============================================================

with tab1:

    st.subheader("Operational Trends")


    # --------------------------------------------------------
    # Daily Orders
    # --------------------------------------------------------

    st.subheader("Daily Orders")

    if not df_ts.empty:

        st.line_chart(
            df_ts,
            x="ORDER_DAY",
            y="DAILY_ORDERS",
            use_container_width=True
        )

    else:

        st.info(
            "No daily order data available."
        )


    # --------------------------------------------------------
    # Daily Revenue
    # --------------------------------------------------------

    st.subheader("Daily Revenue")

    if not df_ts.empty:

        st.area_chart(
            df_ts,
            x="ORDER_DAY",
            y="DAILY_REVENUE",
            use_container_width=True
        )

    else:

        st.info(
            "No daily revenue data available."
        )


    # --------------------------------------------------------
    # Average Delay
    # --------------------------------------------------------

    st.subheader(
        "Average Delay Days (Daily)"
    )

    if not df_ts.empty:

        st.line_chart(
            df_ts,
            x="ORDER_DAY",
            y="AVG_DELAY",
            use_container_width=True
        )

    else:

        st.info(
            "No delay data available."
        )


# ============================================================
# TAB 2: CATEGORICAL BREAKDOWNS
# ============================================================

with tab2:

    st.subheader(
        "Operational Breakdowns"
    )


    # --------------------------------------------------------
    # Orders by Region
    # --------------------------------------------------------

    col_a, col_b = st.columns(2)


    with col_a:

        st.subheader("Orders by Region")

        if not df_f.empty:

            region_data = (
                df_f
                .groupby(
                    "CUSTOMER_REGION"
                )[
                    "TOTAL_ORDERS"
                ]
                .sum()
                .reset_index()
                .sort_values(
                    "TOTAL_ORDERS",
                    ascending=False
                )
            )

            st.bar_chart(
                region_data,
                x="CUSTOMER_REGION",
                y="TOTAL_ORDERS",
                use_container_width=True
            )

        else:

            st.info(
                "No regional data available."
            )


    # --------------------------------------------------------
    # Revenue by Carrier
    # --------------------------------------------------------

    with col_b:

        st.subheader(
            "Revenue by Carrier"
        )

        if not df_d.empty:

            carrier_revenue = (
                df_d
                .groupby(
                    "CARRIER"
                )[
                    "TOTAL_REVENUE"
                ]
                .sum()
                .reset_index()
                .sort_values(
                    "TOTAL_REVENUE",
                    ascending=False
                )
            )

            st.bar_chart(
                carrier_revenue,
                x="CARRIER",
                y="TOTAL_REVENUE",
                use_container_width=True
            )

        else:

            st.info(
                "No carrier data available."
            )


    # --------------------------------------------------------
    # On-Time vs Delayed
    # --------------------------------------------------------

    st.subheader(
        "On-Time vs Delayed Shipments by Carrier"
    )

    if not df_d.empty:

        shipment_data = (
            df_d
            .groupby("CARRIER")[
                [
                    "ON_TIME_SHIPMENTS",
                    "DELAYED_SHIPMENTS"
                ]
            ]
            .sum()
            .reset_index()
        )

        st.bar_chart(
            shipment_data,
            x="CARRIER",
            y=[
                "ON_TIME_SHIPMENTS",
                "DELAYED_SHIPMENTS"
            ],
            use_container_width=True
        )

    else:

        st.info(
            "No shipment data available."
        )


    # --------------------------------------------------------
    # Inventory Turnover
    # --------------------------------------------------------

    st.subheader(
        "Inventory Turnover by Category"
    )

    if not df_i.empty:

        inventory_data = (
            df_i
            .groupby("CATEGORY")[
                "INVENTORY_TURNOVER_RATIO"
            ]
            .mean()
            .reset_index()
            .sort_values(
                "INVENTORY_TURNOVER_RATIO",
                ascending=False
            )
        )

        st.bar_chart(
            inventory_data,
            x="CATEGORY",
            y="INVENTORY_TURNOVER_RATIO",
            use_container_width=True
        )

    else:

        st.info(
            "No inventory data available."
        )


# ============================================================
# TAB 3: SUPPLIER SCORECARDS
# ============================================================

with tab3:

    st.subheader(
        "Supplier Scorecard"
    )


    # --------------------------------------------------------
    # Supplier Table
    # --------------------------------------------------------

    if not df_s.empty:

        supplier_display = df_s[
            [
                "SUPPLIER_NAME",
                "SUPPLIER_COUNTRY",
                "TOTAL_ORDERS",
                "AVG_PERFORMANCE_SCORE",
                "ON_TIME_RATE_PCT",
                "AVG_LEAD_TIME_DAYS"
            ]
        ].copy()


        supplier_display = (
            supplier_display
            .sort_values(
                "ON_TIME_RATE_PCT",
                ascending=False
            )
        )


        st.dataframe(
            supplier_display,
            use_container_width=True,
            hide_index=True
        )


    else:

        st.info(
            "No supplier data available."
        )


    # --------------------------------------------------------
    # Supplier Performance Chart
    # --------------------------------------------------------

    st.subheader(
        "Performance Score vs On-Time Rate"
    )


    if not df_s.empty:

        supplier_chart = (
            df_s[
                [
                    "SUPPLIER_NAME",
                    "AVG_PERFORMANCE_SCORE",
                    "ON_TIME_RATE_PCT"
                ]
            ]
            .set_index(
                "SUPPLIER_NAME"
            )
        )


        st.bar_chart(
            supplier_chart,
            use_container_width=True
        )


    else:

        st.info(
            "No supplier performance data available."
        )


    # --------------------------------------------------------
    # Supplier Revenue
    # --------------------------------------------------------

    st.subheader(
        "Supplier Revenue"
    )


    if not df_s.empty:

        supplier_revenue = (
            df_s[
                [
                    "SUPPLIER_NAME",
                    "TOTAL_REVENUE"
                ]
            ]
            .sort_values(
                "TOTAL_REVENUE",
                ascending=False
            )
            .set_index(
                "SUPPLIER_NAME"
            )
        )


        st.bar_chart(
            supplier_revenue,
            use_container_width=True
        )


    else:

        st.info(
            "No supplier revenue data available."
        )


# ============================================================
# DATA TABLES
# ============================================================

st.divider()

st.subheader(
    "Detailed Operational Data"
)


detail_tab1, detail_tab2, detail_tab3 = st.tabs(
    [
        "Fulfillment",
        "Inventory",
        "Shipments"
    ]
)


# ============================================================
# FULFILLMENT TABLE
# ============================================================

with detail_tab1:

    if not df_f.empty:

        st.dataframe(
            df_f,
            use_container_width=True,
            hide_index=True
        )

    else:

        st.info(
            "No fulfillment data available."
        )


# ============================================================
# INVENTORY TABLE
# ============================================================

with detail_tab2:

    if not df_i.empty:

        st.dataframe(
            df_i,
            use_container_width=True,
            hide_index=True
        )

    else:

        st.info(
            "No inventory data available."
        )


# ============================================================
# SHIPMENT TABLE
# ============================================================

with detail_tab3:

    if not df_d.empty:

        st.dataframe(
            df_d,
            use_container_width=True,
            hide_index=True
        )

    else:

        st.info(
            "No shipment data available."
        )


# ============================================================
# DATA EXPORT
# ============================================================

st.divider()

st.subheader(
    "Export Data"
)


export_col1, export_col2, export_col3 = st.columns(3)


# ------------------------------------------------------------
# Fulfillment Export
# ------------------------------------------------------------

with export_col1:

    if not df_f.empty:

        st.download_button(
            label="⬇️ Download Fulfillment CSV",
            data=df_f.to_csv(
                index=False
            ).encode("utf-8"),
            file_name="flowbridge_fulfillment.csv",
            mime="text/csv"
        )


# ------------------------------------------------------------
# Supplier Export
# ------------------------------------------------------------

with export_col2:

    if not df_s.empty:

        st.download_button(
            label="⬇️ Download Supplier CSV",
            data=df_s.to_csv(
                index=False
            ).encode("utf-8"),
            file_name="flowbridge_suppliers.csv",
            mime="text/csv"
        )


# ------------------------------------------------------------
# Shipment Export
# ------------------------------------------------------------

with export_col3:

    if not df_d.empty:

        st.download_button(
            label="⬇️ Download Shipment CSV",
            data=df_d.to_csv(
                index=False
            ).encode("utf-8"),
            file_name="flowbridge_shipments.csv",
            mime="text/csv"
        )


# ============================================================
# FOOTER
# ============================================================

st.divider()

st.caption(
    "FlowBridge Dashboard • Powered by Snowflake Snowpark & Streamlit"
)