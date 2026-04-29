import os

import altair as alt
import pandas as pd
import snowflake.connector
import streamlit as st
from dotenv import load_dotenv

load_dotenv()


def _connect():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )


@st.cache_data
def get_headline_metrics():
    conn = _connect()
    cur = conn.cursor()
    cur.execute("""
        WITH monthly AS (
            SELECT
                DATE_TRUNC('month', TO_TIMESTAMP_NTZ(CREATED_AT, 9)) AS month,
                SUM(PRICE_USD)       AS revenue,
                COUNT(*)             AS orders,
                AVG(PRICE_USD)       AS aov,
                SUM(ITEMS_PURCHASED) AS items_sold
            FROM RAW.ORDERS
            GROUP BY 1
        ),
        latest AS (
            SELECT DATEADD('month', -1, DATE_TRUNC('month', MAX(month))) AS current_month
            FROM monthly
        )
        SELECT m.month, m.revenue, m.orders, m.aov, m.items_sold
        FROM monthly m, latest l
        WHERE m.month IN (l.current_month, DATEADD('month', -1, l.current_month))
        ORDER BY m.month DESC
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


@st.cache_data
def get_monthly_revenue():
    conn = _connect()
    cur = conn.cursor()
    cur.execute("""
        WITH monthly AS (
            SELECT
                DATE_TRUNC('month', TO_TIMESTAMP_NTZ(CREATED_AT, 9)) AS month,
                SUM(PRICE_USD) AS revenue
            FROM RAW.ORDERS
            GROUP BY 1
        ),
        latest_complete AS (
            SELECT DATEADD('month', -1, DATE_TRUNC('month', MAX(month))) AS cutoff
            FROM monthly
        )
        SELECT m.month, m.revenue
        FROM monthly m, latest_complete l
        WHERE m.month <= l.cutoff
        ORDER BY m.month
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    df = pd.DataFrame(rows, columns=["month", "revenue"])
    df["month"] = pd.to_datetime(df["month"])
    return df


@st.cache_data
def get_top_products(start_date, end_date):
    conn = _connect()
    cur = conn.cursor()
    if start_date:
        cur.execute("""
            SELECT p.PRODUCT_NAME, SUM(oi.PRICE_USD) AS revenue
            FROM RAW.ORDER_ITEMS oi
            JOIN RAW.PRODUCTS p ON oi.PRODUCT_ID = p.PRODUCT_ID
            WHERE TO_TIMESTAMP_NTZ(oi.CREATED_AT, 9) >= %s
              AND TO_TIMESTAMP_NTZ(oi.CREATED_AT, 9) < DATEADD('month', 1, %s)
            GROUP BY p.PRODUCT_NAME
            ORDER BY revenue DESC
            LIMIT 10
        """, (start_date, end_date))
    else:
        cur.execute("""
            SELECT p.PRODUCT_NAME, SUM(oi.PRICE_USD) AS revenue
            FROM RAW.ORDER_ITEMS oi
            JOIN RAW.PRODUCTS p ON oi.PRODUCT_ID = p.PRODUCT_ID
            WHERE TO_TIMESTAMP_NTZ(oi.CREATED_AT, 9) < DATEADD('month', 1, %s)
            GROUP BY p.PRODUCT_NAME
            ORDER BY revenue DESC
            LIMIT 10
        """, (end_date,))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return pd.DataFrame(rows, columns=["product", "revenue"])


@st.cache_data
def get_all_products():
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT PRODUCT_ID, PRODUCT_NAME FROM RAW.PRODUCTS ORDER BY PRODUCT_NAME")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


@st.cache_data
def get_bundle_finder(product_id):
    conn = _connect()
    cur = conn.cursor()
    cur.execute("""
        SELECT p.PRODUCT_NAME, COUNT(DISTINCT oi2.ORDER_ID) AS co_orders
        FROM RAW.ORDER_ITEMS oi1
        JOIN RAW.ORDER_ITEMS oi2
          ON oi1.ORDER_ID = oi2.ORDER_ID
         AND oi1.PRODUCT_ID != oi2.PRODUCT_ID
        JOIN RAW.PRODUCTS p ON oi2.PRODUCT_ID = p.PRODUCT_ID
        WHERE oi1.PRODUCT_ID = %s
        GROUP BY p.PRODUCT_NAME
        ORDER BY co_orders DESC
    """, (product_id,))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return pd.DataFrame(rows, columns=["product", "co_orders"])


def delta(current, prior):
    if prior is None or prior == 0:
        return None
    return current - prior


def fmt_currency(value):
    if value >= 1_000_000:
        return f"${value / 1_000_000:.2f}M"
    if value >= 1_000:
        return f"${value / 1_000:.1f}K"
    return f"${value:.2f}"


# ── Title ─────────────────────────────────────────────────────────────────────
st.title("Basket Craft Dashboard")

# ── Headline metrics ──────────────────────────────────────────────────────────
with st.spinner("Loading metrics..."):
    rows = get_headline_metrics()

if len(rows) == 2:
    (cur_month, cur_rev, cur_orders, cur_aov, cur_items), \
    (_, pri_rev, pri_orders, pri_aov, pri_items) = rows
    label = cur_month.strftime("%B %Y")
elif len(rows) == 1:
    cur_month, cur_rev, cur_orders, cur_aov, cur_items = rows[0]
    pri_rev = pri_orders = pri_aov = pri_items = None
    label = cur_month.strftime("%B %Y")
else:
    st.error("No order data found.")
    st.stop()

st.subheader(f"Headline Metrics — {label}")

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Revenue",   fmt_currency(cur_rev),
            delta=fmt_currency(delta(cur_rev, pri_rev)) if pri_rev else None)
col2.metric("Total Orders",    f"{cur_orders:,}",
            delta=f"{delta(cur_orders, pri_orders):+,}" if pri_orders else None)
col3.metric("Avg Order Value", f"${cur_aov:,.2f}",
            delta=f"${delta(cur_aov, pri_aov):+,.2f}" if pri_aov else None)
col4.metric("Items Sold",      f"{cur_items:,}",
            delta=f"{delta(cur_items, pri_items):+,}" if pri_items else None)

# ── Date filter (shared) ──────────────────────────────────────────────────────
with st.spinner("Loading trend..."):
    trend_df = get_monthly_revenue()

min_date = trend_df["month"].min()
max_date = trend_df["month"].max()

windows = {
    "Last 3 months":  3,
    "Last 6 months":  6,
    "Last 12 months": 12,
    "All time":       None,
    "Custom range":   "custom",
}

st.sidebar.header("Filters")
selected = st.sidebar.selectbox("Date range", list(windows.keys()), index=2)

if windows[selected] == "custom":
    custom = st.sidebar.date_input(
        "Select dates",
        value=(max_date - pd.DateOffset(months=11), max_date),
        min_value=min_date.date(),
        max_value=max_date.date(),
    )
    if len(custom) == 2:
        start_date = pd.Timestamp(custom[0])
        end_date   = pd.Timestamp(custom[1])
    else:
        st.sidebar.caption("Pick an end date to apply the filter.")
        st.stop()
elif windows[selected] is not None:
    start_date = max_date - pd.DateOffset(months=windows[selected] - 1)
    end_date   = max_date
else:
    start_date = None
    end_date   = max_date

filtered_trend = (
    trend_df[trend_df["month"] >= start_date] if start_date is not None else trend_df
)
if windows[selected] == "custom":
    filtered_trend = trend_df[
        (trend_df["month"] >= start_date) & (trend_df["month"] <= end_date)
    ]

st.subheader("Revenue Trend")

# ── Revenue trend chart ───────────────────────────────────────────────────────
trend_chart = (
    alt.Chart(filtered_trend)
    .mark_area(line=True, point=True, color="#0077B6", opacity=0.15)
    .encode(
        x=alt.X("month:T", title="Month", axis=alt.Axis(format="%b %Y")),
        y=alt.Y("revenue:Q", title="Revenue (USD)", axis=alt.Axis(format="$,.0f")),
        tooltip=[
            alt.Tooltip("month:T", title="Month", format="%B %Y"),
            alt.Tooltip("revenue:Q", title="Revenue", format="$,.2f"),
        ],
    )
    .properties(height=350)
)
st.altair_chart(trend_chart, use_container_width=True)

# ── Top products chart ────────────────────────────────────────────────────────
st.subheader("Top Products by Revenue")

with st.spinner("Loading products..."):
    products_df = get_top_products(
        start_date.date() if start_date is not None else None,
        end_date.date(),
    )

bar_chart = (
    alt.Chart(products_df)
    .mark_bar(color="#0096C7")
    .encode(
        x=alt.X("revenue:Q", title="Revenue (USD)", axis=alt.Axis(format="$,.0f")),
        y=alt.Y("product:N", sort="-x", title=None),
        tooltip=[
            alt.Tooltip("product:N", title="Product"),
            alt.Tooltip("revenue:Q", title="Revenue", format="$,.2f"),
        ],
    )
    .properties(height=300)
)
st.altair_chart(bar_chart, use_container_width=True)

# ── Bundle finder ─────────────────────────────────────────────────────────────
st.subheader("Bundle Finder")

all_products = get_all_products()
product_map  = {name: pid for pid, name in all_products}

selected_product = st.selectbox(
    "Pick a product to see what it's bought with",
    list(product_map.keys()),
)

with st.spinner("Finding bundles..."):
    bundle_df = get_bundle_finder(product_map[selected_product])

if bundle_df.empty:
    st.info("No orders found where this product was bought with another product.")
else:
    bundle_chart = (
        alt.Chart(bundle_df)
        .mark_bar(color="#2EC4B6")
        .encode(
            x=alt.X("co_orders:Q", title="Orders containing both products"),
            y=alt.Y("product:N", sort="-x", title=None),
            tooltip=[
                alt.Tooltip("product:N", title="Bought with"),
                alt.Tooltip("co_orders:Q", title="Co-orders"),
            ],
        )
        .properties(height=max(100, len(bundle_df) * 50))
    )
    st.altair_chart(bundle_chart, use_container_width=True)
