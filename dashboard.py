import os
from datetime import datetime

import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv
from pymongo import MongoClient
from streamlit_autorefresh import st_autorefresh

load_dotenv()

MONGODB_URI = os.getenv("MONGODB_URI", "")
MONGODB_DB = os.getenv("MONGODB_DB", "retail_streaming")
MONGODB_COLLECTION = os.getenv("MONGODB_COLLECTION", "orders")
REFRESH_SECONDS = int(os.getenv("DASHBOARD_REFRESH_SECONDS", "3"))

st.set_page_config(
    page_title="Retail Streaming Analytics",
    page_icon="📊",
    layout="wide",
)

st.markdown(
    """
    <style>
    .main {padding-top: 1rem;}
    .metric-card {padding: 0.5rem 0;}
    </style>
    """,
    unsafe_allow_html=True,
)

if not MONGODB_URI:
    st.error("MONGODB_URI is not configured. Add it to the .env file before starting the dashboard.")
    st.stop()

@st.cache_resource
def get_client():
    return MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)

try:
    mongo = get_client()
    mongo.admin.command("ping")
    collection = mongo[MONGODB_DB][MONGODB_COLLECTION]
except Exception as exc:
    st.error(f"MongoDB connection failed: {exc}")
    st.stop()

# Refresh automatically so the dashboard behaves like a live monitoring screen.
st_autorefresh(interval=REFRESH_SECONDS * 1000, key="orders_refresh")

st.title("📊 Retail Streaming Analytics Dashboard")
st.caption(
    f"Kafka topic: orders-topic  •  MongoDB collection: {MONGODB_DB}.{MONGODB_COLLECTION}  "
    f"•  Auto-refresh: every {REFRESH_SECONDS}s"
)

# Pull raw consumed events from MongoDB.
docs = list(collection.find({}, {"_id": 0}).sort("event_timestamp", 1))
if not docs:
    st.info("No consumed records yet. Start Kafka, run consumer.py, then run producer.py.")
    st.stop()

df = pd.DataFrame(docs)

# Normalise types.
df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0)
df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0)
df["event_timestamp"] = pd.to_datetime(df["event_timestamp"], errors="coerce")
df = df.dropna(subset=["event_timestamp"])

# Use the latest event per order for order-level KPIs. This prevents double counting
# if Assignment 3 is later extended to publish multiple events for the same order.
order_df = (
    df.sort_values("event_timestamp")
      .drop_duplicates(subset=["order_id"], keep="last")
      .copy()
)

# Sidebar filters.
st.sidebar.header("Dashboard Filters")
min_ts = order_df["event_timestamp"].min()
max_ts = order_df["event_timestamp"].max()

start_date = st.sidebar.date_input("From date", value=min_ts.date(), min_value=min_ts.date(), max_value=max_ts.date())
end_date = st.sidebar.date_input("To date", value=max_ts.date(), min_value=min_ts.date(), max_value=max_ts.date())

payment_options = sorted(order_df["payment_method"].dropna().unique().tolist())
selected_payments = st.sidebar.multiselect("Payment method", payment_options, default=payment_options)

product_options = sorted(order_df["product_id"].dropna().unique().tolist())
selected_products = st.sidebar.multiselect("Product", product_options, default=product_options)

filtered = order_df[
    (order_df["event_timestamp"].dt.date >= start_date)
    & (order_df["event_timestamp"].dt.date <= end_date)
    & (order_df["payment_method"].isin(selected_payments))
    & (order_df["product_id"].isin(selected_products))
].copy()

# KPI cards.
revenue = filtered["amount"].sum()
orders = filtered["order_id"].nunique()
units = filtered["quantity"].sum()
aov = revenue / orders if orders else 0
last_event = df["event_timestamp"].max()

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Sales Value", f"₹{revenue:,.0f}")
k2.metric("Orders", f"{orders:,}")
k3.metric("Units", f"{units:,.0f}")
k4.metric("Avg. Order Value", f"₹{aov:,.0f}")
k5.metric("Last Event", last_event.strftime("%d %b %H:%M:%S"))

if filtered.empty:
    st.warning("No records match the selected filters.")
    st.stop()

# 1. Revenue trend.
trend = (
    filtered.assign(time_bucket=filtered["event_timestamp"].dt.floor("min"))
            .groupby("time_bucket", as_index=False)["amount"].sum()
)
fig1 = px.line(
    trend,
    x="time_bucket",
    y="amount",
    markers=True,
    title="Sales Value Over Time",
    labels={"time_bucket": "Event Time", "amount": "Sales Value (₹)"},
)
fig1.update_layout(hovermode="x unified")

# 2. Sales by payment method.
payment = (
    filtered.groupby("payment_method", as_index=False)["amount"].sum()
            .sort_values("amount", ascending=False)
)
fig2 = px.bar(
    payment,
    x="payment_method",
    y="amount",
    text_auto=True,
    title="Sales Value by Payment Method",
    labels={"payment_method": "Payment Method", "amount": "Sales Value (₹)"},
)

# 3. Product performance.
product = (
    filtered.groupby("product_id", as_index=False)
            .agg(sales_value=("amount", "sum"), units=("quantity", "sum"))
            .sort_values("sales_value", ascending=False)
)
fig3 = px.bar(
    product,
    x="product_id",
    y="sales_value",
    text_auto=True,
    title="Product-wise Sales Value",
    labels={"product_id": "Product", "sales_value": "Sales Value (₹)"},
)

c1, c2 = st.columns(2)
with c1:
    st.plotly_chart(fig1, use_container_width=True)
with c2:
    st.plotly_chart(fig2, use_container_width=True)

st.plotly_chart(fig3, use_container_width=True)

# 4. Optional fourth chart to strengthen the submission.
events = df.groupby("event_type", as_index=False).size().rename(columns={"size": "events"})
fig4 = px.pie(
    events,
    names="event_type",
    values="events",
    hole=0.45,
    title="Event Type Mix",
)
st.plotly_chart(fig4, use_container_width=True)

# Business insight section.
st.subheader("Business Insights")

payment_leader = payment.iloc[0]
product_leader = product.iloc[0]

insight_cols = st.columns(3)
with insight_cols[0]:
    st.write(
        f"**Payment mix:** {payment_leader['payment_method']} contributes "
        f"₹{payment_leader['amount']:,.0f}, the largest share of sales value in the filtered data."
    )
with insight_cols[1]:
    st.write(
        f"**Top product:** {product_leader['product_id']} generates "
        f"₹{product_leader['sales_value']:,.0f} across {product_leader['units']:,.0f} units."
    )
with insight_cols[2]:
    st.write(
        f"**Order economics:** the filtered average order value is ₹{aov:,.0f}; "
        f"use this as a baseline for monitoring changes as more Kafka events arrive."
    )

st.divider()
st.caption("Data flow: Kafka producer → orders-topic → consumer.py → MongoDB Atlas → Streamlit dashboard")
