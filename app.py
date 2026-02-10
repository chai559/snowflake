import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd

st.set_page_config(
    page_title="Fraud Analytics Dashboard",
    layout="wide"
)

st.title("🚨 Fraud Analytics Dashboard")

# Get Snowflake session
session = get_active_session()

# Load data
df_daily = session.sql("""
    SELECT *
    FROM GOLD.VW_DAILY_FRAUD_METRICS
    ORDER BY TXN_DATE
""").to_pandas()

df_txns = session.sql("""
    SELECT *
    FROM GOLD.FACT_TRANSACTIONS
""").to_pandas()

df_alerts = session.sql("""
    SELECT *
    FROM GOLD.ALERTS
    ORDER BY ALERT_TIME DESC
""").to_pandas()

# Sidebar filters
st.sidebar.header("Filters")

selected_date = st.sidebar.selectbox(
    "Transaction Date",
    sorted(df_daily["TXN_DATE"].astype(str).unique())
)

selected_location = st.sidebar.selectbox(
    "Location",
    ["ALL"] + sorted(df_txns["LOCATION"].unique())
)

# Apply filters
filtered_txns = df_txns.copy()
if selected_location != "ALL":
    filtered_txns = filtered_txns[
        filtered_txns["LOCATION"] == selected_location
    ]

# KPI Metrics
daily_row = df_daily[
    df_daily["TXN_DATE"].astype(str) == selected_date
].iloc[0]

col1, col2, col3 = st.columns(3)
col1.metric("Total Transactions", int(daily_row["TOTAL_TXNS"]))
col2.metric("Fraud Transactions", int(daily_row["FRAUD_TXNS"]))
col3.metric("Fraud Rate (%)", float(daily_row["FRAUD_RATE"]))

# Charts
st.subheader("📈 Fraud Trend")
st.line_chart(
    df_daily.set_index("TXN_DATE")[["TOTAL_TXNS", "FRAUD_TXNS"]]
)

st.subheader("🌍 Fraud by Location")
location_summary = (
    filtered_txns.groupby("LOCATION")["FRAUD_FLAG"]
    .sum()
    .reset_index()
)
st.bar_chart(location_summary.set_index("LOCATION"))

# Alerts
st.subheader("🚨 Active Alerts")
st.dataframe(df_alerts, use_container_width=True)
