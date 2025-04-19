import streamlit as st
import pandas as pd
import plotly.express as px
import time
import hashlib
import psycopg2
from datetime import datetime, timezone, timedelta

# Auth
def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

def check_password():
    if "password_correct" not in st.session_state:
        st.session_state["password_correct"] = False

    def password_entered():
        if hash_password(st.session_state["password"]) == hash_password("admin123"):
            st.session_state["password_correct"] = True
            del st.session_state["password"]
        else:
            st.session_state["password_correct"] = False

    if not st.session_state["password_correct"]:
        st.text_input("🔐 Enter Password", type="password", on_change=password_entered, key="password")
        st.stop()

check_password()
st.set_page_config(page_title="📊 Real-Time Dashboard", layout="wide")
st.title("📊 Real-Time Customer Segmentation")

# Auto-refresh setup
if "last_refresh" not in st.session_state:
    st.session_state["last_refresh"] = time.time()
if st.sidebar.checkbox("Enable Auto-Refresh (5s)", value=True):
    if time.time() - st.session_state["last_refresh"] > 5:
        st.session_state["last_refresh"] = time.time()
        st.rerun()

# DB Connect
try:
    conn = psycopg2.connect(
        host="ep-dry-violet-a4v38rh7-pooler.us-east-1.aws.neon.tech",
        port="5432",
        dbname="neondb",
        user="neondb_owner",
        password="npg_5UbnztxlVuD1",
        sslmode='require'
    )
    cursor = conn.cursor()
except Exception as e:
    st.error(f"Database connection error: {e}")
    st.stop()

# Filters
st.sidebar.header("Filters")
hours = st.sidebar.slider("Last N Hours", 1, 168, 24)
from_time = datetime.now(timezone.utc) - timedelta(hours=hours)
clusters = st.sidebar.multiselect("Clusters", [0, 1, 2], default=[0, 1, 2])

# Data Query
cursor.execute("""
    SELECT record_id, customer_id, name, age, purchase_amount, cluster, created_at
    FROM customer_segments
    WHERE created_at >= %s AND cluster = ANY(%s)
    ORDER BY created_at DESC
    LIMIT 200
""", (from_time, clusters))

rows = cursor.fetchall()
columns = ["record_id", "customer_id", "name", "age", "purchase_amount", "cluster", "created_at"]
df = pd.DataFrame(rows, columns=columns)

# Charts
if df.empty:
    st.warning("⚠️ No data found.")
else:
    col1, col2, col3 = st.columns(3)
    col1.metric("👥 Customers", len(df))
    col2.metric("💸 Revenue", f"${df['purchase_amount'].sum():,.2f}")
    col3.metric("🕒 Last Entry", df["created_at"].max().strftime("%Y-%m-%d %H:%M:%S"))

    # Chart 1: Scatter Age vs Purchase
    st.subheader("🎯 Age vs Purchase by Cluster")
    fig1 = px.scatter(df, x="age", y="purchase_amount", color="cluster", hover_data=["name"])
    st.plotly_chart(fig1, use_container_width=True)

    # Chart 2: Pie Chart
    st.subheader("📊 Cluster Distribution")
    fig2 = px.pie(df, names="cluster", title="Customer Clusters")
    st.plotly_chart(fig2, use_container_width=True)

    # Chart 3: Bar Chart Avg Purchase
    st.subheader("💰 Avg Purchase per Cluster")
    avg_df = df.groupby("cluster")["purchase_amount"].mean().reset_index()
    fig3 = px.bar(avg_df, x="cluster", y="purchase_amount", color="cluster")
    st.plotly_chart(fig3, use_container_width=True)

    # Chart 4: Histogram - Age
    st.subheader("📈 Age Distribution")
    fig4 = px.histogram(df, x="age", nbins=10, color="cluster")
    st.plotly_chart(fig4, use_container_width=True)

    # Data Table
    with st.expander("📋 Raw Data"):
        st.dataframe(df)

    # Download
    st.download_button("⬇ Download CSV", df.to_csv(index=False), "data.csv", "text/csv")
