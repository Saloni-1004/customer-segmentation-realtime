import streamlit as st
import pandas as pd
import plotly.express as px
import time
import hashlib
import psycopg2
from datetime import datetime, timezone, timedelta

# ---------------------------
# 🔐 Simple Login Function
# ---------------------------

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

# Authenticate
check_password()

# ---------------------------
# 🎨 Page Config
# ---------------------------
st.set_page_config(page_title="📊 Real-Time Customer Segmentation", layout="wide")
st.title("📊 Real-Time Customer Segmentation Dashboard")

# ---------------------------
# 🧠 PostgreSQL Connection
# ---------------------------
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
    st.error(f"❌ Failed to connect to PostgreSQL: {e}")
    st.stop()

# ---------------------------
# ⏱️ Sidebar Filters
# ---------------------------
st.sidebar.header("🔍 Filters")

# Auto-refresh
if "last_refresh" not in st.session_state:
    st.session_state["last_refresh"] = time.time()

auto_refresh = st.sidebar.checkbox("Enable Auto-Refresh (5s)", value=True)

if auto_refresh:
    if time.time() - st.session_state["last_refresh"] > 5:
        st.session_state["last_refresh"] = time.time()
        st.rerun()

# Time filter
time_range_hours = st.sidebar.slider("Time Range (Last N Hours)", 1, 168, 48)
from_time = datetime.now(timezone.utc) - timedelta(hours=time_range_hours)

# Cluster filter
clusters = st.sidebar.multiselect("Clusters", [0, 1, 2], default=[0, 1, 2])

# ---------------------------
# 📥 Data Fetch
# ---------------------------
query = """
    SELECT record_id, customer_id, name, age, purchase_amount, cluster, created_at
    FROM customer_segments
    WHERE created_at >= %s AND cluster = ANY(%s)
    ORDER BY created_at DESC
    LIMIT 200
"""
cursor.execute(query, (from_time, clusters))
rows = cursor.fetchall()
columns = ["record_id", "customer_id", "name", "age", "purchase_amount", "cluster", "created_at"]
df = pd.DataFrame(rows, columns=columns)

# ---------------------------
# 📊 Show Metrics and Charts
# ---------------------------
if df.empty:
    st.warning("⚠️ No data found for the selected filter/time range.")
else:
    col1, col2, col3 = st.columns(3)
    col1.metric("🧍 Total Customers", len(df))
    col2.metric("💰 Total Purchase", f"${df['purchase_amount'].sum():,.2f}")
    col3.metric("🕒 Latest Entry", df['created_at'].max().strftime("%Y-%m-%d %H:%M:%S"))

    # Scatter Chart
    fig1 = px.scatter(df, x="age", y="purchase_amount", color="cluster", hover_data=["name", "customer_id"])
    fig1.update_layout(title="🎯 Age vs Purchase by Cluster")
    st.plotly_chart(fig1, use_container_width=True)

    # Pie Chart
    cluster_counts = df["cluster"].value_counts().reset_index()
    cluster_counts.columns = ["Cluster", "Count"]
    fig2 = px.pie(cluster_counts, names="Cluster", values="Count", title="👥 Cluster Distribution")
    st.plotly_chart(fig2, use_container_width=True)

    # Data Table
    with st.expander("📋 View Raw Data"):
        st.dataframe(df, use_container_width=True)

    # Download Option
    st.download_button("📥 Download as CSV", df.to_csv(index=False), "customer_segments.csv", "text/csv")

