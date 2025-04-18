import streamlit as st
import pandas as pd
import plotly.express as px
import time
import hashlib
import psycopg2
import os
from datetime import datetime, timedelta

# Page configuration
st.set_page_config(page_title="📊 Real-Time Customer Segmentation", layout="wide")

# ---------------------------
# 🔐 Simple Login Function
# ---------------------------
def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

def check_password():
    """Check if the user has entered the correct password."""
    if "password_correct" not in st.session_state:
        st.session_state["password_correct"] = False

    def password_entered():
        """Handle password input and validate it."""
        if "password" in st.session_state:  # Check if password exists in session state
            if hash_password(st.session_state["password"]) == hash_password("admin123"):
                st.session_state["password_correct"] = True
                if "password" in st.session_state:  # Check again before deleting
                    del st.session_state["password"]
            else:
                st.session_state["password_correct"] = False

    if not st.session_state["password_correct"]:
        st.text_input("🔐 Enter Password", type="password", on_change=password_entered, key="password")
        return False
    return True

if not check_password():
    st.stop()

# ---------------------------
# Database Connection Function
# ---------------------------
def get_connection():
    return psycopg2.connect(**st.secrets["postgres"])

# ---------------------------
# 📊 Load data from Neon PostgreSQL
# ---------------------------
@st.cache_data(ttl=5)
def load_data(hours=24, selected_clusters=None):
    try:
        conn = get_connection()
        cursor = conn.cursor()
        where_clauses = []
        params = []

        if hours > 0:
            where_clauses.append("created_at > %s")
            params.append(datetime.now() - timedelta(hours=hours))

        if selected_clusters and len(selected_clusters) > 0:
            placeholders = ', '.join(['%s'] * len(selected_clusters))
            where_clauses.append(f"cluster IN ({placeholders})")
            params.extend(selected_clusters)

        query = "SELECT * FROM customer_segments"
        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)

        cursor.execute(query, params)
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        cursor.close()
        conn.close()
        return pd.DataFrame(data, columns=columns)

    except Exception as e:
        st.error(f"Database error: {e}")
        return pd.DataFrame()

# ---------------------------
# 🔍 Sidebar Filters
# ---------------------------
st.sidebar.header("🔍 Filters")
time_range = st.sidebar.slider("Select Time Range (Hours)", 0, 24, 1)
clusters = st.sidebar.multiselect("Select Clusters", options=[0, 1, 2], default=[0, 1, 2])

# ---------------------------
# 📋 Load and render data
# ---------------------------
df = load_data(hours=time_range, selected_clusters=clusters)

if df.empty:
    st.warning("⚠️ No data available. Check database connection or data filters.")
else:
    st.markdown("<h1>📊 Customer Segmentation Dashboard</h1>", unsafe_allow_html=True)

    total_customers = len(df)
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Customers", total_customers)
    col2.metric("Average Purchase", f"${df['purchase_amount'].mean():.2f}")
    col3.metric("Average Age", f"{df['age'].mean():.1f}")

    chart_col, data_col = st.columns(2)

    with chart_col:
        st.subheader("👥 Customers Per Cluster")
        cluster_counts = df.groupby('cluster').size().reset_index(name='count')
        fig1 = px.bar(cluster_counts, x="cluster", y="count", color="cluster", title="Customer Distribution by Segment")
        st.plotly_chart(fig1, use_container_width=True)

        st.subheader("💰 Average Spending by Cluster")
        avg_spending = df.groupby('cluster')['purchase_amount'].mean().reset_index()
        fig2 = px.bar(avg_spending, x="cluster", y="purchase_amount", color="cluster", title="Average Customer Spending")
        st.plotly_chart(fig2, use_container_width=True)

    with data_col:
        st.subheader("📋 Latest Customer Data")
        display_df = df[["customer_id", "name", "age", "purchase_amount", "cluster", "created_at"]]
        if 'created_at' in display_df.columns and not display_df.empty:
            display_df['created_at'] = pd.to_datetime(display_df['created_at']).dt.strftime('%Y-%m-%d %H:%M:%S')
        st.dataframe(display_df.sort_values(by='created_at', ascending=False), height=400, use_container_width=True)

# ---------------------------
# 🔁 Auto-refresh
# ---------------------------
if st.sidebar.checkbox("Enable Auto-Refresh (5s)", value=True):
    time.sleep(5)
    st.rerun()
