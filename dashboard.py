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
        if "password" in st.session_state:
            if hash_password(st.session_state["password"]) == hash_password("admin123"):
                st.session_state["password_correct"] = True
                if "password" in st.session_state:
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

        st.sidebar.markdown(f"**Debug Query**: {query} | Params: {params}")
        cursor.execute(query, params)
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        st.sidebar.markdown(f"**Debug Rows Fetched**: {len(data)}")
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
time_range = st.sidebar.slider("Select Time Range (Hours)", 0, 24, 24)  # Default to 24 hours
clusters = st.sidebar.multiselect("Select Clusters", options=[0, 1, 2], default=[0, 1, 2])

# --------------------------- 
# 📋 Load and render data
# ---------------------------
df = load_data(hours=time_range, selected_clusters=clusters)

if df.empty:
    st.warning("⚠️ No data available. Check database connection or data filters. See debug info in sidebar.")
else:
    st.markdown("<h1 style='text-align: center; color: #4CAF50;'>📊 Real-Time Customer Segmentation Dashboard</h1>", unsafe_allow_html=True)

    # Metrics
    total_customers = len(df)
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Customers", total_customers)
    col2.metric("Average Purchase", f"${df['purchase_amount'].mean():.2f}")
    col3.metric("Average Age", f"{df['age'].mean():.1f}")

    # Charts layout
    chart_col1, chart_col2 = st.columns(2)

    with chart_col1:
        # Cluster-wise Purchase Distribution
        st.subheader("💲 Cluster-wise Purchase Distribution")
        purchase_dist = df.groupby('cluster')['purchase_amount'].sum().reset_index()
        fig1 = px.bar(purchase_dist, x="cluster", y="purchase_amount", color="cluster",
                      color_continuous_scale="teal", title="Cluster-wise Purchase Distribution")
        fig1.update_layout(paper_bgcolor="#1E1E1E", plot_bgcolor="#1E1E1E", font_color="#F5F5F5",
                          xaxis=dict(gridcolor="#555555"), yaxis=dict(gridcolor="#555555"))
        st.plotly_chart(fig1, use_container_width=True)

        # Total Customers Per Cluster
        st.subheader("👥 Total Customers Per Cluster")
        cluster_counts = df.groupby('cluster').size().reset_index(name='count')
        fig3 = px.pie(cluster_counts, values="count", names="cluster", color="cluster",
                      color_discrete_sequence=px.colors.sequential.Viridis,
                      title="Total Customers Per Cluster")
        fig3.update_layout(paper_bgcolor="#1E1E1E", plot_bgcolor="#1E1E1E", font_color="#F5F5F5")
        st.plotly_chart(fig3, use_container_width=True)

    with chart_col2:
        # Age Distribution by Cluster
        st.subheader("📈 Age Distribution by Cluster")
        fig2 = px.histogram(df, x="age", color="cluster", nbins=10, barmode="overlay",
                            color_discrete_sequence=px.colors.sequential.YlOrRd,
                            title="Age Distribution by Cluster")
        fig2.update_layout(paper_bgcolor="#1E1E1E", plot_bgcolor="#1E1E1E", font_color="#F5F5F5",
                          xaxis=dict(gridcolor="#555555"), yaxis=dict(gridcolor="#555555"))
        st.plotly_chart(fig2, use_container_width=True)

        # Average Purchase Per Cluster
        st.subheader("💸 Average Purchase Per Cluster")
        avg_purchase = df.groupby('cluster')['purchase_amount'].mean().reset_index()
        fig4 = px.bar(avg_purchase, x="cluster", y="purchase_amount", color="cluster",
                      color_continuous_scale="teal", title="Average Purchase Per Cluster")
        fig4.update_layout(paper_bgcolor="#1E1E1E", plot_bgcolor="#1E1E1E", font_color="#F5F5F5",
                          xaxis=dict(gridcolor="#555555"), yaxis=dict(gridcolor="#555555"))
        st.plotly_chart(fig4, use_container_width=True)

    # Latest Customer Data
    st.subheader("📋 Latest Customer Data")
    display_df = df[["customer_id", "name", "age", "purchase_amount", "cluster", "created_at"]]
    if 'created_at' in display_df.columns and not display_df.empty:
        display_df['created_at'] = pd.to_datetime(display_df['created_at']).dt.strftime('%Y-%m-%d %H:%M:%S')
    st.dataframe(display_df.sort_values(by='created_at', ascending=False), height=400, use_container_width=True)

    # Alert for high purchases
    if df['purchase_amount'].max() > 2400:
        st.error("⚠️ Alert: Purchase amount exceeds 2400!")

    # Download button
    st.download_button("⬇ Download CSV", df.to_csv(index=False), "customer_segments.csv", "text/csv")

# --------------------------- 
# 🔁 Auto-refresh
# ---------------------------
if st.sidebar.checkbox("Enable Auto-Refresh (5s)", value=True):
    time.sleep(5)
    st.rerun()

# Add basic CSS for styling
st.markdown(
    """
    <style>
    body { background-color: #121212; color: #F5F5F5; }
    .stApp { background-color: #1E1E1E; }
    h1 { text-align: center; color: #4CAF50; padding: 10px; border-radius: 10px; }
    .stMetric { color: #FFFF99; }
    .stDataFrame { background-color: #2A2A2A; color: #F5F5F5; }
    </style>
    """,
    unsafe_allow_html=True
)
