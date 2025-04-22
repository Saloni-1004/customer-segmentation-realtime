import streamlit as st

# Set page config must be the first Streamlit command
st.set_page_config(
    page_title="Customer Segmentation Dashboard", 
    layout="wide",
    initial_sidebar_state="expanded",
    page_icon="🔄"
)

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
import time
from datetime import datetime, timedelta

# Apply dark theme styling
st.markdown("""
<style>
    .main {
        background-color: #0E1117;
        color: white;
    }
    .stApp {
        background-color: #0E1117;
    }
    .stDataFrame {
        background-color: #262730;
    }
    .st-eb {
        background-color: #1F2937;
    }
    .stMetric {
        background-color: #1F2937;
        padding: 15px;
        border-radius: 5px;
    }
    .stSidebar {
        background-color: #1E1E1E;
    }
    .css-1d391kg {
        background-color: #1F2937;
    }
    h1, h2, h3, h4 {
        color: white !important;
    }
    .stPlotlyChart {
        background-color: #1F2937;
        border-radius: 5px;
        padding: 10px;
    }
</style>
""", unsafe_allow_html=True)

# Load environment variables
load_dotenv()

# Get the mode from environment variable
mode = os.getenv("MODE", "local")
st.sidebar.info(f"Running in {mode.upper()} mode")

# Database connection based on mode
if mode == "local":
    db_url = f"postgresql+psycopg2://{os.getenv('LOCAL_DB_USER')}:{os.getenv('LOCAL_DB_PASSWORD')}@{os.getenv('LOCAL_DB_HOST')}:{os.getenv('LOCAL_DB_PORT')}/{os.getenv('LOCAL_DB_NAME')}"
else:
    db_url = f"postgresql+psycopg2://{os.getenv('DEPLOY_DB_USER')}:{os.getenv('DEPLOY_DB_PASSWORD')}@{os.getenv('DEPLOY_DB_HOST')}:{os.getenv('DEPLOY_DB_PORT')}/{os.getenv('DEPLOY_DB_NAME')}?sslmode={os.getenv('DEPLOY_DB_SSLMODE')}"

# Create database engine
try:
    engine = create_engine(db_url)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
        st.sidebar.success("✅ Connected to database")
except Exception as e:
    st.sidebar.error(f"❌ Failed to connect to database: {e}")
    st.stop()

# Streamlit dashboard
st.title("🔄 Real-Time Customer Segmentation Dashboard")

# Sidebar for filters
st.sidebar.header("🔍 Filters")

# Time range filter
time_range = st.sidebar.slider("Select Time Range (Hours)", 0, 24, 1)
from_time = pd.Timestamp.now() - pd.Timedelta(hours=time_range)

# Cluster filter
clusters = st.sidebar.multiselect("Select Clusters", options=sorted([0, 1, 2]), default=[0, 1, 2])

# Purchase amount range
query = "SELECT MIN(purchase_amount) as min, MAX(purchase_amount) as max FROM customer_segments"
limits = pd.read_sql(query, engine).iloc[0]
purchase_min, purchase_max = st.sidebar.slider("Purchase Amount Range", 
                                             min_value=float(limits['min']), 
                                             max_value=float(limits['max']), 
                                             value=(float(limits['min']), float(limits['max'])))

# Auto-refresh toggle
auto_refresh = st.sidebar.checkbox("Enable Auto-Refresh (5s)", value=True)

# Fetch data function
def fetch_data():
    query = f"""
        SELECT record_id, customer_id, name, age, purchase_amount, cluster, created_at 
        FROM customer_segments 
        WHERE created_at >= %s 
        AND cluster IN {tuple(clusters) if clusters else (0, 1, 2)}
        AND purchase_amount BETWEEN %s AND %s
        ORDER BY created_at DESC LIMIT 100
    """
    try:
        df = pd.read_sql(query, engine, params=(from_time, purchase_min, purchase_max))
        if 'created_at' in df.columns:
            df['created_at'] = pd.to_datetime(df['created_at'])
        return df
    except Exception as e:
        st.error(f"Error fetching data: {e}")
        return pd.DataFrame()

# Cluster labels for visualization
cluster_labels = {0: "Low Value", 1: "Medium Value", 2: "High Value"}

# Main loop for auto-refresh
if auto_refresh:
    while True:
        df = fetch_data()

        if df.empty:
            st.warning("⚠️ No data available yet. Waiting for new messages...")
        else:
            # Summary metrics
            st.subheader("📊 Summary Metrics")
            metrics_cols = st.columns(4)
            with metrics_cols[0]:
                st.metric("Total Customers", len(df['customer_id'].unique()))
            with metrics_cols[1]:
                avg_purchase = round(df['purchase_amount'].mean(), 2)
                st.metric("Avg Purchase", f"${avg_purchase}")
            with metrics_cols[2]:
                avg_age = round(df['age'].mean(), 1)
                st.metric("Avg Age", avg_age)
            with metrics_cols[3]:
                latest_entry = df['created_at'].max()
                time_diff = datetime.now() - latest_entry.to_pydatetime()
                seconds_ago = int(time_diff.total_seconds())
                time_ago = f"{seconds_ago} seconds ago" if seconds_ago < 60 else f"{seconds_ago // 60} minutes ago"
                st.metric("Last Update", time_ago)

            # Row 1: First two large graphs
            col1, col2 = st.columns(2)
            with col1:
                st.markdown("<div class='chart-container'>", unsafe_allow_html=True)
                st.markdown("<p class='chart-title'>💰 Cluster-wise Purchase Distribution</p>", unsafe_allow_html=True)
                fig1 = px.bar(df, x="cluster", y="purchase_amount", color="cluster",
                            labels={"cluster": "Cluster", "purchase_amount": "Purchase Amount"},
                            color_continuous_scale="teal")
                fig1.update_layout(title_text="💰 Cluster-wise Purchase Distribution", title_x=0.5)
                st.plotly_chart(fig1, use_container_width=True)
                st.markdown("</div>", unsafe_allow_html=True)

            with col2:
                st.markdown("<div class='chart-container'>", unsafe_allow_html=True)
                st.markdown("<p class='chart-title'>👤 Age Distribution by Cluster</p>", unsafe_allow_html=True)
                fig2 = px.histogram(df, x="age", color="cluster",
                                    labels={"age": "Age", "count": "Number of Customers"},
                                    barmode="overlay", color_discrete_sequence=px.colors.sequential.Viridis)
                fig2.update_layout(title_text="👤 Age Distribution by Cluster", title_x=0.5)
                st.plotly_chart(fig2, use_container_width=True)
                st.markdown("</div>", unsafe_allow_html=True)

            # Row 2: Second two graphs
            col3, col4 = st.columns(2)
            with col3:
                st.markdown("<div class='chart-container'>", unsafe_allow_html=True)
                st.markdown("<p class='chart-title'>👥 Total Customers Per Cluster</p>", unsafe_allow_html=True)
                cluster_counts = df["cluster"].value_counts().reset_index()
                cluster_counts.columns = ["Cluster", "Total Customers"]
                fig3 = px.pie(cluster_counts, values="Total Customers", names="Cluster",
                            color_discrete_sequence=px.colors.sequential.Rainbow)
                fig3.update_layout(title_text="👥 Total Customers Per Cluster", title_x=0.5)
                st.plotly_chart(fig3, use_container_width=True)
                st.markdown("</div>", unsafe_allow_html=True)

            with col4:
                st.markdown("<div class='chart-container'>", unsafe_allow_html=True)
                st.markdown("<p class='chart-title'>💸 Average Purchase Per Cluster</p>", unsafe_allow_html=True)
                avg_purchase = df.groupby("cluster")["purchase_amount"].mean().reset_index()
                fig4 = px.bar(avg_purchase, x="cluster", y="purchase_amount", color="cluster",
                            labels={"cluster": "Cluster", "purchase_amount": "Avg Purchase"},
                            color_continuous_scale="magma")
                fig4.update_layout(title_text="💸 Average Purchase Per Cluster", title_x=0.5)
                st.plotly_chart(fig4, use_container_width=True)
                st.markdown("</div>", unsafe_allow_html=True)

            # Add customer data table
            st.markdown("<div class='chart-container'>", unsafe_allow_html=True)
            st.markdown("<p class='chart-title'>📋 Latest Customer Data</p>", unsafe_allow_html=True)
            st.dataframe(df[["customer_id", "name", "age", "purchase_amount", "cluster", "created_at"]].style.format({"purchase_amount": "${:.2f}"}), use_container_width=True)
            st.markdown("</div>", unsafe_allow_html=True)

            # Add alert for high purchases
            if df["purchase_amount"].max() > 400:
                st.error("⚠️ Alert: High purchase amount detected! Check cluster data.")

            # Export data
            if st.download_button("📥 Download Data as CSV", df.to_csv(index=False), "customer_segments.csv", "text/csv"):
                st.success("Data downloaded successfully!")

        time.sleep(5)
        st.rerun()
else:
    if st.button("🔄 Refresh Data"):
        df = fetch_data()

        if df.empty:
            st.warning("⚠️ No data available yet. Waiting for new messages...")
        else:
            # Summary metrics
            st.subheader("📊 Summary Metrics")
            metrics_cols = st.columns(4)
            with metrics_cols[0]:
                st.metric("Total Customers", len(df['customer_id'].unique()))
            with metrics_cols[1]:
                avg_purchase = round(df['purchase_amount'].mean(), 2)
                st.metric("Avg Purchase", f"${avg_purchase}")
            with metrics_cols[2]:
                avg_age = round(df['age'].mean(), 1)
                st.metric("Avg Age", avg_age)
            with metrics_cols[3]:
                latest_entry = df['created_at'].max()
                time_diff = datetime.now() - latest_entry.to_pydatetime()
                seconds_ago = int(time_diff.total_seconds())
                time_ago = f"{seconds_ago} seconds ago" if seconds_ago < 60 else f"{seconds_ago // 60} minutes ago"
                st.metric("Last Update", time_ago)

            # Row 1: First two large graphs
            col1, col2 = st.columns(2)
            with col1:
                st.markdown("<div class='chart-container'>", unsafe_allow_html=True)
                st.markdown("<p class='chart-title'>💰 Cluster-wise Purchase Distribution</p>", unsafe_allow_html=True)
                fig1 = px.bar(df, x="cluster", y="purchase_amount", color="cluster",
                            labels={"cluster": "Cluster", "purchase_amount": "Purchase Amount"},
                            color_continuous_scale="teal")
                fig1.update_layout(title_text="💰 Cluster-wise Purchase Distribution", title_x=0.5)
                st.plotly_chart(fig1, use_container_width=True)
                st.markdown("</div>", unsafe_allow_html=True)

            with col2:
                st.markdown("<div class='chart-container'>", unsafe_allow_html=True)
                st.markdown("<p class='chart-title'>👤 Age Distribution by Cluster</p>", unsafe_allow_html=True)
                fig2 = px.histogram(df, x="age", color="cluster",
                                    labels={"age": "Age", "count": "Number of Customers"},
                                    barmode="overlay", color_discrete_sequence=px.colors.sequential.Viridis)
                fig2.update_layout(title_text="👤 Age Distribution by Cluster", title_x=0.5)
                st.plotly_chart(fig2, use_container_width=True)
                st.markdown("</div>", unsafe_allow_html=True)

            # Row 2: Second two graphs
            col3, col4 = st.columns(2)
            with col3:
                st.markdown("<div class='chart-container'>", unsafe_allow_html=True)
                st.markdown("<p class='chart-title'>👥 Total Customers Per Cluster</p>", unsafe_allow_html=True)
                cluster_counts = df["cluster"].value_counts().reset_index()
                cluster_counts.columns = ["Cluster", "Total Customers"]
                fig3 = px.pie(cluster_counts, values="Total Customers", names="Cluster",
                            color_discrete_sequence=px.colors.sequential.Rainbow)
                fig3.update_layout(title_text="👥 Total Customers Per Cluster", title_x=0.5)
                st.plotly_chart(fig3, use_container_width=True)
                st.markdown("</div>", unsafe_allow_html=True)

            with col4:
                st.markdown("<div class='chart-container'>", unsafe_allow_html=True)
                st.markdown("<p class='chart-title'>💸 Average Purchase Per Cluster</p>", unsafe_allow_html=True)
                avg_purchase = df.groupby("cluster")["purchase_amount"].mean().reset_index()
                fig4 = px.bar(avg_purchase, x="cluster", y="purchase_amount", color="cluster",
                            labels={"cluster": "Cluster", "purchase_amount": "Avg Purchase"},
                            color_continuous_scale="magma")
                fig4.update_layout(title_text="💸 Average Purchase Per Cluster", title_x=0.5)
                st.plotly_chart(fig4, use_container_width=True)
                st.markdown("</div>", unsafe_allow_html=True)

            # Add customer data table
            st.markdown("<div class='chart-container'>", unsafe_allow_html=True)
            st.markdown("<p class='chart-title'>📋 Latest Customer Data</p>", unsafe_allow_html=True)
            st.dataframe(df[["customer_id", "name", "age", "purchase_amount", "cluster", "created_at"]].style.format({"purchase_amount": "${:.2f}"}), use_container_width=True)
            st.markdown("</div>", unsafe_allow_html=True)

            # Add alert for high purchases
            if df["purchase_amount"].max() > 400:
                st.error("⚠️ Alert: High purchase amount detected! Check cluster data.")

            # Export data
            if st.download_button("📥 Download Data as CSV", df.to_csv(index=False), "customer_segments.csv", "text/csv"):
<<<<<<< HEAD
                st.success("Data downloaded successfully!")
=======
                st.success("Data downloaded successfully!")
>>>>>>> 035dd0f6c347e75ed81045c6841acaf08148db93
