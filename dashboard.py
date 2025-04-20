import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine, text
import urllib.parse
import time
from datetime import datetime, timezone, timedelta

# Streamlit config
st.set_page_config(page_title="📊 Real-Time Customer Segmentation", layout="wide")

# Database connection settings for Neon Database
DB_HOST = "ep-dry-violet-a4v38rh7-pooler.us-east-1.aws.neon.tech"
DB_PORT = 5432
DB_NAME = "neondb"
DB_USER = "neondb_owner"
DB_PASSWORD = urllib.parse.quote_plus("npg_5UbnztxlVuD1")
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode=require"

# Initialize session state for persistent storage - PROPERLY INITIALIZED FIRST
if 'refresh_counter' not in st.session_state:
    st.session_state['refresh_counter'] = 0
if 'last_refresh_time' not in st.session_state:
    st.session_state['last_refresh_time'] = datetime.now().strftime('%H:%M:%S')
if 'last_data_timestamp' not in st.session_state:
    st.session_state['last_data_timestamp'] = None
if 'refresh_timer' not in st.session_state:
    st.session_state['refresh_timer'] = time.time()

# Function to establish database connection
@st.cache_resource
def get_database_engine():
    return create_engine(DATABASE_URL)

# Create sidebar status indicators
st.sidebar.markdown("# 🟢 Dashboard Running")
st.sidebar.markdown(f"### 🕒 Last refresh: {st.session_state['last_refresh_time']}")
st.sidebar.markdown(f"### 🔄 Refresh count: {st.session_state['refresh_counter']}")

# Connect to database and show status
try:
    engine = get_database_engine()
    with engine.connect() as conn:
        st.sidebar.success("✅ Database connected successfully")
        
        # Check if table exists
        result = conn.execute(text("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'customer_segments')"))
        table_exists = result.fetchone()[0]
        
        if table_exists:
            # Count records
            result = conn.execute(text("SELECT COUNT(*) FROM customer_segments"))
            count = result.fetchone()[0]
            st.sidebar.success(f"✅ Found customer_segments table with {count} records")
except Exception as e:
    st.sidebar.error(f"❌ Database connection error: {str(e)}")
    st.error(f"Database connection failed: {str(e)}")
    st.stop()

# Main title
st.title("📊 Real-Time Customer Segmentation Dashboard")

# Sidebar filters
st.sidebar.header("🔍 Filters")
auto_refresh = st.sidebar.checkbox("Enable Auto-Refresh (5s)", value=True)

# Time range filter
time_range = st.sidebar.slider("Time Range (hours)", 1, 168, 48)
from_time = datetime.now(timezone.utc) - timedelta(hours=time_range)

# Cluster filter
clusters = st.sidebar.multiselect("Clusters", [0, 1, 2], default=[0, 1, 2])
if not clusters:
    clusters = [0, 1, 2]  # Default to all clusters if none selected

# Purchase amount range
try:
    with engine.connect() as conn:
        min_query = text("SELECT COALESCE(MIN(purchase_amount), 0) as min FROM customer_segments")
        max_query = text("SELECT COALESCE(MAX(purchase_amount), 1000) as max FROM customer_segments")
        
        min_result = conn.execute(min_query).fetchone()
        max_result = conn.execute(max_query).fetchone()
        
        min_val = float(min_result[0])
        max_val = float(max_result[0])
        
        if min_val >= max_val:
            min_val = 0
            max_val = 1000
            
        purchase_min, purchase_max = st.sidebar.slider("Purchase Amount", 
            min_val, max_val, (min_val, max_val)
        )
except Exception as e:
    st.sidebar.warning(f"Could not fetch purchase limits: {str(e)[:100]}")
    purchase_min, purchase_max = 0.0, 1000.0

# Function to fetch data - critical part for real-time updates
def fetch_data():
    try:
        # Safely handle cluster list for SQL
        cluster_str = ','.join(map(str, clusters)) if clusters else '0,1,2'
        
        query = f"""
            SELECT record_id, customer_id, name, age, purchase_amount, cluster, created_at
            FROM customer_segments
            WHERE created_at >= '{from_time.isoformat()}'
            AND cluster IN ({cluster_str})
            AND purchase_amount BETWEEN {purchase_min} AND {purchase_max}
            ORDER BY created_at DESC
            LIMIT 100
        """
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)
            if not df.empty:
                st.session_state['last_data_timestamp'] = df['created_at'].max()
            return df
    except Exception as e:
        st.error(f"Error fetching data: {str(e)}")
        return pd.DataFrame()

# Manual refresh button
if st.button("🔄 Manual Refresh"):
    with st.spinner("Fetching real-time data..."):
        df = fetch_data()
        st.session_state['refresh_counter'] += 1
        st.session_state['last_refresh_time'] = datetime.now().strftime('%H:%M:%S')
else:
    df = fetch_data()
    st.session_state['refresh_counter'] += 1
    st.session_state['last_refresh_time'] = datetime.now().strftime('%H:%M:%S')
    if df.empty:
        st.warning("⚠️ No new data fetched. Check database or filters.")

# Dashboard rendering - Enhanced with improved styling to match your screenshot
if df.empty:
    st.warning("⚠️ No data available. Check database connection or data filters.")
    
    st.subheader("Database Diagnostic")
    try:
        with engine.connect() as conn:
            test_query = text("SELECT NOW() as current_time")
            result = conn.execute(test_query).fetchone()
            st.success(f"Database is accessible. Current time: {result[0]}")
            
            latest_query = text("SELECT * FROM customer_segments ORDER BY created_at DESC LIMIT 5")
            latest_df = pd.read_sql(latest_query, conn)
            
            if not latest_df.empty:
                st.success(f"Found {len(latest_df)} recent records in the database. Here's a sample:")
                st.dataframe(latest_df)
            else:
                st.error("No records found in the customer_segments table.")
    except Exception as e:
        st.error(f"Diagnostic query failed: {str(e)}")
else:
    st.success(f"✅ Showing {len(df)} records. Last updated: {st.session_state['last_data_timestamp']}")
    
    # Dark theme styling with green accent colors
    plot_template = "plotly_dark"
    cluster_colors = {0: "#1E6E50", 1: "#38A169", 2: "#9AE6B4"}
    
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("💰 Cluster-wise Purchase Distribution")
        fig1 = px.bar(df, x="cluster", y="purchase_amount", color="cluster", 
                    title="Purchase Amount Distribution",
                    labels={"cluster": "Customer Cluster", "purchase_amount": "Purchase Amount (₹)"},
                    template=plot_template,
                    color_discrete_map=cluster_colors)
        fig1.update_layout(height=300)
        st.plotly_chart(fig1, use_container_width=True)

    with col2:
        st.subheader("👥 Age Distribution by Cluster")
        fig2 = px.histogram(df, x="age", color="cluster", barmode="overlay",
                           title="Age Distribution",
                           labels={"age": "Customer Age", "count": "Number of Customers"},
                           template=plot_template,
                           color_discrete_map=cluster_colors)
        fig2.update_layout(height=300)
        st.plotly_chart(fig2, use_container_width=True)

    col3, col4 = st.columns(2)

    with col3:
        st.subheader("👤 Total Customers Per Cluster")
        cluster_counts = df["cluster"].value_counts().reset_index()
        cluster_counts.columns = ["Cluster", "Total"]
        fig3 = px.pie(cluster_counts, values="Total", names="Cluster",
                     title="Customer Segment Distribution",
                     template=plot_template,
                     color="Cluster",
                     color_discrete_map=cluster_colors)
        fig3.update_layout(height=300)
        st.plotly_chart(fig3, use_container_width=True)

    with col4:
        st.subheader("💲 Average Purchase Per Cluster")
        avg_purchase = df.groupby("cluster")["purchase_amount"].mean().reset_index()
        fig4 = px.bar(avg_purchase, x="cluster", y="purchase_amount", color="cluster",
                     title="Average Purchase Amount",
                     labels={"cluster": "Customer Cluster", "purchase_amount": "Avg Purchase Amount (₹)"},
                     template=plot_template,
                     color_discrete_map=cluster_colors)
        fig4.update_layout(height=300)
        st.plotly_chart(fig4, use_container_width=True)

    st.subheader("Latest Customer Data")
    # Format the dataframe to match your screenshot
    df_display = df.copy()
    df_display['purchase_amount'] = df_display['purchase_amount'].apply(lambda x: f"₹{x:.2f}")
    df_display['created_at'] = pd.to_datetime(df_display['created_at']).dt.strftime('%Y-%m-%d %H:%M:%S')
    st.dataframe(df_display[['customer_id', 'name', 'age', 'purchase_amount', 'cluster', 'created_at']], use_container_width=True)

    st.download_button("⬇ Download CSV", df.to_csv(index=False), "customer_segments.csv", "text/csv")

st.markdown("---")
if st.button("🔄 Force Full Page Refresh"):
    st.rerun()

# Improved auto-refresh mechanism with better timing control
if auto_refresh:
    if time.time() - st.session_state['refresh_timer'] >= 5:
        st.session_state['refresh_counter'] += 1
        st.session_state['last_refresh_time'] = datetime.now().strftime('%H:%M:%S')
        st.session_state['refresh_timer'] = time.time()
        st.rerun()
