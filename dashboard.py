import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine, text
import urllib.parse
import time
from datetime import datetime, timezone, timedelta
import traceback

# Streamlit config
st.set_page_config(page_title="📊 Real-Time Customer Segmentation", layout="wide")

# Initialize session state
if 'refresh_counter' not in st.session_state:
    st.session_state['refresh_counter'] = 0
if 'df' not in st.session_state:
    st.session_state.df = pd.DataFrame()
if 'last_timestamp' not in st.session_state:
    st.session_state.last_timestamp = None
if 'db_connected' not in st.session_state:
    st.session_state.db_connected = False
if 'error_message' not in st.session_state:
    st.session_state.error_message = None
if 'connection_tested' not in st.session_state:
    st.session_state.connection_tested = False

# Display running indicator
st.sidebar.markdown(f"### 🟢 Dashboard Running")
st.sidebar.markdown(f"🕒 Last refresh: {datetime.now().strftime('%H:%M:%S')}")
st.sidebar.markdown(f"🔄 Refresh count: {st.session_state['refresh_counter']}")

# Database connection settings
DB_HOST = "ep-dry-violet-a4v38rh7-pooler.us-east-1.aws.neon.tech"
DB_PORT = 5432
DB_NAME = "neondb"
DB_USER = "neondb_owner"
DB_PASSWORD = urllib.parse.quote_plus("npg_5UbnztxlVuD1")
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode=require"

# Main title
st.title("📊 Real-Time Customer Segmentation Dashboard")

# Test database connection
try:
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        st.session_state.db_connected = True
        st.sidebar.success("✅ Database connected successfully")
        
        # Check if table exists
        result = conn.execute(text("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'customer_segments')"))
        table_exists = result.fetchone()[0]
        
        if table_exists:
            # Count records
            result = conn.execute(text("SELECT COUNT(*) FROM customer_segments"))
            count = result.fetchone()[0]
            st.sidebar.success(f"✅ Found customer_segments table with {count} records")
            
            if count == 0:
                st.warning("⚠️ Table exists but contains no records. Make sure your producer and consumer are running.")
        else:
            st.error("❌ The 'customer_segments' table does not exist in the database.")
            st.stop()
            
except Exception as e:
    st.sidebar.error(f"❌ Database connection error")
    st.error(f"❌ Database connection failed: {str(e)}")
    st.error(f"Details: {traceback.format_exc()}")
    st.session_state.error_message = f"Database connection failed: {str(e)}"
    st.stop()

# Sidebar filters
st.sidebar.header("🔍 Filters")
auto_refresh = st.sidebar.checkbox("Enable Auto-Refresh (5s)", value=True)

# More lenient time range approach
time_range = st.sidebar.slider("Time Range (hours)", 1, 168, 48)

# Use a very old from_time to ensure we get data
from_time = datetime.now(timezone.utc) - timedelta(hours=time_range)

# Cluster filter
clusters = st.sidebar.multiselect("Clusters", [0, 1, 2], default=[0, 1, 2])

# Purchase amount range - with better error handling
try:
    with engine.connect() as conn:
        min_query = text("SELECT COALESCE(MIN(purchase_amount), 0) as min FROM customer_segments")
        max_query = text("SELECT COALESCE(MAX(purchase_amount), 1000) as max FROM customer_segments")
        
        min_result = conn.execute(min_query).fetchone()
        max_result = conn.execute(max_query).fetchone()
        
        min_val = float(min_result[0])
        max_val = float(max_result[0])
        
        # Ensure we have a valid range
        if min_val >= max_val:
            min_val = 0
            max_val = 1000
            
        purchase_min, purchase_max = st.sidebar.slider("Purchase Amount", 
            min_val, max_val, (min_val, max_val)
        )
except Exception as e:
    st.sidebar.warning(f"Could not fetch purchase limits: {str(e)[:100]}")
    purchase_min, purchase_max = 0.0, 1000.0

# Debug information
st.sidebar.markdown("### 🔍 Debug Information")
st.sidebar.write(f"From Time: {from_time}")
st.sidebar.write(f"Selected Clusters: {clusters}")
st.sidebar.write(f"Purchase Range: ${purchase_min:.2f} - ${purchase_max:.2f}")

# Fetch data function with better error handling
def fetch_data(_from_time, _clusters, _purchase_min, _purchase_max):
    if not _clusters:  # If no clusters selected, return empty DataFrame
        st.warning("Please select at least one cluster.")
        return pd.DataFrame()
    
    try:
        # Try direct SQL approach
        query = f"""
            SELECT record_id, customer_id, name, age, purchase_amount, cluster, created_at
            FROM customer_segments
            WHERE created_at >= '{_from_time.isoformat()}'
            AND cluster IN ({','.join(map(str, _clusters))})
            AND purchase_amount BETWEEN {_purchase_min} AND {_purchase_max}
            ORDER BY created_at DESC
            LIMIT 100
        """
        
        st.sidebar.code(query, language="sql")
        
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)
            
        if not df.empty:
            st.session_state.last_timestamp = df['created_at'].max()
            st.sidebar.success(f"✅ Retrieved {len(df)} records")
        else:
            st.sidebar.warning("⚠️ Query returned no data")
            
        return df
        
    except Exception as e:
        st.sidebar.error(f"❌ Error fetching data")
        st.error(f"Error fetching data: {str(e)}")
        st.error(f"Details: {traceback.format_exc()}")
        return pd.DataFrame()

# Auto-refresh counter
if auto_refresh:
    st.session_state['refresh_counter'] += 1

# Manual refresh button
if st.button("🔄 Manual Refresh"):
    with st.spinner("Fetching real-time data..."):
        new_df = fetch_data(from_time, clusters, purchase_min, purchase_max)
        if not new_df.empty:
            st.session_state.df = new_df
        else:
            st.warning("No data available with current filters. Try adjusting your filters.")

# Initial data load if needed
if st.session_state.df.empty:
    with st.spinner("Initial data loading..."):
        new_df = fetch_data(from_time, clusters, purchase_min, purchase_max)
        if not new_df.empty:
            st.session_state.df = new_df

df = st.session_state.df

# Dashboard rendering
if df.empty:
    st.warning("⚠️ No data available. Check database connection or data filters.")
    
    # Show a test query output
    st.subheader("Database Diagnostic")
    try:
        with engine.connect() as conn:
            test_query = text("SELECT NOW() as current_time, version() as pg_version")
            result = conn.execute(test_query).fetchone()
            st.success(f"Database is accessible. Current time: {result[0]}, Version: {result[1][:20]}...")
            
            # Try to get the latest records regardless of filters
            latest_query = text("SELECT * FROM customer_segments ORDER BY created_at DESC LIMIT 5")
            latest_df = pd.read_sql(latest_query, conn)
            
            if not latest_df.empty:
                st.success(f"Found {len(latest_df)} recent records in the database. Here's a sample:")
                st.dataframe(latest_df)
            else:
                st.error("No records found in the customer_segments table. Make sure your producer and consumer are running.")
    except Exception as e:
        st.error(f"Diagnostic query failed: {str(e)}")
else:
    st.success(f"✅ Showing {len(df)} records. Last updated: {st.session_state.last_timestamp}")
    
    # Create dashboard layout
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Purchase Amount by Cluster")
        fig1 = px.bar(df, x="cluster", y="purchase_amount", color="cluster", 
                    title="Purchase Amount Distribution",
                    labels={"cluster": "Customer Cluster", "purchase_amount": "Purchase Amount ($)"})
        st.plotly_chart(fig1, use_container_width=True)

    with col2:
        st.subheader("Age Distribution by Cluster")
        fig2 = px.histogram(df, x="age", color="cluster", barmode="overlay",
                           title="Age Distribution",
                           labels={"age": "Customer Age", "count": "Number of Customers"})
        st.plotly_chart(fig2, use_container_width=True)

    col3, col4 = st.columns(2)

    with col3:
        st.subheader("Cluster Distribution")
        cluster_counts = df["cluster"].value_counts().reset_index()
        cluster_counts.columns = ["Cluster", "Total"]
        fig3 = px.pie(cluster_counts, values="Total", names="Cluster",
                     title="Customer Segment Distribution")
        st.plotly_chart(fig3, use_container_width=True)

    with col4:
        st.subheader("Average Purchase by Cluster")
        avg_purchase = df.groupby("cluster")["purchase_amount"].mean().reset_index()
        fig4 = px.bar(avg_purchase, x="cluster", y="purchase_amount", color="cluster",
                     title="Average Purchase Amount",
                     labels={"cluster": "Customer Cluster", "purchase_amount": "Avg Purchase Amount ($)"})
        st.plotly_chart(fig4, use_container_width=True)

    # Data table
    st.subheader("Customer Data")
    st.dataframe(df)

    # Download button
    st.download_button("⬇ Download CSV", df.to_csv(index=False), "customer_segments.csv", "text/csv")

# Force refresh button at bottom of page
st.markdown("---")
if st.button("🔄 Force Full Page Refresh"):
    st.experimental_rerun()

# Auto-refresh mechanism
if auto_refresh:
    time.sleep(5)
    st.experimental_rerun()
