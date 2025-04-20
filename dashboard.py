import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
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

# Initialize session state for persistent storage
if 'refresh_counter' not in st.session_state:
    st.session_state['refresh_counter'] = 0
if 'last_refresh_time' not in st.session_state:
    st.session_state['last_refresh_time'] = datetime.now().strftime('%H:%M:%S')
if 'last_data_timestamp' not in st.session_state:
    st.session_state['last_data_timestamp'] = None
if 'refresh_timer' not in st.session_state:
    st.session_state['refresh_timer'] = time.time()
if 'previous_data' not in st.session_state:
    st.session_state['previous_data'] = None

# Function to establish database connection (no caching)
def get_database_engine():
    return create_engine(DATABASE_URL)

# Sidebar configuration and styling
st.sidebar.markdown("""
    <div style='background-color:#27ae60; padding:10px; border-radius:5px; margin-bottom:10px;'>
        <h2 style='color:white; margin:0;'>🟢 Dashboard Running</h2>
    </div>
""", unsafe_allow_html=True)

st.sidebar.markdown(f"### 🕒 Last refresh: {st.session_state['last_refresh_time']}")
st.sidebar.markdown(f"### 🔄 Refresh count: {st.session_state['refresh_counter']}")

# Connect to database and show status
try:
    engine = get_database_engine()
    with engine.connect() as conn:
        st.sidebar.markdown("""
            <div style='background-color:#d4edda; color:#155724; padding:10px; border-radius:5px; margin-bottom:10px;'>
                <span style='font-weight:bold'>✅ Database connected successfully</span>
            </div>
        """, unsafe_allow_html=True)
        
        result = conn.execute(text("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'customer_segments')"))
        table_exists = result.fetchone()[0]
        
        if table_exists:
            result = conn.execute(text("SELECT COUNT(*) FROM customer_segments"))
            count = result.fetchone()[0]
            st.sidebar.markdown(f"""
                <div style='background-color:#d4edda; color:#155724; padding:10px; border-radius:5px; margin-bottom:20px;'>
                    <span style='font-weight:bold'>✅ Found customer_segments table with {count} records</span>
                </div>
            """, unsafe_allow_html=True)
except Exception as e:
    st.sidebar.error(f"❌ Database connection error: {str(e)}")
    st.error(f"Database connection failed: {str(e)}")
    st.stop()

# Main title
st.title("📊 Real-Time Customer Segmentation Dashboard")

# Sidebar filters section
st.sidebar.markdown("""
    <div style='margin-top:20px;'>
        <h3 style='color:#2c3e50;'>🔍 Filters</h3>
    </div>
""", unsafe_allow_html=True)

auto_refresh = st.sidebar.checkbox("Enable Auto-Refresh (5s)", value=True)

# Time range filter with custom styling
st.sidebar.markdown("Time Range (hours)")
time_range = st.sidebar.slider("", 1, 168, 48, key="time_range_slider", 
                              help="Filter data based on how many hours back to look")
from_time = datetime.now(timezone.utc) - timedelta(hours=time_range)

# Cluster filter
st.sidebar.markdown("Clusters")
clusters = st.sidebar.multiselect("", [0, 1, 2], default=[0, 1, 2], 
                                 format_func=lambda x: f"Cluster {x}")
if not clusters:
    clusters = [0, 1, 2]

# Purchase amount range
try:
    with engine.connect() as conn:
        min_query = text("SELECT COALESCE(MIN(purchase_amount), 0) as min FROM customer_segments")
        max_query = text("SELECT COALESCE(MAX(purchase_amount), 5000) as max FROM customer_segments")
        
        min_result = conn.execute(min_query).fetchone()
        max_result = conn.execute(max_query).fetchone()
        
        min_val = float(min_result[0])
        max_val = float(max_result[0])
        
        if min_val >= max_val:
            min_val = 0
            max_val = 5000
            
        st.sidebar.markdown("Purchase Amount")
        purchase_min, purchase_max = st.sidebar.slider("", 
            min_val, max_val, (min_val, max_val), key="purchase_amount_slider")
except Exception as e:
    st.sidebar.warning(f"Could not fetch purchase limits: {str(e)[:100]}")
    st.sidebar.markdown("Purchase Amount")
    purchase_min, purchase_max = st.sidebar.slider("", 0.0, 5000.0, (0.0, 5000.0))

# Function to fetch data with improved query and error handling
def fetch_data():
    try:
        engine = get_database_engine()
        cluster_str = ','.join(map(str, clusters)) if clusters else '0,1,2'
        from_time = datetime.now(timezone.utc) - timedelta(hours=time_range)
        
        query = f"""
            SELECT record_id, customer_id, name, age, purchase_amount, cluster, created_at
            FROM customer_segments
            WHERE created_at >= '{from_time.isoformat()}'
            AND cluster IN ({cluster_str})
            AND purchase_amount BETWEEN {purchase_min} AND {purchase_max}
            ORDER BY created_at DESC
            LIMIT 100
        """
        st.sidebar.markdown(f"**Debug Query**: {query}")
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)
            if df is not None and not df.empty:
                st.session_state['last_data_timestamp'] = df['created_at'].max()
                prev_data = st.session_state.get('previous_data', pd.DataFrame())
                if not prev_data.empty and not df.empty:
                    if df['record_id'].iloc[0] != prev_data['record_id'].iloc[0]:
                        st.balloons()
                st.session_state['previous_data'] = df.copy()
            else:
                st.session_state['last_data_timestamp'] = None
            st.sidebar.markdown(f"**Debug Rows Fetched**: {len(df) if df is not None else 0}")
            return df if df is not None else pd.DataFrame()
    except Exception as e:
        st.error(f"Error fetching data: {str(e)}")
        st.sidebar.error(f"**Debug Error**: {str(e)}")
        return pd.DataFrame()

# Manual refresh button
col_refresh, col_space = st.columns([1, 3])
with col_refresh:
    if st.button("🔄 Manual Refresh", key="manual_refresh"):
        with st.spinner("Fetching real-time data..."):
            df = fetch_data()
            st.session_state['refresh_counter'] += 1
            st.session_state['last_refresh_time'] = datetime.now().strftime('%H:%M:%S')
    else:
        df = fetch_data()
        st.session_state['refresh_counter'] += 1
        st.session_state['last_refresh_time'] = datetime.now().strftime('%H:%M:%S')

# Dashboard rendering
if df is None or df.empty:
    st.warning("⚠️ No data available. Check database connection or data filters.")
    st.markdown("### Database Diagnostic")
    try:
        with engine.connect() as conn:
            test_query = text("SELECT NOW() as current_time")
            result = conn.execute(test_query).fetchone()
            st.success(f"Database is accessible. Current time: {result[0]}")
            
            latest_query = text("SELECT * FROM customer_segments ORDER BY created_at DESC LIMIT 5")
            latest_df = pd.read_sql(latest_query, conn)
            if latest_df is not None and not latest_df.empty:
                st.success(f"Found {len(latest_df)} recent records in the database. Here's a sample:")
                st.dataframe(latest_df)
            else:
                st.error("No recent records found in the customer_segments table.")
    except Exception as e:
        st.error(f"Diagnostic query failed: {str(e)}")
else:
    st.success(f"✅ Showing {len(df)} records. Last updated: {st.session_state['last_data_timestamp']}")
    
    cluster_colors = {
        0: "#1e6e50",
        1: "#38a169",
        2: "#9ae6b4"
    }
    
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("<h3 style='color:#6c5ce7;'>💰 Cluster-wise Purchase Distribution</h3>", unsafe_allow_html=True)
        fig1 = px.bar(df, x="cluster", y="purchase_amount", color="cluster",
                      title="Purchase Amount Distribution",
                      labels={"cluster": "Customer Cluster", "purchase_amount": "Purchase Amount (₹)"},
                      template="plotly_white",
                      color_discrete_map=cluster_colors)
        fig1.update_layout(height=300)
        st.plotly_chart(fig1, use_container_width=True)

    with col2:
        st.markdown("<h3 style='color:#6c5ce7;'>👥 Age Distribution by Cluster</h3>", unsafe_allow_html=True)
        fig2 = px.histogram(df, x="age", color="cluster", barmode="overlay",
                            title="Age Distribution",
                            labels={"age": "Customer Age", "count": "Number of Customers"},
                            template="plotly_white",
                            color_discrete_map=cluster_colors)
        fig2.update_layout(height=300)
        st.plotly_chart(fig2, use_container_width=True)

    col3, col4 = st.columns(2)
    with col3:
        st.markdown("<h3 style='color:#6c5ce7;'>👤 Total Customers Per Cluster</h3>", unsafe_allow_html=True)
        cluster_counts = df["cluster"].value_counts().reset_index()
        cluster_counts.columns = ["Cluster", "Total"]
        total = cluster_counts["Total"].sum()
        cluster_counts["Percentage"] = cluster_counts["Total"] / total * 100
        fig3 = px.pie(cluster_counts, values="Total", names="Cluster",
                      title="Customer Segment Distribution",
                      template="plotly_white",
                      color="Cluster",
                      color_discrete_map=cluster_colors,
                      hover_data=["Percentage"])
        fig3.update_traces(hovertemplate='<b>Cluster %{label}</b><br>Count: %{value}<br>Percentage: %{customdata[0]:.1f}%')
        fig3.update_layout(height=300)
        st.plotly_chart(fig3, use_container_width=True)

    with col4:
        st.markdown("<h3 style='color:#6c5ce7;'>💲 Average Purchase Per Cluster</h3>", unsafe_allow_html=True)
        avg_purchase = df.groupby("cluster")["purchase_amount"].mean().reset_index()
        fig4 = px.bar(avg_purchase, x="cluster", y="purchase_amount", color="cluster",
                      title="Average Purchase Amount",
                      labels={"cluster": "Customer Cluster", "purchase_amount": "Avg Purchase Amount (₹)"},
                      template="plotly_white",
                      color_discrete_map=cluster_colors)
        fig4.update_layout(height=300, yaxis=dict(tickprefix="₹"))
        st.plotly_chart(fig4, use_container_width=True)

    st.subheader("Latest Customer Data")
    df_display = df.copy()
    df_display['purchase_amount'] = df_display['purchase_amount'].apply(lambda x: f"₹{x:.2f}")
    df_display['created_at'] = pd.to_datetime(df_display['created_at']).dt.strftime('%Y-%m-%d %H:%M:%S')
    st.dataframe(df_display[['record_id', 'customer_id', 'name', 'age', 'purchase_amount', 'cluster', 'created_at']], use_container_width=True)

    st.download_button("⬇ Download CSV", df.to_csv(index=False), "customer_segments.csv", "text/csv")

st.markdown("---")
col_left, col_mid, col_right = st.columns([1, 1, 1])
with col_left:
    if st.button("🔄 Force Full Page Refresh", key="force_refresh"):
        st.rerun()

if auto_refresh:
    current_time = time.time()
    if current_time - st.session_state['refresh_timer'] >= 5:
        st.session_state['refresh_counter'] += 1
        st.session_state['last_refresh_time'] = datetime.now().strftime('%H:%M:%S')
        st.session_state['refresh_timer'] = current_time
        st.rerun()
    time_left = max(0, 5 - (time.time() - st.session_state['refresh_timer']))
    if time_left > 0:
        st.sidebar.markdown(f"Next refresh in: {time_left:.1f}s")
