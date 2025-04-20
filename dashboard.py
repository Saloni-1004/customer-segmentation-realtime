import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine, text
import urllib.parse
import time
from datetime import datetime, timezone, timedelta
import uuid

# Streamlit config
st.set_page_config(
    page_title="📊 Real-Time Customer Segmentation", 
    layout="wide",
    initial_sidebar_state="expanded",
    page_icon="📊",
    menu_items={
        'About': "Real-Time Customer Segmentation Dashboard - Final Year Project"
    }
)

# Add custom CSS for dark theme optimization
st.markdown("""
<style>
    .stApp {
        background-color: #111827;
        color: #f1f5f9;
    }
    .css-18e3th9 {
        padding-top: 2rem;
        padding-bottom: 10rem;
    }
    .stPlotlyChart {
        background-color: #1e293b;
        border-radius: 8px;
        padding: 10px;
        border: 1px solid #334155;
    }
    h1, h2, h3, h4 {
        color: #e2e8f0 !important;
    }
    .stAlert {
        background-color: #374151;
        color: #f1f5f9;
    }
    .stDataFrame {
        background-color: #1e293b;
    }
    .stButton>button {
        background-color: #3b82f6;
        color: white;
    }
    .stDownloadButton>button {
        background-color: #10b981;
        color: white;
    }
</style>
""", unsafe_allow_html=True)

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

# Function to establish database connection with caching
@st.cache_data
def get_database_engine():
    max_retries = 3
    for attempt in range(max_retries):
        try:
            return create_engine(DATABASE_URL)
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(2)  # Wait before retrying
                continue
            raise Exception(f"Failed to connect to database after {max_retries} attempts: {str(e)}")

# Main title with updated styling
st.markdown("""
    <div style='background-color:#1e3a8a; padding:15px; border-radius:10px; margin-bottom:25px; text-align:center;'>
        <h1 style='color:#f0f9ff; margin:0;'>📊 Real-Time Customer Segmentation Dashboard</h1>
        <p style='color:#bfdbfe; margin-top:5px;'>Final Year College Project</p>
    </div>
""", unsafe_allow_html=True)

# Sidebar configuration and styling
st.sidebar.markdown("""
    <div style='background-color:#16a34a; padding:15px; border-radius:8px; margin-bottom:20px; text-align:center;'>
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
            <div style='background-color:#064e3b; color:#d1fae5; padding:12px; border-radius:8px; margin-bottom:20px;'>
                <span style='font-weight:bold'>✅ Database connected successfully</span>
            </div>
        """, unsafe_allow_html=True)
        
        result = conn.execute(text("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'customer_segments')"))
        table_exists = result.fetchone()[0]
        
        if table_exists:
            result = conn.execute(text("SELECT COUNT(*) FROM customer_segments"))
            count = result.fetchone()[0]
            st.sidebar.markdown(f"""
                <div style='background-color:#064e3b; color:#d1fae5; padding:12px; border-radius:8px; margin-bottom:20px;'>
                    <span style='font-weight:bold'>✅ Found customer_segments table with {count} records</span>
                </div>
            """, unsafe_allow_html=True)
            if count == 0:
                st.sidebar.warning("⚠️ The customer_segments table is empty. Add records to display data.")
        else:
            st.sidebar.error("❌ Table 'customer_segments' does not exist.")
            st.stop()
except Exception as e:
    st.sidebar.error(f"❌ Database connection error: {str(e)}. Check credentials or network.")
    st.error(f"Database connection failed: {str(e)}. Please verify the database settings and try again.")
    st.stop()

# Sidebar filters section with improved styling
st.sidebar.markdown("""
    <div style='margin-top:20px; background-color:#1e293b; padding:15px; border-radius:8px;'>
        <h3 style='color:#94a3b8; margin-top:0;'>🔍 Dashboard Filters</h3>
    </div>
""", unsafe_allow_html=True)

# Auto-refresh with customizable interval
st.sidebar.markdown("<p style='margin-bottom:5px; color:#94a3b8;'>Refresh Settings</p>", unsafe_allow_html=True)
auto_refresh = st.sidebar.checkbox(
    "Enable Auto-Refresh", 
    value=True,
    help="Automatically refresh dashboard data at the selected interval"
)
refresh_interval = st.sidebar.selectbox(
    "Refresh Interval",
    options=[5, 10, 30],
    format_func=lambda x: f"{x} seconds",
    help="Choose how often the dashboard refreshes",
    label_visibility="collapsed"
)

# Pagination settings
st.sidebar.markdown("<p style='margin-bottom:5px; color:#94a3b8;'>Records to Display</p>", unsafe_allow_html=True)
page_size = st.sidebar.selectbox(
    "Records per Page",
    options=[50, 100, 500, "All"],
    index=3,  # Default to "All"
    help="Choose how many records to display in the table",
    label_visibility="collapsed"
)

# Table height toggle
st.sidebar.markdown("<p style='margin-bottom:5px; color:#94a3b8;'>Table Display</p>", unsafe_allow_html=True)
use_fixed_height = st.sidebar.checkbox(
    "Use Fixed Table Height (600px)", 
    value=True,
    help="Toggle between fixed height (scrollable) and dynamic height (all rows visible)"
)

# Time range filter with broader default
st.sidebar.markdown("<p style='margin-bottom:5px; color:#94a3b8;'>Time Range (hours)</p>", unsafe_allow_html=True)
time_range = st.sidebar.slider(
    "Time Range", 
    1, 168, 168,  # Default to 168 hours (7 days)
    key="time_range_slider", 
    help="Filter data based on how many hours back to look", 
    label_visibility="collapsed"
)
from_time = datetime.now(timezone.utc) - timedelta(hours=time_range)

# Cluster filter
st.sidebar.markdown("<p style='margin-bottom:5px; color:#94a3b8;'>Customer Clusters</p>", unsafe_allow_html=True)
clusters = st.sidebar.multiselect(
    "Clusters", 
    [0, 1, 2], 
    default=[0, 1, 2], 
    format_func=lambda x: {
        0: "Cluster 0 (Low Spenders)",
        1: "Cluster 1 (Medium Spenders)",
        2: "Cluster 2 (High Spenders)"
    }.get(x), 
    label_visibility="collapsed"
)
if not clusters:
    clusters = [0, 1, 2]

# Purchase amount range with robust defaults
try:
    with engine.connect() as conn:
        min_query = text("SELECT COALESCE(MIN(purchase_amount), 0) as min FROM customer_segments")
        max_query = text("SELECT COALESCE(MAX(purchase_amount), 10000) as max FROM customer_segments")
        
        min_result = conn.execute(min_query).fetchone()
        max_result = conn.execute(max_query).fetchone()
        
        min_val = float(min_result[0])
        max_val = float(max_result[0])
        
        if min_val >= max_val:
            min_val = 0
            max_val = 10000
        
        st.sidebar.markdown("<p style='margin-bottom:5px; color:#94a3b8;'>Purchase Amount (₹)</p>", unsafe_allow_html=True)
        purchase_min, purchase_max = st.sidebar.slider(
            "Purchase Range", 
            min_val, max_val, (min_val, max_val), 
            key="purchase_amount_slider", 
            label_visibility="collapsed"
        )
except Exception as e:
    st.sidebar.warning(f"Could not fetch purchase limits: {str(e)[:100]}. Using defaults.")
    st.sidebar.markdown("<p style='margin-bottom:5px; color:#94a3b8;'>Purchase Amount (₹)</p>", unsafe_allow_html=True)
    purchase_min, purchase_max = st.sidebar.slider(
        "Purchase Range", 0.0, 10000.0, (0.0, 10000.0), 
        label_visibility="collapsed"
    )

# Function to fetch data without limit
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
        """
        if page_size != "All":
            query += f" LIMIT {page_size}"
        
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)
            if df is not None and not df.empty:
                st.session_state['last_data_timestamp'] = df['created_at'].max()
                prev_data = st.session_state.get('previous_data', pd.DataFrame())
                if not prev_data.empty and not df.empty:
                    if df['record_id'].iloc[0] != prev_data['record_id'].iloc[0]:
                        st.balloons()
                st.session_state['previous_data'] = df.copy()
                if len(df) > 1000:
                    st.warning("⚠️ Large dataset detected (>1000 records). Consider using pagination to improve performance.")
            else:
                st.session_state['last_data_timestamp'] = None
            return df if df is not None else pd.DataFrame()
    except Exception as e:
        st.error(f"Error fetching data: {str(e)}. Check database connection or query filters.")
        return pd.DataFrame()

# Manual refresh button
col_refresh, col_space = st.columns([1, 3])
with col_refresh:
    if st.button("🔄 Manual Refresh", key="manual_refresh", use_container_width=True):
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
    st.warning("⚠️ No data available. Check database connection, filters, or add records to customer_segments.")
    st.markdown("### Database Diagnostic")
    try:
        with engine.connect() as conn:
            test_query = text("SELECT NOW() as current_time")
            result = conn.execute(test_query).fetchone()
            st.success(f"Database is accessible. Current time: {result[0]}")
            
            latest_query = text("SELECT * FROM customer_segments ORDER BY created_at DESC")  # Removed LIMIT 5
            latest_df = pd.read_sql(latest_query, conn)
            if latest_df is not None and not latest_df.empty:
                st.success(f"Found {len(latest_df)} recent records in the database. Here's a sample:")
                st.dataframe(latest_df, use_container_width=True)
            else:
                st.error("No records found in the customer_segments table. Please add data.")
                # Fallback sample data
                sample_data = pd.DataFrame({
                    'record_id': [1, 2, 3],
                    'customer_id': ['C001', 'C002', 'C003'],
                    'name': ['John Doe', 'Jane Smith', 'Alice Johnson'],
                    'age': [30, 45, 25],
                    'purchase_amount': [1500.50, 3500.75, 7500.25],
                    'cluster': [0, 1, 2],
                    'created_at': ['2025-04-20 10:00:00', '2025-04-20 11:00:00', '2025-04-20 12:00:00']
                })
                st.markdown("### Sample Data Preview")
                st.dataframe(sample_data, use_container_width=True)
    except Exception as e:
        st.error(f"Diagnostic query failed: {str(e)}. Check database settings.")
else:
    # Success message
    st.markdown(f"""
        <div style='background-color:#064e3b; color:#d1fae5; padding:12px; border-radius:8px; margin-bottom:20px;'>
            <span style='font-weight:bold'>✅ Showing {len(df)} records. Last updated: {st.session_state['last_refresh_time']}</span>
        </div>
    """, unsafe_allow_html=True)
    
    # Color scheme
    cluster_colors = {
        0: "#22c55e",  # Bright green
        1: "#3b82f6",  # Bright blue
        2: "#f59e0b"   # Bright amber
    }
    
    cluster_names = {
        0: "Low Spenders",
        1: "Medium Spenders",
        2: "High Spenders"
    }
    
    # Add cluster names to dataframe
    df['cluster_name'] = df['cluster'].apply(lambda x: cluster_names.get(x, f"Cluster {x}"))
    
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("<h3 style='color:#d1d5db;'>💰 Cluster-wise Purchase Distribution</h3>", unsafe_allow_html=True)
        fig1 = px.bar(
            df, 
            x="cluster", 
            y="purchase_amount", 
            color="cluster",
            title="Purchase Amount by Customer Segment",
            labels={
                "cluster": "Customer Cluster", 
                "purchase_amount": "Purchase Amount (₹)"
            },
            template="plotly_dark",
            color_discrete_map=cluster_colors,
            text_auto='.2s'
        )
        
        fig1.update_layout(
            height=350,
            plot_bgcolor="#1e293b",
            paper_bgcolor="#1e293b",
            font=dict(color="#e2e8f0"),
            xaxis=dict(
                title="Customer Cluster",
                titlefont=dict(size=14),
                tickmode='array',
                tickvals=[0, 1, 2],
                ticktext=["Low Spenders", "Medium Spenders", "High Spenders"]
            ),
            yaxis=dict(
                title="Purchase Amount (₹)",
                titlefont=dict(size=14),
                gridcolor="#475569"
            )
        )
        st.plotly_chart(fig1, use_container_width=True)

    with col2:
        st.markdown("<h3 style='color:#d1d5db;'>👥 Age Distribution by Cluster</h3>", unsafe_allow_html=True)
        fig2 = px.histogram(
            df, 
            x="age", 
            color="cluster", 
            barmode="overlay",
            title="Customer Age Distribution",
            labels={
                "age": "Customer Age", 
                "count": "Number of Customers"
            },
            template="plotly_dark",
            color_discrete_map=cluster_colors,
            opacity=0.8
        )
        
        fig2.update_layout(
            height=350,
            bargap=0.1,
            plot_bgcolor="#1e293b",
            paper_bgcolor="#1e293b",
            font=dict(color="#e2e8f0"),
            xaxis=dict(
                title="Customer Age (years)", 
                titlefont=dict(size=14),
                gridcolor="#475569" 
            ),
            yaxis=dict(
                title="Number of Customers", 
                titlefont=dict(size=14),
                gridcolor="#475569"
            ),
            legend=dict(
                title="Cluster",
                itemsizing="constant",
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            )
        )
        st.plotly_chart(fig2, use_container_width=True)

    col3, col4 = st.columns(2)
    with col3:
        st.markdown("<h3 style='color:#d1d5db;'>👤 Total Customers Per Cluster</h3>", unsafe_allow_html=True)
        cluster_counts = df["cluster"].value_counts().reset_index()
        cluster_counts.columns = ["Cluster", "Total"]
        total = cluster_counts["Total"].sum()
        cluster_counts["Percentage"] = cluster_counts["Total"] / total * 100
        
        cluster_counts["Cluster_Name"] = cluster_counts["Cluster"].map(cluster_names)
        
        fig3 = px.pie(
            cluster_counts, 
            values="Total", 
            names="Cluster_Name",
            title="Customer Segment Distribution",
            template="plotly_dark",
            color="Cluster",
            color_discrete_map={
                "Low Spenders": cluster_colors[0],
                "Medium Spenders": cluster_colors[1],
                "High Spenders": cluster_colors[2]
            },
            hover_data=["Percentage"]
        )
        
        fig3.update_traces(
            textposition='inside', 
            textinfo='percent+label',
            hovertemplate='<b>%{label}</b><br>Count: %{value}<br>Percentage: %{customdata[0]:.1f}%'
        )
        
        fig3.update_layout(
            height=350,
            plot_bgcolor="#1e293b",
            paper_bgcolor="#1e293b",
            font=dict(color="#e2e8f0"),
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=-0.3,
                xanchor="center",
                x=0.5
            )
        )
        st.plotly_chart(fig3, use_container_width=True)

    with col4:
        st.markdown("<h3 style='color:#d1d5db;'>💲 Average Purchase Per Cluster</h3>", unsafe_allow_html=True)
        avg_purchase = df.groupby("cluster")["purchase_amount"].mean().reset_index()
        
        avg_purchase["cluster_name"] = avg_purchase["cluster"].map(cluster_names)
        
        fig4 = px.bar(
            avg_purchase, 
            x="cluster", 
            y="purchase_amount", 
            color="cluster",
            title="Average Purchase Amount by Segment",
            labels={
                "cluster": "Customer Cluster", 
                "purchase_amount": "Avg Purchase Amount (₹)"
            },
            template="plotly_dark",
            color_discrete_map=cluster_colors,
            text_auto='.2s'
        )
        
        fig4.update_layout(
            height=350,
            plot_bgcolor="#1e293b",
            paper_bgcolor="#1e293b",
            font=dict(color="#e2e8f0"),
            xaxis=dict(
                title="Customer Segment",
                titlefont=dict(size=14),
                tickmode='array',
                tickvals=[0, 1, 2],
                ticktext=["Low Spenders", "Medium Spenders", "High Spenders"],
                gridcolor="#475569"
            ),
            yaxis=dict(
                title="Average Purchase Amount (₹)",
                titlefont=dict(size=14),
                gridcolor="#475569"
            )
        )
        st.plotly_chart(fig4, use_container_width=True)

    # Latest customer data display
    st.markdown("<h3 style='color:#d1d5db;'>📋 Latest Customer Data</h3>", unsafe_allow_html=True)
    
    # Format data for display
    df_display = df.copy()
    df_display['purchase_amount'] = df_display['purchase_amount'].apply(lambda x: f"₹{x:.2f}")
    df_display['created_at'] = pd.to_datetime(df_display['created_at']).dt.strftime('%Y-%m-%d %H:%M:%S')
    df_display['cluster'] = df_display['cluster'].apply(lambda x: f"{x} - {cluster_names.get(x, '')}")
    
    # Display table with flexible height
    if use_fixed_height:
        st.dataframe(
            df_display[['record_id', 'customer_id', 'name', 'age', 'purchase_amount', 'cluster', 'created_at']], 
            use_container_width=True,
            height=600  # Increased for more rows
        )
    else:
        st.dataframe(
            df_display[['record_id', 'customer_id', 'name', 'age', 'purchase_amount', 'cluster', 'created_at']], 
            use_container_width=True
        )

    # Download button
    st.download_button(
        "⬇ Download Customer Data (CSV)",
        df.to_csv(index=False),
        "customer_segments.csv",
        "text/csv",
        key="download-csv"
    )

# Footer with project info
st.markdown("---")
col_left, col_mid, col_right = st.columns([1, 2, 1])
with col_left:
    if st.button("🔄 Force Full Refresh", key="force_refresh", use_container_width=True):
        st.rerun()

with col_mid:
    st.markdown("""
        <div style='text-align:center;'>
            <p style='color:#94a3b8;'>Real-Time Customer Segmentation System</p>
            <p style='color:#64748b; font-size:0.8em;'>Final Year College Project | Kafka | PostgreSQL | Streamlit</p>
            <p style='color:#64748b; font-size:0.8em;'>Database Optimization: Consider adding indexes on created_at, cluster, and purchase_amount.</p>
        </div>
    """, unsafe_allow_html=True)

# Auto-refresh functionality
if auto_refresh:
    current_time = time.time()
    if current_time - st.session_state['refresh_timer'] >= refresh_interval:
        st.session_state['refresh_counter'] += 1
        st.session_state['last_refresh_time'] = datetime.now().strftime('%H:%M:%S')
        st.session_state['refresh_timer'] = current_time
        st.rerun()
    time_left = max(0, refresh_interval - (time.time() - st.session_state['refresh_timer']))
    if time_left > 0:
        st.sidebar.markdown(f"""
            <div style='background-color:#1e3a8a; padding:10px; border-radius:5px; margin-top:20px;'>
                <p style='color:#bfdbfe; margin:0;'>Next refresh in: {time_left:.1f}s</p>
            </div>
        """, unsafe_allow_html=True)
