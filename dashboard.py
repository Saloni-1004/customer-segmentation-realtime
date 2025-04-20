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
        'About': "Real-Time Customer Segmentation Dashboard"
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
@st.cache_data(ttl=300)
def get_database_engine():
    try:
        return create_engine(DATABASE_URL)
    except Exception as e:
        st.error(f"Database connection error: {str(e)}.")
        return None

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
db_connected = False
try:
    engine = get_database_engine()
    if engine:
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
                    st.sidebar.warning("⚠️ The customer_segments table is empty.")
                else:
                    db_connected = True
            else:
                st.sidebar.error("❌ Table 'customer_segments' does not exist.")
    else:
        st.sidebar.error("❌ Database engine could not be created.")
except Exception as e:
    st.sidebar.error(f"❌ Database connection error: {str(e)}.")

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

# Function to fetch data from database
def fetch_data():
    if db_connected:
        try:
            cluster_str = ','.join(map(str, clusters)) if clusters else '0,1,2'
            from_time = datetime.now(timezone.utc) - timedelta(hours=time_range)
            
            query = f"""
                SELECT record_id, customer_id, name, age, purchase_amount, cluster, created_at
                FROM customer_segments
                WHERE created_at >= '{from_time.isoformat()}'
                AND cluster IN ({cluster_str})
                ORDER BY created_at DESC
            """
            
            with engine.connect() as conn:
                df = pd.read_sql(query, conn)
                if df is not None and not df.empty:
                    st.session_state['last_data_timestamp'] = df['created_at'].max()
                    prev_data = st.session_state.get('previous_data', pd.DataFrame())
                    if not prev_data.empty and not df.empty:
                        if df['record_id'].iloc[0] != prev_data['record_id'].iloc[0]:
                            st.balloons()
                    st.session_state['previous_data'] = df.copy()
                    return df
                else:
                    return None
        except Exception as e:
            st.warning(f"⚠️ Error fetching data: {str(e)}.")
            return None
    else:
        return None

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
            height=600
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
    
    # Additional section for more realistic data visualization
    st.markdown("<h3 style='color:#d1d5db;'>📈 Customer Behavior Insights</h3>", unsafe_allow_html=True)
    col5, col6 = st.columns(2)
    
    with col5:
        # Purchase frequency over time
        st.markdown("<h4 style='color:#d1d5db;'>Purchase Frequency Timeline</h4>", unsafe_allow_html=True)
        df['date'] = pd.to_datetime(df['created_at']).dt.date
        purchase_timeline = df.groupby('date').size().reset_index(name='count')
        
        fig5 = px.line(
            purchase_timeline, 
            x='date', 
            y='count',
            title="Daily Purchase Frequency",
            labels={"date": "Date", "count": "Number of Purchases"},
            template="plotly_dark",
            markers=True
        )
        
        fig5.update_layout(
            height=300,
            plot_bgcolor="#1e293b",
            paper_bgcolor="#1e293b",
            font=dict(color="#e2e8f0"),
            xaxis=dict(title="Date", titlefont=dict(size=14), gridcolor="#475569"),
            yaxis=dict(title="Number of Purchases", titlefont=dict(size=14), gridcolor="#475569")
        )
        
        st.plotly_chart(fig5, use_container_width=True)
    
    with col6:
        # Age groups distribution
        st.markdown("<h4 style='color:#d1d5db;'>Age Group Distribution</h4>", unsafe_allow_html=True)
        
        # Create age bins
        bins = [18, 25, 35, 45, 55, 65, 100]
        labels = ['18-24', '25-34', '35-44', '45-54', '55-64', '65+']
        df['age_group'] = pd.cut(df['age'], bins=bins, labels=labels, right=False)
        
        age_distribution = df.groupby('age_group').size().reset_index(name='count')
        
        fig6 = px.bar(
            age_distribution,
            x='age_group',
            y='count',
            title="Customer Age Group Distribution",
            labels={"age_group": "Age Group", "count": "Number of Customers"},
            template="plotly_dark",
            color='age_group',
            color_discrete_sequence=px.colors.qualitative.Bold
        )
        
        fig6.update_layout(
            height=300,
            plot_bgcolor="#1e293b",
            paper_bgcolor="#1e293b",
            font=dict(color="#e2e8f0"),
            xaxis=dict(title="Age Group", titlefont=dict(size=14), gridcolor="#475569"),
            yaxis=dict(title="Number of Customers", titlefont=dict(size=14), gridcolor="#475569"),
            showlegend=False
        )
        
        st.plotly_chart(fig6, use_container_width=True)

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
            <p style='color:#64748b; font-size:0.8em;'>System: Automatic Customer Clustering with K-means</p>
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
