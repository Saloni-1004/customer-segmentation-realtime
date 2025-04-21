import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine, text
import urllib.parse
from datetime import datetime, timedelta

# Neon Database Connection
DB_HOST = "ep-dry-violet-a4v38rh7-pooler.us-east-1.aws.neon.tech"
DB_PORT = 5432
DB_NAME = "neondb"
DB_USER = "neondb_owner"
DB_PASSWORD = urllib.parse.quote_plus("npg_5UbnztxlVuD1")
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode=require"

# Database connection management
@st.cache_resource(ttl=300)  # Cache the engine but refresh every 5 minutes
def get_database_engine():
    try:
        return create_engine(DATABASE_URL)
    except Exception as e:
        st.error(f"⚠️ Database connection failed: {e}")
        return None

# Fetch data function with better error handling
def fetch_data(engine, from_time, clusters, purchase_min, purchase_max):
    if not engine:
        return pd.DataFrame()  # Return empty DataFrame
    
    try:
        # Convert clusters to a proper SQL-friendly format
        if not clusters:
            cluster_condition = "cluster IN (0, 1, 2)"
        else:
            cluster_condition = f"cluster IN ({','.join(map(str, clusters))})"
            
        query = f"""
            SELECT record_id, customer_id, name, age, purchase_amount, cluster, created_at 
            FROM customer_segments 
            WHERE created_at >= %s 
            AND {cluster_condition}
            AND purchase_amount BETWEEN %s AND %s
            ORDER BY created_at DESC
        """
        
        df = pd.read_sql(query, engine, params=(from_time, purchase_min, purchase_max))
        return df
    except Exception as e:
        st.sidebar.error(f"⚠️ Error fetching data: {str(e)}")
        return pd.DataFrame()  # Return empty DataFrame

# Get purchase range limits
def get_purchase_range(engine):
    try:
        if engine:
            with engine.connect() as conn:
                result = conn.execute(text("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'customer_segments')"))
                table_exists = result.fetchone()[0]
                
                if table_exists:
                    query = "SELECT MIN(purchase_amount) as min, MAX(purchase_amount) as max FROM customer_segments"
                    limits = pd.read_sql(query, engine).iloc[0]
                    return (
                        float(limits['min']) if not pd.isna(limits['min']) else 0,
                        float(limits['max']) if not pd.isna(limits['max']) else 1000
                    )
    except Exception as e:
        st.sidebar.error(f"⚠️ Error fetching purchase limits: {e}")
    
    return 0, 1000  # Default range

# Main function to render the dashboard
def render_dashboard(df, new_records, auto_refresh):
    current_time = datetime.now().strftime('%H:%M:%S.%f')[:-3]
    
    # Display refresh indicator
    st.markdown(f"""
    <div style="display: flex; justify-content: space-between; align-items: center;">
        <div class="refresh-time">🕒 Last refresh: {current_time}</div>
        <div>{"<span class='live-indicator'>● LIVE</span>" if auto_refresh else ""}</div>
        <div>{f"✨ +{new_records} new records" if new_records > 0 else ""}</div>
    </div>
    """, unsafe_allow_html=True)

    if df.empty:
        st.warning("⚠️ No data available. Check database connection, filters, or add records to customer_segments.")
        return

    # Dashboard Layout - show all 4 charts in a 2x2 grid
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("<div class='chart-container'>", unsafe_allow_html=True)
        st.markdown("<p class='chart-title'>💰 Cluster-wise Purchase Distribution</p>", unsafe_allow_html=True)
        fig1 = px.bar(df, x="cluster", y="purchase_amount", color="cluster",
                    labels={"cluster": "Cluster", "purchase_amount": "Purchase Amount"},
                    color_discrete_sequence=px.colors.qualitative.Set1)
        fig1.update_layout(height=400)
        st.plotly_chart(fig1, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

    with col2:
        st.markdown("<div class='chart-container'>", unsafe_allow_html=True)
        st.markdown("<p class='chart-title'>👤 Age Distribution by Cluster</p>", unsafe_allow_html=True)
        fig2 = px.histogram(df, x="age", color="cluster",
                        labels={"age": "Age", "count": "Number of Customers"},
                        barmode="overlay", color_discrete_sequence=px.colors.qualitative.Set1)
        fig2.update_layout(height=400)
        st.plotly_chart(fig2, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

    col3, col4 = st.columns(2)

    with col3:
        st.markdown("<div class='chart-container'>", unsafe_allow_html=True)
        st.markdown("<p class='chart-title'>👥 Total Customers Per Cluster</p>", unsafe_allow_html=True)
        cluster_counts = df["cluster"].value_counts().reset_index()
        cluster_counts.columns = ["Cluster", "Total Customers"]
        fig3 = px.pie(cluster_counts, values="Total Customers", names="Cluster",
                    color_discrete_sequence=px.colors.qualitative.Bold)
        fig3.update_layout(height=400)
        st.plotly_chart(fig3, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

    with col4:
        st.markdown("<div class='chart-container'>", unsafe_allow_html=True)
        st.markdown("<p class='chart-title'>💸 Average Purchase Per Cluster</p>", unsafe_allow_html=True)
        avg_purchase = df.groupby("cluster")["purchase_amount"].mean().reset_index()
        fig4 = px.bar(avg_purchase, x="cluster", y="purchase_amount", color="cluster",
                    labels={"cluster": "Cluster", "purchase_amount": "Avg Purchase"},
                    color_discrete_sequence=px.colors.qualitative.Set1)
        fig4.update_layout(height=400)
        st.plotly_chart(fig4, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

    # Add customer data table with latest records at the top
    st.markdown("<div class='chart-container'>", unsafe_allow_html=True)
    st.markdown("<p class='chart-title'>📋 Latest Customer Data</p>", unsafe_allow_html=True)
    
    # Show more records - up to 20 instead of 10
    max_records = min(20, len(df))
    st.dataframe(df.head(max_records)[["customer_id", "name", "age", "purchase_amount", "cluster", "created_at"]].style.format({"purchase_amount": "${:.2f}"}), use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)

    # Add alert for high purchases with different threshold
    high_purchases = df[df["purchase_amount"] > 400]
    if not high_purchases.empty:
        st.warning(f"⚠️ Alert: {len(high_purchases)} high purchase amounts detected! Check cluster data.")
        
    # Display overall stats
    col_stats1, col_stats2, col_stats3 = st.columns(3)
    with col_stats1:
        st.metric(label="Total Customers", value=len(df), delta=new_records)
    with col_stats2:
        st.metric(label="Avg Purchase Amount", value=f"${df['purchase_amount'].mean():.2f}")
    with col_stats3:
        st.metric(label="Avg Customer Age", value=f"{df['age'].mean():.1f}")

    # Export data
    if st.download_button("📥 Download Data as CSV", df.to_csv(index=False), "customer_segments.csv", "text/csv"):
        st.success("Data downloaded successfully!")

# Streamlit Page Config
st.set_page_config(page_title="📊 Real-Time Customer Segmentation", layout="wide")

# Custom CSS for Styling
st.markdown(
    """
    <style>
    body { background-color: #121212; color: white; }
    .stApp { background-color: #1E1E1E; }
    h1 { 
        text-align: center; 
        font-size: 40px; 
        font-weight: bold; 
        color: #4CAF50; 
        padding: 10px; 
        background: linear-gradient(to right, #0f2027, #203a43, #2c5364); 
        border-radius: 15px; 
        box-shadow: 0px 4px 15px rgba(0, 255, 0, 0.3); 
        animation: fadeIn 1s ease-in-out; 
    }
    @keyframes fadeIn {
        from { opacity: 0; }
        to { opacity: 1; }
    }
    .chart-container { 
        border-radius: 15px; 
        padding: 20px; 
        background-color: #2A2A2A; 
        box-shadow: 0px 5px 20px rgba(255, 255, 255, 0.15); 
        transition: transform 0.3s ease, box-shadow 0.3s ease; 
        margin-bottom: 20px;
    }
    .chart-container:hover { 
        transform: scale(1.02); 
        box-shadow: 0px 8px 25px rgba(255, 255, 255, 0.25); 
    }
    .chart-title { 
        font-size: 20px; 
        font-weight: bold; 
        margin-bottom: 15px; 
        color: #FFD700; 
        text-shadow: 0px 1px 5px rgba(255, 215, 0, 0.5); 
    }
    .stButton>button { 
        background-color: #4CAF50; 
        color: white; 
        border-radius: 10px; 
        padding: 10px 20px; 
        font-weight: bold; 
        transition: background-color 0.3s ease; 
    }
    .stButton>button:hover { 
        background-color: #45a049; 
        cursor: pointer; 
    }
    .stWarning { 
        background-color: #333333; 
        border: 2px solid #FF4444; 
        border-radius: 10px; 
        padding: 10px; 
        color: #FF4444; 
        animation: shake 0.5s; 
    }
    @keyframes pulse {
        0% { opacity: 1; }
        50% { opacity: 0.5; }
        100% { opacity: 1; }
    }
    .live-indicator {
        color: #FF4444;
        animation: pulse 2s infinite;
        display: inline-block;
        margin-left: 10px;
    }
    .refresh-time {
        font-size: 16px;
        color: #4CAF50;
        font-weight: bold;
    }
    @keyframes shake {
        0% { transform: translateX(0); }
        25% { transform: translateX(-5px); }
        50% { transform: translateX(5px); }
        75% { transform: translateX(-5px); }
        100% { transform: translateX(0); }
    }
    </style>
    """,
    unsafe_allow_html=True
)

# App title
st.markdown("<h1>📊 Real-Time Customer Segmentation Dashboard</h1>", unsafe_allow_html=True)

# Initialize session state variables
if 'prev_record_count' not in st.session_state:
    st.session_state['prev_record_count'] = 0
    
if 'last_refresh_time' not in st.session_state:
    st.session_state['last_refresh_time'] = datetime.now().strftime('%H:%M:%S')
    
if 'refresh_counter' not in st.session_state:
    st.session_state['refresh_counter'] = 0

# Get database engine
engine = get_database_engine()

# Sidebar for filters
st.sidebar.header("🔍 Filters")

# Time range filter
time_range = st.sidebar.slider("Select Time Range (Hours)", 0, 168, 48)
from_time = datetime.now() - timedelta(hours=time_range)

# Cluster filter - ensure we have all clusters by default
clusters = st.sidebar.multiselect("Select Clusters", options=[0, 1, 2], default=[0, 1, 2])

# Get purchase amount range from database
purchase_min_db, purchase_max_db = get_purchase_range(engine)
purchase_min, purchase_max = st.sidebar.slider("Purchase Amount Range", 
                                          min_value=float(purchase_min_db),
                                          max_value=float(purchase_max_db),
                                          value=(float(purchase_min_db), float(purchase_max_db)))

# Auto-refresh and manual refresh
auto_refresh = st.sidebar.checkbox("Enable Auto-Refresh", value=True)
refresh_rate = st.sidebar.slider("Refresh Rate (seconds)", 1, 30, 3)

# Manual refresh button
if not auto_refresh and st.sidebar.button("🔄 Refresh Data"):
    st.session_state['refresh_counter'] += 1

# Fetch data
df = fetch_data(engine, from_time, clusters, purchase_min, purchase_max)

# Calculate new records
new_records = 0
if not df.empty:
    current_record_count = len(df)
    if 'prev_record_count' in st.session_state:
        new_records = current_record_count - st.session_state['prev_record_count']
    st.session_state['prev_record_count'] = current_record_count

# Render dashboard - only once
render_dashboard(df, new_records, auto_refresh)

# Auto-refresh logic using Streamlit's recommended method
if auto_refresh:
    # Use st.experimental_rerun() with a proper auto-refresh framework
    st.empty()
    # Auto refresh using client-side refresh meta tag (as Streamlit doesn't have native interval refresh)
    st.markdown(f"""
        <meta http-equiv="refresh" content="{refresh_rate}">
    """, unsafe_allow_html=True)
