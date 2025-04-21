import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine, text
import urllib.parse
import time
from datetime import datetime, timedelta

# Neon Database Connection
DB_HOST = "ep-dry-violet-a4v38rh7-pooler.us-east-1.aws.neon.tech"
DB_PORT = 5432
DB_NAME = "neondb"
DB_USER = "neondb_owner"
DB_PASSWORD = urllib.parse.quote_plus("npg_5UbnztxlVuD1")
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode=require"

# Initialize database engine without caching to ensure fresh data
def get_database_engine():
    try:
        return create_engine(DATABASE_URL)
    except Exception as e:
        st.error(f"⚠️ Database connection failed: {e}")
        return None

# Fetch data function without caching
def fetch_data(engine, from_time, clusters, purchase_min, purchase_max):
    if not engine:
        return None
    try:
        # Handle the case when clusters list is empty or has only one item
        if not clusters:
            cluster_filter = "(0, 1, 2)"
        elif len(clusters) == 1:
            cluster_filter = f"({clusters[0]},)"  # Add comma for single item tuple
        else:
            cluster_filter = str(tuple(clusters))
            
        query = f"""
            SELECT record_id, customer_id, name, age, purchase_amount, cluster, created_at 
            FROM customer_segments 
            WHERE created_at >= %s 
            AND cluster IN {cluster_filter}
            AND purchase_amount BETWEEN %s AND %s
            ORDER BY created_at DESC
        """
        df = pd.read_sql(query, engine, params=(from_time, purchase_min, purchase_max))
        return df
    except Exception as e:
        st.warning(f"⚠️ Error fetching data: {e}")
        return None

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

st.markdown("<h1>📊 Real-Time Customer Segmentation Dashboard</h1>", unsafe_allow_html=True)

# Initialize session state for latest record count and auto-refresh
if 'prev_record_count' not in st.session_state:
    st.session_state['prev_record_count'] = 0
    
if 'last_refresh_time' not in st.session_state:
    st.session_state['last_refresh_time'] = datetime.now().strftime('%H:%M:%S')
    
if 'refresh_counter' not in st.session_state:
    st.session_state['refresh_counter'] = 0

# Sidebar for filters
st.sidebar.header("🔍 Filters")

# Time range filter
time_range = st.sidebar.slider("Select Time Range (Hours)", 0, 168, 12)  # Default to 12 hours for more frequent updates
from_time = datetime.now() - timedelta(hours=time_range)

# Get engine outside the main loop
engine = get_database_engine()

# Cluster filter
clusters = st.sidebar.multiselect("Select Clusters", options=sorted([0, 1, 2]), default=[0, 1, 2])

# Purchase amount range
if engine:
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'customer_segments')"))
            table_exists = result.fetchone()[0]
            st.sidebar.write(f"Table exists: {table_exists}")
            
            if table_exists:
                query = "SELECT MIN(purchase_amount) as min, MAX(purchase_amount) as max FROM customer_segments"
                limits = pd.read_sql(query, engine).iloc[0]
                purchase_min, purchase_max = st.sidebar.slider("Purchase Amount Range", 
                                                         min_value=float(limits['min']) if not pd.isna(limits['min']) else 0,
                                                         max_value=float(limits['max']) if not pd.isna(limits['max']) else 1000,
                                                         value=(float(limits['min']) if not pd.isna(limits['min']) else 0, float(limits['max']) if not pd.isna(limits['max']) else 1000))
            else:
                purchase_min, purchase_max = 0, 1000
                st.sidebar.warning("⚠️ Table customer_segments does not exist!")
    except Exception as e:
        st.sidebar.error(f"⚠️ Error fetching purchase limits: {e}")
        purchase_min, purchase_max = 0, 1000
else:
    purchase_min, purchase_max = 0, 1000
    st.sidebar.error("⚠️ Database connection failed.")

# Auto-refresh and manual refresh
auto_refresh = st.sidebar.checkbox("Enable Auto-Refresh", value=True)
refresh_rate = st.sidebar.slider("Refresh Rate (seconds)", 1, 30, 3)  # Allow faster refresh rates

# Create placeholders for dynamic content
refresh_indicator = st.empty()
main_content = st.container()

# Manual refresh button when auto-refresh is off
if not auto_refresh:
    if st.sidebar.button("🔄 Refresh Data"):
        st.session_state['refresh_counter'] += 1
        st.rerun()  # Use st.rerun() instead of st.experimental_rerun()

# Main content rendering function - fixes the issue by separating data fetching from visualization
def render_dashboard():
    # Fetch data
    df = fetch_data(engine, from_time, clusters, purchase_min, purchase_max)
    
    current_time = datetime.now().strftime('%H:%M:%S.%f')[:-3]
    st.session_state['last_refresh_time'] = current_time
    
    # Check if we have new data
    new_records = 0
    if df is not None:
        current_record_count = len(df)
        if 'prev_record_count' in st.session_state:
            new_records = current_record_count - st.session_state['prev_record_count']
        st.session_state['prev_record_count'] = current_record_count
    
    # Display refresh indicator
    with refresh_indicator:
        st.markdown(f"""
        <div style="display: flex; justify-content: space-between; align-items: center;">
            <div class="refresh-time">🕒 Last refresh: {current_time}</div>
            <div>{"<span class='live-indicator'>● LIVE</span>" if auto_refresh else ""}</div>
            <div>{f"✨ +{new_records} new records" if new_records > 0 else ""}</div>
        </div>
        """, unsafe_allow_html=True)

    if df is not None and not df.empty:
        # Dashboard Layout
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
        
        # Sort by created_at to show newest first
        df_sorted = df.sort_values(by="created_at", ascending=False).head(10)
        st.dataframe(df_sorted[["customer_id", "name", "age", "purchase_amount", "cluster", "created_at"]].style.format({"purchase_amount": "${:.2f}"}), use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

        # Add alert for high purchases
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
    else:
        st.warning("⚠️ No data available. Check database connection, filters, or add records to customer_segments.")

# Initial render
with main_content:
    render_dashboard()

# Auto-refresh logic using the proper Streamlit way
if auto_refresh:
    # Create a refresh trigger that will rerun the app
    refresh_trigger = st.empty()
    with refresh_trigger:
        st.markdown(f"""
        <div id="refresh-trigger" style="display: none;">
            Refresh trigger: {time.time()}
        </div>
        <script>
            function refreshPage() {{
                setTimeout(function() {{ 
                    window.location.reload();
                }}, {refresh_rate * 1000});
            }}
            refreshPage();
        </script>
        """, unsafe_allow_html=True)
