import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
import time
import urllib.parse
from datetime import datetime, timedelta

# Neon Database Connection
DB_HOST = "ep-dry-violet-a4v38rh7-pooler.us-east-1.aws.neon.tech"
DB_PORT = 5432
DB_NAME = "neondb"
DB_USER = "neondb_owner"
DB_PASSWORD = urllib.parse.quote_plus("npg_5UbnztxlVuD1")
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode=require"

try:
    engine = create_engine(DATABASE_URL)
except Exception as e:
    st.error(f"⚠️ Database connection failed: {e}")
    st.stop()

# Streamlit Page Config
st.set_page_config(page_title="📊 Real-Time Customer Segmentation", layout="wide")

# Custom CSS for Styling - Fixed flickering and improved visibility
st.markdown(
    """
    <style>
    body { background-color: #121212; color: white; }
    .stApp { background-color: #1E1E1E; }
    
    /* Improved header visibility with stronger contrast and brighter colors */
    h1 { 
        text-align: center; 
        font-size: 42px !important; 
        font-weight: bold !important; 
        color: #FFFFFF !important; 
        padding: 15px; 
        background: linear-gradient(to right, #004d40, #00796b, #009688); 
        border-radius: 15px; 
        box-shadow: 0px 4px 15px rgba(0, 255, 255, 0.4); 
        margin-bottom: 25px !important;
        letter-spacing: 1px;
    }
    
    /* Reduced animations to prevent flickering */
    .chart-container { 
        border-radius: 15px; 
        padding: 20px; 
        background-color: #2A2A2A; 
        box-shadow: 0px 5px 20px rgba(255, 255, 255, 0.15); 
        margin-bottom: 20px;
        transition: all 0.2s ease;
    }
    
    /* Softer hover effect */
    .chart-container:hover { 
        transform: scale(1.01); 
        box-shadow: 0px 8px 25px rgba(255, 255, 255, 0.2); 
    }
    
    .chart-title { 
        font-size: 22px !important; 
        font-weight: bold; 
        margin-bottom: 15px; 
        color: #00E5FF !important; 
        text-shadow: 0px 1px 3px rgba(0, 229, 255, 0.4); 
    }
    
    .stButton>button { 
        background-color: #00897B; 
        color: white; 
        border-radius: 10px; 
        padding: 10px 20px; 
        font-weight: bold; 
        transition: background-color 0.2s ease; 
    }
    
    .stButton>button:hover { 
        background-color: #00695C; 
        cursor: pointer; 
    }
    
    .stWarning { 
        background-color: #333333; 
        border: 2px solid #FF4444; 
        border-radius: 10px; 
        padding: 10px; 
        color: #FF4444; 
    }
    
    /* Better visibility for sidebar elements */
    .css-6qob1r.e1fqkh3o3 {
        background-color: #263238;
        padding: 20px 10px;
        border-radius: 10px;
    }
    
    /* Improve slider visibility */
    .stSlider > div > div {
        background-color: #80CBC4 !important;
    }
    
    /* Dashboard header with animation but no flickering */
    .dashboard-header {
        font-size: 46px !important;
        font-weight: bold !important;
        color: #FFFFFF !important;
        text-align: center;
        padding: 20px;
        background: linear-gradient(to right, #004d40, #00796b, #009688);
        border-radius: 15px;
        box-shadow: 0px 4px 15px rgba(0, 255, 255, 0.4);
        margin: 10px 0 30px 0;
    }
    
    /* Improve dataframe styling */
    .dataframe {
        font-size: 14px !important;
    }
    
    /* Fix for plotly charts to prevent flickering */
    .js-plotly-plot {
        transition: none !important;
    }
    
    /* Custom styling for tables to replace matplotlib gradient */
    .highlight {
        background-color: rgba(0, 229, 255, 0.3) !important;
        font-weight: bold;
    }
    </style>
    """,
    unsafe_allow_html=True
)

# Updated Header with better visibility
st.markdown('<div class="dashboard-header">📊 Real-Time Customer Segmentation Dashboard</div>', unsafe_allow_html=True)

# Initialize session state
if 'last_refresh' not in st.session_state:
    st.session_state['last_refresh'] = time.time()
if 'refresh_interval' not in st.session_state:
    st.session_state['refresh_interval'] = 5  # 5 seconds
if 'data' not in st.session_state:
    st.session_state['data'] = None

# Sidebar for filters
st.sidebar.header("🔍 Filters")

# Time range filter
time_range = st.sidebar.slider("Select Time Range (Hours)", 0, 24, 8)
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

auto_refresh = st.sidebar.checkbox("Enable Auto-Refresh (5s)", value=True)

# Fetch data function
def fetch_data():
    # Fix for empty clusters list
    if not clusters:
        cluster_condition = "(0, 1, 2)"
    elif len(clusters) == 1:
        cluster_condition = f"({clusters[0]})"
    else:
        cluster_condition = tuple(clusters)
        
    query = f"""
        SELECT record_id, customer_id, name, age, purchase_amount, cluster, created_at 
        FROM customer_segments 
        WHERE created_at >= %s 
        AND cluster IN {cluster_condition}
        AND purchase_amount BETWEEN %s AND %s
        ORDER BY created_at DESC
    """
    try:
        df = pd.read_sql(query, engine, params=(from_time, purchase_min, purchase_max))
        st.session_state['last_fetch_time'] = datetime.now().strftime('%H:%M:%S')
        st.session_state['row_count'] = len(df)
        return df
    except Exception as e:
        st.error(f"⚠️ Error fetching data: {e}")
        return pd.DataFrame()

# Fetch data with a more stable approach to reduce flickering
current_time = time.time()
if auto_refresh and (current_time - st.session_state['last_refresh'] >= st.session_state['refresh_interval']):
    st.session_state['data'] = fetch_data()
    st.session_state['last_refresh'] = current_time
elif st.session_state['data'] is None or not auto_refresh and st.sidebar.button("🔄 Refresh Data"):
    st.session_state['data'] = fetch_data()
    st.session_state['last_refresh'] = current_time

df = st.session_state['data']

# Display debug info
st.sidebar.write(f"Last Fetch: {st.session_state.get('last_fetch_time', 'N/A')}")
st.sidebar.write(f"Rows Fetched: {st.session_state.get('row_count', 0)}")

if df is None or df.empty:
    st.warning("⚠️ No data available yet. Waiting for new messages...")
else:
    # 🔹 Dashboard Layout
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("<div class='chart-container'>", unsafe_allow_html=True)
        st.markdown("<p class='chart-title'>💰 Cluster-wise Purchase Distribution</p>", unsafe_allow_html=True)
        fig1 = px.bar(df, x="cluster", y="purchase_amount", color="cluster",
                    labels={"cluster": "Cluster", "purchase_amount": "Purchase Amount"},
                    color_discrete_sequence=["#80DEEA", "#4DB6AC", "#00897B"])
        fig1.update_layout(
            title_text="", 
            plot_bgcolor="#2A2A2A",
            paper_bgcolor="#2A2A2A",
            font=dict(color="#FFFFFF"),
            xaxis=dict(gridcolor="#555555"),
            yaxis=dict(gridcolor="#555555")
        )
        st.plotly_chart(fig1, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

    with col2:
        st.markdown("<div class='chart-container'>", unsafe_allow_html=True)
        st.markdown("<p class='chart-title'>👤 Age Distribution by Cluster</p>", unsafe_allow_html=True)
        
        # Improved age distribution chart with distinct colors per cluster
        fig2 = px.histogram(df, x="age", color="cluster",
                           labels={"age": "Age", "count": "Number of Customers"},
                           barmode="overlay", 
                           opacity=0.8,
                           color_discrete_map={
                               0: "#FF9E80",  # Orange-red for cluster 0
                               1: "#B388FF",  # Purple for cluster 1
                               2: "#80D8FF"   # Blue for cluster 2
                           })
        
        fig2.update_layout(
            title_text="", 
            plot_bgcolor="#2A2A2A",
            paper_bgcolor="#2A2A2A",
            font=dict(color="#FFFFFF"),
            xaxis=dict(gridcolor="#555555", title_font=dict(size=14)),
            yaxis=dict(gridcolor="#555555", title_font=dict(size=14)),
            legend=dict(
                title="Cluster",
                font=dict(size=12)
            )
        )
        st.plotly_chart(fig2, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

    col3, col4 = st.columns(2)

    with col3:
        st.markdown("<div class='chart-container'>", unsafe_allow_html=True)
        st.markdown("<p class='chart-title'>👥 Total Customers Per Cluster</p>", unsafe_allow_html=True)
        cluster_counts = df["cluster"].value_counts().reset_index()
        cluster_counts.columns = ["Cluster", "Total Customers"]
        fig3 = px.pie(cluster_counts, values="Total Customers", names="Cluster",
                    color_discrete_map={
                        0: "#FF9E80",  # Orange-red for cluster 0
                        1: "#B388FF",  # Purple for cluster 1
                        2: "#80D8FF"   # Blue for cluster 2
                    })
        fig3.update_layout(
            title_text="", 
            plot_bgcolor="#2A2A2A",
            paper_bgcolor="#2A2A2A",
            font=dict(color="#FFFFFF")
        )
        st.plotly_chart(fig3, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

    with col4:
        st.markdown("<div class='chart-container'>", unsafe_allow_html=True)
        st.markdown("<p class='chart-title'>💸 Average Purchase Per Cluster</p>", unsafe_allow_html=True)
        avg_purchase = df.groupby("cluster")["purchase_amount"].mean().reset_index()
        fig4 = px.bar(avg_purchase, x="cluster", y="purchase_amount", color="cluster",
                    labels={"cluster": "Cluster", "purchase_amount": "Avg Purchase"},
                    color_discrete_sequence=["#FF9E80", "#B388FF", "#80D8FF"])
        fig4.update_layout(
            title_text="", 
            plot_bgcolor="#2A2A2A",
            paper_bgcolor="#2A2A2A",
            font=dict(color="#FFFFFF"),
            xaxis=dict(gridcolor="#555555"),
            yaxis=dict(gridcolor="#555555")
        )
        st.plotly_chart(fig4, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

    # Add customer data table with simple formatting (no matplotlib dependency)
    st.markdown("<div class='chart-container'>", unsafe_allow_html=True)
    st.markdown("<p class='chart-title'>📋 Latest Customer Data</p>", unsafe_allow_html=True)
    
    # Format table without using background_gradient
    display_df = df[["customer_id", "name", "age", "purchase_amount", "cluster", "created_at"]].copy()
    display_df["purchase_amount"] = display_df["purchase_amount"].apply(lambda x: f"${x:.2f}")
    
    st.dataframe(
        display_df,
        use_container_width=True,
        column_config={
            "purchase_amount": st.column_config.TextColumn("Purchase Amount", help="Amount spent by customer"),
            "cluster": st.column_config.NumberColumn("Cluster", help="Customer segment cluster"),
            "created_at": st.column_config.DatetimeColumn("Created At", format="DD/MM/YYYY hh:mm:ss"),
        },
        hide_index=True
    )
    st.markdown("</div>", unsafe_allow_html=True)

    # Add alert for high purchases
    if df["purchase_amount"].max() > 400:
        st.error("⚠️ Alert: High purchase amount detected! Check cluster data.")

    # Export data
    if st.download_button("📥 Download Data as CSV", df.to_csv(index=False), "customer_segments.csv", "text/csv"):
        st.success("Data downloaded successfully!")

# Manual refresh button at the bottom
if not auto_refresh:
    if st.button("🔄 Refresh Data"):
        st.session_state['last_refresh'] = time.time()
        st.rerun()
