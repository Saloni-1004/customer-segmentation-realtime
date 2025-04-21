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

auto_refresh = st.sidebar.checkbox("Enable Auto-Refresh (5s)", value=True)

# Fetch data
query = f"""
    SELECT record_id, customer_id, name, age, purchase_amount, cluster, created_at 
    FROM customer_segments 
    WHERE created_at >= %s 
    AND cluster IN {tuple(clusters) if clusters else (0, 1, 2)}
    AND purchase_amount BETWEEN %s AND %s
    ORDER BY created_at DESC
"""
df = pd.read_sql(query, engine, params=(from_time, purchase_min, purchase_max))

if df.empty:
    st.warning("⚠️ No data available yet. Waiting for new messages...")
else:
    # 🔹 Dashboard Layout
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("<div class='chart-container'>", unsafe_allow_html=True)
        st.markdown("<p class='chart-title'>💰 Cluster-wise Purchase Distribution</p>", unsafe_allow_html=True)
        fig1 = px.bar(df, x="cluster", y="purchase_amount", color="cluster",
                    labels={"cluster": "Cluster", "purchase_amount": "Purchase Amount"},
                    color_continuous_scale="teal")
        fig1.update_layout(title_text="💰 Cluster-wise Purchase Distribution", title_x=0.5)
        st.plotly_chart(fig1, use_container_width=True, key="bar_chart")
        st.markdown("</div>", unsafe_allow_html=True)

    with col2:
        st.markdown("<div class='chart-container'>", unsafe_allow_html=True)
        st.markdown("<p class='chart-title'>👤 Age Distribution by Cluster</p>", unsafe_allow_html=True)
        fig2 = px.histogram(df, x="age", color="cluster",
                            labels={"age": "Age", "count": "Number of Customers"},
                            barmode="overlay", color_discrete_sequence=px.colors.sequential.Viridis)
        fig2.update_layout(title_text="👤 Age Distribution by Cluster", title_x=0.5)
        st.plotly_chart(fig2, use_container_width=True, key="histogram_chart")
        st.markdown("</div>", unsafe_allow_html=True)

    col3, col4 = st.columns(2)

    with col3:
        st.markdown("<div class='chart-container'>", unsafe_allow_html=True)
        st.markdown("<p class='chart-title'>👥 Total Customers Per Cluster</p>", unsafe_allow_html=True)
        cluster_counts = df["cluster"].value_counts().reset_index()
        cluster_counts.columns = ["Cluster", "Total Customers"]
        fig3 = px.pie(cluster_counts, values="Total Customers", names="Cluster",
                    color_discrete_sequence=px.colors.sequential.Rainbow)
        fig3.update_layout(title_text="👥 Total Customers Per Cluster", title_x=0.5)
        st.plotly_chart(fig3, use_container_width=True, key="pie_chart")
        st.markdown("</div>", unsafe_allow_html=True)

    with col4:
        st.markdown("<div class='chart-container'>", unsafe_allow_html=True)
        st.markdown("<p class='chart-title'>💸 Average Purchase Per Cluster</p>", unsafe_allow_html=True)
        avg_purchase = df.groupby("cluster")["purchase_amount"].mean().reset_index()
        fig4 = px.bar(avg_purchase, x="cluster", y="purchase_amount", color="cluster",
                    labels={"cluster": "Cluster", "purchase_amount": "Avg Purchase"},
                    color_continuous_scale="magma")
        fig4.update_layout(title_text="💸 Average Purchase Per Cluster", title_x=0.5)
        st.plotly_chart(fig4, use_container_width=True, key="avg_bar_chart")
        st.markdown("</div>", unsafe_allow_html=True)

    # Add customer data table
    st.markdown("<div class='chart-container'>", unsafe_allow_html=True)
    st.markdown("<p class='chart-title'>📋 Latest Customer Data</p>", unsafe_allow_html=True)
    st.dataframe(df[["customer_id", "name", "age", "purchase_amount", "cluster", "created_at"]].style.format({"purchase_amount": "${:.2f}"}), use_container_width=True, key="data_table")
    st.markdown("</div>", unsafe_allow_html=True)

    # Add alert for high purchases
    if df["purchase_amount"].max() > 400:
        st.error("⚠️ Alert: High purchase amount detected! Check cluster data.")

    # Export data
    if st.download_button("📥 Download Data as CSV", df.to_csv(index=False), "customer_segments.csv", "text/csv"):
        st.success("Data downloaded successfully!")

# Auto-refresh logic
if auto_refresh:
    time.sleep(5)
    st.rerun()

# Manual refresh
if not auto_refresh and st.button("🔄 Refresh Data"):
    st.rerun()
