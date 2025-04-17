import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
import time
import hashlib

# ---------------------------
# 🔐 Simple Login Function
# ---------------------------
def check_password():
    def hash_password(password):
        return hashlib.sha256(password.encode()).hexdigest()

    def password_entered():
        if hash_password(st.session_state["password"]) == hash_password("admin123"):  # Set your password here
            st.session_state["password_correct"] = True
            del st.session_state["password"]
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        st.text_input("🔐 Enter Password", type="password", on_change=password_entered, key="password")
        return False
    elif not st.session_state["password_correct"]:
        st.text_input("🔐 Enter Password", type="password", on_change=password_entered, key="password")
        st.error("❌ Incorrect password")
        return False
    else:
        return True

# Stop page unless logged in
if not check_password():
    st.stop()

# ---------------------------
# 📊 Real-Time Dashboard Code
# ---------------------------

# Supabase PostgreSQL Connection
DATABASE_URL = "postgresql://postgres:Adminsaloni@10@db.fsulfssfgmgxosgpjijw.supabase.co:5432/postgres"
try:
    engine = create_engine(DATABASE_URL)
except Exception as e:
    st.error(f"⚠️ Database connection failed: {e}")
    st.stop()

st.set_page_config(page_title="📊 Real-Time Customer Segmentation", layout="wide")

st.markdown("<h1>📊 Real-Time Customer Segmentation Dashboard</h1>", unsafe_allow_html=True)

# Sidebar Filters
st.sidebar.header("🔍 Filters")

time_range = st.sidebar.slider("Select Time Range (Hours)", 0, 24, 1)
from_time = pd.Timestamp.now() - pd.Timedelta(hours=time_range)

clusters = st.sidebar.multiselect("Select Clusters", options=sorted([0, 1, 2]), default=[0, 1, 2])

query = "SELECT MIN(purchase_amount) as min, MAX(purchase_amount) as max FROM customer_segments"
limits = pd.read_sql(query, engine).iloc[0]
purchase_min, purchase_max = st.sidebar.slider(
    "Purchase Amount Range",
    min_value=float(limits['min']),
    max_value=float(limits['max']),
    value=(float(limits['min']), float(limits['max']))
)

auto_refresh = st.sidebar.checkbox("Enable Auto-Refresh (5s)", value=True)

def load_data():
    query = f"""
        SELECT record_id, customer_id, name, age, purchase_amount, cluster, created_at 
        FROM customer_segments 
        WHERE created_at >= %s 
        AND cluster IN {tuple(clusters) if clusters else (0, 1, 2)}
        AND purchase_amount BETWEEN %s AND %s
        ORDER BY created_at DESC LIMIT 100
    """
    return pd.read_sql(query, engine, params=(from_time, purchase_min, purchase_max))

def render_dashboard(df):
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("💰 Cluster-wise Purchase Distribution")
        fig1 = px.bar(df, x="cluster", y="purchase_amount", color="cluster")
        st.plotly_chart(fig1, use_container_width=True)

    with col2:
        st.subheader("👤 Age Distribution by Cluster")
        fig2 = px.histogram(df, x="age", color="cluster", barmode="overlay")
        st.plotly_chart(fig2, use_container_width=True)

    col3, col4 = st.columns(2)

    with col3:
        st.subheader("👥 Total Customers Per Cluster")
        cluster_counts = df["cluster"].value_counts().reset_index()
        cluster_counts.columns = ["Cluster", "Total Customers"]
        fig3 = px.pie(cluster_counts, values="Total Customers", names="Cluster")
        st.plotly_chart(fig3, use_container_width=True)

    with col4:
        st.subheader("💸 Average Purchase Per Cluster")
        avg_purchase = df.groupby("cluster")["purchase_amount"].mean().reset_index()
        fig4 = px.bar(avg_purchase, x="cluster", y="purchase_amount", color="cluster")
        st.plotly_chart(fig4, use_container_width=True)

    st.subheader("📋 Latest Customer Data")
    st.dataframe(df[["customer_id", "name", "age", "purchase_amount", "cluster", "created_at"]].style.format({"purchase_amount": "${:.2f}"}))

    if df["purchase_amount"].max() > 400:
        st.error("⚠️ Alert: High purchase amount detected!")

    if st.download_button("🗕️ Download Data as CSV", df.to_csv(index=False), "customer_segments.csv", "text/csv"):
        st.success("Data downloaded successfully!")

# Main Loop
if auto_refresh:
    while True:
        df = load_data()
        if df.empty:
            st.warning("⚠️ No data available yet.")
        else:
            render_dashboard(df)
        time.sleep(5)
        st.rerun()
else:
    if st.button("🔄 Refresh Data"):
        df = load_data()
        if df.empty:
            st.warning("⚠️ No data available.")
        else:
            render_dashboard(df)
