import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
import urllib.parse
import time
from datetime import timezone

# Streamlit config
st.set_page_config(page_title="📊 Real-Time Customer Segmentation", layout="wide")

# Initialize session state
if 'autorefresh' not in st.session_state:
    st.session_state['autorefresh'] = time.time()
if 'df' not in st.session_state:
    st.session_state.df = pd.DataFrame()
if 'last_timestamp' not in st.session_state:
    st.session_state.last_timestamp = None

# Neon PostgreSQL connection
DB_HOST = "ep-dry-violet-a4v38rh7-pooler.us-east-1.aws.neon.tech"
DB_PORT = 5432
DB_NAME = "neondb"
DB_USER = "neondb_owner"
DB_PASSWORD = urllib.parse.quote_plus("npg_5UbnztxlVuD1")
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode=require"

try:
    engine = create_engine(DATABASE_URL)
except Exception as e:
    st.error(f"❌ Database connection failed: {e}")
    st.stop()

st.title("📊 Real-Time Customer Segmentation Dashboard")

# Sidebar filters
st.sidebar.header("🔍 Filters")
auto_refresh = st.sidebar.checkbox("Enable Auto-Refresh (5s)", value=True)

# Time range
time_range = st.sidebar.slider("Time Range (hours)", 1, 168, 48)
if st.session_state.last_timestamp:
    from_time = st.session_state.last_timestamp - pd.Timedelta(hours=time_range)
else:
    from_time = pd.Timestamp.now(tz=timezone.utc) - pd.Timedelta(hours=time_range)

# Cluster filter
clusters = st.sidebar.multiselect("Clusters", [0, 1, 2], default=[0, 1, 2])

# Purchase amount range
def get_purchase_limits():
    query = "SELECT MIN(purchase_amount) as min, MAX(purchase_amount) as max FROM customer_segments"
    return pd.read_sql(query, engine).iloc[0]

try:
    limits = get_purchase_limits()
    purchase_min, purchase_max = st.sidebar.slider("Purchase Amount", 
        float(limits['min'] or 0.0), float(limits['max'] or 1000.0),
        (float(limits['min'] or 0.0), float(limits['max'] or 1000.0))
    )
except:
    purchase_min, purchase_max = 0.0, 1000.0

# Fetch data function
def fetch_data(_from_time, _clusters, _purchase_min, _purchase_max):
    query = """
        SELECT record_id, customer_id, name, age, purchase_amount, cluster, created_at
        FROM customer_segments
        WHERE created_at >= %s
        AND cluster IN %s
        AND purchase_amount BETWEEN %s AND %s
        ORDER BY created_at DESC
        LIMIT 100
    """
    df = pd.read_sql(query, engine, params=(_from_time, tuple(_clusters), _purchase_min, _purchase_max))
    if not df.empty:
        st.session_state.last_timestamp = df['created_at'].max()
    return df

# Spinner & main display logic
with st.spinner("Running... Fetching real-time data..."):
    if auto_refresh:
        if time.time() - st.session_state['autorefresh'] > 5:
            st.session_state['autorefresh'] = time.time()
            df = fetch_data(from_time, clusters, purchase_min, purchase_max)
            if not df.empty:
                st.session_state.df = df
            st.experimental_rerun()
    else:
        if st.button("🔄 Manual Refresh"):
            df = fetch_data(from_time, clusters, purchase_min, purchase_max)
            if not df.empty:
                st.session_state.df = df

df = st.session_state.df

# Dashboard Rendering
if df.empty:
    st.warning("⚠️ No data found. Waiting for new real-time records...")
else:
    st.success(f"✅ Showing {len(df)} records. Last updated: {st.session_state.last_timestamp}")
    
    col1, col2 = st.columns(2)

    with col1:
        fig1 = px.bar(df, x="cluster", y="purchase_amount", color="cluster", barmode="group")
        st.plotly_chart(fig1, use_container_width=True)

    with col2:
        fig2 = px.histogram(df, x="age", color="cluster", barmode="overlay")
        st.plotly_chart(fig2, use_container_width=True)

    col3, col4 = st.columns(2)

    with col3:
        cluster_counts = df["cluster"].value_counts().reset_index()
        cluster_counts.columns = ["Cluster", "Total"]
        fig3 = px.pie(cluster_counts, values="Total", names="Cluster")
        st.plotly_chart(fig3, use_container_width=True)

    with col4:
        avg_purchase = df.groupby("cluster")["purchase_amount"].mean().reset_index()
        fig4 = px.bar(avg_purchase, x="cluster", y="purchase_amount", color="cluster")
        st.plotly_chart(fig4, use_container_width=True)

    st.dataframe(df)

    st.download_button("⬇ Download CSV", df.to_csv(index=False), "customer_segments.csv", "text/csv")

