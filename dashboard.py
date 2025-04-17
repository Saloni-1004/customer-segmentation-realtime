import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
import time
import hashlib

# Page configuration
st.set_page_config(page_title="📊 Real-Time Customer Segmentation", layout="wide")

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

# Get Supabase credentials from Streamlit secrets
try:
    # Use Streamlit secrets
    db_host = st.secrets["postgres"]["host"]
    db_port = st.secrets["postgres"]["port"]
    db_name = st.secrets["postgres"]["dbname"]
    db_user = st.secrets["postgres"]["user"]
    db_pass = st.secrets["postgres"]["password"]
    
    # Try direct connection with specific parameters for IPv4
    DATABASE_URL = f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}?sslmode=require"
    
    # Configure engine with minimal pooling parameters
    engine = create_engine(
        DATABASE_URL,
        pool_pre_ping=True,
        pool_recycle=300,
        connect_args={
            "application_name": "streamlit_dashboard",
            "keepalives": 1,
            "keepalives_idle": 30,
            "keepalives_interval": 10,
            "keepalives_count": 5
        }
    )
    
    # Test connection
    with engine.connect() as conn:
        conn.execute("SELECT 1")
    
    st.success("✅ Connected to database")
except Exception as e:
    st.error(f"⚠️ Database connection failed: {e}")
    st.info("Make sure you've set up secrets in Streamlit Cloud and enabled IPv4 access in Supabase!")
    
    # Show troubleshooting info
    st.warning("""
    Troubleshooting steps:
    1. Check if you need to purchase the IPv4 add-on in Supabase
    2. Verify your database credentials are correct
    3. Try connecting with the connection string directly from Supabase dashboard
    """)
    st.stop()

st.markdown("<h1>📊 Real-Time Customer Segmentation Dashboard</h1>", unsafe_allow_html=True)

# Sidebar Filters
st.sidebar.header("🔍 Filters")

time_range = st.sidebar.slider("Select Time Range (Hours)", 0, 24, 1)
from_time = pd.Timestamp.now() - pd.Timedelta(hours=time_range)

# Get available clusters dynamically
try:
    clusters_df = pd.read_sql("SELECT DISTINCT cluster FROM customer_segments ORDER BY cluster", engine)
    available_clusters = clusters_df['cluster'].tolist()
    if not available_clusters:
        available_clusters = [0, 1, 2]  # Default if no data
except:
    available_clusters = [0, 1, 2]  # Default if query fails

clusters = st.sidebar.multiselect("Select Clusters", options=sorted(available_clusters), default=available_clusters)

# Get purchase amount range dynamically
try:
    query = "SELECT COALESCE(MIN(purchase_amount), 0) as min, COALESCE(MAX(purchase_amount), 500) as max FROM customer_segments"
    limits = pd.read_sql(query, engine).iloc[0]
    purchase_min, purchase_max = limits['min'], limits['max']
except:
    purchase_min, purchase_max = 0, 500  # Default if query fails

purchase_range = st.sidebar.slider(
    "Purchase Amount Range",
    min_value=float(purchase_min),
    max_value=float(purchase_max),
    value=(float(purchase_min), float(purchase_max))
)

auto_refresh = st.sidebar.checkbox("Enable Auto-Refresh (5s)", value=True)

@st.cache_data(ttl=10)  # Cache for 10 seconds
def load_data():
    if not clusters:
        return pd.DataFrame()  # Empty dataframe if no clusters selected
    
    # Use parameterized query with tuple for clusters
    cluster_tuple = tuple(clusters) if len(clusters) > 1 else f"({clusters[0]})" if clusters else "(0)"
    
    query = f"""
        SELECT record_id, customer_id, name, age, purchase_amount, cluster, created_at 
        FROM customer_segments 
        WHERE created_at >= '{from_time}' 
        AND cluster IN {cluster_tuple}
        AND purchase_amount BETWEEN {purchase_range[0]} AND {purchase_range[1]}
        ORDER BY created_at DESC LIMIT 100
    """
    
    try:
        return pd.read_sql(query, engine)
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return pd.DataFrame()

def render_dashboard(df):
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("💰 Cluster-wise Purchase Distribution")
        fig1 = px.bar(df, x="cluster", y="purchase_amount", color="cluster", 
                      title="Average Purchase by Cluster")
        st.plotly_chart(fig1, use_container_width=True)

    with col2:
        st.subheader("👤 Age Distribution by Cluster")
        fig2 = px.histogram(df, x="age", color="cluster", barmode="overlay",
                           title="Customer Age Distribution by Cluster")
        st.plotly_chart(fig2, use_container_width=True)

    col3, col4 = st.columns(2)

    with col3:
        st.subheader("👥 Total Customers Per Cluster")
        cluster_counts = df["cluster"].value_counts().reset_index()
        cluster_counts.columns = ["Cluster", "Total Customers"]
        fig3 = px.pie(cluster_counts, values="Total Customers", names="Cluster",
                     title="Customer Distribution Across Clusters")
        st.plotly_chart(fig3, use_container_width=True)

    with col4:
        st.subheader("💸 Average Purchase Per Cluster")
        avg_purchase = df.groupby("cluster")["purchase_amount"].mean().reset_index()
        fig4 = px.bar(avg_purchase, x="cluster", y="purchase_amount", color="cluster",
                     title="Average Spending by Customer Segment")
        st.plotly_chart(fig4, use_container_width=True)

    st.subheader("📋 Latest Customer Data")
    st.dataframe(df[["customer_id", "name", "age", "purchase_amount", "cluster", "created_at"]].style.format({
        "purchase_amount": "${:.2f}"
    }))

    # Insights
    col5, col6 = st.columns(2)
    with col5:
        if df["purchase_amount"].max() > 400:
            st.error("⚠️ Alert: High purchase amount detected!")
        
        if df["cluster"].value_counts().get(2, 0) > df["cluster"].value_counts().get(0, 0):
            st.success("✅ More high-value customers than low-value ones!")
    
    with col6:
        st.metric("Total Revenue", f"${df['purchase_amount'].sum():.2f}", 
                 delta=f"{len(df)} customers")

    if st.download_button("📅 Download Data as CSV", df.to_csv(index=False), "customer_segments.csv", "text/csv"):
        st.success("Data downloaded successfully!")

# Main app logic
if auto_refresh:
    df = load_data()
    if df.empty:
        st.warning("⚠️ No data available yet. Make sure the producer and consumer are running.")
    else:
        render_dashboard(df)
    
    # Add auto-refresh using JavaScript (note: this might not work in all Streamlit deployments)
    st.markdown(
        """
        <script>
        setTimeout(function(){
            window.location.reload();
        }, 5000);
        </script>
        """,
        unsafe_allow_html=True
    )
else:
    if st.button("🔄 Refresh Data"):
        st.cache_data.clear()  # Clear cache to ensure fresh data
        df = load_data()
        if df.empty:
            st.warning("⚠️ No data available or no clusters selected.")
        else:
            render_dashboard(df)
    else:
        df = load_data()
        if df.empty:
            st.warning("⚠️ No data available or no clusters selected.")
        else:
            render_dashboard(df)
