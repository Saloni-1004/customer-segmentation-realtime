import streamlit as st
import pandas as pd
import plotly.express as px
import time
import hashlib
import psycopg2
import os
from datetime import datetime, timedelta

# Page configuration
st.set_page_config(page_title="📊 Real-Time Customer Segmentation", layout="wide")

# ---------------------------
# 🔐 Simple Login Function
# ---------------------------

def hash_password(password):
    """Hash the password using SHA256."""
    return hashlib.sha256(password.encode()).hexdigest()

def check_password():
    """Check if the entered password is correct."""
    if "password_correct" not in st.session_state:
        st.session_state["password_correct"] = False

    def password_entered():
        """Handle password input and validate it."""
        if hash_password(st.session_state["password"]) == hash_password("admin123"):  # Set your password here
            st.session_state["password_correct"] = True
            del st.session_state["password"]  # Remove the password after successful login
        else:
            st.session_state["password_correct"] = False

    if not st.session_state["password_correct"]:
        st.text_input("🔐 Enter Password", type="password", on_change=password_entered, key="password")
        st.error("❌ Incorrect password")
        return False
    else:
        return True

# Check if the user is logged in
if not check_password():
    st.stop()  # Stop the rest of the dashboard if not logged in

# ---------------------------
# Database Connection Function
# ---------------------------
def get_connection():
    # For local testing, use direct parameters
    if "STREAMLIT_SHARING" not in os.environ:
        return psycopg2.connect(
            host="ep-divine-credit-a4zo7ml-pooler.us-east-1.aws.neon.tech",
            port="5432",
            dbname="neondb",
            user="neondb_owner",
            password="npg_N4VDg6bocCul",
            sslmode='require'
        )
    # For Streamlit Cloud, use secrets
    else:
        return psycopg2.connect(**st.secrets["postgres"])

# ---------------------------
# 📊 Load data from Neon PostgreSQL
# ---------------------------
@st.cache_data(ttl=5)
def load_data(hours=24, selected_clusters=None):
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        # Build WHERE clause for filtering
        where_clauses = []
        params = []
        
        # Filter by time if specified
        if hours > 0:
            where_clauses.append("created_at > %s")
            params.append(datetime.now() - timedelta(hours=hours))
            
        # Filter by clusters if specified
        if selected_clusters and len(selected_clusters) > 0:
            placeholders = ', '.join(['%s'] * len(selected_clusters))
            where_clauses.append(f"cluster IN ({placeholders})")
            params.extend(selected_clusters)
            
        # Construct the full query
        query = "SELECT * FROM customer_segments"
        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)
        
        # Execute query
        cursor.execute(query, params)
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        
        # Close connection
        cursor.close()
        conn.close()
        
        # Convert to DataFrame
        df = pd.DataFrame(data, columns=columns)
        return df
        
    except Exception as e:
        st.error(f"Database error: {e}")
        return pd.DataFrame()

# ---------------------------
# 🔍 Sidebar Filters
# ---------------------------
st.sidebar.header("🔍 Filters")
time_range = st.sidebar.slider("Select Time Range (Hours)", 0, 24, 1)
clusters = st.sidebar.multiselect("Select Clusters", options=[0, 1, 2], default=[0, 1, 2])

# ---------------------------
# 📋 Load and render data
# ---------------------------
df = load_data(hours=time_range, selected_clusters=clusters)

if df.empty:
    st.warning("⚠️ No data available. Check database connection or data filters.")
else:
    st.markdown("<h1>📊 Customer Segmentation Dashboard</h1>", unsafe_allow_html=True)
    
    # Show totals
    total_customers = len(df)
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Customers", total_customers)
    col2.metric("Average Purchase", f"${df['purchase_amount'].mean():.2f}")
    col3.metric("Average Age", f"{df['age'].mean():.1f}")
    
    # Charts and data
    chart_col, data_col = st.columns(2)
    
    with chart_col:
        st.subheader("👥 Customers Per Cluster")
        cluster_counts = df.groupby('cluster').size().reset_index(name='count')
        fig1 = px.bar(
            cluster_counts, 
            x="cluster", 
            y="count", 
            color="cluster", 
            labels={"cluster": "Cluster", "count": "Number of Customers"},
            title="Customer Distribution by Segment",
            color_discrete_sequence=px.colors.qualitative.Set1
        )
        st.plotly_chart(fig1, use_container_width=True)
        
        # Add spending by cluster chart
        st.subheader("💰 Average Spending by Cluster")
        avg_spending = df.groupby('cluster')['purchase_amount'].mean().reset_index()
        fig2 = px.bar(
            avg_spending,
            x="cluster",
            y="purchase_amount",
            color="cluster",
            labels={"cluster": "Cluster", "purchase_amount": "Average Purchase ($)"},
            title="Average Customer Spending by Segment",
            color_discrete_sequence=px.colors.qualitative.Set1
        )
        st.plotly_chart(fig2, use_container_width=True)
        
    with data_col:
        st.subheader("📋 Latest Customer Data")
        display_df = df[["customer_id", "name", "age", "purchase_amount", "cluster", "created_at"]]
        
        if 'created_at' in display_df.columns and not display_df.empty:
            display_df['created_at'] = pd.to_datetime(display_df['created_at']).dt.strftime('%Y-%m-%d %H:%M:%S')
            
        st.dataframe(
            display_df.sort_values(by='created_at', ascending=False),
            height=400,
            use_container_width=True
        )

# ---------------------------
# 💾 Auto-refresh
# ---------------------------
if st.sidebar.checkbox("Enable Auto-Refresh (5s)", value=True):
    time.sleep(5)
    st.rerun()
