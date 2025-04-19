import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import time
import hashlib
import psycopg2
import os
from datetime import datetime, timedelta

# Page configuration for dark theme
st.set_page_config(
    page_title="📊 Real-Time Customer Segmentation", 
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for dark theme
dark_theme = """
<style>
    /* Main background */
    .stApp {
        background-color: #0E1117;
        color: #FAFAFA;
    }
    
    /* Sidebar */
    .css-1d391kg {
        background-color: #1E1E1E;
    }
    
    /* Headers */
    h1, h2, h3, h4, h5, h6 {
        color: #FFFFFF !important;
    }
    
    /* Metric values */
    .css-1w26lek {
        color: #FFFFFF;
        font-weight: bold;
    }
    
    /* Dataframe */
    .stDataFrame {
        background-color: #1E1E1E;
    }
    
    /* Cards */
    .card {
        background-color: #1E1E1E;
        border-radius: 10px;
        padding: 20px;
        margin-bottom: 10px;
    }
    
    /* Alert styling */
    .st-alert {
        background-color: #FF4B4B;
        color: white;
    }
    
    /* Button styling */
    .stButton>button {
        background-color: #4CAF50;
        color: white;
    }
    
    /* Icon colors */
    .icon-color {
        color: #4CAF50;
    }
    
    /* Chart background */
    .js-plotly-plot {
        background-color: #1E1E1E !important;
    }
</style>
"""
st.markdown(dark_theme, unsafe_allow_html=True)

# App title with icon
st.markdown('<div style="text-align: center;"><h1 style="color: white; margin-bottom: 20px;">⚡ Real-Time Customer Segmentation Dashboard</h1></div>', unsafe_allow_html=True)

# ---------------------------
# 🔐 Simple Login Function
# ---------------------------
def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

def check_password():
    """Check if the user has entered the correct password."""
    if "password_correct" not in st.session_state:
        st.session_state["password_correct"] = False

    def password_entered():
        """Handle password input and validate it."""
        if "password" in st.session_state:  # Check if password exists in session state
            if hash_password(st.session_state["password"]) == hash_password("admin123"):
                st.session_state["password_correct"] = True
                if "password" in st.session_state:  # Check again before deleting
                    del st.session_state["password"]
            else:
                st.session_state["password_correct"] = False

    if not st.session_state["password_correct"]:
        st.text_input("🔐 Enter Password", type="password", on_change=password_entered, key="password")
        return False
    return True

if not check_password():
    st.stop()

# ---------------------------
# Database Connection Function
# ---------------------------
def get_connection():
    return psycopg2.connect(**st.secrets["postgres"])

# ---------------------------
# 📊 Load data from Neon PostgreSQL
# ---------------------------
@st.cache_data(ttl=5)
def load_data(hours=24, selected_clusters=None):
    try:
        conn = get_connection()
        cursor = conn.cursor()
        where_clauses = []
        params = []

        if hours > 0:
            where_clauses.append("created_at > %s")
            params.append(datetime.now() - timedelta(hours=hours))

        if selected_clusters and len(selected_clusters) > 0:
            placeholders = ', '.join(['%s'] * len(selected_clusters))
            where_clauses.append(f"cluster IN ({placeholders})")
            params.extend(selected_clusters)

        query = "SELECT * FROM customer_segments"
        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)

        cursor.execute(query, params)
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        cursor.close()
        conn.close()
        return pd.DataFrame(data, columns=columns)

    except Exception as e:
        st.error(f"Database error: {e}")
        return pd.DataFrame()

# ---------------------------
# 🔍 Sidebar Filters
# ---------------------------
st.sidebar.markdown('<h2 style="color: #4CAF50;">🔍 Filters</h2>', unsafe_allow_html=True)
time_range = st.sidebar.slider("Select Time Range (Hours)", 0, 24, 1)
clusters = st.sidebar.multiselect("Select Clusters", options=[0, 1, 2], default=[0, 1, 2])

# Add auto-refresh checkbox
auto_refresh = st.sidebar.checkbox("Enable Auto-Refresh (5s)", value=True)

# ---------------------------
# 📋 Load and render data
# ---------------------------
df = load_data(hours=time_range, selected_clusters=clusters)

if df.empty:
    st.warning("⚠️ No data available. Check database connection or data filters.")
else:
    # Key metrics
    total_customers = len(df)
    avg_purchase = df['purchase_amount'].mean()
    avg_age = df['age'].mean()
    
    # Cluster metrics
    cluster_counts = df.groupby('cluster').size().reset_index(name='count')
    
    # Calculate percentages
    total_count = cluster_counts['count'].sum()
    cluster_counts['percentage'] = round((cluster_counts['count'] / total_count) * 100, 1)
    
    # Calculate average spending per cluster
    avg_spending = df.groupby('cluster')['purchase_amount'].mean().reset_index()
    
    # Calculate age distribution 
    age_bins = [10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60]
    df['age_group'] = pd.cut(df['age'], bins=age_bins)
    age_distribution = df.groupby(['age_group']).size().reset_index(name='count')
    
    # Create layout with 2x2 grid for charts
    col1, col2 = st.columns(2)
    
    # Top row charts
    with col1:
        st.markdown('<div class="card"><h3 style="color: #4CAF50;">💲 Cluster-wise Purchase Distribution</h3>', unsafe_allow_html=True)
        
        # Create bar chart with green palette 
        fig_purchase = go.Figure()
        fig_purchase.add_trace(go.Bar(
            x=avg_spending['cluster'].astype(str),
            y=avg_spending['purchase_amount'],
            marker_color=['#1E5631', '#2E8B57', '#3CB371'],
            text=avg_spending['purchase_amount'].round(2),
            textposition='auto'
        ))
        
        fig_purchase.update_layout(
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font=dict(color='white'),
            margin=dict(l=40, r=40, t=30, b=40),
            height=300,
            xaxis=dict(title='Cluster', tickfont=dict(color='white')),
            yaxis=dict(title='Average Purchase ($)', tickfont=dict(color='white'), gridcolor='#333333')
        )
        
        st.plotly_chart(fig_purchase, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="card"><h3 style="color: #FFD700;">👨‍👩‍👧‍👦 Age Distribution by Cluster</h3>', unsafe_allow_html=True)
        
        # Create histogram for age distribution
        fig_age = px.histogram(
            df, 
            x='age',
            nbins=20,
            color_discrete_sequence=['#FFED00', '#FFD700', '#FFBF00']
        )
        
        fig_age.update_layout(
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font=dict(color='white'),
            margin=dict(l=40, r=40, t=30, b=40),
            height=300,
            xaxis=dict(title='Age', tickfont=dict(color='white')),
            yaxis=dict(title='Count', tickfont=dict(color='white'), gridcolor='#333333')
        )
        
        st.plotly_chart(fig_age, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Bottom row charts
    col3, col4 = st.columns(2)
    
    with col3:
        st.markdown('<div class="card"><h3 style="color: #4CAF50;">👥 Total Customers Per Cluster</h3>', unsafe_allow_html=True)
        
        # Create pie chart
        fig_pie = go.Figure(data=[go.Pie(
            labels=cluster_counts['cluster'].astype(str),
            values=cluster_counts['count'],
            textinfo='percent',
            marker=dict(colors=['#1E5631', '#4CAF50', '#8BC34A']),
            textfont=dict(color='black'),
            hovertemplate="Cluster %{label}<br>Count: %{value}<br>Percentage: %{percent}"
        )])
        
        fig_pie.update_layout(
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font=dict(color='white'),
            margin=dict(l=40, r=40, t=30, b=40),
            height=300,
            legend=dict(font=dict(color='white'))
        )
        
        st.plotly_chart(fig_pie, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col4:
        st.markdown('<div class="card"><h3 style="color: #4CAF50;">💰 Average Purchase Per Cluster</h3>', unsafe_allow_html=True)
        
        # Create bar chart for average purchase
        fig_avg = go.Figure()
        fig_avg.add_trace(go.Bar(
            x=avg_spending['cluster'].astype(str),
            y=avg_spending['purchase_amount'],
            marker_color=['#1E5631', '#2E8B57', '#3CB371'],
            text=avg_spending['purchase_amount'].round(2),
            textposition='auto'
        ))
        
        fig_avg.update_layout(
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font=dict(color='white'),
            margin=dict(l=40, r=40, t=30, b=40),
            height=300,
            xaxis=dict(title='Cluster', tickfont=dict(color='white')),
            yaxis=dict(title='Average Purchase ($)', tickfont=dict(color='white'), gridcolor='#333333')
        )
        
        st.plotly_chart(fig_avg, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Customer data table with styling
    st.markdown('<div class="card"><h3 style="color: white;">📋 Latest Customer Data</h3>', unsafe_allow_html=True)
    
    # Function to highlight rows based on condition
    def highlight_high_purchase(s, threshold=400):
        return ['background-color: #ff4d4d; color: white' if x > threshold else '' for x in s]
    
    # Format and display the table
    display_df = df[["customer_id", "name", "age", "purchase_amount", "cluster", "created_at"]]
    if 'created_at' in display_df.columns and not display_df.empty:
        display_df['created_at'] = pd.to_datetime(display_df['created_at']).dt.strftime('%Y-%m-%d %H:%M:%S')
    
    # Style the dataframe
    styled_df = display_df.sort_values(by='created_at', ascending=False).style.apply(
        highlight_high_purchase, threshold=400, subset=['purchase_amount']
    )
    
    st.dataframe(styled_df, height=200, use_container_width=True)
    
    # Add warning for high purchases
    high_purchases = display_df[display_df['purchase_amount'] > 400]
    if not high_purchases.empty:
        st.markdown('<p style="color: #ff4d4d;">⚠️ Alert: Purchase amount exceeds 400</p>', unsafe_allow_html=True)
    
    # Add download button
    csv = display_df.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="Download Data as CSV",
        data=csv,
        file_name='customer_segments.csv',
        mime='text/csv',
    )
    
    st.markdown('</div>', unsafe_allow_html=True)

# ---------------------------
# 🔁 Auto-refresh
# ---------------------------
if auto_refresh:
    time.sleep(5)
    st.rerun()
