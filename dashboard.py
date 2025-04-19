import streamlit as st
import pandas as pd
import plotly.express as px
import numpy as np
from datetime import datetime
import random

# Set page configuration
st.set_page_config(layout="wide", page_title="Customer Segmentation Dashboard", page_icon="🧠")

# Apply dark theme styling
st.markdown("""
<style>
    .main {
        background-color: #121212;
        color: white;
    }
    .stMetric {
        background-color: #1e1e1e;
        padding: 15px;
        border-radius: 5px;
    }
    .st-emotion-cache-1wivap2 {
        border-radius: 5px;
    }
    h1, h2, h3, h4 {
        color: white !important;
    }
    .stDataFrame {
        background-color: #1e1e1e;
    }
    .css-18e3th9 {
        padding-top: 1rem;
    }
    .css-1d391kg {
        padding-top: 3.5rem;
    }
</style>
""", unsafe_allow_html=True)

# Title with icon
st.markdown("<h1 style='text-align: left; color: white;'>🟢 Real-Time Customer Segmentation Dashboard</h1>", unsafe_allow_html=True)

# Generate sample data if needed
def generate_sample_data(n=200):
    names = [f"customer_{i}" for i in range(1, n+1)]
    ages = np.random.randint(18, 65, n)
    
    # Create purchase amounts that correlate somewhat with age
    base_purchases = np.random.lognormal(5, 1, n)
    age_factor = (ages - 18) / 47  # Normalize age to 0-1
    purchase_amounts = base_purchases * (0.7 + 0.6 * age_factor)
    purchase_amounts = np.round(purchase_amounts, 2)
    
    # Simple clustering based on age and purchase amount
    def assign_cluster(age, amount):
        if age < 30 and amount < 200:
            return 0
        elif age >= 40 or amount > 300:
            return 1
        else:
            return 2
    
    clusters = [assign_cluster(age, amount) for age, amount in zip(ages, purchase_amounts)]
    
    # Generate timestamps for the past week
    current_time = datetime.now()
    timestamps = [current_time.replace(
        day=current_time.day - random.randint(0, 7),
        hour=random.randint(0, 23),
        minute=random.randint(0, 59),
        second=random.randint(0, 59)
    ).strftime("%Y-%m-%d %H:%M:%S") for _ in range(n)]
    
    return pd.DataFrame({
        "name": names,
        "age": ages,
        "purchase_amount": purchase_amounts,
        "cluster": clusters,
        "timestamp": timestamps
    })

# Generate or use real data
if "df" not in st.session_state:
    st.session_state.df = generate_sample_data()

df = st.session_state.df

# KPIs
total_customers = len(df)
average_purchase = df["purchase_amount"].mean()
average_age = df["age"].mean()
churn_rate = round((df["purchase_amount"] < 100).sum() / total_customers * 100, 2)

col1, col2, col3, col4 = st.columns(4)
col1.metric("👥 Total Customers", f"{total_customers}")
col2.metric("💸 Avg. Purchase", f"₹{average_purchase:.2f}")
col3.metric("🎂 Avg. Age", f"{average_age:.1f} yrs")
col4.metric("⚠️ Potential Churn Rate", f"{churn_rate}%")

st.markdown("---")

# Charts layout
col_left, col_right = st.columns(2)

with col_left:
    st.markdown("### 💲 Cluster-wise Purchase Distribution")
    fig = px.bar(
        df.groupby("cluster")["purchase_amount"].mean().reset_index(),
        x="cluster", 
        y="purchase_amount",
        labels={"purchase_amount": "Average Purchase", "cluster": "Cluster"},
        color_discrete_sequence=['#2ecc71', '#2ecc71', '#2ecc71']
    )
    fig.update_layout(
        plot_bgcolor="#121212",
        paper_bgcolor="#121212",
        font_color="white",
        margin=dict(l=10, r=10, t=10, b=10),
        xaxis=dict(
            title_font=dict(color="white"),
            tickfont=dict(color="white"),
            gridcolor="#333333"
        ),
        yaxis=dict(
            title_font=dict(color="white"),
            tickfont=dict(color="white"),
            gridcolor="#333333"
        )
    )
    st.plotly_chart(fig, use_container_width=True)

with col_right:
    st.markdown("### 📊 Age Distribution by Cluster")
    fig = px.histogram(
        df, 
        x="age", 
        color="cluster",
        barmode="overlay",
        color_discrete_sequence=px.colors.sequential.YlGn
    )
    fig.update_layout(
        plot_bgcolor="#121212",
        paper_bgcolor="#121212",
        font_color="white",
        margin=dict(l=10, r=10, t=10, b=10),
        xaxis=dict(
            title_font=dict(color="white"),
            tickfont=dict(color="white"),
            gridcolor="#333333"
        ),
        yaxis=dict(
            title_font=dict(color="white"),
            tickfont=dict(color="white"),
            gridcolor="#333333"
        ),
        legend=dict(
            title_font=dict(color="white"),
            font=dict(color="white")
        )
    )
    st.plotly_chart(fig, use_container_width=True)

col_left2, col_right2 = st.columns(2)

with col_left2:
    st.markdown("### 👨‍👩‍👧‍👦 Total Customers Per Cluster")
    pie_df = df["cluster"].value_counts().reset_index()
    pie_df.columns = ["cluster", "count"]
    fig = px.pie(
        pie_df, 
        names="cluster", 
        values="count",
        color="cluster",
        color_discrete_sequence=px.colors.sequential.Greens
    )
    fig.update_layout(
        plot_bgcolor="#121212",
        paper_bgcolor="#121212",
        font_color="white",
        margin=dict(l=10, r=10, t=10, b=10),
        legend=dict(
            title_font=dict(color="white"),
            font=dict(color="white")
        )
    )
    fig.update_traces(
        textinfo='percent+label',
        textfont_color="white"
    )
    st.plotly_chart(fig, use_container_width=True)

with col_right2:
    st.markdown("### 💲 Average Purchase Per Cluster")
    cluster_purchase = df.groupby("cluster")["purchase_amount"].mean().reset_index()
    fig = px.bar(
        cluster_purchase, 
        x="cluster", 
        y="purchase_amount",
        labels={"purchase_amount": "Average Purchase", "cluster": "Cluster"},
        color_discrete_sequence=['#2a9d8f', '#2a9d8f', '#2a9d8f']
    )
    fig.update_layout(
        plot_bgcolor="#121212",
        paper_bgcolor="#121212",
        font_color="white",
        margin=dict(l=10, r=10, t=10, b=10),
        xaxis=dict(
            title_font=dict(color="white"),
            tickfont=dict(color="white"),
            gridcolor="#333333"
        ),
        yaxis=dict(
            title_font=dict(color="white"),
            tickfont=dict(color="white"),
            gridcolor="#333333"
        )
    )
    st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

# Display recent data
st.subheader("📄 Latest Customer Data")
st.dataframe(
    df.sort_values("timestamp", ascending=False).head(10)[["name", "age", "purchase_amount", "cluster", "timestamp"]],
    use_container_width=True,
    height=300
)

# Add a placeholder for alerts
st.markdown("<div style='background-color:#e74c3c; padding: 10px; border-radius: 5px;'>⚠️ Alert: Purchase amount exceeds 2400</div>", unsafe_allow_html=True)

# Add a sidebar with filters if needed
with st.sidebar:
    st.header("Dashboard Controls")
    
    # Add date range filter
    st.subheader("Date Range")
    min_date = pd.to_datetime(df["timestamp"]).min().date()
    max_date = pd.to_datetime(df["timestamp"]).max().date()
    start_date = st.date_input("Start Date", min_date)
    end_date = st.date_input("End Date", max_date)
    
    # Add cluster filter
    st.subheader("Filter by Cluster")
    clusters = sorted(df["cluster"].unique())
    selected_clusters = st.multiselect("Select Clusters", clusters, default=clusters)
    
    # Add button to generate new data
    if st.button("Refresh Data"):
        st.session_state.df = generate_sample_data()
        st.rerun()
