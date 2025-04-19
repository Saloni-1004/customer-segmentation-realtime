import streamlit as st
import pandas as pd
import plotly.express as px
from kafka import KafkaConsumer
import json
from datetime import datetime

st.set_page_config(layout="wide")
st.title("🧠 Real-Time Customer Segmentation Dashboard")

# Initialize session state to store customer data
if "customer_data" not in st.session_state:
    st.session_state.customer_data = []

# Kafka setup
KAFKA_TOPIC = "customer-segmentation"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="latest",
    enable_auto_commit=True,
    group_id='dashboard-group'
)

# Collect one latest message
for message in consumer:
    data = message.value
    if data and all(k in data for k in ["name", "age", "purchase_amount", "cluster"]):
        data["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        st.session_state.customer_data.append(data)
    break

# Convert to DataFrame
if st.session_state.customer_data:
    df = pd.DataFrame(st.session_state.customer_data)
    df["purchase_amount"] = pd.to_numeric(df["purchase_amount"], errors='coerce')
    df["age"] = pd.to_numeric(df["age"], errors='coerce')
    df["cluster"] = pd.to_numeric(df["cluster"], errors='coerce')

    # KPIs
    total_customers = len(df)
    average_purchase = df["purchase_amount"].mean()
    average_age = df["age"].mean()
    churn_rate = round((df["purchase_amount"] < 100).sum() / total_customers * 100, 2)

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("👥 Total Customers", total_customers)
    col2.metric("💸 Avg. Purchase", f"₹{average_purchase:.2f}")
    col3.metric("🎂 Avg. Age", f"{average_age:.1f} yrs")
    col4.metric("⚠️ Potential Churn Rate", f"{churn_rate}%")

    st.markdown("---")

    # Charts layout
    col5, col6 = st.columns(2)
    with col5:
        fig = px.bar(df.groupby("cluster")["purchase_amount"].mean().reset_index(),
                     x="cluster", y="purchase_amount",
                     title="💰 Average Purchase Per Cluster",
                     color_discrete_sequence=['#2ecc71'])
        st.plotly_chart(fig, use_container_width=True)

    with col6:
        fig = px.histogram(df, x="age", color="cluster", barmode="overlay",
                           title="📊 Age Distribution by Cluster",
                           color_discrete_sequence=px.colors.sequential.YlGn)
        st.plotly_chart(fig, use_container_width=True)

    col7, col8 = st.columns(2)
    with col7:
        pie_df = df["cluster"].value_counts().reset_index()
        pie_df.columns = ["cluster", "count"]
        fig = px.pie(pie_df, names="cluster", values="count",
                     title="🧑‍🤝‍🧑 Total Customers Per Cluster",
                     color_discrete_sequence=px.colors.sequential.Greens)
        st.plotly_chart(fig, use_container_width=True)

    with col8:
        cluster_purchase = df.groupby("cluster")["purchase_amount"].sum().reset_index()
        fig = px.bar(cluster_purchase, x="cluster", y="purchase_amount",
                     title="📈 Total Purchase by Cluster",
                     color_discrete_sequence=px.colors.sequential.Blues)
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # Display recent data
    st.subheader("📄 Latest Customer Entries")
    st.dataframe(df.tail(10)[["name", "age", "purchase_amount", "cluster", "timestamp"]], use_container_width=True, height=300)
else:
    st.info("Waiting for customer data... Make sure your Kafka producer is running and sending data to the topic.")
