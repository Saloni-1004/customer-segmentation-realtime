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
# 🔢 Real-Time Customer Segmentation Dashboard
# ---------------------------

# PostgreSQL connection setup
try:
    conn = psycopg2.connect(
        host="ep-divine-credit-a4zo7ml-pooler.us-east-1.aws.neon.tech",
        port="5432",
        dbname="neondb",
        user="neondb_owner",
        password="npg_N4VDg6bocCul",
        sslmode='require'
    )
    cursor = conn.cursor()
    st.write("[INFO] Connected to PostgreSQL.")
except Exception as e:
    st.write(f"[ERROR] Failed to connect to PostgreSQL: {e}")
    st.stop()

# ---------------------------
# Fetch Data for Customer Segmentation
# ---------------------------
# You can replace this with your query to fetch customer segmentation data from the database
cursor.execute("SELECT * FROM customer_segments LIMIT 100;")
data = cursor.fetchall()

# Convert data to DataFrame
columns = ["record_id", "customer_id", "name", "age", "purchase_amount", "cluster", "created_at"]
df = pd.DataFrame(data, columns=columns)

# Display customer segmentation data
st.write("### Customer Segmentation Data", df)

# ---------------------------
# Create a Plotly Visualization for Clusters
# ---------------------------
fig = px.scatter(df, x="age", y="purchase_amount", color="cluster", hover_data=["name", "customer_id"])
fig.update_layout(title="Customer Segments - Age vs Purchase Amount")
st.plotly_chart(fig)

# ---------------------------
# Time-based Filtering
# ---------------------------
# Filter customers who joined in the last 30 days
today = datetime.today()
thirty_days_ago = today - timedelta(days=30)
filtered_df = df[df["created_at"] >= thirty_days_ago]

st.write(f"### Customers who joined in the last 30 days ({today.strftime('%Y-%m-%d')})")
st.write(filtered_df)

# ---------------------------
# Display Real-Time Updates (Optional)
# ---------------------------
# Simulate real-time data update
st.write("### Real-Time Data Simulation")
for i in range(10):
    st.write(f"Real-time update #{i+1}: New customer data received...")
    time.sleep(1)
