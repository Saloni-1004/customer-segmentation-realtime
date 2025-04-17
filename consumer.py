import json
import time
import random
import requests
import psycopg2

# Supabase PostgreSQL Connection
conn = psycopg2.connect(
    host="db.fsulfssfgmgxosgpjjiw.supabase.co",
    dbname="postgres",
    user="postgres",
    password="Adminsaloni@10",  # 👈 change this to your real password
    port=5432
)
cursor = conn.cursor()

# Function to insert data into Supabase PostgreSQL
def insert_customer(data):
    cursor.execute("""
        INSERT INTO customer_segments (customer_id, name, age, purchase_amount, created_at, cluster)
        VALUES (%s, %s, %s, %s, to_timestamp(%s), %s)
    """, (
        data["customer_id"],
        data["name"],
        data["age"],
        data["purchase_amount"],
        data["timestamp"],
        data["cluster"]
    ))
    conn.commit()

# Fetch users from Fake Store API
def fetch_real_data():
    response = requests.get("https://fakestoreapi.com/users")
    if response.status_code == 200:
        return response.json()
    return []

# Unique counters
customer_id_counter = 1
name_counter = 1

# Main loop
while True:
    users = fetch_real_data()
    if not users:
        print("⚠️ Failed to fetch data from API, retrying in 5 seconds...")
        time.sleep(5)
        continue

    for user in users:
        customer_id = str(customer_id_counter)
        unique_name = f"{user['name']['firstname']}_{name_counter}"

        data = {
            "customer_id": customer_id,
            "name": unique_name,
            "age": random.randint(18, 65),
            "purchase_amount": round(random.uniform(10, 500), 2),
            "timestamp": time.time(),
            "cluster": random.randint(0, 2)
        }

        try:
            insert_customer(data)
            print(f"✅ Inserted into Supabase: {data}")
        except Exception as e:
            print(f"❌ Failed to insert: {e}")

        customer_id_counter += 1
        name_counter += 1

    time.sleep(5)

