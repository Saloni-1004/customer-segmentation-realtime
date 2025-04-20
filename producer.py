import requests
import random
import time
from sqlalchemy import create_engine, text
import urllib.parse
from datetime import datetime

# Neon Database Connection
DB_HOST = "ep-dry-violet-a4v38rh7-pooler.us-east-1.aws.neon.tech"
DB_PORT = 5432
DB_NAME = "neondb"
DB_USER = "neondb_owner"
DB_PASSWORD = urllib.parse.quote_plus("npg_5UbnztxlVuD1")
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode=require"

# Initialize database engine
try:
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))  # Test connection
        print("Database connection established successfully.")
except Exception as e:
    print(f"Failed to connect to database: {e}")
    exit(1)

def fetch_real_data():
    response = requests.get("https://fakestoreapi.com/users")
    if response.status_code == 200:
        return response.json()
    return []

# Counters for customer_id and name uniqueness
customer_id_counter = 1  # Start at 1
name_counter = 1

while True:
    users = fetch_real_data()
    if not users:
        print("Failed to fetch data from API, retrying in 5 seconds...")
        time.sleep(5)
        continue

    for user in users:
        customer_id = str(customer_id_counter)  # Simple integer as customer_id (e.g., "1", "2")
        unique_name = f"{user['name']['firstname']}_{name_counter}"  # e.g., "john_1"
        
        data = {
            "customer_id": customer_id,
            "name": unique_name,
            "age": random.randint(18, 65),
            "purchase_amount": round(random.uniform(10, 500), 2),
            "cluster": random.randint(0, 2),  # Randomly assign cluster 0, 1, or 2
            "created_at": datetime.now()  # Use current timestamp
        }
        
        # Insert data into customer_segments table
        with engine.connect() as conn:
            try:
                query = text("""
                    INSERT INTO customer_segments (customer_id, name, age, purchase_amount, cluster, created_at)
                    VALUES (:customer_id, :name, :age, :purchase_amount, :cluster, :created_at)
                """)
                conn.execute(query, **data)  # Corrected to use dictionary unpacking with text()
                conn.commit()
                print(f"Successfully inserted record with customer_id: {customer_id}")
            except Exception as e:
                print(f"Insertion failed: {e}")
        
        customer_id_counter += 1  # Increment customer_id
        name_counter += 1  # Increment name counter
    
    time.sleep(5)  # Wait 5 seconds before fetching the next batch
