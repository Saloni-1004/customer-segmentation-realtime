import sys
import json
import psycopg2
from kafka import KafkaConsumer
import pandas as pd
from sklearn.cluster import KMeans

# Windows Unicode Encode Error Fix
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

print("[INFO] Script starting...")

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    'customer-data',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    group_id='customer-group'
)
print("[INFO] Kafka consumer initialized.")

# Connect to Neon Database
try:
    conn = psycopg2.connect(
        dbname="neondb",
        user="neondb_owner",
        password="npg_5UbnztxlVuD1",
        host="ep-dry-violet-a4v38rh7-pooler.us-east-1.aws.neon.tech",
        port="5432",
        sslmode="require"
    )
    conn.autocommit = True

    cur = conn.cursor()
    print("[INFO] Connected to Neon database successfully!")

except Exception as e:
    print(f"[ERROR] Failed to connect to Neon database: {str(e)}")
    exit(1)

# Process Kafka Messages
data = []
try:
    print("[INFO] Waiting for messages from Kafka...")
    for message in consumer:
        msg = message.value
        print(f"[INFO] Received message: {msg}")
        data.append(msg)

        # Process in Batches of 5
        if len(data) >= 5:
            df = pd.DataFrame(data)
            required_columns = ['customer_id', 'name', 'age', 'purchase_amount']

            if not all(col in df.columns for col in required_columns):
                print(f"[WARNING] Missing columns in data. Found: {df.columns}")
                data = []
                continue

            # Apply K-Means Clustering
            kmeans = KMeans(n_clusters=3, random_state=42)
            df['cluster'] = kmeans.fit_predict(df[['age', 'purchase_amount']])
            print(f"[INFO] Clustered {len(data)} messages.")

            # Insert Data into Neon Database
            for _, row in df.iterrows():
                cur.execute(
                    """
                    INSERT INTO customer_segments (customer_id, name, age, purchase_amount, cluster, created_at)
                    VALUES (%s, %s, %s, %s, %s, NOW())
                    """,
                    (row['customer_id'], row['name'], row['age'], row['purchase_amount'], row['cluster'])
                )
            print(f"[INFO] Stored {len(data)} messages in database.")
            data = []  # Clear batch after processing

except Exception as e:
    print(f"[ERROR] Consumer encountered an issue: {str(e)}")
except KeyboardInterrupt:
    print("[INFO] Stopping consumer...")
finally:
    cur.close()
    conn.close()
    print("[INFO] Database connection closed.")
