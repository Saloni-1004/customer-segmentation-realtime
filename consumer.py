import sys
import json
import time
import psycopg2
from kafka import KafkaConsumer
import pandas as pd
from sklearn.cluster import KMeans
from kafka.errors import NoBrokersAvailable
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Windows Unicode Encode Error Fix
sys.stdout.reconfigure(encoding='utf-8', errors='replace') if hasattr(sys.stdout, 'reconfigure') else None

print("[INFO] Script starting...")

# Get the mode from environment variable
mode = os.getenv("MODE", "local")
print(f"[INFO] Running in {mode.upper()} mode")

# Initialize Kafka Consumer with retry logic
def initialize_consumer():
    max_retries = 5
    for attempt in range(max_retries):
        try:
            consumer = KafkaConsumer(
                'customer-data',
                bootstrap_servers=['localhost:9092'],
                auto_offset_reset='earliest',
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                group_id='customer-group'
            )
            print("[INFO] Kafka consumer initialized.")
            return consumer
        except NoBrokersAvailable:
            print(f"[INFO] Attempt {attempt + 1} failed: No Kafka brokers available. Retrying in 5 seconds...")
            time.sleep(5)
        except Exception as e:
            print(f"[ERROR] Failed to initialize Kafka consumer: {e}")
            time.sleep(5)
    print("[ERROR] Max retries reached. Exiting.")
    exit(1)

# Get database connection details based on mode
def get_db_connection():
    try:
        if mode == "local":
            conn = psycopg2.connect(
                dbname=os.getenv("LOCAL_DB_NAME"),
                user=os.getenv("LOCAL_DB_USER"),
                password=os.getenv("LOCAL_DB_PASSWORD"),
                host=os.getenv("LOCAL_DB_HOST"),
                port=os.getenv("LOCAL_DB_PORT")
            )
        else:
            conn = psycopg2.connect(
                dbname=os.getenv("DEPLOY_DB_NAME"),
                user=os.getenv("DEPLOY_DB_USER"),
                password=os.getenv("DEPLOY_DB_PASSWORD"),
                host=os.getenv("DEPLOY_DB_HOST"),
                port=os.getenv("DEPLOY_DB_PORT"),
                sslmode=os.getenv("DEPLOY_DB_SSLMODE")
            )
        conn.autocommit = True
        print("[INFO] Connected to database successfully!")
        return conn
    except Exception as e:
        print(f"[ERROR] Failed to connect to database: {str(e)}")
        exit(1)

# Initialize consumer
consumer = initialize_consumer()

# Connect to the appropriate database
conn = get_db_connection()
cur = conn.cursor()

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
            # Convert to DataFrame
            df = pd.DataFrame(data)
            required_columns = ['customer_id', 'name', 'age', 'purchase_amount']

            if not all(col in df.columns for col in required_columns):
                print(f"[WARNING] Missing columns in data. Found: {df.columns}")
                data = []
                continue

            # Apply K-Means Clustering
            try:
                kmeans = KMeans(n_clusters=3, random_state=42)
                df['cluster'] = kmeans.fit_predict(df[['age', 'purchase_amount']])
                print(f"[INFO] Clustered {len(data)} messages.")
            except Exception as e:
                print(f"[ERROR] Clustering failed: {e}")
                # Set default cluster as 0 if clustering fails
                df['cluster'] = 0

            # Insert Data into Database
            inserted_count = 0
            for _, row in df.iterrows():
                try:
                    cur.execute(
                        """
                        INSERT INTO customer_segments (customer_id, name, age, purchase_amount, cluster, created_at)
                        VALUES (%s, %s, %s, %s, %s, NOW())
                        """,
                        (row['customer_id'], row['name'], row['age'], row['purchase_amount'], row['cluster'])
                    )
                    inserted_count += 1
                except Exception as e:
                    print(f"[ERROR] Failed to insert row: {e}")
            
            print(f"[INFO] Stored {inserted_count} of {len(data)} messages in database.")
            data = []  # Clear batch after processing

except NoBrokersAvailable as e:
    print(f"[ERROR] Consumer encountered an issue: {e}. Check if Kafka is running on localhost:9092.")
    consumer = initialize_consumer()  # Reinitialize consumer
except Exception as e:
    print(f"[ERROR] Consumer encountered an issue: {str(e)}")
except KeyboardInterrupt:
    print("[INFO] Stopping consumer...")
finally:
    cur.close()
    conn.close()
<<<<<<< HEAD
    print("[INFO] Database connection closed.")
=======
    print("[INFO] Database connection closed.")
>>>>>>> 035dd0f6c347e75ed81045c6841acaf08148db93
