import random
import time
import json
import os
from sqlalchemy import create_engine, text
import urllib.parse
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from dotenv import load_dotenv
import sys

# Windows Unicode Encode Error Fix
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# Load environment variables
load_dotenv()

# Get the mode from environment variable
mode = os.getenv("MODE", "local")
print(f"Running in {mode.upper()} mode")

# Database connection based on mode
if mode == "local":
    DB_HOST = os.getenv("LOCAL_DB_HOST")
    DB_PORT = os.getenv("LOCAL_DB_PORT") 
    DB_NAME = os.getenv("LOCAL_DB_NAME")
    DB_USER = os.getenv("LOCAL_DB_USER")
    DB_PASSWORD = urllib.parse.quote_plus(os.getenv("LOCAL_DB_PASSWORD"))
    DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
else:
    DB_HOST = os.getenv("DEPLOY_DB_HOST")
    DB_PORT = os.getenv("DEPLOY_DB_PORT")
    DB_NAME = os.getenv("DEPLOY_DB_NAME")
    DB_USER = os.getenv("DEPLOY_DB_USER")
    DB_PASSWORD = urllib.parse.quote_plus(os.getenv("DEPLOY_DB_PASSWORD"))
    DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode={os.getenv('DEPLOY_DB_SSLMODE')}"

# Initialize Kafka producer with retry logic
def initialize_producer():
    max_retries = 5
    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=['localhost:9092'],
                value_serializer=lambda x: json.dumps(x).encode('utf-8')
            )
            print("Kafka producer initialized successfully.")
            return producer
        except NoBrokersAvailable:
            print(f"Attempt {attempt + 1} failed: No Kafka brokers available. Retrying in 5 seconds...")
            time.sleep(5)
        except Exception as e:
            print(f"Failed to initialize Kafka producer: {e}")
            time.sleep(5)
    print("Max retries reached. Exiting.")
    exit(1)

producer = initialize_producer()

# Initialize database engine
try:
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))  # Test connection
        print("Database connection established successfully.")
except Exception as e:
    print(f"Failed to connect to database: {e}")
    exit(1)

# Sample first names and last names for generating random names
first_names = ['John', 'Jane', 'Michael', 'Emily', 'David', 'Sarah', 'Robert', 'Lisa', 
               'William', 'Emma', 'James', 'Olivia', 'Daniel', 'Sophia', 'Matthew', 'Ava']
last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Miller', 'Davis', 'Garcia',
              'Wilson', 'Martinez', 'Anderson', 'Taylor', 'Thomas', 'Moore', 'Jackson', 'Martin']

# Counters for customer_id and name uniqueness
customer_id_counter = 1

print("Starting to generate customer data...")

try:
    while True:
        # Generate between 1-3 customers per batch
        for _ in range(random.randint(1, 3)):
            # Generate a unique customer ID
            customer_id = str(customer_id_counter)
            
            # Generate a random name
            first_name = random.choice(first_names)
            last_name = random.choice(last_names)
            unique_name = f"{first_name}_{last_name}_{customer_id_counter}"
            
            # Generate age (weighted towards specific age groups)
            age_group = random.choices(
                [1, 2, 3, 4, 5],  # 1=18-25, 2=26-35, 3=36-45, 4=46-55, 5=56-65
                weights=[0.25, 0.3, 0.25, 0.15, 0.05]
            )[0]
            
            age_ranges = {
                1: (18, 25),
                2: (26, 35),
                3: (36, 45),
                4: (46, 55),
                5: (56, 65)
            }
            
            age = random.randint(*age_ranges[age_group])
            
            # Purchase amount based on age (with some randomness)
            base_amount = {
                1: 50,   # 18-25
                2: 150,  # 26-35
                3: 250,  # 36-45
                4: 200,  # 46-55
                5: 100   # 56-65
            }[age_group]
            
            # Add randomness factor (±50% of base amount)
            variation = random.uniform(0.5, 1.5)
            purchase_amount = round(base_amount * variation, 2)
            
            # Create customer data
            data = {
                "customer_id": customer_id,
                "name": unique_name,
                "age": age,
                "purchase_amount": purchase_amount,
                "created_at": datetime.now().isoformat()
            }
            
            # Send message to Kafka with retry
            try:
                producer.send('customer-data', value=data)
                producer.flush()  # Ensure message is sent
                print(f"Sent message to Kafka: {data}")
            except NoBrokersAvailable as e:
                print(f"Failed to send message: {e}. Check if Kafka is running on localhost:9092.")
                producer = initialize_producer()  # Reinitialize producer
            except Exception as e:
                print(f"Failed to send message to Kafka: {e}")
            
            # Insert data directly into database as well
            kafka_data = data.copy()
            kafka_data["cluster"] = random.randint(0, 2)  # Assign a random cluster
            kafka_data["created_at"] = datetime.now()
            
            try:
                with engine.connect() as conn:
                    query = text("""
                        INSERT INTO customer_segments (customer_id, name, age, purchase_amount, cluster, created_at)
                        VALUES (:customer_id, :name, :age, :purchase_amount, :cluster, :created_at)
                    """)
                    conn.execute(query, parameters=kafka_data)
                    conn.commit()
                    print(f"Successfully inserted record into database: {kafka_data['customer_id']}")
            except Exception as e:
                print(f"Database insertion failed: {e}")
            
            # Increment the customer ID counter
            customer_id_counter += 1
        
        # Wait between batches (2-5 seconds)
        delay = random.uniform(2, 5)
        print(f"Waiting {delay:.1f} seconds before next batch...")
        time.sleep(delay)

except KeyboardInterrupt:
    print("Producer stopped by user.")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    print("Producer shutting down.")