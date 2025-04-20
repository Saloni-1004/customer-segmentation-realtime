from kafka import KafkaProducer
import json
import time
import requests
import logging
import random
from datetime import datetime
import os
import pickle
import names  # Add this library: pip install names for random name generation

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize Kafka producer
try:
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    logger.info("Kafka producer initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize Kafka producer: {e}")
    exit(1)

# File to store persistent customer data
CUSTOMER_DATA_FILE = 'customer_profiles.pkl'

# Load or initialize customer profiles dictionary
customer_profiles = {}
if os.path.exists(CUSTOMER_DATA_FILE):
    try:
        with open(CUSTOMER_DATA_FILE, 'rb') as f:
            customer_profiles = pickle.load(f)
        logger.info(f"Loaded {len(customer_profiles)} existing customer profiles")
    except Exception as e:
        logger.error(f"Error loading customer profiles: {e}")

def fetch_fake_store_api():
    """Fetch customer data from Fake Store API"""
    try:
        response = requests.get("https://fakestoreapi.com/users", timeout=10)
        if response.status_code == 200:
            users = response.json()
            logger.info(f"Successfully fetched {len(users)} users from Fake Store API")
            return users
        logger.warning(f"API returned status code {response.status_code}")
        return []
    except Exception as e:
        logger.error(f"Failed to fetch data from Fake Store API: {e}")
        return []

# Generate realistic customer data with bounded purchase amounts
def generate_customer_data(user):
    # Generate a unique customer_id with a chance for new customers
    base_id = str(user.get("id", 0))
    if random.random() < 0.2:  # 20% chance for a new customer
        customer_id = str(int(time.time()) + random.randint(1000, 9999))  # Unique ID
    else:
        customer_id = base_id

    # Get or generate name data
    if customer_id in customer_profiles:
        profile = customer_profiles[customer_id]
        name_data = profile.get("name", {})
        if isinstance(name_data, dict):
            firstname = name_data.get("firstname", "")
            lastname = name_data.get("lastname", "")
        else:
            firstname = names.get_first_name()
            lastname = names.get_last_name()
    else:
        try:
            name_data = user.get("name", {})
            firstname = name_data.get("firstname", names.get_first_name())
            lastname = name_data.get("lastname", names.get_last_name())
        except (KeyError, TypeError):
            firstname = names.get_first_name()
            lastname = names.get_last_name()

    # Ensure age is available
    if customer_id in customer_profiles:
        age = customer_profiles[customer_id].get("age", random.randint(18, 80))
    else:
        age = user.get("age", random.randint(18, 80))
        # Try to get age from nested structure if available
        if not isinstance(age, int):
            try:
                age = user.get("dob", {}).get("age", random.randint(18, 80))
            except (KeyError, TypeError):
                age = random.randint(18, 80)

    # Realistic purchase amount update - IMPORTANT: Keep under database limit (999,999.99)
    if customer_id in customer_profiles:
        profile = customer_profiles[customer_id]
        previous_amount = profile.get('purchase_amount', 0)
        # Calculate new amount with realistic changes
        change_percent = random.uniform(-0.10, 0.20)  # -10% to +20% change
        
        # Ensure purchase amount stays within database limits (precision 10, scale 2)
        new_amount = round(max(10.0, previous_amount * (1 + change_percent)), 2)
        new_amount = min(new_amount, 999999.99)  # Ensure below database limit
        
        # Occasional big purchase (but still within limits)
        if random.random() < 0.05:
            new_amount = round(random.uniform(500, 5000), 2)
            new_amount = min(new_amount, 999999.99)
    else:
        # New customer - reasonable purchase amount
        new_amount = round(random.uniform(10, 2000), 2)

    # Determine segment based on purchase amount
    if new_amount < 100:
        segment = "Segment-0"  # Low spenders
    elif new_amount < 500:
        segment = "Segment-1"  # Medium spenders
    else:
        segment = "Segment-2"  # High spenders

    # Create data object with only the existing columns
    data = {
        "id": base_id,  # Keep original ID for reference
        "customer_id": customer_id,
        "name": {"firstname": firstname, "lastname": lastname},
        "age": age,
        "purchase_amount": new_amount,
        "segment": segment,
        "timestamp": time.time()
    }
    
    # Update profile in storage
    customer_profiles[customer_id] = data

    return data

# Function to save customer profiles periodically
def save_customer_profiles():
    try:
        with open(CUSTOMER_DATA_FILE, 'wb') as f:
            pickle.dump(customer_profiles, f)
        logger.info(f"Saved {len(customer_profiles)} customer profiles")
    except Exception as e:
        logger.error(f"Error saving customer profiles: {e}")

logger.info("Starting data production loop...")
save_counter = 0

try:
    while True:
        users = fetch_fake_store_api()
        if not users:
            users = [{"id": i} for i in range(1, 11)]  # Fallback if API fails
            logger.warning("Using fallback user data")
        
        for user in users:
            try:
                data = generate_customer_data(user)
                producer.send('customer-data', value=data)
                full_name = f"{data['name']['firstname']} {data['name']['lastname']}"
                logger.info(f"Sent: Customer ID: {data['customer_id']}, Name: {full_name}, Age: {data['age']}, Amount: ${data['purchase_amount']:.2f}")
                # Short delay between messages to avoid flooding
                time.sleep(0.2)
            except Exception as e:
                logger.error(f"Error processing user data: {e}")
        
        # Save customer profiles periodically
        save_counter += 1
        if save_counter >= 5:
            save_customer_profiles()
            save_counter = 0
        
        logger.info(f"Processed {len(users)} users. Waiting 5 seconds before next batch...")
        time.sleep(5)
except KeyboardInterrupt:
    logger.info("Producer stopped by user")
    producer.flush()
    save_customer_profiles()
