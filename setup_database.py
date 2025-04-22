import os
import time
import socket
import psycopg2
from dotenv import load_dotenv
import sys

# Windows Unicode Encode Error Fix
sys.stdout.reconfigure(encoding='utf-8', errors='replace') if hasattr(sys.stdout, 'reconfigure') else None

# Load environment variables
load_dotenv()

def is_port_open(host, port):
    """Check if the database port is open and accepting connections"""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(2)  # 2 Second timeout
    result = sock.connect_ex((host, port))
    sock.close()
    return result == 0

def setup_database(mode):
    """Set up the database tables for the specified mode"""
    print(f"Setting up {mode} database...")
    
    if mode == "local":
        host = os.getenv("LOCAL_DB_HOST")
        port = int(os.getenv("LOCAL_DB_PORT"))
        
        # Check if PostgreSQL is running locally
        if not is_port_open(host, port):
            print(f"Error: PostgreSQL is not running on {host}:{port}")
            print("Please start PostgreSQL server and try again.")
            return False
    
    try:
        # Connect to the database
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
        cur = conn.cursor()
        
        # Create table if it doesn't exist
        cur.execute("""
        CREATE TABLE IF NOT EXISTS customer_segments (
            id SERIAL PRIMARY KEY,
            customer_id VARCHAR(255),
            name VARCHAR(255),
            age INTEGER,
            purchase_amount FLOAT,
            cluster INTEGER,
            created_at TIMESTAMP
        );
        """)
        
        print(f"✓ {mode.upper()} database setup complete!")
        
        # Close connection
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"Error setting up {mode} database: {str(e)}")
        return False
    
    return True

if __name__ == "__main__":
    mode = os.getenv("MODE", "local")
    print(f"Running in {mode.upper()} mode")
    
    # Setup only the database for the current mode
    if mode == "local":
        if setup_database("local"):
            print("✓ Local database is set up and ready to use!")
        else:
            print("There were issues setting up the local database.")
    else:
        if setup_database("deploy"):
            print("✓ Deploy database is set up and ready to use!")
        else:
            print("There were issues setting up the deploy database.")