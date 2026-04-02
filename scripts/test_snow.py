import sys
import os
# Add the parent directory to the python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import snowflake.connector
from config import Config 
from dotenv import load_dotenv

load_dotenv() # Crucial to load your SNOW_PASS from .env

try:
    conn = snowflake.connector.connect(
        user=Config.SNOW_USER,
        password=os.getenv('SNOW_PASS'),
        account=Config.SNOW_ACCOUNT,
        warehouse=Config.SNOW_WH,
        database=Config.SNOW_DB,
        schema=Config.SNOW_SCHEMA
    )
    print("✅ Success! HOPEFUL7 is connected to Snowflake.")
    conn.close()
except Exception as e:
    print(f"❌ Connection failed: {e}")