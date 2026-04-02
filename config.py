# Here is where I keep my Credentials.
# Note - Create a .env file later on
# Fill '_" with coorect details


import os
from dotenv import load_dotenv

# Load variables from your .env file
load_dotenv()

class Config:
    # --- PostgreSQL  ---
    DB_HOST = '_' 
    DB_PORT = '_'
    DB_USER = '_'
    DB_PASS = '_'
    DB_NAME = '_'
    DB_SCHEMA = '_'
    
    # --- Snowflake (Specific to your AWS Standard Edition or the cloud you use in this case i used aws astandard edition) ---
    # Using the Account Identifier is most reliable for the connector
    SNOW_ACCOUNT = os.getenv('SNOW_ACCOUNT', '_') 
    SNOW_USER = os.getenv('SNOW_USER', '_')               
    SNOW_PASS = os.getenv('SNOW_PASS')                          
    
    # Standard Edition defaults
    SNOW_WH = os.getenv('SNOW_WAREHOUSE', 'COMPUTE_WH')         
    SNOW_DB = os.getenv('SNOW_DATABASE', 'FOOD_PRICES_PROD')    
    SNOW_SCHEMA = os.getenv('SNOW_SCHEMA', 'FOOD_ETL')          
    
    # --- File Paths ---
    RAW_DATA_PATH = 'data/wfp_food_prices_ken.csv'

# Local use in windows / other scripts
DB_CONFIG = {
    'user': Config.DB_USER,
    'pass': Config.DB_PASS,
    'host': Config.DB_HOST,
    'port': Config.DB_PORT,
    'db': Config.DB_NAME,
    'schema': Config.DB_SCHEMA
}