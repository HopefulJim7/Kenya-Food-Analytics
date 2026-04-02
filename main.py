import logging
import os
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from config import Config
from scripts.extract import extract_data
from scripts.clean import clean_data
from scripts.load import (
    clear_tables,
    filter_existing_dimension_rows,
    get_latest_loaded_date,
    load_to_postgres,
)

# Setup logging to help see progresss in my terminal
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

def load_to_snowflake(df, table_name):
    try:
        conn = snowflake.connector.connect(
            user=Config.SNOW_USER,
            password=Config.SNOW_PASS,
            account=Config.SNOW_ACCOUNT,
            warehouse=Config.SNOW_WH,
            database=Config.SNOW_DB,
            schema=Config.SNOW_SCHEMA
        )
        
        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=table_name.upper(),
            database=Config.SNOW_DB,
            schema=Config.SNOW_SCHEMA,
            auto_create_table=True,
            overwrite=True
        )
        
        if success:
            logging.info(f"Successfully loaded {nrows} rows to Snowflake table {table_name.upper()}.")
        conn.close()
    except Exception as e:
        logging.error(f"Snowflake Load Error: {e}")
        raise

def run_pipeline(full_refresh=False):
    # URL - for downlaoding the data set
    URL = "https://data.humdata.org/dataset/wfp-food-prices-for-kenya/resource/517ee1bf-2437-4f8c-aa1b-cb9925b9d437/download/wfp_food_prices_ken.csv"
    DATA_PATH = "data/wfp_food_prices_ken.csv"
    
    try:
        logging.info("--- ⏩ Starting Pipeline ---")

        if full_refresh:
            logging.info("Full refresh requested. Clearing existing warehouse tables first.")
            clear_tables()
        # Extract - makes sure the file exists in data folder
        raw_csv_file = extract_data(URL, DATA_PATH)

        # Step 1: Read and clean the extracted CSV
        raw_df = pd.read_csv(raw_csv_file)
        df = clean_data(raw_df)

        latest_loaded_date = None if full_refresh else get_latest_loaded_date()
        if latest_loaded_date is not None:
            df = df[pd.to_datetime(df['date_key']) > pd.Timestamp(latest_loaded_date)].copy()
            logging.info(f"Incremental filter applied. {len(df)} rows remain after {latest_loaded_date}.")

        if df.empty:
            logging.info("No new dates found to process. Pipeline finished without loading new rows.")
            return
        
        # Step 2
        dim_commodity = df[['commodity_name', 'category', 'unit']].drop_duplicates()
        dim_market = df[['market_name', 'region', 'county']].drop_duplicates()
        dim_date = df[['date_key', 'year', 'month', 'day', 'month_name']].drop_duplicates()

        dim_commodity_filtered = filter_existing_dimension_rows(dim_commodity, 'dim_commodity')
        dim_market_filtered = filter_existing_dimension_rows(dim_market, 'dim_market')
        dim_date_filtered = filter_existing_dimension_rows(dim_date, 'dim_date')
        
        load_to_postgres(dim_commodity_filtered, 'dim_commodity')
        load_to_postgres(dim_market_filtered, 'dim_market')
        load_to_postgres(dim_date_filtered, 'dim_date')
        
        # Step 3: Load Fact Table
        fact_prices = df[
            ['date_key', 'commodity_name', 'market_name', 'unit_weight', 'price_value', 'price_per_kg']
        ]
        load_to_postgres(fact_prices, 'fact_prices')

        logging.info("Mirroring data to Snowflake...")
        load_to_snowflake(dim_commodity, 'dim_commodity')
        load_to_snowflake(dim_market, 'dim_market')
        load_to_snowflake(dim_date, 'dim_date')
        load_to_snowflake(fact_prices, 'fact_prices')
        
        logging.info("--- ETL COMPLETE: Data is now in Star Schema format 🎉 ---")   
        
    except Exception as e:
        logging.error(f"Pipeline crashed 💥: {e}")
        raise
        
if __name__ == "__main__":
    full_refresh = os.getenv("FULL_REFRESH", "").lower() in {"1", "true", "yes"}
    run_pipeline(full_refresh=full_refresh)