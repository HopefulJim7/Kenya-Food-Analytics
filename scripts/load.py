import sys
import os
import logging
import pandas as pd
from sqlalchemy import create_engine, text

# Fix pathing for Ubuntu/WSL environments
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config import DB_CONFIG

# Configure logging to see progress in your WSL terminal
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

def get_engine():
    """Creates a SQLAlchemy engine using the project configuration."""
    # Using psycopg2 driver explicitly for robust Postgres support
    conn_str = f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['pass']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['db']}"
    return create_engine(conn_str)


def get_latest_loaded_date():
    """Return the latest date_key already present in the fact table."""
    engine = get_engine()
    schema = DB_CONFIG.get('schema', 'food_etl')
    query = text(f"SELECT MAX(date_key) FROM {schema}.fact_prices")

    try:
        with engine.connect() as conn:
            latest_date = conn.execute(query).scalar()
            if latest_date is not None:
                logging.info(f"Latest loaded date in {schema}.fact_prices: {latest_date}")
            else:
                logging.info(f"No rows found in {schema}.fact_prices. Initial load required.")
            return latest_date
    except Exception as e:
        logging.warning(f"Could not determine latest loaded date. Falling back to full available dataset: {e}")
        return None


def filter_existing_dimension_rows(df, table_name):
    """Keep only dimension rows that do not already exist in PostgreSQL."""
    engine = get_engine()
    schema = DB_CONFIG.get('schema', 'food_etl')
    key_columns = {
        'dim_commodity': ['commodity_name', 'unit'],
        'dim_market': ['market_name', 'region', 'county'],
        'dim_date': ['date_key'],
    }

    natural_keys = key_columns.get(table_name)
    if natural_keys is None or df.empty:
        return df.copy()

    query = text(f"SELECT {', '.join(natural_keys)} FROM {schema}.{table_name}")

    try:
        with engine.connect() as conn:
            existing_df = pd.read_sql_query(query, conn)
    except Exception as e:
        logging.warning(f"Could not read existing rows from {schema}.{table_name}. Loading provided rows as-is: {e}")
        return df.copy()

    if existing_df.empty:
        return df.copy()

    existing_keys = {
        tuple("" if pd.isna(value) else str(value) for value in row)
        for row in existing_df[natural_keys].itertuples(index=False, name=None)
    }

    incoming_keys = [
        tuple("" if pd.isna(value) else str(value) for value in row)
        for row in df[natural_keys].itertuples(index=False, name=None)
    ]
    mask = [key not in existing_keys for key in incoming_keys]
    filtered_df = df.loc[mask].copy()

    logging.info(
        f"Filtered {len(df) - len(filtered_df)} existing rows from {schema}.{table_name}; "
        f"{len(filtered_df)} new rows remain."
    )
    return filtered_df

def clear_tables():
    """
    Cleans the Star Schema before a fresh reload.
    Uses RESTART IDENTITY to reset SERIAL primary keys to 1.
    """
    engine = get_engine()
    truncate_query = """
    TRUNCATE TABLE food_etl.fact_prices RESTART IDENTITY CASCADE;
    TRUNCATE TABLE food_etl.dim_commodity RESTART IDENTITY CASCADE;
    TRUNCATE TABLE food_etl.dim_market RESTART IDENTITY CASCADE; 
    TRUNCATE TABLE food_etl.dim_date RESTART IDENTITY CASCADE;   
    """
    try:
        with engine.begin() as conn:  # engine.begin() automatically handles commits/rollbacks
            conn.execute(text(truncate_query))
            logging.info("Database tables in 'food_etl' truncated successfully (CASCADE). 🧹")
    except Exception as e:
        logging.error(f"Failed to truncate tables ❌ : {e}")
        raise
    
def load_to_postgres(df, table_name):
    """
    Main loading function used by the DAG.
    Handles schema mapping, chunking (f405 fix), and column filtering.
    """
    engine = get_engine()
    schema = DB_CONFIG.get('schema', 'food_etl')

    table_columns = {
        'dim_commodity': ['commodity_name', 'category', 'unit'],
        'dim_market': ['market_name', 'region', 'county'],
        'dim_date': ['date_key', 'year', 'month', 'day', 'month_name'],
        'fact_prices': [
            'date_key',
            'commodity_name',
            'market_name',
            'price_value',
            'unit_weight',
            'price_per_kg',
        ],
    }

    target_columns = table_columns.get(table_name)
    if target_columns is None:
        raise ValueError(f"Unsupported target table: {table_name}")

    # Keep only the columns that belong in the destination table.
    cols_to_use = [col for col in target_columns if col in df.columns]
    df_to_load = df[cols_to_use].copy()

    if df_to_load.empty:
        logging.info(f"No rows to load for {schema}.{table_name}. Skipping.")
        return

    try:
        logging.info(f"🚀 Starting load of {len(df_to_load)} rows to {schema}.{table_name}...")
        
        # 2. Perform the load with chunking to stay under the Postgres parameter limit
        df_to_load.to_sql(
            name=table_name, 
            con=engine, 
            schema=schema, 
            if_exists='append', 
            index=False, 
            method='multi',
        )
        
        logging.info(f"✅ Loaded {table_name} successfully into {schema}. 🎉")
        
    except Exception as e:
        logging.error(f"Load failed for {table_name} 🌋: {e}")
        raise

if __name__ == "__main__":
    # Optional: Quick local test block
    logging.info("Load script initialized in standalone mode.")
