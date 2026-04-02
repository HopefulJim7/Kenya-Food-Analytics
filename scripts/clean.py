import pandas as pd
import numpy as np
import logging
import re
import os

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

def get_unit_weight(unit_str):
    """ 
    Normalize weights to KG/L (i.e 90kg -> 90.0, '400 g' -> 0.4)
    Ensures return is always a float to prevent SQL type errors.
    """
    unit_str = str(unit_str).upper()
    match = re.search(r'(\d+\.?\d*)', unit_str)
    num = float(match.group(1)) if match else 1.0
    
    if 'KG' in unit_str or unit_str == 'L':
        return float(num)
    elif ' G' in unit_str or 'ML ' in unit_str:
        return float(num / 1000.0)
    return float(num)

def clean_data(df):
    """
    Cleans the Kenya Food Price data and strictly flattens types for SQL.
    Prepares data specifically for chunked bulk insertion into Postgres.
    """
    try: 
        logging.info(f"Starting cleaning process for {len(df)} rows.")
        
        # 1. Date Engineering
        df['date'] = pd.to_datetime(df['date'])
        df['year'] = df['date'].dt.year.astype(int)
        df['month'] = df['date'].dt.month.astype(int)
        df['day'] = df['date'].dt.day.astype(int)
        df['month_name'] = df['date'].dt.month_name().astype(str)
        df['date_key'] = df['date'].dt.strftime('%Y-%m-%d')
        
        # 2. Imputation (Handling missing prices)
        # Added .numeric_only=True to ensure we don't try to median strings
        df['price'] = df.groupby('commodity')['price'].transform(
            lambda x: x.fillna(x.median() if not x.dropna().empty else 0)
        )
        
        # 3. Normalization & Math
        df['unit_weight'] = df['unit'].apply(get_unit_weight).astype(float)
        
        # Force price to numeric and calculate price_per_kg
        df['price_value'] = pd.to_numeric(df['price'], errors='coerce').fillna(0).astype(float)
        df['price_per_kg'] = (df['price_value'] / df['unit_weight'].replace(0, 1)).astype(float)
        
        # 4. Renaming to match Star Schema
        rename_map = {
            'commodity': 'commodity_name',
            'market': 'market_name', 
            'admin1': 'region',
            'admin2': 'county'
        }
        df.rename(columns=rename_map, inplace=True)
        
        # 5. Final Column Selection & Scrubbing
        final_cols = [
            'date_key', 'year', 'month', 'day', 'month_name',
            'commodity_name', 'category', 'unit', 'unit_weight',
            'market_name', 'region', 'county', 
            'price_value', 'price_per_kg'
        ]
        
        existing_cols = [c for c in final_cols if c in df.columns]
        df_final = df[existing_cols].copy()
        
        
        for col in df_final.columns:
            if df_final[col].dtype == 'object':
                df_final[col] = df_final[col].astype(str).replace(['nan', 'None', 'NaN'], None)
            elif pd.api.types.is_float_dtype(df_final[col]):
                df_final[col] = df_final[col].fillna(0.0).astype(float)
            elif pd.api.types.is_integer_dtype(df_final[col]):
                df_final[col] = df_final[col].astype(int)

        logging.info("Cleaning successful. 🎉 Data types strictly flattened for SQL.")
        return df_final
        
    except Exception as e:
        logging.error(f"Clean failed: {e}")
        raise

if __name__ == "__main__":
    test_path = "data/wfp_food_prices_ken.csv"
    if os.path.exists(test_path):
        test_df = pd.read_csv(test_path)
        cleaned = clean_data(test_df)
        print("\n--- PREVIEW (First 5 Rows) ---")
        print(cleaned.head())
        print("\n--- DATA TYPES (SQL READY) ---")
        print(cleaned.dtypes)