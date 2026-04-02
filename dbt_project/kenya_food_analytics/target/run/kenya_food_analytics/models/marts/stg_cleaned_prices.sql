
  create view "food_prices_prod_v1"."food_etl"."stg_cleaned_prices__dbt_tmp"
    
    
  as (
    

SELECT 
    -- 1. Date Key (Ensuring it's a proper date type)
    CAST(date_key AS DATE) as date_key,

    -- 2. Identifiers (Using names from your manual SQL)
    commodity_name,
    market_name,

    -- 3. Cleaning: Handling numeric conversion and nulls (Requirement #8)
    -- This handles commas in prices and converts to NUMERIC for math
    CAST(REPLACE(CAST(price_value AS TEXT), ',', '') AS NUMERIC) as price_numeric,
    
    -- 4. Normalization (Requirement #1.o)
    COALESCE(unit_weight, 1.0) as unit_weight,
    
    -- 5. Derived Column
    ROUND(CAST(price_value AS NUMERIC) / COALESCE(unit_weight, 1.0), 2) as price_per_kg,

    -- 6. Metadata (Requirement #7)
    created_at as load_timestamp
FROM "food_prices_prod_v1"."food_etl"."fact_prices"
WHERE price_value IS NOT NULL
  );