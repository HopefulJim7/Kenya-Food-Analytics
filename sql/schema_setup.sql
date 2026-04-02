CREATE SCHEMA IF NOT EXISTS food_etl;

--Dimension: Commodity (The "What")
CREATE TABLE food_etl.dim_commodity (
    commodity_id SERIAL PRIMARY KEY,
	commodity_name VARCHAR(100) NOT NULL,
	category VARCHAR(50),
	unit VARCHAR(20),
	UNIQUE(commodity_name, unit) -- This helps to prevent duplicate oentires for the same item
);

-- 2. Dimension: Market ( The "Where")
CREATE TABLE food_etl.dim_market (
market_id SERIAL PRIMARY KEY,
market_name VARCHAR(100) NOT NULL,
region VARCHAR(100), -- Maps to admin1
county VARCHAR(100), -- Maps to admin2
UNIQUE(market_name, region, county)
);

-- 3. Dimension: Date (The "When")
CREATE TABLE food_etl.dim_date (
     date_key DATE PRIMARY KEY,
	 year INT,
	 month INT,
	 day INT,
	 month_name VARCHAR(20)
);

-- 4. FACT TABLE: PRICES (The " How Much")
CREATE TABLE food_etl.fact_prices (
    fact_id SERIAL PRIMARY KEY,
	date_key DATE REFERENCES food_etl.dim_date(date_key),
	commodity_name VARCHAR(100), -- names used to load, dbt -> link IDS later
	market_name VARCHAR(100),
	price_value NUMERIC(12, 2),
	unit_weight NUMERIC(10, 3),
	price_per_kg NUMERIC(12, 2),
	created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

TRUNCATE TABLE food_etl.fact_prices CASCADE;

-- Verification if Data loade to postgres - verify in pgAdmin
SELECT 
    d.year,
    d.month_name,
    m.county,
    m.market_name,
    c.commodity_name,
    f.price_value,
    f.price_per_kg
FROM food_etl.fact_prices f
JOIN food_etl.dim_date d ON f.date_key = d.date_key
JOIN food_etl.dim_market m ON f.market_name = m.market_name
JOIN food_etl.dim_commodity c ON f.commodity_name = c.commodity_name
ORDER BY d.date_key DESC
LIMIT 20;