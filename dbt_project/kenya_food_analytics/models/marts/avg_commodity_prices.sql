-- Average Price per Commodity per County


{{ config(materialized='table') }}

SELECT 
    c.commodity_name,
    c.category,
    m.county,
    m.market_name,
    d.month_name,
    d.year,
    -- Requirement #9: Calculate Average Price
    ROUND(AVG(p.price_per_kg), 2) as average_price
FROM {{ source('food_etl', 'fact_prices') }} p
JOIN {{ source('food_etl', 'dim_commodity') }} c ON p.commodity_name = c.commodity_name
JOIN {{ source('food_etl', 'dim_market') }} m ON p.market_name = m.market_name
JOIN {{ source('food_etl', 'dim_date') }} d ON p.date_key = d.date_key
GROUP BY 1, 2, 3, 4, 5, 6
