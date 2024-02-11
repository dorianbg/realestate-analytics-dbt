{{ config(materialized='view', schema='sales_houses', alias='vw_avg_ask_px_per_loc_year_size') }}

{{ avg_px_per_loc_and_size_and_age_macro('2mv_price_history_sales_houses', 'staging', 'sale_house_desc', '270 DAY', '30000') }}