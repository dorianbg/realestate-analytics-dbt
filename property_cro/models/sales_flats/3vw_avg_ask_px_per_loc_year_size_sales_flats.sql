{{ config(materialized='view', schema='sales_flats', alias='vw_avg_ask_px_per_loc_year_size') }}

{{ avg_px_per_loc_and_size_and_age_macro('2mv_price_history_sales_flats', 'staging', 'sale_flat_desc', '270 DAY', '30000') }}
