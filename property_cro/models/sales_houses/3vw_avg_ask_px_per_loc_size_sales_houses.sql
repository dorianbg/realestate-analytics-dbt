{{ config(materialized='view', schema='sales_houses', alias='vw_avg_ask_px_per_loc_size') }}

{{ avg_px_per_loc_and_size_macro('2vw_price_history_sales_houses', '270 DAY', '30000') }}
