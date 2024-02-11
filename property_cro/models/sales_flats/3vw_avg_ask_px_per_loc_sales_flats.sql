{{ config(materialized='view', schema='sales_flats', alias='vw_avg_ask_px_per_loc') }}

{{ avg_px_per_loc_macro('2mv_price_history_sales_flats', '270 DAY', '30000') }}
