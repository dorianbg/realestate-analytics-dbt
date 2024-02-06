{{ config(materialized='view', schema='sales_flats', alias='vw_price_history') }}

{{ price_history_macro('staging', 'sales_flats', '1vw_last_ad_time_sales_flats', '1000000') }}
