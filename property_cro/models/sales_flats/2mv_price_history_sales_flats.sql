{{ config(materialized='table', schema='sales_flats', alias='mv_price_history') }}

{{ price_history_macro('staging', 'sales_flats', '1vw_last_ad_time_sales_flats', '12000', '1000000', '300', '10000') }}
