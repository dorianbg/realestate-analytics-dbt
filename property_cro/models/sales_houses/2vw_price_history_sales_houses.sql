{{ config(materialized='view', schema='sales_houses', alias='vw_price_history') }}

{{ price_history_macro('staging', 'sales_houses', '1vw_last_ad_time_sales_houses', '1000000') }}
    