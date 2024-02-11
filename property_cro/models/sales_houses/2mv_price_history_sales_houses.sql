{{ config(materialized='table', schema='sales_houses', alias='mv_price_history') }}

{{ price_history_macro('staging', 'sales_houses', '1vw_last_ad_time_sales_houses', '10000', '1000000', '50', '10000') }}
    