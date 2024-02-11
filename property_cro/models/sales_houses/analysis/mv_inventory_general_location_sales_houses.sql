{{ config(materialized='table', schema='sales_houses_analysis', alias='mv_inventory_general_location_sales_houses') }}

{{ inventory_per_general_location_macro('4mv_enriched_ad_sales_houses', 'staging', 'sales_houses', '2mv_price_history_sales_houses') }}
