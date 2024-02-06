{{ config(materialized='table', schema='sales_flats_analysis', alias='mv_inventory_sales_flats') }}

{{ inventory_macro('4mv_enriched_ad_sales_flats', 'staging', 'sales_flats', '2vw_price_history_sales_flats') }}