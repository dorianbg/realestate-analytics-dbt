{{ config(materialized='table', schema='sales_flats_analysis', alias='mv_inventory_general_location_sales_flats') }}

{{ inventory_per_general_location_macro('4mv_enriched_ad_sales_flats', 'staging', 'sales_flats', '2mv_price_history_sales_flats') }}