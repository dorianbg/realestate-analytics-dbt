{{ config(materialized='table', schema='rentals_flats_analysis', alias='mv_inventory_rentals_flats') }}

{{ inventory_macro('4mv_enriched_ad_rentals_flats', 'staging', 'rentals_flats', '2vw_price_history_rentals_flats') }}
