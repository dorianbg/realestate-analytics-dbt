{{ config(materialized='table', schema='rentals_flats_analysis', alias='mv_inventory_general_location_rentals_flats') }}

{{ inventory_per_general_location_macro('4mv_enriched_ad_rentals_flats', 'staging', 'rentals_flats', '2mv_price_history_rentals_flats') }}
