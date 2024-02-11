{{ config(materialized='view', schema='rentals_flats', alias='vw_avg_ask_px_per_loc_year_size') }}

{{ avg_px_per_loc_and_size_and_age_macro('2mv_price_history_rentals_flats', 'staging', 'rental_flat_desc', '360 DAY', '150') }}
