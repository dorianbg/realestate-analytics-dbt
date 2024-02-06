{{ config(materialized='view', schema='rentals_flats', alias='vw_avg_ask_px_per_loc_size') }}

{{ avg_px_per_loc_and_size_macro('2vw_price_history_rentals_flats', '360 DAY', '150') }}
