{{ config(materialized='view', schema='rentals_flats', alias='vw_price_history') }}

{{ price_history_macro('staging', 'rentals_flats', '1vw_last_ad_time_rentals_flats', '10000') }}
