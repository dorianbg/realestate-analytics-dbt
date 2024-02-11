{{ config(materialized='table', schema='rentals_flats', alias='mv_price_history') }}

{{ price_history_macro('staging', 'rentals_flats', '1vw_last_ad_time_rentals_flats', '200', '10000', '2.5', '50') }}
