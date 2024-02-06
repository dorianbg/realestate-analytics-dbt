{{ config(materialized='view', schema='rentals_flats', alias='vw_last_ad_time') }}

{{ last_ad_time_macro('staging', 'rentals_flats') }}