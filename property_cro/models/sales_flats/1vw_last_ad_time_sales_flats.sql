{{ config(materialized='view', schema='sales_flats', alias='vw_last_ad_time') }}

{{ last_ad_time_macro('staging', 'sales_flats') }}