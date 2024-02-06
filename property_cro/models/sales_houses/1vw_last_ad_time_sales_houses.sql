{{ config(materialized='view', schema='sales_houses', alias='vw_last_ad_time') }}

{{ last_ad_time_macro('staging', 'sales_houses') }}