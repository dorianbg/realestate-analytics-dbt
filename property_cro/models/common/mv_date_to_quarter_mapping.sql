{{ config(materialized='table', schema='common', alias='mv_date_to_quarter_mapping') }}


{{ date_to_quarter_mapping_macro() }}

-- maps date to relevant quarter, eg.
-- date_, quarter
-- 2021-04-09,2021-05-15
-- 2021-04-22,2021-05-15
-- 2021-04-30,2021-05-15
-- 2021-05-12,2021-05-15
-- 2021-06-08,2021-08-14
-- 2021-06-10,2021-08-14
-- 2021-06-19,2021-08-14
-- 2021-07-21,2021-08-14
-- 2021-07-30,2021-08-14