{{ config(materialized='table', schema='sales_houses', alias='mv_enriched_ad') }}

select
    {{ enriched_ads_base_col_select_macro('0') }}
    sd.building_type,
    sd.house_type,
    sd.total_number_of_floors_in_building
from {{ ref('2vw_price_history_sales_houses')}} s
join {{ ref('3vw_avg_ask_px_per_loc_sales_houses')}}  a on s.location = a.location
left join {{ source('staging', 'sale_house_desc') }} sd on s.ad_id = sd.ad_id
left join {{ ref('3vw_avg_ask_px_per_loc_size_sales_houses')}} a2 on s.location = a2.location and FLOOR(s.size / 20) * 20 = a2.size
left join {{ ref('3vw_avg_ask_px_per_loc_year_size_sales_houses')}} a3 on s.location = a3.location and FLOOR(s.size / 20) * 20 = a3.size and FLOOR(sd.year_built / 20) * 20 = a3.year_range