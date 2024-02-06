{{ config(materialized='table', schema='rentals_flats', alias='mv_enriched_ad') }}

select
    {{ enriched_ads_base_col_select_macro('1') }}
    sd.apartment_type,
    case
        when RIGHT(sd.total_number_of_floors_in_building, 1) like '.'
            then SUBSTR(sd.total_number_of_floors_in_building, 1, LENGTH(sd.total_number_of_floors_in_building)-1)::integer
        when lower(sd.total_number_of_floors_in_building) like '%prizemlje%'
            then 1
        when sd.total_number_of_floors_in_building like '25+'
            then 25
        else sd.total_number_of_floors_in_building::integer
    end as total_number_of_floors_in_building
from {{ ref('2vw_price_history_rentals_flats') }} s
join {{ ref('3vw_avg_ask_px_per_loc_rentals_flats') }}  a on s.location = a.location
left join {{ source('staging', 'rental_flat_desc') }} sd on s.ad_id = sd.ad_id
left join {{ ref('3vw_avg_ask_px_per_loc_size_rentals_flats') }} a2 on s.location = a2.location and FLOOR(s.size / 20) * 20 = a2.size
left join {{ ref('3vw_avg_ask_px_per_loc_year_size_rentals_flats') }} a3 on s.location = a3.location and FLOOR(s.size / 20) * 20 = a3.size and FLOOR(sd.year_built / 20) * 20 = a3.year_range