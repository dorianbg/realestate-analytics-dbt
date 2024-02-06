{{ config(materialized='view', schema='sales_flats_alerts', alias='vw_modern_central_flats') }}

select location,
       title,
       link,
       flat_type,
       floor,
       size,
       price,
       first_price,
       current_price,
       num_seen,
       price_m2,
       area_price_m2,
       area_discount,
       area_sized_price_m2,
       area_aged_sized_price_m2,
       price_drop_pecentage,
       first_seen,
       last_seen,
       ad_id,
       latest_seen_date,
       status,
       days_on_the_market,
       price_3,
       price_7,
       price_15,
       price_30,
       price_45,
       price_60,
       url,
       location2,
       street,
       num_views,
       apartment_type,
       number_of_floors,
       year_built,
       year_last_renovated,
       total_liveable_area,
       net_area,
       number_of_rooms,
       number_of_parking_spots,
       balcony_terrace,
       total_number_of_floors_in_building,
       furbishment_state,
       epc_rating,
       table_data,
       description,
       extra_description,
       seen_time,
       advertiser
from {{ ref('4mv_enriched_ad_sales_flats') }}
where
  location in (select location from {{ ref('zagreb_locations') }}) and
     (
        (year_built > 1970 )
        or ((lower(title) like '%novogradnj%' or lower(title) like '%novij%'
            or lower(description) like '%novogradnj%' or lower(description) like '%novij%')
            AND year_built is null
        )
     )
--     and number_of_rooms >= 1
--      and (total_number_of_floors_in_building > 3 or total_number_of_floors_in_building is null)
--       and ((number_of_rooms = 3 and price <= 185000 and price_m2 < area_price_m2 and year_built > 1980)
--                or (number_of_rooms = 2 and price <= 140000 and price_m2 < area_price_m2 and year_built > 1965))
    and status = 'active'
    and number_of_rooms > 0
    and price >= 55000
    and price <= 250000
--     and (
--             (price <= 180000 and number_of_rooms = 3)
--             or (price <= 150000 and number_of_rooms = 2)
--     )
    and price_m2 >= 1200
    and price_m2 <= 3700
    and price_m2 < 1.5 * area_aged_sized_price_m2
    and (number_of_floors = 'Jednoetažni' or number_of_floors is null)
    and (total_number_of_floors_in_building is null
             or floor_desc is null
            or (floor_desc != total_number_of_floors_in_building::text and floor_desc is not null)
        )
    and (floor is null or floor like '%kat%')
    and size > 25
    and (flat_type is null or flat_type not like 'Stan u kući')
    order by first_seen desc