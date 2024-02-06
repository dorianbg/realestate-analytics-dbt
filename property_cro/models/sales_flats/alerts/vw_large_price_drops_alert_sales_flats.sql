{{ config(materialized='view', schema='sales_flats_alerts', alias='vw_large_price_drops') }}

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
    floor like '%kat%'
    and size > 28
    and size < 99
    and current_price between 10000 and 210000
    and current_price between 0.4 * first_price and 0.99 * first_price
    and (
        (location2 is null and price_m2 < area_price_m2 * 1.45)
        or
        (location2 is not null and
            (
                (year_built > 2000 and price_m2 < area_price_m2 * 1.6)
                or
                (year_built < 2000 and price_m2 < area_price_m2 * 1.5)
            )
            and (year_built is null or year_built > 1970)
            and (floor is null or total_number_of_floors_in_building is null or floor_desc != total_number_of_floors_in_building::text)
            and (description is null or lower(description) not like '%potkrovlj%' )
        )
    )
    and (total_number_of_floors_in_building is null
            or (floor_desc != total_number_of_floors_in_building::text and floor_desc is not null)
    )
    and flat_type = 'Stan u stambenoj zgradi'
    and status = 'active'
order by first_seen desc