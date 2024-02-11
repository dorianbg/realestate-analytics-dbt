{{ config(materialized='view', schema='sales_flats_alerts', alias='vw_new_ads') }}

select
       m1.location,
       m1.title,
       m1.link,
       m1.flat_type,
       m1.floor,
       m1.size,
       m1.price,
       m1.first_price,
       m1.current_price,
       m1.num_seen,
       m1.price_m2,
       m1.area_price_m2,
       m1.area_discount,
       m1.area_sized_price_m2,
       m1.area_sized_discount,
       m1.area_aged_sized_price_m2,
       m1.area_aged_sized_discount,
       m1.price_drop_pecentage,
       m1.first_seen,
       m1.last_seen,
       m1.ad_id,
       m1.latest_seen_date,
       m1.status,
       m1.days_on_the_market,
       m1.price_7,
       m1.price_15,
       m1.price_30,
       m1.price_45,
       m1.price_60,
       m1.url,
       m1.location2,
       m1.street,
       m1.num_views,
       m1.apartment_type,
       m1.number_of_floors,
       m1.year_built,
       m1.year_last_renovated,
       m1.total_liveable_area,
       m1.net_area,
       m1.number_of_rooms,
       m1.number_of_parking_spots,
       m1.balcony_terrace,
       m1.total_number_of_floors_in_building,
       m1.furbishment_state,
       m1.epc_rating,
       m1.table_data,
       m1.description,
       m1.extra_description,
       m1.seen_time,
       m1.advertiser
from {{ ref('4mv_enriched_ad_sales_flats') }} as m1
where
    m1.location in (select location from {{ ref('zagreb_locations') }})
    and m1.floor like '%kat%'
    and (
        (m1.price between 100000 and 250000 and (m1.year_built is null or m1.year_built >= 1971))
    )
    and m1.price > 1000
    and m1.first_seen between {{ dbt.current_timestamp() }}::date - INTERVAL '7 DAY' and {{ dbt.current_timestamp() }}::date
    and m1.status = 'active'
order by m1.first_seen desc