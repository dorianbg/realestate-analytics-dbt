{{ config(materialized='view', schema='sales_flats_alerts', alias='vw_best_streets_zg') }}

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
(
    -- središće
    lower(description) like '%brune bušića%' or
    lower(title) like '%brune bušića%' or
    lower(description) like '%knifer%' or
    lower(title) like '%knifer%' or
    lower(description) like '%kantoci%' or
    lower(title) like '%kantoci%' or
    lower(description) like '%bakić%' or
    lower(title) like '%bakić%' or
        -- savica
    lower(description) like '%lastovska%' or
    lower(title) like '%lastovska%' or
    lower(description) like '%zinke kunc%' or
    lower(title) like '%zinke kunc%' or
    lower(description) like '%zinke kunc%' or
    lower(title) like '%zinke kunc%' or
    -- borovje
    lower(description) like '%grada chicaga%' or
    lower(title) like '%grada chicaga%' or
    lower(description) like '%miševečka%' or
    lower(title) like '%miševečka%' or
    -- sigečica
    lower(description) like '%rapska%' or
    lower(title) like '%rapska%' or
    lower(description) like '%kornatska%' or
    lower(title) like '%kornatska%' or
    lower(description) like '%folnegovića%' or
    lower(title) like '%folnegovića%' or
    -- cvjetno naselje
    lower(description) like '%cvjetna cesta%' or
    lower(title) like '%cvjetna cesta%' or
    -- maksimir
    lower(description) like '%kraljevićeva%' or
    lower(title) like '%kraljevićeva%' or
    lower(description) like '%božidarevićeva%' or
    lower(title) like '%božidarevićeva%' or
    lower(description) like '%čikoševa%' or
    lower(title) like '%čikoševa%' or
    lower(description) like '%crnčićeva%' or
    lower(title) like '%crnčićeva%' or
    lower(description) like '%klovićeva%' or
    lower(title) like '%klovićeva%' or
    lower(description) like '%bužanova%' or
    lower(title) like '%bužanova%' or
    lower(description) like '%harambašićeva%' or
    lower(title) like '%harambašićeva%' or
    lower(description) like '%ružmarink%' or
    lower(title) like '%ružmarink%' or
    lower(description) like '%čerinina%' or
    lower(title) like '%čerinina%' or
    -- centar
    lower(description) like '%jurkovićev%' or
    lower(title) like '%jurkovićev%' or
    lower(description) like '%centar zvonimir%' or
    lower(title) like '%centar zvonimir%' or
    lower(description) like '%banjavčićev%' or
    lower(title) like '%banjavčićev%' or
    lower(description) like '%ljudevita posavskog%' or
    lower(title) like '%ljudevita posavskog%' or
    lower(description) like '%vrbanićeva%' or
    lower(title) like '%vrbanićeva%' or
    -- trnje
    lower(description) like '%pile%' or
    lower(title) like '%pile%' or
    lower(description) like '%štrigina%' or
    lower(title) like '%štrigina%' or
    lower(description) like '%supilova%' or
    lower(title) like '%supilova%' or
    lower(description) like '%strojarska%' or
    lower(title) like '%strojarska%' or
    -- crnomerec
    lower(description) like '%bleiweisova%' or
    lower(title) like '%bleiweisova%' or
    lower(description) like '%slovenska%' or
    lower(title) like '%slovenska%' or
    -- vrbani
    lower(description) like '%palinovečka%' or
    lower(title) like '%palinovečka%' or
    -- jarun
    lower(description) like '%hrgovići%' or
    lower(title) like '%hrgovići%' or
    lower(description) like '%bernarda vukasa%' or
    lower(title) like '%bernarda vukasa%' or
    lower(description) like '%stipančića%' or
    lower(title) like '%stipančića%' or
    lower(description) like '%macanovića%' or
    lower(title) like '%macanovića%' or
    lower(description) like '%ljubića vojvode%' or
    lower(title) like '%ljubića vojvode%' or
    -- jarun
    lower(location) like '%jarun%' or
    lower(location) like '%gredice%' or
    -- ravnice
    lower(description) like '%radauševa%' or
    lower(title) like '%radauševa%' or
    lower(description) like '%augustinčića%' or
    lower(title) like '%augustinčića%' or
    lower(description) like '%šeferova%' or
    lower(title) like '%šeferova%'
)
and (year_built > 1970 or year_built is null or description not like '%starij%')
and status = 'active'
and (floor is null or total_number_of_floors_in_building is null or floor_desc != total_number_of_floors_in_building::text)
and number_of_floors = 'Jednoetažni'
and floor like '%kat'
and flat_type = 'Stan u stambenoj zgradi'
and (furbishment_state is null or furbishment_state!= 'Za renoviranje')
and size > 30
and price between 60000 and 190000
and price_m2 <= 3500
order by first_seen desc