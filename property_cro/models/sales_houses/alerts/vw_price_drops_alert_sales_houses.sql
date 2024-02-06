{{ config(materialized='view', schema='sales_houses_alerts', alias='vw_price_drops') }}

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
    m1.building_type,
    m1.house_type,
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
from {{ ref('4mv_enriched_ad_sales_houses')}} as m1
where price <= 200000
  and price_m2 <= 1750
  and price_m2 > 300
  and size < 200
  and current_price between 0.4 * first_price and 0.99 * first_price
  and (year_built is null or year_built > 2003)
  and (year_last_renovated is null or year_last_renovated > 2000)
--     and number_of_floors not in ('Prizemnica','Visoka prizemnica')
--     and total_number_of_floors_in_building not in ('Prizemnica','Visoka prizemnica')
  and coalesce(furbishment_state,'') not in ('Za renoviranje','Roh-bau')
  and lower(description) not like '%roh bau%'
  and lower(description) not like '%roh-bau%'
  and location2 not in ('Grad Zagreb, Peščenica - Žitnjak, Kozari put')
  and m1.price > 1000
  and m1.first_seen between {{ dbt_utils.current_timestamp() }} - INTERVAL '120 DAY' and {{ dbt_utils.current_timestamp() }}
  and m1.status = 'active'
--     and (
--         (lower(advertiser) not like '%nekretnin%' and lower(advertiser) not like '%agencij%'
--              and lower(advertiser) not like '%real estate%' and lower(advertiser) not like '%d.o.o.%'
--              and lower(advertiser) not like '%d.d.%'
--           and lower(advertiser) not in ('opereta', 'eurovilla d.o.o.', 'aps', 'b2 kapital d.o.o.', 'stan grad immobilien'))
--         or lower(title) like '%bez provizije%'
--         or lower(description) like '%bez provizije%'
--     )
order by m1.first_seen desc