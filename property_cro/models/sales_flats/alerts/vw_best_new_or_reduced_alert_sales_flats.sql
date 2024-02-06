{{ config(materialized='view', schema='sales_flats_alerts', alias='vw_best_new_or_reduced_flats') }}

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
    location in (select location from {{ ref('zagreb_locations') }}) and
    status = 'active' and
    (floor is null or total_number_of_floors_in_building is null or floor_desc != total_number_of_floors_in_building::text)
 	and (
        (floor is null or floor like '%kat%')
        or
        (floor is null or floor like '%kat%')
    )
	and (total_number_of_floors_in_building is null or total_number_of_floors_in_building > 2)
 	and (description is null or lower(description) not like '%potkrovlj%' )
	and
    (
	    (year_built > 1970 and year_built < 1980 and (
            price/net_area < 1.1 * area_price_m2
            or price/total_liveable_area < 1.1 * area_price_m2
        ))
        or
	    (year_built >= 1980 and year_built < 2001 and (
            price/net_area < 1.25 * area_price_m2
            or price/total_liveable_area < 1.25 * area_price_m2
        ))
	    or
	    (year_built >= 2001
	         and (
                price / net_area < 1.5 * area_price_m2
                or price / total_liveable_area < 1.5 * area_price_m2)
	    )
    )
	and (
		lower(title) like '%adaptiran%'
		or lower(title) like '%renoviran%'
		or lower(description) like '%adaptiran%'
        or lower(description) like '%renoviran%'
		or year_built > 2000
		or year_last_renovated > 2000
		or (year_built between 1984 and 2000 and price_m2 < area_price_m2 * 1.1)
		or (year_built between 1971 and 1983 and price_m2 < area_price_m2 * 1.25)
		or (year_built is null and year_last_renovated is null)
	)
    and (
        (lower(title) not like '%adaptacij%' and lower(title) not like '%renovacij%' and lower(title) not like '%mansardni%')
        or (lower(description) not like '%adaptacij%' and lower(description) not like '%renovacij%' and lower(description) not like '%mansardni%')
	)
	and flat_type = 'Stan u stambenoj zgradi'
    and price > 50000
    and price_m2 < 4000
    and ((price <= 210000 and year_built < 2005) or (year_built >= 2005 and price <= 300000))
    and (number_of_floors = 'Jednoetažni' or number_of_floors is null)
    and location not like 'Sesvete%'
    and location not like 'Podsljeme%'
    and location not like 'Novi Zagreb - Zapad, Lučko'
    order by first_seen desc