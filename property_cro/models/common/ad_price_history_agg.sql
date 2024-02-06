{% if "postgres" in target.name %}

{{ config(materialized='table', schema='common', alias='ad_price_history_agg', post_hook=update_target_table_from_source('analytics_common.ad_price_history_agg', 'public.ad_price_history_agg_api')) }}

select
    apha.ad_id,
    apha.price_change_history,
    apha.created_at,
    apha.version,
    dets.price_m2 as price_sqm,
    dets.area_sized_price_m2 as area_sized_price_sqm,
    dets.year_built,
    case
        when (floor_desc is null or floor_desc like '') and (total_number_of_floors_in_building is null or total_number_of_floors_in_building like '')
            then ''
        when (floor_desc is not null and floor_desc <> '') and (total_number_of_floors_in_building is null or total_number_of_floors_in_building like '')
            then floor_desc
        when (floor_desc is null or floor_desc like '') and (total_number_of_floors_in_building is not null and total_number_of_floors_in_building <> '')
            then total_number_of_floors_in_building || ' katova'
        when (floor_desc is not null and floor_desc <> '') and (total_number_of_floors_in_building is not null and total_number_of_floors_in_building <> '')
            then floor_desc || ' / ' || total_number_of_floors_in_building
        end as floor,
    dets.advertiser,
    dets.days_on_the_market
from (
         select
             aph.ad_id,
             json_agg(change_date || '|' || price_old || '|' || price_new order by change_date) as price_change_history,
             current_timestamp(0) as created_at,
             1 as version
         from {{ ref('ad_price_history')}} aph
         where aph.price_new like '%€%'
         group by aph.ad_id
     ) apha
         left join (
    select ad_id, price_m2, area_sized_price_m2, year_built, total_number_of_floors_in_building::varchar, advertiser, coalesce(floor_desc, replace(floor, 'kat',''))::varchar as floor_desc, days_on_the_market  from {{ ref('4mv_enriched_ad_sales_flats') }}
    union all
    select ad_id, price_m2, area_sized_price_m2, year_built, total_number_of_floors_in_building::varchar, advertiser, coalesce(floor_desc, replace(floor, 'kat',''))::varchar as floor_desc, days_on_the_market from {{ ref('4mv_enriched_ad_rentals_flats') }}
    union all
    select ad_id, price_m2, area_sized_price_m2, year_built, total_number_of_floors_in_building::varchar, advertiser, coalesce(floor_desc, replace(floor, 'kat',''))::varchar as floor_desc, days_on_the_market from {{ ref('4mv_enriched_ad_sales_houses') }}
) dets on apha.ad_id = dets.ad_id

{%- else -%} select '1' as skipped_table

{% endif %}

