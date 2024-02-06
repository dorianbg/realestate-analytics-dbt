{{ config(materialized='table', schema='common', alias='ad_price_changes_per_week') }}

with t1 as (
    select
        apha.*,
        case
            when price_old > price_new then 'price_drop'
            when price_old < price_new then 'price_rise'
            else 'same_price'
            end as price_change_type,
            (price_new/dets.size - price_old/dets.size) / (price_old/dets.size) as price_change
    from (
        select
            ltrim(price_new,'€')::int as price_new,
            ltrim(price_old,'€')::int as price_old,
            type,
            change_date,
            ad_id,
            price_change_reason
        from {{ ref('ad_price_history') }}
    ) apha
    left join (
        select ad_id, size from {{ ref('4mv_enriched_ad_sales_flats') }} union all
        select ad_id, size from {{ ref('4mv_enriched_ad_rentals_flats') }} union all
        select ad_id, size from {{ ref('4mv_enriched_ad_sales_houses') }}
    ) dets on apha.ad_id = dets.ad_id
    where price_old > 100 and price_new > 100 and price_change_reason = 'px_chg'
)
select
    count(distinct ad_id),
    round(avg(price_change)::numeric,2) as avg_price_change,
    type,
    price_change_type,
    date_trunc('week', change_date::date)::date as change_week
from t1
where price_change < 1
group by
    type,
    price_change_type,
    date_trunc('week', change_date::date)
order by change_week desc