{{ config(materialized='table', schema='common', alias='ad_price_history') }}

with cte as (
    select
        seen_date, ad_id, price, type,
        row_number() over (partition by ad_id order by seen_date) as row_number
    from (
        select seen_date, ad_id, price, 'sales_flats' as type from {{ source('staging', 'sales_flats') }} union all
        select seen_date, ad_id, price, 'sales_houses' as type from {{ source('staging', 'sales_houses') }} union all
        select seen_date, ad_id, price, 'rentals_flats' as type from {{ source('staging', 'rentals_flats') }}
    ) a
), cte2 as (
    select b1.seen_date as sd_old, b1.ad_id as ad_id_old, b1.price as price_old, b1.row_number as rn_old,
           b2.seen_date as sd_new, b2.ad_id as ad_id_new, b2.price as price_new, b2.row_number as rn_new, b1.type
    from cte as b1
    left join cte as b2
        on b1.ad_id = b2.ad_id and b1.row_number = b2.row_number - 1
)
select
    ad_id_old as ad_id, type,
    case when rn_old = 1 then 'first_seen'
        when sd_new is null then 'last_seen'
        when price_new <= 0.99 * price_old or price_new >= 1.01 * price_old then 'px_chg'
    end as price_change_reason,
    case when rn_old = 1 then sd_old
        when sd_new is null then sd_old
        when price_new <= 0.99 * price_old or price_new >= 1.01 * price_old then sd_new
    end as change_date,
    case when rn_old = 1 then 'first'
        when sd_new is null then ('€' || round(price_old::numeric,0)::varchar)
        when price_new <= 0.99 * price_old or price_new >= 1.01 * price_old then ('€' || round(price_old::numeric, 0)::varchar)
    end as price_old,
    case when rn_old = 1 then ('€' || round(price_old::numeric,0)::varchar)
        when sd_new is null then 'current'
        when price_new <= 0.99 * price_old or price_new >= 1.01 * price_old then ('€' || round(price_new::numeric, 0)::varchar)
    end as price_new
from cte2
where rn_old = 1 -- first price
    or sd_new is null -- last price -- decided to remove this for the time being
    or price_new <= 0.99 * price_old or price_new >= 1.01 * price_old 