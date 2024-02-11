{% macro price_history_macro(source_schema, source_table, ref_name, source_min_price, source_max_price, source_min_price_m2, source_max_price_m2) %}

with t as (
    select
		las.ad_id,
        t.last_seen,
        t.first_seen,
        price,
        seen_date,
        t.days_on_the_market,
        t.status,
        first_value(price) over (partition by las.ad_id order by seen_date desc range between UNBOUNDED preceding and unbounded following) as current_price,
		last_value(price) over (partition by las.ad_id order by seen_date desc range between UNBOUNDED preceding and unbounded following) as first_price
	from {{ source(source_schema, source_table) }} las
    join {{ ref(ref_name) }} as t
        on las.ad_id = t.ad_id
    where las.price between {{source_min_price}} and {{source_max_price}}
),
ad_price_history as (
    select
        ad_id,
        last_seen,
        first_seen,
        current_price,
        first_price,
        days_on_the_market,
        status,
        min(price) filter ( where seen_date <= t.last_seen - interval '60 day') as price_60,
        min(price) filter ( where seen_date between t.last_seen - interval '60 day' and t.last_seen - interval '45 day') as price_45,
        min(price) filter ( where seen_date between t.last_seen - interval '45 day' and t.last_seen - interval '30 day') as price_30,
        min(price) filter ( where seen_date between t.last_seen - interval '30 day' and t.last_seen - interval '15 day') as price_15,
        min(price) filter ( where seen_date between t.last_seen - interval '15 day' and t.last_seen - interval '7 day') as price_7,
        min(price) filter ( where seen_date between t.last_seen - interval '7 day' and t.last_seen - interval '3 day') as price_3,
        count(ad_id) as num_seen,
        max(seen_date) as latest_seen_date
    from t
    group by ad_id, last_seen, first_seen, current_price, first_price, days_on_the_market, status
), last_ad as (
    select
        ad.ad_id,
        ad.seen_date,
        ad.location,
        ad.title,
        ad.link,
        ad.flat_type,
        ad.floor,
        ad.size,
        ad.price
    from (select row_number() over (partition by ad_id order by seen_time desc) as rown,
                 *
          from {{ source(source_schema, source_table) }}) ad
    where rown = 1
)
select
    ad.location,
    ad.title,
    ad.link,
    ad.flat_type,
    ad.floor,
    ad.size,
    ad.price,
    aph.ad_id,
    aph.last_seen,
    aph.first_seen,
    aph.current_price,
    aph.first_price,
    aph.price_60,
    aph.price_45,
    aph.price_30,
    aph.price_15,
    aph.price_7,
    aph.price_3,
    aph.num_seen,
    aph.latest_seen_date,
    aph.days_on_the_market,
    aph.status
from ad_price_history aph
join last_ad ad
	on aph.ad_id = ad.ad_id
	and aph.last_seen = ad.seen_date
where (ad.price/ad.size) between {{source_min_price_m2}} and {{source_max_price_m2}}
{% endmacro %}
