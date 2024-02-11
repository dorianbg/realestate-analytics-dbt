{% macro inventory_per_general_location_macro(mat_view, source_schema, source_table, price_history_view) %}

with zg_loc as (
    select distinct location from {{ ref(mat_view) }}
    where location2 like 'Grad Zagreb,%' or location2 like 'Zagrebačka,%'
),
new_ads as (
    select quarter,
           substring(s.location FROM 1 FOR position(',' IN s.location) - 1) as location,
           count(distinct ad_id) filter (where price_m2 < 10000) as new_ads_qty,
           round((avg(days_on_the_market) filter (where price_m2 < 10000))::numeric,1) as new_ads_dom,
           round((avg(price_m2) filter (where price_m2 < 10000))::numeric,1) as new_ads_avg_px_m2,
           round((avg(price) filter (where price_m2 < 10000))::numeric,1) as new_ads_avg_px,
           round((avg(size) filter (where price_m2 < 10000))::numeric,1) as new_ads_avg_size
    from {{ ref(mat_view) }} s
    join {{ref ('mv_date_to_quarter_mapping') }} d on first_seen = date_
    where location in (select location from zg_loc)
    group by d.quarter, substring(s.location FROM 1 FOR position(',' IN s.location) - 1)
), sold_ads as (
    select
        quarter,
        substring(s.location FROM 1 FOR position(',' IN s.location) - 1) as location,
        count(distinct ad_id) filter (where price_m2 < 10000) as sold_ads_qty,
        round((avg(days_on_the_market) filter (where price_m2 < 10000))::numeric,1) as sold_ads_dom,
        round((avg(price_m2) filter (where price_m2 < 10000))::numeric,1) as sold_ads_avg_px_m2,
        round((avg(price) filter (where price_m2 < 10000))::numeric,1) as sold_ads_avg_px,
        round((avg(size) filter (where price_m2 < 10000))::numeric,1) as sold_ads_avg_size
    from {{ ref(mat_view) }} s
    join {{ref ('mv_date_to_quarter_mapping') }} d
        on s.first_seen = d.date_
    where status = 'inactive'
        and location in (select location from zg_loc)
    group by d.quarter, substring(s.location FROM 1 FOR position(',' IN s.location) - 1)
), actives as (
    select
        d.quarter,
        substring(s.location FROM 1 FOR position(',' IN s.location) - 1) as location,
        count(distinct s.ad_id) filter (where s.price < 10000000) as active_ads_qty,
        round((avg(days_on_the_market) filter (where s.price < 10000000))::numeric,1) as active_ads_dom,
        round((avg(s.price/s.size) filter (where s.price < 10000000))::numeric,1) as active_ads_avg_px_m2,
        round((avg(s.price) filter (where s.price < 10000000))::numeric,1) as active_ads_avg_px,
        round((avg(s.size) filter (where s.price < 10000000))::numeric,1) as active_ads_avg_size
    from {{ source(source_schema, source_table) }} s
    join {{ ref(price_history_view) }} as hist2
        on s.ad_id = hist2.ad_id
    join {{ref ('mv_date_to_quarter_mapping') }} d
        on seen_date = date_
    where s.location in (select location from zg_loc)
    group by quarter, substring(s.location FROM 1 FOR position(',' IN s.location) - 1)
)
select
    a.quarter as year_month,
    a.location,
    new_ads_qty,
    new_ads_dom,
    new_ads_avg_px_m2,
    new_ads_avg_px,
    new_ads_avg_size,
    sold_ads_qty,
    sold_ads_dom,
    sold_ads_avg_px_m2,
    sold_ads_avg_px,
    sold_ads_avg_size,
    active_ads_qty,
    active_ads_dom,
    active_ads_avg_px_m2,
    active_ads_avg_px,
    active_ads_avg_size
from new_ads n
join sold_ads r on n.quarter = r.quarter and n.location = r.location
join actives a on n.quarter = a.quarter and n.location = a.location
order by n.quarter desc
{% endmacro %}