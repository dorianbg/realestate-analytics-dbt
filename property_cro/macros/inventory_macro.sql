{% macro inventory_macro(mat_view, source_schema, source_table, price_history_view) %}

{% if "duckdb" in target.name %}

with zg_loc as (
    select distinct location from {{ ref(mat_view) }}
    where location2 like 'Grad Zagreb,%' or location2 like 'Zagrebačka,%'
),
new_ads as (
    select
        cast(strftime(first_seen,'%Y%m') as integer) as year_month,
        count(distinct ad_id) filter (where price_m2 < 10000) as new_ads_qty,
        round((avg(days_on_the_market) filter (where price_m2 < 10000))::numeric,1) as new_ads_dom,
        round((avg(price_m2) filter (where price_m2 < 10000))::numeric,1) as new_ads_avg_px_m2,
        round((avg(price) filter (where price_m2 < 10000))::numeric,1) as new_ads_avg_px,
        round((avg(size) filter (where price_m2 < 10000))::numeric,1) as new_ads_avg_size
    from {{ ref(mat_view) }}
    where location in (select location from zg_loc)
    group by cast(strftime(first_seen, '%Y%m') as integer)
), sold_ads as (
    select
        cast(strftime(last_seen,'%Y%m') as integer) as year_month,
        count(distinct ad_id) filter (where price_m2 < 10000) as sold_ads_qty,
        round((avg(days_on_the_market) filter (where price_m2 < 10000))::numeric,1) as sold_ads_dom,
        round((avg(price_m2) filter (where price_m2 < 10000))::numeric,1) as sold_ads_avg_px_m2,
        round((avg(price) filter (where price_m2 < 10000))::numeric,1) as sold_ads_avg_px,
        round((avg(size) filter (where price_m2 < 10000))::numeric,1) as sold_ads_avg_size
    from {{ ref(mat_view) }}
    where status = 'inactive' and location in (select location from zg_loc)
    group by cast(strftime(last_seen,'%Y%m') as integer)
), actives as (
    select
        cast(strftime(seen_date,'%Y%m') as integer) as year_month,
        count(distinct s.ad_id) filter (where s.price < 10000000) as active_ads_qty,
        round((avg(days_on_the_market) filter (where s.price < 10000000))::numeric,1) as active_ads_dom,
        round((avg(s.price/s.size) filter (where s.price < 10000000))::numeric,1) as active_ads_avg_px_m2,
        round((avg(s.price) filter (where s.price < 10000000))::numeric,1) as active_ads_avg_px,
        round((avg(s.size) filter (where s.price < 10000000))::numeric,1) as active_ads_avg_size
    from {{ source(source_schema, source_table) }} s
    join {{ ref(price_history_view) }} as hist2
    on s.ad_id = hist2.ad_id
    where s.location in (select location from zg_loc)
    group by cast(strftime(seen_date, '%Y%m') as integer)
)
select
    strptime(concat(n.year_month,'01'), '%Y%m%d') as year_month,
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
         join sold_ads r on n.year_month = r.year_month
         join actives a on n.year_month = a.year_month
order by n.year_month desc

{% elif "postgres" in target.name %}

with zg_loc as (
    select distinct location from {{ ref(mat_view) }}
    where location2 like 'Grad Zagreb,%'
),
new_ads as (
    select
        cast(to_char(first_seen,'YYYYmm') as integer) as year_month,
        count(distinct ad_id) filter (where price_m2 < 10000) as new_ads_qty,
        round((avg(days_on_the_market) filter (where price_m2 < 10000))::numeric,1) as new_ads_dom,
        round((avg(price_m2) filter (where price_m2 < 10000))::numeric,1) as new_ads_avg_px_m2,
        round((avg(price) filter (where price_m2 < 10000))::numeric,1) as new_ads_avg_px,
        round((avg(size) filter (where price_m2 < 10000))::numeric,1) as new_ads_avg_size
    from {{ ref(mat_view) }}
    where location in (select location from zg_loc)
    group by cast(to_char(first_seen, 'YYYYmm') as integer)
), sold_ads as (
    select
        cast(to_char(last_seen,'YYYYmm') as integer) as year_month,
        count(distinct ad_id) filter (where price_m2 < 10000) as sold_ads_qty,
        round((avg(days_on_the_market) filter (where price_m2 < 10000))::numeric,1) as sold_ads_dom,
        round((avg(price_m2) filter (where price_m2 < 10000))::numeric,1) as sold_ads_avg_px_m2,
        round((avg(price) filter (where price_m2 < 10000))::numeric,1) as sold_ads_avg_px,
        round((avg(size) filter (where price_m2 < 10000))::numeric,1) as sold_ads_avg_size
    from {{ ref(mat_view) }}
    where status = 'inactive'
      and location in (select location from zg_loc)
    group by cast(to_char(last_seen,'YYYYmm') as integer)
), actives as (
    select
        cast(to_char(seen_date,'YYYYmm') as integer) as year_month,
        count(distinct s.ad_id) filter (where s.price < 10000000) as active_ads_qty,
        round((avg(days_on_the_market) filter (where s.price < 10000000))::numeric,1) as active_ads_dom,
        round((avg(s.price/s.size) filter (where s.price < 10000000))::numeric,1) as active_ads_avg_px_m2,
        round((avg(s.price) filter (where s.price < 10000000))::numeric,1) as active_ads_avg_px,
        round((avg(s.size) filter (where s.price < 10000000))::numeric,1) as active_ads_avg_size
    from {{ source(source_schema, source_table) }} s
    join {{ ref(price_history_view) }} as hist2
    on s.ad_id = hist2.ad_id
    where s.location in (select location from zg_loc)
    group by cast(to_char(seen_date, 'YYYYmm') as integer)
)
select
    TO_DATE(concat(n.year_month,'01'), 'YYYYMMDD') as year_month,
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
         join sold_ads r on n.year_month = r.year_month
         join actives a on n.year_month = a.year_month
order by n.year_month desc

{%- else -%} invalid_database

{% endif %}

{% endmacro %}