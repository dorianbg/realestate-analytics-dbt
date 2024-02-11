{% macro avg_px_per_loc_and_size_and_age_macro(ref_name, source_schema, source_table, lookback_days, min_price, max_price_m2) %}

select distinct
	location,
    FLOOR(year_built / 20) * 20 as year_range,
    FLOOR(size / 20) * 20 as size,
	count(distinct vh.ad_id) area_total_ads,
	avg(price/size) as area_average
from {{ ref(ref_name) }} vh
join {{ source(source_schema, source_table) }} sd on vh.ad_id = sd.ad_id
WHERE last_seen >=  {{ dbt.current_timestamp() }}::date - INTERVAL '{{lookback_days}}'
    and year_built is not null and size is not null
    and price > {{min_price}}
group by location, FLOOR(year_built / 20), FLOOR(size / 20)

{% endmacro %}
