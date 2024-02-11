{% macro avg_px_per_loc_and_size_macro(ref_name, lookback_days, min_price) %}

select distinct 
	location,
    FLOOR(size / 20) * 20 as size,
	count(distinct ad_id) area_total_ads,
	avg(price/size) as area_average
from {{ ref(ref_name) }}
WHERE last_seen >  {{ dbt.current_timestamp() }}::date - INTERVAL '{{lookback_days}}'
    and price > {{min_price}}
group by location, FLOOR(size / 20)

{% endmacro %}
