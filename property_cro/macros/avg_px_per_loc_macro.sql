{% macro avg_px_per_loc_macro(ref_name, lookback_days, min_price) %}

select distinct
	location,
	count(ad_id) over (partition by location) as area_total_ads,
	avg(price/size) over (partition by location) as area_average
from {{ ref(ref_name) }}
where last_seen > {{ dbt.current_timestamp() }}::date - INTERVAL '{{lookback_days}}'
    and price > {{min_price}}

{% endmacro %}
