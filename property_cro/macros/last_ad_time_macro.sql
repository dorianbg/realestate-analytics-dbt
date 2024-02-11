{% macro last_ad_time_macro(source_schema, source_table) %}

select
    ad_id,
    min(seen_date) as first_seen,
    max(seen_date) as last_seen,
    case
        when max(seen_date) < {{ dbt.current_timestamp() }}::date - INTERVAL '4 DAY'
            then 'inactive'
        else 'active'
    end as status,
    max(seen_date) - min(seen_date) as days_on_the_market
from {{ source(source_schema, source_table) }}
group by ad_id

{% endmacro %}
