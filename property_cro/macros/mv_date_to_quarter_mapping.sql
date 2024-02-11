{% macro date_to_quarter_mapping_macro() %}

{% if "duckdb" in target.name %}

with quarterly as (
    SELECT current_date - interval (91 * generate_series) day AS quarter
    FROM generate_series(0, 50)
), daily as (
    select current_date - interval (generate_series) day AS date_
    from generate_series(0,4550)
), joined as (
    select
        date_, quarter, extract('day' from date_ - quarter),
        ROW_NUMBER() OVER (PARTITION BY date_ ORDER BY extract('day' from date_ - quarter) desc) as rn
    from quarterly q
             join daily d on q.quarter > d.date_
)
select date_, quarter, *
from joined
where rn = 1

    {% elif "postgres" in target.name %}

with quarterly as (
    SELECT current_date - 91 * sequence.day AS quarter
    FROM generate_series(0, 50) AS sequence(day)
), daily as (
    select current_date - sequence.day AS date_
    from generate_series(0,4550) AS sequence(day)
), joined as (
    select
        date_, quarter,
        ROW_NUMBER() OVER (PARTITION BY date_ ORDER BY abs(date_ - quarter)) as rn
    from quarterly q
             join daily d
                  on q.quarter > d.date_
)
select date_, quarter
from joined
where rn = 1


{%- else -%} invalid_database

{% endif %}

{% endmacro %}