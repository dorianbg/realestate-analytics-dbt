{% macro update_target_table_from_source(source_table, target_table) %}

  {% set query %}
    create table if not exists {{target_table}} as select * from {{source_table}} where 0 = 1;
    delete from {{target_table}}; 
    insert into {{target_table}} select * from {{source_table}};
  {% endset %}

  {% do run_query(query) %}
{% endmacro %}
