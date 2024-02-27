# Real estate analytics dbt

Models strictly apply to data from the Croatian housing market.

### Data model explained

##### Base data tables
- sales_flats, rentals_flats, sales_houses
  - high level data -e.g. from the general search view
- sale_flat_desc, rental_flat_desc, sale_house_desc
  - very detailed data - e.g. data from the specific ads page
- favorite_ads
  - simple table for tracking with manually added ad_id's
- sent_alerts
  - table containing all sent alerts
  - used to limit how many times an analytics alert can be sent

##### Base views - live in one of analytics_(sales_flats/slaes_houses/rentals_flats) scchemas
- vw_last_ad_time
  - looks at the high level data to determine when an ad was first time and last time seen
- vw_price_history
  - gathers the historical and latest price for each ad
- vw_avg_ask_px_per_loc
  - looks at the price of ads seen in last N days to determine the average price for a location
- vw_avg_ask_px_per_loc_size
  - looks at the price of ads seen in last 60 days to determine the average price for a given location and property size
- vw_avg_ask_px_per_loc_year_size
  - looks at the price of ads seen in last 60 days to determine the average price for a given location, property size and property age
- mv_enriched_ad
  - joins together all of the derived data into a single view

##### Alerts

Various custom queries on top of clean and enriched data

### DBT data model docs

DBT model docs should be visible as part of this repositories github pages.  
Locally you can generate docs using: 
```
dbt docs generate --profiles-dir property_cro --project-dir property_cro --profile property_cro_postgres; 
dbt docs serve --profiles-dir property_cro --project-dir property_cro --profile property_cro_postgres
```

### Refresh DBT docs in repository

Example:
```
cp property_cro/target/*.json property_cro/target/*.html property_cro/target/graph.gpickle docs && zip -jrq docs.zip docs
```

### Running DBT from CLI

Example for PostgreSQL:  
```
poetry run dbt run --profiles-dir property_cro --project-dir property_cro --profile property_cro_postgres --target dev_postgres --threads 4
```

Example for DuckDB:   
```
poetry run dbt run --profiles-dir property_cro --project-dir property_cro --profile property_cro_duckdb  --target dev_duckdb --threads 4
```


### DBT profile example 

```
config:
  send_anonymous_usage_stats: false
  use_colors: true
  partial_parse: true
  warn_error: false
  debug: false
  version_check: true
  fail_fast: false

property_cro_postgres:
  target: dev_postgres
  outputs:
    dev_postgres:
      type: postgres
      host: localhost
      port: 5432
      user: "{{ env_var('PGUSER') }}"
      password: "{{ env_var('PGPASSWORD') }}"
      dbname: property_cro
      schema: analytics
      threads: 4
      keepalives_idle: 0 # default 0, indicating the system default. See below
      connect_timeout: 10 # default 10 seconds

property_cro_duckdb:
  target: dev_duckdb
  outputs:
    dev_duckdb:
      type: duckdb
      path: prop_cro.duckdb
      extensions:
        - postgres
      attach:
        - path: "dbname=property_cro host=127.0.0.1 user={{ env_var('PGUSER') }} password={{ env_var('PGPASSWORD') }}"
          type: postgres
          alias: property_cro
      settings:
        pg_experimental_filter_pushdown: true
      threads: 4

```