{{
    config(
        materialized='view'
    )
}}

select 
    -- identifiers
    pulocationid as pickup_locationid,
    dolocationid as dropoff_locationid,
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    -- trip info
    sr_flag as store_and_fwd_flag
    -- payment info
from {{ source('staging','fhv_tripdata') }}
where cast(pickup_datetime as date) between '2019-01-01' and '2019-12-31'

-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}