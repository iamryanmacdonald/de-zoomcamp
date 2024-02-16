{{ config(materialized='view') }}

with taxi_data as (
    select
        *
    from {{ source('staging', 'fhv_taxi_data') }}
)

select
    dispatching_base_num,
    pickup_datetime,
    dropOff_datetime as dropoff_datetime,
    PUlocationID as pickup_location_id,
    DOlocationID as dropoff_location_id,
    SR_Flag as sr_flag,
    Affiliated_base_number as affiliated_base_number
from taxi_data
where pickup_datetime between '2019-01-01' and '2019-12-31'

{% if var('is_test_run', default=true) %}
limit 100
{% endif %}