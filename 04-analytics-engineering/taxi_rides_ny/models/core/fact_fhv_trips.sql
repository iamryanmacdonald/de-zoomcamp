{{ config(materialized='table') }}

with fhv_taxi_data as (
    select * from {{ ref('stg_fhv_taxi_data') }}
),
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

select
    fhv_taxi_data.dispatching_base_num,
    fhv_taxi_data.pickup_datetime,
    fhv_taxi_data.dropoff_datetime,
    fhv_taxi_data.pickup_location_id,
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,
    fhv_taxi_data.dropoff_location_id,
    dropoff_zone.borough as dropoff_borough,
    dropoff_zone.zone as dropoff_zone,
    fhv_taxi_data.sr_flag,
    fhv_taxi_data.affiliated_base_number
from fhv_taxi_data
inner join dim_zones as pickup_zone
on fhv_taxi_data.pickup_location_id = pickup_zone.location_id
inner join dim_zones as dropoff_zone
on fhv_taxi_data.dropoff_location_id = dropoff_zone.location_id