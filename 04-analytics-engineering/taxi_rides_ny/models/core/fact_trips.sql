{{ config(materialized='table') }}

with green_taxi_data as (
    select
        *,
        'Green' as service_type
    from {{ ref('stg_green_taxi_data') }}
),
yellow_taxi_data as (
    select
        *,
        'Yellow' as service_type
    from {{ ref('stg_yellow_taxi_data') }}
),
combined_taxi_data as (
    select * from green_taxi_data
    union all
    select * from yellow_taxi_data
),
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

select
    combined_taxi_data.trip_id,
    combined_taxi_data.vendor_id,
    combined_taxi_data.service_type,
    combined_taxi_data.rate_code,
    combined_taxi_data.pickup_location_id,
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,
    combined_taxi_data.dropoff_location_id,
    dropoff_zone.borough as dropoff_borough,
    dropoff_zone.zone as dropoff_zone,
    combined_taxi_data.pickup_datetime,
    combined_taxi_data.dropoff_datetime,
    combined_taxi_data.store_and_fwd_flag,
    combined_taxi_data.passenger_count,
    combined_taxi_data.trip_distance,
    combined_taxi_data.trip_type,
    combined_taxi_data.fare_amount,
    combined_taxi_data.extra,
    combined_taxi_data.mta_tax,
    combined_taxi_data.tip_amount,
    combined_taxi_data.tolls_amount,
    combined_taxi_data.ehail_fee,
    combined_taxi_data.imp_surcharge,
    combined_taxi_data.total_amount,
    combined_taxi_data.payment_type,
    combined_taxi_data.payment_type_description
from combined_taxi_data
inner join dim_zones as pickup_zone
on combined_taxi_data.pickup_location_id = pickup_zone.location_id
inner join dim_zones as dropoff_zone
on combined_taxi_data.dropoff_location_id = dropoff_zone.location_id