{{ config(materialized='view') }}

with taxi_data as (
    select
        *,
        row_number() over(partition by vendor_id, pickup_datetime) as rn
    from {{ source('staging', 'green_taxi_data') }}
    where vendor_id is not null
)
select
    -- identifiers
    {{ dbt_utils.generate_surrogate_key(['vendor_id', 'pickup_datetime']) }} as trip_id,
    {{ dbt.safe_cast('vendor_id', api.Column.translate_type('integer')) }} as vendor_id,
    safe_cast(safe_cast(rate_code as numeric) as integer) as rate_code,
    {{ dbt.safe_cast('pickup_location_id', api.Column.translate_type('integer')) }} as pickup_location_id,
    {{ dbt.safe_cast('dropoff_location_id', api.Column.translate_type('integer')) }} as dropoff_location_id,

    -- timestamps
    safe_cast(pickup_datetime as timestamp) as pickup_datetime,
    safe_cast(dropoff_datetime as timestamp) as dropoff_datetime,

    -- trip information
    store_and_fwd_flag,
    {{ dbt.safe_cast('passenger_count', api.Column.translate_type('integer')) }} as passenger_count,
    safe_cast(trip_distance as numeric) as trip_distance,
    safe_cast(trip_type as numeric) as trip_type,

    -- payment information
    safe_cast(fare_amount as numeric) as fare_amount,
    safe_cast(extra as numeric) as extra,
    safe_cast(mta_tax as numeric) as mta_tax,
    safe_cast(tip_amount as numeric) as tip_amount,
    safe_cast(tolls_amount as numeric) as tolls_amount,
    safe_cast(ehail_fee as numeric) as ehail_fee,
    safe_cast(imp_surcharge as numeric) as imp_surcharge,
    safe_cast(total_amount as numeric) as total_amount,
    safe_cast(payment_type as numeric) as payment_type,
    {{ get_payment_type_description('payment_type') }} as payment_type_description,
from taxi_data
where rn = 1

{% if var('is_test_run', default=true) %}
limit 100
{% endif %}