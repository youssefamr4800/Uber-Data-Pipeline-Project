{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='trip_id'
) }}

SELECT
    trip_id,
    customer_id,
    driver_id,
    vehicle_id,
    trip_start_time,
    trip_end_time,
    distance_km,
    fare_amount,
    last_updated_timestamp
FROM {{ ref('trips') }}

{% if is_incremental() %}
WHERE last_updated_timestamp >= (
    SELECT COALESCE(
        MAX(last_updated_timestamp),
        CAST('1900-01-01' AS TIMESTAMP)
    )
    FROM {{ this }}
)
{% endif %}