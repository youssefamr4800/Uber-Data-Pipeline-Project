{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='payment_id'
) }}

SELECT
    *
FROM {{ source('source_silver', 'payments') }}

{% if is_incremental() %}
WHERE last_updated_timestamp >= (
    SELECT COALESCE(
        MAX(last_updated_timestamp),
        CAST('1900-01-01' AS TIMESTAMP)
    )
    FROM {{ this }}
)
{% endif %}