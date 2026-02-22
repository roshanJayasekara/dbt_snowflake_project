{{ config(
    materialized = 'incremental',
    unique_key = 'booking_id',
    incremental_strategy = 'append'
) }}

select *
from {{ source('staging', 'bookings') }}

{% if is_incremental() %}
where created_at >= (
    select coalesce(max(created_at), '1900-01-01')
    from {{ this }}
)
{% endif %}
