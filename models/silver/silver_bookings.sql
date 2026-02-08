{{
  config(
    materialized='incremental',
    unique_key='booking_id',
    on_schema_change='sync_all_columns'
  )
}}

with source as (
    select * from {{ ref('bronze_bookings') }}
)
select
    booking_id,
    listing_id,
    cast(booking_date as date) as booking_date,
    nights_booked,
    booking_amount as base_price,
    cleaning_fee,
    service_fee,
    booking_status,
    upper(booking_status) as status_code,
    created_at
from source

{% if is_incremental() %}
  -- This filter will only be applied on an incremental run
  -- It looks for records newer than the max created_at already in the table
  where created_at > (select max(created_at) from {{ this }})
{% endif %}