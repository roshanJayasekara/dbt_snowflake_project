{{ config(
    materialized = 'incremental',
    unique_key = 'listing_id',
    incremental_strategy = 'append'
) }}

select *
from {{ source('staging', 'listings') }}

{% if is_incremental() %}
where created_at > (
    select coalesce(max(created_at), '1900-01-01')
    from {{ this }}
)
{% endif %}
