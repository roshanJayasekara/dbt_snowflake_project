-- Secure view joining listings with their hosts for the silver layer
{{ config(
  materialized='view',
  secure=true
) }}


{{
  config(
    materialized='incremental',
    unique_key='listing_id',
    incremental_strategy='merge'
  )
}}

{#
  Build a list of host columns at compile time and prefix them with `host_`.
  We intentionally skip `host_id` so the top-level `host_id` (from listings)
  remains the single `host_id` column.
#}
{% set host_cols = adapter.get_columns_in_relation(ref('bronze_hosts')) %}
{% set host_selects = [] %}
{% for col in host_cols %}
  {% if col.name != 'host_id' %}
    {% do host_selects.append('h.' ~ adapter.quote(col.name) ~ ' as host_' ~ col.name) %}
  {% endif %}
{% endfor %}

select
  l.*,
  {{ host_selects | join(',\n  ') }}
from {{ ref('bronze_listings') }} as l
left join {{ ref('bronze_hosts') }} as h
  on l.host_id = h.host_id

{% if is_incremental() %}
  where l.created_at > (select coalesce(max(created_at), '1900-01-01') from {{ this }})
{% endif %}

