-- Gold layer: Comprehensive analytical table combining bookings, listings, and hosts
-- All columns from each table are included with prefixes: b_ for bookings, l_ for listings, h_ for hosts
{{ config(
    materialized='table',
    unique_key='b_booking_id'
) }}

{#
  Build dynamic column lists with prefixes for each table
#}
{% set bookings_cols = adapter.get_columns_in_relation(ref('bronze_bookings')) %}
{% set listings_cols = adapter.get_columns_in_relation(ref('bronze_listings')) %}
{% set hosts_cols = adapter.get_columns_in_relation(ref('bronze_hosts')) %}

{% set bookings_selects = [] %}
{% set listings_selects = [] %}
{% set hosts_selects = [] %}

{% for col in bookings_cols %}
  {% do bookings_selects.append('b.' ~ adapter.quote(col.name) ~ ' as b_' ~ col.name) %}
{% endfor %}

{% for col in listings_cols %}
  {% do listings_selects.append('l.' ~ adapter.quote(col.name) ~ ' as l_' ~ col.name) %}
{% endfor %}

{% for col in hosts_cols %}
  {% do hosts_selects.append('h.' ~ adapter.quote(col.name) ~ ' as h_' ~ col.name) %}
{% endfor %}

select
  {{ bookings_selects | join(',\n  ') }},
  {{ listings_selects | join(',\n  ') }},
  {{ hosts_selects | join(',\n  ') }}
from {{ ref('bronze_bookings') }} as b
left join {{ ref('bronze_listings') }} as l
  on b.listing_id = l.listing_id
left join {{ ref('bronze_hosts') }} as h
  on l.host_id = h.host_id
