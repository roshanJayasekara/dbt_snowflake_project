{% macro create_secure_views_bulk() %}

  {# --- ADD NEW TABLES TO THIS LIST --- #}
  {% set tables_to_secure = [
      'silver_bookings',
      'silver_listings_hosts',
      'gold_bookings_analytics'
  ] %}
  {# ----------------------------------- #}

  {% for table_name in tables_to_secure %}
    
    {% set source_relation = ref(table_name) %}
    
    {% set sql %}
      CREATE OR REPLACE SECURE VIEW secure_views.{{ table_name }}_secure AS
      SELECT * FROM {{ source_relation }};
    {% endset %}

    {% do run_query(sql) %}
    {{ log("Created Secure View: " ~ table_name ~ "_secure", info=True) }}
    
  {% endfor %}

{% endmacro %}