{% macro create_secure_views_bulk() %}

  {# --- ADD NEW TABLES TO THIS LIST --- #}
  {% set tables_to_secure = [
      'silver_bookings',
      'silver_listings_hosts',
      'gold_bookings_analytics'
  ] %}
  {# ----------------------------------- #}

  {% if execute %}
    {# Create the schema if it doesn't exist #}
    {% set create_schema_sql %}
      CREATE SCHEMA IF NOT EXISTS secure_views;
    {% endset %}
    {% do run_query(create_schema_sql) %}
    {{ log("Schema secure_views created or already exists", info=True) }}
  {% endif %}

  {% for table_name in tables_to_secure %}
    
    {% set source_relation = ref(table_name) %}
    
    {% set sql %}
      CREATE OR REPLACE SECURE VIEW secure_views.{{ table_name }}_secure AS
      SELECT * FROM {{ source_relation }};
    {% endset %}

    {% if execute %}
      {% do run_query(sql) %}
      {{ log("Created Secure View: " ~ table_name ~ "_secure", info=True) }}
    {% endif %}
    
  {% endfor %}

{% endmacro %}