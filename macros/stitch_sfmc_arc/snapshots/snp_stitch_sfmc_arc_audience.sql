{% macro create_snp_stitch_sfmc_arc_audience(
    source_name='src_stitch_sfmc_arc',
    source_table_name='arc_audience') %}
  {% snapshot snp_stitch_sfmc_arc_audience %} 
    {{
        config(
          target_database="{{var('database')}}",
          target_schema='snapshots',
          unique_key='subscriberkey',
          strategy='timestamp',
          updated_at='modifieddate',
        )
    }}

    select * from {{ source(source_name,source_table_name) }}
  {% endsnapshot %}
{% endmacro %}