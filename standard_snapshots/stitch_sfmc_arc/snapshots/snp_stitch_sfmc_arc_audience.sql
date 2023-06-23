{% snapshot snp_stitch_sfmc_arc_audience %} 
  {{
      config(
        target_database=var('database'),
        target_schema=target.schema + '_snapshots',
        unique_key='subscriberkey',
        strategy='timestamp',
        updated_at='modifieddate',
      )
  }}

  select * from {{ source('src_stitch_sfmc_arc', 'arc_audience') }}
{% endsnapshot %}