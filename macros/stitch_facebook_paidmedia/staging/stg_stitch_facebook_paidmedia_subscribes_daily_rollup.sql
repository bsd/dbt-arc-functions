{% macro create_stg_stitch_facebook_paidmedia_subscribes_daily_rollup(
    source_name="src_stitch_facebook_paidmedia", source_table="ads_insights"
) %}

    with
        actions as (
            select
                ad_id,
                date_start,
                action_value.value.action_type,
                action_value.value._28d_view
            from {{ source(source_name, source_table) }}
            cross join unnest(action_values) as action_value
        )
    select
        ad_id as message_id,
        safe_cast(date_start as timestamp) as date_timestamp,
        sum(_28d_view) as subscribes
    from actions
    where
        action_type in unnest(
            [
                'offsite_conversion.fb_pixel_complete_registration',
                'complete_registration',
                'omni_complete_registration'
            ]
        )
    group by 1, 2
    order by 1, 2

{% endmacro %}
