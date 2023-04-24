{% macro create_stg_frakture_global_message_paidmedia_ad_summary_by_date() %}
    select distinct *
    from
        {{
            source(
                "frakture_global_message_paidmedia", "global_message_summary_by_date"
            )
        }}
    where message_id is not null and channel != 'email'
{% endmacro %}
