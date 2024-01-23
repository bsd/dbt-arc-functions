{% macro create_stg_stitch_facebook_paidmedia_extract_fields_from_adcreative_parameterized(
    campaign_regex,
    objective_regex,
    audience_regex,
    source_adcreative_name="src_stitch_facebook_paidmedia",
    source_adcreative_table_name="adcreative",
    source_ads_name="src_stitch_facebook_paidmedia",
    source_ads_table_name="ads",
    source_code_regex="\\\\?(.*)"
) %}
with
    adcreative_id_to_source_code as (
        select distinct *
        from
            (
                select
                    id as adcreative_id,
                    regexp_extract(
                        link_url,
                        {% if source_code_regex == "" %} null
                        {% else %} '{{ source_code_regex }}'
                        {% endif %}
                    ) as source_code,
                    link_url as link,
                from {{ source(source_adcreative_name, source_adcreative_table_name) }}
                where link_url is not null
                union all
                select
                    id as adcreative_id,
                    regexp_extract(
                        object_story_spec.link_data.link,
                        {% if source_code_regex == "" %} null
                        {% else %} '{{ source_code_regex }}'
                        {% endif %}
                    ) as source_code,
                    object_story_spec.link_data.link as link,
                from {{ source(source_adcreative_name, source_adcreative_table_name) }}
                where object_story_spec.link_data.link is not null
                union all
                select
                    id as adcreative_id,
                    regexp_extract(
                        object_story_spec.video_data.call_to_action.value.link,
                        {% if source_code_regex == "" %} null
                        {% else %} '{{ source_code_regex }}'
                        {% endif %}
                    ) as source_code,
                    object_story_spec.video_data.call_to_action.value.link as link,
                from {{ source(source_adcreative_name, source_adcreative_table_name) }}
                where object_story_spec.video_data.call_to_action.value.link is not null
                union all
                select
                    id as adcreative_id,
                    regexp_extract(
                        ca.value.link,
                        {% if source_code_regex == "" %} null
                        {% else %} '{{ source_code_regex }}'
                        {% endif %}
                    ) as source_code,
                    ca.value.link as link,
                from {{ source(source_adcreative_name, source_adcreative_table_name) }}
                cross join unnest(object_story_spec.link_data.child_attachments) as ca
                where ca.value.link is not null
            )
    ),

    ad_id_to_adcreative_id as (
        select distinct id as ad_id, creative.id as adcreative_id
        from {{ source(source_ads_name, source_ads_table_name) }}
    ),

    ad_id_to_source_code as (
        select distinct ad_id, source_code, link
        from ad_id_to_adcreative_id
        inner join
            adcreative_id_to_source_code
            on ad_id_to_adcreative_id.adcreative_id
            = adcreative_id_to_source_code.adcreative_id
    ),

    ad_id_to_single_source_code as (
        select
            ad_id,
            max(link) as link_single,
            array_agg(link) as link_array,
            max(source_code) as source_code_single,
            array_agg(source_code) as source_code_array
        from ad_id_to_source_code
        group by 1
    )

select
    ad_id,
    source_code_single,
    source_code_array,
    link_single,
    link_array,
    regexp_extract(
        source_code_single,
        {% if audience_regex == "" %} null
        {% else %} '{{ audience_regex }}'
        {% endif %}
    ) as audience,
    regexp_extract(
        source_code_single,
        {% if campaign_regex == "" %} null
        {% else %} '{{ campaign_regex }}'
        {% endif %}
    ) as campaign,
    regexp_extract(
        source_code_single,
        {% if objective_regex == "" %} null
        {% else %} '{{ objective_regex }}'
        {% endif %}
    ) as objective
from ad_id_to_single_source_code

{% endmacro %}
