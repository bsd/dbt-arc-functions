{% macro create_stg_adhoc_google_spreadsheets_paidmedia_pacing_budget() %}
    select
        safe_cast(start_date as date) as campaign_start_date,
        safe_cast(end_date as date) as campaign_end_date,
        safe_cast(lower(campaign_name) as string) as campaign_name,
        safe_cast(budget as float64) as budget,
        safe_cast(description as string) as descriptions
    from
        {{
            source(
                "adhoc_google_spreadsheets_paidmedia_pacing",
                "spreadsheet_paidmedia_pacing_budget",
            )
        }}
{% endmacro %}
