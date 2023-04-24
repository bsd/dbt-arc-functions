{% macro create_stg_adhoc_google_spreadsheets_paidmedia_monthly_goals() %}
    select
        safe_cast(initcap(month) || ' ' || year as string) as month_year,
        safe_cast(lower(objective) as string) as objective,
        safe_cast(lower(channel) as string) as channel,
        safe_cast(lower(platform) as string) as platform,
        safe_cast(projected_spend as float64) as projected_spend,
        safe_cast(projected_revenue as float64) as projected_revenue
    from
        {{
            source(
                "adhoc_google_spreadsheets_paidmedia_goals",
                "spreadsheet_paidmedia_monthly_goals",
            )
        }}
{% endmacro %}
