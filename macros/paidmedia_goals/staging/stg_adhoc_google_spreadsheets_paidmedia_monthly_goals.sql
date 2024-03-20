{% macro create_stg_adhoc_google_spreadsheets_paidmedia_monthly_goals() %}
    select
        safe_cast(initcap(month) || ' ' || year as string) as month_year,
        lower(safe_cast(objective as string)) as objective,
        initcap(safe_cast(channel as string)) as channel,
        initcap(safe_cast(platform as string)) as platform,
        safe_cast(projected_spend as float64) as projected_spend,
        safe_cast(projected_revenue as float64) as projected_revenue,
        safe_cast(monthly_revenue_target as float64) as monthly_revenue_target,
        safe_cast(monthly_gifts_target as float64) as monthly_gifts_target,
        safe_cast(1x_donor_target as float64) as 1x_donor_target,
        safe_cast(1x_donor_revenue_target as float64) as 1x_donor_revenue_target,
        safe_cast(total_gifts_target as float64) as total_gifts_target
    from
        {{
            source(
                "adhoc_google_spreadsheets_paidmedia_goals",
                "spreadsheet_paidmedia_monthly_goals",
            )
        }}
{% endmacro %}