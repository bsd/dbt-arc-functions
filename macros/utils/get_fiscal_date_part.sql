{%- macro get_fiscal_year(date_to_convert, fiscal_year_start="01-01") -%}
{%- set date_to_convert_year -%}
EXTRACT(YEAR FROM CAST({{date_to_convert}} AS DATE))
{%- endset -%}
{%- set date_to_convert_fiscal_year_start_string -%}
CAST({{date_to_convert_year}} AS STRING) || '-' || CAST('{{fiscal_year_start }}' AS STRING)
{%- endset -%}
{%- set date_to_convert_fiscal_year_start_date -%}
PARSE_DATE('%Y-%m-%d', {{date_to_convert_fiscal_year_start_string}})
{%- endset -%}
case
    when cast('{{fiscal_year_start}}' as string) = '01-01'
    then {{ date_to_convert_year }}
    when
        {{ date_to_convert_fiscal_year_start_date }}
        > cast({{ date_to_convert }} as date)
    then {{ date_to_convert_year }}
    else {{ date_to_convert_year }} + 1
end
{%- endmacro -%}

{%- macro get_fiscal_quarter(date_to_convert, fiscal_year_start="01-01") -%}
{%- set date_to_convert_year -%}
EXTRACT(YEAR FROM CAST({{date_to_convert}} AS DATE))
{%- endset -%}
{%- set date_to_convert_fiscal_year_start_string -%}
CAST({{date_to_convert_year}} AS STRING) || '-' || CAST('{{fiscal_year_start }}' AS STRING)
{%- endset -%}
{%- set date_to_convert_fiscal_year_start_date -%}
PARSE_DATE('%Y-%m-%d', {{date_to_convert_fiscal_year_start_string}})
{%- endset -%}
{%- set date_to_convert_year_before -%}
EXTRACT(YEAR FROM CAST({{date_to_convert}} AS DATE)) - 1
{%- endset -%}
{%- set date_to_convert_fiscal_year_before_start_string -%}
CAST({{date_to_convert_year_before}} AS STRING) || '-' || CAST('{{fiscal_year_start }}' AS STRING)
{%- endset -%}
{%- set date_to_convert_fiscal_year_before_start_date -%}
PARSE_DATE('%Y-%m-%d', {{date_to_convert_fiscal_year_before_start_string}})
{%- endset -%}
{%- set days_since_fiscal_year_before_start_date -%}
DATE_DIFF(CAST({{date_to_convert}} AS DATE), {{date_to_convert_fiscal_year_before_start_date}}, DAYS)
{%- endset -%}
case
    when cast('{{fiscal_year_start}}' as string) = '01-01'
    then extract(quarter from cast({{ date_to_convert }} as date))
    when
        cast({{ date_to_convert }} as date)
        >= {{ date_to_convert_fiscal_year_start_date }}
    then
        date_diff(
            cast({{ date_to_convert }} as date),
            {{ date_to_convert_fiscal_year_start_date }},
            quarter
        )
        + 1
    else
        date_diff(
            cast({{ date_to_convert }} as date),
            {{ date_to_convert_fiscal_year_before_start_date }},
            quarter
        )
        + 1
end
{%- endmacro -%}

{%- macro get_fiscal_month(date_to_convert, fiscal_year_start="01-01") -%}
{%- set date_to_convert_year -%}
EXTRACT(YEAR FROM CAST({{date_to_convert}} AS DATE))
{%- endset -%}
{%- set date_to_convert_fiscal_year_start_string -%}
CAST({{date_to_convert_year}} AS STRING) || '-' || CAST('{{fiscal_year_start }}' AS STRING)
{%- endset -%}
{%- set date_to_convert_fiscal_year_start_date -%}
PARSE_DATE('%Y-%m-%d', {{date_to_convert_fiscal_year_start_string}})
{%- endset -%}
{%- set date_to_convert_year_before -%}
EXTRACT(YEAR FROM CAST({{date_to_convert}} AS DATE)) - 1
{%- endset -%}
{%- set date_to_convert_fiscal_year_before_start_string -%}
CAST({{date_to_convert_year_before}} AS STRING) || '-' || CAST('{{fiscal_year_start }}' AS STRING)
{%- endset -%}
{%- set date_to_convert_fiscal_year_before_start_date -%}
PARSE_DATE('%Y-%m-%d', {{date_to_convert_fiscal_year_before_start_string}})
{%- endset -%}
{%- set days_since_fiscal_year_before_start_date -%}
DATE_DIFF(CAST({{date_to_convert}} AS DATE), {{date_to_convert_fiscal_year_before_start_date}}, DAYS)
{%- endset -%}
case
    when cast('{{fiscal_year_start}}' as string) = '01-01'
    then extract(month from cast({{ date_to_convert }} as date))
    when
        cast({{ date_to_convert }} as date)
        >= {{ date_to_convert_fiscal_year_start_date }}
    then
        date_diff(
            cast({{ date_to_convert }} as date),
            {{ date_to_convert_fiscal_year_start_date }},
            month
        )
        + 1
    else
        date_diff(
            cast({{ date_to_convert }} as date),
            {{ date_to_convert_fiscal_year_before_start_date }},
            month
        )
        + 1
end
{%- endmacro -%}

{%- macro get_fiscal_week(date_to_convert, fiscal_year_start="01-01") -%}
{%- set date_to_convert_year -%}
EXTRACT(YEAR FROM CAST({{date_to_convert}} AS DATE))
{%- endset -%}
{%- set date_to_convert_fiscal_year_start_string -%}
CAST({{date_to_convert_year}} AS STRING) || '-' || CAST('{{fiscal_year_start }}' AS STRING)
{%- endset -%}
{%- set date_to_convert_fiscal_year_start_date -%}
PARSE_DATE('%Y-%m-%d', {{date_to_convert_fiscal_year_start_string}})
{%- endset -%}
{%- set date_to_convert_year_before -%}
EXTRACT(YEAR FROM CAST({{date_to_convert}} AS DATE)) - 1
{%- endset -%}
{%- set date_to_convert_fiscal_year_before_start_string -%}
CAST({{date_to_convert_year_before}} AS STRING) || '-' || CAST('{{fiscal_year_start }}' AS STRING)
{%- endset -%}
{%- set date_to_convert_fiscal_year_before_start_date -%}
PARSE_DATE('%Y-%m-%d', {{date_to_convert_fiscal_year_before_start_string}})
{%- endset -%}
{%- set days_since_fiscal_year_before_start_date -%}
DATE_DIFF(CAST({{date_to_convert}} AS DATE), {{date_to_convert_fiscal_year_before_start_date}}, DAYS)
{%- endset -%}
case
    when cast('{{fiscal_year_start}}' as string) = '01-01'
    then extract(week from cast({{ date_to_convert }} as date))
    when
        cast({{ date_to_convert }} as date)
        >= {{ date_to_convert_fiscal_year_start_date }}
    then
        date_diff(
            cast({{ date_to_convert }} as date),
            {{ date_to_convert_fiscal_year_start_date }},
            week
        )
        + 1
    else
        date_diff(
            cast({{ date_to_convert }} as date),
            {{ date_to_convert_fiscal_year_before_start_date }},
            week
        )
        + 1
end
{%- endmacro -%}

{%- macro get_fiscal_day(date_to_convert, fiscal_year_start="01-01") -%}
{%- set date_to_convert_year -%}
EXTRACT(YEAR FROM CAST({{date_to_convert}} AS DATE))
{%- endset -%}
{%- set date_to_convert_fiscal_year_start_string -%}
CAST({{date_to_convert_year}} AS STRING) || '-' || CAST('{{fiscal_year_start }}' AS STRING)
{%- endset -%}
{%- set date_to_convert_fiscal_year_start_date -%}
PARSE_DATE('%Y-%m-%d', {{date_to_convert_fiscal_year_start_string}})
{%- endset -%}
{%- set date_to_convert_year_before -%}
EXTRACT(YEAR FROM CAST({{date_to_convert}} AS DATE)) - 1
{%- endset -%}
{%- set date_to_convert_fiscal_year_before_start_string -%}
CAST({{date_to_convert_year_before}} AS STRING) || '-' || CAST('{{fiscal_year_start }}' AS STRING)
{%- endset -%}
{%- set date_to_convert_fiscal_year_before_start_date -%}
PARSE_DATE('%Y-%m-%d', {{date_to_convert_fiscal_year_before_start_string}})
{%- endset -%}
{%- set days_since_fiscal_year_before_start_date -%}
DATE_DIFF(CAST({{date_to_convert}} AS DATE), {{date_to_convert_fiscal_year_before_start_date}}, DAYS)
{%- endset -%}
case
    when cast('{{fiscal_year_start}}' as string) = '01-01'
    then extract(day from cast({{ date_to_convert }} as date))
    when
        cast({{ date_to_convert }} as date)
        >= {{ date_to_convert_fiscal_year_start_date }}
    then
        date_diff(
            cast({{ date_to_convert }} as date),
            {{ date_to_convert_fiscal_year_start_date }},
            day
        )
        + 1
    else
        date_diff(
            cast({{ date_to_convert }} as date),
            {{ date_to_convert_fiscal_year_before_start_date }},
            day
        )
        + 1
end
{%- endmacro -%}
