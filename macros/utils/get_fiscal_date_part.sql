{%- macro get_fiscal_year(date_to_convert, fiscal_year_start="01-01") -%}
    {%- set date_to_convert_year -%}
EXTRACT(YEAR FROM {{date_to_convert}})
    {%- endset -%}
    {%- set date_to_convert_fiscal_year_start_string -%}
CAST({{date_to_convert_year}} AS STRING) || '-' || CAST('{{fiscal_year_start | as_text}}' AS STRING)
    {%- endset -%}
    {%- set date_to_convert_fiscal_year_start_date -%}
PARSE_DATE('%Y-%m-%d', {{date_to_convert_fiscal_year_start_string}})
    {%- endset -%}
    case
        when cast('{{fiscal_year_start | as_text}}' as string) = '01-01'
        then {{ date_to_convert_year }}
        when {{ date_to_convert_fiscal_year_start_date }} > {{ date_to_convert }}
        then {{ date_to_convert_year }}
        else {{ date_to_convert_year }} + 1
    end
{%- endmacro -%}
