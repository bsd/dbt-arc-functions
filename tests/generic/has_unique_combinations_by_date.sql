{% test has_unique_combinations_by_date(model, date_column) %}

    {{ config(severity="warn") }}

    select {{ date_column }}, count(*) as num_combinations
    from {{ model }}
    group by 1
    having count(*) > 1

{% endtest %}
