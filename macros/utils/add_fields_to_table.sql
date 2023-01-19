{% macro add_fields_to_table(fields_list, table_name) %}
    {% assert type(fields_list) is list %}
    {% set table = ref(table_name) if table_name in refs else source(table_name) %}
    {% for field_name in fields_list %}
        {% if field_name not in table.columns %}
            {{ table.with_column(field_name, None) }}
        {% else %}
            {{ table }}
        {% endif %}
    {% endfor %}
{% endmacro %}
