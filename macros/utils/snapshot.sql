{% macro snapshot(snapshot_name) %}

{% set snapshot_relation = ref(snapshot_name) %}

{# Make sure this is the actual name of your target in prod #}
{% if target.name != "prod" %}

{# Change this part based on how you name things in prod #}
{% set prod_snapshot_relation = adapter.get_relation(
    database=snapshot_relation.database,
    schema="prod_snapshots",
    identifier=snapshot_relation.identifier,
) %}

{% endif %}

{#
Use an or operator to handle:
1. the case where the prod version has not yet been created
2. the case where we are in prod
#}
{{ return(prod_snapshot_relation or snapshot_relation) }}
{% endmacro %}
