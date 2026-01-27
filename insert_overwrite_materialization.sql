{#
    Custom materialization: insert_overwrite
    
    This materialization uses Snowflake's INSERT OVERWRITE statement to atomically
    replace all data in a table without dropping and recreating it. This preserves
    table metadata, grants, and other properties.
    
    Behavior:
    - If the target table does not exist: Creates a new table using CREATE TABLE AS
    - If the target is a view: Raises an error (cannot INSERT OVERWRITE into a view)
    - If the target table exists:
        1. Creates a temporary table with LIMIT 0 to infer the source query schema
        2. Compares source schema (column names and types) with target table schema
        3. If schemas match: Executes INSERT OVERWRITE to replace data in place
        4. If schemas differ: Recreates the table using CREATE OR REPLACE TABLE
    
    Usage:
        {{ config(materialized='insert_overwrite') }}
        SELECT ...
#}

{% materialization insert_overwrite, adapter='snowflake' %}

    {# Set up relation references #}
    {%- set target_relation = this.incorporate(type='table') -%}
    {%- set existing_relation = load_relation(this) -%}
    {%- set tmp_relation = make_temp_relation(target_relation) -%}

    {# Run pre-hooks #}
    {{ run_hooks(pre_hooks, inside_transaction=False) }}
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    {# Case 1: Table does not exist - create it #}
    {% if existing_relation is none %}
        {% call statement('main') %}
            create table {{ target_relation }} as (
                {{ sql }}
            )
        {% endcall %}

    {# Case 2: Target is a view - raise error #}
    {% elif existing_relation.is_view %}
        {{ exceptions.raise_compiler_error("Cannot INSERT OVERWRITE into a view. Please drop the view first.") }}

    {# Case 3: Table exists - check schema and either INSERT OVERWRITE or recreate #}
    {% else %}
        {# Create a temporary table with no rows to infer source query schema #}
        {% call statement('create_tmp') %}
            create temporary table {{ tmp_relation }} as (
                {{ sql }}
            ) limit 0
        {% endcall %}

        {# Get column metadata from both source query and target table #}
        {% set source_columns = adapter.get_columns_in_relation(tmp_relation) %}
        {% set target_columns = adapter.get_columns_in_relation(existing_relation) %}

        {# Build list of (column_name, data_type) tuples for source #}
        {% set source_schema = [] %}
        {% for col in source_columns %}
            {% do source_schema.append((col.name | upper, col.dtype | upper)) %}
        {% endfor %}

        {# Build list of (column_name, data_type) tuples for target #}
        {% set target_schema = [] %}
        {% for col in target_columns %}
            {% do target_schema.append((col.name | upper, col.dtype | upper)) %}
        {% endfor %}

        {# Compare schemas (sorted to ignore column order differences) #}
        {% set schemas_match = (source_schema | sort) == (target_schema | sort) %}

        {# Clean up temporary table #}
        {% call statement('drop_tmp') %}
            drop table if exists {{ tmp_relation }}
        {% endcall %}

        {# If schemas match: use INSERT OVERWRITE for efficient in-place replacement #}
        {% if schemas_match %}
            {% call statement('main') %}
                insert overwrite into {{ target_relation }}
                {{ sql }}
            {% endcall %}

        {# If schemas differ: recreate table with new schema #}
        {% else %}
            {% call statement('main') %}
                create or replace table {{ target_relation }} as (
                    {{ sql }}
                )
            {% endcall %}
        {% endif %}
    {% endif %}

    {# Run post-hooks #}
    {{ run_hooks(post_hooks, inside_transaction=True) }}

    {# Persist column and model documentation #}
    {% do persist_docs(target_relation, model) %}

    {{ run_hooks(post_hooks, inside_transaction=False) }}

    {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
