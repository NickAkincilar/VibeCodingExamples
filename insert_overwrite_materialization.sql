{#
    Custom Materialization: insert_overwrite
    
    Uses Snowflake's INSERT OVERWRITE to atomically replace table data while preserving
    the table object (grants, metadata, etc.).
    
    Behavior:
    - Table doesn't exist: Creates new table via CREATE TABLE AS
    - Target is a view: Raises error
    - Table exists with matching schema: INSERT OVERWRITE (fastest)
    - Table exists with different schema: ALTER TABLE to add/drop columns, then INSERT OVERWRITE
    
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

    {# Case 1: Table doesn't exist - create it #}
    {% if existing_relation is none %}
        {% call statement('main') %}
            create table {{ target_relation }} as (
                {{ sql }}
            )
        {% endcall %}

    {# Case 2: Target is a view - cannot INSERT OVERWRITE into a view #}
    {% elif existing_relation.is_view %}
        {{ exceptions.raise_compiler_error("Cannot INSERT OVERWRITE into a view. Please drop the view first.") }}

    {# Case 3: Table exists - compare schemas and sync if needed #}
    {% else %}
        {# Create temp table with LIMIT 0 to infer source query schema without loading data #}
        {% call statement('create_tmp') %}
            create temporary table {{ tmp_relation }} as (
                {{ sql }}
            ) limit 0
        {% endcall %}

        {# Get column metadata from source query and target table #}
        {% set source_columns = adapter.get_columns_in_relation(tmp_relation) %}
        {% set target_columns = adapter.get_columns_in_relation(existing_relation) %}

        {# Build source column name list and mapping for lookup #}
        {% set source_col_names = [] %}
        {% set source_col_map = {} %}
        {% for col in source_columns %}
            {% do source_col_names.append(col.name | upper) %}
            {% do source_col_map.update({col.name | upper: col}) %}
        {% endfor %}

        {# Build target column name list #}
        {% set target_col_names = [] %}
        {% for col in target_columns %}
            {% do target_col_names.append(col.name | upper) %}
        {% endfor %}

        {# Identify columns to drop (in target but not in source) #}
        {% set cols_to_drop = [] %}
        {% for col_name in target_col_names %}
            {% if col_name not in source_col_names %}
                {% do cols_to_drop.append(col_name) %}
            {% endif %}
        {% endfor %}

        {# Identify columns to add (in source but not in target) #}
        {% set cols_to_add = [] %}
        {% for col_name in source_col_names %}
            {% if col_name not in target_col_names %}
                {% do cols_to_add.append(source_col_map[col_name]) %}
            {% endif %}
        {% endfor %}

        {# Clean up temp table #}
        {% call statement('drop_tmp') %}
            drop table if exists {{ tmp_relation }}
        {% endcall %}

        {# Drop columns that no longer exist in source query #}
        {% if cols_to_drop | length > 0 %}
            {% call statement('drop_columns') %}
                alter table {{ target_relation }} drop column {{ cols_to_drop | join(', ') }}
            {% endcall %}
        {% endif %}

        {# Add new columns from source query #}
        {% if cols_to_add | length > 0 %}
            {% for col in cols_to_add %}
                {% call statement('add_column_' ~ loop.index) %}
                    alter table {{ target_relation }} add column {{ col.name }} {{ col.dtype }}
                {% endcall %}
            {% endfor %}
        {% endif %}

        {# Perform INSERT OVERWRITE to atomically replace all data #}
        {% call statement('main') %}
            insert overwrite into {{ target_relation }}
            {{ sql }}
        {% endcall %}
    {% endif %}

    {# Run post-hooks #}
    {{ run_hooks(post_hooks, inside_transaction=True) }}

    {# Persist documentation to database #}
    {% do persist_docs(target_relation, model) %}

    {{ run_hooks(post_hooks, inside_transaction=False) }}

    {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
