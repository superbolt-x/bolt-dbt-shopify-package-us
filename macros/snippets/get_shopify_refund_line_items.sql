{#-
    Shopify's GraphQL Admin API schema renamed `order_line_refund` to
    `refund_line_item` and moved the money fields into `*_set_shop_amount`.
    Connectors created before the switch still land the legacy shape.

    This macro emits a SELECT that normalises BOTH shapes to the legacy column
    names, so downstream models (and get_shopify_clean_field) stay unchanged.
    Handles legacy-only, graphql-only, and mixed multi-store projects.

    NOTE: get_relations_by_pattern runs a query, so during dbt's parse phase
    (execute == False) both lists come back empty and this macro renders to an
    empty string. That is expected -- the real relations are resolved on the
    run phase. Every check below must therefore be guarded by `execute`.
-#}

{%- macro get_shopify_refund_line_items() -%}

{%- set legacy_relations  = dbt_utils.get_relations_by_pattern('shopify_raw_us%', 'order_line_refund') -%}
{%- set graphql_relations = dbt_utils.get_relations_by_pattern('shopify_raw_us%', 'refund_line_item')  -%}

{%- if execute and legacy_relations | length == 0 and graphql_relations | length == 0 -%}
    {{ exceptions.raise_compiler_error(
        "No refund line item table found in shopify_raw_us%. Looked for 'order_line_refund' (legacy) and 'refund_line_item' (GraphQL). Check that the Fivetran sync has landed."
    ) }}
{%- endif -%}

{%- if legacy_relations | length > 0 %}
    SELECT
        id,
        order_line_id,
        refund_id,
        location_id,
        restock_type,
        quantity,
        subtotal,
        total_tax,
        _fivetran_synced
    FROM ({{ dbt_utils.union_relations(relations = legacy_relations) }}) AS legacy_refund_line
{%- endif %}

{%- if legacy_relations | length > 0 and graphql_relations | length > 0 %}
    UNION ALL
{%- endif %}

{%- if graphql_relations | length > 0 %}
    SELECT
        id,
        order_line_id,
        refund_id,
        location_id,
        restock_type,
        quantity,
        subtotal_set_shop_amount  AS subtotal,
        total_tax_set_shop_amount AS total_tax,
        _fivetran_synced
    FROM ({{ dbt_utils.union_relations(relations = graphql_relations) }}) AS graphql_refund_line
{%- endif %}

{%- endmacro -%}
