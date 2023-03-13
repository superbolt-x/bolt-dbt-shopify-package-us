{%- set schema_name,
        refund_table_name,
        adjustment_table_name,
        line_refund_table_name,
        transaction_table_name
        = 'shopify_raw_us',
        'refund',
        'order_adjustment',
        'order_line_refund',
        'transaction' -%}

{%- set refund_selected_fields = [
    "id",
    "order_id",
    "processed_at"
] -%}

{%- set adjustment_selected_fields = [
    "refund_id",
    "amount",
    "tax_amount",
    "kind"
] -%}

{%- set line_refund_selected_fields = [
    "refund_id",
    "subtotal",
    "total_tax"
] -%}

{%- set transaction_selected_fields = [
    "refund_id",
    "subtotal",
    "total_tax"
] -%}

{%- set refund_raw_tables = dbt_utils.get_relations_by_pattern('shopify_raw_us%', 'refund') -%}
{%- set adjustment_raw_tables = dbt_utils.get_relations_by_pattern('shopify_raw_us%', 'order_adjustment') -%}
{%- set line_refund_raw_tables = dbt_utils.get_relations_by_pattern('shopify_raw_us%', 'order_line_refund') -%}

WITH 
    -- To tackle the signal loss between Fivetran and Shopify transformations
    stellar_signal AS 
    (SELECT _fivetran_synced
    FROM {{ source('shopify_raw_us', 'order') }}
    LIMIT 1
    ),

    refund_raw_data AS 
    ({{ dbt_utils.union_relations(relations = refund_raw_tables) }}),

    refund_staging AS 
    (SELECT 
        
        {% for field in refund_selected_fields -%}
        {{ get_shopify_clean_field(refund_table_name, field)}}
        {%- if not loop.last %},{% endif %}
        {% endfor %}

    FROM refund_raw_data
    ),

    adjustment_raw_data AS 
    ({{ dbt_utils.union_relations(relations = adjustment_raw_tables) }}),

    adjustment_staging AS 
    (SELECT 
        
        {% for field in adjustment_selected_fields -%}
        {{ get_shopify_clean_field(adjustment_table_name, field)}}
        {%- if not loop.last %},{% endif %}
        {% endfor %}

    FROM adjustment_raw_data
    ),

    adjustment AS 
    (SELECT 
        refund_id,
        SUM(CASE WHEN refund_kind = 'refund_discrepancy' THEN refund_amount END) as subtotal_refund,
        SUM(CASE WHEN refund_kind = 'shipping_refund' THEN refund_amount END) as shipping_refund,
        SUM(refund_tax_amount) as tax_refund
    FROM adjustment_staging
    GROUP BY refund_id
    ),

    line_refund_raw_data AS 
    ({{ dbt_utils.union_relations(relations = line_refund_raw_tables) }}),

    line_refund_staging AS 
    (SELECT 
        
        {% for field in line_refund_selected_fields -%}
        {{ get_shopify_clean_field(line_refund_table_name, field)}}
        {%- if not loop.last %},{% endif %}
        {% endfor %}

    FROM line_refund_raw_data
    ),

    line_refund AS 
    (SELECT 
        refund_id, 
        SUM(refund_subtotal) as subtotal_refund,
        SUM(refund_total_tax) as tax_refund
    FROM line_refund_staging
    GROUP BY refund_id
    )

    SELECT order_id, 
        refund_id,
        processed_at as refund_date,
        ABS(COALESCE(SUM(adjustment.subtotal_refund),0)) as subtotal_order_refund,
        COALESCE(SUM(line_refund.subtotal_refund),0) as subtotal_line_refund,
        ABS(COALESCE(SUM(shipping_refund),0)) as shipping_refund,
        ABS(COALESCE(SUM(adjustment.tax_refund),0)) + COALESCE(SUM(line_refund.tax_refund),0) as tax_refund
    FROM refund_staging
    LEFT JOIN adjustment USING(refund_id)
    LEFT JOIN line_refund USING(refund_id)
    GROUP BY order_id, refund_id, refund_date
