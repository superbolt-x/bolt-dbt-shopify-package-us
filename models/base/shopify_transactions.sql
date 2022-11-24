{%- set selected_fields = [
    "id",
    "order_id",
    "refund_id",
    "amount",
    "created_at",
    "processed_at",
    "message",
    "kind",
    "status"
] -%}

{%- set schema_name,
        table_name
        = 'shopify_raw', 'transaction' -%}

{%- set raw_tables = dbt_utils.get_relations_by_pattern('shopify_raw%', 'transaction') -%}

WITH raw_data AS 
    ({{ dbt_utils.union_relations(relations = raw_tables) }}
    ),
    
    staging AS 
    (SELECT 

        {% for column in selected_fields -%}
        {{ get_shopify_clean_field(table_name, column)}}
        {%- if not loop.last %},{% endif %}
        {% endfor %}

    FROM raw_data
    ),

    transactions AS 
    (SELECT 
        order_id, 
        created_at::date as transaction_date,
        COALESCE(SUM(CASE WHEN kind in ('sale','authorization') THEN transaction_amount END),0) as paid_by_customer,
        COALESCE(SUM(CASE WHEN kind = 'refund' THEN transaction_amount END),0) as refunded
    FROM staging
    WHERE status = 'success'
    GROUP BY order_id, transaction_date)

SELECT *,
    order_id||'_'||transaction_date as unique_key
FROM transactions