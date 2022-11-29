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
        = 'shopify_raw_us', 'transaction' -%}

WITH raw_table AS 
    (SELECT 

        {% for column in selected_fields -%}
        {{ get_shopify_clean_field(table_name, column)}}
        {%- if not loop.last %},{% endif %}
        {% endfor %}

    FROM {{ source(schema_name, table_name) }}),

    staging AS 
    (SELECT 
        order_id, 
        created_at::date as transaction_date,
        COALESCE(SUM(CASE WHEN kind in ('sale','authorization') THEN transaction_amount END),0) as paid_by_customer,
        COALESCE(SUM(CASE WHEN kind = 'refund' THEN transaction_amount END),0) as refunded
    FROM raw_table
    WHERE status = 'success'
    GROUP BY order_id, transaction_date)

SELECT *,
    order_id||'_'||transaction_date as unique_key
FROM staging