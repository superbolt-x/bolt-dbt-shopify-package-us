{%- set schema_name,
        item_table_name, 
        item_fund_table_name
        = 'shopify_raw', 'order_line', 'order_line_refund' -%}

{%- set item_selected_fields = [
    "order_id",
    "id",
    "product_id",
    "variant_id",
    "title",
    "variant_title",
    "name",
    "price",
    "quantity",
    "sku",
    "fulfillable_quantity",
    "fulfillment_status",
    "gift_card",
    "index"

] -%}

{%- set item_refund_selected_fields = [
    "order_line_id",
    "refund_id",
    "quantity",
    "subtotal"
] -%}

{%- set order_line_raw_tables = dbt_utils.get_relations_by_pattern('shopify_raw%', 'order_line') -%}
{%- set order_line_refund_raw_tables = dbt_utils.get_relations_by_pattern('shopify_raw%', 'order_line_refund') -%}

WITH order_line_raw_data AS 
    ({{ dbt_utils.union_relations(relations = order_line_raw_tables) }}),

    items AS 
    (SELECT 

        {% for column in item_selected_fields -%}
        {{ get_shopify_clean_field(item_table_name, column)}}
        {%- if not loop.last %},{% endif %}
        {% endfor %}

    FROM order_line_raw_data
    ),

    order_line_refund_raw_data AS 
    ({{ dbt_utils.union_relations(relations = order_line_refund_raw_tables) }}),

    refund_raw AS 
    (SELECT 
        
        {% for column in item_refund_selected_fields -%}
        {{ get_shopify_clean_field(item_fund_table_name, column)}}
        {%- if not loop.last %},{% endif %}
        {% endfor %}

    FROM order_line_refund_raw_data
    ),

    refund AS 
    (SELECT 
        order_line_id,
        SUM(refund_quantity) as refund_quantity,
        SUM(refund_subtotal) as refund_subtotal
    FROM refund_raw
    GROUP BY order_line_id
    )

SELECT *,
    quantity - refund_quantity as net_quantity,
    price * quantity - refund_subtotal as net_subtotal,
    order_line_id as unique_key
FROM items 
LEFT JOIN refund USING(order_line_id)
