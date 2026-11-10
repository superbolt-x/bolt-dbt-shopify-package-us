{%- set schema_name,
        item_table_name, 
        item_fund_table_name
        = 'shopify_raw_us', 'order_line', 'order_line_refund' -%}
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
{#- Fulfillment is only synced for some clients. Only wire it in when both raw tables exist. -#}
{%- set fulfillment_selected_fields = [
    "id",
    "created_at",
    "status"
] -%}
{%- set order_line_raw_tables = dbt_utils.get_relations_by_pattern('shopify_raw_us%', 'order_line') -%}
{%- set fulfillment_order_line_raw_tables = dbt_utils.get_relations_by_pattern('shopify_raw_us%', 'fulfillment_order_line') -%}
{%- set fulfillment_raw_tables = dbt_utils.get_relations_by_pattern('shopify_raw_us%', 'fulfillment') -%}
{%- set has_fulfillment = (fulfillment_order_line_raw_tables | length > 0) and (fulfillment_raw_tables | length > 0) -%}
WITH order_line_raw_data AS 
    ({{ dbt_utils.union_relations(relations = order_line_raw_tables) }}),
    items AS 
    (SELECT 
        {% for column in item_selected_fields -%}
        {{ shopify_fx_line_field(get_shopify_clean_field(item_table_name, column), column) }}
        {%- if not loop.last %},{% endif %}
        {% endfor %}
    FROM order_line_raw_data
    {{ shopify_fx_join('ON fx.fx_order_id = order_line_raw_data.order_id') }}
    ),
    order_line_refund_raw_data AS 
    ({{ get_shopify_refund_line_items() }}),
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
        SUM(refund_subtotal){{ shopify_fx_agg() }} as refund_subtotal
    FROM refund_raw
    {%- if shopify_fx_enabled() %}
    LEFT JOIN (SELECT id as order_line_id, order_id FROM order_line_raw_data) fx_line USING(order_line_id)
    {{ shopify_fx_join('ON fx.fx_order_id = fx_line.order_id') }}
    {%- endif %}
    GROUP BY order_line_id
    )

    {%- if has_fulfillment %}
    ,fulfillment_order_line_raw_data AS
    ({{ dbt_utils.union_relations(relations = fulfillment_order_line_raw_tables) }}),

    fulfillment_raw_data AS
    ({{ dbt_utils.union_relations(relations = fulfillment_raw_tables) }}),

    fulfillment_staging AS
    (SELECT
        {% for field in fulfillment_selected_fields -%}
        {{ get_shopify_clean_field('fulfillment', field) }}
        {%- if not loop.last %},{% endif %}
        {% endfor %}
    FROM fulfillment_raw_data
    ),

    -- line-grain fulfillment date = first successful fulfillment containing that line
    fulfillment_lines AS
    (SELECT
        fol.order_line_id,
        MIN(CASE WHEN fs.status = 'success' THEN fs.created_at END)::date as fulfillment_date
    FROM fulfillment_order_line_raw_data fol
    JOIN fulfillment_staging fs ON fol.fulfillment_id = fs.id
    GROUP BY 1
    )
    {%- endif %}

SELECT *,
    quantity - refund_quantity as net_quantity,
    price * quantity - refund_subtotal as net_subtotal,
    order_line_id as unique_key
FROM items
LEFT JOIN refund USING(order_line_id)
{%- if has_fulfillment %}
LEFT JOIN fulfillment_lines USING(order_line_id)
{%- endif %}
