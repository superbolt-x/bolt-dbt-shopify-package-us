{{ config (
    alias = target.database + '_shopify_us_daily_sales_by_order_line_item',
    materialized='incremental',
    unique_key='unique_key',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
)}}

{#- line-grain fulfillment_date only exists when both fulfillment raw tables are synced (some clients only) -#}
{%- set has_fulfillment = (dbt_utils.get_relations_by_pattern('shopify_raw_us%', 'fulfillment_order_line') | length > 0) and (dbt_utils.get_relations_by_pattern('shopify_raw_us%', 'fulfillment') | length > 0) -%}


WITH orders AS
    (SELECT *
    FROM {{ ref('shopify_us_daily_sales_by_order') }}
    {%- if is_incremental() %}
    -- 30-day lookback, same window as shopify_daily_sales_by_order. Run --full-refresh
    -- periodically for late changes (refunds/cancellations outside the window).
    WHERE date >= (select max(date)-30 from {{ this }})
    {%- endif %}
    ),

    line_items AS 
    (SELECT *
    FROM {{ ref('shopify_us_line_items') }}
    ),

    products AS
    (SELECT product_id, product_type, product_vendor as vendor, product_tags, product_handle, product_status, count(*)
    FROM {{ ref('shopify_us_products') }}
    GROUP BY 1,2,3,4,5,6
    ),

    sales AS 
    (SELECT 
        date,
        cancelled_at,
        order_id, 
        customer_id,
        customer_acquisition_date,
        customer_order_index,
        order_tags, 
        order_line_id,
        product_id,
        variant_id,
        sku,
        product_title,
        variant_title,
        item_title,
        index,
        gift_card,
        price,
        quantity,
        line_items.fulfillment_status as item_fulfillment_status,
        {%- if has_fulfillment %}
        line_items.fulfillment_date as fulfillment_date,
        {%- endif %}
        fulfillable_quantity,
        net_subtotal,
        price * quantity as gross_sales,
        discount_rate,
        (price * quantity) * COALESCE(subtotal_revenue / NULLIF(gross_revenue,0)) as subtotal_sales,
        (price * quantity) * COALESCE(total_revenue / NULLIF(gross_revenue,0)) as total_sales,
        quantity - COALESCE(refund_quantity,0) as net_quantity
    FROM orders 
    LEFT JOIN line_items USING(order_id)
    )

SELECT *,
    date||'_'||order_line_id as unique_key
FROM sales
LEFT JOIN (SELECT product_id, product_type, vendor, product_tags, product_handle, product_status FROM products) USING(product_id) -- vendor ya viene aliaseado desde products
