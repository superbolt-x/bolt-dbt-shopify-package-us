{{ config (
    alias = target.database + '_shopify_us_daily_sales_by_order_line_item',
    materialized='incremental',
    unique_key='unique_key',
    on_schema_change='append_new_columns'
)}}


WITH orders AS
    (SELECT *
    FROM {{ ref('shopify_us_daily_sales_by_order') }}
    ),

    line_items AS
    (SELECT *
    FROM {{ ref('shopify_us_line_items') }}
    ),

    products AS
    (SELECT product_id, product_type, product_tags, product_handle, product_status, count(*)
    FROM {{ ref('shopify_us_products') }}
    GROUP BY 1,2,3,4,5
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
LEFT JOIN (SELECT product_id, product_type, product_tags, product_handle, product_status FROM products) USING(product_id)
