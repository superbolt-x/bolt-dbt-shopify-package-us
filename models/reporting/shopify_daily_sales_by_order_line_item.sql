{{ config (
    alias = target.database + '_shopify_daily_sales_by_order_line_item',
    materialized='incremental',
    unique_key='unique_key'
)}}


WITH orders AS 
    (SELECT *
    FROM {{ ref('shopify_daily_sales_by_order') }}
    {% if is_incremental() -%}

    -- this filter will only be applied on an incremental run
    WHERE date >= (select max(date)-90 from {{ this }})

    {% endif %}
    ),

    line_items AS 
    (SELECT *
    FROM {{ ref('shopify_line_items') }}
    ),

    products AS 
    (SELECT product_id, variant_id, product_type, product_tags
    FROM {{ ref('shopify_products') }}
    ),
    
    customers AS 
    (SELECT customer_id, customer_acquisition_date, customer_tags
    FROM {{ ref('shopify_customers') }}
    ),

    sales AS 
    (SELECT 
        date, 
        order_id, 
        customer_id,
        customer_order_index,
        order_tags, 
        order_line_id,
        product_id,
        variant_id,
        sku,
        product_title,
        variant_title,
        item_title,
        gift_card,
        price,
        quantity,
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
LEFT JOIN products USING(product_id, variant_id)
LEFT JOIN customers USING(customer_id)