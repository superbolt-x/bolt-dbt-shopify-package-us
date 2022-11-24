{{ config (
    alias = target.database + '_shopify_us_daily_sales_by_product'
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
    (SELECT DISTINCT product_id, product_title, product_type, product_tags
    FROM {{ ref('shopify_us_products') }}
    ),

    sales AS 
    (SELECT 
        date, 
        product_id,
        COUNT(DISTINCT order_id) as orders,
        COUNT(DISTINCT CASE WHEN customer_order_index = 1 THEN order_id END) as first_orders,
        COUNT(DISTINCT CASE WHEN customer_order_index > 1 THEN order_id END) as repeat_orders,
        SUM(price * quantity) as gross_sales,
        SUM(CASE WHEN customer_order_index = 1 THEN price * quantity END) as first_order_gross_sales,
        SUM(CASE WHEN customer_order_index > 1 THEN price * quantity END) as repeat_order_gross_sales,
        SUM(price * quantity * COALESCE(subtotal_revenue / NULLIF(gross_revenue,0))) as subtotal_sales,
        SUM(CASE WHEN customer_order_index = 1 THEN price * quantity * COALESCE(subtotal_revenue / NULLIF(gross_revenue,0)) END) as first_order_subtotal_sales,
        SUM(CASE WHEN customer_order_index > 1 THEN price * quantity * COALESCE(subtotal_revenue / NULLIF(gross_revenue,0)) END) as repeat_order_subtotal_sales,
        SUM(price * quantity * COALESCE(total_revenue / NULLIF(gross_revenue,0))) as total_sales,
        SUM(CASE WHEN customer_order_index = 1 THEN price * quantity * COALESCE(total_revenue / NULLIF(gross_revenue,0)) END) as first_order_total_sales,
        SUM(CASE WHEN customer_order_index > 1 THEN price * quantity * COALESCE(total_revenue / NULLIF(gross_revenue,0)) END) as repeat_order_total_sales
    FROM orders 
    LEFT JOIN line_items USING(order_id)
    GROUP BY date, product_id
    )

SELECT *,
    date||'_'||product_id as unique_key
FROM sales 
LEFT JOIN products USING(product_id)
