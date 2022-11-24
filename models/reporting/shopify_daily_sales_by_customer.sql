{{ config (
    alias = target.database + '_shopify_daily_sales_by_customer'
)}}

WITH orders AS 
    (SELECT *
    FROM {{ ref('shopify_daily_sales_by_order') }}
    ),

    refunds AS 
    (SELECT date, 
        customer_id, 
        COALESCE(SUM(refunded),0) as returns,
        COALESCE(SUM(net_payment),0) as net_payment
    FROM {{ ref('shopify_daily_sales_by_transaction') }}
    GROUP BY date, customer_id
    ),

    customers AS 
    (SELECT customer_id, customer_acquisition_date, customer_tags
    FROM {{ ref('shopify_customers') }} 
    ),

    sales AS 
    (SELECT *, 
        subtotal_sales - COALESCE(returns,0) as net_sales
    FROM 
        (SELECT 
            date, 
            customer_id, 
            COUNT(order_id) as orders,
            COUNT(CASE WHEN customer_order_index = 1 THEN order_id END) as first_orders,
            COUNT(CASE WHEN customer_order_index > 1 THEN order_id END) as repeat_orders,
            SUM(gross_revenue) as gross_sales,
            COALESCE(SUM(CASE WHEN customer_order_index = 1 THEN gross_revenue END),0) as first_order_gross_sales,
            COALESCE(SUM(CASE WHEN customer_order_index > 1 THEN gross_revenue END),0) as repeat_order_gross_sales,
            SUM(subtotal_revenue) as subtotal_sales,
            COALESCE(SUM(CASE WHEN customer_order_index = 1 THEN gross_revenue-COALESCE(total_discounts,0) END),0) as first_order_subtotal_sales,
            COALESCE(SUM(CASE WHEN customer_order_index > 1 THEN gross_revenue-COALESCE(total_discounts,0) END),0) as repeat_order_subtotal_sales,
            SUM(total_revenue) as total_revenue,
            COALESCE(SUM(CASE WHEN customer_order_index = 1 THEN gross_revenue-COALESCE(total_discounts,0)+COALESCE(total_tax,0)+COALESCE(shipping_price,0) END),0) as first_order_total_sales,
            COALESCE(SUM(CASE WHEN customer_order_index > 1 THEN gross_revenue-COALESCE(total_discounts,0)+COALESCE(total_tax,0)+COALESCE(shipping_price,0) END),0) as repeat_order_total_sales
        FROM orders
        GROUP BY date, customer_id)
    LEFT JOIN refunds USING(date, customer_id)
    )

SELECT *,
    date||'_'||customer_id as unique_key
FROM sales 
LEFT JOIN customers USING(customer_id)