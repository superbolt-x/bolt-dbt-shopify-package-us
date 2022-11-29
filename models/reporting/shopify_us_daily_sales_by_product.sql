{{ config (
    alias = target.database + '_shopify_us_daily_sales_by_product'
)}}


{%- set schema_name,
        product_tag_table_name
        = 'shopify_raw_us', 'product_tag'-%}

WITH orders AS 
    (SELECT *
    FROM {{ ref('shopify_us_daily_sales_by_order') }}
    ),

    line_items AS 
    (SELECT *
    FROM {{ ref('shopify_us_line_items') }}
    ),

    {% set product_tag_table_exists = check_source_exists(schema_name, product_tag_table_name) -%}
    products AS 
    (SELECT DISTINCT product_id, product_title, product_type
        {%- if product_tag_table_exists %}
        , product_tags
        {%- endif %}
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
