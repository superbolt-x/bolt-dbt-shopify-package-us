{{ config (
    alias = target.database + '_shopify_us_daily_sales_by_order',
    materialized='incremental',
    unique_key='unique_key'
)}}


{%- set sales_channel_exclusion_list = "'"~var("sales_channel_exclusion").split('|')|join("','")~"'" -%}

WITH giftcard_deduction AS 
    (SELECT 
        order_id, 
        CASE WHEN items_count = giftcard_count THEN 'true' ELSE 'false' END as giftcard_only,
        giftcard_deduction
    FROM 
        (SELECT 
            order_id, 
            SUM(quantity) as items_count,
            COALESCE(SUM(CASE WHEN gift_card = 'true' THEN quantity END),0) as giftcard_count,
            COALESCE(SUM(CASE WHEN gift_card = 'true' THEN price * quantity END),0) as giftcard_deduction
        FROM {{ ref('shopify_us_line_items') }}
        GROUP BY 1)
    ),

    orders AS 
    (SELECT 
        order_date as date, 
        order_id, 
        customer_id, 
        customer_order_index,
        gross_revenue - COALESCE(giftcard_deduction,0) as gross_revenue,
        total_discounts,
        discount_rate,
        subtotal_revenue,
        total_tax, 
        shipping_price, 
        total_revenue,
        order_tags
    FROM {{ ref('shopify_us_orders') }}
    LEFT JOIN giftcard_deduction USING(order_id)
    WHERE giftcard_only = 'false'
    AND cancelled_at IS NULL
    AND source_name NOT IN ({{ sales_channel_exclusion_list }})
    AND (order_tags !~* '{{ var("order_tags_keyword_exclusion")}}' OR order_tags IS NULL)
    {% if is_incremental() -%}

    -- this filter will only be applied on an incremental run
    AND order_date >= (select max(date)-90 from {{ this }})

    {% endif %}
    ),

    customers AS 
    (SELECT customer_id, customer_acquisition_date, customer_tags
    FROM {{ ref('shopify_us_customers') }}
    )

SELECT *,
    {{ get_date_parts('date') }},
    date||'_'||order_id as unique_key
FROM orders 
LEFT JOIN customers USING(customer_id)
