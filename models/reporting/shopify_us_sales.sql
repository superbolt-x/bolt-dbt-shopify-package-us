{{ config (
    alias = target.database + '_shopify_us_sales'
)}}

{%- set date_granularity_list = ['day','week','month','quarter','year'] -%}

WITH 
    {%- for date_granularity in date_granularity_list %}

    transactions_{{date_granularity}} AS 
    (SELECT 
        '{{date_granularity}}' as date_granularity,
        {{date_granularity}} as date,
        SUM(paid_by_customer) as paid_by_customer,
        SUM(refunded) as refunded,
        SUM(net_payment) as net_payment
    FROM {{ ref('shopify_us_daily_sales_by_transaction') }}
    GROUP BY date_granularity, {{date_granularity}}
    ),

    sales_{{date_granularity}} AS 
    (SELECT 
        '{{date_granularity}}' as date_granularity,
        {{date_granularity}} as date,
        COUNT(*) as orders,
        COUNT(CASE WHEN customer_order_index = 1 THEN order_id END) as first_orders,
        COUNT(CASE WHEN customer_order_index > 1 THEN order_id END) as repeat_orders,
        COALESCE(SUM(gross_revenue),0) as gross_sales,
        COALESCE(SUM(CASE WHEN customer_order_index = 1 THEN gross_revenue END),0) as first_order_gross_sales,
        COALESCE(SUM(CASE WHEN customer_order_index > 1 THEN gross_revenue END),0) as repeat_order_gross_sales,
        COALESCE(SUM(total_discounts),0) as discounts,
        COALESCE(SUM(CASE WHEN customer_order_index = 1 THEN total_discounts END),0) as first_order_discounts,
        COALESCE(SUM(CASE WHEN customer_order_index > 1 THEN total_discounts END),0) as repeat_order_discounts,
        SUM(COALESCE(gross_revenue,0) - COALESCE(total_discounts,0)) as subtotal_sales,
        COALESCE(SUM(CASE WHEN customer_order_index = 1 THEN gross_revenue-COALESCE(total_discounts,0) END),0) as first_order_subtotal_sales,
        COALESCE(SUM(CASE WHEN customer_order_index > 1 THEN gross_revenue-COALESCE(total_discounts,0) END),0) as repeat_order_subtotal_sales,
        COALESCE(SUM(total_tax),0) as gross_tax, 
        COALESCE(SUM(shipping_price),0) as gross_shipping,
        COALESCE(SUM(gross_revenue-COALESCE(total_discounts,0)+COALESCE(total_tax,0)+COALESCE(shipping_price,0)),0) as total_sales,
        COALESCE(SUM(CASE WHEN customer_order_index = 1 THEN gross_revenue-COALESCE(total_discounts,0)+COALESCE(total_tax,0)+COALESCE(shipping_price,0) END),0) as first_order_total_sales,
        COALESCE(SUM(CASE WHEN customer_order_index > 1 THEN gross_revenue-COALESCE(total_discounts,0)+COALESCE(total_tax,0)+COALESCE(shipping_price,0) END),0) as repeat_order_total_sales
    FROM {{ ref('shopify_us_daily_sales_by_order') }}
    GROUP BY date_granularity, {{date_granularity}})
    {%- if not loop.last %},{%- endif %}
    {%- endfor %}

{% for date_granularity in date_granularity_list -%}
SELECT 
    s.*, 
    t.refunded as returns,
    s.subtotal_sales - t.refunded as net_sales,
    net_payment
FROM sales_{{date_granularity}} s
LEFT JOIN transactions_{{date_granularity}} t USING(date_granularity, date)
{% if not loop.last %}UNION ALL
{% endif %}

{%- endfor %}
