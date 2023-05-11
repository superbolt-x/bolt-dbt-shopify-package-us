{{ config (
    alias = target.database + '_shopify_us_sales'
)}}

{%- set date_granularity_list = ['day','week','month','quarter','year'] -%}

WITH 
    {%- for date_granularity in date_granularity_list %}

    refunds_{{date_granularity}} AS 
    (SELECT 
        '{{date_granularity}}' as date_granularity,
        {{date_granularity}} as date,
        SUM(COALESCE(subtotal_refund,0)) as subtotal_refund,
        SUM(COALESCE(shipping_refund,0)) as shipping_refund,
        SUM(COALESCE(tax_refund,0)) as tax_refund,
        SUM(COALESCE(subtotal_refund,0)-COALESCE(shipping_refund,0)-COALESCE(tax_refund,0)) as total_refund
    FROM {{ ref('shopify_us_daily_refunds') }}
    WHERE cancelled_at is null
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
        COALESCE(SUM(subtotal_discount+shipping_discount),0) as discounts,
        COALESCE(SUM(subtotal_discount),0) as subtotal_discounts,
        COALESCE(SUM(shipping_discount),0) as shipping_discounts,
        COALESCE(SUM(CASE WHEN customer_order_index = 1 THEN subtotal_discount+shipping_discount END),0) as first_order_discounts,
        COALESCE(SUM(CASE WHEN customer_order_index = 1 THEN subtotal_discount END),0) as first_order_subtotal_discounts,
        COALESCE(SUM(CASE WHEN customer_order_index = 1 THEN shipping_discount END),0) as first_order_shipping_discounts,
        COALESCE(SUM(CASE WHEN customer_order_index > 1 THEN subtotal_discount+shipping_discount END),0) as repeat_order_discounts,
        COALESCE(SUM(CASE WHEN customer_order_index > 1 THEN subtotal_discount END),0) as repeat_order_subtotal_discounts,
        COALESCE(SUM(CASE WHEN customer_order_index > 1 THEN shipping_discount END),0) as repeat_order_shipping_discounts,
        SUM(COALESCE(gross_revenue,0) - COALESCE(subtotal_discount,0)) as subtotal_sales,
        COALESCE(SUM(CASE WHEN customer_order_index = 1 THEN COALESCE(gross_revenue,0) - COALESCE(subtotal_discount,0) END),0) as first_order_subtotal_sales,
        COALESCE(SUM(CASE WHEN customer_order_index > 1 THEN COALESCE(gross_revenue,0) - COALESCE(subtotal_discount,0) END),0) as repeat_order_subtotal_sales,
        COALESCE(SUM(total_tax),0) as gross_tax, 
        COALESCE(SUM(shipping_price),0) as gross_shipping,
        COALESCE(SUM(subtotal_revenue+COALESCE(total_tax,0)+COALESCE(shipping_price,0)),0) as total_sales,
        COALESCE(SUM(CASE WHEN customer_order_index = 1 THEN subtotal_revenue+COALESCE(total_tax,0)+COALESCE(shipping_price,0) END),0) as first_order_total_sales,
        COALESCE(SUM(CASE WHEN customer_order_index > 1 THEN subtotal_revenue+COALESCE(total_tax,0)+COALESCE(shipping_price,0) END),0) as repeat_order_total_sales
    FROM {{ ref('shopify_us_daily_sales_by_order') }}
    WHERE cancelled_at is null
    GROUP BY date_granularity, {{date_granularity}})
    {%- if not loop.last %},{%- endif %}
    {%- endfor %}

{% for date_granularity in date_granularity_list -%}
SELECT 
    s.*, 
    coalesce(r.subtotal_refund,0) as subtotal_returns,
    coalesce(r.shipping_refund,0) as shipping_returns,
    coalesce(r.tax_refund,0) as tax_returns,
    s.subtotal_sales - coalesce(r.subtotal_refund,0) as net_sales,
    s.total_sales - coalesce(r.total_refund,0) as total_net_sales
FROM sales_{{date_granularity}} s
LEFT JOIN refunds_{{date_granularity}} r USING(date_granularity, date)
{% if not loop.last %}UNION ALL
{% endif %}

{%- endfor %}
