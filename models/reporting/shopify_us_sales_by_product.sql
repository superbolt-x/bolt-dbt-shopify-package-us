{{ config (
    alias = target.database + '_shopify_us_sales_by_product',
    materialized = 'incremental',
    unique_key = 'unique_key',
    incremental_strategy = 'delete+insert',
    on_schema_change = 'append_new_columns'
)}}

{%- set date_granularity_list = ['day','week','month','quarter','year'] -%}

{#- Reprocess from the start of the year containing (max date - 30d). Reading whole
    periods keeps the week/month/quarter/year roll-ups complete; data older than the
    30-day lookback does not change. Run with --full-refresh periodically to refresh
    historical rows affected by late changes outside the window. -#}

WITH
    orders AS
    (SELECT order_id, shipping_discount, subtotal_discount, total_tax, shipping_price, subtotal_revenue, total_revenue, gross_revenue
    FROM {{ ref('shopify_us_daily_sales_by_order') }}
    ),

    line_items AS
    (SELECT *
    FROM {{ ref('shopify_us_daily_sales_by_order_line_item') }}
    {%- if is_incremental() %}
    WHERE date >= date_trunc('year', (select dateadd(day,-30,max(date)) from {{ ref('shopify_us_daily_sales_by_order') }}))::date
    {%- endif %}
    ),
    products AS 
    (SELECT DISTINCT product_id, variant_id, product_title, product_type
    FROM {{ ref('shopify_us_products') }}
    ),

    sales_interm AS 
    (SELECT 
        date,
        cancelled_at,
        order_id, 
        customer_id,
        customer_order_index,
        order_tags, 
        order_line_id,
        product_id,
        variant_id,
        sku,
        item_title,
        index,
        gift_card,
        price,
        quantity,
        price * quantity as gross_sales,
        discount_rate,
        gross_revenue as gross_revenue_order,
        price * quantity as gross_revenue_item,
        (price * quantity) * COALESCE(shipping_discount / NULLIF(gross_revenue,0)) as shipping_discount_item,
        (price * quantity) * COALESCE(subtotal_discount / NULLIF(gross_revenue,0)) as subtotal_discount_item,
        (price * quantity) * COALESCE(total_tax / NULLIF(gross_revenue,0)) as total_tax_item,
        (price * quantity) * COALESCE(shipping_price / NULLIF(gross_revenue,0)) as shipping_price_item,
        (price * quantity) * COALESCE(subtotal_revenue / NULLIF(gross_revenue,0)) as subtotal_revenue_item,
        (price * quantity) * COALESCE(total_revenue / NULLIF(gross_revenue,0)) as total_revenue_item,
        COALESCE(net_quantity,0) as net_quantity
    FROM orders 
    LEFT JOIN line_items USING(order_id)
    ),

    sales AS
    (SELECT 
        date,
        cancelled_at,
        order_id,
        product_title,
        product_type,
        product_id,
        variant_id,
        customer_id, 
        customer_order_index,
        quantity,
        net_quantity,
        gross_revenue_item,
        shipping_discount_item,
        subtotal_discount_item,
        discount_rate,
        subtotal_revenue_item,
        total_tax_item, 
        shipping_price_item, 
        total_revenue_item,
        order_tags
    FROM sales_interm LEFT JOIN products USING(product_id,variant_id)
    ),
    
    {%- for date_granularity in date_granularity_list %}

    refunds_{{date_granularity}} AS 
    (SELECT 
        '{{date_granularity}}' as date_granularity,
        DATE_TRUNC('{{date_granularity}}', date) as dg,
        product_title,
        product_type,
        SUM(COALESCE(subtotal_refund,0)) as subtotal_refund,
        SUM(COALESCE(shipping_refund,0)) as shipping_refund,
        SUM(COALESCE(tax_refund,0)) as tax_refund,
        SUM(COALESCE(subtotal_refund,0)-COALESCE(shipping_refund,0)-COALESCE(tax_refund,0)) as total_refund
 FROM {{ ref('shopify_us_daily_refunds_by_product') }}
    WHERE cancelled_at is null
    {%- if is_incremental() %}
    AND date >= date_trunc('year', (select dateadd(day,-30,max(date)) from {{ ref('shopify_us_daily_sales_by_order') }}))::date
    {%- endif %}
    GROUP BY date_granularity, dg, product_title, product_type
    ),

    sales_{{date_granularity}} AS 
    (SELECT 
        '{{date_granularity}}' as date_granularity,
        DATE_TRUNC('{{date_granularity}}', date) as dg,
        product_title,
        product_type,
        COALESCE(COUNT(DISTINCT(order_id)),0) as orders,
        COALESCE(COUNT(DISTINCT(CASE WHEN customer_order_index = 1 THEN order_id END)),0) as first_orders,
        COALESCE(COUNT(DISTINCT(CASE WHEN customer_order_index > 1 THEN order_id END)),0) as repeat_orders,
        COALESCE(SUM(quantity),0) as quantity,
        COALESCE(SUM(CASE WHEN customer_order_index = 1 THEN quantity END),0) as first_order_quantity,
        COALESCE(SUM(CASE WHEN customer_order_index > 1 THEN quantity END),0) as repeat_order_quantity,
        COALESCE(SUM(net_quantity),0) as net_quantity,
        COALESCE(SUM(CASE WHEN customer_order_index = 1 THEN net_quantity END),0) as first_order_net_quantity,
        COALESCE(SUM(CASE WHEN customer_order_index > 1 THEN net_quantity END),0) as repeat_order_net_quantity,
        COALESCE(SUM(gross_revenue_item),0) as gross_sales,
        COALESCE(SUM(CASE WHEN customer_order_index = 1 THEN gross_revenue_item END),0) as first_order_gross_sales,
        COALESCE(SUM(CASE WHEN customer_order_index > 1 THEN gross_revenue_item END),0) as repeat_order_gross_sales,
        COALESCE(SUM(subtotal_discount_item+shipping_discount_item),0) as discounts,
        COALESCE(SUM(subtotal_discount_item),0) as subtotal_discounts,
        COALESCE(SUM(shipping_discount_item),0) as shipping_discounts,
        COALESCE(SUM(CASE WHEN customer_order_index = 1 THEN subtotal_discount_item+shipping_discount_item END),0) as first_order_discounts,
        COALESCE(SUM(CASE WHEN customer_order_index = 1 THEN subtotal_discount_item END),0) as first_order_subtotal_discounts,
        COALESCE(SUM(CASE WHEN customer_order_index = 1 THEN shipping_discount_item END),0) as first_order_shipping_discounts,
        COALESCE(SUM(CASE WHEN customer_order_index > 1 THEN subtotal_discount_item+shipping_discount_item END),0) as repeat_order_discounts,
        COALESCE(SUM(CASE WHEN customer_order_index > 1 THEN subtotal_discount_item END),0) as repeat_order_subtotal_discounts,
        COALESCE(SUM(CASE WHEN customer_order_index > 1 THEN shipping_discount_item END),0) as repeat_order_shipping_discounts,
        SUM(COALESCE(gross_revenue_item,0) - COALESCE(subtotal_discount_item,0)) as subtotal_sales,
        COALESCE(SUM(CASE WHEN customer_order_index = 1 THEN COALESCE(gross_revenue_item,0) - COALESCE(subtotal_discount_item,0) END),0) as first_order_subtotal_sales,
        COALESCE(SUM(CASE WHEN customer_order_index > 1 THEN COALESCE(gross_revenue_item,0) - COALESCE(subtotal_discount_item,0) END),0) as repeat_order_subtotal_sales,
        COALESCE(SUM(total_tax_item),0) as gross_tax, 
        COALESCE(SUM(shipping_price_item),0) as gross_shipping,
        COALESCE(SUM(subtotal_revenue_item+COALESCE(total_tax_item,0)+COALESCE(shipping_price_item,0)),0) as total_sales,
        COALESCE(SUM(CASE WHEN customer_order_index = 1 THEN subtotal_revenue_item+COALESCE(total_tax_item,0)+COALESCE(shipping_price_item,0) END),0) as first_order_total_sales,
        COALESCE(SUM(CASE WHEN customer_order_index > 1 THEN subtotal_revenue_item+COALESCE(total_tax_item,0)+COALESCE(shipping_price_item,0) END),0) as repeat_order_total_sales
    FROM sales
    WHERE cancelled_at is null
    AND customer_id is not null
    GROUP BY date_granularity, dg, product_title, product_type)
    {%- if not loop.last %},{%- endif %}
    {%- endfor %}

{% for date_granularity in date_granularity_list -%}
SELECT 
    date_granularity,
    dg as date,
    product_title,
    product_type,
    SUM(COALESCE(orders,0)) as orders,
    SUM(COALESCE(first_orders,0)) as first_orders,
    SUM(COALESCE(repeat_orders,0)) as repeat_orders,
    SUM(COALESCE(quantity,0)) as quantity,
    SUM(COALESCE(first_order_quantity,0)) as first_order_quantity,
    SUM(COALESCE(repeat_order_quantity,0)) as repeat_order_quantity,
    SUM(COALESCE(net_quantity,0)) as net_quantity,
    SUM(COALESCE(first_order_net_quantity,0)) as first_order_net_quantity,
    SUM(COALESCE(repeat_order_net_quantity,0)) as repeat_order_net_quantity,
    SUM(COALESCE(gross_sales,0)) as gross_sales,
    SUM(COALESCE(first_order_gross_sales,0)) as first_order_gross_sales,
    SUM(COALESCE(repeat_order_gross_sales,0)) as repeat_order_gross_sales,
    SUM(COALESCE(discounts,0)) as discounts,
    SUM(COALESCE(subtotal_discounts,0)) as subtotal_discounts,
    SUM(COALESCE(shipping_discounts,0)) as shipping_discounts,
    SUM(COALESCE(first_order_discounts,0)) as first_order_discounts,
    SUM(COALESCE(first_order_subtotal_discounts,0)) as first_order_subtotal_discounts,
    SUM(COALESCE(first_order_shipping_discounts,0)) as first_order_shipping_discounts,
    SUM(COALESCE(repeat_order_discounts,0)) as repeat_order_discounts,
    SUM(COALESCE(repeat_order_subtotal_discounts,0)) as repeat_order_subtotal_discounts,
    SUM(COALESCE(repeat_order_shipping_discounts,0)) as repeat_order_shipping_discounts,
    SUM(COALESCE(subtotal_sales,0)) as subtotal_sales,
    SUM(COALESCE(first_order_subtotal_sales,0)) as first_order_subtotal_sales,
    SUM(COALESCE(repeat_order_subtotal_sales,0)) as repeat_order_subtotal_sales,
    SUM(COALESCE(gross_tax,0)) as gross_tax, 
    SUM(COALESCE(gross_shipping,0)) as gross_shipping,
    SUM(COALESCE(total_sales,0)) as total_sales,
    SUM(COALESCE(first_order_total_sales,0)) as first_order_total_sales,
    SUM(COALESCE(repeat_order_total_sales,0)) as repeat_order_total_sales,
    SUM(coalesce(r.subtotal_refund,0)) as subtotal_returns,
    SUM(coalesce(r.shipping_refund,0)) as shipping_returns,
    SUM(coalesce(r.tax_refund,0)) as tax_returns,
    SUM(COALESCE(s.subtotal_sales,0) - coalesce(r.subtotal_refund,0)) as net_sales,
    SUM(COALESCE(s.total_sales,0) - coalesce(r.total_refund,0)) as total_net_sales,
    md5(date_granularity||'_'||dg||'_'||coalesce(product_title,'')||'_'||coalesce(product_type,'')) as unique_key
FROM sales_{{date_granularity}} s
FULL JOIN refunds_{{date_granularity}} r USING(date_granularity, dg, product_title, product_type)
GROUP BY date_granularity, date, product_title, product_type
{% if not loop.last %}UNION ALL
{% endif %}
 {%- endfor %}
