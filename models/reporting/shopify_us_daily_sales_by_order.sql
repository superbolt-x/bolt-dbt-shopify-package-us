{{ config (
    alias = target.database + '_shopify_us_daily_sales_by_order',
    materialized='incremental',
    unique_key='unique_key',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
)}}


{#- fulfillment_date only exists downstream when the raw fulfillment table is synced (some clients only) -#}
{%- set has_fulfillment = dbt_utils.get_relations_by_pattern('shopify_raw_us%', 'fulfillment') | length > 0 -%}

{%- set sales_channel_exclusion_list = "'"~var("sales_channel_exclusion").split('|')|join("','")~"'" -%}
{%- set sales_channel_inclusion_list = "'"~var("sales_channel_inclusion").split('|')|join("','")~"'" -%}
{%- set shipping_country_exclusion_list = "'"~var("shipping_countries_excluded").split('|')|join("','")~"'" -%}
{%- set shipping_country_inclusion_list = "'"~var("shipping_countries_included").split('|')|join("','")~"'" -%}

WITH giftcard_deduction AS
    (SELECT
        order_id,
        CASE WHEN items_count = giftcard_count THEN 'true' ELSE 'false' END as giftcard_only,
        giftcard_deduction
    FROM
        (SELECT
            order_id,
            SUM(quantity) as items_count,
            COALESCE(SUM(CASE WHEN gift_card is true THEN quantity END),0) as giftcard_count,
            COALESCE(SUM(CASE WHEN gift_card is true THEN price * quantity END),0) as giftcard_deduction
        FROM {{ ref('shopify_us_line_items') }}
        GROUP BY 1)
    ),

    orders AS
    (SELECT
        order_date as date,
        cancelled_at::date as cancelled_at,
        customer_first_order_date as customer_acquisition_date,
        order_id,
        customer_id,
        customer_order_index,
        gross_revenue - COALESCE(giftcard_deduction,0) as gross_revenue,
        total_discounts-gross_revenue+subtotal_revenue as shipping_discount,
        gross_revenue-subtotal_revenue as subtotal_discount,
        discount_rate,
        subtotal_revenue,
        current_subtotal_price,
        total_tax,
        shipping_price,
        total_revenue,
        order_tags,
        order_name,
        email,
        financial_status,
        fulfillment_status,
        currency,
        source_name,
        referring_site,
        landing_site_base_url,
        shipping_address_country,
        shipping_address_country_code,
        shipping_address_province,
        shipping_address_city,
        shipping_address_zip,
        billing_address_first_name,
        billing_address_last_name,
        billing_address_phone,
        billing_address_address_1,
        billing_address_city,
        billing_address_country_code,
        billing_address_province,
        billing_address_zip,
        discount_code,
        created_at,
        processed_at,
        customer_last_order_date
        {%- if has_fulfillment %},
        fulfillment_date
        {%- endif %}

    FROM {{ ref('shopify_us_orders') }}
    LEFT JOIN giftcard_deduction USING(order_id)
    WHERE giftcard_only = 'false'
    --AND cancelled_at IS NULL
    AND (order_tags !~* '{{ var("order_tags_keyword_exclusion")}}' OR order_tags IS NULL)
    AND (email !~* '{{ var("email_address_exclusion")}}' OR email IS NULL)
    {%- if var('financial_status') != 'dummy' %}
    AND financial_status ~* '{{ var("financial_status")}}'
    {%- endif %}
    {%- if var('order_test') != 'dummy' %}
    AND "test" !~* '{{ var("order_test")}}'
    {%- endif %}
    {%- if var('shipping_countries_excluded') != 'dummy' %}
    AND (shipping_address_country_code NOT IN ({{ shipping_country_exclusion_list }}) OR shipping_address_country_code IS NULL)
    {%- endif %}
    {%- if var('shipping_countries_included') != 'dummy' %}
    AND shipping_address_country_code IN ({{ shipping_country_inclusion_list }})
    {%- endif %}
    {%- if var('sales_channel_exclusion') != 'dummy' %}
    AND (source_name NOT IN ({{ sales_channel_exclusion_list }}) OR source_name IS NULL)
    {%- endif %}
    {%- if var('sales_channel_inclusion') != 'dummy' %}
    AND source_name IN ({{ sales_channel_inclusion_list }})
    {%- endif %}
    {%- if is_incremental() %}
    -- 30-day lookback: order-level fields (financial_status, cancellations, refunds) can
    -- still change within this window. Run --full-refresh periodically for late changes.
    AND order_date >= (select max(date)-30 from {{ this }})
    {%- endif %}
    )

SELECT *,
    {{ get_date_parts('date') }},
    date||'_'||order_id as unique_key
FROM orders
