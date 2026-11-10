{{ config (
    alias = target.database + '_shopify_us_daily_refunds'
)}}

{%- set sales_channel_exclusion_list = "'"~var("sales_channel_exclusion").split('|')|join("','")~"'" -%}
{%- set sales_channel_inclusion_list = "'"~var("sales_channel_inclusion").split('|')|join("','")~"'" -%}
{%- set shipping_country_exclusion_list = "'"~var("shipping_countries_excluded").split('|')|join("','")~"'" -%}
{%- set shipping_country_inclusion_list = "'"~var("shipping_countries_included").split('|')|join("','")~"'" -%}

WITH

    giftcard_deduction AS 
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

    refunds AS 
    (SELECT 
        refund_date::date as date,
        refund_id,
        order_id, 
        sum(case when giftcard_only = 'true' then 0
             else subtotal_refund - amount_discrepancy_refund 
        end) as subtotal_refund,
        sum(amount_shipping_refund) as shipping_refund,
        sum(total_tax_refund) + sum(tax_amount_discrepancy_refund) + sum(tax_amount_shipping_refund) as tax_refund
    FROM {{ ref('shopify_us_refunds') }}
    LEFT JOIN giftcard_deduction USING(order_id)
    {%- if var('shipping_countries_included') != 'dummy' %}
    WHERE shipping_address_country_code IN ({{ shipping_country_inclusion_list }})
    {%- endif %}
    GROUP BY date, refund_id, order_id
    ),

    order_customer AS 
    (SELECT order_id, customer_id, cancelled_at, customer_order_index
    FROM {{ ref('shopify_us_orders') }}
    WHERE 1=1
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
    )

SELECT *,
    {{ get_date_parts('date') }}
FROM refunds
LEFT JOIN order_customer USING(order_id)
--WHERE cancelled_at is null
