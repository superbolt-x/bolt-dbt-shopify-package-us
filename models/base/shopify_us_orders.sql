{%- set schema_name,
        order_table_name, 
        discount_table_name,
        shipping_table_name,
        tag_table_name,
        refund_table_name,
        adjustment_table_name,
        line_refund_table_name
        = 'shopify_raw_us', 
        'order', 
        'order_discount_code', 
        'order_shipping_line',
        'order_tag',
        'refund',
        'order_adjustment',
        'order_line_refund' -%}
        
{%- set order_selected_fields = [
    "id",
    "name",
    "customer_id",
    "email",
    "created_at",
    "processed_at",
    "updated_at",
    "cancelled_at",
    "financial_status",
    "fulfillment_status",
    "currency",
    "total_line_items_price",
    "total_discounts",
    "subtotal_price",
    "total_tax",
    "total_price",
    "original_total_duties_set",
    "current_total_discounts",
    "current_subtotal_price",
    "current_total_tax",
    "current_total_duties_set",
    "current_total_price",
    "source_name",
    "referring_site",
    "landing_site_base_url",
    "shipping_address_first_name",
    "shipping_address_last_name",
    "shipping_address_company",
    "shipping_address_phone",
    "shipping_address_address_1",
    "shipping_address_address_2",
    "shipping_address_city",
    "shipping_address_country",
    "shipping_address_country_code",
    "shipping_address_province",
    "shipping_address_province_code",
    "shipping_address_zip",
    "billing_address_first_name",
    "billing_address_last_name",
    "billing_address_company",
    "billing_address_phone",
    "billing_address_address_1",
    "billing_address_address_2",
    "billing_address_city",
    "billing_address_country",
    "billing_address_country_code",
    "billing_address_province",
    "billing_address_province_code",
    "billing_address_zip"
] -%}

{%- set discount_selected_fields = [
    "order_id",
    "code"
] -%}

{%- set shipping_selected_fields = [
    "order_id",
    "title",
    "price",
    "discounted_price"
] -%}

{%- set refund_selected_fields = [
    "id",
    "order_id",
    "processed_at"
] -%}

{%- set adjustment_selected_fields = [
    "refund_id",
    "amount",
    "tax_amount",
    "kind"
] -%}

{%- set line_refund_selected_fields = [
    "refund_id",
    "subtotal",
    "total_tax"
] -%}

{%- set order_raw_tables = dbt_utils.get_relations_by_pattern('shopify_raw_us%', 'order') -%}
{%- set discount_raw_tables = dbt_utils.get_relations_by_pattern('shopify_raw_us%', 'order_discount_code') -%}
{%- set shipping_raw_tables = dbt_utils.get_relations_by_pattern('shopify_raw_us%', 'order_shipping_line') -%}
{%- set tag_raw_tables = dbt_utils.get_relations_by_pattern('shopify_raw_us%', 'order_tag') -%}
{%- set refund_raw_tables = dbt_utils.get_relations_by_pattern('shopify_raw_us%', 'refund') -%}
{%- set adjustment_raw_tables = dbt_utils.get_relations_by_pattern('shopify_raw_us%', 'order_adjustment') -%}
{%- set line_refund_raw_tables = dbt_utils.get_relations_by_pattern('shopify_raw_us%', 'order_line_refund') -%}

WITH 
    -- To tackle the signal loss between Fivetran and Shopify transformations
    stellar_signal AS 
    (SELECT _fivetran_synced
    FROM {{ source('shopify_raw_us', 'order') }}
    LIMIT 1
    ),

    order_raw_data AS 
    ({{ dbt_utils.union_relations(relations = order_raw_tables) }}),

    orders AS 
    (SELECT 

        {% for field in order_selected_fields -%}
        {{ get_shopify_clean_field(order_table_name, field)}}
        {%- if not loop.last %},{% endif %}
        {% endfor %}

    FROM order_raw_data
    ),

    discount_raw_data AS 
    ({{ dbt_utils.union_relations(relations = discount_raw_tables) }}),

    discount_staging AS 
    (SELECT 
        
        {% for field in discount_selected_fields -%}
        {{ get_shopify_clean_field(discount_table_name, field)}}
        {%- if not loop.last %},{% endif %}
        {% endfor %}

    FROM discount_raw_data
    ),

    discount AS 
    (SELECT order_id, 
        LISTAGG(discount_code, ', ') WITHIN GROUP (ORDER BY discount_code) as discount_code
    FROM discount_staging
    GROUP BY order_id
    ),

    shipping_raw_data AS 
    ({{ dbt_utils.union_relations(relations = shipping_raw_tables) }}),

    shipping_staging AS 
    (SELECT 
        
        {% for field in shipping_selected_fields -%}
        {{ get_shopify_clean_field(shipping_table_name, field)}}
        {%- if not loop.last %},{% endif %}
        {% endfor %}

    FROM shipping_raw_data
    ),

    shipping AS 
    (SELECT order_id, 
        LISTAGG(shipping_title, ', ') WITHIN GROUP (ORDER BY shipping_title) as shipping_title,
        COALESCE(SUM(shipping_price),0) as shipping_price,
        COALESCE(SUM(discounted_shipping_price),0) as discounted_shipping_price,
        COALESCE(SUM(shipping_price-discounted_shipping_price),0) as shipping_discounts
    FROM shipping_staging
    GROUP BY order_id
    ),

    tag_raw_data AS 
    ({{ dbt_utils.union_relations(relations = tag_raw_tables) }}),

    tags AS 
    (SELECT order_id, 
        LISTAGG(value, ', ') WITHIN GROUP (ORDER BY index) as order_tags
    FROM tag_raw_data
    GROUP BY order_id
    ),

    refund_raw_data AS 
    ({{ dbt_utils.union_relations(relations = refund_raw_tables) }}),

    refund_staging AS 
    (SELECT 
        
        {% for field in refund_selected_fields -%}
        {{ get_shopify_clean_field(refund_table_name, field)}}
        {%- if not loop.last %},{% endif %}
        {% endfor %}

    FROM refund_raw_data
    ),

    adjustment_raw_data AS 
    ({{ dbt_utils.union_relations(relations = adjustment_raw_tables) }}),

    adjustment_staging AS 
    (SELECT 
        
        {% for field in adjustment_selected_fields -%}
        {{ get_shopify_clean_field(adjustment_table_name, field)}}
        {%- if not loop.last %},{% endif %}
        {% endfor %}

    FROM adjustment_raw_data
    ),

    adjustment AS 
    (SELECT 
        refund_id,
        SUM(CASE WHEN refund_kind = 'refund_discrepancy' THEN refund_amount END) as subtotal_refund,
        SUM(CASE WHEN refund_kind = 'shipping_refund' THEN refund_amount END) as shipping_refund,
        SUM(refund_tax_amount) as tax_refund
    FROM adjustment_staging
    GROUP BY refund_id
    ),

    line_refund_raw_data AS 
    ({{ dbt_utils.union_relations(relations = line_refund_raw_tables) }}),

    line_refund_staging AS 
    (SELECT 
        
        {% for field in line_refund_selected_fields -%}
        {{ get_shopify_clean_field(line_refund_table_name, field)}}
        {%- if not loop.last %},{% endif %}
        {% endfor %}

    FROM line_refund_raw_data
    ),

    line_refund AS 
    (SELECT 
        refund_id, 
        SUM(refund_subtotal) as subtotal_refund,
        SUM(refund_total_tax) as tax_refund
    FROM line_refund_staging
    GROUP BY refund_id
    ),

    refund AS 
    (SELECT order_id, 
        ABS(COALESCE(SUM(adjustment.subtotal_refund),0)) as subtotal_order_refund,
        COALESCE(SUM(line_refund.subtotal_refund),0) as subtotal_line_refund,
        ABS(COALESCE(SUM(shipping_refund),0)) as shipping_refund,
        ABS(COALESCE(SUM(adjustment.tax_refund),0)) + COALESCE(SUM(line_refund.tax_refund),0) as tax_refund
    FROM refund_staging
    LEFT JOIN adjustment USING(refund_id)
    LEFT JOIN line_refund USING(refund_id)
    GROUP BY order_id
    )

SELECT *,
    processed_at::date as order_date, 
    {{ get_date_parts('order_date') }},
    COALESCE(total_discounts/NULLIF(gross_revenue,0),0) as discount_rate,
    -- exclude cancelled orders vs Shopify includes cancelled orders
    MIN(CASE WHEN cancelled_at IS NULL THEN order_date END) OVER (PARTITION BY customer_id) as customer_first_order_date,
    MAX(CASE WHEN cancelled_at IS NULL THEN order_date END) OVER (PARTITION BY customer_id) as customer_last_order_date, 
    CASE WHEN cancelled_at IS NULL THEN ROW_NUMBER() OVER (PARTITION BY customer_id, cancelled_at IS NULL ORDER BY order_date) END as customer_order_index,
    order_id as unique_key
FROM orders 
LEFT JOIN discount USING(order_id)
LEFT JOIN shipping USING(order_id)
LEFT JOIN tags USING(order_id)
LEFT JOIN refund USING(order_id)
