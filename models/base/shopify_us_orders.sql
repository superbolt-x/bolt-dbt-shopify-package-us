{%- set order_selected_fields = [
    "id",
    "name",
    "customer_id",
    "email",
    "created_at",
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
    "total_price_usd",
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
    "discounted_price"
] -%}

{%- set refund_selected_fields = [
    "id",
    "order_id"
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

WITH orders AS 
    (SELECT 

        {% for field in order_selected_fields -%}
        {{ get_shopify_clean_field(order_table_name, field)}}
        {%- if not loop.last %},{% endif %}
        {% endfor %}

    FROM {{ source(schema_name, order_table_name) }}),

    discount_raw AS 
    (SELECT 
        
        {% for field in discount_selected_fields -%}
        {{ get_shopify_clean_field(discount_table_name, field)}}
        {%- if not loop.last %},{% endif %}
        {% endfor %}

    FROM {{ source(schema_name, discount_table_name) }}
    ),

    discount AS 
    (SELECT order_id, 
        LISTAGG(discount_code, ', ') WITHIN GROUP (ORDER BY discount_code) as discount_code
    FROM discount_raw
    GROUP BY order_id
    ),

    shipping_raw AS 
    (SELECT 
        
        {% for field in shipping_selected_fields -%}
        {{ get_shopify_clean_field(shipping_table_name, field)}}
        {%- if not loop.last %},{% endif %}
        {% endfor %}

    FROM {{ source(schema_name, shipping_table_name) }}
    ),

    shipping AS 
    (SELECT order_id, 
        LISTAGG(shipping_title, ', ') WITHIN GROUP (ORDER BY shipping_title) as shipping_title,
        COALESCE(SUM(shipping_price),0) as shipping_price
    FROM shipping_raw
    GROUP BY order_id
    ),

    tags AS 
    (SELECT order_id, 
        LISTAGG(value, ', ') WITHIN GROUP (ORDER BY index) as order_tags
    FROM {{ source(schema_name, tag_table_name) }}
    GROUP BY order_id
    ),

    refund_raw AS 
    (SELECT 
        
        {% for field in refund_selected_fields -%}
        {{ get_shopify_clean_field(refund_table_name, field)}}
        {%- if not loop.last %},{% endif %}
        {% endfor %}

    FROM {{ source(schema_name, refund_table_name) }}
    ),

    adjustment_raw AS 
    (SELECT 
        
        {% for field in adjustment_selected_fields -%}
        {{ get_shopify_clean_field(adjustment_table_name, field)}}
        {%- if not loop.last %},{% endif %}
        {% endfor %}

    FROM {{ source(schema_name, adjustment_table_name) }}
    ),

    adjustment AS 
    (SELECT 
        refund_id,
        SUM(CASE WHEN refund_kind = 'refund_discrepancy' THEN refund_amount END) as subtotal_refund,
        SUM(CASE WHEN refund_kind = 'shipping_refund' THEN refund_amount END) as shipping_refund,
        SUM(refund_tax_amount) as tax_refund
    FROM adjustment_raw
    GROUP BY refund_id
    ),

    line_refund_raw AS 
    (SELECT 
        
        {% for field in line_refund_selected_fields -%}
        {{ get_shopify_clean_field(line_refund_table_name, field)}}
        {%- if not loop.last %},{% endif %}
        {% endfor %}

    FROM {{ source(schema_name, line_refund_table_name) }}
    ),

    line_refund AS 
    (SELECT 
        refund_id, 
        SUM(refund_subtotal) as subtotal_refund,
        SUM(refund_total_tax) as tax_refund
    FROM line_refund_raw
    GROUP BY refund_id
    ),

    refund AS 
    (SELECT order_id, 
        ABS(COALESCE(SUM(adjustment.subtotal_refund),0)) as subtotal_order_refund,
        COALESCE(SUM(line_refund.subtotal_refund),0) as subtotal_line_refund,
        ABS(COALESCE(SUM(shipping_refund),0)) as shipping_refund,
        ABS(COALESCE(SUM(adjustment.tax_refund),0)) + COALESCE(SUM(line_refund.tax_refund),0) as tax_refund
    FROM refund_raw 
    LEFT JOIN adjustment USING(refund_id)
    LEFT JOIN line_refund USING(refund_id)
    GROUP BY order_id
    )

SELECT *,
    created_at::date as order_date, 
    {{ get_date_parts('order_date') }},
    COALESCE(total_discounts/NULLIF(gross_revenue,0),0) as discount_rate,
    -- include cancelled orders to match Shopify
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_at) as customer_order_index,
    order_id as unique_key
FROM orders 
LEFT JOIN discount USING(order_id)
LEFT JOIN shipping USING(order_id)
LEFT JOIN tags USING(order_id)
LEFT JOIN refund USING(order_id)