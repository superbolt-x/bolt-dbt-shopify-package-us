{%- set product_selected_fields = [
    "id",
    "title",
    "handle",
    "product_type",
    "status",
    "created_at",
    "updated_at",
    "published_at"
] -%}

{%- set variant_selected_fields = [
    "product_id",
    "id",
    "title",
    "price",
    "sku"
] -%}

{%- set schema_name,
        product_table_name, 
        variant_table_name,
        tag_table_name
        = 'shopify_raw_us', 'product', 'product_variant', 'product_tag'-%}

WITH products AS 
    (SELECT 

        {% for column in product_selected_fields -%}
        {{ get_shopify_clean_field(product_table_name, column)}}
        {%- if not loop.last %},{% endif %}
        {% endfor %}

    FROM {{ source(schema_name, product_table_name) }}),

    variants AS 
    (SELECT 
        
        {% for column in variant_selected_fields -%}
        {{ get_shopify_clean_field(variant_table_name, column)}}
        {%- if not loop.last %},{% endif %}
        {% endfor %}

    FROM {{ source(schema_name, variant_table_name) }})

    {%- set tag_table_exists = check_source_exists(schema_name, tag_table_name) %}
    {%- if tag_table_exists %}

    ,tags AS 
    (SELECT product_id, LISTAGG(value, ', ') WITHIN GROUP (ORDER BY index) as product_tags
    FROM {{ source(schema_name, tag_table_name) }}
    GROUP BY product_id
    )
    {%- endif %}

SELECT *,
    product_id||'_'||variant_id as unique_key
FROM products 
LEFT JOIN variants USING(product_id)
{%- if tag_table_exists %}
LEFT JOIN tags USING(product_id)
{%- endif %}