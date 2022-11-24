{%- set schema_name,
        product_table_name, 
        variant_table_name,
        tag_table_name
        = 'shopify_raw_us', 'product', 'product_variant', 'product_tag'-%}

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

{%- set product_raw_tables = dbt_utils.get_relations_by_pattern('shopify_raw_us%', 'product') -%}
{%- set variant_raw_tables = dbt_utils.get_relations_by_pattern('shopify_raw_us%', 'product_variant') -%}
{%- set tag_raw_tables = dbt_utils.get_relations_by_pattern('shopify_raw_us%', 'product_tag') -%}

WITH product_raw_data AS 
    ({{ dbt_utils.union_relations(relations = product_raw_tables) }}),
    
    products AS 
    (SELECT 

        {% for column in product_selected_fields -%}
        {{ get_shopify_clean_field(product_table_name, column)}}
        {%- if not loop.last %},{% endif %}
        {% endfor %}

    FROM product_raw_data
    ),

    variant_raw_data AS 
    ({{ dbt_utils.union_relations(relations = variant_raw_tables) }}),

    variants AS 
    (SELECT 
        
        {% for column in variant_selected_fields -%}
        {{ get_shopify_clean_field(variant_table_name, column)}}
        {%- if not loop.last %},{% endif %}
        {% endfor %}

    FROM variant_raw_data
    ),

    tag_raw_data AS 
    ({{ dbt_utils.union_relations(relations = tag_raw_tables) }}),

    tags AS 
    (SELECT product_id, LISTAGG(value, ', ') WITHIN GROUP (ORDER BY index) as product_tags
    FROM tag_raw_data
    GROUP BY product_id
    )

SELECT *,
    product_id||'_'||variant_id as unique_key
FROM products 
LEFT JOIN variants USING(product_id)
LEFT JOIN tags USING(product_id)
