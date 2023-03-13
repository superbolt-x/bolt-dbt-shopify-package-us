{%- set schema_name,
        customer_table_name,
        customer_tag_table_name
        = 'shopify_raw_us', 'customer','customer_tag' -%}
        
{%- set selected_fields = [
    "id",
    "first_name",
    "last_name",    
    "email",
    "created_at"
] -%}

{%- set customer_raw_tables = dbt_utils.get_relations_by_pattern('shopify_raw_us%', 'customer') -%}
{%- set tag_raw_tables = dbt_utils.get_relations_by_pattern('shopify_raw_us%', 'customer_tag') -%}

WITH customer_raw_data AS 
    ({{ dbt_utils.union_relations(relations = customer_raw_tables) }}),

    customers AS 
    (SELECT 

        {% for column in selected_fields -%}
        {{ get_shopify_clean_field(customer_table_name, column)}}
        {%- if not loop.last %},{% endif %}
        {% endfor %}

    FROM customer_raw_data
    )


SELECT *,
    customer_id as unique_key
FROM customers 
