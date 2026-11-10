{#- One row per order: its currency, processed date and the factor converting its amounts to var shopify_currency.
    With no target currency set, the factor is 1 and the model is a plain order list. -#}

{%- set order_raw_tables = dbt_utils.get_relations_by_pattern('shopify_raw_us%', 'order') -%}
{%- set target = shopify_target_currency() -%}

WITH order_raw_data AS
    ({{ dbt_utils.union_relations(relations = order_raw_tables) }}),

    orders AS
    (SELECT
        id as order_id,
        currency as order_currency,
        (processed_at at time zone '{{ var("time_zone") }}')::date as order_date
    FROM order_raw_data
    )

SELECT
    order_id as fx_order_id,
    order_currency,
    order_date,
    {%- if shopify_fx_enabled() %}
    CASE WHEN order_currency = '{{ target }}' OR rate_from.conversion_rate IS NULL OR rate_to.conversion_rate IS NULL THEN 1.0
         ELSE rate_to.conversion_rate::float / NULLIF(rate_from.conversion_rate::float, 0) END::float as fx_factor
    {%- else %}
    1.0::float as fx_factor
    {%- endif %}
FROM orders
{%- if shopify_fx_enabled() %}
LEFT JOIN utilities.currency rate_from ON rate_from.date = orders.order_date AND rate_from.currency = orders.order_currency
LEFT JOIN utilities.currency rate_to ON rate_to.date = orders.order_date AND rate_to.currency = '{{ target }}'
{%- endif %}
