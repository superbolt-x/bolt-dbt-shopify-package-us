{{ config (
    alias = target.database + '_shopify_us_cohort_analysis'
)}}

{%- set date_granularity_list = ['week','month','quarter'] -%}

WITH 
    orders AS (
      SELECT order_id, date AS order_date, subtotal_revenue, customer_id, customer_acquisition_date::date AS customer_acquisition_day, customer_order_index
      FROM {{ ref('shopify_us_daily_sales_by_order') }}
    ),
    
    {%- for date_granularity in date_granularity_list %}
    
    cohort_size_{{date_granularity}} AS(
      SELECT 
        DATE_TRUNC('{{date_granularity}}', customer_acquisition_day::date) AS cohort,
        '{{date_granularity}}' as date_granularity,
        COUNT(DISTINCT customer_id) AS new_customers
      FROM orders 
      GROUP BY cohort, date_granularity 
      ORDER BY cohort, date_granularity
    ),

    cohort_arpu_and_repeat_rate_{{date_granularity}} AS(
      SELECT 
        DATE_TRUNC( '{{date_granularity}}', customer_acquisition_day) AS cohort,
        '{{date_granularity}}' as date_granularity,
        FLOOR(DATEDIFF(day, customer_acquisition_day, order_date)/CASE '{{date_granularity}}' WHEN 'week' THEN 7 WHEN 'month' THEN 30.42 WHEN 'quarter' THEN 91.25 END)+1 AS retention,
        COALESCE(SUM(SUM(subtotal_revenue)) over (partition BY cohort ORDER BY retention rows between unbounded preceding AND current row)::decimal
        /NULLIF(SUM(COUNT(CASE WHEN customer_order_index = 1 THEN customer_id END)) over (partition BY cohort),0),0) AS arpu,
        COALESCE(SUM(COUNT(CASE WHEN customer_order_index = 2 THEN customer_id END)) over (partition by cohort ORDER BY retention rows unbounded preceding)::float
        /NULLIF(SUM(COUNT(CASE WHEN customer_order_index = 1 THEN customer_id END)) over (partition by cohort),0),0) AS repeat_rate
    FROM orders 
    WHERE 1=1
    AND cohort IS NOT NULL
    GROUP BY cohort, date_granularity, retention 
    ORDER BY cohort, date_granularity, retention
    ){%- if not loop.last %},{%- endif %}
    {%- endfor %}

{% for date_granularity in date_granularity_list -%}
SELECT *
    FROM cohort_arpu_and_repeat_rate_{{date_granularity}} LEFT JOIN cohort_size_{{date_granularity}} USING(cohort, date_granularity)
{% if not loop.last %}UNION ALL
{% endif %}

{%- endfor %}
