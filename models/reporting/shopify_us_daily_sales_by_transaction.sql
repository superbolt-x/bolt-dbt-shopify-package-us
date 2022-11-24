{{ config (
    alias = target.database + '_shopify_us_daily_sales_by_transaction'
)}}

WITH 
    transactions AS 
    (SELECT 
        transaction_date as date,
        order_id, 
        COALESCE(SUM(paid_by_customer),0) as paid_by_customer,
        COALESCE(SUM(refunded),0) as refunded,
        SUM(COALESCE(paid_by_customer,0)-COALESCE(refunded,0)) as net_payment
    FROM {{ ref('shopify_us_transactions') }}
    GROUP BY date, order_id
    ),

    order_customer AS 
    (SELECT order_id, customer_id
    FROM {{ ref('shopify_us_orders') }}
    )

SELECT *,
    {{ get_date_parts('date') }}
FROM transactions
LEFT JOIN order_customer USING(order_id)
