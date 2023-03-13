



{{ config (
    alias = target.database + '_shopify_us_daily_refunds'
)}}

WITH 
    refunds AS 
    (SELECT 
        refund_date::date as date,
        refund_id,
        order_id, 
        sum(case
            when subtotal_order_refund > 0 and subtotal_line_refund+tax_refund+shipping_refund=0 then subtotal_order_refund
            when subtotal_line_refund > 0 and subtotal_order_refund > 0 then -tax_refund
            when shipping_refund>0 and subtotal_order_refund>0 and subtotal_line_refund+tax_refund=0 then -shipping_refund
            when subtotal_line_refund>0 and subtotal_order_refund=0 then subtotal_line_refund
            else 0
        end) as subtotal_refund,
        sum(shipping_refund) as shipping_refund,
        sum(tax_refund) tax_refund
    FROM {{ ref('shopify_us_refunds') }}
    GROUP BY date, refund_id, order_id
    ),

    order_customer AS 
    (SELECT order_id, customer_id, cancelled_at
    FROM {{ ref('shopify_us_orders') }}
    )

SELECT *,
    {{ get_date_parts('date') }}
FROM refunds
LEFT JOIN order_customer USING(order_id)
--WHERE cancelled_at is null
