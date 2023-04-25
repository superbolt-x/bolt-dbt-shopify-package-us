{{ config (
    alias = target.database + '_shopify_us_daily_refunds'
)}}

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
