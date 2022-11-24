{%- macro get_shopify_clean_field(table_name, column_name) -%}

 {%- if '_at' in column_name and table_name != 'customer' -%}
        {{column_name}} at time zone '{{var("time_zone")}}' as {{column_name}}

{%- elif table_name == 'order' %}
    {%- if column_name in ('id','name') -%}
        {{column_name}} as order_{{column_name}}


    {%- elif column_name == 'total_line_items_price' -%}
        {{column_name}} as gross_revenue

    {%- elif column_name in ('subtotal_price','total_price','total_price_usd') -%}
        {{column_name}} as {{ column_name | replace("price","revenue") }}

    {%- else -%}
        {{column_name}}

    {%- endif %}

{%- elif table_name == 'order_discount_code' -%}

    {%- if column_name != 'order_id' -%}
        {{column_name}} as discount_{{column_name}}

    {%- else -%}
    {{column_name}}

    {%- endif -%}

{%- elif table_name == 'order_shipping_line' -%}

    {%- if column_name == 'discounted_price' -%}
        {{column_name}} as shipping_price

    {%- elif column_name != 'order_id' -%}
        {{column_name}} as shipping_{{column_name}}

    {%- else -%}
    {{column_name}}

    {%- endif -%}

{%- elif table_name == 'order_adjustment' -%}
    {%- if not 'id' in column_name -%}
        {{column_name}} as refund_{{column_name}}

    {%- else -%}
    {{column_name}}

    {%- endif -%}

{%- elif table_name == 'transaction' -%}

    {%- if column_name in ("id","amount") -%}
        {{column_name}} as transaction_{{column_name}}

    {%- else -%}
    {{column_name}}

    {%- endif -%}

{%- elif table_name == 'order_line' -%}
    {%- if column_name == 'id' -%}
        {{column_name}} as order_line_id
    
    {%- elif column_name == 'name' -%}
        {{column_name}} as item_title

    {%- elif column_name == 'title' -%}
        {{column_name}} as product_title

    {%- else -%}
    {{column_name}}

    {%- endif -%}

{%- elif table_name == 'order_line_refund' -%}
    {%- if column_name in ('quantity','subtotal','total_tax') -%}
        {{column_name}} as refund_{{column_name}}

    {%- else -%}
    {{column_name}}

    {%- endif -%}

{%- elif table_name == 'refund' -%}
    {%- if column_name == 'id' -%}
        {{column_name}} as refund_id

    {%- else -%}
    {{column_name}}

    {%- endif -%}

{%- elif table_name == 'product' -%}
    {%- if not 'product' in column_name -%}
    {{column_name}} as product_{{column_name}}

    {%- else -%}
    {{column_name}}

    {%- endif -%}

{%- elif table_name == 'product_variant' -%}
    {%- if not 'product' in column_name -%}
    {{column_name}} as variant_{{column_name}}

    {%- else -%}
    {{column_name}}

    {%- endif -%}

{%- elif table_name == 'customer' -%}

    {%- if column_name == 'id' -%}
        {{column_name}} as customer_id
    
    {%- elif column_name == 'created_at' -%}
        ({{column_name}} at time zone '{{var("time_zone")}}')::date as customer_acquisition_date

    {%- else -%}
    {{column_name}}

    {%- endif -%}
 
{%- else -%}
    {{column_name}}

{%- endif -%}
  
{%- endmacro -%}
