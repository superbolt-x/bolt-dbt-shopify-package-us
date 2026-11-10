{#-
    Optional currency conversion. Off unless the project var `shopify_currency` is set to a currency code.
    Set it in the client's dbt_project.yml (regional packages read sho_<region>_currency instead).
    Every order amount is converted from the order's own currency to that target currency, using
    utilities.currency (conversion_rate = units of currency per 1 USD) at the order's processed date.
    A currency with no rate on that date (or an unknown value) is left unconverted.
-#}

{%- macro shopify_target_currency() -%}
{{- var('sho_us_currency', 'none') | string | upper -}}
{%- endmacro -%}

{%- macro shopify_fx_enabled() -%}
{{- return(shopify_target_currency() | trim not in ['NONE', '']) -}}
{%- endmacro -%}

{#- LEFT JOIN to the per-order conversion factor, aliased fx. `on_clause` is e.g. "USING(order_id)" -#}
{%- macro shopify_fx_join(on_clause) -%}
{%- if shopify_fx_enabled() -%}
LEFT JOIN {{ ref('shopify_us_order_fx') }} fx {{ on_clause }}
{%- endif -%}
{%- endmacro -%}

{#- multiply a row-level amount / an aggregated amount by the order's factor -#}
{%- macro shopify_fx_row() -%}{%- if shopify_fx_enabled() %} * fx.fx_factor{%- endif -%}{%- endmacro -%}
{%- macro shopify_fx_agg() -%}{%- if shopify_fx_enabled() %} * MAX(fx.fx_factor){%- endif -%}{%- endmacro -%}

{#- wrap an already-cleaned order field ("col as alias") when it is an amount; 'currency' becomes the target -#}
{%- macro shopify_fx_line_field(expr, field) -%}
{%- if shopify_fx_enabled() and field == 'price' -%}
{%- set parts = expr.split(' as ') -%}
(({{ parts[0] | trim }})::float * fx.fx_factor) as {{ (parts[1] | trim) if parts | length > 1 else field }}
{%- else -%}
{{ expr }}
{%- endif -%}
{%- endmacro -%}

{%- macro shopify_fx_order_field(expr, field) -%}
{%- set money_fields = ['total_line_items_price','total_discounts','subtotal_price','total_price','total_tax',
                        'current_total_discounts','current_subtotal_price','current_total_tax','current_total_price'] -%}
{%- if shopify_fx_enabled() and field in money_fields -%}
{%- set parts = expr.split(' as ') -%}
(({{ parts[0] | trim }})::float * fx.fx_factor) as {{ (parts[1] | trim) if parts | length > 1 else field }}
{%- elif shopify_fx_enabled() and field == 'currency' -%}
'{{ shopify_target_currency() }}' as currency
{%- else -%}
{{ expr }}
{%- endif -%}
{%- endmacro -%}
