-- Grain: one row per counted order line (order_id, line_no): delivered orders, quantity > 0,
-- ordered on or after the first supplier week. Each line is costed at the supplier cost version
-- valid on its order date (point-in-time join); lines with no valid version stay, uncosted.

{% set first_supplier_week = '2026-08-31' %}

with counted_lines as (
    select
        i.order_id,
        i.line_no,
        o.order_date,
        s.city,
        p.sku,
        p.category,
        i.quantity,
        i.line_revenue
    from {{ ref('stg_order_items') }} i
    join {{ ref('stg_orders') }}   o on o.order_id = i.order_id
    join {{ ref('stg_stores') }}   s on s.store_id = o.store_id
    join {{ ref('stg_products') }} p on p.product_id = i.product_id
    where o.status = 'delivered'
      and i.quantity > 0
      and o.order_date >= date '{{ first_supplier_week }}'
)

select
    l.order_id,
    l.line_no,
    l.order_date,
    weekofyear(l.order_date)::integer   as iso_week,
    l.city,
    l.category,
    l.sku,
    l.quantity,
    l.line_revenue,
    h.unit_cost_sar,
    l.quantity * h.unit_cost_sar        as line_cost,
    h.unit_cost_sar is not null         as is_costed
from counted_lines l
left join {{ ref('product_cost_history') }} h
    on  h.sku = l.sku
    and l.order_date >= h.valid_from
    and (l.order_date < h.valid_to or h.valid_to is null)
