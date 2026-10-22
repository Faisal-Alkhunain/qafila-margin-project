-- One row per order line, (order_id, line_no). Negative quantities are kept here; the marts filter them.
select
    order_id,
    line_no,
    product_id,
    quantity,
    unit_price,
    discount,
    round(quantity * unit_price - discount, 2) as line_revenue
from {{ source('raw', 'order_items') }}
