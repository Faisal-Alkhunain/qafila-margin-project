-- Reconciliation: weekly_margin holds every eligible order line of the source, and its revenue,
-- computed independently from the staging layer. Returns a row (= fails) when they disagree.
with source_lines as (
    select count(*) as lines, round(sum(i.line_revenue), 2) as revenue
    from {{ ref('stg_order_items') }} i
    join {{ ref('stg_orders') }} o using (order_id)
    where o.status = 'delivered' and i.quantity > 0 and o.order_date >= date '2026-08-31'
),

mart as (
    select sum(lines) as lines, round(sum(revenue_sar), 2) as revenue
    from {{ ref('weekly_margin') }}
)

select s.lines as source_lines, m.lines as mart_lines, s.revenue as source_revenue, m.revenue as mart_revenue
from source_lines s, mart m
where s.lines <> m.lines or abs(s.revenue - m.revenue) > 1
