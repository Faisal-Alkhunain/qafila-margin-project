-- Grain: one row per iso_week x city x category with at least one counted order line.
-- Uncosted lines count in lines and revenue_sar, but not in the cost columns.
select
    iso_week,
    city,
    category,
    count(*)::integer                                                    as lines,
    round(sum(line_revenue), 2)                                          as revenue_sar,
    round(coalesce(sum(line_revenue) filter (where is_costed), 0), 2)    as costed_revenue_sar,
    round(coalesce(sum(line_cost), 0), 2)                                as cost_sar,
    round(coalesce(sum(line_revenue) filter (where is_costed), 0)
          - coalesce(sum(line_cost), 0), 2)                              as margin_sar,
    count(*) filter (where not is_costed)::integer                       as uncosted_lines
from {{ ref('fct_order_line_cost') }}
group by iso_week, city, category
