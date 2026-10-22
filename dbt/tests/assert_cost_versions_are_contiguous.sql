-- SCD2: versions of a SKU never overlap or leave gaps (valid_to = next valid_from),
-- every interval is non-empty, every cost is positive, and consecutive versions differ in cost.
select *
from (
    select
        sku, unit_cost_sar, valid_from, valid_to,
        lead(valid_from)    over (partition by sku order by valid_from) as next_valid_from,
        lag(unit_cost_sar)  over (partition by sku order by valid_from) as previous_cost
    from {{ ref('product_cost_history') }}
)
where valid_to is distinct from next_valid_from
   or valid_to <= valid_from
   or unit_cost_sar <= 0
   or unit_cost_sar = previous_cost
