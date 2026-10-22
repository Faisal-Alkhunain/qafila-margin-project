-- SCD2 history of supplier costs. Grain: one row per SKU per cost version.
-- A version is valid on [valid_from, valid_to); valid_to is NULL for the current version.

with usable_quotes as (
    -- quarantined rows are ignored here and everywhere downstream
    select source_file, sku, valid_from, round(unit_cost_sar, 2) as unit_cost_sar
    from {{ ref('int_supplier_quotes_checked') }}
    where reason is null
),

latest_quote as (
    -- the same SKU quoted for the same day in several files (e.g. W39 and W39_fix):
    -- the file whose name sorts last wins
    select sku, valid_from, unit_cost_sar
    from usable_quotes
    qualify row_number() over (partition by sku, valid_from order by source_file desc) = 1
),

cost_changes as (
    -- a week where the cost is unchanged is not a new version; a week where the SKU is
    -- missing has no quote at all, so the previous version simply stays open
    select sku, valid_from, unit_cost_sar
    from latest_quote
    qualify unit_cost_sar is distinct from
            lag(unit_cost_sar) over (partition by sku order by valid_from)
)

select
    sku,
    unit_cost_sar,
    valid_from,
    lead(valid_from) over w          as valid_to,
    lead(valid_from) over w is null  as is_current
from cost_changes
window w as (partition by sku order by valid_from)
