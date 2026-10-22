-- Grain: one row per supplier row (same as stg_supplier_prices), with the reason it can't be used.
-- reason is NULL for usable rows. Rule order matters: a SKU Qafila doesn't sell is unknown_sku
-- even when its cost is also bad.
select
    s.source_file,
    s.sku,
    s.valid_from,
    s.raw_cost,
    s.unit_cost_sar,
    case
        when p.sku is null                                    then 'unknown_sku'
        when s.unit_cost_sar is null or s.unit_cost_sar <= 0  then 'invalid_cost'
    end as reason
from {{ ref('stg_supplier_prices') }} s
left join {{ ref('stg_products') }} p on p.sku = s.sku
