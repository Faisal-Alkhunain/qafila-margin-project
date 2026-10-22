-- One row per row of every supplier file, as delivered (no filtering here).
-- W36-W38 send the cost in unit_cost; from W39 the column is called cost_sar.
select
    source_file,
    supplier_id,
    trim(sku)                                         as sku,
    valid_from::date                                  as valid_from,
    coalesce(cost_sar, unit_cost)                     as raw_cost,
    {{ parse_cost('coalesce(cost_sar, unit_cost)') }} as unit_cost_sar
from {{ source('raw', 'supplier_prices') }}
