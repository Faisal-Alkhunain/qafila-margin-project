-- SCD2: every SKU has exactly one current version, and it is the one with no valid_to.
select sku, count(*) filter (where is_current) as current_versions
from {{ ref('product_cost_history') }}
group by sku
having count(*) filter (where is_current) <> 1
    or count(*) filter (where is_current and valid_to is not null) > 0
