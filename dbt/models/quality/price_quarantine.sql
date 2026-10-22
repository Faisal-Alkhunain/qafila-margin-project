-- Grain: one row per supplier row we cannot use, with the reason (unknown_sku or invalid_cost).
select source_file, sku, valid_from, raw_cost, reason
from {{ ref('int_supplier_quotes_checked') }}
where reason is not null
