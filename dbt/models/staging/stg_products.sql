-- One row per product. SKUs are trimmed so they match the supplier files.
select
    product_id,
    trim(sku) as sku,
    name_en,
    category
from {{ source('raw', 'products') }}
