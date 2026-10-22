-- One row per dark store.
select
    store_id,
    city
from {{ source('raw', 'stores') }}
