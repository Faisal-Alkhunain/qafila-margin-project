-- One row per order. The app stores UTC as text; the business day is the Riyadh calendar day.
select
    order_id,
    store_id,
    status,
    created_at::timestamp                         as created_at_utc,
    {{ to_riyadh('created_at') }}::date           as order_date
from {{ source('raw', 'orders') }}
