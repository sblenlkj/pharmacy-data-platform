-- v_batch: партии.
select
    batch_bk,
    sku,
    series_no,
    manufactured_date,
    expiry_date,
    received_at
from {{ ref('dim_batch') }}
