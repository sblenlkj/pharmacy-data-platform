{{ config(
    unique_key=['purchase_bk', 'line_no'],
    properties={'partitioning': "ARRAY['month(ordered_at)']"}
) }}

-- A new line may refer to an old order/batch. Header-only and batch-only
-- changes also recalculate the affected lines; no live PostgreSQL reads.
select
    l.purchase_bk, l.line_no, o.supplier_bk, o.dc_bk, l.sku, l.batch_bk,
    o.ordered_at, b.received_at,
    l.quantity, l.unit_cost, l.currency,
    {{ lineage_columns('l') }}
from {{ ref('stg_wms_purchase_line') }} l
join {{ ref('stg_wms_purchase_order') }} o on o.purchase_bk = l.purchase_bk
left join {{ ref('stg_wms_batch') }} b on b.batch_bk = l.batch_bk
where {{ changed('l') }} or {{ changed('o') }} or {{ changed('b') }}
