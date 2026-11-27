{{ config(
    unique_key=['receipt_bk', 'line_no'],
    properties={'partitioning': "ARRAY['month(receipt_dt)']"}
) }}

-- Recompute affected lines using the FULL processed header and location state.
-- A late line or late header is picked up when either dependency changes.
select
    l.receipt_bk, l.line_no, l.sku, l.batch_bk, l.quantity,
    l.unit_price, l.line_discount, l.is_rx_snapshot, l.sku_name_snapshot,
    cr.pharmacy_bk, r.customer_bk, r.receipt_dt, r.doc_type,
    r.parent_receipt_bk, r.currency,
    {{ lineage_columns('l') }}
from {{ ref('stg_pos_receipt_line') }} l
join {{ ref('stg_pos_receipt') }} r on r.receipt_bk = l.receipt_bk
join {{ ref('stg_pos_shift') }} sh on sh.shift_bk = r.shift_bk
join {{ ref('stg_pos_cash_register') }} cr on cr.register_bk = sh.register_bk
where {{ changed('l') }} or {{ changed('r') }}
   or {{ changed('sh') }} or {{ changed('cr') }}
