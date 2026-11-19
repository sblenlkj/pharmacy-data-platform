-- v_purchase_line: строки закупок. line_cost = quantity * unit_cost —
-- формула источника (wms.v_purchase_line), в факте не хранится (ADR-009).
select
    purchase_bk,
    line_no,
    supplier_bk,
    dc_bk,
    sku,
    batch_bk,
    ordered_at,
    received_at,
    quantity,
    unit_cost,
    quantity * unit_cost as line_cost,
    currency
from {{ ref('fact_purchase_line') }}
