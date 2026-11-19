-- v_sale_line: позиции чеков. Soft delete клиента: LEFT JOIN к НЕудалённым
-- dim_customer → customer_bk NULL после удаления, строка и суммы остаются
-- (ADR-009; чекер history.privacy). line_amount — по формуле источника
-- (migrations/pos/003: quantity * unit_price - line_discount).
select
    f.receipt_bk,
    f.line_no,
    f.sku,
    f.batch_bk,
    f.quantity,
    f.unit_price,
    f.line_discount,
    f.quantity * f.unit_price - f.line_discount as line_amount,
    f.is_rx_snapshot,
    f.sku_name_snapshot,
    f.pharmacy_bk,
    c.customer_bk,
    f.receipt_dt,
    f.doc_type,
    f.parent_receipt_bk,
    f.currency
from {{ ref('fact_sale_line') }} f
left join {{ ref('dim_customer') }} c
    on c.customer_bk = f.customer_bk
   and c.deleted_at is null
