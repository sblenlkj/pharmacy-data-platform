-- pos.public.receipt: заголовок чека (клиент/дата/тип документа/валюта).
--
-- pharmacy_bk — обогащение из ctl.receipt_pharmacy: в источнике
-- аптека достижима только через receipt → shift → cash_register → pharmacy,
-- а shift/cash_register исключены из репликации (dwh_mapping.md §9).
-- Справочник синхронизирует scripts/load_dds из pos.contract.v_receipt
-- (той же формулы join, что и эталон сверки чекера) — без окна порции:
-- окно фильтрует только сами чеки (события receipt), атрибут заголовка
-- берётся по receipt_bk целиком. Inner join: FK shift → cash_register →
-- pharmacy гарантирует строку каждому чеку (v_sale_line.pharmacy_bk
-- NOT NULL по контракту).
with receipts as (
{{
    cdc_latest(
        'pos', 'public.receipt',
        ['receipt_bk'],
        [
            after('receipt_bk') ~ ' as receipt_bk',
            after('shift_bk') ~ ' as shift_bk',
            after('cashier_bk') ~ ' as cashier_bk',
            after('customer_bk') ~ ' as customer_bk',
            after_ts('receipt_dt') ~ ' as receipt_dt',
            after('doc_type') ~ ' as doc_type',
            after('parent_receipt_bk') ~ ' as parent_receipt_bk',
            after('currency') ~ ' as currency',
            'topic',
            'partition_id',
            'kafka_offset',
        ]
    )
}}
)
select
    r.*,
    p.pharmacy_bk
from receipts r
join {{ source('ctl', 'receipt_pharmacy') }} p on p.receipt_bk = r.receipt_bk
