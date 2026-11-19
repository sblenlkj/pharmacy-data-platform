{# Партиционирование — свойством WITH таблицы Iceberg (dbt-trino 1.12
   не знает partition_by). Спец по ADR-010: month(receipt_dt). #}
{{ config(
    unique_key=['receipt_bk', 'line_no'],
    properties={
        'partitioning': "ARRAY['month(receipt_dt)']",
    }
) }}

-- fact_sale_line: позиция чека. Заголовочные атрибуты (клиент, аптека,
-- дата/тип документа, родитель, валюта) — из события receipt; основной
-- источник строки (lineage) — событие receipt_line (dwh_mapping.md §9–10).
-- receipt и receipt_line пишутся одной PG-транзакцией (FK parent-first);
-- окно порции покрывает оба события, т.к. загрузка идёт после доставки
-- всей порции в raw (протокол чекера). Ручной load_dds посреди доставки
-- может оставить заголовок за окном — строка доедет следующей порцией.
-- pharmacy_bk — обогащение stg_pos_receipt из ctl.receipt_pharmacy
-- (маршрут shift → cash_register → pharmacy вне репликации).
-- line_amount НЕ хранится: производное значение считается в v_sale_line
-- по формуле источника. Snapshot-атрибуты — физические колонки.
select
    l.receipt_bk,
    l.line_no,
    l.sku,
    l.batch_bk,
    l.quantity,
    l.unit_price,
    l.line_discount,
    l.is_rx_snapshot,
    l.sku_name_snapshot,
    r.pharmacy_bk,
    r.customer_bk,
    r.receipt_dt,
    r.doc_type,
    r.parent_receipt_bk,
    r.currency,
    {{ lineage_columns('l') }}
from {{ ref('stg_pos_receipt_line') }} l
join {{ ref('stg_pos_receipt') }} r on r.receipt_bk = l.receipt_bk
