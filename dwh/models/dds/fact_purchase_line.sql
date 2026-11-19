{# Партиционирование — свойством WITH таблицы Iceberg (dbt-trino 1.12
   не знает partition_by). Спец по ADR-010: month(ordered_at). #}
{{ config(
    unique_key=['purchase_bk', 'line_no'],
    properties={
        'partitioning': "ARRAY['month(ordered_at)']",
    }
) }}

-- fact_purchase_line: строка закупки. Поставщик/РЦ/дата заказа — из события
-- purchase_order (одна PG-транзакция с purchase_line; окно покрывает оба
-- события — загрузка идёт после доставки порции целиком, протокол чекера);
-- received_at — из batch (LEFT: партия опциональна и часто старше окна —
-- fallback на загруженный dim_batch); основной источник строки — событие
-- purchase_line (§9–10). line_cost НЕ хранится: считается в v_purchase_line.
-- Партиция month(ordered_at).
select
    l.purchase_bk,
    l.line_no,
    o.supplier_bk,
    o.dc_bk,
    l.sku,
    l.batch_bk,
    o.ordered_at,
    coalesce(b.received_at, db.received_at) as received_at,
    l.quantity,
    l.unit_cost,
    l.currency,
    {{ lineage_columns('l') }}
from {{ ref('stg_wms_purchase_line') }} l
join {{ ref('stg_wms_purchase_order') }} o on o.purchase_bk = l.purchase_bk
left join {{ ref('stg_wms_batch') }} b on b.batch_bk = l.batch_bk
left join {{ ref('dim_batch') }} db on db.batch_bk = l.batch_bk
