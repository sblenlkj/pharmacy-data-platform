{{ config(
    unique_key='batch_bk'
) }}

-- dim_batch (SCD1): партии; связка SKU ↔ партия для фактов
-- (sku движения, received_at закупки — dwh_mapping.md §9). Merge-ключ —
-- batch_bk (ADR-011); источник — stg_wms_batch из обработанного состояния.
select
    b.batch_bk,
    b.sku,
    b.series_no,
    b.manufactured_date,
    b.expiry_date,
    b.received_at,
    {{ lineage_columns('b') }}
from {{ ref('stg_wms_batch') }} b
where {{ changed('b') }}
