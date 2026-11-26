{{ config(
    unique_key=['sku', 'valid_from']
) }}

-- dim_sku_version (SCD2): единственный носитель историзируемых атрибутов
-- SKU — имя, категория, признак Rx. valid_to НЕ хранится: выводится
-- lead(valid_from) в v_sku_history (ADR-011) — повтор порции не может
-- «сдвинуть» границу. Естественный ключ (sku, valid_from) = merge-ключ.
-- Источник — обработанное состояние stg_catalog_sku_version; категория —
-- полное состояние справочника.
select
    v.sku_bk as sku,
    v.valid_from,
    v.sku_name,
    c.category_name as category,
    v.is_rx,
    {{ lineage_columns('v') }}
from {{ ref('stg_catalog_sku_version') }} v
join {{ ref('stg_catalog_sku_category') }} c on c.id = v.category_id
where {{ changed('v') }} or {{ changed('c') }}
