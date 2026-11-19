{{ config(
    unique_key=['sku', 'price_scope', 'scope_bk', 'valid_from']
) }}

-- dim_price (SCD2 по области действия): valid_to хранится — приходит
-- событием закрытия версии (UPDATE price_version), значения вычисляются
-- только из payload события — повтор порции воспроизводит их байт-в-байт
-- (ADR-011: guarded-семантика без now()). Естественный ключ
-- (sku, price_scope, scope_bk, valid_from) = merge-ключ.
-- Источник — stg_catalog_price_version в окне порции (версии, затронутые
-- событиями), справочник областей — полное состояние.
select
    p.sku_bk as sku,
    ps.scope_name as price_scope,
    p.scope_bk,
    p.price,
    p.currency,
    p.valid_from,
    p.valid_to,
    {{ lineage_columns('p') }}
from {{ ref('stg_catalog_price_version') }} p
join {{ ref('stg_catalog_price_scope') }} ps on ps.id = p.price_scope_id
