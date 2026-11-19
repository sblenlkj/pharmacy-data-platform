-- v_sku: текущий срез товара. Историзируемые имя/категория/Rx — из открытой
-- версии dim_sku_version (последней по valid_from; открытая версия в таблице
-- одна — ключ sku+valid_from), что исключает рассинхрон с v_sku_history
-- (ADR-009, кросс-проверка чекера history.versions).
with current_version as (
    select sku, sku_name, category, is_rx
    from (
        select
            v.*,
            row_number() over (partition by sku order by valid_from desc) as rn
        from {{ ref('dim_sku_version') }} v
    )
    where rn = 1
)
select
    s.sku,
    v.sku_name,
    s.inn_name,
    s.manufacturer_name,
    s.drug_form,
    v.category,
    v.is_rx,
    s.is_vital,
    s.pack_qty,
    s.is_active
from {{ ref('dim_sku') }} s
join current_version v on v.sku = s.sku
