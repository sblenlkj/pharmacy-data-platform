-- Инварианты истории (ADR-011, полуоткрытые интервалы без пересечений,
-- ровно одна открытая версия): тест возвращает нарушения.
-- v_sku_history: valid_to там выводится lead(valid_from) — сам инвариант.

-- пересечения/пустые интервалы версий SKU
select 'sku_overlap' as invariant, sku as k1, cast(valid_from as varchar) as k2
from {{ ref('v_sku_history') }} h
where h.valid_to is not null and h.valid_to <= h.valid_from
   or exists (select 1 from {{ ref('v_sku_history') }} n
              where n.sku = h.sku and n.valid_from > h.valid_from
                and (h.valid_to is null or n.valid_from < h.valid_to))

union all

-- не ровно одна открытая версия SKU
select 'sku_open_versions', sku, cast(count(*) as varchar)
from {{ ref('v_sku_history') }}
where valid_to is null
group by sku having count(*) <> 1

union all

-- пересечения/пустые интервалы версий цены в области действия
select 'price_overlap', concat(sku, ':', price_scope, ':', scope_bk) as k1,
       cast(valid_from as varchar) as k2
from {{ ref('dim_price') }} h
where h.valid_to is not null and h.valid_to <= h.valid_from
   or exists (select 1 from {{ ref('dim_price') }} n
              where n.sku = h.sku and n.price_scope = h.price_scope
                and n.scope_bk = h.scope_bk and n.valid_from > h.valid_from
                and (h.valid_to is null or n.valid_from < h.valid_to))

union all

-- не ровно одна действующая цена в области действия
select 'price_open_versions', concat(sku, ':', price_scope, ':', scope_bk) as k1,
       cast(count(*) as varchar)
from {{ ref('dim_price') }}
where valid_to is null
group by 2 having count(*) <> 1
