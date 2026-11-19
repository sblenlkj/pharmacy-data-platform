-- v_sku_history: версии SKU полуоткрытыми интервалами [valid_from, valid_to).
-- valid_to = valid_from следующей версии (lead), NULL = версия открыта;
-- границы выводятся при чтении, повтор порции их не сдвигает (ADR-011).
select
    sku,
    sku_name,
    category,
    is_rx,
    valid_from,
    lead(valid_from) over (partition by sku order by valid_from) as valid_to
from {{ ref('dim_sku_version') }}
