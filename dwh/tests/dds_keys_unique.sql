-- Составные ключи DDS уникальны (dbt unique не умеет кортежи):
-- факты и версионируемые справочники. Тест возвращает строки-дубликаты.
select 'fact_sale_line' as model, receipt_bk as k1, cast(line_no as varchar) as k2
from {{ ref('fact_sale_line') }} group by 2, 3 having count(*) > 1
union all
select 'fact_purchase_line', purchase_bk, cast(line_no as varchar)
from {{ ref('fact_purchase_line') }} group by 2, 3 having count(*) > 1
union all
select 'fact_movement', movement_bk, ''
from {{ ref('fact_movement') }} group by 2 having count(*) > 1
union all
select 'dim_sku_version', sku, cast(valid_from as varchar)
from {{ ref('dim_sku_version') }} group by 2, 3 having count(*) > 1
union all
select 'dim_price', sku, concat(price_scope, ':', scope_bk, ':', cast(valid_from as varchar))
from {{ ref('dim_price') }} group by 2, 3 having count(*) > 1
