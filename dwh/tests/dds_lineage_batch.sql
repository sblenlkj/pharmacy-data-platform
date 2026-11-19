-- Lineage-инвариант ADR-011: каждая строка DDS помечена порцией
-- загрузчика; строк с меткой manual (ручной dbt run без --vars) быть
-- не должно. NULL-метки невозможны (merge_exclude_columns сохраняет
-- значение, вставка всегда с vars).
select 'fact_sale_line' as model, receipt_bk as k1, cast(line_no as varchar) as k2
from {{ ref('fact_sale_line') }} where _load_batch_id is null or _load_batch_id = 'manual'
union all
select 'fact_purchase_line', purchase_bk, cast(line_no as varchar)
from {{ ref('fact_purchase_line') }} where _load_batch_id is null or _load_batch_id = 'manual'
union all
select 'fact_movement', movement_bk, ''
from {{ ref('fact_movement') }} where _load_batch_id is null or _load_batch_id = 'manual'
union all
select 'dim_sku', sku, ''
from {{ ref('dim_sku') }} where _load_batch_id is null or _load_batch_id = 'manual'
union all
select 'dim_customer', customer_bk, ''
from {{ ref('dim_customer') }} where _load_batch_id is null or _load_batch_id = 'manual'
