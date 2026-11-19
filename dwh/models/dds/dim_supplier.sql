{{ config(
    unique_key='supplier_bk'
) }}

-- dim_supplier (SCD1): справочник поставщиков. Merge по supplier_bk
-- (ADR-011); источник — stg_wms_supplier в окне порции.
select
    s.supplier_bk,
    s.supplier_name,
    s.country,
    s.is_active,
    {{ lineage_columns('s') }}
from {{ ref('stg_wms_supplier') }} s
