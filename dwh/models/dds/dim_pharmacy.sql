{{ config(
    unique_key='pharmacy_bk'
) }}

-- dim_pharmacy (SCD1): справочник аптек. Merge по pharmacy_bk (ADR-011);
-- источник — stg_pos_pharmacy из обработанного состояния.
select
    p.pharmacy_bk,
    p.pharmacy_name,
    p.region,
    p.city,
    p.address,
    p.opened_at,
    p.closed_at,
    p.is_active,
    {{ lineage_columns('p') }}
from {{ ref('stg_pos_pharmacy') }} p
where {{ changed('p') }}
