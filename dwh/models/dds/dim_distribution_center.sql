{{ config(
    unique_key='dc_bk'
) }}

-- dim_distribution_center (SCD1): справочник распредцентров.
-- Merge по dc_bk (ADR-011); источник — stg_wms_distribution_center
-- в окне порции.
select
    d.dc_bk,
    d.dc_name,
    d.region,
    d.city,
    d.is_active,
    {{ lineage_columns('d') }}
from {{ ref('stg_wms_distribution_center') }} d
