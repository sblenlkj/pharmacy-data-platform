{{ config(
    unique_key='sku'
) }}

-- dim_sku: только SCD1-атрибуты (ADR-009); имя/категория/Rx — единственный
-- носитель dim_sku_version. inn_name — агрегат M:N действующих веществ,
-- как в catalog.v_sku источника (string_agg ', ' по алфавиту).
--
-- Инкремент: merge по sku; источник — stg_catalog_sku в окне
-- порции, справочники-спутники (производитель, лекформа, действующие
-- вещества M:N) — полное состояние маршрутов (filtered=false): события
-- связей могли прийти раньше окна, порядок между топиками не определён —
-- детерминизм даёт «последний выигрывает» по (topic, partition_id,
-- kafka_offset) внутри каждого маршрута.
with ingredients as (
    select
    sai.sku_bk,
    array_join(array_sort(array_agg(ai.inn_name)), ', ') as inn_name
    from {{ ref('stg_catalog_sku_active_ingredient') }} sai
    join {{ ref('stg_catalog_active_ingredient') }} ai using (ingredient_bk)
    group by sai.sku_bk
)
select
    s.sku_bk as sku,
    i.inn_name,
    m.manufacturer_name,
    df.drug_form_name as drug_form,
    s.is_vital,
    s.pack_qty,
    s.is_active,
    {{ lineage_columns('s') }}
from {{ ref('stg_catalog_sku') }} s
join {{ ref('stg_catalog_manufacturer') }} m on m.manufacturer_bk = s.manufacturer_bk
left join {{ ref('stg_catalog_drug_form') }} df on df.drug_form_bk = s.drug_form_bk
left join ingredients i on i.sku_bk = s.sku_bk
