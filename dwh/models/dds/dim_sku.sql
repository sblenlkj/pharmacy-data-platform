{{ config(unique_key='sku') }}

with ingredients as (
    select sai.sku_bk,
           array_join(array_sort(array_agg(ai.inn_name)), ', ') as inn_name,
           bool_or({{ changed('sai') }} or {{ changed('ai') }}) as affected
    from {{ ref('stg_catalog_sku_active_ingredient') }} sai
    join {{ ref('stg_catalog_active_ingredient') }} ai using (ingredient_bk)
    group by sai.sku_bk
)
select
    s.sku_bk as sku, i.inn_name, m.manufacturer_name,
    df.drug_form_name as drug_form, s.is_vital, s.pack_qty, s.is_active,
    {{ lineage_columns('s') }}
from {{ ref('stg_catalog_sku') }} s
join {{ ref('stg_catalog_manufacturer') }} m on m.manufacturer_bk = s.manufacturer_bk
left join {{ ref('stg_catalog_drug_form') }} df on df.drug_form_bk = s.drug_form_bk
left join ingredients i on i.sku_bk = s.sku_bk
where {{ changed('s') }} or {{ changed('m') }} or {{ changed('df') }}
   or coalesce(i.affected, false)
