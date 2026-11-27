{{ config(unique_key='customer_bk') }}

-- Satellite-only changes propagate without a synthetic UPDATE customer.
with profile_changes as (
    select customer_bk, bool_or({{ changed('p') }}) as affected
    from {{ ref('stg_crm_customer_profile_version') }} p
    group by customer_bk
),
profile as (
    select * from {{ ref('stg_crm_customer_profile_version') }} where is_current
),
marketing as (
    select c.customer_bk,
           bool_or(ct.type_name = 'marketing'
                   and c.revoked_at is null and ct.deleted_at is null) as marketing_opt_in,
           bool_or({{ changed('c') }} or {{ changed('ct') }}) as affected
    from {{ ref('stg_crm_consent') }} c
    join {{ ref('stg_crm_consent_type') }} ct on ct.id = c.consent_type_id
    group by c.customer_bk
)
select
    c.customer_bk, c.birth_year, c.sex, p.city, p.region,
    case when l.deleted_at is null then l.level_name end as loyalty_level,
    coalesce(m.marketing_opt_in, false) as marketing_opt_in,
    c.registered_at, c.deleted_at,
    {{ lineage_columns('c') }}
from {{ ref('stg_crm_customer') }} c
left join profile p on p.customer_bk = c.customer_bk
left join profile_changes pc on pc.customer_bk = c.customer_bk
left join {{ ref('stg_crm_loyalty_level') }} l on l.id = p.loyalty_level_id
left join marketing m on m.customer_bk = c.customer_bk
where {{ changed('c') }} or coalesce(pc.affected, false)
   or {{ changed('l') }} or coalesce(m.affected, false)
