{{ config(
    unique_key='customer_bk'
) }}

-- dim_customer (SCD1): текущий срез клиента. Профиль — текущая версия
-- customer_profile_version (is_current), уровень лояльности — по id,
-- marketing_opt_in — производное состояние consent-событий (как в
-- crm.v_customer источника: bool_or(revoked_at IS NULL AND тип не удалён),
-- отсутствующие согласия → false). Soft delete хранится колонкой
-- deleted_at и разрешается на уровне представлений (ADR-009).
--
-- Инкремент: merge по customer_bk; источник — stg_crm_customer
-- в окне порции (затронутые клиенты), атрибуты-спутники (профиль, уровень,
-- согласия) — полное состояние маршрутов (filtered=false): события профиля
-- и согласий могли прийти раньше окна.
with profile as (
    select * from {{ ref('stg_crm_customer_profile_version') }}
    where is_current
),
loyalty as (
    select * from {{ ref('stg_crm_loyalty_level') }}
    where deleted_at is null
),
marketing as (
    select
    c.customer_bk,
    bool_or(c.revoked_at is null and ct.deleted_at is null) as marketing_opt_in
    from {{ ref('stg_crm_consent') }} c
    join {{ ref('stg_crm_consent_type') }} ct on ct.id = c.consent_type_id
    where ct.type_name = 'marketing'
    group by c.customer_bk
)
select
    c.customer_bk,
    c.birth_year,
    c.sex,
    p.city,
    p.region,
    l.level_name as loyalty_level,
    coalesce(m.marketing_opt_in, false) as marketing_opt_in,
    c.registered_at,
    c.deleted_at,
    {{ lineage_columns('c') }}
from {{ ref('stg_crm_customer') }} c
left join profile p on p.customer_bk = c.customer_bk
left join loyalty l on l.id = p.loyalty_level_id
left join marketing m on m.customer_bk = c.customer_bk
