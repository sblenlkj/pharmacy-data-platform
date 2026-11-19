-- v_customer: видимые клиенты. Фильтр deleted_at IS NULL скрывает
-- soft-deleted, dim хранит всех (ADR-009).
select
    customer_bk,
    birth_year,
    sex,
    city,
    region,
    loyalty_level,
    marketing_opt_in,
    registered_at
from {{ ref('dim_customer') }}
where deleted_at is null
