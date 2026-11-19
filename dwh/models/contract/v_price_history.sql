-- v_price_history: версии цен по областям действия. is_current —
-- производное (valid_to IS NULL), не хранится (ADR-009).
select
    sku,
    price_scope,
    scope_bk,
    price,
    currency,
    valid_from,
    valid_to,
    valid_to is null as is_current
from {{ ref('dim_price') }}
