-- v_movement: движения партий. dst_bk зануляется для удалённого клиента
-- (dst_type='customer' и клиент скрыт) — чекер history.privacy; видимый
-- клиент и прочие типы локаций остаются как в источнике.
select
    m.movement_bk,
    m.batch_bk,
    m.sku,
    m.movement_type,
    m.src_type,
    m.src_bk,
    m.dst_type,
    case when m.dst_type = 'customer' then c.customer_bk else m.dst_bk end as dst_bk,
    m.qty,
    m.dispatched_at,
    m.moved_at
from {{ ref('fact_movement') }} m
left join {{ ref('dim_customer') }} c
    on c.customer_bk = m.dst_bk
   and c.deleted_at is null
