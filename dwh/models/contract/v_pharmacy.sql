-- v_pharmacy: справочник аптек (без audit-колонок).
select
    pharmacy_bk,
    pharmacy_name,
    region,
    city,
    address,
    opened_at,
    closed_at,
    is_active
from {{ ref('dim_pharmacy') }}
