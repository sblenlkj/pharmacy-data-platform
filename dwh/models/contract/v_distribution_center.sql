-- v_distribution_center: распредцентры.
select
    dc_bk,
    dc_name,
    region,
    city,
    is_active
from {{ ref('dim_distribution_center') }}
