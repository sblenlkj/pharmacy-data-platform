-- v_supplier: поставщики.
select
    supplier_bk,
    supplier_name,
    country,
    is_active
from {{ ref('dim_supplier') }}
