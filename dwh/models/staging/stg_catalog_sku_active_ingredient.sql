-- catalog.public.sku_active_ingredient: связь M:N SKU ↔ действующие вещества;
-- inn_name dim_sku — агрегат (как в источнике crm.v_sku… catalog.v_sku:
-- string_agg(inn_name, ', ' ORDER BY inn_name)).
{{
    cdc_latest(
        'catalog', 'public.sku_active_ingredient',
        ['sku_bk', 'ingredient_bk'],
        [
            after('sku_bk') ~ ' as sku_bk',
            after('ingredient_bk') ~ ' as ingredient_bk',
            'topic',
            'partition_id',
            'kafka_offset',
        ],
        filtered=false
    )
}}
