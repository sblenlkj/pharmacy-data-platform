-- catalog.public.sku_category: справочник категорий (ключ Kafka — id).
{{
    cdc_latest(
        'catalog', 'public.sku_category',
        ['id'],
        [
            after('id') ~ ' as id',
            after('category_name') ~ ' as category_name',
            'topic',
            'partition_id',
            'kafka_offset',
        ],
        filtered=false
    )
}}
