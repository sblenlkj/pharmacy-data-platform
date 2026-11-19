-- catalog.public.sku_version: версии имени/категории/признака Rx.
-- valid_to в источнике НЕ хранится: выводится lead(valid_from) в
-- v_sku_history (ADR-011). Ключ Kafka — суррогатный id.
{{
    cdc_latest(
        'catalog', 'public.sku_version',
        ['id'],
        [
            after('id') ~ ' as id',
            after('sku_bk') ~ ' as sku_bk',
            after('sku_name') ~ ' as sku_name',
            after('category_id') ~ ' as category_id',
            after_bool('is_rx') ~ ' as is_rx',
            after_ts('valid_from') ~ ' as valid_from',
            'topic',
            'partition_id',
            'kafka_offset',
        ]
    )
}}
