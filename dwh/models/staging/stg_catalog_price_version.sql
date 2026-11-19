-- catalog.public.price_version: версии цен; valid_to приходит событием
-- закрытия (UPDATE той же версии). Ключ Kafka — суррогатный id, merge-ключ
-- dim_price — (sku, price_scope, scope_bk, valid_from) — ADR-011.
{{
    cdc_latest(
        'catalog', 'public.price_version',
        ['id'],
        [
            after('id') ~ ' as id',
            after('sku_bk') ~ ' as sku_bk',
            after('price_scope_id') ~ ' as price_scope_id',
            after('scope_bk') ~ ' as scope_bk',
            after_dec('price') ~ ' as price',
            after('currency') ~ ' as currency',
            after_ts('valid_from') ~ ' as valid_from',
            after_ts('valid_to') ~ ' as valid_to',
            'topic',
            'partition_id',
            'kafka_offset',
        ]
    )
}}
