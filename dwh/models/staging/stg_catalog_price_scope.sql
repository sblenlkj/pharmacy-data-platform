-- catalog.public.price_scope: справочник областей действия цен
-- (chain | region | pharmacy).
{{
    cdc_latest(
        'catalog', 'public.price_scope',
        ['id'],
        [
            after('id') ~ ' as id',
            after('scope_name') ~ ' as scope_name',
            'topic',
            'partition_id',
            'kafka_offset',
        ],
        filtered=false
    )
}}
