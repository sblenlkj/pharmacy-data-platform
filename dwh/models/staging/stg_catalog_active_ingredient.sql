-- catalog.public.active_ingredient: справочник действующих веществ (INN).
{{
    cdc_latest(
        'catalog', 'public.active_ingredient',
        ['ingredient_bk'],
        [
            after('ingredient_bk') ~ ' as ingredient_bk',
            after('inn_name') ~ ' as inn_name',
            'topic',
            'partition_id',
            'kafka_offset',
        ],
        filtered=false
    )
}}
