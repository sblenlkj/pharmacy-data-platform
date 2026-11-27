{{
    cdc_latest(
        'pos', 'public.cash_register',
        ['register_bk'],
        [
            after('register_bk') ~ ' as register_bk',
            after('pharmacy_bk') ~ ' as pharmacy_bk',
            'topic',
            'partition_id',
            'kafka_offset'
        ]
    )
}}
