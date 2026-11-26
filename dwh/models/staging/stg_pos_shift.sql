{{
    cdc_latest(
        'pos', 'public.shift',
        ['shift_bk'],
        [
            after('shift_bk') ~ ' as shift_bk',
            after('register_bk') ~ ' as register_bk',
            'topic',
            'partition_id',
            'kafka_offset'
        ]
    )
}}
