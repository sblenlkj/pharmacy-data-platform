-- pos.public.pharmacy: справочник аптек (SCD1, soft delete → is_active).
{{
    cdc_latest(
        'pos', 'public.pharmacy',
        ['pharmacy_bk'],
        [
            after('pharmacy_bk') ~ ' as pharmacy_bk',
            after('pharmacy_name') ~ ' as pharmacy_name',
            after('region') ~ ' as region',
            after('city') ~ ' as city',
            after('address') ~ ' as address',
            after_date('opened_at') ~ ' as opened_at',
            after_date('closed_at') ~ ' as closed_at',
            after_bool('is_active') ~ ' as is_active',
            'topic',
            'partition_id',
            'kafka_offset',
        ]
    )
}}
