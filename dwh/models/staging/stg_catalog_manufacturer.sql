-- catalog.public.manufacturer: справочник производителей.
{{
    cdc_latest(
        'catalog', 'public.manufacturer',
        ['manufacturer_bk'],
        [
            after('manufacturer_bk') ~ ' as manufacturer_bk',
            after('manufacturer_name') ~ ' as manufacturer_name',
            'topic',
            'partition_id',
            'kafka_offset',
        ],
        filtered=false
    )
}}
