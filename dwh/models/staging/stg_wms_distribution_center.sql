-- wms.public.distribution_center: справочник распредцентров.
{{
    cdc_latest(
        'wms', 'public.distribution_center',
        ['dc_bk'],
        [
            after('dc_bk') ~ ' as dc_bk',
            after('dc_name') ~ ' as dc_name',
            after('region') ~ ' as region',
            after('city') ~ ' as city',
            after_bool('is_active') ~ ' as is_active',
            'topic',
            'partition_id',
            'kafka_offset',
        ]
    )
}}
