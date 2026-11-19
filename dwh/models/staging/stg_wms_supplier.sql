-- wms.public.supplier: справочник поставщиков.
{{
    cdc_latest(
        'wms', 'public.supplier',
        ['supplier_bk'],
        [
            after('supplier_bk') ~ ' as supplier_bk',
            after('supplier_name') ~ ' as supplier_name',
            after('country') ~ ' as country',
            after_bool('is_active') ~ ' as is_active',
            'topic',
            'partition_id',
            'kafka_offset',
        ]
    )
}}
