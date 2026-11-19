-- wms.public.batch: партии. Справочник-связка: sku движения и received_at
-- строки закупки берутся отсюда (dwh_mapping.md §9).
{{
    cdc_latest(
        'wms', 'public.batch',
        ['batch_bk'],
        [
            after('batch_bk') ~ ' as batch_bk',
            after('sku') ~ ' as sku',
            after('series_no') ~ ' as series_no',
            after_date('manufactured_date') ~ ' as manufactured_date',
            after_date('expiry_date') ~ ' as expiry_date',
            after_ts('received_at') ~ ' as received_at',
            'topic',
            'partition_id',
            'kafka_offset',
        ]
    )
}}
