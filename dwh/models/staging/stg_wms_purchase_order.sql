-- wms.public.purchase_order: заголовок закупки (поставщик/РЦ/дата заказа).
{{
    cdc_latest(
        'wms', 'public.purchase_order',
        ['purchase_bk'],
        [
            after('purchase_bk') ~ ' as purchase_bk',
            after('supplier_bk') ~ ' as supplier_bk',
            after('dc_bk') ~ ' as dc_bk',
            after_ts('ordered_at') ~ ' as ordered_at',
            'topic',
            'partition_id',
            'kafka_offset',
        ]
    )
}}
