-- wms.public.purchase_line: строка закупки (batch_bk опционален — приход
-- без партии). Основной источник строки факта — событие строки (§10).
{{
    cdc_latest(
        'wms', 'public.purchase_line',
        ['purchase_bk', 'line_no'],
        [
            after('purchase_bk') ~ ' as purchase_bk',
            after_int('line_no') ~ ' as line_no',
            after('sku') ~ ' as sku',
            after('batch_bk') ~ ' as batch_bk',
            after_dec('quantity', 18, 3) ~ ' as quantity',
            after_dec('unit_cost') ~ ' as unit_cost',
            after('currency') ~ ' as currency',
            'topic',
            'partition_id',
            'kafka_offset',
        ]
    )
}}
