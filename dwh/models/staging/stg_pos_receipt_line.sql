-- pos.public.receipt_line: позиция чека; snapshot-атрибуты (sku_name_snapshot,
-- is_rx_snapshot) сохраняются в факте как в источнике (ADR-011 «снимок
-- продажи»). Основной источник строки факта — событие строки (§10).
{{
    cdc_latest(
        'pos', 'public.receipt_line',
        ['receipt_bk', 'line_no'],
        [
            after('receipt_bk') ~ ' as receipt_bk',
            after_int('line_no') ~ ' as line_no',
            after('sku') ~ ' as sku',
            after('batch_bk') ~ ' as batch_bk',
            after_dec('quantity', 18, 3) ~ ' as quantity',
            after_dec('unit_price') ~ ' as unit_price',
            after_dec('line_discount') ~ ' as line_discount',
            after_bool('is_rx_snapshot') ~ ' as is_rx_snapshot',
            after('sku_name_snapshot') ~ ' as sku_name_snapshot',
            'topic',
            'partition_id',
            'kafka_offset',
        ]
    )
}}
