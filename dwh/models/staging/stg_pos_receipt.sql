-- Physical receipt header; pharmacy is resolved from CDC shift/register in DDS.
{{
    cdc_latest(
        'pos', 'public.receipt',
        ['receipt_bk'],
        [
            after('receipt_bk') ~ ' as receipt_bk',
            after('shift_bk') ~ ' as shift_bk',
            after('cashier_bk') ~ ' as cashier_bk',
            after('customer_bk') ~ ' as customer_bk',
            after_ts('receipt_dt') ~ ' as receipt_dt',
            after('doc_type') ~ ' as doc_type',
            after('parent_receipt_bk') ~ ' as parent_receipt_bk',
            after('currency') ~ ' as currency',
            'topic',
            'partition_id',
            'kafka_offset',
        ]
    )
}}
