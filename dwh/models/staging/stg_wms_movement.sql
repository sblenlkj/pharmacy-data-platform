-- wms.public.movement: движение партии; endpoints полиморфные
-- (src_type/dst_type ∈ dc | pharmacy | supplier | customer, dwh_mapping.md §7.1).
-- sku выводится через партию (dim/fact), в событии движения его нет.
{{
    cdc_latest(
        'wms', 'public.movement',
        ['movement_bk'],
        [
            after('movement_bk') ~ ' as movement_bk',
            after('batch_bk') ~ ' as batch_bk',
            after('movement_type') ~ ' as movement_type',
            after('src_type') ~ ' as src_type',
            after('src_bk') ~ ' as src_bk',
            after('dst_type') ~ ' as dst_type',
            after('dst_bk') ~ ' as dst_bk',
            after_dec('qty', 18, 3) ~ ' as qty',
            after_ts('dispatched_at') ~ ' as dispatched_at',
            after_ts('moved_at') ~ ' as moved_at',
            'topic',
            'partition_id',
            'kafka_offset',
        ]
    )
}}
