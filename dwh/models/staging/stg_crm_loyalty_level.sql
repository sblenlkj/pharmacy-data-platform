-- crm.public.loyalty_level: справочник уровней лояльности (ключ Kafka — id).
{{
    cdc_latest(
        'crm', 'public.loyalty_level',
        ['id'],
        [
            after('id') ~ ' as id',
            after('level_name') ~ ' as level_name',
            after_ts('deleted_at') ~ ' as deleted_at',
            'topic',
            'partition_id',
            'kafka_offset',
        ],
        filtered=false
    )
}}
