-- crm.public.customer: строка клиента (birth_year/sex здесь, профиль — в
-- customer_profile_version). deleted_at — soft delete (ADR-007-семантика).
{{
    cdc_latest(
        'crm', 'public.customer',
        ['customer_bk'],
        [
            after('customer_bk') ~ ' as customer_bk',
            after_int('birth_year') ~ ' as birth_year',
            after('sex') ~ ' as sex',
            after_ts('registered_at') ~ ' as registered_at',
            after_ts('deleted_at') ~ ' as deleted_at',
            'topic',
            'partition_id',
            'kafka_offset',
        ]
    )
}}
