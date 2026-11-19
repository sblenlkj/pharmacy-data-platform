-- crm.public.consent_type: справочник типов согласий (marketing — для
-- marketing_opt_in).
{{
    cdc_latest(
        'crm', 'public.consent_type',
        ['id'],
        [
            after('id') ~ ' as id',
            after('type_name') ~ ' as type_name',
            after_ts('deleted_at') ~ ' as deleted_at',
            'topic',
            'partition_id',
            'kafka_offset',
        ],
        filtered=false
    )
}}
