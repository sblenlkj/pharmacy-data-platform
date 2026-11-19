-- crm.public.consent: согласия; marketing_opt_in dim_customer — производное
-- текущее состояние (dwh_mapping.md §7.7).
{{
    cdc_latest(
        'crm', 'public.consent',
        ['consent_bk'],
        [
            after('consent_bk') ~ ' as consent_bk',
            after('customer_bk') ~ ' as customer_bk',
            after('consent_type_id') ~ ' as consent_type_id',
            after_ts('issued_at') ~ ' as issued_at',
            after_ts('revoked_at') ~ ' as revoked_at',
            'topic',
            'partition_id',
            'kafka_offset',
        ],
        filtered=false
    )
}}
