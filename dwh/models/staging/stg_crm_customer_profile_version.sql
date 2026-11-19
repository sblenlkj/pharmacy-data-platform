-- crm.public.customer_profile_version: версии профиля (город/регион/уровень
-- лояльности); текущая — is_current. Ключ Kafka — суррогатный id.
{{
    cdc_latest(
        'crm', 'public.customer_profile_version',
        ['id'],
        [
            after('id') ~ ' as id',
            after('customer_bk') ~ ' as customer_bk',
            after('city') ~ ' as city',
            after('region') ~ ' as region',
            after('loyalty_level_id') ~ ' as loyalty_level_id',
            after_ts('valid_from') ~ ' as valid_from',
            after_bool('is_current') ~ ' as is_current',
            'topic',
            'partition_id',
            'kafka_offset',
        ],
        filtered=false
    )
}}
