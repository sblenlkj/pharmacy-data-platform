-- catalog.public.drug_form: справочник лекарственных форм.
{{
    cdc_latest(
        'catalog', 'public.drug_form',
        ['drug_form_bk'],
        [
            after('drug_form_bk') ~ ' as drug_form_bk',
            after('drug_form_name') ~ ' as drug_form_name',
            'topic',
            'partition_id',
            'kafka_offset',
        ],
        filtered=false
    )
}}
