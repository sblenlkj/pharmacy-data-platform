-- catalog.public.sku: SCD1-атрибуты товара (ADR-009); имя/категория/Rx —
-- историзируются в dim_sku_version, здесь их нет.
{{
    cdc_latest(
        'catalog', 'public.sku',
        ['sku_bk'],
        [
            after('sku_bk') ~ ' as sku_bk',
            after('manufacturer_bk') ~ ' as manufacturer_bk',
            after('drug_form_bk') ~ ' as drug_form_bk',
            after_dec('pack_qty', 18, 3) ~ ' as pack_qty',
            after_bool('is_vital') ~ ' as is_vital',
            after_bool('is_active') ~ ' as is_active',
            'topic',
            'partition_id',
            'kafka_offset',
        ]
    )
}}
