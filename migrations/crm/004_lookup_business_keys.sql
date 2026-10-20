BEGIN;

ALTER TABLE loyalty_level
    ADD COLUMN level_bk text GENERATED ALWAYS AS
        ('LVL-' || upper(regexp_replace(level_name, '[^a-zA-Z0-9]+', '_', 'g'))) STORED;
ALTER TABLE loyalty_level
    ADD CONSTRAINT uq_loyalty_level_bk UNIQUE (level_bk);

ALTER TABLE contact_type
    ADD COLUMN contact_type_bk text GENERATED ALWAYS AS
        ('CTYPE-' || upper(regexp_replace(type_name, '[^a-zA-Z0-9]+', '_', 'g'))) STORED;
ALTER TABLE contact_type
    ADD CONSTRAINT uq_contact_type_bk UNIQUE (contact_type_bk);

ALTER TABLE consent_type
    ADD COLUMN consent_type_bk text GENERATED ALWAYS AS
        ('CONSENT-TYPE-' || upper(regexp_replace(type_name, '[^a-zA-Z0-9]+', '_', 'g'))) STORED;
ALTER TABLE consent_type
    ADD CONSTRAINT uq_consent_type_bk UNIQUE (consent_type_bk);

-- Гард BK-иммутабельности переиспользует reject_business_key_change из
-- 002_invariants.sql: BK справочника сгенерирован из имени, поэтому
-- защищаем само имя-источник (изменение имени = изменение BK).
CREATE TRIGGER trg_loyalty_level_bk_immutable
    BEFORE UPDATE ON loyalty_level FOR EACH ROW
    EXECUTE FUNCTION reject_business_key_change('level_name');
CREATE TRIGGER trg_contact_type_bk_immutable
    BEFORE UPDATE ON contact_type FOR EACH ROW
    EXECUTE FUNCTION reject_business_key_change('type_name');
CREATE TRIGGER trg_consent_type_bk_immutable
    BEFORE UPDATE ON consent_type FOR EACH ROW
    EXECUTE FUNCTION reject_business_key_change('type_name');

COMMIT;
