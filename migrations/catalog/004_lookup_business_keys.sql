BEGIN;

ALTER TABLE sku_category
    ADD COLUMN category_bk text GENERATED ALWAYS AS
        ('CAT-' || upper(regexp_replace(category_name, '[^a-zA-Z0-9]+', '_', 'g'))) STORED;
ALTER TABLE sku_category
    ADD CONSTRAINT uq_sku_category_bk UNIQUE (category_bk);

ALTER TABLE price_scope
    ADD COLUMN price_scope_bk text GENERATED ALWAYS AS
        ('PSCOPE-' || upper(regexp_replace(scope_name, '[^a-zA-Z0-9]+', '_', 'g'))) STORED;
ALTER TABLE price_scope
    ADD CONSTRAINT uq_price_scope_bk UNIQUE (price_scope_bk);

-- Гард BK-иммутабельности переиспользует reject_business_key_change из
-- 002_invariants.sql: BK справочника сгенерирован из имени, поэтому
-- защищаем само имя-источник (изменение имени = изменение BK).
CREATE TRIGGER trg_sku_category_bk_immutable
    BEFORE UPDATE ON sku_category FOR EACH ROW
    EXECUTE FUNCTION reject_business_key_change('category_name');
CREATE TRIGGER trg_price_scope_bk_immutable
    BEFORE UPDATE ON price_scope FOR EACH ROW
    EXECUTE FUNCTION reject_business_key_change('scope_name');

COMMIT;
