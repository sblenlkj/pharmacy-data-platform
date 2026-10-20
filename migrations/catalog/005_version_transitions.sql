BEGIN;

-- C11: текущая версия цены всегда с открытым окном. valid_to у is_current
-- запрещён целиком, поэтому флаг не может «протухнуть» со временем — у текущей
-- версии просто нет даты окончания. Закрытие — только явный UPDATE с
-- is_current = false (о чём RAISE ниже), автопереключение убрано.
ALTER TABLE price_version
    ADD CONSTRAINT ck_price_version_current_is_open
    CHECK (NOT is_current OR valid_to IS NULL);

CREATE FUNCTION apply_price_version_transition()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'UPDATE' AND NEW.is_current AND NEW.valid_to IS NOT NULL THEN
        RAISE EXCEPTION 'closing price version of % requires is_current = false in the same UPDATE',
            NEW.sku_bk
            USING ERRCODE = 'P0001';
    END IF;

    IF NEW.is_current THEN
        UPDATE price_version
           SET is_current = false,
               valid_to = NEW.valid_from
         WHERE sku_bk = NEW.sku_bk
           AND price_scope_id = NEW.price_scope_id
           AND scope_bk = NEW.scope_bk
           AND is_current
           AND valid_from < NEW.valid_from;
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_price_version_transition
    BEFORE INSERT OR UPDATE ON price_version
    FOR EACH ROW EXECUTE FUNCTION apply_price_version_transition();

CREATE FUNCTION apply_sku_version_transition()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.is_current THEN
        UPDATE sku_version
           SET is_current = false
         WHERE sku_bk = NEW.sku_bk
           AND is_current
           AND valid_from < NEW.valid_from;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_sku_version_transition
    BEFORE INSERT OR UPDATE ON sku_version
    FOR EACH ROW EXECUTE FUNCTION apply_sku_version_transition();

COMMIT;
