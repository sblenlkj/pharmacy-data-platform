BEGIN;

CREATE FUNCTION apply_profile_version_transition()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.is_current THEN
        UPDATE customer_profile_version
           SET is_current = false
         WHERE customer_bk = NEW.customer_bk
           AND is_current
           AND valid_from < NEW.valid_from;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_profile_version_transition
    BEFORE INSERT OR UPDATE ON customer_profile_version
    FOR EACH ROW EXECUTE FUNCTION apply_profile_version_transition();

COMMIT;
