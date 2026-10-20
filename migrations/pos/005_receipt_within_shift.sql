BEGIN;

CREATE FUNCTION assert_receipt_within_shift()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    v_opened timestamptz;
    v_closed timestamptz;
BEGIN
    SELECT opened_at, closed_at INTO v_opened, v_closed
      FROM shift WHERE shift_bk = NEW.shift_bk;
    IF NOT FOUND THEN
        RETURN NEW;  -- отсутствие смены отклонит FK
    END IF;
    IF NEW.receipt_dt < v_opened
       OR (v_closed IS NOT NULL AND NEW.receipt_dt > v_closed) THEN
        RAISE EXCEPTION 'receipt % (%) is outside its shift window [%, %]',
            NEW.receipt_bk, NEW.receipt_dt, v_opened, v_closed
            USING ERRCODE = 'P0001';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_receipt_within_shift
    BEFORE INSERT OR UPDATE OF shift_bk, receipt_dt ON receipt
    FOR EACH ROW EXECUTE FUNCTION assert_receipt_within_shift();

CREATE FUNCTION assert_shift_window_covers_receipts()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF EXISTS (
        SELECT 1
          FROM receipt
         WHERE shift_bk = NEW.shift_bk
           AND (receipt_dt < NEW.opened_at
                OR (NEW.closed_at IS NOT NULL AND receipt_dt > NEW.closed_at))
    ) THEN
        RAISE EXCEPTION 'shift % window does not cover its receipts', NEW.shift_bk
            USING ERRCODE = 'P0001';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_shift_window_covers_receipts
    BEFORE UPDATE OF opened_at, closed_at ON shift
    FOR EACH ROW EXECUTE FUNCTION assert_shift_window_covers_receipts();

COMMIT;
