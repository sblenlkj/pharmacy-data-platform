-- I7: остаток не может стать отрицательным.
-- Сущность: wms.stock, CHECK (qty_on_hand >= 0).
-- Условие: ветка 1 — прямая вставка отрицательного остатка; ветка 2 —
-- движение списывает больше остатка источника, в том числе путь
-- NOT FOUND → INSERT -qty в trg_movement_apply_stock.
-- Обе ветки реально выполняются внутри одного DO-блока: официальный чекер
-- прогоняет файл одним cur.execute(), где первая же ошибка прерывает
-- оставшиеся стейтменты, — SAVEPOINT вне DO-блока вторую ветку не спасал.
-- Падение ветки 1 ожидаемо и гасится EXCEPTION; терминальное падение
-- теста — 23514 от ветки 2. Если ветка неожиданно проходит, файл падает
-- с P0001 и отличается от ожидаемого кода ошибки.

INSERT INTO batch (batch_bk, sku, series_no, manufactured_date, expiry_date, received_at)
VALUES ('BATCH-I07', 'SKU-I07', 'SERIES-I07', '2026-01-01', '2027-01-01', '2026-02-01 00:00+00');

DO $$
BEGIN
    BEGIN
        INSERT INTO stock (location_type, location_bk, batch_bk, qty_on_hand)
        VALUES ('dc', 'DC-I07', 'BATCH-I07', -1);
        RAISE EXCEPTION 'I07 branch 1: direct negative qty_on_hand was accepted'
            USING ERRCODE = 'P0001';
    EXCEPTION WHEN check_violation THEN
        NULL;  -- ожидаемо: CHECK (qty_on_hand >= 0)
    END;

    INSERT INTO movement
    (movement_bk, batch_bk, movement_type, src_type, src_bk, dst_type, dst_bk, qty, dispatched_at, moved_at)
    VALUES ('MOV-I07', 'BATCH-I07', 'transfer', 'dc', 'DC-I07', 'pharmacy', 'PH-I07', 1,
            '2026-03-01 09:00+00', '2026-03-01 10:00+00');
    RAISE EXCEPTION 'I07 branch 2: overdrawing movement was accepted'
        USING ERRCODE = 'P0001';
END $$;
