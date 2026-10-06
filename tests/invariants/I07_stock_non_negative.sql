INSERT INTO batch (batch_bk, sku, series_no, manufactured_date, expiry_date, received_at)
VALUES ('BATCH-I07', 'SKU-I07', 'SERIES-I07', '2026-01-01', '2027-01-01', '2026-02-01 00:00+00');

INSERT INTO movement
(movement_bk, batch_bk, movement_type, src_type, src_bk, dst_type, dst_bk, qty, dispatched_at, moved_at)
VALUES ('MOV-I07', 'BATCH-I07', 'transfer', 'dc', 'DC-I07', 'pharmacy', 'PH-I07', 1,
        '2026-03-01 09:00+00', '2026-03-01 10:00+00');
