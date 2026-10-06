INSERT INTO pharmacy VALUES
('PH-I03', 'I03 Pharmacy', 'Region', 'City', 'Address', '2020-01-01', NULL, true, NULL, DEFAULT, DEFAULT);
INSERT INTO cash_register (register_bk, pharmacy_bk) VALUES ('REG-I03', 'PH-I03');
INSERT INTO cashier (cashier_bk) VALUES ('CASH-I03');
INSERT INTO shift (shift_bk, register_bk, opened_at, closed_at)
VALUES ('SHIFT-I03', 'REG-I03', '2026-01-01 00:00+00', '2026-12-31 23:59+00');
INSERT INTO receipt (receipt_bk, shift_bk, cashier_bk, receipt_dt, doc_type, total_amount, currency)
VALUES ('RCPT-I03', 'SHIFT-I03', 'CASH-I03', '2026-06-01 12:00+00', 'sale', 10, 'RUB');
INSERT INTO receipt_line
(receipt_bk, line_no, sku, batch_bk, quantity, unit_price, line_discount,
 is_rx_snapshot, sku_name_snapshot, batch_expiry_date_snapshot)
VALUES ('RCPT-I03', 1, 'SKU-I03', 'BATCH-I03', 1, 9, 0, false, 'I03 SKU', '2027-01-01');
INSERT INTO payment (receipt_bk, payment_no, payment_method, amount, currency)
VALUES ('RCPT-I03', 1, 'cash', 10, 'RUB');
