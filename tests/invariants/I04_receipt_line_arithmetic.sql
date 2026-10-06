INSERT INTO pharmacy VALUES
('PH-I04', 'I04 Pharmacy', 'Region', 'City', 'Address', '2020-01-01', NULL, true, NULL, DEFAULT, DEFAULT);
INSERT INTO cash_register (register_bk, pharmacy_bk) VALUES ('REG-I04', 'PH-I04');
INSERT INTO cashier (cashier_bk) VALUES ('CASH-I04');
INSERT INTO shift (shift_bk, register_bk, opened_at) VALUES ('SHIFT-I04', 'REG-I04', '2026-01-01 00:00+00');
INSERT INTO receipt (receipt_bk, shift_bk, cashier_bk, receipt_dt, doc_type, total_amount, currency)
VALUES ('RCPT-I04', 'SHIFT-I04', 'CASH-I04', '2026-06-01 12:00+00', 'sale', 10, 'RUB');

INSERT INTO receipt_line
(receipt_bk, line_no, sku, batch_bk, quantity, unit_price, line_discount,
 is_rx_snapshot, sku_name_snapshot, batch_expiry_date_snapshot)
VALUES ('RCPT-I04', 1, 'SKU-I04', 'BATCH-I04', 1, 10, 11, false, 'I04 SKU', '2027-01-01');
