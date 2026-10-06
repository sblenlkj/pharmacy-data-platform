INSERT INTO pharmacy VALUES
('PH-I11', 'I11 Pharmacy', 'Region', 'City', 'Address', '2020-01-01', NULL, true, NULL, DEFAULT, DEFAULT);
INSERT INTO cash_register (register_bk, pharmacy_bk) VALUES ('REG-I11', 'PH-I11');
INSERT INTO cashier (cashier_bk) VALUES ('CASH-I11');
INSERT INTO shift (shift_bk, register_bk, opened_at) VALUES ('SHIFT-I11', 'REG-I11', '2026-01-01 00:00+00');
INSERT INTO receipt (receipt_bk, shift_bk, cashier_bk, receipt_dt, doc_type, total_amount, currency)
VALUES ('SALE-I11', 'SHIFT-I11', 'CASH-I11', '2026-06-01 12:00+00', 'sale', 20, 'RUB');
INSERT INTO receipt_line
(receipt_bk, line_no, sku, batch_bk, quantity, unit_price, line_discount,
 is_rx_snapshot, sku_name_snapshot, batch_expiry_date_snapshot)
VALUES ('SALE-I11', 1, 'SKU-I11', 'BATCH-I11', 2, 10, 0, false, 'I11 SKU', '2027-01-01');
INSERT INTO payment VALUES ('SALE-I11', 1, 'cash', 20, 'RUB', DEFAULT, DEFAULT);
INSERT INTO receipt
(receipt_bk, shift_bk, cashier_bk, receipt_dt, doc_type, parent_receipt_bk, total_amount, currency)
VALUES ('REFUND-I11-A', 'SHIFT-I11', 'CASH-I11', '2026-06-02 12:00+00', 'refund', 'SALE-I11', 10, 'RUB');
INSERT INTO receipt_line
(receipt_bk, line_no, sku, batch_bk, quantity, unit_price, line_discount,
 is_rx_snapshot, sku_name_snapshot, batch_expiry_date_snapshot)
VALUES ('REFUND-I11-A', 1, 'SKU-I11', 'BATCH-I11', 1, 10, 0, false, 'I11 SKU', '2027-01-01');
INSERT INTO payment VALUES ('REFUND-I11-A', 1, 'cash', 10, 'RUB', DEFAULT, DEFAULT);
INSERT INTO receipt
(receipt_bk, shift_bk, cashier_bk, receipt_dt, doc_type, parent_receipt_bk, total_amount, currency)
VALUES ('REFUND-I11-B', 'SHIFT-I11', 'CASH-I11', '2026-06-03 12:00+00', 'refund', 'SALE-I11', 20, 'RUB');
INSERT INTO receipt_line
(receipt_bk, line_no, sku, batch_bk, quantity, unit_price, line_discount,
 is_rx_snapshot, sku_name_snapshot, batch_expiry_date_snapshot)
VALUES ('REFUND-I11-B', 1, 'SKU-I11', 'BATCH-I11', 2, 10, 0, false, 'I11 SKU', '2027-01-01');
INSERT INTO payment VALUES ('REFUND-I11-B', 1, 'cash', 20, 'RUB', DEFAULT, DEFAULT);
