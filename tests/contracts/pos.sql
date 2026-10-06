INSERT INTO pharmacy
(pharmacy_bk, pharmacy_name, region, city, address, opened_at, is_active)
VALUES ('PH-CONTRACT', 'Contract Pharmacy', 'Moscow', 'Moscow', 'Test 1', '2020-01-01', true);
INSERT INTO cash_register (register_bk, pharmacy_bk) VALUES ('REG-CONTRACT', 'PH-CONTRACT');
INSERT INTO cashier (cashier_bk) VALUES ('CASH-CONTRACT');
INSERT INTO shift (shift_bk, register_bk, opened_at)
VALUES ('SHIFT-CONTRACT', 'REG-CONTRACT', '2025-01-01 00:00+00');
INSERT INTO receipt
(receipt_bk, shift_bk, cashier_bk, customer_bk, receipt_dt, doc_type, total_amount, currency)
VALUES ('SALE-CONTRACT', 'SHIFT-CONTRACT', 'CASH-CONTRACT', 'CUSTOMER-CONTRACT',
        '2025-02-01 12:00+00', 'sale', 20, 'RUB');
INSERT INTO receipt_line
(receipt_bk, line_no, sku, batch_bk, quantity, unit_price, line_discount,
 is_rx_snapshot, sku_name_snapshot, batch_expiry_date_snapshot)
VALUES
('SALE-CONTRACT', 1, 'SKU-A', 'BATCH-A', 2, 5, 1, false, 'SKU A', '2026-01-01'),
('SALE-CONTRACT', 2, 'SKU-B', 'BATCH-B', 1, 12, 1, false, 'SKU B', '2026-01-01');
INSERT INTO payment (receipt_bk, payment_no, payment_method, amount, currency)
VALUES ('SALE-CONTRACT', 1, 'cash', 10, 'RUB'),
       ('SALE-CONTRACT', 2, 'card', 10, 'RUB');
INSERT INTO receipt
(receipt_bk, shift_bk, cashier_bk, receipt_dt, doc_type, parent_receipt_bk, total_amount, currency)
VALUES ('REFUND-CONTRACT', 'SHIFT-CONTRACT', 'CASH-CONTRACT', '2025-02-02 12:00+00',
        'refund', 'SALE-CONTRACT', 5, 'RUB');
INSERT INTO receipt_line
(receipt_bk, line_no, sku, batch_bk, quantity, unit_price, line_discount,
 is_rx_snapshot, sku_name_snapshot, batch_expiry_date_snapshot)
VALUES ('REFUND-CONTRACT', 1, 'SKU-A', 'BATCH-A', 1, 5, 0, false, 'SKU A', '2026-01-01');
INSERT INTO payment (receipt_bk, payment_no, payment_method, amount, currency)
VALUES ('REFUND-CONTRACT', 1, 'cash', 5, 'RUB');

SELECT 'SMOKE:pos_receipt', count(*) FROM contract.v_receipt
 WHERE receipt_bk = 'SALE-CONTRACT' AND pharmacy_bk = 'PH-CONTRACT'
   AND discount_amount = 2;
SELECT 'SMOKE:pos_lines', count(*) FROM (
    SELECT 1 FROM contract.v_receipt_line
     WHERE receipt_bk = 'SALE-CONTRACT'
    HAVING count(*) = 2 AND sum(line_amount) = 20
) q;
SELECT 'SMOKE:pos_payments', count(*) FROM (
    SELECT 1 FROM contract.v_payment
     WHERE receipt_bk = 'SALE-CONTRACT'
    HAVING count(*) = 2 AND sum(amount) = 20
) q;
SELECT 'SMOKE:pos_refund', count(*) FROM contract.v_receipt
 WHERE receipt_bk = 'REFUND-CONTRACT' AND doc_type = 'refund'
   AND parent_receipt_bk = 'SALE-CONTRACT';
