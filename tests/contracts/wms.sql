INSERT INTO supplier (supplier_bk, supplier_name, country, is_active)
VALUES ('SUP-CONTRACT', 'Contract Supplier', 'RU', true);
INSERT INTO distribution_center (dc_bk, dc_name, region, city, is_active)
VALUES ('DC-CONTRACT', 'Contract DC', 'Moscow', 'Moscow', true);
INSERT INTO purchase_order (purchase_bk, supplier_bk, dc_bk, ordered_at)
VALUES ('PO-CONTRACT', 'SUP-CONTRACT', 'DC-CONTRACT', '2025-01-01 10:00+00');
INSERT INTO batch
(batch_bk, sku, series_no, manufactured_date, expiry_date, received_at)
VALUES ('BATCH-CONTRACT', 'SKU-CONTRACT', 'SERIES-CONTRACT', '2024-12-01',
        '2026-12-01', '2025-01-10 10:00+00');
INSERT INTO purchase_line
(purchase_bk, line_no, sku, batch_bk, quantity, unit_cost, currency)
VALUES ('PO-CONTRACT', 1, 'SKU-CONTRACT', 'BATCH-CONTRACT', 10, 100, 'RUB'),
       ('PO-CONTRACT', 2, 'SKU-PENDING', NULL, 5, 50, 'RUB');
INSERT INTO movement
(movement_bk, batch_bk, movement_type, src_type, src_bk, dst_type, dst_bk,
 qty, dispatched_at, moved_at)
VALUES ('MOV-CONTRACT-IN', 'BATCH-CONTRACT', 'receipt', 'supplier', 'SUP-CONTRACT',
        'dc', 'DC-CONTRACT', 10, '2025-01-09 10:00+00', '2025-01-10 10:00+00');
INSERT INTO movement
(movement_bk, batch_bk, movement_type, src_type, src_bk, dst_type, dst_bk,
 qty, dispatched_at, moved_at)
VALUES ('MOV-CONTRACT-OUT', 'BATCH-CONTRACT', 'transfer', 'dc', 'DC-CONTRACT',
        'pharmacy', 'PH-CONTRACT', 4, '2025-01-11 10:00+00', '2025-01-12 10:00+00');

SELECT 'SMOKE:wms_received_line', count(*) FROM contract.v_purchase_line
 WHERE purchase_bk = 'PO-CONTRACT' AND line_no = 1
   AND received_at = '2025-01-10 10:00+00' AND line_cost = 1000;
SELECT 'SMOKE:wms_pending_line', count(*) FROM contract.v_purchase_line
 WHERE purchase_bk = 'PO-CONTRACT' AND line_no = 2
   AND received_at IS NULL AND batch_bk IS NULL AND line_cost = 250;
SELECT 'SMOKE:wms_movement', count(*) FROM contract.v_movement
 WHERE movement_bk = 'MOV-CONTRACT-OUT' AND sku = 'SKU-CONTRACT';
SELECT 'SMOKE:wms_stock', count(*) FROM (
    SELECT 1 FROM contract.v_stock
     WHERE batch_bk = 'BATCH-CONTRACT'
    HAVING count(*) = 2 AND sum(qty_on_hand) = 10
       AND count(*) FILTER (WHERE location_type = 'dc' AND qty_on_hand = 6) = 1
       AND count(*) FILTER (WHERE location_type = 'pharmacy' AND qty_on_hand = 4) = 1
) q;
