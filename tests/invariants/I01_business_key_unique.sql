-- I1: business key уникален.
-- Сущность: catalog.manufacturer, колонка manufacturer_bk (PRIMARY KEY).
-- Условие: повторная вставка строки с уже существующим manufacturer_bk = 'MFR-I01'
-- должна упасть с 23505 (unique violation) на второй INSERT.

INSERT INTO manufacturer (manufacturer_bk, manufacturer_name)
VALUES ('MFR-I01', 'Invariant I01 Manufacturer A');

INSERT INTO manufacturer (manufacturer_bk, manufacturer_name)
VALUES ('MFR-I01', 'Invariant I01 Manufacturer B');
