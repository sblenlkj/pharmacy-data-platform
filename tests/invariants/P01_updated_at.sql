INSERT INTO manufacturer (manufacturer_bk, manufacturer_name)
VALUES ('MFR-P01', 'Positive P01 Manufacturer');
SELECT pg_sleep(0.01);
UPDATE manufacturer SET manufacturer_name = 'Positive P01 Manufacturer Updated'
WHERE manufacturer_bk = 'MFR-P01';
SELECT updated_at > created_at AS ok FROM manufacturer WHERE manufacturer_bk = 'MFR-P01';
