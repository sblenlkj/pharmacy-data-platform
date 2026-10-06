INSERT INTO manufacturer (manufacturer_bk, manufacturer_name)
VALUES ('MFR-I02-A', 'Invariant I02 Manufacturer');

UPDATE manufacturer
   SET manufacturer_bk = 'MFR-I02-B'
 WHERE manufacturer_bk = 'MFR-I02-A';
