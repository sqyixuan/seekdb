INSERT INTO zc_t1 (pk, c1, c2, c3) VALUES (0,NULL,NULL,NULL);
INSERT INTO zc_t1 (pk, c1, c2, c3) VALUES (68,-52,45,-86);
INSERT INTO zc_t1 (pk, c1, c2, c3) VALUES (51,27,-6,-8);
INSERT INTO zc_t1 (pk, c1, c2, c3) VALUES (-13,47,-47,26);
INSERT INTO zc_t1 (pk, c1, c2, c3) VALUES (-24,82,75,-10);
INSERT INTO zc_t1 (pk, c1, c2, c3) VALUES (-67,-86,72,62);
INSERT INTO zc_t1 (pk, c1, c2, c3) VALUES (18,41,-77,-12);
INSERT INTO zc_t1 (pk, c1, c2, c3) VALUES (14,-15,-53,65);
INSERT INTO zc_t1 (pk, c1, c2, c3) VALUES (39,-29,65,-62);
INSERT INTO zc_t1 (pk, c1, c2, c3) VALUES (89,18,93,-75);
INSERT INTO zc_t1 (pk, c1, c2, c3) VALUES (88,-99,9,35);

UPDATE zc_t1 SET pk = 10, c1 = 20, c2 = 30, c3 = 40 WHERE pk = (SELECT MIN(pk) FROM zc_t1);

UPDATE zc_t1 SET c1 = 92 WHERE MOD(pk, 2) = 0; 

DELETE FROM zc_t1 WHERE c1 > 50 AND c1 < 70; 

UPDATE zc_t1 SET c2 = c2 + 15; 

UPDATE zc_t1 SET c3 = pk, c1 = c2 WHERE pk < c3;

INSERT INTO zc_t1 (pk, c1, c2, c3) VALUES (25, 50, 75, 100);

UPDATE zc_t1 SET pk = 83, c1 = 54, c2 = 39, c3 = 22 WHERE pk = (SELECT MAX(pk) FROM zc_t1);

DELETE FROM zc_t1 WHERE pk BETWEEN 10 AND 20;
