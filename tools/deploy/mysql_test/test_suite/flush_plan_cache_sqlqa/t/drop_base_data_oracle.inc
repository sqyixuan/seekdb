--disable_warnings
--error 0,942
DROP TABLE temp_fee2 CASCADE CONSTRAINTS purge;
--error 0,01418
drop index idx_b2;
--error 0,01418
drop index idx_c2;
--enable_warnings

--disable_warnings
--error 0,942
DROP TABLE temp_fee CASCADE CONSTRAINTS purge;
--error 0,01418
drop index idx_b;
--error 0,01418
drop index idx_c;
--enable_warnings

--disable_warnings
--error 0,942
drop table gbktable CASCADE CONSTRAINTS purge;
--enable_warnings

SET NAMES UTF8;