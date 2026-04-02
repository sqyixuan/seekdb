--disable_result_log
--disable_abort_on_error
delete from t;
## int8 ##
--error 1264
insert into t(col_int8) values (-129);
insert into t(col_int8) values (-128);
insert into t(col_int8) values (127);
--error 1264
insert into t(col_int8) values (128);
## int16 ##
--error 1264
insert into t(col_int16) values (-32769);
insert into t(col_int16) values (-32768);
insert into t(col_int16) values (32767);
--error 1264
insert into t(col_int16) values (32768);
## int24 ##
--error 1264
insert into t(col_int24) values (-8388609);
insert into t(col_int24) values (-8388608);
insert into t(col_int24) values (8388607);
--error 1264
insert into t(col_int24) values (8388608);
## int32 ##
--error 1264
insert into t(col_int32) values (-2147483649);
insert into t(col_int32) values (-2147483648);
insert into t(col_int32) values (2147483647);
--error 1264
insert into t(col_int32) values (2147483648);
## int64 ##
--error 1265
insert into t(col_int64) values ('11a');
insert into t(col_int64) values ('22.4 ');
insert into t(col_int64) values (' 33.5');
insert into t(col_int64) values (' 44 ');
--error 1265
insert into t(col_int64) values ('55 5');
--error 1265
insert into t(col_int64) values ('66 a');
--error 1265
insert into t(col_int64) values ('77. 9');
--error 1265
insert into t(col_int64) values ('88.a');
--error 1265
insert into t(col_int64) values ('99.9a');
--error 1264
insert into t(col_int64) values (-9223372036854775809);
insert into t(col_int64) values (-9223372036854775808);
insert into t(col_int64) values (9223372036854775807);
--error 1264
insert into t(col_int64) values (9223372036854775808);
## uint8 ##
--error 1264
insert into t(col_uint8) values (-1);
insert into t(col_uint8) values (0);
insert into t(col_uint8) values (255);
--error 1264
insert into t(col_uint8) values (256);
## uint16 ##
--error 1264
insert into t(col_uint16) values (-1);
insert into t(col_uint16) values (0);
insert into t(col_uint16) values (65535);
--error 1264
insert into t(col_uint16) values (65536);
## uint24 ##
--error 1264
insert into t(col_uint24) values (-1);
insert into t(col_uint24) values (0);
insert into t(col_uint24) values (16777215);
--error 1264
insert into t(col_uint24) values (16777216);
## uint32 ##
--error 1264
insert into t(col_uint32) values (-1);
insert into t(col_uint32) values (0);
insert into t(col_uint32) values (4294967295);
--error 1264
insert into t(col_uint32) values (4294967296);
## uint64 ##
--error 1265
insert into t(col_uint64) values ('11a');
insert into t(col_uint64) values ('22.4 ');
insert into t(col_uint64) values (' 33.5');
insert into t(col_uint64) values (' 44 ');
--error 1265
insert into t(col_uint64) values ('55 5');
--error 1265
insert into t(col_uint64) values ('66 a');
--error 1265
insert into t(col_uint64) values ('77. 9');
--error 1265
insert into t(col_uint64) values ('88.a');
--error 1265
insert into t(col_uint64) values ('99.9a');
--error 1264
insert into t(col_uint64) values (-1);
insert into t(col_uint64) values (0);
insert into t(col_uint64) values (18446744073709551615);
--error 1264
insert into t(col_uint64) values (18446744073709551616);
## year ##
--error 1265
insert into t(col_year) values ('2001a');
--error 1264
insert into t(col_year) values ('201a');
--error 1265
insert into t(col_year) values ('21a');
--error 1265
insert into t(col_year) values ('2a');
--error 1264
insert into t(col_year) values (1900);
insert into t(col_year) values (1901);
insert into t(col_year) values (2155);
--error 1264
insert into t(col_year) values (2156);
## varchar ##
insert into t(col_varchar) values ('bb        ');
--error 1406
insert into t(col_varchar) values ('cc       !');
insert into t(col_varchar) values (0x6565202020202020202020);
--error 1406
insert into t(col_varchar) values (0x6666202020202020202021);
--error 1406
insert into t(col_varchar) values (0x6767000000000000000000);
--error 1406
insert into t(col_varchar) values (0x6868000000000000000021);
insert into t(col_varchar) values ('支付宝          ');
--error 1406
insert into t(col_varchar) values ('淘宝           !');
## char ##
insert into t(col_char) values ('bb        ');
--error 1406
insert into t(col_char) values ('cc       !');
insert into t(col_char) values (0x6565202020202020202020);
--error 1406
insert into t(col_char) values (0x6666202020202020202021);
--error 1406
insert into t(col_char) values (0x6767000000000000000000);
--error 1406
insert into t(col_char) values (0x6868000000000000000021);
insert into t(col_char) values ('支付宝          ');
--error 1406
insert into t(col_char) values ('淘宝           !');
## varbinary ##
--error 1406
insert into t(col_varbinary) values ('bb        ');
--error 1406
insert into t(col_varbinary) values ('cc       !');
--error 1406
insert into t(col_varbinary) values (0x6565202020202020202020);
--error 1406
insert into t(col_varbinary) values (0x6666202020202020202021);
--error 1406
insert into t(col_varbinary) values (0x6767000000000000000000);
--error 1406
insert into t(col_varbinary) values (0x6868000000000000000021);
--error 1406
insert into t(col_varbinary) values ('支付宝          ');
--error 1406
insert into t(col_varbinary) values ('淘宝           !');
## binary ##
--error 1406
insert into t(col_binary) values ('bb        ');
--error 1406
insert into t(col_binary) values ('cc       !');
--error 1406
insert into t(col_binary) values (0x6565202020202020202020);
--error 1406
insert into t(col_binary) values (0x6666202020202020202021);
--error 1406
insert into t(col_binary) values (0x6767000000000000000000);
--error 1406
insert into t(col_binary) values (0x6868000000000000000021);
--error 1406
insert into t(col_binary) values ('支付宝          ');
--error 1406
insert into t(col_binary) values ('淘宝           !');
## string column with length is 0 ##
insert into t(col_varchar0) values ('');
--error 1406
insert into t(col_varchar0) values ('aa');
insert into t(col_varbinary0) values ('');
--error 1406
insert into t(col_varbinary0) values ('aa');
--enable_abort_on_error
--enable_result_log
