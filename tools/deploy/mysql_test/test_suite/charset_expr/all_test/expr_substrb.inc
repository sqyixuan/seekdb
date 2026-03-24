--result_format 4
--echo // test utf16
--error 0, 942
drop table utf16_substrb_tbl;
create table utf16_substrb_tbl(c1 nvarchar2(100));
--echo // 'abcd' -> '0061006200630064'
insert into utf16_substrb_tbl values('abcd');

select rawtohex(c1), substrb(c1, 0, 0), rawtohex(substrb(c1, 0, 0)) from utf16_substrb_tbl;

select rawtohex(c1), substrb(c1, 0, 1), rawtohex(substrb(c1, 0, 1)) from utf16_substrb_tbl;
select rawtohex(c1), substrb(c1, 0, 3), rawtohex(substrb(c1, 0, 3)) from utf16_substrb_tbl;
select rawtohex(c1), substrb(c1, 0, 4), rawtohex(substrb(c1, 0, 4)) from utf16_substrb_tbl;
select rawtohex(c1), substrb(c1, 0, 5), rawtohex(substrb(c1, 0, 5)) from utf16_substrb_tbl;
select rawtohex(c1), substrb(c1, 0), rawtohex(substrb(c1, 0)) from utf16_substrb_tbl;

select rawtohex(c1), substrb(c1, 1, 0), rawtohex(substrb(c1, 1, 0)) from utf16_substrb_tbl;
select rawtohex(c1), substrb(c1, 1, 1), rawtohex(substrb(c1, 1, 1)) from utf16_substrb_tbl;
select rawtohex(c1), substrb(c1, 1, 3), rawtohex(substrb(c1, 1, 3)) from utf16_substrb_tbl;
select rawtohex(c1), substrb(c1, 1, 4), rawtohex(substrb(c1, 1, 4)) from utf16_substrb_tbl;
select rawtohex(c1), substrb(c1, 1, 5), rawtohex(substrb(c1, 1, 5)) from utf16_substrb_tbl;
select rawtohex(c1), substrb(c1, 1), rawtohex(substrb(c1, 1)) from utf16_substrb_tbl;

select rawtohex(c1), substrb(c1, 2, 0), rawtohex(substrb(c1, 2, 0)) from utf16_substrb_tbl;
select rawtohex(c1), substrb(c1, 2, 1), rawtohex(substrb(c1, 2, 1)) from utf16_substrb_tbl;
--echo // bug should return null
select rawtohex(c1), substrb(c1, 2, 2), rawtohex(substrb(c1, 2, 2)), rawtohex(substrb(c1, 2, 2)) from utf16_substrb_tbl;
select rawtohex(c1), substrb(c1, 2, 3), rawtohex(substrb(c1, 2, 3)), rawtohex(substrb(c1, 2, 3)) from utf16_substrb_tbl;
--echo // bug should return 'b'
select rawtohex(c1), substrb(c1, 2, 4), rawtohex(substrb(c1, 2, 4)), rawtohex(substrb(c1, 2, 4)) from utf16_substrb_tbl;

--echo // start pos is negative
--echo // 'abcd' -> '0061006200630064'
select rawtohex(c1), substrb(c1, -1, 0), rawtohex(substrb(c1, -1, 0)) from utf16_substrb_tbl;
select rawtohex(c1), substrb(c1, -1, 1), rawtohex(substrb(c1, -1, 1)) from utf16_substrb_tbl;
select rawtohex(c1), substrb(c1, -1), rawtohex(substrb(c1, -1)) from utf16_substrb_tbl;

select rawtohex(c1), substrb(c1, -2, 0), rawtohex(substrb(c1, -2, 0)) from utf16_substrb_tbl;
select rawtohex(c1), substrb(c1, -2, 1), rawtohex(substrb(c1, -2, 1)) from utf16_substrb_tbl;
select rawtohex(c1), substrb(c1, -2), rawtohex(substrb(c1, -2)) from utf16_substrb_tbl;

select rawtohex(c1), substrb(c1, -3, 0), rawtohex(substrb(c1, -3, 0)) from utf16_substrb_tbl;
--echo // bug here, expected null
select rawtohex(c1), substrb(c1, -3, 1), rawtohex(substrb(c1, -3, 1)) from utf16_substrb_tbl;
--echo // bug here, expected 'd'
select rawtohex(c1), substrb(c1, -3), rawtohex(substrb(c1, -3)) from utf16_substrb_tbl;

