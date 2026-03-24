select to_base64(c1) from base_test1;
select from_base64(c1) from base_test1;
select from_base64('MTI3') from base_test1;
select from_base64(to_base64(c1)) from base_test1;

select to_base64(c2) from base_test1;
select from_base64(c2) from base_test1;
select from_base64('MzI3Njc=') from base_test1;
select from_base64(to_base64(c2)) from base_test1;

select to_base64(c3) from base_test1;
select from_base64(c3) from base_test1;
select from_base64('ODM4ODYwNw==') from base_test1;
select from_base64(to_base64(c3)) from base_test1;

select to_base64(c4) from base_test1;
select from_base64(c4) from base_test1;
select from_base64('MjE0NzQ4MzY0Nw==') from base_test1;
select from_base64(to_base64(c4)) from base_test1;

select to_base64(c5) from base_test1;
select from_base64(c5) from base_test1;
select from_base64('MTIzNDU2Nzg5MTIzNDU2Nzg=') from base_test1;
select from_base64(to_base64(c5)) from base_test1;

select to_base64(c6) from base_test1;
select from_base64(c6) from base_test1;
select from_base64('MS4xMTEx') from base_test1;
select from_base64(to_base64(c6)) from base_test1;

select to_base64(c7) from base_test1;
select from_base64(c7) from base_test1;
select from_base64('MTIzLjEyMzExMjM=') from base_test1;
select from_base64(to_base64(c7)) from base_test1;

select to_base64(c8) from base_test1;
select from_base64(c8) from base_test1;
select from_base64('MS4xMA==') from base_test1;
select from_base64(to_base64(c8)) from base_test1;

select to_base64(c9) from base_test1;
select from_base64(c9) from base_test1;
select from_base64('YXNkZuaxieWtlw==') from base_test1;
select from_base64(to_base64(c9)) from base_test1;

select to_base64(c10) from base_test1;
select from_base64(c10) from base_test1;
select from_base64('ZGphbHNrZGpmbGFzamTmsYnlrZfllYrmiYvliqjpmIDvvIFAI0Dvv6Ul4oCm4oCmJirvvIjvvInigJTigJQr') from base_test1;
select from_base64(to_base64(c10)) from base_test1;

select to_base64(c11) from base_test1;
select from_base64(c11) from base_test1;
select from_base64('AQIDBP////8AAAAMqssAAA==') from base_test1;
select from_base64(to_base64(c11)) from base_test1;
select from_base64(hex('AQIDBP////8AAAAMqssAAA==')) from base_test1;
select from_base64(to_base64(hex(c11))) from base_test1;

select to_base64(c12) from base_test1;
select from_base64(c12) from base_test1;
select from_base64('AEE=') from base_test1;
select from_base64(to_base64(c12)) from base_test1;





select to_base64(c1) from base_test2;
select from_base64(c1) from base_test2;
select from_base64('ZGphbHNrZGpmbGFzamTmsYnlrZfllYrmiYvliqjpmIDvvIFAI0Dvv6Ul4oCm4oCmJirvvIjvvInigJTigJQr') from base_test2;
select from_base64(to_base64(c1)) from base_test2;

select to_base64(c2) from base_test2;
select from_base64(c2) from base_test2;
select from_base64('YWJjAAAAAAAAAA==') from base_test2;
select from_base64(to_base64(c2)) from base_test2;

select to_base64(c3) from base_test2;
select from_base64(c3) from base_test2;
select from_base64('YWJjZGVmZ2hpamtsbW5vcHFyc3R1dnd4eXo=') from base_test2;
select from_base64(to_base64(c3)) from base_test2;

select to_base64(c4) from base_test2;
select from_base64(c4) from base_test2;
select from_base64('bWVkaXVt') from base_test2;
select from_base64(to_base64(c4)) from base_test2;

select to_base64(c5) from base_test2;
select from_base64(c5) from base_test2;
select from_base64('YSxiLGM=') from base_test2;
select from_base64(to_base64(c5)) from base_test2;

select to_base64(c6) from base_test2;
select from_base64(c6) from base_test2;
select from_base64('OTk5OS0xMi0zMQ==') from base_test2;
select from_base64(to_base64(c6)) from base_test2;

select to_base64(c7) from base_test2;
select from_base64(c7) from base_test2;
select from_base64('MjA6NTk6NTk=') from base_test2;
select from_base64(to_base64(c7)) from base_test2;

select to_base64(c8) from base_test2;
select from_base64(c8) from base_test2;
select from_base64('MjAyMg==') from base_test2;
select from_base64(to_base64(c8)) from base_test2;

select to_base64(c9) from base_test2;
select from_base64(c9) from base_test2;
select from_base64('MjAyMS0wMS0wMSAwMDowMDowMA==') from base_test2;
select from_base64(to_base64(c9)) from base_test2;

select to_base64(c10) from base_test2;
select from_base64(c10) from base_test2;
select from_base64('MjAxOC0wMS0xOSAwMzoxNDowOA==') from base_test2;
select from_base64(to_base64(c10)) from base_test2;

select to_base64(c11) from base_test2;
select from_base64(c11) from base_test2;
select from_base64('W3siYSI6IDF9LCAiMTIzYXNkZiJd') from base_test2;
select from_base64(to_base64(c11)) from base_test2;




--error 0,1051
DROP TABLE base_test1;
--error 0,1051
DROP TABLE base_test2;

