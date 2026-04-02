--echo #############类型函数嵌套使用 number_format_model-1-3及之下所有##############

--echo #############to_char嵌套to_char number_format_model-1-3-1##############

select to_char(to_char(1234.34,'9,,,,,,,,,,99,9.99')) from dual;
select to_char(to_char(1234.34,'0,0,0,0.00')) from dual;
select to_char(to_char(1234,'999900EEEE')) from dual;
select to_char(to_char(29989329, '9.99EEEE')) from dual;
select to_char(to_char(12345, '9,,,9,,9,9,9')) from dual;
select to_char(to_char(123*0.34,'9999.9')) from dual;
select to_char(to_char(12,'9990999.9eeee')) from dual;
select to_char(to_char(-485,'999V')) from dual;
select to_char(to_char(1234.34,'0000d00l')) from dual;
select to_char(to_char(100000000,'99G9999999v')) from dual;
select to_char(to_char(1234.34,'0000d00')) from dual;
select to_char(to_char(12345,'9G9G9G9g9')) from dual;
select to_char(to_char(-485,'99$9Mi')) from dual;
select to_char(to_char(100.00, '9,$99999999B')) from dual;
select to_char(to_char(12,'9990999.$9')) from dual;
select to_char(to_char(52345/5, '9,9,9,$9,9')) from dual;
select to_char(to_char(23.12, '99.99BCMI')) from dual;
select to_char(to_char(100000000, '9,9999,9999BL')) from dual;
select to_char(to_char(23.12, '99D99BUS')) from dual;
###select to_char(to_char(485,'999UEEEE')) from dual;
select to_char(to_char(100.2356, '99999,99C')) from dual;
select to_char(to_char(123, '999999999L')) from dual;
select to_char(to_char(100.2356, '99999,99U')) from dual;
select to_char(to_char(1,'9BEEEE'))  from dual;
###select to_char(to_char(123, '9999V999999EEEE')) from dual;
select to_char(to_char(20080320.2365, '9.9EEEE')) from dual;
select to_char(to_char(123, '9999999999EEEE')) from dual;
select to_char(to_char(20, 'fmRn')) from dual;
select to_char(to_char(0.000, 'FMRn')) from dual;
select to_char(to_char(123, 'RN')) from dual;
select to_char(to_char(3999, 'rN')) from dual;
select to_char(to_char(100.2356, '99999,99VC')) from dual;
select to_char(to_char(123, '999999V999S')) from dual;
select to_char(to_char(0.1,'0,9VC'))  from dual;
select to_char(to_char(123, '999999V999')) from dual;
select to_char(to_char(1485,'XXX')) from dual;
select to_char(to_char(100.2356, 'FM0XXX')) from dual;
###select to_char(to_char(1234.34,'0000.00X')) from dual;
select to_char(to_char(485,'XXX')) from dual;
select to_char(to_char(1234.34,'xxx')) from dual;
select to_char(to_char(12345, '9,,,9,,9,9,9$Mi')) from dual;
select to_char(to_char(0+0, '9,9,9,9,9vPR')) from dual;
select to_char(to_char(1234, 'fm9999s')) from dual;
select to_char(to_char(1234, 'sb9999')) from dual;
select to_char(to_char(1234, 'L9999Mi')) from dual;
select to_char(to_char(100.2356, '9,99999999Mi')) from dual;
select to_char(to_char(12,'9990999.9PR')) from dual;
select to_char(to_char(0*0, '9CB')) from dual;
select to_char(to_char(1*0.3, 'FM9B')) from dual;
select to_char(to_char(123, '9B')) from dual;
select to_char(to_char(12345, '9,,,9,,9,9,9B')) from dual;
select to_char(to_char(100000000, 'FM9,9999,9999c')) from dual;
select to_char(to_char(12,'FM9990999.9pr')) from dual;
select to_char(to_char(12,'FM9990999.9')) from dual;
select to_char(to_char(1234.34,'FM9,9,9,9.99')) from dual;
select to_char(to_char(1234, 'fmtm9')) from dual;
select to_char(to_char('', 'fmtme')) from dual;
select to_char(to_char(12345+12345, 'tM9')) from dual;
select to_char(to_char(123*0.34,'tM9')) from dual;


--echo #############to_char嵌套to_number number_format_model-1-3-2##############
--error 01722
select to_number(to_char(1234.34,'9,,,,,,,,,,99,9.99')) from dual;
--error 01722
select to_number(to_char(1234.34,'0,0,0,0.00')) from dual;
select to_number(to_char(1234,'999900EEEE')) from dual;
select to_number(to_char(29989329, '9.99EEEE')) from dual;
--error 01722
select to_number(to_char(12345, '9,,,9,,9,9,9')) from dual;
select to_number(to_char(123*0.34,'9999.9')) from dual;
select to_number(to_char(12,'9990999.9eeee')) from dual;
select to_number(to_char(-485,'999V')) from dual;
--error 01722
select to_number(to_char(1234.34,'0000d00l')) from dual;
--error 01722
select to_number(to_char(100000000,'99G9999999v')) from dual;
select to_number(to_char(1234.34,'0000d00')) from dual;
--error 01722
select to_number(to_char(12345,'9G9G9G9g9')) from dual;
--error 01722
select to_number(to_char(-485,'99$9Mi')) from dual;
--error 01722
select to_number(to_char(100.00, '9,$99999999B')) from dual;
--error 01722
select to_number(to_char(12,'9990999.$9')) from dual;
--error 01722
select to_number(to_char(52345/5, '9,9,9,$9,9')) from dual;
--error 01722
select to_number(to_char(23.12, '99.99BCMI')) from dual;
--error 01722
select to_number(to_char(100000000, '9,9999,9999BL')) from dual;
--error 01722
select to_number(to_char(23.12, '99D99BUS')) from dual;
###to_number 处理to_char处理后的数据,因to_char(485,'999UEEEE')处理结果有问题
###--error 01722
###select to_number(to_char(485,'999UEEEE')) from dual;
--error 01722
select to_number(to_char(100.2356, '99999,99C')) from dual;
--error 01722
select to_number(to_char(123, '999999999L')) from dual;
--error 01722
select to_number(to_char(100.2356, '99999,99U')) from dual;
select to_number(to_char(1,'9BEEEE'))  from dual;
####select to_number(to_char(123, '9999V999999EEEE')) from dual;
select to_number(to_char(20080320.2365, '9.9EEEE')) from dual;
select to_number(to_char(123, '9999999999EEEE')) from dual;
--error 01722
select to_number(to_char(20, 'fmRn')) from dual;
--error 01722
select to_number(to_char(0.000, 'FMRn')) from dual;
--error 01722
select to_number(to_char(123, 'RN')) from dual;
--error 01722
select to_number(to_char(3999, 'rN')) from dual;
--error 01722
select to_number(to_char(100.2356, '99999,99VC')) from dual;
--error 01722
select to_number(to_char(123, '999999V999S')) from dual;
--error 01722
select to_number(to_char(0.1,'0,9VC'))  from dual;
select to_number(to_char(123, '999999V999')) from dual;
--error 01722
select to_number(to_char(1485,'XXX')) from dual;
###select to_number(to_char(100.2356, 'FM0XXX')) from dual;
###--error 01722
###select to_number(to_char(1234.34,'0000.00X')) from dual;
select to_number(to_char(485,'XXX')) from dual;
--error 01722
select to_number(to_char(1234.34,'xxx')) from dual;
--error 01722
select to_number(to_char(12345, '9,,,9,,9,9,9$Mi')) from dual;
select to_number(to_char(0+0, '9,9,9,9,9vPR')) from dual;
--error 01722
select to_number(to_char(1234, 'fm9999s')) from dual;
select to_number(to_char(1234, 'sb9999')) from dual;
--error 01722
select to_number(to_char(1234, 'L9999Mi')) from dual;
select to_number(to_char(100.2356, '9,99999999Mi')) from dual;
select to_number(to_char(12,'9990999.9PR')) from dual;
--error 01722
select to_number(to_char(0*0, '9CB')) from dual;
###select to_number(to_char(1*0.3, 'FM9B')) from dual;
###select to_char(1*0.3, 'FM9B') from dual;
--error 01722
select to_number(to_char(123, '9B')) from dual;
--error 01722
select to_number(to_char(12345, '9,,,9,,9,9,9B')) from dual;
--error 01722
select to_number(to_char(100000000, 'FM9,9999,9999c')) from dual;
select to_number(to_char(12,'FM9990999.9pr')) from dual;
select to_number(to_char(12,'FM9990999.9')) from dual;
--error 01722
select to_number(to_char(1234.34,'FM9,9,9,9.99')) from dual;
select to_number(to_char(1234, 'fmtm9')) from dual;
select to_number(to_char('', 'fmmtme')) from dual;
select to_number(to_char(12345+12345, 'tM9')) from dual;
select to_number(to_char(123*0.34,'tM9')) from dual;


select to_char(to_number(to_char(1234,'999900EEEE'))) from dual;
select to_char(to_number(to_char(29989329, '9.99EEEE'))) from dual;
select to_char(to_number(to_char(123*0.34,'9999.9'))) from dual;
select to_char(to_number(to_char(12,'9990999.9eeee'))) from dual;
select to_char(to_number(to_char(-485,'999V'))) from dual;
select to_char(to_number(to_char(1,'9BEEEE')))  from dual;
###select to_char(to_number(to_char(123, '9999V999999EEEE'))) from dual;
select to_char(to_number(to_char(20080320.2365, '9.9EEEE'))) from dual;
select to_char(to_number(to_char(123, '9999999999EEEE'))) from dual;
select to_char(to_number(to_char(123, '999999V999'))) from dual;
select to_char(to_number(to_char(100.2356, 'FM0XXX'))) from dual;
select to_char(to_number(to_char(485,'XXX'))) from dual;
select to_char(to_number(to_char(0+0, '9,9,9,9,9vPR'))) from dual;
select to_char(to_number(to_char(1234, 'sb9999'))) from dual;
select to_char(to_number(to_char(100.2356, '9,99999999Mi'))) from dual;
select to_char(to_number(to_char(12,'9990999.9PR'))) from dual;
###select to_char(to_number(to_char(1*0.3, 'FM9B'))) from dual;
select to_char(to_number(to_char(12,'FM9990999.9pr'))) from dual;
select to_char(to_number(to_char(12,'FM9990999.9'))) from dual;
select to_char(to_number(to_char(1234, 'fmtm9'))) from dual;
select to_char(to_number(to_char('', 'fmmtme'))) from dual;
select to_char(to_number(to_char(12345+12345, 'tM9'))) from dual;
select to_char(to_number(to_char(123*0.34,'tM9'))) from dual;


--echo #############to_char嵌套其他函数 number_format_model-1-3-3##############

select floor(to_number(to_char(1234,'999900EEEE'))) from dual;
select decode(to_char(1234.34,'0000d00l'),to_char(1234.34,'0000d00l'),'好的','1234.34$$','不好','1234.34$$$','再说') from dual;
select decode(to_char(1234.34,'0000d00l'),'1234.34$$','好的',to_char(1234.34,'0000d00l'),'不好','1234.34$$$','再说') from dual;
select decode(to_char(1234.34,'0000d00'),'1234.34','好的','1234.34$$','不好','1234.34$$$','再说') from dual;
select decode(to_char(1234.34,'0000d00'),'1234.34','haode','1234.34$$','buhao','1234.34$$$','zaishuo') from dual;
select decode(to_char(3999, 'rN'),'mmmcmxcix','haode','1234.34$$','buhao','1234.34$$$','zaishuo') from dual;
select decode(to_char(100.00, '9,$99999999B'),'$100','haode','1234.34$$','buhao','1234.34$$$','zaishuo') from dual;
select replace(to_char(1234.34,'0000d00l'),'1234.34$','好的好的') from dual;
select replace(to_char(1234.34,'0000d00'),'1234.34','好的好的') from dual;
select replace(to_char(1234.34,'0000d00'),'1234.34','好的好的') from dual;
select replace(to_char(3999, 'rN'),'mmmcmxcix','好的好的') from dual;
select replace(to_char(100.00, '9,$99999999B'),'$100','好的好的') from dual;
select translate(to_char(1234.34,'9,,,,,,,,,,99,9.99'),'1,,,,,,,,,,23,4.34','wo') 返回值 from dual;
select translate(to_char(123*0.34,'tM9'),'41.82','英文') 返回值 from dual;
select translate(to_char(12,'9990999.9PR'),'0012.0','wo') 返回值 from dual;
--error 00909,900
select translate(to_char(100000000, 'FM9,9999,9999c'),'to_char(100000000, 'FM9,9999,9999c')','英文') 返回值 from dual;
select length(to_char(12345, '9,,,9,,9,9,9$Mi')) 返回值 from dual;
select length(to_char(1234.34,'xxx')) 返回值 from dual;
select length(to_char(0*0, '9CB')) 返回值 from dual;
select length(to_char(0.000, 'FMRn')) 返回值 from dual;
select lengthb(to_char(12345, '9,,,9,,9,9,9$Mi')) 返回值 from dual;
select lengthb(to_char(1234.34,'xxx')) 返回值 from dual;
select lengthb(to_char(0*0, '9CB')) 返回值 from dual;
select lengthb(to_char(0.000, 'FMRn')) 返回值 from dual;
select cast(to_char(123, '9999999999EEEE') as varchar2(80)) as 新列 from dual;
select cast(to_char(12,'9990999.9eeee') as varchar2(80)) as 新列 from dual;
select cast(to_char(12345,'9G9G9G9g9') as varchar2(8)) as 新列 from dual;
select cast(to_char(485,'XXX') as varchar2(8)) as 新列 from dual;
select cast(to_char(1*0.3, 'FM9B') as varchar2(8)) as 新列 from dual;
select * from dual where  to_char(12,'FM9990999.9pr') like '.%';
select * from dual where  to_char(1234, 'L9999Mi') like '1234%';
--error 01722
select remainder( to_char(1485,'XXX'),2) from dual;
select remainder(to_char(100.2356, 'FM0XXX'),3) from dual;
--error 01722
select remainder( to_char(12345, '9,,,9,,9,9,9$Mi'),2) from dual;
select abs(to_number(to_char(123*0.34,'tM9'))) from dual;
select abs(to_number(to_char(1234, 'fmtm9'))) from dual;
select abs(to_number(to_char(1,'9BEEEE'))) from dual;
select abs(to_number(to_char(123*0.34,'9999.9'))) from dual;
select abs(to_char(123*0.34,'9999.9')) from dual;
select concat(to_char(1234.34,'0000d00l'), to_char(100000000,'99G9999999v')) from dual;
select concat( to_char(12345,'9G9G9G9g9'), to_char(100000000,'99G9999999v')) from dual;
select concat(to_char(1234.34,'0000d00l'),  to_char(52345/5, '9,9,9,$9,9')) from dual;
select RPAD( to_char(23.12, '99.99BCMI'), 200, '$') from dual;
select RPAD(  to_char(100000000, '9,9999,9999BL'), 200, '$') from dual;
###select RPAD( to_char(485,'999UEEEE'), 200, '$') from dual;
select LPAD( to_char(23.12, '99.99BCMI'), 200, '$') from dual;
select LPAD(  to_char(100000000, '9,9999,9999BL'), 200, '$') from dual;
###select LPAD( to_char(485,'999UEEEE'), 200, '$') from dual;
select instr( to_char(100.2356, '99999,99C') ,'00') from dual;
select instr(  to_char(123, '999999999L') ,'00') from dual;
select instr(  to_char(100.2356, '99999,99U') ,'00') from dual;
select instr(  to_char(1,'9BEEEE') ,'00') from dual;
select instrb( to_char(100.2356, '99999,99C') ,'00') from dual;
select instrb(  to_char(123, '999999999L') ,'00') from dual;
select instrb(  to_char(100.2356, '99999,99U') ,'00') from dual;
select instrb(  to_char(1,'9BEEEE') ,'00') from dual;
###select substr(to_char(123, '9999V999999EEEE'),3,5) from dual;
select substr(to_char(20080320.2365, '9.9EEEE'),3,5) from dual;
###select substr( to_char(123, '9999999999EEEE'),3,5) from dual;
select substr(to_char(20, 'fmRn'),3,5) from dual;
###select substrb(to_char(123, '9999V999999EEEE'),3,5) from dual;
select substrb(to_char(20080320.2365, '9.9EEEE'),3,5) from dual;
###select substrb( to_char(123, '9999999999EEEE'),2,5) from dual;
select to_char(123, '9999999999EEEE') from dual;
select substrb(to_char(20, 'fmRn'),3,5) from dual;
select initcap(to_char(0.000, 'FMRn')) from dual;
select initcap(to_char(123, 'RN')) from dual;
select initcap( to_char(3999, 'rN')) from dual;
select initcap(to_char(100.2356, '99999,99VC')) from dual;
select initcap(to_char(123, '999999V999S')) from dual;
select round(to_number(to_char(1234, 'fmtm9'))) from dual;
select round(to_number(to_char(29989329, '9.99EEEE'))) from dual;
select round(to_number(to_char('', 'fmmtme'))) from dual;
SELECT COALESCE(to_char(1485,'XXX'),to_char(1485,'XXX'),'3','4','5') FROM dual;
SELECT COALESCE(to_char(100.2356, 'FM0XXX'),to_char(100.2356, 'FM0XXX'),'3','4','5') FROM dual;
###SELECT COALESCE(to_char(1234.34,'0000.00X'),to_char(1234.34,'0000.00X'),'3','4','5') FROM dual;
SELECT COALESCE(to_char(0*0, '9CB'),to_char(1485,'XXX'),'3','4','5') FROM dual;
select lower( to_char(100.2356, 'FM0XXX'))  from dual;
select lower( to_char(12345, '9,,,9,,9,9,9$Mi'))  from dual;
select lower( to_char(12345, '9,,,9,,9,9,9$Mi'))  from dual;
select lower( to_char(12345, '9,,,9,,9,9,9$Mi'))  from dual;
select upper( to_char(100.2356, 'FM0XXX'))  from dual;
select upper( to_char(12345, '9,,,9,,9,9,9$Mi'))  from dual;
select upper( to_char(12345, '9,,,9,,9,9,9$Mi'))  from dual;
select upper( to_char(12345, '9,,,9,,9,9,9$Mi'))  from dual;
SELECT TRIM(to_char(485,'XXX')) FROM dual;
select length(TRIM(to_char(485,'XXX'))) FROM dual;
SELECT LTRIM(to_char(485,'XXX')) FROM dual;
select length(LTRIM(to_char(485,'XXX'))) FROM dual;
SELECT RTRIM(to_char(485,'XXX')) FROM dual;
SELECT TRIM(to_char(0*0, '9CB')) FROM dual;
select length(TRIM(to_char(0*0, '9CB'))) FROM dual;
SELECT LTRIM(to_char(0*0, '9CB')) FROM dual;
select length(LTRIM(to_char(0*0, '9CB'))) FROM dual;
SELECT RTRIM(to_char(0*0, '9CB')) FROM dual;
--error 30001
SELECT trim(leading 'USD' from  to_char(100000000, 'FM9,9999,9999c')) FROM dual;
--error 30001
SELECT trim(both 'USD' from  to_char(100000000, 'FM9,9999,9999c')) FROM dual;
--error 30001
SELECT trim(trailing 'USD' from  to_char(100000000, 'FM9,9999,9999c')) FROM dual;
select sinh( to_char(12,'FM9990999.9pr'))  FROM dual;
select sinh(to_char(12,'FM9990999.9') )  FROM dual;
--error 01426
select sinh(to_char(1234, 'fmtm9'))  FROM dual;
select cosh( to_char(12,'FM9990999.9pr'))  FROM dual;
select cosh(to_char(12,'FM9990999.9') )  FROM dual;
--error 01426
select cosh(to_char(1234, 'fmtm9'))  FROM dual;
###select tanh( to_char(12,'FM9990999.9pr'))  FROM dual;
###select tanh(to_char(12,'FM9990999.9') )  FROM dual;
--error 01426
select tanh(to_char(1234, 'fmtm9'))  FROM dual;
select nvl(null,to_char('', 'fmmtme')) from dual;
select nvl(null,to_char(12345+12345, 'tM9')) from dual;
select nvl(to_char('', 'fmmtme'),null) from dual;
select nvl(to_char(12345+12345, 'tM9'),null) from dual;
select ceil(to_char(1234, 'fmtm9'))  FROM dual;
select ceil( to_char(12,'FM9990999.9pr'))  FROM dual;
select ceil(to_char(12,'FM9990999.9') )  FROM dual;
select floor(to_char(1234, 'fmtm9'))  FROM dual;
select floor( to_char(12,'FM9990999.9pr'))  FROM dual;
select floor(to_char(12,'FM9990999.9') )  FROM dual;
--error 01428,600
select asin(to_number(to_char(1234, 'fmtm9'))) from dual;
--error 01428,600
select asin(to_number(to_char(29989329, '9.99EEEE'))) from dual;
select asin(to_number(to_char('', 'fmmtme'))) from dual;
--error 01428,600
select acos(to_char(1234, 'fmtm9'))  FROM dual;
--error 01428,600
select acos( to_char(12,'FM9990999.9pr'))  FROM dual;
--error 01428,600
select acos(to_char(12,'FM9990999.9') )  FROM dual;
select atan(to_char(1234, 'fmtm9'))  FROM dual;
select atan( to_char(12,'FM9990999.9pr'))  FROM dual;
select atan(to_char(12,'FM9990999.9') )  FROM dual;
--error 00909
select atan2(to_char(1234, 'fmtm9'))  FROM dual;
--error 00909
select atan2( to_char(12,'FM9990999.9pr'))  FROM dual;
--error 00909
select atan2(to_char(12,'FM9990999.9') )  FROM dual;
select sqrt(to_char(1234, 'fmtm9'))  FROM dual;
select sqrt( to_char(12,'FM9990999.9pr'))  FROM dual;
select sqrt(to_char(12,'FM9990999.9') )  FROM dual;
select sign(to_char(1234, 'fmtm9'))  FROM dual;
select sign( to_char(12,'FM9990999.9pr'))  FROM dual;
select sign(to_char(12,'FM9990999.9') )  FROM dual;
####select CHR(to_char(29989329, '9.99EEEE')) from dual;
--error 01722
select CHR(to_char(1234.34,'0000d00l')) from dual;
--error 01722
select CHR( to_char(12345,'9G9G9G9g9')) from dual;
--error 01722
select CHR(to_char(100.2356, '99999,99C')) from dual;
select ascii(to_char(123, '999999999L')) from dual;
select ascii(to_char(100000000, 'FM9,9999,9999c')) from dual;
select ascii(to_char(12,'FM9990999.9')) from dual;
select ascii( to_char(1234.34,'xxx')) from dual;
select * from dual where ascii(to_char(123, '999999999L'))<>100;
select * from dual where ascii(to_char(100000000, 'FM9,9999,9999c'))<>100;
select * from dual where ascii(to_char(12,'FM9990999.9'))<>100;
select * from dual where  ascii( to_char(1234.34,'xxx'))<>100;
select * from dual where ascii(to_char(123, '999999999L')) is NULL;
select * from dual where ascii(to_char(100000000, 'FM9,9999,9999c')) is NULL;
select * from dual where ascii(to_char(12,'FM9990999.9')) is NULL;
select * from dual where  ascii( to_char(1234.34,'xxx')) is NULL;
select * from dual where ascii(to_char(123, '999999999L')) is not NULL;
select * from dual where ascii(to_char(100000000, 'FM9,9999,9999c')) is not NULL;
select * from dual where ascii(to_char(12,'FM9990999.9')) is not NULL;
select * from dual where  ascii( to_char(1234.34,'xxx')) is not NULL;
select * from dual where ascii(to_char(123, '999999999L')) between 1 and 1000;
select * from dual where ascii(to_char(100000000, 'FM9,9999,9999c')) between 1 and 1000;
select * from dual where ascii(to_char(12,'FM9990999.9')) between 1 and 1000;
select * from dual where  ascii( to_char(1234.34,'xxx')) between 1 and 1000;
###select REGEXP_COUNT(to_char(1234, 'L9999Mi') ,'$',1,'i') from DUAL;
###select REGEXP_COUNT( to_char(123, '999999999L') ,'$',1,'i') from DUAL;
###select REGEXP_COUNT(to_char(23.12, '99D99BUS') ,'$',1,'i') from DUAL;
###select REGEXP_COUNT(to_char(52345/5, '9,9,9,$9,9') ,'$',1,'i') from DUAL;
select * from dual where ascii(to_char(123, '999999999L')) != 100;
select * from dual where ascii(to_char(100000000, 'FM9,9999,9999c'))!= 100;
select * from dual where ascii(to_char(12,'FM9990999.9'))!= 100;
select * from dual where  ascii( to_char(1234.34,'xxx'))!= 100;
###select  to_char(100000000, '9,9999,9999BL')||to_char(485,'999UEEEE') from dual;
select   to_char(0.000, 'FMRn')||to_char(123, 'RN') from dual;
###select   to_char(100.2356, '99999,99VC')||to_char(485,'999UEEEE') from dual;
select q'[to_char(1234.34,'0000.00X')]' names from dual;
select nvl2(null,to_char('', 'fmmtme'),to_char('', 'fmmtme')) from dual;
select nvl2(null,to_char(12345+12345, 'tM9'),to_char('', 'fmmtme'))from dual;
select nvl2(to_char('', 'fmmtme'),null,to_char('', 'fmmtme')) from dual;
select nvl2(to_char(12345+12345, 'tM9'),null,to_char('', 'fmmtme')) from dual;
###select power(to_number(to_char(123, '999999V999')),2) from dual;
select to_number(to_char(123, '999999V999')) from dual;
select power(to_number(to_char(12,'9990999.9PR')),2) from dual;
--error 01426
select exp(to_number(to_char(1, '9999V999999EEEE'))) from dual;
select exp(to_number(to_char(2,'9990999.9eeee'))) from dual;
select ln(to_number(to_char(3, '9999V999999EEEE'))) from dual;
select ln(to_number(to_char(2,'9990999.9eeee'))) from dual;
select ln(to_char(1234, 'sb9999')) from dual;
select ln(to_char(12,'9990999.9PR')) from dual;
select log(to_char(1234, 'sb9999'),2) from dual;
select log(to_char(12,'9990999.9PR'),2) from dual;
select to_single_byte(to_char(12345, '9,,,9,,9,9,9B')) from dual;
select to_multi_byte(to_char(1234, 'L9999Mi')) from dual;
select to_single_byte( to_char(12345, '9,,,9,,9,9,9$Mi') ) from dual;
select to_multi_byte( to_char(485,'XXX') ) from dual;
select utl_i18n.string_to_raw(to_char(12345, '9,,,9,,9,9,9$Mi'),'ZHS16GBK') from dual;
select utl_i18n.string_to_raw(to_char(485,'XXX'),'ZHS16GBK') from dual;
select utl_i18n.string_to_raw(to_char(3999, 'rN'),'ZHS16GBK') from dual;
select utl_i18n.string_to_raw(to_char(1485,'XXX'),'ZHS16GBK') from dual;
select to_nchar(to_number(to_char(3, '9999V999999EEEE'))) from dual;
select to_nchar(to_number(to_char(2,'9990999.9eeee'))) from dual;
select to_nchar(to_char(1234, 'sb9999')) from dual;
select to_nchar(to_char(12,'9990999.9PR')) from dual;


select * from dual where  to_char(to_char(12,'FM9990999.9pr')) like '.%';
select * from dual where  to_char(to_char(1234, 'L9999Mi')) like '1234%';
select * from dual where to_char(ascii(to_char(123, '999999999L')))<>100;
select * from dual where to_char(ascii(to_char(100000000, 'FM9,9999,9999c')))<>100;
select * from dual where to_char(ascii(to_char(12,'FM9990999.9')))<>100;
select * from dual where  to_char(ascii( to_char(1234.34,'xxx')))<>100;
select * from dual where to_char(ascii(to_char(123, '999999999L'))) is NULL;
select * from dual where to_char(ascii(to_char(100000000, 'FM9,9999,9999c'))) is NULL;
select * from dual where to_char(ascii(to_char(12,'FM9990999.9'))) is NULL;
select * from dual where  to_char(ascii( to_char(1234.34,'xxx'))) is NULL;
select * from dual where to_char(ascii(to_char(123, '999999999L'))) is not NULL;
select * from dual where to_char(ascii(to_char(100000000, 'FM9,9999,9999c'))) is not NULL;
select * from dual where to_char(ascii(to_char(12,'FM9990999.9'))) is not NULL;
select * from dual where  to_char(ascii( to_char(1234.34,'xxx'))) is not NULL;
select * from dual where to_char(ascii(to_char(123, '999999999L'))) between 1 and 1000;
select * from dual where to_char(ascii(to_char(100000000, 'FM9,9999,9999c'))) between 1 and 1000;
select * from dual where to_char(ascii(to_char(12,'FM9990999.9'))) between 1 and 1000;
select * from dual where  to_char(ascii( to_char(1234.34,'xxx'))) between 1 and 1000;
select * from dual where to_char(ascii(to_char(123, '999999999L'))) != 100;
select * from dual where to_char(ascii(to_char(100000000, 'FM9,9999,9999c')))!= 100;
select * from dual where to_char(ascii(to_char(12,'FM9990999.9')))!= 100;
select * from dual where  to_char(ascii( to_char(1234.34,'xxx')))!= 100;
###select to_char(to_char(100000000, '9,9999,9999BL')||to_char(485,'999UEEEE')) from dual;
select to_char(to_char(0.000, 'FMRn')||to_char(123, 'RN')) from dual;
###select to_char(to_char(100.2356, '99999,99VC')||to_char(485,'999UEEEE')) from dual;
select to_char(q'[to_char(1234.34,'0000.00X')]') names from dual;

select to_char(floor(to_number(to_char(1234,'999900EEEE')))) from dual;
select to_char(decode(to_char(1234.34,'0000d00l'),to_char(1234.34,'0000d00l'),'好的','1234.34$$','不好','1234.34$$$','再说')) from dual;
select to_char(decode(to_char(1234.34,'0000d00l'),'1234.34$$','好的',to_char(1234.34,'0000d00l'),'不好','1234.34$$$','再说')) from dual;
select to_char(decode(to_char(1234.34,'0000d00'),'1234.34','好的','1234.34$$','不好','1234.34$$$','再说')) from dual;
select to_char(decode(to_char(1234.34,'0000d00'),'1234.34','haode','1234.34$$','buhao','1234.34$$$','zaishuo')) from dual;
select to_char(decode(to_char(3999, 'rN'),'mmmcmxcix','haode','1234.34$$','buhao','1234.34$$$','zaishuo')) from dual;
select to_char(decode(to_char(100.00, '9,$99999999B'),'$100','haode','1234.34$$','buhao','1234.34$$$','zaishuo')) from dual;
select to_char(replace(to_char(1234.34,'0000d00l'),'1234.34$','好的好的')) from dual;
select to_char(replace(to_char(1234.34,'0000d00'),'1234.34','好的好的')) from dual;
select to_char(replace(to_char(1234.34,'0000d00'),'1234.34','好的好的')) from dual;
select to_char(replace(to_char(3999, 'rN'),'mmmcmxcix','好的好的')) from dual;
select to_char(replace(to_char(100.00, '9,$99999999B'),'$100','好的好的')) from dual;
select to_char(translate(to_char(1234.34,'9,,,,,,,,,,99,9.99'),'1,,,,,,,,,,23,4.34','wo')) 返回值 from dual;
select to_char(translate(to_char(123*0.34,'tM9'),'41.82','英文')) 返回值 from dual;
select to_char(translate(to_char(12,'9990999.9PR'),'0012.0','wo')) 返回值 from dual;
select to_char(length(to_char(12345, '9,,,9,,9,9,9$Mi'))) 返回值 from dual;
select to_char(length(to_char(1234.34,'xxx'))) 返回值 from dual;
select to_char(length(to_char(0*0, '9CB'))) 返回值 from dual;
select to_char(length(to_char(0.000, 'FMRn'))) 返回值 from dual;
select to_char(lengthb(to_char(12345, '9,,,9,,9,9,9$Mi'))) 返回值 from dual;
select to_char(lengthb(to_char(1234.34,'xxx'))) 返回值 from dual;
select to_char(lengthb(to_char(0*0, '9CB'))) 返回值 from dual;
select to_char(lengthb(to_char(0.000, 'FMRn'))) 返回值 from dual;
select to_char(cast(to_char(123, '9999999999EEEE') as varchar2(80))) from dual;
select to_char(cast(to_char(12,'9990999.9eeee') as varchar2(80))) from dual;
select to_char(cast(to_char(12345,'9G9G9G9g9') as varchar2(80))) from dual;
select to_char(cast(to_char(485,'XXX') as varchar2(80))) from dual;
select to_char(cast(to_char(1*0.3, 'FM9B') as varchar2(8))) from dual;
--error 01722
select to_char(remainder( to_char(1485,'XXX'),2)) from dual;
select to_char(remainder(to_char(100.2356, 'FM0XXX'),3)) from dual;
--error 01722
select to_char(remainder( to_char(12345, '9,,,9,,9,9,9$Mi'),2)) from dual;
select to_char(abs(to_number(to_char(123*0.34,'tM9')))) from dual;
select to_char(abs(to_number(to_char(1234, 'fmtm9')))) from dual;
select to_char(abs(to_number(to_char(1,'9BEEEE')))) from dual;
select to_char(abs(to_number(to_char(123*0.34,'9999.9')))) from dual;
select to_char(abs(to_char(123*0.34,'9999.9'))) from dual;
select to_char(concat(to_char(1234.34,'0000d00l'), to_char(100000000,'99G9999999v'))) from dual;
select to_char(concat( to_char(12345,'9G9G9G9g9'), to_char(100000000,'99G9999999v'))) from dual;
select to_char(concat(to_char(1234.34,'0000d00l'),  to_char(52345/5, '9,9,9,$9,9'))) from dual;
select to_char(RPAD( to_char(23.12, '99.99BCMI'), 200, '$')) from dual;
select to_char(RPAD(  to_char(100000000, '9,9999,9999BL'), 200, '$')) from dual;
###select to_char(RPAD( to_char(485,'999UEEEE'), 200, '$')) from dual;
select to_char(LPAD( to_char(23.12, '99.99BCMI'), 200, '$')) from dual;
select to_char(LPAD(  to_char(100000000, '9,9999,9999BL'), 200, '$')) from dual;
###select to_char(LPAD( to_char(485,'999UEEEE'), 200, '$')) from dual;
select to_char(instr( to_char(100.2356, '99999,99C') ,'00')) from dual;
select to_char(instr(  to_char(123, '999999999L') ,'00')) from dual;
select to_char(instr(  to_char(100.2356, '99999,99U') ,'00')) from dual;
select to_char(instr(  to_char(1,'9BEEEE') ,'00')) from dual;
select to_char(instrb( to_char(100.2356, '99999,99C') ,'00')) from dual;
select to_char(instrb(  to_char(123, '999999999L') ,'00')) from dual;
select to_char(instrb(  to_char(100.2356, '99999,99U') ,'00')) from dual;
select to_char(instrb(  to_char(1,'9BEEEE') ,'00')) from dual;
###select to_char(substr(to_char(123, '9999V999999EEEE'),3,5)) from dual;
select to_char(substr(to_char(20080320.2365, '9.9EEEE'),3,5)) from dual;
###select to_char(substr( to_char(123, '9999999999EEEE'),3,5)) from dual;
select to_char(substr(to_char(20, 'fmRn'),3,5)) from dual;
###select to_char(substrb(to_char(123, '9999V999999EEEE'),3,5)) from dual;
select to_char(substrb(to_char(20080320.2365, '9.9EEEE'),3,5)) from dual;
###select to_char(substrb( to_char(123, '9999999999EEEE'),2,5)) from dual;
select to_char(substrb(to_char(20, 'fmRn'),3,5)) from dual;
select to_char(initcap(to_char(0.000, 'FMRn'))) from dual;
select to_char(initcap(to_char(123, 'RN'))) from dual;
select to_char(initcap( to_char(3999, 'rN'))) from dual;
select to_char(initcap(to_char(100.2356, '99999,99VC'))) from dual;
select to_char(initcap(to_char(123, '999999V999S'))) from dual;
select to_char(round(to_number(to_char(1234, 'fmtm9')))) from dual;
select to_char(round(to_number(to_char(29989329, '9.99EEEE')))) from dual;
select to_char(round(to_number(to_char('', 'fmmtme')))) from dual;
SELECT to_char(COALESCE(to_char(1485,'XXX'),to_char(1485,'XXX'),'3','4','5')) FROM dual;
SELECT to_char(COALESCE(to_char(100.2356, 'FM0XXX'),to_char(100.2356, 'FM0XXX'),'3','4','5')) FROM dual;
SELECT to_char(COALESCE(to_char(0*0, '9CB'),to_char(1485,'XXX'),'3','4','5')) FROM dual;
select to_char(lower( to_char(100.2356, 'FM0XXX')))  from dual;
select to_char(lower( to_char(12345, '9,,,9,,9,9,9$Mi')))  from dual;
select to_char(lower( to_char(12345, '9,,,9,,9,9,9$Mi')))  from dual;
select to_char(lower( to_char(12345, '9,,,9,,9,9,9$Mi')))  from dual;
select to_char(upper( to_char(100.2356, 'FM0XXX')))  from dual;
select to_char(upper( to_char(12345, '9,,,9,,9,9,9$Mi')))  from dual;
select to_char(upper( to_char(12345, '9,,,9,,9,9,9$Mi')))  from dual;
select to_char(upper( to_char(12345, '9,,,9,,9,9,9$Mi')))  from dual;
SELECT to_char(TRIM(to_char(485,'XXX'))) FROM dual;
select to_char(length(TRIM(to_char(485,'XXX')))) FROM dual;
SELECT to_char(LTRIM(to_char(485,'XXX'))) FROM dual;
select to_char(length(LTRIM(to_char(485,'XXX')))) FROM dual;
SELECT to_char(RTRIM(to_char(485,'XXX'))) FROM dual;
SELECT to_char(TRIM(to_char(0*0, '9CB'))) FROM dual;
select to_char(length(TRIM(to_char(0*0, '9CB')))) FROM dual;
SELECT to_char(LTRIM(to_char(0*0, '9CB'))) FROM dual;
select to_char(length(LTRIM(to_char(0*0, '9CB')))) FROM dual;
SELECT to_char(RTRIM(to_char(0*0, '9CB'))) FROM dual;
select to_char(sinh( to_char(12,'FM9990999.9pr')))  FROM dual;
select to_char(sinh(to_char(12,'FM9990999.9')) )  FROM dual;
select to_char(cosh( to_char(12,'FM9990999.9pr')))  FROM dual;
select to_char(cosh(to_char(12,'FM9990999.9') ))  FROM dual;
select to_char(tanh( to_char(12,'FM9990999.9pr')))  FROM dual;
select to_char(tanh(to_char(12,'FM9990999.9') ))  FROM dual;
select to_char(nvl(null,to_char('', 'fmmtme'))) from dual;
select to_char(nvl(null,to_char(12345+12345, 'tM9'))) from dual;
select to_char(nvl(to_char('', 'fmmtme'),null)) from dual;
select to_char(nvl(to_char(12345+12345, 'tM9'),null)) from dual;
select to_char(ceil(to_char(1234, 'fmtm9')))  FROM dual;
select to_char(ceil( to_char(12,'FM9990999.9pr')))  FROM dual;
select to_char(ceil(to_char(12,'FM9990999.9') ))  FROM dual;
select to_char(floor(to_char(1234, 'fmtm9')))  FROM dual;
select to_char(floor( to_char(12,'FM9990999.9pr')))  FROM dual;
select to_char(floor(to_char(12,'FM9990999.9') ))  FROM dual;
select to_char(asin(to_number(to_char('', 'fmmtme')))) from dual;
select to_char(atan(to_char(1234, 'fmtm9')))  FROM dual;
select to_char(atan( to_char(12,'FM9990999.9pr')))  FROM dual;
select to_char(atan(to_char(12,'FM9990999.9')) )  FROM dual;
select to_char(sqrt(to_char(1234, 'fmtm9')))  FROM dual;
select to_char(sqrt( to_char(12,'FM9990999.9pr')))  FROM dual;
select to_char(sqrt(to_char(12,'FM9990999.9')))  FROM dual;
select to_char(sign(to_char(1234, 'fmtm9')))  FROM dual;
select to_char(sign( to_char(12,'FM9990999.9pr')))  FROM dual;
select to_char(sign(to_char(12,'FM9990999.9') ))  FROM dual;
###select to_char(CHR(to_char(29989329, '9.99EEEE'))) from dual;
select to_char(ascii(to_char(123, '999999999L'))) from dual;
select to_char(ascii(to_char(100000000, 'FM9,9999,9999c'))) from dual;
select to_char(ascii(to_char(12,'FM9990999.9'))) from dual;
select to_char(ascii( to_char(1234.34,'xxx'))) from dual;
###select to_char(REGEXP_COUNT(to_char(1234, 'L9999Mi') ,'$',1,'i')) from DUAL;
###select to_char(REGEXP_COUNT( to_char(123, '999999999L') ,'$',1,'i')) from DUAL;
###select to_char(REGEXP_COUNT(to_char(23.12, '99D99BUS') ,'$',1,'i')) from DUAL;
###select to_char(REGEXP_COUNT(to_char(52345/5, '9,9,9,$9,9') ,'$',1,'i')) from DUAL;
select to_char(nvl2(null,to_char('', 'fmmtme'),to_char('', 'fmmtme'))) from DUAL;
select to_char(nvl2(null,to_char(12345+12345, 'tM9'),to_char('', 'fmmtme'))) from DUAL;
select to_char(nvl2(to_char('', 'fmmtme'),null,to_char('', 'fmmtme'))) from DUAL;
select to_char(nvl2(to_char(12345+12345, 'tM9'),null,to_char('', 'fmmtme'))) from DUAL;
select to_char(power(to_number(to_char(123, '999999V999')),2)) from dual;
select to_char(power(to_number(to_char(12,'9990999.9PR') ),2)) from dual;
select to_char(exp(to_number(to_char(2,'9990999.9eeee')))) from dual;
select to_char(ln(to_number(to_char(3, '9999V999999EEEE')))) from dual;
select to_char(ln(to_number(to_char(2,'9990999.9eeee')))) from dual;
select to_char(ln(to_char(1234, 'sb9999'))) from dual;
select to_char(ln(to_char(12,'9990999.9PR'))) from dual;
select to_char(log(to_char(1234, 'sb9999'),2)) from dual;
select to_char(log(to_char(12,'9990999.9PR'),2)) from dual;
select to_char(to_single_byte(to_char(12345, '9,,,9,,9,9,9B'))) from dual;
select to_char(to_multi_byte(to_char(1234, 'L9999Mi'))) from dual;
select to_char(to_single_byte( to_char(12345, '9,,,9,,9,9,9$Mi')) ) from dual;
select to_char(to_multi_byte( to_char(485,'XXX') )) from dual;
###select to_char(utl_i18n_string_to_raw(to_char(12345, '9,,,9,,9,9,9$Mi'),'ZHS16GBK')) from dual;
###select to_char(utl_i18n_string_to_raw(to_char(485,'XXX'),'ZHS16GBK')) from dual;
###select to_char(utl_i18n_string_to_raw(to_char(3999, 'rN'),'ZHS16GBK')) from dual;
###select to_char(utl_i18n_string_to_raw(to_char(1485,'XXX'),'ZHS16GBK')) from dual;
select to_char(to_nchar(to_number(to_char(3, '9999V999999EEEE')))) from dual;
select to_char(to_nchar(to_number(to_char(2,'9990999.9eeee')))) from dual;
select to_char(to_nchar(to_char(1234, 'sb9999'))) from dual;
select to_char(to_nchar(to_char(12,'9990999.9PR'))) from dual;