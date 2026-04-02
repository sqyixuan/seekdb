--echo #############类型函数嵌套使用 number_format_model-2-3及之下所有##############

--echo #############to_number嵌套to_number number_format_model-2-3-1##############

select to_number(to_number('1.23E+03','9999.00EEEE')) from dual;
select to_number(to_number('','0V')) from dual;
select to_number(to_number('-485','999B')) from dual;
select to_number(to_number('001E5','000XX')) from dual;
select to_number(to_number('1,,,,,,,,,,23,4.34','9,,,,,,,,,,99,9.99PR')) from dual;
select to_number(to_number('1234.34','0000.00B')) from dual;
select to_number(to_number('123.00', '9999999D99')) from dual;
select to_number(to_number(1234.34,'0000.00B')) from dual;
select to_number(to_number('$1234.34', '9999d99$')) from dual;
select to_number(to_number('1,00000000+','9G99999999S')) from dual;
select to_number(to_number('1,00.', '99999G99BD')) from dual;
select to_number(to_number(123, '9999999.99')) from dual;
select to_number(to_number('$0012.0','9990999.$9PR')) from dual;
select to_number(to_number('$1,00', '99$999,99b')) from dual;
select to_number(to_number('$1,0,4,6,9', '9,9,9,$9,9')) from dual;
select to_number(to_number('$100', '9,$99999999B')) from dual;
select to_number(to_number('', 'BC99MI')) from dual;
select to_number(to_number('$23', 'FMU99')) from dual;
select to_number(to_number('2.0E+07', '0D0eeee')) from dual;
select to_number(to_number('2.E+07', '9DEEEE')) from dual;
select to_number(to_number(-0.1,'99.999BEEEE')) from dual;
select to_number(to_number('', 'fmRN')) from dual;
select to_number(to_number('', '9,9,9,V9,9MI')) from dual;
select to_number(to_number(NULL, '9,9V,9,9,9mi')) from dual;
select to_number(to_number(NULL, '9,9V,9,9,9XX')) from dual;
###select to_number(to_number(-485,'XXX')) from dual;
###select to_number(to_number(1234.34,'0000.00X')) from dual;
select to_number(to_number('003039', '0000XX')) from dual;
select to_number(to_number('0064', 'FM0XXX')) from dual;
select to_number(to_number('0064', 'FM0XXX')) from dual;
select to_number(to_number(-1234.34+1234.56,'FM9,9,9,9.99PR')) from dual;
select to_number(to_number(-1234, 's9b999')) from dual;
select to_number(to_number('.22','FM9,9,9,9.99PR')) from dual;
select to_number(to_number('1234+', 'fm9999s')) from dual;
###select to_number(to_number(0*0, '9CB')) from dual;
select to_number(to_number(0+0, '9BPR')) from dual;
select to_number(to_number('', 'FM9B')) from dual;
select to_number(to_number('', 'FMB9d9')) from dual;
select to_number(to_number('2,3', 'FMB9,9')) from dual;
select to_number(to_number('23,', 'FMB99,')) from dual;
select to_number(to_number('0012.+','FM9990999.9s')) from dual;
select to_number(to_number(NULL, 'tmex')) from dual;
select to_number(to_number(NULL, 'ftme')) from dual;
select to_number(to_number('', 'fmmtme')) from dual;


--echo #############to_number嵌套to_char number_format_model-2-3-2##############

--error 01722
select to_number(to_char(-485,'999V'),'999V') from dual;
--error 01722
select to_number(to_char(123, '999999V999'), '999999V999') from dual;
--error 01722
select to_number(to_char(0+0, '9,9,9,9,9vPR'), '9,9,9,9,9vPR') from dual;
###--error 01722
###select to_number(to_char(12,'9990999.9PR'),'9990999.9PR') from dual;
--error 01722
select to_number(to_char(1234, 'fmtm9'), 'fmtm9') from dual;
--error 01722
select to_number(to_char(12345+12345, 'tM9'), 'tM9') from dual;
--error 01722
select to_number(to_char(123*0.34,'tM9'),'tM9') from dual;
--error 01722
select to_number(to_char(123, '9999V999999EEEE'), '9999V999999EEEE') from dual;
--error 01722
select to_char(to_number(to_char(-485,'999V'),'999V')) from dual;
--error 01722
select to_char(to_number(to_char(123, '999999V999'), '999999V999')) from dual;
--error 01722
select to_char(to_number(to_char(0+0, '9,9,9,9,9vPR'), '9,9,9,9,9vPR')) from dual;
###--error 01722
###select to_char(to_number(to_char(12,'9990999.9PR'),'9990999.9PR')) from dual;
--error 01722
select to_char(to_number(to_char(1234, 'fmtm9'), 'fmtm9')) from dual;
--error 01722
select to_char(to_number(to_char(12345+12345, 'tM9'), 'tM9')) from dual;
--error 01722
select to_char(to_number(to_char(123*0.34,'tM9'),'tM9')) from dual;
--error 01722
select to_char(to_number(to_char(123, '9999V999999EEEE'), '9999V999999EEEE')) from dual;



###select to_number(to_char(1234,'999900EEEE'),'999900EEEE') from dual;
select to_number(to_char(29989329, '9.99EEEE'), '9.99EEEE') from dual;
select to_number(to_char(123*0.34,'9999.9'),'9999.9') from dual;
###select to_number(to_char(12,'9990999.9eeee'),'9990999.9eeee') from dual;
select to_number(to_char(1234.34,'0000d00'),'0000d00') from dual;
###select to_number(to_char(1,'9BEEEE'),'9BEEEE')  from dual;
select to_number(to_char(20080320.2365, '9.9EEEE'), '9.9EEEE') from dual;
###select to_number(to_char(123, '9999999999EEEE'), '9999999999EEEE') from dual;
select to_number(to_char(100.2356, 'FM0XXX'), 'FM0XXX') from dual;
select to_number(to_char(485,'XXX'),'XXX') from dual;
select to_number(to_char(1234, 'sb9999'), 'sb9999') from dual;
select to_number(to_char(100.2356, '9,99999999Mi'), '9,99999999Mi') from dual;
###select to_number(to_char(12,'FM9990999.9pr'),'FM9990999.9pr') from dual;
###select to_number(to_char(12,'FM9990999.9'),'FM9990999.9') from dual;
select to_number(to_char('', 'fmmtme'), 'fmmtme') from dual;
###select to_number(to_char(1*0.3, 'FM9B'), 'FM9B') from dual;

select to_char(to_number(to_char(1234,'999900EEEE'))) from dual;
select to_char(to_number(to_char(29989329, '9.99EEEE'))) from dual;
select to_char(to_number(to_char(123*0.34,'9999.9'))) from dual;
select to_char(to_number(to_char(12,'9990999.9eeee'))) from dual;
select to_char(to_number(to_char(-485,'999V'))) from dual;
select to_char(to_number(to_char(1234.34,'0000d00'))) from dual;
select to_char(to_number(to_char(1,'9BEEEE')))  from dual;
select to_char(to_number(to_char(20080320.2365, '9.9EEEE'))) from dual;
select to_char(to_number(to_char(123, '9999999999EEEE'))) from dual;
select to_char(to_number(to_char(123, '999999V999'))) from dual;
select to_char(to_number(to_char(100.2356, 'FM0XXX'))) from dual;
select to_char(to_number(to_char(485,'XXX'))) from dual;
select to_char(to_number(to_char(0+0, '9,9,9,9,9vPR'))) from dual;
select to_char(to_number(to_char(1234, 'sb9999'))) from dual;
select to_char(to_number(to_char(100.2356, '9,99999999Mi'))) from dual;
###select to_char(to_number(to_char(12,'9990999.9PR'))) from dual;
###select to_char(to_number(to_char(12,'FM9990999.9pr'))) from dual;
###select to_char(to_number(to_char(12,'FM9990999.9'))) from dual;
select to_char(to_number(to_char(1234, 'fmtm9'))) from dual;
select to_char(to_number(to_char('', 'fmmtme'))) from dual;
select to_char(to_number(to_char(12345+12345, 'tM9'))) from dual;
select to_char(to_number(to_char(123*0.34,'tM9'))) from dual;
###结果不一致。
###select to_char(to_number(to_char(123, '9999V999999EEEE'))) from dual;
###select to_char(to_number(to_char(1*0.3, 'FM9B'))) from dual;

###select to_char(to_number(to_char(1234,'999900EEEE'),'999900EEEE')) from dual;
select to_char(to_number(to_char(29989329, '9.99EEEE'), '9.99EEEE')) from dual;
select to_char(to_number(to_char(123*0.34,'9999.9'),'9999.9')) from dual;
###select to_char(to_number(to_char(12,'9990999.9eeee'),'9990999.9eeee')) from dual;
select to_char(to_number(to_char(1234.34,'0000d00'),'0000d00')) from dual;


###select to_char(to_number(to_char(1,'9BEEEE'),'9BEEEE'))  from dual;
select to_char(to_number(to_char(20080320.2365, '9.9EEEE'), '9.9EEEE')) from dual;
###select to_char(to_number(to_char(123, '9999999999EEEE'), '9999999999EEEE')) from dual;
select to_char(to_number(to_char(100.2356, 'FM0XXX'), 'FM0XXX')) from dual;
select to_char(to_number(to_char(485,'XXX'),'XXX')) from dual;
select to_char(to_number(to_char(1234, 'sb9999'), 'sb9999')) from dual;
select to_char(to_number(to_char(100.2356, '9,99999999Mi'), '9,99999999Mi')) from dual;
###select to_char(to_number(to_char(12,'FM9990999.9pr'),'FM9990999.9pr')) from dual;
select to_char(to_number(to_char(12,'FM9990999.9'),'FM9990999.9')) from dual;
select to_char(to_number(to_char('', 'fmmtme'), 'fmmtme')) from dual;
###select to_char(to_number(to_char(1*0.3, 'FM9B'), 'FM9B')) from dual;

select to_char(to_number('1.23E+03','9999.00EEEE')) from dual;
select to_char(to_number('','0V')) from dual;
select to_char(to_number('-485','999B')) from dual;
select to_char(to_number('001E5','000XX')) from dual;
select to_char(to_number('1,,,,,,,,,,23,4.34','9,,,,,,,,,,99,9.99PR')) from dual;
select to_char(to_number('1234.34','0000.00B')) from dual;
select to_char(to_number('123.00', '9999999D99')) from dual;
select to_char(to_number(1234.34,'0000.00B')) from dual;
select to_char(to_number('$1234.34', '9999d99$')) from dual;
select to_char(to_number('1,00000000+','9G99999999S')) from dual;
select to_char(to_number('1,00.', '99999G99BD')) from dual;
select to_char(to_number(123, '9999999.99')) from dual;
select to_char(to_number('$0012.0','9990999.$9PR')) from dual;
select to_char(to_number('$1,00', '99$999,99b')) from dual;
select to_char(to_number('$1,0,4,6,9', '9,9,9,$9,9')) from dual;
select to_char(to_number('$100', '9,$99999999B')) from dual;
select to_char(to_number('', 'BC99MI')) from dual;
select to_char(to_number('$23', 'FMU99')) from dual;
select to_char(to_number('2.0E+07', '0D0eeee')) from dual;
select to_char(to_number('2.E+07', '9DEEEE')) from dual;
select to_char(to_number(-0.1,'99.999BEEEE')) from dual;
select to_char(to_number('', 'fmRN')) from dual;
select to_char(to_number('', '9,9,9,V9,9MI')) from dual;
select to_char(to_number(NULL, '9,9V,9,9,9mi')) from dual;
select to_char(to_number(NULL, '9,9V,9,9,9XX')) from dual;
###select to_char(to_number(-485,'XXX')) from dual;
###select to_char(to_number(1234.34,'0000.00X')) from dual;
select to_char(to_number('003039', '0000XX')) from dual;
select to_char(to_number('0064', 'FM0XXX')) from dual;
select to_char(to_number('0064', 'FM0XXX')) from dual;
select to_char(to_number(-1234.34+1234.56,'FM9,9,9,9.99PR')) from dual;
select to_char(to_number(-1234, 's9b999')) from dual;
select to_char(to_number('.22','FM9,9,9,9.99PR')) from dual;
select to_char(to_number('1234+', 'fm9999s')) from dual;
###select to_char(to_number(0*0, '9CB')) from dual;
select to_char(to_number(0+0, '9BPR')) from dual;
select to_char(to_number('', 'FM9B')) from dual;
select to_char(to_number('', 'FMB9d9')) from dual;
select to_char(to_number('2,3', 'FMB9,9')) from dual;
select to_char(to_number('23,', 'FMB99,')) from dual;
select to_char(to_number('0012.+','FM9990999.9s')) from dual;
select to_char(to_number(NULL, 'tmex')) from dual;
select to_char(to_number(NULL, 'ftme')) from dual;
select to_char(to_number('', 'fmmtme')) from dual;

select to_char(to_number('1.23E+03','9999.00EEEE'),'9999.00EEEE') from dual;
select to_char(to_number('','0V'),'0V') from dual;
select to_char(to_number('-485','999B'),'999B') from dual;
select to_char(to_number('001E5','000XX'),'000XX') from dual;
select to_char(to_number('1,,,,,,,,,,23,4.34','9,,,,,,,,,,99,9.99PR'),'9,,,,,,,,,,99,9.99PR') from dual;
select to_char(to_number('1234.34','0000.00B'),'0000.00B') from dual;
select to_char(to_number('123.00', '9999999D99'), '9999999D99') from dual;
select to_char(to_number(1234.34,'0000.00B'),'0000.00B') from dual;
select to_char(to_number('$1234.34', '9999d99$'), '9999d99$') from dual;
select to_char(to_number('1,00000000+','9G99999999S'),'9G99999999S') from dual;
select to_char(to_number('1,00.', '99999G99BD'), '99999G99BD') from dual;
select to_char(to_number(123, '9999999.99'), '9999999.99') from dual;
select to_char(to_number('$0012.0','9990999.$9PR'),'9990999.$9PR') from dual;
select to_char(to_number('$1,00', '99$999,99b'), '99$999,99b') from dual;
select to_char(to_number('$1,0,4,6,9', '9,9,9,$9,9'), '9,9,9,$9,9') from dual;
select to_char(to_number('$100', '9,$99999999B'), '9,$99999999B') from dual;
select to_char(to_number('', 'BC99MI'), 'BC99MI') from dual;
select to_char(to_number('$23', 'FMU99'), 'FMU99') from dual;
###select to_char(to_number('2.0E+07', '0D0eeee'), '0D0eeee') from dual;
###select to_char(to_number('2.E+07', '9DEEEE'), '9DEEEE') from dual;
select to_char(to_number(-0.1,'99.999BEEEE'),'99.999BEEEE') from dual;
select to_char(to_number('', 'fmRN'), 'fmRN') from dual;
select to_char(to_number('', '9,9,9,V9,9MI'), '9,9,9,V9,9MI') from dual;
select to_char(to_number(NULL, '9,9V,9,9,9mi'), '9,9V,9,9,9mi') from dual;
select to_char(to_number(NULL, '9,9V,9,9,9XX'), '9,9V,9,9,9XX') from dual;
###select to_char(to_number(-485,'XXX'),'XXX') from dual;
###select to_char(to_number(1234.34,'0000.00X'),'0000.00X') from dual;
select to_char(to_number('003039', '0000XX'), '0000XX') from dual;
select to_char(to_number('0064', 'FM0XXX'), 'FM0XXX') from dual;
select to_char(to_number('0064', 'FM0XXX'), 'FM0XXX') from dual;
select to_char(to_number(-1234.34+1234.56,'FM9,9,9,9.99PR'),'FM9,9,9,9.99PR') from dual;
select to_char(to_number(-1234, 's9b999'), 's9b999') from dual;
select to_char(to_number('.22','FM9,9,9,9.99PR'),'FM9,9,9,9.99PR') from dual;
select to_char(to_number('1234+', 'fm9999s'), 'fm9999s') from dual;
###select to_char(to_number(0*0, '9CB'), '9CB') from dual;
select to_char(to_number(0+0, '9BPR'), '9BPR') from dual;
select to_char(to_number('', 'FM9B'), 'FM9B') from dual;
select to_char(to_number('', 'FMB9d9'), 'FMB9d9') from dual;
select to_char(to_number('2,3', 'FMB9,9'), 'FMB9,9') from dual;
select to_char(to_number('23,', 'FMB99,'), 'FMB99,') from dual;
select to_char(to_number('0012.+','FM9990999.9s'),'FM9990999.9s') from dual;
select to_char(to_number(NULL, 'tmex'), 'tmex') from dual;
select to_char(to_number(NULL, 'ftme'), 'ftme') from dual;
select to_char(to_number('', 'fmmtme'), 'fmmtme') from dual;

select to_number(to_char(to_number('1.23E+03','9999.00EEEE'))) from dual;
select to_number(to_char(to_number('','0V'))) from dual;
select to_number(to_char(to_number('-485','999B'))) from dual;
select to_number(to_char(to_number('001E5','000XX'))) from dual;
select to_number(to_char(to_number('1,,,,,,,,,,23,4.34','9,,,,,,,,,,99,9.99PR'))) from dual;
select to_number(to_char(to_number('1234.34','0000.00B'))) from dual;
select to_number(to_char(to_number('123.00', '9999999D99'))) from dual;
select to_number(to_char(to_number(1234.34,'0000.00B'))) from dual;
select to_number(to_char(to_number('$1234.34', '9999d99$'))) from dual;
select to_number(to_char(to_number('1,00000000+','9G99999999S'))) from dual;
select to_number(to_char(to_number('1,00.', '99999G99BD'))) from dual;
select to_number(to_char(to_number(123, '9999999.99'))) from dual;
select to_number(to_char(to_number('$0012.0','9990999.$9PR'))) from dual;
select to_number(to_char(to_number('$1,00', '99$999,99b'))) from dual;
select to_number(to_char(to_number('$1,0,4,6,9', '9,9,9,$9,9'))) from dual;
select to_number(to_char(to_number('$100', '9,$99999999B'))) from dual;
select to_number(to_char(to_number('', 'BC99MI'))) from dual;
select to_number(to_char(to_number('$23', 'FMU99'))) from dual;
select to_number(to_char(to_number('2.0E+07', '0D0eeee'))) from dual;
select to_number(to_char(to_number('2.E+07', '9DEEEE'))) from dual;
select to_number(to_char(to_number(-0.1,'99.999BEEEE'))) from dual;
select to_number(to_char(to_number('', 'fmRN'))) from dual;
select to_number(to_char(to_number('', '9,9,9,V9,9MI'))) from dual;
select to_number(to_char(to_number(NULL, '9,9V,9,9,9mi'))) from dual;
select to_number(to_char(to_number(NULL, '9,9V,9,9,9XX'))) from dual;
###select to_number(to_char(to_number(-485,'XXX'))) from dual;
###select to_number(to_char(to_number(1234.34,'0000.00X'))) from dual;
select to_number(to_char(to_number('003039', '0000XX'))) from dual;
select to_number(to_char(to_number('0064', 'FM0XXX'))) from dual;
select to_number(to_char(to_number('0064', 'FM0XXX'))) from dual;
select to_number(to_char(to_number(-1234.34+1234.56,'FM9,9,9,9.99PR'))) from dual;


select to_number(to_char(to_number(-1234, 's9b999'))) from dual;
select to_number(to_char(to_number('.22','FM9,9,9,9.99PR'))) from dual;
select to_number(to_char(to_number('1234+', 'fm9999s'))) from dual;
###select to_number(to_char(to_number(0*0, '9CB'))) from dual;
select to_number(to_char(to_number(0+0, '9BPR'))) from dual;
select to_number(to_char(to_number('', 'FM9B'))) from dual;
select to_number(to_char(to_number('', 'FMB9d9'))) from dual;
select to_number(to_char(to_number('2,3', 'FMB9,9'))) from dual;
select to_number(to_char(to_number('23,', 'FMB99,'))) from dual;
select to_number(to_char(to_number('0012.+','FM9990999.9s'))) from dual;
select to_number(to_char(to_number(NULL, 'tmex'))) from dual;
select to_number(to_char(to_number(NULL, 'ftme'))) from dual;
select to_number(to_char(to_number('', 'fmmtme'))) from dual;
select to_number(to_char(to_number('1.23E+03','9999.00EEEE'),'9999.00EEEE')) from dual;

select to_number(to_char(to_number('','0V'),'0V')) from dual;
select to_number(to_char(to_number('-485','999B'),'999B')) from dual;
select to_number(to_char(to_number('001E5','000XX'),'000XX')) from dual;
select to_number(to_char(to_number('1234.34','0000.00B'),'0000.00B')) from dual;
select to_number(to_char(to_number('123.00', '9999999D99'), '9999999D99')) from dual;
select to_number(to_char(to_number(1234.34,'0000.00B'),'0000.00B')) from dual;
select to_number(to_char(to_number(123, '9999999.99'), '9999999.99')) from dual;
select to_number(to_char(to_number('', 'BC99MI'), 'BC99MI')) from dual;
select to_number(to_char(to_number('2.0E+07', '0D0eeee'), '0D0eeee')) from dual;
select to_number(to_char(to_number('2.E+07', '9DEEEE'), '9DEEEE')) from dual;
select to_number(to_char(to_number(-0.1,'99.999BEEEE'),'99.999BEEEE')) from dual;
select to_number(to_char(to_number('', 'fmRN'), 'fmRN')) from dual;
select to_number(to_char(to_number('', '9,9,9,V9,9MI'), '9,9,9,V9,9MI')) from dual;
select to_number(to_char(to_number(NULL, '9,9V,9,9,9mi'), '9,9V,9,9,9mi')) from dual;
select to_number(to_char(to_number(NULL, '9,9V,9,9,9XX'), '9,9V,9,9,9XX')) from dual;
select to_number(to_char(to_number('003039', '0000XX'), '0000XX')) from dual;
select to_number(to_char(to_number('0064', 'FM0XXX'), 'FM0XXX')) from dual;
select to_number(to_char(to_number('0064', 'FM0XXX'), 'FM0XXX')) from dual;
select to_number(to_char(to_number(-1234.34+1234.56,'FM9,9,9,9.99PR'),'FM9,9,9,9.99PR')) from dual;
select to_number(to_char(to_number(-1234, 's9b999'), 's9b999')) from dual;
select to_number(to_char(to_number('.22','FM9,9,9,9.99PR'),'FM9,9,9,9.99PR')) from dual;
select to_number(to_char(to_number('', 'FM9B'), 'FM9B')) from dual;
select to_number(to_char(to_number('', 'FMB9d9'), 'FMB9d9')) from dual;
select to_number(to_char(to_number(NULL, 'tmex'), 'tmex')) from dual;
select to_number(to_char(to_number(NULL, 'ftme'), 'ftme')) from dual;
select to_number(to_char(to_number('', 'fmmtme'), 'fmmtme')) from dual;

--error 01722
select to_number(to_char(to_number('$1234.34', '9999d99$'), '9999d99$')) from dual;
--error 01722
select to_number(to_char(to_number('1,00000000+','9G99999999S'),'9G99999999S')) from dual;
--error 01722
select to_number(to_char(to_number('1,00.', '99999G99BD'), '99999G99BD')) from dual;
--error 01722
select to_number(to_char(to_number('1,,,,,,,,,,23,4.34','9,,,,,,,,,,99,9.99PR'),'9,,,,,,,,,,99,9.99PR')) from dual;
--error 01722
select to_number(to_char(to_number('$0012.0','9990999.$9PR'),'9990999.$9PR')) from dual;
--error 01722
select to_number(to_char(to_number('$1,00', '99$999,99b'), '99$999,99b')) from dual;
--error 01722
select to_number(to_char(to_number('$1,0,4,6,9', '9,9,9,$9,9'), '9,9,9,$9,9')) from dual;
--error 01722
select to_number(to_char(to_number('$100', '9,$99999999B'), '9,$99999999B')) from dual;
--error 01722
select to_number(to_char(to_number('$23', 'FMU99'), 'FMU99')) from dual;
--error 01722
select to_number(to_char(to_number(-485,'XXX'),'XXX')) from dual;
###--error 01722
###select to_number(to_char(to_number(1234.34,'0000.00X'),'0000.00X')) from dual;
--error 01722
select to_number(to_char(to_number('1234+', 'fm9999s'), 'fm9999s')) from dual;
--error 01722
select to_number(to_char(to_number(0*0, '9CB'), '9CB')) from dual;
--error 01722
select to_number(to_char(to_number(0+0, '9BPR'), '9BPR')) from dual;
--error 01722
select to_number(to_char(to_number('2,3', 'FMB9,9'), 'FMB9,9')) from dual;
--error 01722
select to_number(to_char(to_number('23,', 'FMB99,'), 'FMB99,')) from dual;
--error 01722
select to_number(to_char(to_number('0012.+','FM9990999.9s'),'FM9990999.9s')) from dual;

--echo #############to_char嵌套其他函数 number_format_model-2-3-3##############

select floor(to_number(to_number('1.23E+03','9999.00EEEE'))) from dual;
select decode(to_number('1.23E+03','9999.00EEEE'),to_number('1.23E+03','9999.00EEEE'),'好的','1234.34$$','不好','1234.34$$$','再说') from dual;
select decode(to_number('1,,,,,,,,,,23,4.34','9,,,,,,,,,,99,9.99PR'),'1234.34$$','好的',to_number('1,,,,,,,,,,23,4.34','9,,,,,,,,,,99,9.99PR'),'不好','1234.34$$$','再说') from dual;
select decode(to_number('1234.34','0000.00B'),'1234.34','好的','1234.34$$','不好','1234.34$$$','再说') from dual;
select decode(to_number(1234.34,'0000.00B'),'1234.34','haode','1234.34$$','buhao','1234.34$$$','zaishuo') from dual;
select decode(to_number('$1234.34', '9999d99$'),'mmmcmxcix','haode','1234.34$$','buhao','1234.34$$$','zaishuo') from dual;
select decode(to_number('1,00000000+','9G99999999S'),'$100','haode','1234.34$$','buhao','1234.34$$$','zaishuo') from dual;
select replace(to_number('1,00.', '99999G99BD'),'100','好的好的') from dual;
select replace(to_number('$1,0,4,6,9', '9,9,9,$9,9'),'10469','好的好的') from dual;
select replace(to_number('2.0E+07', '0D0eeee'),'20000000','好的好的') from dual;
###select replace(to_number(-485,'XXX'),'4294956165','好的好的') from dual;
select replace(to_number('003039', '0000XX'),'$100','好的好的') from dual;
select translate(to_number('0064', 'FM0XXX'),'100','wo') 返回值 from dual;
select translate(to_number('0064', 'FM0XXX'),'100','英文') 返回值 from dual;
select translate(to_number(-1234.34+1234.56,'FM9,9,9,9.99PR'),'.22','wo') 返回值 from dual;
select translate(to_number(-1234.34+1234.56,'FM9,9,9,9.99PR'),to_number(-1234.34+1234.56,'FM9,9,9,9.99PR'),'英文') 返回值 from dual;
select length(to_number(-1234, 's9b999')) 返回值 from dual;
select length(to_number('1234+', 'fm9999s')) 返回值 from dual;
select length(to_number('2.E+07', '9DEEEE')) 返回值 from dual;
select length(to_number('$100', '9,$99999999B')) 返回值 from dual;
select lengthb(to_number('1,00000000+','9G99999999S')) 返回值 from dual;
select lengthb(to_number('2.0E+07', '0D0eeee')) 返回值 from dual;
###select lengthb(to_number(-485,'XXX')) 返回值 from dual;
###select lengthb(to_number(1234.34,'0000.00X')) 返回值 from dual;
select cast(to_number(-1234, 's9b999') as varchar2(80)) as 新列 from dual;
select cast(to_number('1234+', 'fm9999s') as varchar2(80)) as 新列 from dual;
select cast(to_number('2,3', 'FMB9,9') as varchar2(8)) as 新列 from dual;
select cast(to_number(-0.1,'99.999BEEEE') as varchar2(8)) as 新列 from dual;
select cast(to_number(NULL, '9,9V,9,9,9mi') as varchar2(8)) as 新列 from dual;
select * from dual where  to_number('2.0E+07', '0D0eeee') like '.%';
select * from dual where  to_number('$0012.0','9990999.$9PR') like '1234%';
select remainder( to_number('1,00000000+','9G99999999S'),2) from dual;
select remainder(to_number('$1,0,4,6,9', '9,9,9,$9,9'),3) from dual;
select remainder( to_number('2.0E+07', '0D0eeee'),2) from dual;
###select abs(to_number(-485,'XXX')) from dual;
select abs(to_number('0064', 'FM0XXX')) from dual;
select abs(to_number('1234+', 'fm9999s')) from dual;
select abs(to_number('2,3', 'FMB9,9')) from dual;
select abs(to_number('0012.+','FM9990999.9s')) from dual;
select concat(to_number('123.00', '9999999D99'), to_char(100000000,'99G9999999v')) from dual;
select concat( to_char(12345,'9G9G9G9g9'), to_number('$1234.34', '9999d99$')) from dual;
select concat(to_number('1,00.', '99999G99BD'),  to_number('1,00.', '99999G99BD')) from dual;
select RPAD( to_number('$1234.34', '9999d99$'), 2, '$') from dual;
select RPAD(  to_char(100000000, '9,9999,9999BL'), 200, '$') from dual;
select RPAD( to_number('1.23E+03','9999.00EEEE'), 123, '$') from dual;
select LPAD( to_char(23.12, '99.99BCMI'), 2, '$') from dual;
select LPAD(  to_number('-485','999B'), 485, '$') from dual;
select LPAD( to_number(123, '9999999.99'), 123, '$') from dual;
select instr( to_number('$1,00', '99$999,99b') ,'00') from dual;
select instr(  to_number('$1,0,4,6,9', '9,9,9,$9,9') ,'46') from dual;
select instr(  to_number('2.0E+07', '0D0eeee') ,'00') from dual;
select instr(  to_number('2.E+07', '9DEEEE') ,'00') from dual;
###select instrb( to_number(-485,'XXX') ,'42') from dual;
select instrb(  to_number('0064', 'FM0XXX') ,'00') from dual;
select instrb(  to_number('1234+', 'fm9999s') ,'00') from dual;
select instrb(  to_number('2.E+07', '9DEEEE') ,'00') from dual;
###select substr(to_number(-485,'XXX'),3,5) from dual;
###select substr(to_number(1234.34,'0000.00X'),3,5) from dual;
select substr( to_number('003039', '0000XX'),3,5) from dual;
select substr(to_number('0064', 'FM0XXX'),3,5) from dual;
select substrb(to_number(-1234.34+1234.56,'FM9,9,9,9.99PR'),1,2) from dual;
###select substrb(to_number(1234.34,'0000.00X'),3,5) from dual;
select substrb( to_number('003039', '0000XX'),2,5) from dual;
select substrb(to_number('$1234.34', '9999d99$'),3,5) from dual;
select initcap(to_number('1,00000000+','9G99999999S')) from dual;
select initcap(to_number('$1,0,4,6,9', '9,9,9,$9,9')) from dual;
select initcap( to_number('2.0E+07', '0D0eeee')) from dual;
###select initcap(to_number(-485,'XXX')) from dual;
###select initcap(to_number(1234.34,'0000.00X')) from dual;
select round(to_number('$0012.0','9990999.$9PR')) from dual;
select round(to_number('$1,00', '99$999,99b')) from dual;
select round(to_number('$1,0,4,6,9', '9,9,9,$9,9')) from dual;

###select lower( to_number(1234.34,'0000.00X'))  from dual;
select lower( to_number('003039', '0000XX'))  from dual;
select lower( to_number(-1234, 's9b999'))  from dual;
select lower( to_number('1234+', 'fm9999s'))  from dual;
select upper( to_number('2,3', 'FMB9,9'))  from dual;
select upper( to_number('23,', 'FMB99,'))  from dual;
select upper( to_number('0012.+','FM9990999.9s'))  from dual;
select upper( to_number('$1234.34', '9999d99$'))  from dual;
SELECT TRIM(to_number('2.E+07', '9DEEEE')) FROM dual;
###select length(TRIM(to_number(-485,'XXX'))) FROM dual;
###SELECT LTRIM(to_number(1234.34,'0000.00X')) FROM dual;
select length(LTRIM(to_number('003039', '0000XX'))) FROM dual;
SELECT RTRIM(to_number('0064', 'FM0XXX')) FROM dual;
SELECT TRIM(to_number(-1234, 's9b999')) FROM dual;
select length(TRIM(to_number('1234+', 'fm9999s'))) FROM dual;
SELECT LTRIM(to_char(0*0, '9CB')) FROM dual;
select length(LTRIM(to_number(0+0, '9BPR'))) FROM dual;
SELECT RTRIM(to_number('2,3', 'FMB9,9')) FROM dual;
###select sinh(to_number(123, '9999999.99'))  FROM dual;
###select cosh(to_number(-0.1,'99.999BEEEE'))  FROM dual;
select tanh(to_number('123.00', '9999999D99'))  FROM dual;
select nvl(null,to_number('$1,0,4,6,9', '9,9,9,$9,9')) from dual;
select nvl(null,to_number('$100', '9,$99999999B')) from dual;
select nvl(to_number('$23', 'FMU99'),null) from dual;
###select nvl(to_number(1234.34,'0000.00X'),null) from dual;
select ceil(to_number('0064', 'FM0XXX'))  FROM dual;
select ceil(to_number('0064', 'FM0XXX'))  FROM dual;
select ceil(to_number(-1234, 's9b999'))  FROM dual;
select floor(to_number('1234+', 'fm9999s'))  FROM dual;
select floor(to_number('1,00000000+','9G99999999S'))  FROM dual;
select floor(to_number('$1,0,4,6,9', '9,9,9,$9,9'))  FROM dual;

select asin(to_number(to_char('', 'fmmtme'))) from dual;
select atan(to_number('23,', 'FMB99,'))  FROM dual;
select atan(to_number('2.0E+07', '0D0eeee'))  FROM dual;
select atan(to_number('$100', '9,$99999999B'))  FROM dual;
select sqrt(to_number('$100', '9,$99999999B'))  FROM dual;
select sqrt(to_number('001E5','000XX'))  FROM dual;
select sqrt(to_number('1.23E+03','9999.00EEEE'))  FROM dual;
select sign(to_number('$23', 'FMU99'))  FROM dual;
select sign(to_number('2.E+07', '9DEEEE'))  FROM dual;
select sign(to_number('$1,00', '99$999,99b'))  FROM dual;
select CHR(to_number('2.0E+07', '0D0eeee')) from dual;
select CHR(to_number('0064', 'FM0XXX')) from dual;
select CHR(to_number('23,', 'FMB99,')) from dual;
###select CHR(to_number('1234+', 'fm9999s')) from dual;
select ascii(to_number('003039', '0000XX')) from dual;
###select ascii(to_number(-485,'XXX')) from dual;
select ascii(to_number('2.0E+07', '0D0eeee')) from dual;
###select ascii(to_number(-485,'XXX')) from dual;

select * from dual where ascii(to_number('$1,0,4,6,9', '9,9,9,$9,9'))<>100;
select * from dual where ascii(to_number('$100', '9,$99999999B'))<>100;
select * from dual where ascii(to_number('2.0E+07', '0D0eeee'))<>100;
select * from dual where  ascii(to_number('2.E+07', '9DEEEE'))<>100;
select * from dual where ascii(to_number('003039', '0000XX')) is NULL;
select * from dual where ascii(to_number('0064', 'FM0XXX')) is NULL;
select * from dual where ascii(to_number('0064', 'FM0XXX')) is NULL;
select * from dual where  ascii(to_number(-1234, 's9b999')) is NULL;
select * from dual where ascii(to_number('1234+', 'fm9999s')) is not NULL;
select * from dual where ascii(to_number('23,', 'FMB99,')) is not NULL;
select * from dual where ascii(to_number(0+0, '9BPR')) is not NULL;
select * from dual where  ascii(to_number('003039', '0000XX')) is not NULL;
###select * from dual where ascii(to_number(-485,'XXX')) between 1 and 1000;
select * from dual where ascii(to_number('$23', 'FMU99')) between 1 and 1000;
select * from dual where ascii(to_number('$0012.0','9990999.$9PR')) between 1 and 1000;
select * from dual where  ascii(to_number('1,00.', '99999G99BD')) between 1 and 1000;
select REGEXP_COUNT(to_number('$0012.0','9990999.$9PR') ,'1',1,'i') from DUAL;
select REGEXP_COUNT(to_number('$1,00', '99$999,99b') ,'1',1,'i') from DUAL;
select REGEXP_COUNT(to_number('$1,0,4,6,9', '9,9,9,$9,9') ,'1',1,'i') from DUAL;
select REGEXP_COUNT(to_number('$23', 'FMU99') ,'2',1,'i') from DUAL;

select * from dual where ascii(to_number('2.E+07', '9DEEEE')) != 100;
###select * from dual where ascii(to_number(1234.34,'0000.00X'))!= 100;
select * from dual where ascii(to_number('0064', 'FM0XXX'))!= 100;
select * from dual where  ascii(to_number('1234+', 'fm9999s'))!= 100;
select to_number('$1234.34', '9999d99$')||to_number('$1234.34', '9999d99$') from dual;
select to_number(123, '9999999.99')||to_number(123, '9999999.99') from dual;
###select to_number('$1,0,4,6,9', '9,9,9,$9,9')||to_char(485,'999UEEEE') from dual;
select q'[to_number('$23', 'FMU99')]' names from dual;
select nvl2(null,to_number('$100', '9,$99999999B'),to_number(123, '9999999.99')) from dual;
select nvl2(null,to_number('$100', '9,$99999999B'),to_number('2.E+07', '9DEEEE'))from dual;
select nvl2(to_number('003039', '0000XX'),null,to_number('003039', '0000XX')) from dual;
select nvl2(to_number('0064', 'FM0XXX'),null,to_number('0064', 'FM0XXX')) from dual;
select power(to_number(to_number(-1234.34+1234.56,'FM9,9,9,9.99PR')),2) from dual;
###select to_number(to_number(0*0, '9CB')) from dual;
select power(to_number(to_number('1234+', 'fm9999s')),2) from dual;
select exp(to_number(to_number(123, '9999999.99'))) from dual;
select exp(to_number(to_number('$0012.0','9990999.$9PR'))) from dual;
select ln(to_number(to_number('$1,0,4,6,9', '9,9,9,$9,9'))) from dual;
select ln(to_number(to_number('$23', 'FMU99'))) from dual;
###select ln(to_number(-485,'XXX')) from dual;
select ln(to_number('0064', 'FM0XXX')) from dual;
select log(to_number(-1234.34+1234.56,'FM9,9,9,9.99PR'),2) from dual;

select to_single_byte(to_number(NULL, '9,9V,9,9,9mi')) from dual;
###select to_multi_byte(to_number(1234.34,'0000.00X')) from dual;
select to_single_byte(to_number('003039', '0000XX')) from dual;
select to_multi_byte( to_number('0064', 'FM0XXX')) from dual;
select utl_i18n.string_to_raw(TO_CHAR(to_number(-1234.34+1234.56,'FM9,9,9,9.99PR')),'ZHS16GBK') from dual;
select utl_i18n.string_to_raw(TO_CHAR(to_number(-1234, 's9b999')),'ZHS16GBK') from dual;
select utl_i18n.string_to_raw(TO_CHAR(to_number('1234+', 'fm9999s')),'ZHS16GBK') from dual;
select utl_i18n.string_to_raw(TO_CHAR(to_number('2,3', 'FMB9,9')),'ZHS16GBK') from dual;
select to_nchar(to_number(to_number('0012.+','FM9990999.9s'))) from dual;
select to_nchar(to_number(to_number('23,', 'FMB99,'))) from dual;
select to_nchar(to_number('003039', '0000XX')) from dual;
select to_nchar(to_number('2.0E+07', '0D0eeee')) from dual;


select * from dual where  to_char(to_number('2.E+07', '9DEEEE')) like '.%';
select * from dual where  to_char(to_number(-0.1,'99.999BEEEE')) like '1234%';
##select * from dual where to_char(ascii(to_number(-485,'XXX')))<>100;
##select * from dual where to_char(ascii(to_number(1234.34,'0000.00X')))<>100;
select * from dual where to_char(ascii(to_number('003039', '0000XX')))<>100;
select * from dual where  to_char(ascii(to_number(-1234, 's9b999')))<>100;
select * from dual where to_char(ascii(to_number('$1234.34', '9999d99$'))) is NULL;
select * from dual where to_char(ascii(to_number('1,00000000+','9G99999999S'))) is NULL;
select * from dual where to_char(ascii(to_number('1234.34','0000.00B'))) is NULL;
select * from dual where  to_char(ascii(to_number('1,,,,,,,,,,23,4.34','9,,,,,,,,,,99,9.99PR'))) is NULL;
select * from dual where to_char(ascii(to_number('1.23E+03','9999.00EEEE'))) is not NULL;
select * from dual where to_char(ascii(to_number('2.0E+07', '0D0eeee'))) is not NULL;
select * from dual where to_char(ascii(to_number('2.E+07', '9DEEEE'))) is not NULL;
###select * from dual where  to_char(ascii(to_number(-485,'XXX'))) is not NULL;
###select * from dual where to_char(ascii(to_number(1234.34,'0000.00X'))) between 1 and 1000;
select * from dual where to_char(ascii(to_number(-1234, 's9b999'))) between 1 and 1000;
select * from dual where to_char(ascii(to_number('2,3', 'FMB9,9'))) between 1 and 1000;
select * from dual where  to_char(ascii(to_number('23,', 'FMB99,'))) between 1 and 1000;
select * from dual where to_char(ascii(to_number('0012.+','FM9990999.9s'))) != 100;
select * from dual where to_char(ascii(to_number('1234+', 'fm9999s')))!= 100;
select * from dual where to_char(ascii(to_number('.22','FM9,9,9,9.99PR')))!= 100;
select * from dual where  to_char(ascii(to_number('0064', 'FM0XXX')))!= 100;
select to_char(to_number('003039', '0000XX')||to_number('003039', '0000XX')) from dual;
select to_char(to_number(-1234.34+1234.56,'FM9,9,9,9.99PR')||to_number(-1234.34+1234.56,'FM9,9,9,9.99PR')) from dual;
select to_char(to_number('1234+', 'fm9999s')||to_number('1234+', 'fm9999s')) from dual;
select to_char(q'[to_number('2,3', 'FMB9,9')]') names from dual;

select to_number(floor(to_number(to_number('1.23E+03','9999.00EEEE'))))  from dual;
select to_number(replace(to_number('003039', '0000XX'), '12345'),'$100','好的好的')  from dual;
select to_number(length(to_number('$100', '9,$99999999B'))) 返回值 from dual;
select to_number(lengthb(to_number('1,00000000+','9G99999999S'))) 返回值 from dual;
select to_number(lengthb(to_number('2.0E+07', '0D0eeee'))) 返回值 from dual;
select to_number(cast(to_number(-1234, 's9b999') as varchar2(80))) as 新列 from dual;
select to_number(remainder( to_number('1,00000000+','9G99999999S'),2))  from dual;
###select to_number(abs(to_number(-485,'XXX')))  from dual;
select to_number(RPAD( to_number('$1234.34', '9999d99$'), 2, '$'))  from dual;
select to_number(instr( to_number('$1,00', '99$999,99b') ,'00'))  from dual;
select to_number(instrb(  to_number('2.E+07', '9DEEEE') ,'00'))  from dual;
###select to_number(substr(to_number(-485,'XXX'),3,5))  from dual;
select to_number(substrb(to_number('$1234.34', '9999d99$'),3,5))  from dual;
select to_number(initcap(to_number('1,00000000+','9G99999999S')))  from dual;
###select to_number(initcap(to_number(1234.34,'0000.00X')))  from dual;
select to_number(round(to_number('$0012.0','9990999.$9PR')))  from dual;
select to_number(round(to_number('$1,00', '99$999,99b')))  from dual;

###select to_number(lower( to_number(1234.34,'0000.00X')))  from dual;
select to_number(lower( to_number('003039', '0000XX')))  from dual;
SELECT to_number(TRIM(to_number('2.E+07', '9DEEEE')))  from dual;
###select to_number(length(TRIM(to_number(-485,'XXX'))))  from dual;
###SELECT to_number(LTRIM(to_number(1234.34,'0000.00X')))  from dual;
select to_number(length(LTRIM(to_number('003039', '0000XX'))))  from dual;
SELECT to_number(RTRIM(to_number('0064', 'FM0XXX')))  from dual;
SELECT to_number(TRIM(to_number(-1234, 's9b999')))  from dual;
select to_number(length(TRIM(to_number('1234+', 'fm9999s'))))  from dual;
SELECT to_number(LTRIM(to_char(0*0, '9CB')))  from dual;
select to_number(length(LTRIM(to_number(0+0, '9BPR'))))  from dual;
SELECT to_number(RTRIM(to_number('2,3', 'FMB9,9')))  from dual;

select to_number(sinh(to_number(123, '9999999.99')))  FROM dual;
select to_number(cosh(to_number(-0.1,'99.999BEEEE')))  FROM dual;
select to_number(tanh(to_number('123.00', '9999999D99')))  FROM dual;
select to_number(nvl(null,to_number('$1,0,4,6,9', '9,9,9,$9,9')))  from dual;
select to_number(nvl(null,to_number('$100', '9,$99999999B')))  from dual;
select to_number(nvl(to_number('$23', 'FMU99'),null))  from dual;
select to_number(ceil(to_number('0064', 'FM0XXX')))  FROM dual;
select to_number(ceil(to_number('0064', 'FM0XXX')))  FROM dual;
select to_number(ceil(to_number(-1234, 's9b999')))  FROM dual;
select to_number(floor(to_number('1234+', 'fm9999s')))  FROM dual;
select to_number(floor(to_number('1,00000000+','9G99999999S')))  FROM dual;
select to_number(asin(to_number(to_char('', 'fmmtme'))))  from dual;
select to_number(atan(to_number('23,', 'FMB99,')))  FROM dual;
select to_number(atan(to_number('2.0E+07', '0D0eeee')))  FROM dual;
select to_number(atan(to_number('$100', '9,$99999999B')))  FROM dual;
select to_number(sqrt(to_number('$100', '9,$99999999B')))  FROM dual;
select to_number(sqrt(to_number('001E5','000XX')))  FROM dual;
select to_number(sqrt(to_number('1.23E+03','9999.00EEEE')))  FROM dual;
select to_number(sign(to_number('$23', 'FMU99')))  FROM dual;
select to_number(sign(to_number('2.E+07', '9DEEEE')))  FROM dual;
select to_number(sign(to_number('$1,00', '99$999,99b')))  FROM dual;
select to_number(ascii(to_number('003039', '0000XX')))  from dual;
select to_number(ascii(to_number('2.0E+07', '0D0eeee')))  from dual;

select to_number(REGEXP_COUNT(to_number('$0012.0','9990999.$9PR') ,'1',1,'i'))  from dual;
select to_number(REGEXP_COUNT(to_number('$1,00', '99$999,99b') ,'1',1,'i'))  from dual;
select to_number(REGEXP_COUNT(to_number('$1,0,4,6,9', '9,9,9,$9,9') ,'1',1,'i'))  from dual;
select to_number(REGEXP_COUNT(to_number('$23', 'FMU99') ,'2',1,'i'))  from dual;
select to_number(nvl2(null,to_number('$100', '9,$99999999B'),to_number(123, '9999999.99')))  from dual;
select to_number(nvl2(null,to_number('$100', '9,$99999999B'),to_number('2.E+07', '9DEEEE'))) from dual;
select to_number(nvl2(to_number('003039', '0000XX'),null,to_number('003039', '0000XX')))  from dual;
select to_number(nvl2(to_number('0064', 'FM0XXX'),null,to_number('0064', 'FM0XXX')))  from dual;
select to_number(power(to_number(to_number(-1234.34+1234.56,'FM9,9,9,9.99PR')),2))  from dual;
###select to_number(to_number(to_number(0*0, '9CB')))  from dual;
select to_number(power(to_number(to_number('1234+', 'fm9999s')),2))  from dual;
select to_number(exp(to_number(to_number(123, '9999999.99'))))  from dual;
select to_number(exp(to_number(to_number('$0012.0','9990999.$9PR'))))  from dual;
select to_number(ln(to_number(to_number('$1,0,4,6,9', '9,9,9,$9,9'))))  from dual;
select to_number(ln(to_number(to_number('$23', 'FMU99'))))  from dual;
###select to_number(ln(to_number(-485,'XXX')))  from dual;
select to_number(ln(to_number('0064', 'FM0XXX')))  from dual;
###select to_number(log(to_number(-1234.34+1234.56,'FM9,9,9,9.99PR'),2))  from dual;
select to_number(to_single_byte(to_number(NULL, '9,9V,9,9,9mi')))  from dual;
select to_number(to_single_byte(to_number('003039', '0000XX')))  from dual;
###OB报错内部错误600
###select to_number(utl_i18n_string_to_raw(to_number(-1234.34+1234.56,'FM9,9,9,9.99PR'),'ZHS16GBK'))  from dual;
###select to_number(utl_i18n_string_to_raw(to_number(-1234, 's9b999'),'ZHS16GBK'))  from dual;
###select to_number(utl_i18n_string_to_raw(to_number('1234+', 'fm9999s'),'ZHS16GBK'))  from dual;
###select to_number(utl_i18n_string_to_raw(to_number('2,3', 'FMB9,9'),'ZHS16GBK'))  from dual;
select to_number(to_nchar(to_number(to_number('0012.+','FM9990999.9s'))))  from dual;
select to_number(to_nchar(to_number(to_number('23,', 'FMB99,'))))  from dual;
select to_number(to_nchar(to_number('003039', '0000XX')))  from dual;
select to_number(to_nchar(to_number('2.0E+07', '0D0eeee')))  from dual;
select to_number(ceil(to_number(-1234, 's9b999')), 's9b999')  FROM dual;
select to_number(asin(to_number(to_char('', 'fmmtme'))), 'fmmtme')  from dual;

--error 00932
SELECT COALESCE(to_number('1,00000000+','9G99999999S'),to_number('1,00000000+','9G99999999S'),'3','4','5') FROM dual;
--error 00932
SELECT COALESCE(to_number('$0012.0','9990999.$9PR'),to_number('$0012.0','9990999.$9PR'),'3','4','5') FROM dual;
--error 00932
SELECT COALESCE(to_number('1,,,,,,,,,,23,4.34','9,,,,,,,,,,99,9.99PR'),to_number('1,,,,,,,,,,23,4.34','9,,,,,,,,,,99,9.99PR'),'3','4','5') FROM dual;
--error 00932
SELECT COALESCE(to_number('2.0E+07', '0D0eeee'),to_number('2.E+07', '9DEEEE'),'3','4','5') FROM dual;
--error 30001
SELECT trim(leading 'USD' from  to_number('0012.+','FM9990999.9s')) FROM dual;
--error 30001
SELECT trim(both 'USD' from  to_number('0012.+','FM9990999.9s')) FROM dual;
--error 30001
SELECT trim(trailing 'USD' from  to_number('$1,0,4,6,9', '9,9,9,$9,9')) FROM dual;

--error 01426
select sinh(to_number('1,00000000+','9G99999999S'))  FROM dual;
--error 01426
select sinh(to_number('$1,0,4,6,9', '9,9,9,$9,9'))  FROM dual;
--error 01426
select cosh(to_number('2.0E+07', '0D0eeee'))  FROM dual;
--error 01426
select cosh(to_number('2.E+07', '9DEEEE'))  FROM dual;
--error 01426
select tanh(to_number('1.23E+03','9999.00EEEE'))  FROM dual;
--error 01426
select tanh(to_number('001E5','000XX'))  FROM dual;
--error 01428,600
select asin(to_number('$1,0,4,6,9', '9,9,9,$9,9')) from dual;
--error 01428,600
select asin(to_number('2.0E+07', '0D0eeee')) from dual;
--error 01428,600,01481
select acos(to_number(1234.34,'0000.00X'))  FROM dual;
--error 01428,600
select acos(to_number('003039', '0000XX'))  FROM dual;
--error 01428,600
select acos(to_number('0012.+','FM9990999.9s'))  FROM dual;
--error 00909
select atan2(to_number('1,00.', '99999G99BD'))  FROM dual;
--error 00909
select atan2(to_number('$0012.0','9990999.$9PR'))  FROM dual;
--error 00909
select atan2(to_number('$1,00', '99$999,99b'))  FROM dual;
--error 01428
select log(to_number(-1234, 's9b999'),2) from dual;
--error 01722
select to_number(decode(to_number('1.23E+03','9999.00EEEE'),to_number('1.23E+03','9999.00EEEE'),'好的','1234.34$$','不好','1234.34$$$','再说'))  from dual;
--error 01722
select to_number(decode(to_number('1,,,,,,,,,,23,4.34','9,,,,,,,,,,99,9.99PR'),'1234.34$$','好的',to_number('1,,,,,,,,,,23,4.34','9,,,,,,,,,,99,9.99PR'),'不好','1234.34$$$','再说'))  from dual;
--error 01722
select to_number(replace(to_number(-485,'XXX'),'4294956165','好的好的'))  from dual;
--error 01722
select to_number(translate(to_number('0064', 'FM0XXX'),'100','wo')) 返回值 from dual;
--error 01722
select to_number(concat(to_number('123.00', '9999999D99'), to_char(100000000,'99G9999999v')))  from dual;