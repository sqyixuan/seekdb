--echo #############所有冲突检查number_format_model-1-2及之下所有##############

--echo #######################数值类'0', '9'  number_format_model-1-2-1#############################

select to_char(1234,'9999.00EEEE') from dual;
select to_char(1234,'0000.99EEEE') from dual;
select to_char(1234,'0000.00eeee') from dual;
select to_char(1234,'999900EEEE') from dual;
select to_char(1234,'0000V00') from dual;
select to_char(1234,'0V') from dual;

select to_char(-0.1,'00.00') from dual;
###select to_char(0.1,'0.0XXXX')  from dual;
###select to_char(0.1,'0,0XXX')  from dual;
select to_char(485,'000XX') from dual;
select to_char(100.2356, '00000,00mI') from dual;
select to_char('', '0,0,0,0,0PR') from dual;
select to_char(NULL, '0,0,0,0,0S') from dual;
select to_char(0, '0,0,0,0,09999') from dual;

select to_char(123, '999999999mI') from dual;
select to_char(485,'999S') from dual;
select to_char(-485,'999B') from dual;
select to_char(1485,'9,999B') from dual;
select to_char('', '9,9,9,9,9B') from dual;
select to_char(NULL, '9,9,9,9,9FM') from dual;
select to_char(29989329, '9.99EEEE') from dual;
select to_char(29989329, '9.99EEEE') from dual;
select to_char('', '0,0,0,0,0tm9') from dual;
select to_char(NULL, '0,0,0,0,0tm9') from dual;
select to_char('', '9,9,9,9,9TME') from dual;
select to_char(NULL, '9,9,9,9,9tm9') from dual;

--echo #######################数值类'0', '9'  预期异常验证#############################
--error 01481
select to_char(9, '9,9,9,9,9TME') from dual;
--error 01481
select to_char(-9.1,'99.99XXX') from dual;
--error 01481
select to_char(9.1,'9.XXX9')  from dual;
--error 01481
select to_char(9.1,'XXX9,9')  from dual;
--error 01481
select to_char(12,'999V9999.9') from dual;
--error 01481
select to_char(32, '0xx999') from dual;
--error 01481
select to_char(32, '0xX999') from dual;
--error 01481
select to_char(32, '0999XX') from dual;
--error 01481
select to_char(-485,'00XXX0') from dual;
--error 01481
select to_char(1485,'0,00XXX0') from dual;
--error 01481
select to_char(12,'0000XXX000.0') from dual;
--error 01481
select to_char(-485,'00XXX0') from dual;
--error 01481
select to_char(1485,'0,00XXX0') from dual;
###--error 01481
###select to_char(12345, '0,,,0,,V0,0,0') from dual;
--error 01481
select to_char(1234,'9999RN') from dual;
--error 01481
select to_char(1234,'0000.99rn') from dual;
--error 01481
select to_char(1234,'0000.0Rn') from dual;
--error 01481
select to_char(1234,'999900rN') from dual;
--error 01481
select to_char(1234,'000000RN') from dual;
--error 01481
select to_char(1234,'0RN') from dual;
--error 01481
select to_char(12345, '0,,,0,,0,0,0RN') from dual;
--error 01481
select to_char(-0.1,'00.00rn') from dual;
--error 01481
select to_char(0.1,'0.0rN')  from dual;
--error 01481
select to_char(0.1,'0,0Rn')  from dual;
--error 01481
select to_char(12,'0000000.0tme') from dual;
--error 01481
select to_char(485,'000tme') from dual;
--error 01481
select to_char(-485,'000tme') from dual;
--error 01481
select to_char(1485,'0,000TME') from dual;
--error 01481
select to_char(100.2356, '00000,00tmE') from dual;
--error 01481
select to_char(0, '0,0,0,0,0tme') from dual;
--error 01481
select to_char(32, '0xxTME') from dual;
--error 01481
select to_char(32, '0xXTMe') from dual;
--error 01481
select to_char(32, '0XXTM9') from dual;
--error 01481
select to_char(123, '999999999Rn') from dual;
--error 01481
select to_char(-9.1,'99.99rN') from dual;
--error 01481
select to_char(9.1,'9.9rn')  from dual;
--error 01481
select to_char(9.1,'9,9Rn')  from dual;
--error 01481
select to_char(12,'9999999.9tme') from dual;
--error 01481
select to_char(485,'999tme') from dual;
--error 01481
select to_char(-485,'999TME') from dual;
--error 01481
select to_char(1485,'9,999TM9') from dual;
--error 01481
select to_char(9, '9,9,9,9,9tm9') from dual;
--error 01481
select to_char(29989329, '9.99EEEEtm9') from dual;
--error 01481
select to_char(29989329, '9.99EEEERN') from dual;

--echo #######################分组类',', '.'  number_format_model-1-2-2#############################

select to_char(100000000, '$9,99') from dual;
select to_char(100000000, '9$9,999') from dual;
select to_char(123, '999999C999') from dual;
select to_char(0.1,'0.9U')  from dual;
select to_char(12,'9990999.9eeee') from dual;
select to_char(485,'999EEEE') from dual;
select to_char(-485,'999V') from dual;
select to_char(1234.34,'9,,,,,,,,,,9,,,,,,,,,,,,9,9.99MI') from dual;
select to_char(1234.34,'9,,,,,,,,,,99,9.99PR') from dual;
select to_char(0.2356, '9999,.9s') from dual;
select to_char(1234.34,'9,9,9,.999S') from dual;
select to_char(1234.34,'.b') from dual;
select to_char(1234.34,'0000.00B') from dual;
select to_char(123, '9999999D99') from dual;

--echo #######################分组类',', '.'  预期异常验证#############################
--error 01481
select to_char(123*0.34,'9fm999.9') from dual;
--error 01481
select to_char(1234.34+1234.56,'9,9,9,9.99FM') from dual;
###--error 01481
###select to_char(100.2356, '99V999,99') from dual;
--error 01481
select to_char(100.2356, '9,999XXX99999') from dual;
###--error 01481
###select to_char(1485,'9V,999') from dual;
--error 01481
select to_char(0.1,'0U,9')  from dual;
--error 01481
select to_char(-0.1,'99.9L9') from dual;
--error 01481
select to_char(100000000, '9,9d9') from dual;
--error 01481
select to_char(100000000, '99,9D99') from dual;
--error 01481
select to_char(-0.1,'99.99D') from dual;
--error 01481
select to_char(0.1,'0.9D')  from dual;
--error 01481
select to_char(0.1,'0,9G')  from dual;
--error 01481
select to_char(-0.1,'99.99XXXX') from dual;
--error 01481
select to_char(0.1,'0.9XXXX')  from dual;
--error 01481
select to_char(0.1,'0,9XXXX')  from dual;
--error 01481
select to_char(12,'9990999.9G') from dual;
--error 01481
select to_char(485,'99,Gtm9') from dual;
--error 01481
select to_char(-485,'999,tme') from dual;
--error 01481
select to_char(1485,'9,999G') from dual;
--error 01481
select to_char(100.2356, '99999,99D') from dual;
--error 01481
select to_char(100.2356, '9,99999999D') from dual;
--error 01481
select to_char(1234.34,'9,,,,,,,,,,9,,,,,,,,,,,,9,9.99G') from dual;
--error 01481
select to_char(1234.34,'9,,,,,,,,,,99,9.99rN') from dual;
--error 01481
select to_char(0.2356, '9999,.9RN') from dual;
--error 01481
select to_char(1234.34,'9,9,9,.999rn') from dual;
--error 01481
select to_char(1234.34,'.D') from dual;
--error 01481
select to_char(1234.34,'0000.00TME') from dual;
--error 01481
select to_char(123*0.34,'9999.9TM9') from dual;
--error 01481
select to_char(1234.34+1234.56,'9,9,9,9.99xxx') from dual;

--echo #######################国际标准分组类D, G  number_format_model-1-2-3#############################

select to_char(1234.34, '9999d99$') from dual;
select to_char(1234.34,'D$') from dual;
select to_char(1234.34,'0000D$00') from dual;
select to_char(123*0.34,'9999D9C') from dual;
select to_char(0.2356, '9999d9c') from dual;
select to_char(1234.34,'dL') from dual;
select to_char(1234.34,'0000d00l') from dual;
select to_char(123*0.34,'9999d9L') from dual;
select to_char(1234.34+1234.56,'9999d99l') from dual;
select to_char(123.456, '99G9D999u') from dual;
select to_char(12345,'9GGG9GG9G9G9U') from dual;
###冲突检查，D/G与EEEE类型不冲突；OB报错，ORACLE无报错
###select to_char(12345,'9G9G9G9G9eeee') from dual;
###select to_char(123.456, '99g9D999EEEE') from dual;
select to_char(12345,'9ggg9gg9G9G9PR') from dual;
select to_char(12345,'9G9G9G9g9Mi') from dual;
select to_char(100000000,'9G9999G9999s') from dual;
select to_char(100000000,'9G99999999S') from dual;
select to_char(100000000,'99G9999999v') from dual;
select to_char(100000000,'999G999G99V9') from dual;
select to_char(1000,'999G999G999b') from dual;
select to_char(1485,'9G999Gb') from dual;
select to_char(100.2356, '99999G99BD') from dual;
select to_char(100.2356, 'B9G99999999D') from dual;
select to_char(123, '9999999.99') from dual;
select to_char(0.1,'0G9G')  from dual;
--echo #######################国际标准分组类D, G  预期异常验证#############################
###--error 01481
###select to_char(100000000,'999G999G99V9GGG') from dual;
--error 01481
select to_char(100000000, '9,,9d9') from dual;
--error 01481
select to_char(100000000, '99G.99') from dual;
--error 01481
select to_char(-0.1,'99.99.') from dual;
--error 01481
select to_char(0.1,'0D9d')  from dual;
--error 01481
select to_char(-0.1,'99D99XXXX') from dual;
--error 01481
select to_char(0.1,'0D9XXXX')  from dual;
--error 01481
select to_char(0.1,'0G9XXXX')  from dual;
--error 01481
select to_char(12,'9990999D9G') from dual;
--error 01481
select to_char(485,'99GGtm9') from dual;
--error 01481
select to_char(-485,'999Gtme') from dual;
--error 01481
select to_char(1234.34,'9,,,,,,,,,,9,,,,,,,,,,,,9,9D99G') from dual;
--error 01481
select to_char(1234.34,'9,,,,,,,,,,99G9D99rN') from dual;
--error 01481
select to_char(0.2356, '9999GD9RN') from dual;
--error 01481
select to_char(1234.34,'9,9,9GD999rn') from dual;
--error 01481
select to_char(1234.34,'DD') from dual;
--error 01481
select to_char(1234.34,'0000D00TME') from dual;
--error 01481
select to_char(123*0.34,'9999D9TM9') from dual;
##--error 01481
##select to_char(1234.34+1234.56,'9G9G9,9.99xxx') from dual;
##--echo #######################美元符号类$  number_format_model-1-2-4#############################
##select to_char(100000000, '9,9999,$9V999') from dual;
select to_char(123, '9999$999v99') from dual;
select to_char(-0.1,'99.$99Mi') from dual;
select to_char(0.1,'0.$9PR')  from dual;
select to_char(0.1,'0,$9S')  from dual;
select to_char(12,'9990999.$9PR') from dual;
select to_char(485,'999$S') from dual;
select to_char(-485,'99$9Mi') from dual;
select to_char(1485,'9,9$99b') from dual;
select to_char(100.2356, '99$999,99b') from dual;
select to_char(100.2356, '$9,99999999B') from dual;
select to_char(100.00, '9,$99999999B') from dual;
select to_char(52345/5, '9,9,9,$9,9') from dual;
select to_char('', '$9,9,9,9,v9') from dual;
select to_char(NULL, '9,9$,9,9,v9') from dual;
select to_char(0, '9,9,9$,9,v9') from dual;
select to_char('', '$9,9,9,9XXX,v9') from dual;
select to_char(NULL, '9,9$,9,9XvXX,9') from dual;

###--echo #######################美元符号类$预期异常验证#############################
--error 01481
select to_char(0.2356, '9$,99999999BB') from dual;
--error 01481
select to_char(12345+12345, '$9,9,9,9,9fm') from dual;
--error 01481
select to_char(12345-12345, '9,$9,9,9,9FM') from dual;
--error 01481
select to_char(12345*5, '9,9,$9,9,9fm') from dual;
###--error 01481
###select to_char(100000000, '999,$999,99V9,,,') from dual;
--error 01481
select to_char(1234.34,'9,9,9,9.9$9EEEE') from dual;
--error 01481
select to_char(1234.34,'9,9,9,9.99$EEEE') from dual;
--error 01481
select to_char(1234,'9,9,9,9.$99EEEE') from dual;
--error 01481
select to_char(12345, '$9,,,9,,9,9,9EEEE') from dual;
--error 01481
select to_char(1234.34,'9,9,9,9.9$9$') from dual;
--error 01481
select to_char(1234.34,'9,9,9,9.99$C') from dual;
--error 01481
select to_char(1234,'9,9,9,9.$99L') from dual;
--error 01481
select to_char(12345, '$9,,,9,,9,9,9U') from dual;
--error 01481
select to_char(100000000, '9,9999,$9999CL') from dual;
--error 01481
select to_char(100000000, '999,$999,999,,,CLU') from dual;
--error 01481
select to_char(123, '9999$99999rn') from dual;
--error 01481
select to_char(-0.1,'99.$99rN') from dual;
--error 01481
select to_char(0.1,'0.$9Rn')  from dual;
--error 01481
select to_char(0.1,'0,$9RN')  from dual;
--error 01481
select to_char(12,'9990999.$9TME') from dual;
--error 01481
select to_char(485,'999$TM9') from dual;
--error 01481
select to_char(-485,'99$9tm9') from dual;
--error 01481
select to_char(1485,'9,9$99tme') from dual;
--error 01481
select to_char(100.2356, '99$999,99tme') from dual;
--error 01481
select to_char(100.2356, '$9,99999999tm9') from dual;
--error 01481
select to_char(100.00, '9,$99999999xxxx') from dual;
--error 01481
select to_char(0.2356, '9$,99999999XXXX') from dual;
--error 01481
select to_char(12345+12345, '$9,9,9,9,9XXX') from dual;
--error 01481
select to_char(12345-12345, '9,$9,9,9,9TM9') from dual;
--error 01481
select to_char(12345*5, '9,9,$9,9,9U') from dual;
--error 01481
select to_char(52345/5, 'XXX9,9,9,$9,9') from dual;
--error 01481
select to_char(0, '9,9,9$,9,9XXX') from dual;


--echo #######################国际货币类C, L, U  number_format_model-1-2-5#############################
select to_char(23, 'BC99MI') from dual;
select to_char(23, '99BC99PR') from dual;
select to_char(23.12, '99D99BC') from dual;
select to_char(23, 'FMC99S') from dual;
select to_char(23.12, '99.99BCMI') from dual;
select to_char(23.12, '99.99BCPR') from dual;
select to_char(23.12, '99.99BCS') from dual;
select to_char(12345, '9,,,9,,9,9,9CMI') from dual;
select to_char(100000000, '9,9999,9999CPR') from dual;
select to_char(123, '999999999CS') from dual;
select to_char(0.1,'0,V9C')  from dual;
select to_char(-485,'999CB') from dual;
select to_char(100000000, '9,9999,9999BL') from dual;
select to_char(123, '999999999VL') from dual;
select to_char(23, 'FMU99') from dual;
select to_char(23.12, '99.99BUPR') from dual;
select to_char(23.12, '99D99BUS') from dual;
select to_char(123, '999999999Ub') from dual;
###OB与ORACLE结果不一致
###select to_char(485,'999UEEEE') from dual;
###select to_char(-485,'999UEEEE') from dual;


--echo #######################国际货币类C, L, U  预期异常验证#############################
--error 01481
select to_char(1485,'9,999UEEEE') from dual;
--error 01481
select to_char(12,'9990999.9UEEEE') from dual;
--error 01481
select to_char(-0.1,'99.99U$') from dual;
--error 01481
select to_char(0.1,'0.9Ueeee')  from dual;
--error 01481
select to_char(0.1,'0,9UEEEE')  from dual;
--error 01481
select to_char(12345, '9,,,9,,9,9,9lU') from dual;
--error 01481
select to_char(100000000, '9,9999,9999Ueeee') from dual;
--error 01481
select to_char(23.12, '99.99fmBUMI') from dual;
--error 01481
select to_char(-0.1,'99.V99L') from dual;
--error 01481
select to_char(0.1,'0.9LV')  from dual;
--error 01481
select to_char(0.1,'0,9LV')  from dual;
--error 01481
select to_char(12,'9990999.9Lfm') from dual;
--error 01481
select to_char(23.12, '99.99fmBU') from dual;
--error 01481
select to_char(23.12, '99.99BlMIB') from dual;
--error 01481
select to_char(23.12, 'B99.99BlPR') from dual;
--error 01481
select to_char(23.12, '99B.99BlS') from dual;
###--error 01481
###select to_char(12345, '9,,,9,,9,9,9LB') from dual;
--error 01481
select to_char(12,'9990999.V9C') from dual;
--error 01481
select to_char(485,'999CV') from dual;
--error 01481
select to_char(-0.1,'99.99VC') from dual;
--error 01481
select to_char(0.1,'0.9CV')  from dual;
--error 01481
select to_char(23, 'BC99l') from dual;
--error 01481
select to_char(23, '99BC99U') from dual;
--error 01481
select to_char(23.12, '99.99BCC') from dual;
--error 01481
select to_char(23, 'FMCL99') from dual;
--error 01481
select to_char(23.12, '99.99BCLMI') from dual;
--error 01481
select to_char(23.12, '99.99BCUPR') from dual;
--error 01481
select to_char(23.12, '99.99BC$S') from dual;
--error 01481
select to_char(12345, '9,,,9,,9,9,9$C') from dual;
--error 01481
select to_char(100000000, '9,9999,9999C$') from dual;
--error 01481
select to_char(123, '999999999Crn') from dual;
--error 01481
select to_char(-0.1,'99.99CRN') from dual;
--error 01481
select to_char(0.1,'0.9CRn')  from dual;
--error 01481
select to_char(0.1,'0,9CrN')  from dual;
--error 01481
select to_char(12,'9990999.9Crn') from dual;
--error 01481
select to_char(485,'999Ctme') from dual;
--error 01481
select to_char(-485,'999Ctm9') from dual;
--error 01481
select to_char(23.12, '99.99BlMIl') from dual;
--error 01481
select to_char(23.12, '99.99BllPR') from dual;
--error 01481
select to_char(23.12, '99.99BlcS') from dual;
--error 01481
select to_char(12345, '9,,,9,,9,9,9cL') from dual;
--error 01481
select to_char(100000000, '9,9999,9999Lu') from dual;
--error 01481
select to_char(123, '999999999Lxx') from dual;
--error 01481
select to_char(-0.1,'99.99Lxxxxx') from dual;
--error 01481
select to_char(0.1,'0.9xxxL')  from dual;
--error 01481
select to_char(0.1,'0,9XXXXL')  from dual;
--error 01481
select to_char(12,'9990999.9XXXL') from dual;
--error 01481
select to_char(23.12, '99.99XXBU') from dual;
--error 01481
select to_char(23, 'FMUX99') from dual;
--error 01481
select to_char(23.12, '99.99XBUMI') from dual;
--error 01481
select to_char(23.12, '99.99XBUPR') from dual;
--error 01481
select to_char(23.12, '99.99XXBUS') from dual;
--error 01481
select to_char(12345, '9,,,9,,9,9,9RNU') from dual;
--error 01481
select to_char(100000000, '9,9999,9999RNU') from dual;
--error 01481
select to_char(123, '999999999rnU') from dual;
--error 01481
select to_char(-0.1,'99.99rnU') from dual;
--error 01481
select to_char(0.1,'0.9RRNU')  from dual;
--error 01481
select to_char(0.1,'0,9rNNU')  from dual;
--error 01481
select to_char(12,'9990999.9FMU') from dual;
--error 01481
select to_char(485,'999TM9U') from dual;
--error 01481
select to_char(-485,'999UTME') from dual;
--error 01481
select to_char(1485,'9,999XXU') from dual;

--echo #######################科学计数法类EEEE  number_format_model-1-2-6#############################

select to_char(20080320, '0D0EEEE') from dual;
select to_char(20080320, '0D0eeee') from dual;
###select to_char(20080320, '9DEEEE') from dual;

###select to_char(0.0, 'L0.0EEEE') from dual;
###select to_char(123, '9999V999999EEEE') from dual;
select to_char(-0.1,'99.999BEEEE') from dual;
select to_char(0.1,'0.99BEEEE')  from dual;
select to_char(1,'9BEEEE')  from dual;


--echo #######################科学计数法类EEEE  预期异常验证#############################
--error 01481
select to_char(20080320, '0.0EEEEEEEE') from dual;
--error 01481
select to_char(20080320, '0.0eeeeEEEE') from dual;
--error 01481
select to_char(20080320, '9.EEEEEEEE') from dual;
--error 01481
select to_char(20080320, '0EEEErn') from dual;
--error 01481
select to_char(20080320.2365, '0.0EEEERN') from dual;
--error 01481
select to_char(0.265, '0.0EEEErN') from dual;
--error 01481
select to_char(-0.2568, '0.0EEEERn') from dual;
--error 01481
select to_char(-0.0, '0.0EEEEtm9') from dual;
--error 01481
select to_char(0.0, '0.0EEEEtme') from dual;
--error 01481
select to_char(123, '9999999999EEEETM9') from dual;
--error 01481
select to_char(-0.1,'99.999EEEETME') from dual;
--error 01481
select to_char(0.1,'0.99EEEEXX')  from dual;
--error 01481
select to_char(12,'9990999.99EEEEXX') from dual;
--error 01481
select to_char(485,'9999XXEEEE') from dual;
--error 01481
select to_char(-485,'9999xxEEEE') from dual;
--error 01481
select to_char(12,'9990999.99XXXEEEE') from dual;
--error 01481
select to_char(485,'9999MiEEEE') from dual;
--error 01481
select to_char(-485,'9999FMEEEE') from dual;

--echo #######################罗马字符转换类RN, rn  number_format_model-1-2-7#############################

select to_char(0.000, 'fmRN') from dual;
select to_char(0.000, 'FMRN') from dual;
select to_char(0.000, 'fMRN') from dual;
select to_char(0.000, 'FmRN') from dual;

select to_char(20, 'fmRn') from dual;
select to_char(0.000, 'FMRn') from dual;
select to_char(0.000, 'fMRn') from dual;
select to_char(0.000, 'FmRn') from dual;

select to_char(0.000, 'fmrN') from dual;
select to_char(0.000, 'FMrN') from dual;
select to_char(0.000, 'fMrN') from dual;
select to_char(0.000, 'FmrN') from dual;

select to_char(0.000, 'fmrn') from dual;
select to_char(0.000, 'FMrn') from dual;
select to_char(0.000, 'fMrn') from dual;
select to_char(0.000, 'Fmrn') from dual;

--echo #######################罗马字符转换类RN, rn  预期异常验证#############################
--error 01481
select to_char(123, 'RNFM') from dual;
--error 01481
select to_char(0, 'RNfm') from dual;
--error 01481
select to_char(3999, 'fRNm') from dual;
--error 01481
select to_char(3998, 'RNFM') from dual;
--error 01481
select to_char(123, 'RN,') from dual;
--error 01481
select to_char(0, 'RN.') from dual;
--error 01481
select to_char(0.000, 'RND') from dual;
--error 01481
select to_char(3999, 'RNG') from dual;
--error 01481
select to_char(3998, 'RN$') from dual;
--error 01481
select to_char(3998.0, 'RNC') from dual;
--error 01481
select to_char(3998.0 + 1, 'RNL') from dual;
--error 01481
select to_char(-1, 'RNU') from dual;
--error 01481
select to_char(0.111, 'RN00') from dual;
--error 01481
select to_char(-0.5, 'RN99') from dual;
--error 01481
select to_char(1.5, 'RNeeee') from dual;
--error 01481
select to_char(4000, 'RNrn') from dual;
--error 01481
select to_char(5000, 'RNV') from dual;
--error 01481
select to_char(-200, 'RNx') from dual;
--error 01481
select to_char(3999.5, 'RNMi') from dual;
--error 01481
select to_char(-200, 'RNPR') from dual;
--error 01481
select to_char(3999.5, 'RNs') from dual;
--error 01481
select to_char(-200, 'RNS') from dual;
--error 01481
select to_char(3999.5, 'RNB') from dual;
--error 01481
select to_char(-200, 'RNfm') from dual;
--error 01481
select to_char(3999.5, 'RNFM') from dual;
--error 01481
select to_char(-200, 'RNtme') from dual;
--error 01481
select to_char(3999.5, 'RNTM9') from dual;

--echo #######################指数计算转换类V  number_format_model-1-2-8#############################
select to_char(2, '99v9999999999') from dual;
select to_char(100000000, '9,9999,9v999C') from dual;
select to_char(123, '999999V999$') from dual;
select to_char(0.1,'0,9VC')  from dual;
select to_char(485,'99V9C') from dual;
select to_char(-485,'999VC') from dual;
select to_char(1485,'9,999VC') from dual;
select to_char(100.2356, '99999,99VC') from dual;
select to_char(100.2356, '9,99999999VC') from dual;
select to_char(100.00, '9,99999999VC') from dual;
###select to_char(2, '99v99EEEE') from dual;
###select to_char(2, '9v999EEEE') from dual;
select to_char('', '9,9,9,V9,9MI') from dual;
select to_char(NULL, '9,9V,9,9,9mi') from dual;
select to_char(0, '9,9,9,9,9VPR') from dual;
select to_char(123, '999999V999S') from dual;
select to_char(0.1,'0,V9B')  from dual;
select to_char('', '9,9,9,V9,9X') from dual;
select to_char(NULL, '9,9V,9,9,9XX') from dual;

--echo #######################指数计算转换类V  预期异常验证#############################
--error 01481
select to_char(2, '9v99999D999999') from dual;
--error 01481
select to_char(23, 'FMU99VL') from dual;
--error 01481
select to_char(2, '99v9999999999V') from dual;
--error 01481
select to_char(2, '9v9999999999V9') from dual;
--error 01481
select to_char(23, 'FMU99VV') from dual;
--error 01481
select to_char(100000000, '9,9999,9v999VC') from dual;
--error 01481
select to_char(123, '999999V999Crn') from dual;
--error 01481
select to_char(0.1,'0,9VCRN')  from dual;
--error 01481
select to_char(485,'99V9CRn') from dual;
--error 01481
select to_char(-485,'999VCrN') from dual;
--error 01481
select to_char(1485,'9,999VCtme') from dual;
--error 01481
select to_char(100.2356, '99999,99VCtm9') from dual;
--error 01481
select to_char(100.2356, '9,99999999VCvc') from dual;
--error 01481
select to_char(100.00, '9,99999999VCVC') from dual;
--error 01481
select to_char(2, '99v99XXX') from dual;
--error 01481
select to_char(2, '9v999EEEEXXX') from dual;
--error 01481
select to_char(0, '9,9,9,9,9VX') from dual;
--error 01481
select to_char(123, '999999VX999') from dual;
--error 01481
select to_char(0.1,'0,VX9')  from dual;


--echo #######################十六进制转换类X  number_format_model-1-2-9#############################


select to_char(12345, '0000XX') from dual;
select to_char(100000000, '0000XX') from dual;
select to_char(123, '000XX') from dual;
###select to_char(-0.1,'0000XX') from dual;
###OB与ORACLE结果不一致
###select to_char(0.1,'fmXX')  from dual;
###select to_char(0.1,'FMXXX')  from dual;
select to_char(-485,'XXX') from dual;
select to_char(1485,'XXX') from dual;
select to_char(1234.34*5,'FM0xx') from dual;
###select to_char(-0.1,'FM0XX') from dual;
select to_char(100.2356, 'FM0XXX') from dual;
select to_char(100.2356, 'FM0XXX') from dual;
select to_char(100.00, 'FM0XXX') from dual;
###select to_char(0.2356, 'FM0XX') from dual;
select to_char('', '9X,9,9,9,9') from dual;
select to_char(NULL, '9,9,9X,9,9') from dual;
###select to_char(1234.34,'0000.00X') from dual;

--echo #######################十六进制转换类X  预期异常验证#############################
--error 01481
select to_char(12,'XXfm') from dual;
--error 01481
select to_char(485,'XXXFM') from dual;
--error 01481
select to_char(32, 'xx0000') from dual;
--error 01481
select to_char(32, 'xx0000') from dual;
--error 01481
select to_char(32, 'xx,') from dual;
--error 01481
select to_char(32, 'xxg') from dual;
--error 01481
select to_char(12345, 'XX.') from dual;
--error 01481
select to_char(100000000, 'XXd') from dual;
--error 01481
select to_char(123, 'XX999') from dual;
--error 01481
select to_char(-0.1,'XX9') from dual;
--error 01481
select to_char(0.1,'XX9')  from dual;
--error 01481
select to_char(0.1,'XXX9')  from dual;
--error 01481
select to_char(12,'XX009') from dual;
--error 01481
select to_char(485,'XXX900') from dual;
--error 01481
select to_char(-485,'XXX09') from dual;
--error 01481
select to_char(1485,'XXX90') from dual;
--error 01481
select to_char(100.2356, 'XXX$') from dual;
--error 01481
select to_char(100.2356, '$XXX') from dual;
--error 01481
select to_char(100.00, 'XXXc') from dual;
--error 01481
select to_char(0.2356, 'XXL') from dual;
--error 01481
select to_char(12345+12345, 'XXU') from dual;
--error 01481
select to_char(12345-12345, 'XXXXeeee') from dual;
--error 01481
select to_char(12345*5, 'xxxRN') from dual;
--error 01481
select to_char(123434/6,'FM0xxxRN') from dual;
--error 01481
select to_char(1234.34*5,'FM0xxrn') from dual;
--error 01481
select to_char(-0.1,'FM0VXX') from dual;
--error 01481
select to_char(100.2356, 'FM0XVXX') from dual;
--error 01481
select to_char(100.2356, 'FM0XXvX') from dual;
--error 01481
select to_char(100.00, 'FM0VXXX') from dual;
--error 01481
select to_char(0.2356, 'FM0BXX') from dual; 
--error 01481
select to_char(1234.34,'Xtm9') from dual;


--echo #######################正负符号类MI, PR, S  number_format_model-1-2-10#############################


select to_char(12345, '9,,,9,,9,9,9$Mi') from dual;
select to_char(0.1,'0G9Mi')  from dual;
select to_char(12,'9990999.9eeeeMi') from dual;
select to_char(485,'999EEEEMi') from dual;
select to_char(100.00, '9,9999999v9Mi') from dual;
select to_char(0.2356, '9,99999999VMi') from dual;
select to_char(0+0, '9,9,9,9,9vPR') from dual;
select to_char(0*0, '9,9,9,9,9VPR') from dual;
select to_char(-1234.34,'.bPR') from dual;
select to_char(-1234.34,'0000.00BPR') from dual;
select to_char(-1234.34+1234.56,'FM9,9,9,9.99PR') from dual;
select to_char(-1234, 'S999V9') from dual;
select to_char(1234, 'S999V9') from dual;
select to_char(1234, 'fm9999s') from dual;
select to_char(-1234, 's9b999') from dual;
select to_char(1234, 'sb9999') from dual;
select to_char(1234, 'FMSb9999') from dual;
select to_char(1234, 'FMsb9999') from dual;

--echo #######################正负符号类MI, PR, S  预期异常验证#############################
--error 01481
select to_char(-1234, '9999sfm') from dual;
--error 01481
select to_char(-1234, '9999Seeee') from dual;
--error 01481
select to_char(1234, '9999Seeee') from dual;
--error 01481
select to_char(-123434/6,'9,9,9,9.99FMPR') from dual;
--error 01481
select to_char(-1234.34*5,'9,9,9,99.9PRFM') from dual;
--error 01481
select to_char(-123*0.34,'9999.9FMPR') from dual;
--error 01481
select to_char(-1234.34,'9,9,9,9.V99PR') from dual;
--error 01481
select to_char(-1234.34,'9,,,,,,,,,,99V,9.99PR') from dual;
--error 01481
select to_char(-0.2356, '999V9,.9PR') from dual;
--error 01481
select to_char(-1234.34,'9,9,9,.9V99PR') from dual;
--error 01481
select to_char(100.2356, '99999,99MiEEEE') from dual;
--error 01481
select to_char(100.2356, '9,99999999EEEEMi') from dual;
--error 01481
select to_char(-485,'999MiEEEE') from dual;
--error 01481
select to_char(1485,'9,999MiEEEE') from dual;
--error 01481
select to_char(1234, 'L9999Mi$') from dual;
--error 01481
select to_char(100000000, '9,9999,9999Mic') from dual;
--error 01481
select to_char(100000000, '999,999,999,,,MiC') from dual;
--error 01481
select to_char(123, '999999999MiL') from dual;
--error 01481
select to_char(-0.1,'99.99MiU') from dual;
--error 01481
select to_char(0.1,'0d9MiU')  from dual;
--error 01481
select to_char(1234, 'L9999MiPR') from dual;
--error 01481
select to_char(12345, 'PR9,,,9,,9,9,9Mi') from dual;
--error 01481
select to_char(100000000, 'PR9,9999,9999Mi') from dual;
--error 01481
select to_char(100000000, '999,99S9,999,,,Mi') from dual;
--error 01481
select to_char(123, '999999999SMi') from dual;
--error 01481
select to_char(-0.1,'99.99MiS') from dual;
--error 01481
select to_char(0.1,'S0.9Mi')  from dual;
--error 01481
select to_char(0.1,'0,9MiS')  from dual;
--error 01481
select to_char(12,'9990999.9Mirn') from dual;
--error 01481
select to_char(485,'999MiRN') from dual;
--error 01481
select to_char(-485,'999MiRn') from dual;
--error 01481
select to_char(1485,'9,999MirN') from dual;
--error 01481
select to_char(100.2356, '99999,99Mitme') from dual;
--error 01481
select to_char(100.2356, '9,99999999Mitm9') from dual;
--error 01481
select to_char(100.00, '9,99999999MiTME') from dual;
--error 01481
select to_char(0.2356, '9,99999999MiTM9') from dual;
--error 01481
select to_char(0+0, '9,9,9,9,9PRXXX') from dual;
--error 01481
select to_char(0*0, '9,9,9,9,9PRXXXX') from dual;
--error 01481
select to_char(-1234.34,'9,9,9,9.99PRXX') from dual;
--error 01481
select to_char(-1234.34,'9,,,,,,,,,,99,9.99PRMi') from dual;
--error 01481
select to_char(-0.2356, '9999,.9PRs') from dual;
--error 01481
select to_char(-1234.34,'9,9,9,.999SPR') from dual;
--error 01481
select to_char(-1234.34,'.PRmI') from dual;
--error 01481
select to_char(-1234.34,'0000.00PRMi') from dual;
--error 01481
select to_char(-123*0.34,'s9999.9PR') from dual;
--error 01481
select to_char(-1234.34+1234.56,'S9,9,9,9.99PR') from dual;
--error 01481
select to_char(-123434/6,'9,9,9,9.99SPR') from dual;
--error 01481
select to_char(-1234.34*5,'9,9,9,99.9XXXPR') from dual;
--error 01481
select to_char(-1234, '9999SmI') from dual;
--error 01481
select to_char(1234, '9999SMi') from dual;
--error 01481
select to_char(-1234, 'S9999Pr') from dual;
--error 01481
select to_char(1234, 'S9999PR') from dual;
--error 01481
select to_char(-1234, '9999stme') from dual;
--error 01481
select to_char(1234, '9999sTM9') from dual;
--error 01481
select to_char(-1234, 's9999TME') from dual;
--error 01481
select to_char(1234, 's9999tm9') from dual;
--error 01481
select to_char(1234, 'FMS9999XX') from dual;
--error 01481
select to_char(1234, 'FMs9999RN') from dual;

--echo #######################空格填充类B  number_format_model-1-2-11#############################

select to_char(0, '009B') from dual;
select to_char(0.00, '9$B') from dual;
select to_char(0*0, '9CB') from dual;
select to_char(0+0, '9LB') from dual;
select to_char(0, '0UB') from dual;
select to_char(123, '9vB') from dual;
select to_char(1*0.3, 'FM9B') from dual;
select to_char(0, '9,BS') from dual;
select to_char(0*0, '9BMI') from dual;
select to_char(0+0, '9BPR') from dual;

--echo #######################空格填充类B  预期异常验证#############################
--error 01481
select to_char(0+3, '9BB') from dual;
--error 01481
select to_char(100, '0BBBB') from dual;
--error 01481
select to_char(00001, '9,BBBBBBB') from dual;
--error 01481
select to_char(0.012, '9xxxxB') from dual;
--error 01481
select to_char(0, '9,eeeeB') from dual;
--error 01481
select to_char(0, '9Brn') from dual;
--error 01481
select to_char(0.00, '9BRN') from dual;
--error 01481
select to_char(0, '0SB') from dual;
--error 01481
select to_char(123, '9BXX') from dual;
--error 01481
select to_char(0.012, '9XXB') from dual;
--error 01481
select to_char(1*0.3, 'XX9B') from dual;
--error 01481
select to_char(0+3, '9sB') from dual;
--error 01481
select to_char(100, '0BRN') from dual;
--error 01481
select to_char(00001, '9,Brn') from dual;
--error 01481
select to_char(0+3, '9TM9B') from dual;
--error 01481
select to_char(100, '0BTME') from dual;
--error 01481
select to_char(00001, '9,BTM9') from dual;

--echo #######################填充模式类FM  number_format_model-1-2-12#############################
select to_char(23, 'FMB9d9') from dual;
select to_char(' 23', 'FMB9,9') from dual;
select to_char('                                                                                                                                                    23', 'FMB99,') from dual;
select to_char('                 23                                                                                                                                   ', 'FMB9d9') from dual;
select to_char(12345, 'fm9,,,9,,9,9,9$') from dual;
select to_char(100000000, 'FM9,9999,9999c') from dual;
select to_char(123, 'FM999999999EEEE') from dual;
select to_char(12,'FM9990999.9pr') from dual;
select to_char(12,'FM9990999.9s') from dual;
select to_char(12,'FM9990999.9S') from dual;
select to_char(12,'FM9990999.9B') from dual;

--echo #######################填充模式类FM  预期异常验证#############################
--error 01481
select to_char(-0.1,'FM99.99rn') from dual;
--error 01481
select to_char(0.1,'FM0.9v')  from dual;
--error 01481
select to_char(0.1,'FM0,9xxMI')  from dual;
--error 01481
select to_char(12,'FM9990999.9FM') from dual;
--error 01481
select to_char(12,'FM9990999.9TM9') from dual;
--error 01481
select to_char(100000000, 'FM999,999,999L,,,') from dual;

--echo #######################输出格式类TME, TM9  number_format_model-1-2-13#############################

select to_char(1234, 'FMtme') from dual;
select to_char(1234, 'fmtm9') from dual;
###select to_char(99999999999999999999999999999999999999999999999999999999999999999, 'FMtme') from dual;
###select to_char(99999999999999999999999999999999999999999999999999999999999999999, 'FMtm9') from dual;
###select to_char(9999999999999999999999999999999999999999999999999999999999999999, 'FMtme') from dual;
###select to_char(9999999999999999999999999999999999999999999999999999999999999999, 'FMtm9') from dual;
###select to_char(999999999999999999999999999999999999999999999999999999999999999, 'FMtme') from dual;
###select to_char(999999999999999999999999999999999999999999999999999999999999999, 'FMtm9') from dual;
select to_char('', 'tmexx') from dual;
select to_char(NULL, 'tmex') from dual;
select to_char('', 'fmmtme') from dual;
select to_char(NULL, 'ftme') from dual;
--echo #######################输出格式类TME, TM9  预期异常验证#############################
--error 01481
select to_char(12345, 'tmefm') from dual;
--error 01481
select to_char(100000000, 'tmeFM') from dual;
--error 01481
select to_char(100000000, 'tmeFM') from dual;
--error 01481
select to_char(1234, ',tme') from dual;
--error 01481
select to_char(1234, '.tm9') from dual;
--error 01481
select to_char(99999999999999999999999999999999999999999999999999999999999999999, '0tme') from dual;
--error 01481
select to_char(99999999999999999999999999999999999999999999999999999999999999999, '9tm9') from dual;
--error 01481
select to_char(9999999999999999999999999999999999999999999999999999999999999999, 'tme9') from dual;
--error 01481
select to_char(9999999999999999999999999999999999999999999999999999999999999999, 'tm99') from dual;
--error 01481
select to_char(999999999999999999999999999999999999999999999999999999999999999, 'tme$') from dual;
--error 01481
select to_char(999999999999999999999999999999999999999999999999999999999999999, 'tm9c') from dual;
--error 01481
select to_char(12345, 'tmel') from dual;
--error 01481
select to_char(100000000, 'tmeU') from dual;
--error 01481
select to_char(100000000, 'tmeC') from dual;
--error 01481
select to_char(123, 'tmeCL') from dual;
--error 01481
select to_char(-0.1,'tmeeeee') from dual;
--error 01481
select to_char(0.1,'tmeeee')  from dual;
--error 01481
select to_char(0.1,'tmeEEEE')  from dual;
--error 01481
select to_char(12,'tmern') from dual;
--error 01481
select to_char(485,'tmeRN') from dual;
--error 01481
select to_char(-485,'tmev') from dual;
--error 01481
select to_char(1485,'tmve') from dual;
--error 01481
select to_char(100.2356, 'vtme') from dual;
--error 01481
select to_char(0, 'xtme') from dual;
--error 01481
select to_char(12,'tmeMi') from dual;
--error 01481
select to_char(485,'tmePR') from dual;
--error 01481
select to_char(-485,'tmeS') from dual;
--error 01481
select to_char(1485,'tmEB') from dual;
--error 01481
select to_char(100.2356, 'Btme') from dual;
--error 01481
select to_char(0, '9tme') from dual;