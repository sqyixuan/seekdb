delimiter //;
CREATE OR REPLACE PACKAGE PDMS.pkg_number_trans IS

  FUNCTION f_bin_to_oct(p_str IN VARCHAR2) RETURN VARCHAR2;

  FUNCTION f_bin_to_dec(p_str IN VARCHAR2) RETURN VARCHAR2;

  FUNCTION f_bin_to_hex(p_str IN VARCHAR2) RETURN VARCHAR2;

  FUNCTION f_oct_to_bin(p_str IN VARCHAR2) RETURN VARCHAR2;

  FUNCTION f_oct_to_dec(p_str IN VARCHAR2) RETURN VARCHAR2;

  FUNCTION f_oct_to_hex(p_str IN VARCHAR2) RETURN VARCHAR2;

  FUNCTION f_hex_to_bin(p_str IN VARCHAR2) RETURN VARCHAR2;

  FUNCTION f_hex_to_oct(p_str IN VARCHAR2) RETURN VARCHAR2;

  FUNCTION f_hex_to_dec(p_str IN VARCHAR2) RETURN VARCHAR2;

  FUNCTION f_dec_to_bin(p_int IN VARCHAR2) RETURN VARCHAR2;

  FUNCTION f_dec_to_oct(p_int IN VARCHAR2) RETURN VARCHAR2;

  FUNCTION f_dec_to_hex(p_int IN VARCHAR2) RETURN VARCHAR2;

END pkg_number_trans;
//

CREATE OR REPLACE PACKAGE BODY PDMS.pkg_number_trans IS


  FUNCTION f_bin_to_oct(p_str IN VARCHAR2) RETURN VARCHAR2 IS
    ----------------------------------------------------------------------------------------------------------------------
    -- 对象名称: f_bin_to_oct
    -- 对象描述: 二进制转换八进制
    -- 输入参数: p_str 二进制字符串
    -- 返回结果: 八进制字符串
    -- 测试用例: SELECT pkg_number_trans.f_bin_to_oct('11110001010') FROM dual;
    -- 备    注: 需要定义f_stragg函数和type_str_agg类型
    ----------------------------------------------------------------------------------------------------------------------
    v_return VARCHAR2(4000);
    v_bin    VARCHAR2(4000);
  BEGIN
    v_bin := substr('00' || p_str, -3 * ceil(length(p_str) / 3));
    select listagg(data1,'') within group(order by data1) INTO v_return
      FROM (SELECT (CASE upper(substr(v_bin, (rownum - 1) * 3 + 1, 3))
                     WHEN '000' THEN '0'
                     WHEN '001' THEN '1'
                     WHEN '010' THEN '2'
                     WHEN '011' THEN '3'
                     WHEN '100' THEN '4'
                     WHEN '101' THEN '5'
                     WHEN '110' THEN '6'
                     WHEN '111' THEN '7'
                   END) data1
              FROM dual
            CONNECT BY rownum <= length(v_bin) / 3);
    RETURN v_return;
  EXCEPTION
    WHEN OTHERS THEN
      RETURN NULL;
  END f_bin_to_oct;

  FUNCTION f_bin_to_dec(p_str IN VARCHAR2) RETURN VARCHAR2 IS
    ----------------------------------------------------------------------------------------------------------------------
    -- 对象名称: f_bin_to_dec
    -- 对象描述: 二进制转换十进制
    -- 输入参数: p_str 二进制字符串
    -- 返回结果: 十进制字符串
    -- 测试用例: SELECT pkg_number_trans.f_bin_to_dec('11110001010') FROM dual;
    ----------------------------------------------------------------------------------------------------------------------
    v_return  VARCHAR2(4000);
  BEGIN
    SELECT SUM(data1) INTO v_return
      FROM (SELECT substr(p_str, rownum, 1) * power(2, length(p_str) - rownum) data1
              FROM dual
            CONNECT BY rownum <= length(p_str));
    RETURN v_return;
  EXCEPTION
    WHEN OTHERS THEN
      RETURN NULL;
  END f_bin_to_dec;

  FUNCTION f_bin_to_hex(p_str IN VARCHAR2) RETURN VARCHAR2 IS
    ----------------------------------------------------------------------------------------------------------------------
    -- 对象名称: f_bin_to_hex
    -- 对象描述: 二进制转换十六进制
    -- 输入参数: p_str 二进制字符串
    -- 返回结果: 十六进制字符串
    -- 测试用例: SELECT pkg_number_trans.f_bin_to_oct('11110001010') FROM dual;
    -- 备    注: 需要定义f_stragg函数和type_str_agg类型
    ----------------------------------------------------------------------------------------------------------------------
    v_return VARCHAR2(4000);
    v_bin    VARCHAR2(4000);
  BEGIN
    v_bin := substr('000' || p_str, -4 * ceil(length(p_str) / 4));
    select listagg(data1,'') within group(order by data1) INTO v_return
      FROM (SELECT (CASE upper(substr(v_bin, (rownum - 1) * 4 + 1, 4))
                     WHEN '0000' THEN '0'
                     WHEN '0001' THEN '1'
                     WHEN '0010' THEN '2'
                     WHEN '0011' THEN '3'
                     WHEN '0100' THEN '4'
                     WHEN '0101' THEN '5'
                     WHEN '0110' THEN '6'
                     WHEN '0111' THEN '7'
                     WHEN '1000' THEN '8'
                     WHEN '1001' THEN '9'
                     WHEN '1010' THEN 'A'
                     WHEN '1011' THEN 'B'
                     WHEN '1100' THEN 'C'
                     WHEN '1101' THEN 'D'
                     WHEN '1110' THEN 'E'
                     WHEN '1111' THEN 'F'
                   END) data1
              FROM dual
            CONNECT BY rownum <= length(v_bin) / 4);
    RETURN v_return;
  EXCEPTION
    WHEN OTHERS THEN
      RETURN NULL;
  END f_bin_to_hex;

  FUNCTION f_oct_to_bin(p_str IN VARCHAR2) RETURN VARCHAR2 IS
    ----------------------------------------------------------------------------------------------------------------------
    -- 对象名称: f_oct_to_bin
    -- 对象描述: 八进制转换二进制
    -- 输入参数: p_str 八进制字符串
    -- 返回结果: 二进制字符串
    -- 测试用例: SELECT pkg_number_trans.f_oct_to_bin('3612') FROM dual;
    -- 备    注: 需要定义f_stragg函数和type_str_agg类型
    ----------------------------------------------------------------------------------------------------------------------
    v_return VARCHAR2(4000);
  BEGIN
    select listagg(data1,'') within group(order by data1) INTO v_return
      FROM (SELECT (CASE upper(substr(p_str, rownum, 1))
                     WHEN '0' THEN '000'
                     WHEN '1' THEN '001'
                     WHEN '2' THEN '010'
                     WHEN '3' THEN '011'
                     WHEN '4' THEN '100'
                     WHEN '5' THEN '101'
                     WHEN '6' THEN '110'
                     WHEN '7' THEN '111'
                   END) data1
              FROM dual
            CONNECT BY rownum <= length(p_str));
    RETURN v_return;
  EXCEPTION
    WHEN OTHERS THEN
      RETURN NULL;
  END f_oct_to_bin;

  FUNCTION f_oct_to_dec(p_str IN VARCHAR2) RETURN VARCHAR2 IS
    ----------------------------------------------------------------------------------------------------------------------
    -- 对象名称: f_oct_to_dec
    -- 对象描述: 八进制转换十进制
    -- 输入参数: p_str 八进制字符串
    -- 返回结果: 十进制字符串
    -- 测试用例: SELECT pkg_number_trans.f_oct_to_dec('3612') FROM dual;
    ----------------------------------------------------------------------------------------------------------------------
    v_return  VARCHAR2(4000);
  BEGIN
    SELECT SUM(data1) INTO v_return
      FROM (SELECT substr(p_str, rownum, 1) * power(8, length(p_str) - rownum) data1
              FROM dual
            CONNECT BY rownum <= length(p_str));
    RETURN v_return;
  EXCEPTION
    WHEN OTHERS THEN
      RETURN NULL;
  END f_oct_to_dec;

  FUNCTION f_oct_to_hex(p_str IN VARCHAR2) RETURN VARCHAR2 IS
    ----------------------------------------------------------------------------------------------------------------------
    -- 对象名称: f_oct_to_bin
    -- 对象描述: 八进制转换十六进制
    -- 输入参数: p_str 八进制字符串
    -- 返回结果: 十六进制字符串
    -- 测试用例: SELECT pkg_number_trans.f_oct_to_hex('3612') FROM dual;
    ----------------------------------------------------------------------------------------------------------------------
    v_return VARCHAR2(4000);
    v_bin    VARCHAR2(4000);
  BEGIN
    SELECT pkg_number_trans.f_oct_to_bin(p_str) INTO v_bin FROM dual;
    SELECT pkg_number_trans.f_bin_to_hex(v_bin) INTO v_return FROM dual;
    RETURN v_return;
  EXCEPTION
    WHEN OTHERS THEN
      RETURN NULL;
  END f_oct_to_hex;

  FUNCTION f_dec_to_bin(p_int IN VARCHAR2) RETURN VARCHAR2 IS
    ----------------------------------------------------------------------------------------------------------------------
    -- 对象名称: f_dec_to_bin
    -- 对象描述: 十进制转换二进制
    -- 输入参数: p_str 十进制字符串
    -- 返回结果: 二进制字符串
    -- 测试用例: SELECT pkg_number_trans.f_dec_to_bin('1930') FROM dual;
    ----------------------------------------------------------------------------------------------------------------------
    v_return VARCHAR2(4000);
    v_hex    VARCHAR2(4000);
  BEGIN
    SELECT pkg_number_trans.f_dec_to_hex(p_int) INTO v_hex FROM dual;
    SELECT pkg_number_trans.f_hex_to_bin(v_hex) INTO v_return FROM dual;
    RETURN v_return;
  EXCEPTION
    WHEN OTHERS THEN
      RETURN NULL;
  END f_dec_to_bin;

  FUNCTION f_dec_to_oct(p_int IN VARCHAR2) RETURN VARCHAR2 IS
    ----------------------------------------------------------------------------------------------------------------------
    -- 对象名称: f_dec_to_oct
    -- 对象描述: 十进制转换八进制
    -- 输入参数: p_str 十进制字符串
    -- 返回结果: 八进制字符串
    -- 测试用例: SELECT pkg_number_trans.f_dec_to_oct('1930') FROM dual;
    ----------------------------------------------------------------------------------------------------------------------
    v_return VARCHAR2(4000);
    v_bin    VARCHAR2(4000);
  BEGIN
    SELECT pkg_number_trans.f_dec_to_bin(p_int) INTO v_bin FROM dual;
    v_bin := substr('00' || v_bin, -3 * ceil(length(v_bin) / 3));
    select listagg(data1,'') within group(order by data1) INTO v_return
      FROM (SELECT (CASE upper(substr(v_bin, (rownum - 1) * 3 + 1, 3))
                     WHEN '000' THEN '0'
                     WHEN '001' THEN '1'
                     WHEN '010' THEN '2'
                     WHEN '011' THEN '3'
                     WHEN '100' THEN '4'
                     WHEN '101' THEN '5'
                     WHEN '110' THEN '6'
                     WHEN '111' THEN '7'
                   END) data1
              FROM dual
            CONNECT BY rownum <= length(v_bin) / 3);
    RETURN v_return;
  EXCEPTION
    WHEN OTHERS THEN
      RETURN NULL;
  END f_dec_to_oct;

  FUNCTION f_dec_to_hex(p_int IN VARCHAR2) RETURN VARCHAR2 IS
    ----------------------------------------------------------------------------------------------------------------------
    -- 对象名称: f_dec_to_oct
    -- 对象描述: 十进制转换十六进制
    -- 输入参数: p_str 十进制字符串
    -- 返回结果: 十六进制字符串
    -- 测试用例: SELECT pkg_number_trans.f_dec_to_hex('1930') FROM dual;
    ----------------------------------------------------------------------------------------------------------------------
    v_return VARCHAR2(4000);
  BEGIN
    SELECT upper(TRIM(to_char(p_int, 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'))) INTO v_return FROM dual;
    RETURN v_return;
  EXCEPTION
    WHEN OTHERS THEN
      RETURN NULL;
  END f_dec_to_hex;

  FUNCTION f_hex_to_bin(p_str IN VARCHAR2) RETURN VARCHAR2 IS
    ----------------------------------------------------------------------------------------------------------------------
    -- 对象名称: f_dec_to_oct
    -- 对象描述: 十六进制转换二进制
    -- 输入参数: p_str 十六进制字符串
    -- 返回结果: 二进制字符串
    -- 测试用例: SELECT pkg_number_trans.f_hex_to_oct('78A') FROM dual;
    ----------------------------------------------------------------------------------------------------------------------
    v_return VARCHAR2(4000);
  BEGIN
    select listagg(data1,'') within group(order by data1) INTO v_return
      FROM (SELECT (CASE upper(substr(p_str, rownum, 1))
                     WHEN '0' THEN '0000'
                     WHEN '1' THEN '0001'
                     WHEN '2' THEN '0010'
                     WHEN '3' THEN '0011'
                     WHEN '4' THEN '0100'
                     WHEN '5' THEN '0101'
                     WHEN '6' THEN '0110'
                     WHEN '7' THEN '0111'
                     WHEN '8' THEN '1000'
                     WHEN '9' THEN '1001'
                     WHEN 'A' THEN '1010'
                     WHEN 'B' THEN '1011'
                     WHEN 'C' THEN '1100'
                     WHEN 'D' THEN '1101'
                     WHEN 'E' THEN '1110'
                     WHEN 'F' THEN '1111'
                   END) data1
              FROM dual
            CONNECT BY rownum <= length(p_str));
    RETURN v_return;
  EXCEPTION
    WHEN OTHERS THEN
      RETURN NULL;
  END f_hex_to_bin;

  FUNCTION f_hex_to_oct(p_str IN VARCHAR2) RETURN VARCHAR2 IS
    ----------------------------------------------------------------------------------------------------------------------
    -- 对象名称: f_dec_to_oct
    -- 对象描述: 十六进制转换八进制
    -- 输入参数: p_str 十六进制字符串
    -- 返回结果: 八进制字符串
    -- 测试用例: SELECT pkg_number_trans.f_hex_to_oct('78A') FROM dual;
    ----------------------------------------------------------------------------------------------------------------------
    v_return VARCHAR2(4000);
    v_bin    VARCHAR2(4000);
  BEGIN
    SELECT pkg_number_trans.f_hex_to_bin(p_str) INTO v_bin FROM dual;
    SELECT pkg_number_trans.f_bin_to_oct(v_bin) INTO v_return FROM dual;
    RETURN v_return;
  EXCEPTION
    WHEN OTHERS THEN
      RETURN NULL;
  END f_hex_to_oct;

  FUNCTION f_hex_to_dec(p_str IN VARCHAR2) RETURN VARCHAR2 IS
    ----------------------------------------------------------------------------------------------------------------------
    -- 对象名称: f_hex_to_dec
    -- 对象描述: 十六进制转换十进制
    -- 输入参数: p_str 十六进制字符串
    -- 返回结果: 十进制字符串
    -- 测试用例: SELECT pkg_number_trans.f_hex_to_dec('78A') FROM dual;
    ----------------------------------------------------------------------------------------------------------------------
    v_return  VARCHAR2(4000);
  BEGIN
    SELECT SUM(data1) INTO v_return
      FROM (SELECT (CASE upper(substr(p_str, rownum, 1))
                     WHEN 'A' THEN '10'
                     WHEN 'B' THEN '11'
                     WHEN 'C' THEN '12'
                     WHEN 'D' THEN '13'
                     WHEN 'E' THEN '14'
                     WHEN 'F' THEN '15'
                     ELSE substr(p_str, rownum, 1)
                   END) * power(16, length(p_str) - rownum) data1
              FROM dual
            CONNECT BY rownum <= length(p_str));
    RETURN v_return;
  EXCEPTION
    WHEN OTHERS THEN
      RETURN NULL;
  END f_hex_to_dec;

END pkg_number_trans;
//
delimiter ;//