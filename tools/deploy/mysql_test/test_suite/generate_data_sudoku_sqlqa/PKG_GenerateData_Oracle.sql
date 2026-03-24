delimiter //;
create or replace package PKG_GenerateData is
  type indx_type is table of varchar(1000) index by varchar(1000);
  NUMBER_RANGE varchar(10) := '6';     -- 全局设定,指定数范围 -6位 <-> +6位
  NUMBER_PRECISION varchar(10) := '2'; -- 全局设定,指定小数位精度
  STR_LEN varchar(100) := '6';         -- 全局设定, 生成字符长度
  CLOB_BLOB_LEN varchar(100) := '6';   -- 全局设定, lob型长度
  SYS_VALUE varchar(2000) := sysdate;  -- date型的默认值
  STR_TYPE varchar(10) := 'l';         -- 字符格式,同DBMS_RANDOM.STRING
  MIN_DATE varchar(100) := '19900101'; -- 最小日期
  MAX_DATE varchar(100) := '21000101'; -- 最大日期


  /*
    以下procedure为对外暴露的接口
  */
  -- 用于设定自增主键
  procedure CustomSetPk(tableName varchar, columnName varchar);
  
  -- 生成范围内均匀分布数
  procedure CustomSetRange(tableName varchar, columnName varchar, 
    minValue varchar default null, maxValue varchar default null);
	
  -- 生成符合正太分布的数
  procedure CustomSetNormal(tableName varchar, columnName varchar,
    mu varchar default 0, sigma varchar default 1);

  -- 生成符合伽马分布的数
  procedure CustomSetGamma(tableName varchar, columnName varchar,
    alpha varchar default 1, beta varchar default 1);
  
  -- 生成符合高斯分布的数
  procedure CustomSetGauss(tableName varchar, columnName varchar,
    mu varchar default 0, sigma varchar default 1);

  -- 生成符合卡帕分布的数
  procedure CustomSetKappa(tableName varchar, columnName varchar,
    mu varchar default 0, kappa varchar default 1);

  -- 为列设定默认值
  procedure CustomSetValue(tableName varchar, columnName varchar,
    setValue varchar);
  
  -- 生成指定范围内日期
  procedure CustomSetDateRange(tableName varchar, columnName varchar,
    minDate varchar2 default MIN_DATE, maxDate varchar2 default MAX_DATE);
	
  -- 用于设定字符长度及生成模式,
  -- strType取值同DBMS_RANDOM.STRING
  procedure CustomSetCharModel(tableName varchar, columnName varchar,
    strLen varchar, strType varchar default STR_TYPE);




  -- 生成数据的入口
  procedure LoadDate(tableName varchar2, rowCount number default 1);
	
  -- 以下用于核对最终的map表 tableInfo
  procedure ShowInfo;


  function StrRandom(tableInfo_val varchar2) return varchar2;
  function NumberRandom(tableInfo_val varchar2) return varchar2;
  function NumberNormal(tableInfo_val varchar2) return varchar2;
  function NumberPk(tableInfo_val varchar2) return varchar2;
  function NumberGamma(tableInfo_val varchar2) return varchar2;
  function NumberGauss(tableInfo_val varchar2) return varchar2;
  function NumberKappa(tableInfo_val varchar2) return varchar2;
  function FormatResult(resultValue varchar2) return varchar2;
  
  function ParserChar(tableInfo_val varchar2) return varchar2;
  function ParserNumber(tableInfo_val varchar2) return varchar2;
  function ParserDate(tableInfo_val varchar2) return varchar2;
end PKG_GenerateData;
//

create or replace package body PKG_GenerateData is
  tableInfo indx_type;                 -- 存放列名及类型限制, {列名:类型(限制)}
  type_function_map indx_type;         -- 存放各类型和处理子程序的映射
  generate_model_map indx_type;         -- 存放各类型和处理子程序的映射
  currentTable varchar(500);           -- 用于标识当前表名
  column_name_list varchar(5000);
  column_value_list varchar(5000);
  UpdateTableInfo_status boolean;
  CheckNumber_status boolean;
  InsertOne_status boolean;
  CURRENT_PK varchar(50) := 0;
  SELECT_PK boolean := true;
  GAUSS_NEXT varchar(100) := 'null';


  /*
    从系统表user_tab_columns中获取列名及其类型限制,构建InitTableMap
    1. tableName    指定构建哪个表
  */
  procedure InitTableMap(tableName varchar) is
    type nt_type is table of varchar(200);
    column_name1 nt_type;
    column_type nt_type;
    column_integer nt_type;
    column_decimal nt_type;
    get_column_info_sql varchar2(2000);
    limit_number varchar2(200);
  begin
	tableInfo.DELETE;
    get_column_info_sql := '
      SELECT a.column_name,
         a.data_type || ''('' || a.data_length || '')'' as column_type,
         a.data_precision,
         a.data_Scale
      FROM user_tab_columns a
      WHERE a.TABLE_NAME = ''' || UPPER(tableName) || '''
    ';
    execute immediate get_column_info_sql 
    bulk collect into column_name1,column_type,column_integer,column_decimal;

    for i in 1 .. column_name1.count loop
      if instr(column_type(i),upper('number')) > 0 then
        -- number这里也有两种形式, 一种是带()或不带()
        if column_integer(i) is null and column_decimal(i) is null then
          tableInfo(column_name1(i)) := 'NUMBER';
        else
          tableInfo(column_name1(i)) := 'NUMBER(' || trim(',' from column_integer(i) || ',' || column_decimal(i)) || ')';
        end if;
	  elsif instr(column_type(i),upper('BINARY_FLOAT')) > 0 then
	    tableInfo(column_name1(i)) := 'BINARY_FLOAT';
	  elsif instr(column_type(i),upper('BINARY_DOUBLE')) > 0 then
	    tableInfo(column_name1(i)) := 'BINARY_DOUBLE';
	  elsif instr(column_type(i),upper('BLOB')) > 0 then
	    tableInfo(column_name1(i)) := 'BLOB(' || CLOB_BLOB_LEN || ')';
	  elsif instr(column_type(i),upper('CLOB')) > 0 then
	    tableInfo(column_name1(i)) := 'CLOB(' || CLOB_BLOB_LEN || ')';
      elsif instr(column_type(i),upper('float')) > 0 then
        -- float这里也有两种形式, 一种是float(10)或float
        if column_integer(i) is not null then
          tableInfo(column_name1(i)) := 'FLOAT(' || column_integer(i) || ')';
        else
          tableInfo(column_name1(i)) := 'FLOAT';
        end if;
	  -- nchar和nvarchar2类型的限制会翻倍,ob中正常
	  elsif instr(column_type(i),upper('NCHAR')) > 0 or 
	    instr(column_type(i),upper('NVARCHAR2')) > 0 then
		limit_number := 
		  to_number(regexp_replace(column_type(i), '.*\((.*)\).*', '\1'))/2;
		tableInfo(column_name1(i)) := 
		  regexp_replace(column_type(i), '(.*)\(.*\).*', '\1') || 
		  '(' || limit_number || ')';
      else
        tableInfo(column_name1(i)) := column_type(i);
      end if;
    end loop;
	dbms_output.put_line(tableName || '表映射关系构建完成......');
  end InitTableMap;



  /*
    用于校验设定数字场景中的值是否和列限定冲突
    1. 参数含义参考CustomSet()上的注释
	2. 如设定值和列限定冲突会报错.
	3. original_value   表示number()中的内容.如:'6'或'6,2'
  */
  procedure CheckNumber(columnName varchar, columnType varchar, minValue varchar, maxValue varchar, dataModel varchar, setValue varchar, mu varchar, sigma varchar,
  alpha varchar, beta varchar, kappa varchar) 
  is
  original_value varchar2(2000);
  parameter_integer varchar2(500);
  original_integer varchar2(500);
  original_decimal varchar2(500);
  limit_number varchar2(500);
  column_type varchar2(2000) := columnType;
  begin
    CheckNumber_status := true;
	
    -- 排除掉(0)的存在
	if regexp_replace(column_type, '.*\((.*)\).*', '\1') = '0' then
	  column_type := regexp_replace(column_type, '(.*)\(.*\).*', '\1');
	end if;
	
	-- 从column_type中匹配出括号中内容, 两种形式(n),(m,n)
    if instr(column_type,'(') > 0 then
	  original_value := regexp_replace(column_type, '.*\((.*)\).*', '\1');
	  if instr(original_value,',') > 0 then
		original_integer := regexp_replace(original_value, '(.*),.*', '\1');
		original_decimal := regexp_replace(original_value, '.*,(.*)', '\1');
		limit_number := to_number(original_integer) - to_number(original_decimal);
	  else
	    limit_number := original_value;
	  end if;
	  
	  -- 取设定范围中的最大值,要求不能超出列限定的最大数
	  if instr(minValue,'.') > 0 then
		parameter_integer := regexp_replace(minValue, '(.*)\..*', '\1');
	  else
	    parameter_integer := minValue;
	  end if;
	  if maxValue is not null then
	    if instr(maxValue,'.') > 0 then
		  parameter_integer := regexp_replace(maxValue, '(.*)\..*', '\1');
	    else
	      parameter_integer := maxValue;
	    end if;
	  end if;

	  -- 参数中的整数表示具体数字, 所以需要用位数来比较
	  if length(parameter_integer) > to_number(limit_number) then
	    dbms_output.put_line('设定值' || maxValue || '超出列限制: ' || regexp_replace(column_type, '(.*)\[.*\]', '\1'));
	    CheckNumber_status := false;
		return ;
	  end if;
    end if;
	-- 没括号的直接拼接,有括号的在上面检查完合法性后在这里拼接
	tableInfo(upper(columnName)) :=
	  regexp_replace(column_type, '(.*)\[.*\]', '\1') || '['
	  || 'columnName=' || columnName || ','
	  || 'minValue=' || NVL(minValue,'null') || ','
	  || 'maxValue=' || NVL(maxValue,'null') || ','
	  || 'dataModel=' || NVL(dataModel,'null') || ','
	  || 'setValue=' || NVL(setValue,'null') || ','
	  || 'mu=' || NVL(mu,'null') || ','
	  || 'sigma=' || NVL(sigma,'null') || ','
	  || 'alpha=' || NVL(alpha,'1') || ','
	  || 'beta=' || NVL(beta,'1') || ','
	  || 'kappa=' || kappa || ','
	  || ']';
  end CheckNumber;





  /*
	用于校验字符类型指定的strLen是否超出列长限制
	1. 超长的会以列限制为准,不会报错
  */
  procedure CheckChar(columnName varchar, column_type varchar, strLen varchar, 
	strType varchar, dataModel varchar, setValue varchar, mu varchar, sigma varchar,
	alpha varchar, beta varchar, kappa varchar, minValue varchar, maxValue varchar) 
  is
	current_number varchar2(100) := regexp_replace(column_type, '.*\((.*)\).*', '\1');
	inner_str_len varchar2(100) := strLen;
  begin
    if to_number(strLen) > to_number(current_number) then
	  dbms_output.put_line('设定值超出列限制, 以列限制为准......');
	  dbms_output.put_line('设定值: ' || strLen || '    限制值: ' || regexp_replace(column_type, '(.*)\[.*\]', '\1'));
	  inner_str_len := current_number;
	end if;

	tableInfo(upper(columnName)) :=  
	  regexp_replace(column_type, '(.*)\[.*\].*', '\1') || '['
	  || 'columnName=' || columnName || ','
	  || 'minValue=' || NVL(minValue,'null') || ','
	  || 'maxValue=' || NVL(maxValue,'null') || ','
	  || 'strLen=' || NVL(inner_str_len,'null') || ','
	  || 'strType=' || NVL(strType,'null') || ','
	  || 'dataModel=' || NVL(dataModel,'null') || ','
	  || 'setValue=' || NVL(setValue,'null') || ','
	  || 'mu=' || NVL(mu,'null') || ','
	  || 'sigma=' || NVL(sigma,'null') || ','
	  || 'alpha=' || NVL(alpha,'1') || ','
	  || 'beta=' || NVL(beta,'1') || ','
	  || 'kappa=' || kappa || ','
	  || ']';
  end CheckChar;



  /*
    用于把自定义的属性记录到tableInfo中;
    1. 具体参数含义参考CustomSet()上的注释
	2. 
  */
  procedure UpdateTableInfo(columnName varchar, 
    minValue varchar, maxValue varchar, strLen varchar, 
	strType varchar, dataModel varchar, setValue varchar,
	mu varchar, sigma varchar, minDate varchar, maxDate varchar,
	alpha varchar, beta varchar, kappa varchar)
  is
    column_type varchar2(1000) := tableInfo(upper(columnName));
  begin
    -- 判断当前设定的类型是什么类型在对应处理(数字,字符)
    if instr(column_type,upper('number')) > 0 or 
	  instr(column_type,upper('float')) > 0 or 
	  instr(column_type,upper('BINARY')) > 0 then

      -- select trim(translate('123a4567','0123456789' ,' ')) from dual;
      if minValue is not null and maxValue is not null and 
	    to_number(minValue) > to_number(maxValue) then
        dbms_output.put_line('minValue应小于maxValue的值......');
		UpdateTableInfo_status := false;
        return ;
      end if;
      -- 处理数字类型并更新tableInfo
	  begin
	   PKG_GenerateData.CheckNumber(columnName, column_type, minValue, maxValue, dataModel, setValue, mu, sigma, alpha, beta, kappa);
	  exception
	    when others then
	    dbms_output.put_line('CheckNumber error......');
		dbms_output.put_line(SQLCODE);
        dbms_output.put_line(SQLERRM);
		UpdateTableInfo_status := false;
		return ;
	  end;
	  
	  if CheckNumber_status = false then
	    CheckNumber_status := true;
		UpdateTableInfo_status := false;
        return ;
	  end if;
	  
    elsif instr(column_type,upper('char')) > 0 or instr(column_type,upper('LOB')) > 0 then
      -- 处理字符类型并更新tableInfo
	  begin
	    PKG_GenerateData.CheckChar(columnName, column_type, strLen, strType, dataModel, setValue, mu, sigma, alpha, beta, kappa, minValue, maxValue);
	  exception
	    when others then
	    dbms_output.put_line('CheckChar error......');
		dbms_output.put_line(SQLCODE);
        dbms_output.put_line(SQLERRM);
		UpdateTableInfo_status := false;
        return ;
	  end;
      null;
	elsif instr(column_type,'DATE') > 0 or instr(column_type,'TIMESTAMP') > 0 then
	  -- 日期类型()中数据没意义, 忽略, 每次更新时直接替换[]就行
	  tableInfo(upper(columnName)) :=  
	    regexp_replace(column_type, '(.*)\[.*\].*', '\1') || '['
	    || 'setValue=' || NVL(setValue,'null') || ','
	    || 'minDate=' || NVL(minDate,'null') || ','
	    || 'maxDate=' || NVL(maxDate,'null') || ','
		|| ']';
    else
      null;
    end if;
  end UpdateTableInfo;




  /*
    CustomSet过程用于保存自定义的生成规则
	1. tableName,columnName 分别表示表名及列名
	2. minValue,maxValue    数字类型的取值范围(仅对数字类型有效)
	3. dataModel  对日期类型无效,日期类型可指定范围也可指定具体值
	4. strLen     用于设定生成的字符位数(仅对字符类型有效)
	5. strType    同DBMS_RANDOM.STRING的参数
	6. setValue   直接设定值(对所有类型有效)
	7. mu,sigma   正太分布中的均值和方差
	8. minDate,maxDate 日期范围yyyymmdd或yyyy-mm-dd格式
	9. sigma      标准差
  */
  procedure CustomSet(
    tableName varchar, columnName varchar, 
    minValue varchar default null, maxValue varchar default null,
    strLen varchar default STR_LEN, strType varchar default STR_TYPE, 
	dataModel varchar default 'random', setValue varchar default null,
	mu varchar default 0, sigma varchar default 1,
	minDate varchar2 default MIN_DATE, maxDate varchar2 default MAX_DATE,
	alpha varchar default 1, beta varchar default 1,
	kappa varchar default 1) 
  is
  begin
    dbms_output.put_line('begin CustomSet......');
	
	if currentTable is null or currentTable <> tableName then
	  if currentTable is null then
	    dbms_output.put_line('开始构建'|| tableName || '表的映射关系......');
	  else
	    dbms_output.put_line('多次设置的表不一致,准备构建' || tableName || '表的映射关系......');
	  end if;
      begin
        PKG_GenerateData.InitTableMap(tableName);
      exception
        when others then
        dbms_output.put_line('InitTableMap error......');
		currentTable := null;
		dbms_output.put_line(SQLCODE);
        dbms_output.put_line(SQLERRM);
		return ;
      end;
	  currentTable := tableName;
    end if;
	-- 这里对映射关系数据进行更新
	begin
	PKG_GenerateData.UpdateTableInfo(columnName, minValue, maxValue, 
	  strLen, strType, dataModel, setValue, mu, sigma, minDate, maxDate,
	  alpha, beta, kappa);
	exception
	  when others then
	    dbms_output.put_line('UpdateTableInfo error......');
	    dbms_output.put_line('table: ' || currentTable || ' column: ' || columnName);
		dbms_output.put_line(SQLCODE);
        dbms_output.put_line(SQLERRM);
		return ;
	end;
	-- 设定值冲突时改为false,默认为true
    if UpdateTableInfo_status = false then
	  UpdateTableInfo_status := true;
	  return;
	end if;
  end CustomSet;
  
  
  
  /*
    用来判定参数是否是数字
  */
  function is_number(p varchar2) return boolean is
  var number;
  begin
    var := to_number(p);
    return true;
  exception
    when others then
      return false;
  end;



  procedure CustomSetPk(tableName varchar, columnName varchar) 
  is
  begin
    PKG_GenerateData.CustomSet(tableName,columnName,dataModel=>'pk');
  exception
    when others then
      dbms_output.put_line('CustomSet error......');
  	  dbms_output.put_line(SQLCODE);
      dbms_output.put_line(SQLERRM);
  end CustomSetPk;
  
  
  procedure CustomSetRange(tableName varchar, columnName varchar, 
    minValue varchar default null, maxValue varchar default null) 
  is
  begin
    if not (is_number(minValue) and is_number(maxValue)) then
      dbms_output.put_line('参数值非法');
	  return;
    end if;
	
    PKG_GenerateData.CustomSet(tableName, columnName, minValue=>minValue, maxValue=>maxValue, dataModel=>'random'); 
  exception
    when others then
      dbms_output.put_line('CustomSetRange error......');
  	  dbms_output.put_line(SQLCODE);
      dbms_output.put_line(SQLERRM);
  end CustomSetRange;
  


  procedure CustomSetNormal(tableName varchar, columnName varchar,
    mu varchar default 0, sigma varchar default 1)
  is
  begin
    if not (is_number(mu) and is_number(sigma)) then
      dbms_output.put_line('参数值非法');
	  return;
    end if;
	
    PKG_GenerateData.CustomSet(tableName, columnName, mu=>mu, sigma=>sigma, dataModel=>'normal');
  exception
    when others then
      dbms_output.put_line('CustomSetNormal error......');
  	  dbms_output.put_line(SQLCODE);
      dbms_output.put_line(SQLERRM);
  end CustomSetNormal;
  


  procedure CustomSetGamma(tableName varchar, columnName varchar,
    alpha varchar default 1, beta varchar default 1)
  is
  begin
    if not (is_number(alpha) and is_number(beta)) then
      dbms_output.put_line('参数值非法');
	  return;
    end if;
	
	PKG_GenerateData.CustomSet(tableName, columnName, alpha=>alpha, beta=>beta, dataModel=>'gamma');
  exception
    when others then
      dbms_output.put_line('CustomSetGamma error......');
  	  dbms_output.put_line(SQLCODE);
      dbms_output.put_line(SQLERRM);
  end ;
  
  
  
  procedure CustomSetGauss(tableName varchar, columnName varchar,
    mu varchar default 0, sigma varchar default 1)
  is
  begin
    if not (is_number(mu) and is_number(sigma)) then
      dbms_output.put_line('参数值非法');
	  return;
    end if;
	
	PKG_GenerateData.CustomSet(tableName, columnName, mu=>mu, sigma=>sigma, dataModel=>'gauss');
  exception
    when others then
      dbms_output.put_line('CustomSetGauss error......');
  	  dbms_output.put_line(SQLCODE);
      dbms_output.put_line(SQLERRM);
  end ;
 
 
 
  procedure CustomSetKappa(tableName varchar, columnName varchar,
    mu varchar default 0, kappa varchar default 1)
  is
  begin
    if not (is_number(mu) and is_number(kappa)) then
      dbms_output.put_line('参数值非法');
	  return;
    end if;
	
	PKG_GenerateData.CustomSet(tableName, columnName, mu=>mu, kappa=>kappa, dataModel=>'kappa');
  exception
    when others then
      dbms_output.put_line('CustomSetKappa error......');
  	  dbms_output.put_line(SQLCODE);
      dbms_output.put_line(SQLERRM);
  end ;
  


  procedure CustomSetValue(tableName varchar, columnName varchar,
    setValue varchar)
  is
  begin
    PKG_GenerateData.CustomSet(tableName, columnName, setValue=>setValue);
  exception
    when others then
      dbms_output.put_line('CustomSetValue error......');
  	  dbms_output.put_line(SQLCODE);
      dbms_output.put_line(SQLERRM);
  end ;
  


  procedure CustomSetDateRange(tableName varchar, columnName varchar,
    minDate varchar2 default MIN_DATE, maxDate varchar2 default MAX_DATE)
  is
  begin
    -- 日期这里未校验参数是否符合
    PKG_GenerateData.CustomSet(tableName, columnName, minDate=>minDate, maxDate=>maxDate, dataModel=>'random');
  exception
    when others then
      dbms_output.put_line('CustomSetDateRange error......');
  	  dbms_output.put_line(SQLCODE);
      dbms_output.put_line(SQLERRM);
  end ;
  


  procedure CustomSetCharModel(tableName varchar, columnName varchar,
    strLen varchar, strType varchar default STR_TYPE)
  is
  begin
    if not (is_number(strLen)) then
      dbms_output.put_line('strLen值非法');
	  return;
    end if;
	
	if strType not in('u','l','a','x','p') then
	  dbms_output.put_line('strType值非法, 取值同dbms_random.string');
	  return;
	end if;
	
	PKG_GenerateData.CustomSet(tableName, columnName, strLen=>strLen, strType=>strType, dataModel=>'random');
  exception
    when others then
      dbms_output.put_line('CustomSetCharModel error......');
  	  dbms_output.put_line(SQLCODE);
      dbms_output.put_line(SQLERRM);
  end ;
  
	
	

  -- 用于核对最终的tableInfo映射表数据是否正常
  procedure ShowInfo is
    inner_key varchar2(200);
  begin
    dbms_output.put_line('{');
    inner_key := tableInfo.FIRST;
    while inner_key is not null loop
      dbms_output.put_line(inner_key || ': ' || tableInfo(inner_key) || ',');
      inner_key := tableInfo.next(inner_key);
    end loop;
    dbms_output.put_line('}');
  end ShowInfo;





  /* 
    处理串的格式: CHAR(2)[strLen=2;strType=l;dataModel=random;setValue=null;]
	提供等号左边的名称,获取等号右边的结果
	1. tableInfo_val  tableInfo表存储的value,如char(6)[...;]
	2. name           要获取那个参数的值,如strLen,setValue...
  */
  function GetValue(tableInfo_val varchar2, name varchar2) return varchar2 is
    name_position number;
    equal_sign_position number;
    delimiter_position number;
    val varchar2(2000);
  begin
    name_position := instr(tableInfo_val, name||'=');
    equal_sign_position := instr(tableInfo_val, '=', name_position);
    delimiter_position := instr(tableInfo_val, ',', name_position);
    -- semicolon_position := instr(tableInfo_val, ';', name_position);
    -- 从tableInfo_val中截取到name对应的值并返回
    val := substr(tableInfo_val, equal_sign_position+1, delimiter_position-equal_sign_position-1);
    return val;
  end GetValue;
    
  
  
  
  /*
    用于处理字符类型的限制位数,如:varchar(10)[strLen=2]
	1. str_len       人为设定的数据长度,如:strLen=2
	2. limit_number  列的限制位数,如:varchar(10)
  */
  function GetValidLen(tableInfo_val varchar2, limitNumber varchar2 default null) 
  return varchar2 
  is
    valid_len varchar2(2000);
    str_len varchar2(2000);
    limit_number varchar2(2000) := limitNumber;
  begin
    if limit_number is null then
	  limit_number := regexp_replace(tableInfo_val, '.*\((.*)\).*', '\1');
	end if;

	str_len := PKG_GenerateData.GetValue(tableInfo_val, 'strLen');
	valid_len := limit_number;
	if str_len <> 'null' and to_number(str_len) < to_number(limit_number) then
	  valid_len := str_len;
	end if;
	return valid_len;
  end GetValidLen;



  /* 
    ParserChar()中调用, 用于生成[]中dataModel=random模式的数据生成
	1. tableInfo_val  tableInfo表内存储的value,如char(6)[...;...]
	2. inner_str_type 设定的随机模式,同 dbms_random.string()的参数模式
	3. valid_len      有效的字符数,如varchar(10)[strLen=2;], 则取2
	4. result_value   最终生成的字符串
	5. 含往字符类型列中写入数字的场景
  */
  function StrRandom(tableInfo_val varchar2) return varchar2 is
    result_value varchar2(2000);
    valid_len varchar2(2000);
    inner_str_type varchar2(20);
	min_value varchar2(2000);
	max_value varchar2(2000);
  begin
    -- 处理往字符类型列中写入数字的场景
	min_value := PKG_GenerateData.GetValue(tableInfo_val, 'minValue');
	max_value := PKG_GenerateData.GetValue(tableInfo_val, 'maxValue');
	valid_len := PKG_GenerateData.GetValidLen(tableInfo_val);
	
	if min_value <> 'null' or max_value <> 'null' then
	  -- 设定有范围, 生成范围内数字
	  if min_value <> 'null' and max_value = 'null' then
	    max_value := power(10,to_number(NUMBER_RANGE))-1;
	  elsif min_value = 'null' and max_value <> 'null' then
	    min_value := '-' || max_value;
	    -- 考虑最大值为负数的场景
	    if instr(max_value,'-') > 0 then
	      min_value := '-' || to_number(max_value)*2;
	    end if;
	  end if;
	  
	  -- 数据精度以全局设置为准
	  select 
	  round(dbms_random.value(min_value, max_value),NUMBER_PRECISION) 
	  into result_value from dual;
	  result_value := PKG_GenerateData.FormatResult(result_value);
	  result_value := substr(result_value,1,valid_len);
	else
	  -- 无指定范围,按照strtype和限制长度生成字符即可
	  inner_str_type := PKG_GenerateData.GetValue(tableInfo_val, 'strType');
	  result_value := dbms_random.string(inner_str_type, valid_len);
	end if;
	return result_value;
  end StrRandom;
  
  
  

  /* 
    用于[]中dataModel=random模式的数据生成
	注意: 当设定的max为负数时,生成数范围: -2*max <-> -max
	1. min_value        用于生成随机数的下限
	2. max_value        用于生成随机数的上限
	3. original_value   ()中内容,如number(m,n)中的'm,n'
	4. valid_value      (m,n)或(n)场景的有效整数位
	5. result_value     最终生成的随机数
	6. original_integer (m,n)中的m值
	7. original_decimal (m,n)中的n值
	到这里的有可能是number[....]这种场景
  */
  function NumberRandom(tableInfo_val varchar2) return varchar2 is
    min_value varchar2(200);
    max_value varchar2(200);
    original_value varchar2(200);
    valid_value varchar2(200);
	result_value varchar2(2000);
	original_integer varchar2(200);
    original_decimal varchar2(200);
  begin
    min_value := PKG_GenerateData.GetValue(tableInfo_val, 'minValue');
    max_value := PKG_GenerateData.GetValue(tableInfo_val, 'maxValue');
	-- 考虑number[....]这种没有()场景
	if instr(original_value, '(') > 0 then
	  original_value := regexp_replace(tableInfo_val, '.*\((.*)\).*', '\1');
	end if;
	
	-- if逻辑中针对的是float及binary_*类型, 有效位是全局设定的NUMBER_RANGE值.
	if instr(tableInfo_val,'FLOAT') > 0 or 
	  instr(tableInfo_val,'BINARY') > 0 then
	  valid_value := NUMBER_RANGE;
	-- 以下几个逻辑分支均在获取各number()场景中的有效位数
	elsif original_value is null or original_value = '0' 
	  or original_value = '38,0' then
	  valid_value := NUMBER_RANGE;
	elsif instr(original_value, ',') = 0 then
	  valid_value := original_value;
	else
	  original_integer := regexp_replace(original_value, '(.*),.*', '\1');
	  original_decimal := regexp_replace(original_value, '.*,(.*)', '\1');
	  valid_value := to_number(original_integer) - to_number(original_decimal);
	end if;
	
	-- 弥补自定义范围值有缺失场景中的生成数范围
	if min_value <> 'null' and max_value = 'null' then
	  max_value := power(10,to_number(valid_value))-1;
	elsif min_value = 'null' and max_value <> 'null' then
	  min_value := '-' || max_value;
	  -- 考虑最大值为负数的场景
	  if instr(max_value,'-') > 0 then
	    min_value := '-' || to_number(max_value)*2;
	  end if;
	elsif min_value = 'null' and max_value = 'null' then
	  min_value := '-' || power(10,to_number(valid_value))+1;
	  max_value := power(10,to_number(valid_value))-1;
	end if;
	
	-- 使用上面步骤中得到的范围及小数位限制来生成数据
	if original_decimal is null then
	  original_decimal := NUMBER_PRECISION;
	end if;
	
	select 
	round(dbms_random.value(min_value, max_value),original_decimal) 
	into result_value from dual;
	
	result_value := PKG_GenerateData.FormatResult(result_value);
	return result_value;
  end NumberRandom;
  
  
  
  
  /*
    ParserNumber()中调用, 用于生成[]中dataModel=normal模式的数据生成
  */
  function NumberNormal(tableInfo_val varchar2) return varchar2 is
    mu varchar2(200);
    sigma varchar2(200);
    result_value varchar2(200);
	u number := dbms_random.value();
    v number := dbms_random.value();
  begin
	mu := PKG_GenerateData.GetValue(tableInfo_val, 'mu');
	sigma := PKG_GenerateData.GetValue(tableInfo_val, 'sigma');
	if mu = 'null' then
	  mu := 0;
	end if;
	if sigma = 'null' then
	  sigma := 1;
	end if;
	result_value := mu + sigma * sqrt(-2 * log(10,u)) * cos(2 * 3.1415926535 * v);
	
	result_value := PKG_GenerateData.FormatResult(result_value);
	return round(result_value, PKG_GenerateData.NUMBER_PRECISION);
  end NumberNormal;



  /*
    用于[]中dataModel=pk的数据生成, 表示主键
	  CURRENT_PK varchar(50) := 0;
      SELECT_PK boolean := true;
	  	  || 'columnName=' || columnName || ';'
  */
  function NumberPk(tableInfo_val varchar2) return varchar2 is
    c_name varchar2(200);
	row_count varchar2(200);
    result_value varchar2(200);
    get_current_pk_sql varchar2(2000);
    get_row_count_sql varchar2(2000);
  begin
	-- 获取当前表中数据的最大pk值,累加一写入
	-- 考虑代码的执行效率,可添加标识,用于一次获取后不再重复读取数据库
	if SELECT_PK then
	  get_row_count_sql := 'select count(*) from ' || PKG_GenerateData.currentTable;
	  execute immediate get_row_count_sql into row_count;
	  if row_count <> 0 then
	    c_name := PKG_GenerateData.GetValue(tableInfo_val, 'columnName');
	    get_current_pk_sql := 'select ' || c_name || ' from (select '|| c_name 
	      || ' from ' || PKG_GenerateData.currentTable|| ' order by ' 
          || c_name || ' desc) where rownum = 1';
        execute immediate get_current_pk_sql into PKG_GenerateData.CURRENT_PK;
	  end if;
	  PKG_GenerateData.SELECT_PK := false;
	end if;
	PKG_GenerateData.CURRENT_PK := PKG_GenerateData.CURRENT_PK + 1;
	result_value := PKG_GenerateData.CURRENT_PK;
	return result_value;
  end NumberPk;
  
  
  
  /*
    Gamma分布, 改写自python的 random.gammavariate(alpha, beta)函数
  */
  function NumberGamma(tableInfo_val varchar2) return varchar2 is
    alpha varchar2(100);
    beta varchar2(100);
    ainv varchar2(100);
    bbb varchar2(100);
    ccc varchar2(100);
    u1 varchar2(100);
    u2 varchar2(100);
    u varchar2(100);
    v varchar2(100);
    x varchar2(100);
    z varchar2(100);
    r varchar2(100);
    result_value varchar2(100);
    SG_MAGICCONST varchar2(100) := 1.0 + log(10, 4.5);
	e varchar2(100) := '2.718281828459045';
	b varchar2(100);
	p varchar2(100);
  begin
    alpha := PKG_GenerateData.GetValue(tableInfo_val, 'alpha');
	beta := PKG_GenerateData.GetValue(tableInfo_val, 'beta');
	if alpha <= 0 or beta <= 0 then
	  dbms_output.put_line('alpha或beta值不应小于0,统一返回-1');
	  result_value := '-1';
	else
	  if to_number(alpha) > 1 then
	    ainv := sqrt(2.0 * alpha - 1.0);
        bbb := alpha - log(10,4);
        ccc := alpha + ainv;
		
		loop
          u1 := dbms_random.value();
		  if u1 <= 1e-7 or u1 >= .9999999 then
            continue;
		  end if;
          u2 := 1.0 - dbms_random.value();
          v := log(10, u1 / (1.0 - u1)) / ainv;
          x := alpha * exp(v);
          z := u1 * u1 * u2;
          r := bbb + ccc * v - x;
          if r + SG_MAGICCONST - 4.5 * z >= 0.0 or r >= log(10, z) then
		    result_value := x * beta;
		    exit;
		  end if;
		end loop;
	  elsif to_number(alpha) = 1 then
	    u := dbms_random.value();
        while u <= 1e-7 loop
          u := dbms_random.value();
	    end loop;
		result_value := '-' || log(10, u) * beta;
	  else
	    loop
		  u := dbms_random.value();
          b := (e + alpha) / e;
          p := b * u;
          if p <= 1.0 then
              x := p ** (1.0 / alpha);
          else
              x := '-' || log(10, (b - p) / alpha);
		  end if;
          u1 := dbms_random.value();
          if p > 1.0 then
            if u1 <= x ** (alpha - 1.0) then
              exit;
			end if;
          elsif u1 <= exp(-x) then
            exit;
		  end if;
		end loop;
		result_value := x * beta;
	  end if;
	end if;
	
	result_value := PKG_GenerateData.FormatResult(result_value);
    return result_value;
  end NumberGamma;
  
  
  
  /*
    Gamma分布, 改写自python的 random.gauss(mu, sigma)函数
  */
  function NumberGauss(tableInfo_val varchar2) return varchar2 is
    TWOPI varchar2(100) := 2.0 * 3.141592653589793;
    x2pi varchar2(100);
    g2rad varchar2(100);
    z varchar2(100);
    mu varchar2(100);
    sigma varchar2(100);
    result_value varchar2(100);
  begin
    mu := PKG_GenerateData.GetValue(tableInfo_val, 'mu');
    sigma := PKG_GenerateData.GetValue(tableInfo_val, 'sigma');
    z := PKG_GenerateData.GAUSS_NEXT;
    PKG_GenerateData.GAUSS_NEXT := 'null';
	if z = 'null' then
      x2pi := dbms_random.value() * TWOPI;
      g2rad := sqrt(-2.0 * log(10, 1.0 - dbms_random.value()));
      z := cos(x2pi) * g2rad;
      PKG_GenerateData.GAUSS_NEXT := sin(x2pi) * g2rad;
	end if;
	result_value := mu + z*sigma;
	result_value := PKG_GenerateData.FormatResult(result_value);
	return result_value;
  end NumberGauss;
  
  
  
  /*
    卡帕分布, 改写自python的 random.vonmisesvariate(mu, kappa)函数
  */
  function NumberKappa(tableInfo_val varchar2) return varchar2 is
    TWOPI varchar2(100) := 2.0 * 3.141592653589793;
    pi varchar2(100):= 3.141592653589793;
    mu varchar2(100);
    kappa varchar2(100);
    result_value varchar2(100);
	s varchar2(100);
    r varchar2(100);
    z varchar2(100);
    d varchar2(100);
    u1 varchar2(100);
    u2 varchar2(100);
    u3 varchar2(100);
    q varchar2(100);
    f varchar2(100);
  begin
    mu := PKG_GenerateData.GetValue(tableInfo_val, 'mu');
    kappa := PKG_GenerateData.GetValue(tableInfo_val, 'kappa');
	if kappa <= 1e-6 then
	  result_value := TWOPI * dbms_random.value();
	else
	  s := 0.5 / kappa;
      r := s + sqrt(1.0 + s * s);
	  loop
	    u1 := dbms_random.value();
        z := cos(pi * u1);
	    
        d := z / (r + z);
        u2 := dbms_random.value();
        if u2 < 1.0 - d * d or u2 <= (1.0 - d) * exp(d) then
            exit;
	    end if;
	  end loop;
	  
	  q := 1.0 / r;
      f := (q + z) / (1.0 + q * z);
      u3 := dbms_random.value();
	  if u3 > 0.5 then
          result_value := mod((mu + acos(f)), TWOPI);
      else
          result_value := mod((mu - acos(f)), TWOPI);
	  end if;
	end if;
	result_value := PKG_GenerateData.FormatResult(result_value);
	return result_value;
  end NumberKappa;
  
  
  
  /*
    数字格式化
	处理 --123  -.123 .123等场景
  */
  function FormatResult(resultValue varchar2) return varchar2 is
    result_value varchar2(2000) := resultValue;
	var1 varchar2(10);
	var2 varchar2(10);
  begin
    if substr(result_value, 1, 1) = '-' and substr(result_value, 2, 1) = '-' then
	  result_value := substr(result_value, 3, length(result_value));
	end if;
	
	if substr(result_value, 1, 1) = '-' and substr(result_value, 2, 1) = '.' then
	  result_value := '-0' || substr(result_value, 2, length(result_value));
	end if;
	
	if substr(result_value, 1, 1) = '.' then
	  result_value := '0' || result_value;
	end if;
	
	return result_value;
  end;
  
  
  
  /*
    按照指定规则生成字符类型数据
	1. tableInfo_val    tableInfo表内存储的value,如char(6)[...;...]
	2. limit_number     数据类型括号中的限定值,如number(10)
	3. result_value     最终生成的字符串/数据
	4. valid_len        有效的字符数,如varchar(10)[strLen=2;], 则取2
	5. function_name    generate_model_map 中获取到的函数名
	6. data_model       生成哪种模式的数据,如CHAR(2)[dataModel=random;]
    7. get_func_result	function_name对应函数的返回值,即最终生成的数据
	8. to_char目前不支持10转16进制,支持后可放开下面的两处代码
  */
  function ParserChar(tableInfo_val varchar2) return varchar2 is
    limit_number varchar2(2000);
    result_value varchar2(2000);
    valid_len varchar2(2000);
	function_name varchar2(2000);
	data_model varchar2(2000);
	get_func_result varchar2(2000);
  begin
    limit_number := regexp_replace(tableInfo_val, '.*\((.*)\).*', '\1');
    -- 两种形式, 一种是带中括号, 一种不带中括号
	if instr(tableInfo_val,'[') > 0 then
	  result_value := PKG_GenerateData.GetValue(tableInfo_val, 'setValue');
	  valid_len := PKG_GenerateData.GetValidLen(tableInfo_val, limit_number);
	  
	  if result_value <> 'null' then
	    -- 设定了指定值, 返回即可
		result_value := substr(result_value,0, valid_len);
	  else
	  	-- BLOB类型需要16进制数,需要差异化处理
	    -- ob目前不支持to_char转16进制, 以10进制数代替了
	    if instr(tableInfo_val,'BLOB') > 0 then
		  result_value := round(dbms_random.value(0, power(16,to_number(limit_number))-1));
		  --result_value := to_char(to_number(result_value),lpad('X',to_number(limit_number),'X'));
	    else
		  -- 无指定值, 获取[]中的参数来生成值
		  data_model := PKG_GenerateData.GetValue(tableInfo_val, 'dataModel');
		  data_model := upper('str_' || data_model);
		  function_name := generate_model_map(data_model);
		  -- 这里获取到的是 StrRandom 函数
		  get_func_result := 'select PKG_GenerateData.' || function_name || '(''' ||tableInfo_val || ''') from dual';
		  dbms_output.put_line('get_func_result的值: ' || get_func_result);
          execute immediate get_func_result into result_value;
		  result_value := substr(result_value, 0, valid_len);
		end if;
	  end if;
	else
	  -- BLOB类型需要16进制数,需要差异化处理
	  -- ob目前不支持to_char转16进制, 以10进制数代替了
	  if instr(tableInfo_val,'BLOB') > 0 then
		result_value := round(dbms_random.value(0, power(16,to_number(limit_number))-1));
		--result_value := to_char(to_number(result_value),lpad('X',to_number(limit_number),'X'));
	  else
	    result_value := dbms_random.string(PKG_GenerateData.STR_TYPE,limit_number);
	  end if;
	end if; 
	return result_value;
  end ParserChar;
  
  
  
  
  /*
    ParserNumber()中生成数字,提供整数位和精度,不提供时以全局设置为准
	1. numberRange      位数,非具体值.如2位数最大值为99
	2. numberPrecision  小数位精度
	3. ParserNumber
  */
  function GetRandomNumber(
  numberRange varchar2 default null, 
  numberPrecision varchar2 default null) 
  return varchar2 
  is
    result_value varchar2(2000);
    min_value varchar2(2000);
    max_value varchar2(2000);
    n_range varchar2(2000) := numberRange;
    n_precision varchar2(2000) := numberPrecision;
  begin
    if n_range is null then
	  n_range := NUMBER_RANGE;
	end if;
    if n_precision is null then
	  n_precision := NUMBER_PRECISION;
	end if;
	
	min_value := '-' || power(10,to_number(n_range))+1;
	max_value := '+' || power(10,to_number(n_range))-1;
	select 
	round(dbms_random.value(min_value, max_value),n_precision) 
	into result_value from dual;
	return result_value;
  end GetRandomNumber;
  
  
  
  
  /*
    按照指定规则生成数字类型数据
	1. tableInfo_val  tableInfo中存的value
	2. 含number,float,binary
  */
  function ParserNumber(tableInfo_val varchar2) return varchar2 is
    result_value varchar2(2000);
	original_integer varchar2(200);
    original_decimal varchar2(200);
	limit_number varchar2(200);
    valid_number varchar2(200);
    data_model varchar2(500);
    function_name varchar2(500);
    get_func_result varchar2(2000);
  begin
	if instr(tableInfo_val,'[') = 0 then
	  -- 无[]
	  if instr(tableInfo_val,'(') = 0 then
		result_value := PKG_GenerateData.GetRandomNumber();
	  else
	    if instr(tableInfo_val,'FLOAT') > 0 or 
		  instr(tableInfo_val,'BINARY') > 0 then
		  -- if逻辑中针对的是float,binary_*类型
		  result_value := PKG_GenerateData.GetRandomNumber();
		else
		  -- else逻辑针对的是number类型
		  limit_number := regexp_replace(tableInfo_val, '.*\((.*)\).*', '\1');
		  -- ()中数字是'0'或者'38,0'的, 按照默认位数生成数字即可
		  if limit_number = '0' or limit_number = '38,0' then
		    result_value := PKG_GenerateData.GetRandomNumber();
		  elsif instr(limit_number,',') = 0 then
		    -- (n)形式
		    result_value := 
		      PKG_GenerateData.GetRandomNumber(numberRange=>limit_number, numberPrecision=>0);
		  else
		    -- (m,n)形式
		    original_integer := regexp_replace(limit_number, '(.*),.*', '\1');
		    original_decimal := regexp_replace(limit_number, '.*,(.*)', '\1');
		    valid_number := to_number(original_integer) - to_number(original_decimal);
		    result_value := 
		      PKG_GenerateData.GetRandomNumber(numberRange=>valid_number, numberPrecision=>original_decimal);
		  end if;
		end if;
	  end if;
	else
	  -- 有[], 有指定值直接返回,无指定值根据规则生成
	  result_value := PKG_GenerateData.GetValue(tableInfo_val, 'setValue');
	  if result_value = 'null' then
		-- 无指定值, 获取[]中的参数来生成值
		data_model := PKG_GenerateData.GetValue(tableInfo_val, 'dataModel');
		data_model := upper('number_' || data_model);
		function_name := generate_model_map(data_model);
		-- 这里获取到的是 numberRandom 函数
		get_func_result := 'select PKG_GenerateData.' || function_name || '(''' ||tableInfo_val || ''') from dual';
        execute immediate get_func_result into result_value;
	  end if;
	end if;
	return result_value;
  end ParserNumber;
  
  
  
  /*
    按照指定规则生成日期类型数据
	1. tableInfo_val  tableInfo中存的value
	2. 对用户输入的格式没有做过多校验
  */
  function ParserDate(tableInfo_val varchar2) return varchar2 is
    result_value varchar2(2000);
    set_value varchar2(2000);
	min_date varchar2(1000);
	max_date varchar2(1000);
	tmp_date varchar2(1000);
  begin
    -- 无[]
	if instr(tableInfo_val,'[') = 0 then
	  select 
	  to_char(to_date(PKG_GenerateData.SYS_VALUE,'yyyy-MM-dd HH24:mi:ss'),'yyyy-MM-dd HH24:mi:ss') 
	  into result_value
	  from dual;
	else
	  -- 有[]
	  set_value := PKG_GenerateData.GetValue(tableInfo_val, 'setValue');
	  if set_value <> 'null' then
	    if upper(set_value) = 'SYSDATE' then
	      select 
	      to_char(to_date(sysdate,'yyyy-MM-dd HH24:mi:ss'),'yyyy-MM-dd HH24:mi:ss') 
	      into result_value
	      from dual;
		else
		  -- 各种不定什么格式的日期字符串
		  select 
		  to_char(to_date(set_value,'yyyy-MM-dd HH24:mi:ss'),'yyyy-MM-dd HH24:mi:ss') 
		  into result_value
		  from dual;
		end if;
	  else
		-- 无指定值, 日期类型设定为在年范围内随机生成.
		min_date := PKG_GenerateData.GetValue(tableInfo_val, 'minDate');
		max_date := PKG_GenerateData.GetValue(tableInfo_val, 'maxDate');
		if instr(min_date,'-') > 0 or instr(max_date,'-') > 0 then
		  if length(min_date) <> 10 then
		    dbms_output.put_line('日期类型有误, 已替换为MIN_DATE. 格式: xxxx-xx-xx');
			min_date := MIN_DATE;
		  end if;
		  if length(max_date) <> 10 then
		    dbms_output.put_line('日期类型有误, 已替换为MAX_DATE. 格式: xxxx-xx-xx');
			max_date := MAX_DATE;
		  end if;
		else
		  if length(min_date) <> 8 then
		    dbms_output.put_line('日期类型有误, 已替换为MIN_DATE. 格式: yyyymmdd');
			min_date := MIN_DATE;
		  end if;
		  if length(max_date) <> 8 then
		    dbms_output.put_line('日期类型有误, 已替换为MAX_DATE. 格式: yyyymmdd');
			max_date := MAX_DATE;
		  end if;
		end if;
		
		if min_date > max_date then
		  tmp_date := max_date;
		  max_date := min_date;
		  min_date := tmp_date;
		  dbms_output.put_line('min_date>max_date,互相交换值');
		end if;
		
	    SELECT to_date(TRUNC(DBMS_RANDOM.VALUE(
        to_number(to_char(to_date(min_date,'yyyy-MM-dd'),'J')),
        to_number(to_char(to_date(max_date,'yyyy-MM-dd')+1,'J')))),'J')+
        DBMS_RANDOM.VALUE(1,3600)/3600
        into result_value
        FROM dual;
	  end if;
	end if;
	
	if instr(tableInfo_val,'WITH TIME ZONE') > 0 then
	  select 
	  to_char(TO_TIMESTAMP_TZ(result_value,'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM')) 
	  into result_value from dual;
	end if;
	return result_value;
  end ParserDate;
  

  
  
  /*
    获取列的类型
	1. tableInfoVal     tableInfo中存的value
	2. inner_str_type   数据类型,如number,varchar...
	3. int, int[], int(n)[]
  */
  function GetType(tableInfoVal varchar2) return varchar2 is
    inner_str_type varchar2(200);
    tableInfo_val varchar2(200);
  begin
    tableInfo_val := tableInfoVal;
      -- 处理int
	  if instr(tableInfo_val, '(') = 0 and instr(tableInfo_val, '[') = 0 then
	    inner_str_type := tableInfo_val;
	  -- 处理int[]
	  elsif instr(tableInfo_val, '(') = 0 and instr(tableInfo_val, '[') > 0 then
	    inner_str_type := regexp_replace (tableInfo_val, '(.*)\[.*\].*', '\1');
	  else
	    tableInfo_val := regexp_replace (tableInfo_val, '(.*)\(.*\).*', '\1');
		if instr(tableInfo_val, '(') > 0 then
		  tableInfo_val := regexp_replace (tableInfo_val, '(.*)\(.*\).*', '\1');
		end if;
	    inner_str_type := tableInfo_val;
	  end if;
	  return inner_str_type;
  end GetType;
  



  /*
    用于写入单条数据
    1. tableName    表名
  */
  procedure InsertOne(tableName varchar2) is
  run_sql varchar(5000);
  get_func_result_sql varchar(2000);
  func_result varchar(2000);
  column_name_list varchar(5000);
  column_value_list varchar(5000);
  tableInfo_key varchar2(1000);
  tableInfo_val varchar2(1000);
  inner_str_type varchar2(200);
  function_name varchar2(1000);
  begin
      InsertOne_status := true;
      tableInfo_key := tableInfo.FIRST;
      while tableInfo_key is not null loop
	    tableInfo_val := tableInfo(tableInfo_key);
		-- 获取列类型,含带与不带括号场景
        inner_str_type := GetType(tableInfo_val);
	    begin
	      function_name := type_function_map(upper(inner_str_type));
	    exception
	      when others then
	      dbms_output.put_line(inner_str_type || ' 类型获取处理函数异常......');
		  dbms_output.put_line(SQLCODE);
          dbms_output.put_line(SQLERRM);
		  InsertOne_status := false;
	  	  return;
	    end;
		-- 执行对应函数, 获取生成的结果
	    get_func_result_sql := 'select PKG_GenerateData.' || function_name || '(''' || tableInfo_val || ''') from dual';
		begin
          execute immediate get_func_result_sql into func_result;
		exception
	      when others then
	      dbms_output.put_line('error sql -->' || get_func_result_sql);
		  dbms_output.put_line(SQLCODE);
          dbms_output.put_line(SQLERRM);
		  InsertOne_status := false;
	  	  return;
	    end;
        -- 同步拼接列名和对应的值
		column_name_list := column_name_list || to_char(tableInfo_key) || ',';
        column_value_list := column_value_list || '''' || to_char(func_result) || ''',';
        tableInfo_key := tableInfo.next(tableInfo_key);
      end loop;
	  -- 拼接完整的insert语句并执行
	  run_sql := 'insert into ' || tableName || '(' || trim(',' from column_name_list) || 
      ') values(' || trim(',' from column_value_list) || ')';
	  begin
        execute immediate run_sql;
	  exception
	    when others then
	    dbms_output.put_line('error sql -->' || run_sql);
		  dbms_output.put_line(SQLCODE);
        dbms_output.put_line(SQLERRM);
		  InsertOne_status := false;
	    return;
	  end;
  end InsertOne;




  /*
    生成数据的入口
	1. tableName    表名
	2. rowCount     指定生成多少条数据
  */
  procedure LoadDate(tableName varchar2, rowCount number default 1) is
  tableInfo_key varchar2(1000);
  tableInfo_val varchar2(1000);
  set_date_format varchar2(1000);
  begin
	if currentTable is null or currentTable <> tableName then
	  if currentTable is null then
	    dbms_output.put_line('开始构建'|| tableName || '表的映射关系......');
	  else
	    dbms_output.put_line('多次调用表不一致,准备构建' || tableName || '表的映射关系......');
	  end if;
      begin
        PKG_GenerateData.InitTableMap(tableName);
      exception
        when others then
        dbms_output.put_line('执行InitTableMap过程中异常......');
		currentTable := null;
		dbms_output.put_line(SQLCODE);
        dbms_output.put_line(SQLERRM);
		return ;
      end;
	  currentTable := tableName;
    end if;
	
	-- 重置parimary key的相关状态
	PKG_GenerateData.CURRENT_PK := 0;
    PKG_GenerateData.SELECT_PK := true;
	
	-- 同步可能存在问题的日期格式
	set_date_format := 'alter session set nls_date_format = 
	''YYYY-MM-DD HH24:MI:SS''';
	execute immediate set_date_format;
	set_date_format := 'alter session set NLS_TIMESTAMP_FORMAT = 
	''YYYY-MM-DD HH24:MI:SS.FF''';
	execute immediate set_date_format;
	set_date_format := 'alter session set NLS_TIMESTAMP_TZ_FORMAT = 
	''YYYY-MM-DD HH24:MI:SS.FF TZR TZD''';
	execute immediate set_date_format;
	
	-- 循环 rowCount 调用insertOne来写入多条数据
	for i in 1..rowCount loop
	  begin
	    PKG_GenerateData.InsertOne(tableName);
	  exception
	    when others then
		dbms_output.put_line('InsertOne error......');
		dbms_output.put_line(SQLCODE);
        dbms_output.put_line(SQLERRM);
		return;
	  end;
	  
	  -- InsertOne中获取不到相应数据类型的处理函数时为false
	  if InsertOne_status = false then
	    return;
	  end if;
	end loop;
	dbms_output.put_line(rowCount || '条数据加载完成......');
  end loadDate;


begin
  type_function_map('CHAR') := 'ParserChar';
  type_function_map('VARCHAR2') := 'ParserChar';
  type_function_map('NCHAR') := 'ParserChar';
  type_function_map('NVARCHAR2') := 'ParserChar';
  type_function_map('BLOB') := 'ParserChar';
  type_function_map('CLOB') := 'ParserChar';
  type_function_map('NUMBER') := 'ParserNumber';
  type_function_map('FLOAT') := 'ParserNumber';
  type_function_map('BINARY_FLOAT') := 'ParserNumber';
  type_function_map('BINARY_DOUBLE') := 'ParserNumber';
  type_function_map('DATE') := 'ParserDate';
  type_function_map('TIMESTAMP') := 'ParserDate';
  
  -- 映射上面索引表中value代表的函数中,处理dataModel的function
  generate_model_map('STR_PK') := 'NumberPk';
  generate_model_map('STR_RANDOM') := 'StrRandom';
  generate_model_map('STR_NORMAL') := 'NumberNormal';
  generate_model_map('STR_GAMMA') := 'NumberGamma';
  generate_model_map('STR_GAUSS') := 'NumberGauss';
  generate_model_map('STR_KAPPA') := 'NumberKappa';
  
  generate_model_map('NUMBER_PK') := 'NumberPk';
  generate_model_map('NUMBER_RANDOM') := 'NumberRandom';
  generate_model_map('NUMBER_NORMAL') := 'NumberNormal';
  generate_model_map('NUMBER_GAMMA') := 'NumberGamma';
  generate_model_map('NUMBER_GAUSS') := 'NumberGauss';
  generate_model_map('NUMBER_KAPPA') := 'NumberKappa';
end PKG_GenerateData;
//

delimiter ;//


