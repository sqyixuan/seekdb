delimiter //;


CREATE OR REPLACE TYPE "DAMS_ARRYTYPE" IS TABLE OF VARCHAR2(4000)
//

CREATE OR REPLACE TYPE "CLOB_TABLE" as table of CLOB
//

CREATE OR REPLACE TYPE "NUMBER_TABLE" as table of number
//

CREATE OR REPLACE TYPE "VARCHAR_TABLE" as table of varchar2(32767)
//

CREATE OR REPLACE TYPE "TP_ARRAY_TABLE" IS TABLE OF VARCHAR2(4000)
//

CREATE OR REPLACE TYPE "CTP_TYPE_ARRAYTYPE" IS TABLE OF VARCHAR2(100)
//

CREATE OR REPLACE TYPE "ARRYTYPE" is table of Varchar2(10)
//

CREATE OR REPLACE TYPE "DCT_ARRYTYPE" IS TABLE OF VARCHAR2(32767 )
//



CREATE OR REPLACE TYPE "ITEM_OBJECT" AS OBJECT
(
  NAME  VARCHAR2(500),
  VALUE VARCHAR2(2000)
);
//




CREATE OR REPLACE TYPE "ITEM_TABLE" as table of item_object;
//
CREATE OR REPLACE TYPE "MAP_OBJECT" AS OBJECT
(
  item_list item_table,
  MEMBER PROCEDURE ad_item(p_name IN VARCHAR2, p_value IN VARCHAR2),
  MEMBER FUNCTION map_value(p_name IN VARCHAR2) RETURN VARCHAR2,
  MEMBER FUNCTION map_position(p_name IN VARCHAR2) RETURN NUMBER,
  MEMBER PROCEDURE clear,
  MEMBER FUNCTION count RETURN NUMBER,
  MAP MEMBER FUNCTION get_all_item RETURN VARCHAR2
)
not final;
//
--echo ##############以下是function部分#################
--echo ############## FUNC_CTP_LG_CANBEUSEDBYUSER #################
CREATE OR REPLACE FUNCTION FUNC_CTP_LG_CANBEUSEDBYUSER(
userbranchlevel IN VARCHAR2,
userbranchid    IN VARCHAR2,
itemlevel       IN VARCHAR2,
itembranchid    IN VARCHAR2)
RETURN VARCHAR2 IS
    flag     VARCHAR2(10);
    intlevel INTEGER;
BEGIN
	flag := '0'; --0不可使用，1可以使用
	IF (itemlevel IS NULL OR itemlevel = 'null' OR TRIM(itemlevel) IS NULL) AND
		(itembranchid IS NULL OR itembranchid = 'null' OR
		TRIM(itembranchid) IS NULL) THEN
		RETURN flag;
	ELSE
		IF userbranchlevel IS NOT NULL AND userbranchlevel != 'null' AND
			TRIM(userbranchlevel) IS NOT NULL AND itemlevel IS NOT NULL AND
			itemlevel != 'null' AND TRIM(itemlevel) IS NOT NULL THEN
			intlevel := to_number(userbranchlevel);
			IF intlevel <= length(itemlevel) THEN
				IF substr(itemlevel,intlevel,1) = '1' THEN
					flag := 1;
					dbms_output.put_line('1......');
					RETURN flag;
				END IF;
			END IF;
		ELSE
			IF itembranchid IS NOT NULL AND itembranchid != 'null' AND
				TRIM(itembranchid) IS NOT NULL THEN
				IF userbranchid = itembranchid THEN
					flag := 1;
					dbms_output.put_line('2......');
					RETURN flag;
				END IF;
			END IF;
		END IF;
	END IF;
	RETURN flag;

EXCEPTION
	WHEN OTHERS THEN
		RETURN flag;
END FUNC_CTP_LG_CANBEUSEDBYUSER;
//


CREATE OR REPLACE FUNCTION FUNC_CTP_LG_GETBINARYSTR(branchLevel IN VARCHAR2,
srcStr IN VARCHAR2) Return VARCHAR2
IS
    retStr VARCHAR2(50);
    i_branchLevel INTEGER;
    binArr arryType;
    i INTEGER;
    len INTEGER;
    pos INTEGER;
    idx INTEGER;
    tmp INTEGER;
BEGIN
    retStr := '';
    i_branchLevel := to_number(branchLevel);
    binArr := arryType();
    for i in 1..i_branchLevel LOOP
        binArr.extend();
        binArr(i) := '0';
    end LOOP;
	
    len := LENGTH(srcStr);
    pos := 0;
    i := 1;
    WHILE i <= len
    LOOP
        pos := INSTR(srcStr,',',i);
        if pos = 0 then
           pos := len+1;
        end if;
        idx := to_number(TRIM(SUBSTR(srcStr,i,pos-i)));
        if idx >= 1 and idx <= i_branchLevel then
           binArr(idx) := '1';
		   dbms_output.put_line('1......');
        end if;
        i := pos+1;
    END LOOP;
	
    for i in 1..i_branchLevel LOOP
        retStr := retStr || binArr(i);
		dbms_output.put_line('2......');
    end LOOP;
    
    SELECT to_number(retStr) INTO tmp FROM dual;
    IF (tmp = 0) THEN
       retStr:='';
    END IF;
    
    return retStr;
    EXCEPTION
      WHEN OTHERS THEN
        return '';
END FUNC_CTP_LG_GETBINARYSTR;
//


CREATE OR REPLACE FUNCTION FUNC_CTP_LG_GETSPLITSTRING (
in_str IN VARCHAR2, in_split IN VARCHAR2) RETURN ARRYTYPE
AS
   v_count1    INTEGER;
   v_count2    INTEGER;
   v_strlist   arrytype;
   v_node      VARCHAR2 (2000);
BEGIN
   v_count2 := 0;
   v_strlist := arrytype ();

   IF (in_str IS NULL) OR (LENGTH (in_str) <= 0)
   THEN
      RETURN NULL;
   END IF;

   FOR v_i IN 1 .. LENGTH (in_str)
   LOOP
      v_count1 := INSTRB (in_str, in_split, 1, v_i);
      v_count2 := INSTRB (in_str, in_split, 1, v_i + 1);
      v_node := SUBSTRB (in_str, v_count1 + 1, v_count2 - v_count1 - 1);

      IF v_node IS NULL
      THEN
         v_node := '';
		 dbms_output.put_line('1......');
      END IF;

      IF (v_count2 = 0) OR (v_count2 IS NULL)
      THEN
         EXIT;
      ELSE
         v_strlist.EXTEND ();
         v_strlist (v_i) := v_node;
         v_node := '';
		 dbms_output.put_line('2......');
      END IF;
   END LOOP;

   RETURN v_strlist;
END FUNC_CTP_LG_GETSPLITSTRING;
//
   

CREATE OR REPLACE FUNCTION func_DCT_clob_analyze(str IN CLOB,
i_separator IN VARCHAR2) RETURN DCT_ARRYTYPE IS
  RESULT DCT_ARRYTYPE;
  i      INTEGER;
  b_pos  INTEGER;
  e_pos  INTEGER;
  len    INTEGER;

  separator VARCHAR2(10);
  sep_len   NUMBER;
BEGIN
  separator := i_separator;
  IF (separator IS NULL)
  THEN
    separator := '$|$';
  END IF;
  sep_len := length(separator);
  RESULT  := DCT_ARRYTYPE();
  i       := 1;
  b_pos   := 1;
  e_pos   := dbms_lob.instr(str, separator, 1, i);

  WHILE (e_pos != 0)
  LOOP
    RESULT.EXTEND();
    len := e_pos - b_pos;
    RESULT(i) := dbms_lob.substr(str, len, b_pos);
	
    i := i + 1;
    b_pos := e_pos + sep_len;
    e_pos := dbms_lob.instr(str, separator, 1, i);
	dbms_output.put_line('2......');
  END LOOP;
  
  e_pos := dbms_lob.getlength(str) + 1;
  IF (b_pos <> e_pos)
  THEN
    RESULT.EXTEND();
    len := dbms_lob.getlength(str) + 1 - b_pos;
    RESULT(i) := dbms_lob.substr(str, len, b_pos);
	dbms_output.put_line('3......');
  END IF;
  RETURN(RESULT);

EXCEPTION
  WHEN OTHERS THEN
    RETURN NULL;
END func_DCT_clob_analyze;
//


CREATE OR REPLACE FUNCTION func_DCT_str_analyze(str IN VARCHAR2, 
i_separator IN VARCHAR2) RETURN DCT_ARRYTYPE IS
  RESULT DCT_ARRYTYPE;
  i      INTEGER;
  b_pos  INTEGER;
  e_pos  INTEGER;
  len    INTEGER;

  separator VARCHAR2(10);
  sep_len   NUMBER;
BEGIN
  separator := i_separator;
  IF (separator IS NULL)
  THEN
    separator := '$|$';
  END IF;
  sep_len := length(separator);
  RESULT  := DCT_ARRYTYPE();
  i       := 1;
  b_pos   := 1;
  e_pos   := instr(str, separator, 1, i);

  WHILE (e_pos != 0)
  LOOP
    RESULT.EXTEND();
	
    IF (e_pos - b_pos > 4000)
    THEN
      len := 4000;
    ELSE
      len := e_pos - b_pos;
    END IF;
    RESULT(i) := substr(str, b_pos, len);
	
    i := i + 1;
    b_pos := e_pos + sep_len;
    e_pos := instr(str, separator, 1, i);
	dbms_output.put_line('1......');
  END LOOP;
  
  e_pos := length(str) + 1;
  IF (b_pos <> e_pos)
  THEN
    RESULT.EXTEND();
    RESULT(i) := substr(str, b_pos, length(str));
	dbms_output.put_line('2......');
  END IF;
  RETURN(RESULT);

EXCEPTION
  WHEN OTHERS THEN
    RETURN NULL;
END func_DCT_str_analyze;
//

CREATE OR REPLACE FUNCTION func_dams_getarrayfromstr(i_str IN VARCHAR2,
split IN VARCHAR2 := ',') RETURN dbms_sql.varchar2_table IS

  v_pos    BINARY_INTEGER;
  v_oldpos BINARY_INTEGER;
  i        BINARY_INTEGER;
  v_temp   VARCHAR2(2000);
  t_res    dbms_sql.varchar2_table;
BEGIN
  v_oldpos := 1;
  i        := 1;
  v_pos    := instr(i_str, split);
  
  IF v_pos = 0 THEN
    t_res(1) := i_str;
	dbms_output.put_line('1......');
  END IF;
  
  WHILE v_pos > 0
  LOOP
    v_temp := substr(i_str, v_oldpos, v_pos - v_oldpos);
    t_res(i) := v_temp;
    i := i + 1;
    v_oldpos := v_pos + length(split);
    v_pos := instr(i_str, split, v_oldpos);
	dbms_output.put_line('2......');
  END LOOP;

  t_res(i) := substr(i_str, v_oldpos);

  RETURN t_res;
END func_dams_getarrayfromstr;
//



CREATE OR REPLACE FUNCTION func_split2_clob(list IN CLOB,
delimiter IN VARCHAR2 DEFAULT ',') RETURN dams_arrytype AS
  splitted dams_arrytype := dams_arrytype();
  i        PLS_INTEGER := 0;
  list_    CLOB := list;
BEGIN
  IF list IS NULL THEN
    dbms_output.put_line('1......');
    RETURN splitted;
  ELSE
    LOOP
      i := instr(list_, delimiter);
      IF i > 0 THEN
        splitted.extend(1);
        splitted(splitted.last) := to_char(substr(list_, 1, i - 1));
        list_ := substr(list_, i + length(delimiter));
		dbms_output.put_line('2......');
      ELSE
        splitted.extend(1);
        splitted(splitted.last) := to_char(list_);
		dbms_output.put_line('3......');
        RETURN splitted;
      END IF;
    END LOOP;

  END IF;
END func_split2_clob;
//


CREATE OR REPLACE FUNCTION FUNC_CTP_LG_GETARRAYFROMSTR(tmpstr IN VARCHAR2)
RETURN ctp_type_arraytype IS
	i       INTEGER;
	pos     INTEGER;
	len     NUMBER;
	objdata ctp_type_arraytype;
BEGIN
	pos     := 1;
	objdata := ctp_type_arraytype();
	i       := instr(tmpstr ,'$|$' ,pos);
	IF i IS NULL OR i <= 0 THEN
		objdata.EXTEND();
		objdata(1) := tmpstr;
		dbms_output.put_line('1......');
		RETURN objdata;
	END IF;

	len := to_number(substr(tmpstr ,pos ,i - 1));
	pos := i + 3;

	FOR j IN 1 .. len - 1
	LOOP
		objdata.EXTEND();
		i := instr(tmpstr ,'$|$' ,pos);
		IF i = 0 THEN
		    dbms_output.put_line('2......');
			RETURN NULL;
		END IF;
		objdata(j) := substr(tmpstr ,pos ,i - 1 - pos + 1);
		pos := i + 3;
		dbms_output.put_line('3......');
	END LOOP;
	objdata.EXTEND();
	objdata(len) := substr(tmpstr ,pos ,length(tmpstr) - pos + 1);
	dbms_output.put_line('4......');
	RETURN objdata;
EXCEPTION
	WHEN OTHERS THEN
		RETURN NULL;
END FUNC_CTP_LG_GETARRAYFROMSTR;
//

CREATE OR REPLACE FUNCTION FUNC_CTP_LG_CANBEUSEDBYROLE(
roleBranchId IN VARCHAR2,
rolelevel    IN VARCHAR2,
itemlevel    IN VARCHAR2,
itembranchid IN VARCHAR2)
RETURN VARCHAR2 IS
    flag     VARCHAR2(10);
    maxindex INTEGER;
    i        INTEGER;
    tmpLEVEL VARCHAR2(100);
    intlevel INTEGER;
    tmpCount INTEGER;
BEGIN
	flag := '0'; 
	IF (itemlevel IS NULL OR itemlevel = 'null' 
	OR TRIM(itemlevel) IS NULL) 
	AND (itembranchid IS NULL OR itembranchid = 'null' OR
	TRIM(itembranchid) IS NULL) THEN
	    dbms_output.put_line('1......');
		RETURN flag;
	ELSE
		IF rolelevel IS NOT NULL AND rolelevel != 'null' 
		AND TRIM(rolelevel) IS NOT NULL THEN
			IF itemlevel IS NOT NULL AND itemlevel != 'null' 
			AND TRIM(itemlevel) IS NOT NULL THEN
				maxindex := length(itemlevel);
				IF length(rolelevel) < maxindex THEN
					maxindex := length(rolelevel); --itemLevel和roleLevel两者取最小
				END IF;
				i := 1;
				FOR i IN 1 .. maxindex
				LOOP
					IF substr(itemlevel ,i ,1) = '1' 
					AND substr(rolelevel ,i ,1) = '1' THEN
						flag := 1;
						dbms_output.put_line('2......');
						RETURN flag;
					END IF;
				END LOOP;

			ELSE
				IF itembranchid IS NOT NULL AND itembranchid != 'null' AND
					TRIM(itembranchid) IS NOT NULL THEN
					SELECT branch_level
					  INTO tmpLEVEL
					  FROM ctp_branch
					 WHERE id = itembranchid;
					intlevel := to_number(tmpLEVEL);
					
         	        IF intlevel <= length(rolelevel) 
			        AND substr(rolelevel ,intlevel ,1) = '1' THEN
                        SELECT count(*) into  tmpCount from ctp_branch where id=itembranchid and id in( select id from ctp_branch start with id=roleBranchId connect by prior id=parent_id);
			        	
                        if tmpCount>0 THEN
			        	    flag := 1;
							dbms_output.put_line('3......');
			        	    RETURN flag;
                        END IF;
			        END IF;
				END IF;
			END IF;
		END IF;
	END IF;
	RETURN flag;

EXCEPTION
	WHEN OTHERS THEN
		RETURN flag;
END FUNC_CTP_LG_CANBEUSEDBYROLE;
//


CREATE OR REPLACE FUNCTION FUNC_CTP_LG_HASAUTHPRI(
  i_priAll in varchar2,
  i_priSelf in varchar2,
  i_priOther in varchar2,
  i_userBranchLevel in varchar2,
  i_opBranchLevel in varchar2 
  )
  return varchar2
is
  flag varchar2(1);
begin
  flag:=0;
  if i_priAll is null and i_priSelf is null and i_priOther is null then
      dbms_output.put_line('1......');
      return flag;
  end if;
  if(i_priAll='1') then
     flag:=1;
	 dbms_output.put_line('2......');
     return flag;
  else
      if to_number(i_userBranchLevel) =to_number(i_opBranchLevel) then
        if i_priSelf='1' then
          flag:=1;
		  dbms_output.put_line('3......');
          return flag;
        end if;
      else 
        if i_priOther is not null and i_priOther!='0' and (to_number(i_opBranchLevel)-to_number(i_userBranchLevel)<=to_number(i_priOther)) then
           flag:=1;
		   dbms_output.put_line('4......');
           return flag;
        end if;
     end if;
  end if;
  return flag;
EXCEPTION
 WHEN OTHERS THEN
      return flag;
END;
//



CREATE OR REPLACE function func_split(str in VARCHAR2,i_separator IN VARCHAR2)
  
return TP_ARRAY_TABLE is
  resultarr TP_ARRAY_TABLE;
  i      INTEGER;
  b_pos  INTEGER;
  e_pos  INTEGER;
  len    INTEGER;

  separator VARCHAR2(10);
  sep_len   NUMBER;
BEGIN
  separator := i_separator;
  IF(separator IS NULL)THEN
     separator := '$|$';
  END IF;
  sep_len := length(separator);
  resultarr := TP_ARRAY_TABLE();
  i := 1;
  b_pos  := 1;
  e_pos  := instr(str,separator,1,i);

  WHILE(e_pos!= 0)LOOP
     resultarr.extend();
     IF(e_pos-b_pos > 4000)THEN
        len := 4000;
     ELSE
        len := e_pos-b_pos;
     END IF;
     resultarr(i) := substr(str,b_pos,len);
     i := i+1;
     b_pos := e_pos+sep_len;
     e_pos := instr(str,separator,1,i);
	 dbms_output.put_line('1......');
  END LOOP;
  e_pos := length(str)+1;
  IF(b_pos <> e_pos)THEN
     resultarr.extend();
     resultarr(i) := substr(str,b_pos,length(str));
	 dbms_output.put_line('2......');
  END IF;
  RETURN(resultarr);

  EXCEPTION
     WHEN OTHERS THEN
          RETURN NULL;
end func_split;
//

CREATE OR REPLACE FUNCTION func_split2(list IN VARCHAR2,
delimiter IN VARCHAR2 DEFAULT ',') RETURN dams_arrytype AS
  splitted dams_arrytype := dams_arrytype();
  i        PLS_INTEGER := 0;
  list_    VARCHAR2(32767) := list;
BEGIN
  IF list IS NULL THEN
    dbms_output.put_line('1......');
    RETURN splitted;
  ELSE
    LOOP
      i := instr(list_, delimiter);
      IF i > 0 THEN
        splitted.extend(1);
        splitted(splitted.last) := substr(list_, 1, i - 1);
        list_ := substr(list_, i + length(delimiter));
		dbms_output.put_line('2......');
      ELSE
        splitted.extend(1);
        splitted(splitted.last) := list_;
		dbms_output.put_line('3......');
        RETURN splitted;
      END IF;
    END LOOP;
  END IF;
END func_split2;
//

CREATE OR REPLACE PROCEDURE PROC_CTP_LG_CHECKAUTHUSER(
    i_opUserId             in varchar2,--登录用户
    i_authUserId           IN  VARCHAR2,--授权用户
    i_opFlag               IN VARCHAR2,--0新增机构，1其它
    i_opBranchId           IN VARCHAR2,--被操作的对象的机构id
    i_opBranchLevel        IN VARCHAR2,--被操作的对象的机构级别
    i_opBranchPId          IN VARCHAR2,--被操作的对象的机构的父机构id，用于机构新增
    out_procRetCode    OUT VARCHAR2--  -1数据库操作异常 0有权限 1授权用户不存在 2授权用户等于登录用户 3授权用户没有维护此对象的权限 4 授权用户被冻结 5授权用户的机构被冻结

    )
IS
 v_userStatus ctp_user.status%type;
 v_priAll ctp_user.privilege_all%type;
 v_priSelf ctp_user.privilege_self%type;
 v_priOther ctp_user.privilege_other%type;
 v_userBranchId ctp_branch.id%type;
 v_userBranchLevel ctp_branch.branch_level%type;
 v_flag varchar(1);
 v_count  varchar(3);

 cursor v_userRole is
 select privilege_all,privilege_self,privilege_all from CTP_ROLE_USER_REL a,ctp_role b WHERE a.USER_ID=i_authUserId and a.role_id=b.id;
BEGIN
 out_procRetCode:='0';
 v_flag:='0';

  select count(*) into v_count from ctp_user where id=i_authUserId;--1授权用户不存在
  if v_count=0 then
      out_procRetCode:='1';
	  dbms_output.put_line('1......');
      return;
  end if;

  select a.status,a.privilege_all,a.privilege_self,a.privilege_other,b.id,b.branch_level into v_userStatus,v_priAll,v_priSelf,v_priOther,v_userBranchId,v_userBranchLevel from CTP_USER a,ctp_branch b WHERE a.ID=i_authUserId and a.branch_id=b.id;

  if i_opUserId=i_authUserId then
      out_procRetCode:='2';
	  dbms_output.put_line('2......');
      return;
  end if;

  if to_number(v_userBranchLevel)>to_number(i_opBranchLevel) then
      out_procRetCode:='3';
	  dbms_output.put_line('3......');
      return;
  end if;

  if i_opFlag='0' then
       select count(*) into v_count from CTP_BRANCH where id=v_userBranchId and id in(select id from ctp_branch start with id=i_opBranchPId connect by id=prior parent_id);--授权柜员的所属机构必须是新增机构的父机构或其父链机构
	   dbms_output.put_line('4......');
  else
      select count(*) into v_count from CTP_BRANCH where id=v_userBranchId and id in(select id from ctp_branch start with id=i_opBranchId connect by id=prior parent_id);--授权柜员的所属机构必须是被维护的对象的机构或其父链机构
	  dbms_output.put_line('5......');
  end if;

  if v_count=0 then
     out_procRetCode:='3';
	 dbms_output.put_line('6......');
     return;
  end if;

  if v_userStatus!='1' then
     out_procRetCode:='4';
	 dbms_output.put_line('7......');
     return;
  end if;

  SELECT count(*) into v_count FROM CTP_BRANCH WHERE  status!='1' start with id=v_userBranchId  connect by id=prior parent_id;
  if v_count>0 then--授权用户的父链机构不能被冻结
     out_procRetCode:='5';
	 dbms_output.put_line('8......');
     return;
  end if;

  if v_priAll='0' and v_priSelf='0' and v_priOther='0' then --如果用户没有定义权限
            open v_userRole;
            LOOP
             FETCH v_userRole INTO v_priAll,v_priSelf,v_priOther;
             exit when v_userRole%notfound;
             v_flag:=func_ctp_lg_hasauthpri(v_priAll,v_priSelf,v_priOther,v_userBranchLevel,i_opBranchLevel);
                exit when  v_flag='1' ;
			dbms_output.put_line('9......');
      end loop;
  else--用户定义了权限
           v_flag:=func_ctp_lg_hasauthpri(v_priAll,v_priSelf,v_priOther,v_userBranchLevel,i_opBranchLevel);
		   dbms_output.put_line('10......');
  end if;
  if v_flag='0' then
       out_procRetCode:='3';
  else
       out_procRetCode:='0';
  end if;
  return;


EXCEPTION
    WHEN OTHERS THEN
   out_procRetCode:='-1';--数据库操组异常
 return;
END;
//


CREATE OR REPLACE PROCEDURE PROC_CTP_LG_GETCURRROLEINFO(
i_userid        IN  VARCHAR2,
i_defaultroleid IN  VARCHAR2, --选择角色
o_retcode       OUT VARCHAR2,
o_defaultroleid OUT VARCHAR2, --选择角色
o_userpriall    OUT VARCHAR2, --用户权限管理全部级别标志
o_userpriself   OUT VARCHAR2, --用户权限管理本级别标志
o_userpriother  OUT VARCHAR2 --用户权限管理下级级别数
) IS
  v_num          VARCHAR2(2);
  v_rolepriall   VARCHAR2(1);
  v_rolepriself  VARCHAR2(1);
  v_rolepriother VARCHAR2(2);
BEGIN
  o_retcode := '0';

  SELECT COUNT(*)
    INTO v_num
    FROM ctp_role_user_rel
   WHERE user_id = i_userid AND role_id = i_defaultroleid;
  IF (v_num > 0) THEN
    o_defaultroleid := i_defaultroleid;
	dbms_output.put_line('1......');
  ELSE
    SELECT COUNT(*)
      INTO v_num
      FROM ctp_role_user_rel
     WHERE user_id = i_userid;
    IF (v_num > 0) THEN
      SELECT role_id
        INTO o_defaultroleid
        FROM ctp_role_user_rel
       WHERE user_id = i_userid AND rownum = 1;
	   dbms_output.put_line('2......');
    END IF;
  END IF;

  --查询用户、角色的权限级别，若用户的权限没有进行设定（三个值都为0，则看角色的权限）
  SELECT privilege_all, privilege_self, privilege_other
    INTO o_userpriall, o_userpriself, o_userpriother
    FROM ctp_user
   WHERE id = i_userid;

  IF (o_defaultroleid IS NOT NULL) THEN
    SELECT privilege_all, privilege_self, privilege_other
      INTO v_rolepriall, v_rolepriself, v_rolepriother
      FROM ctp_role
     WHERE id = o_defaultroleid;

    IF (o_userpriall = '0' AND o_userpriself = '0' AND o_userpriother = '0') THEN
      IF (v_rolepriall IS NULL) THEN
        o_userpriall := '';
      ELSE
        o_userpriall := v_rolepriall;
      END IF;

      IF (v_rolepriself IS NULL) THEN
        o_userpriself := '';
      ELSE
        o_userpriself := v_rolepriself;
      END IF;

      IF (v_rolepriother IS NULL) THEN
        o_userpriother := '';
      ELSE
        o_userpriother := v_rolepriother;
      END IF;
	  dbms_output.put_line('3......');
    END IF;
  END IF;
  RETURN;
EXCEPTION
  WHEN OTHERS THEN
    o_retcode := '-1';
    RETURN;
END PROC_CTP_LG_GETCURRROLEINFO;
//


CREATE OR REPLACE PROCEDURE PROC_CTP_LG_UPDATESHORTCUTMENU(
i_userid        IN VARCHAR2,
i_defaultroleid IN VARCHAR2,
i_itemid        IN VARCHAR2,
o_retcode       OUT VARCHAR2) 
IS
	itemobj ctp_type_arraytype;
	v_num   VARCHAR2(2);
BEGIN
	o_retcode := '0';

	IF (i_defaultroleid IS NULL) THEN
		DELETE FROM ctp_user_shortcut_menu
		 WHERE user_id = i_userid;

		IF i_itemid IS NOT NULL THEN
			itemobj := func_ctp_lg_getarrayfromstr(i_itemid);

			FOR i IN 1 .. itemobj.COUNT
			LOOP
				SELECT COUNT(*)
				  INTO v_num
				  FROM ctp_user_shortcut_menu
				 WHERE user_id = i_userid AND item_id = itemobj(i);  -- 4

				IF v_num = 0 THEN
					INSERT INTO ctp_user_shortcut_menu
					VALUES
						(i_userid, '', itemobj(i));
				    dbms_output.put_line('10......');
				END IF;
			END LOOP;
		END IF;
	ELSE  -- CTP_USER_SHORTCUT_MENU
		DELETE FROM ctp_user_shortcut_menu
		 WHERE user_id = i_userid AND role_id = i_defaultroleid;

		IF i_itemid IS NOT NULL THEN
			itemobj := func_ctp_lg_getarrayfromstr(i_itemid);

			FOR i IN 1 .. itemobj.COUNT
			LOOP
				SELECT COUNT(*)
				  INTO v_num
				  FROM ctp_user_shortcut_menu
				 WHERE user_id = i_userid AND role_id = i_defaultroleid AND
						 item_id = itemobj(i);

				IF v_num = 0 THEN
					INSERT INTO ctp_user_shortcut_menu
					VALUES
						(i_userid, i_defaultroleid, itemobj(i));
					dbms_output.put_line('20......');
				END IF;
			END LOOP;
		END IF;

		UPDATE ctp_role_user_rel
			SET menuchg_flag = '1'
		 WHERE user_id = i_userid AND role_id = i_defaultroleid;
		 dbms_output.put_line('30......');
	END IF;

	RETURN;

EXCEPTION
	WHEN OTHERS THEN
		o_retcode := '-1';
		RETURN;
END PROC_CTP_LG_UPDATESHORTCUTMENU;
//

CREATE OR REPLACE PROCEDURE PROC_CTP_LG_JOURNALPROCEDURE(
in_trancode         IN VARCHAR2, --交易代码
in_trandate         IN VARCHAR2, --交易日期（yyyyMMddHHmmss）
in_areacode         IN VARCHAR2, --地区代码
in_netterminal      IN VARCHAR2, --网点代码
in_userid           IN VARCHAR2, --客户号
in_errorcode        IN VARCHAR2, --交易状态
in_errormessage     IN VARCHAR2, --错误信息
in_errorlocation    IN VARCHAR2, --错误定位
in_errordescription IN VARCHAR2, --错误描述
in_errorstack       IN VARCHAR2, --错误堆栈
in_journalstr       IN VARCHAR2, --日志内容
out_procsign        OUT VARCHAR2 --标志位（0，成功；-1 异常）
) AS
	p_datestr      VARCHAR2(8);
	v_log_serialno VARCHAR2(20);
BEGIN
	out_procsign := '0';

	SELECT tranlogsequence.NEXTVAL
	  INTO v_log_serialno
	  FROM dual;

	v_log_serialno := lpad(v_log_serialno ,20 ,'0');
	BEGIN
		SELECT to_char(SYSDATE ,'YYYYMMDD')
		  INTO p_datestr
		  FROM dual; --得到当前日期
		INSERT INTO tranlog
			(logserialno,
			 trancode,
			 trandate,
			 areacode,
			 netterminal,
			 userid,
			 errorcode,
			 errormessage,
			 errorlocation,
			 errordescription,
			 errorstack,
			 journal)
		VALUES
			(v_log_serialno,
			 in_trancode,
			 p_datestr,
			 in_areacode,
			 in_netterminal,
			 in_userid,
			 in_errorcode,
			 in_errormessage,
			 in_errorlocation,
			 in_errordescription,
			 in_errorstack,
			 in_journalstr);

		COMMIT;
		dbms_output.put_line('1......');
		RETURN;
	EXCEPTION
		WHEN OTHERS THEN
			out_procsign := '-1';
			ROLLBACK;
			RETURN;
	END;
EXCEPTION
	WHEN OTHERS THEN
		out_procsign := '-1';
		ROLLBACK;
		RETURN;
END;
//
delimiter ;//
