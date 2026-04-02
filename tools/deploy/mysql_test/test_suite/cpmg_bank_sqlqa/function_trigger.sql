

CREATE TABLE "CPMG"."CTP_BRANCH" (
	"ID" VARCHAR2(20 CHAR) NOT NULL,
	"NAME" VARCHAR2(100 CHAR) NOT NULL,
	"DESCRIPTION" VARCHAR2(500 CHAR),
	"STATUS" VARCHAR2(2 CHAR) DEFAULT 1  NOT NULL,
	"REGION_ID" VARCHAR2(20 CHAR),
	"NET_TERMINAL" VARCHAR2(20 CHAR),
	"PARENT_ID" VARCHAR2(20 CHAR),
	"BRANCH_LEVEL" VARCHAR2(3 CHAR) DEFAULT 1  NOT NULL,
	"MODI_USER" VARCHAR2(20 CHAR),
	"MODI_TIME" VARCHAR2(8 CHAR),
	"SHORT_NAME" VARCHAR2(80 CHAR),
	"BRANCH_CATEGORY" VARCHAR2(1 CHAR),
	"FINACE_ID" VARCHAR2(40 CHAR),
	"ADDR" VARCHAR2(120 CHAR),
	"ZIPCODE" VARCHAR2(6 CHAR),
	"PHONE" VARCHAR2(40 CHAR),
	"SIGN" VARCHAR2(12 CHAR),
	"ADMIN_LEVEL" VARCHAR2(4 CHAR),
	"OPEN_TIME" VARCHAR2(6 CHAR),
	"LAST_ALT_TYPE" VARCHAR2(16 CHAR),
	"BALANCE_ID" VARCHAR2(36 CHAR),
	"CODECERT_ID" VARCHAR2(36 CHAR),
	"REVOKE_TIME" VARCHAR2(6 CHAR),
	CONSTRAINT "PK_CTP_BRANCH" PRIMARY KEY ("ID")
);

CREATE TABLE "CPMG"."CAP_TRANS_BRANCH_SORT" (
	"TRANS_BRANCH_ID" VARCHAR2(20 CHAR) NOT NULL,
	"TRANS_BRANCH_NAME" VARCHAR2(80 CHAR),
	"SORT_NO" NUMBER
);

CREATE TABLE "CPMG"."DIC_ALL_KIND" (
	"OPERATION_KIND" VARCHAR2(30 CHAR) NOT NULL,
	"KIND_ID" VARCHAR2(30 CHAR) NOT NULL,
	"KIND_NAME" VARCHAR2(200 CHAR),
	"REMARK" VARCHAR2(200 CHAR)
	
);


CREATE TABLE "CPMG"."PRM_IRA_ORGAN" (
	"ZONE_NO" VARCHAR2(5 CHAR) NOT NULL,
	"BR_NO" VARCHAR2(5 CHAR) NOT NULL,
	"ACT_BR_NO" VARCHAR2(5 CHAR),
	"BANK_FLAG" VARCHAR2(1 CHAR) NOT NULL,
	"ORGAN_NAME" VARCHAR2(60 CHAR),
	"BELONG_ZONENO" VARCHAR2(5 CHAR),
	"BELONG_BRNO" VARCHAR2(5 CHAR),
	"BELONG_BANK_FLAG" VARCHAR2(1 CHAR),
	"BR_TYPE" VARCHAR2(1 CHAR),
	"START_DATE" VARCHAR2(8 BYTE) NOT NULL,
	"END_DATE" VARCHAR2(8 CHAR),
	"IS_SALES" VARCHAR2(1 CHAR),
	CONSTRAINT "PK_PRM_IRA_ORGAN" PRIMARY KEY ("ZONE_NO", "BR_NO", "BANK_FLAG", "START_DATE")
);

CREATE TABLE "CPMG"."PRM_XSFR2" (
	"PRODUCT_ID" VARCHAR2(10 CHAR),
	"YWPZ_NAME" VARCHAR2(50 CHAR),
	"SUBJECT_ALL" VARCHAR2(200 CHAR) NOT NULL,
	"YWPZ_ALL" VARCHAR2(200 CHAR) NOT NULL,
	"APP_TYPE" VARCHAR2(1 CHAR),
	"PAR_COEF" NUMBER(9,6),
	"START_DATE" VARCHAR2(8 BYTE) NOT NULL,
	"END_DATE" VARCHAR2(8 CHAR),
	"SEG_SEQ" VARCHAR2(5 CHAR),
	CONSTRAINT "PK_PRM_XSFR2" PRIMARY KEY ("SUBJECT_ALL", "YWPZ_ALL", "START_DATE")
);

CREATE SEQUENCE "CPMG"."SEQ_XSFR2" START WITH 121 INCREMENT BY 1 MINVALUE 1 MAXVALUE 1000000000 NOCYCLE CACHE 500 NOORDER;

CREATE TABLE "CPMG"."PRM_PRODUCT" (
	"PRODUCT_ID" VARCHAR2(10 CHAR) NOT NULL,
	"PRODUCT_LEVEL" VARCHAR2(1 CHAR),
	"PRODUCT_NAME" VARCHAR2(100 CHAR),
	"PARENT_ID" VARCHAR2(10 CHAR),
	"START_DATE" VARCHAR2(8 BYTE) NOT NULL,
	"END_DATE" VARCHAR2(8 CHAR),
	CONSTRAINT "PK_PRM_PRODUCT" PRIMARY KEY ("PRODUCT_ID", "START_DATE")
);


delimiter //;

CREATE OR REPLACE TYPE        "CTP_TYPE_ARRAYTYPE"                                          IS TABLE OF VARCHAR2(100);
//
CREATE OR REPLACE FUNCTION FUNC_GET_BRANCHNAME(BRANCHID IN VARCHAR2)
  RETURN VARCHAR2 IS
  RETVALUE CTP_BRANCH.NAME%TYPE;
BEGIN  
  --20160121 总行下的一级分行及一级分行营业部的名字另初始化
  SELECT max(C.TRANS_BRANCH_NAME) INTO RETVALUE 
  FROM CAP_TRANS_BRANCH_SORT C 
  WHERE C.TRANS_BRANCH_ID = BRANCHID;
  
  if retVALUE is null then  
    SELECT MAX(B.NAME) INTO RETVALUE FROM CTP_BRANCH B WHERE B.ID = BRANCHID;    
    if retVALUE is null then
      SELECT max(ORGAN_NAME) into RETVALUE
                           FROM PRM_IRA_ORGAN
                          WHERE (ZONE_NO || BR_NO) = BRANCHID
                          and END_DATE = '99991231';
    end if;                    
  end if;
  RETURN RETVALUE;

EXCEPTION
  WHEN OTHERS THEN

    RETURN RETVALUE;
END;
//
  
  

CREATE OR REPLACE function CTP_FUNC_BANKLEVEL2(ZONENO IN VARCHAR2,
                                               BRNO   IN VARCHAR2)
  RETURN VARCHAR2 IS
  ora_name   VARCHAR2(20) := null;
  P_ZONEBRNO VARCHAR2(10) := null;
BEGIN
  SELECT p.organ_name
    into ora_name
    FROM prm_ira_organ p
   where ((p.zone_no = zoneno and p.belong_bank_flag = '4' and
         p.bank_flag = '3')
      or (p.zone_no || p.br_no = zoneno || brno And
         p.belong_bank_flag = '4' and p.bank_flag = '2'))
         and p.end_date=to_char(to_date('99991231','YYYYMMDD'),'YYYYMMDD');
  return ora_name;
EXCEPTION
  WHEN OTHERS THEN
    select p.belong_zoneno || p.belong_brno
      into P_ZONEBRNO
      from prm_ira_organ p
     where p.zone_no || p.br_no = zoneno || brno
       And p.belong_bank_flag = '2'
       and p.bank_flag = '0'
       and p.end_date=to_char(to_date('99991231','YYYYMMDD'),'YYYYMMDD');
    select p.organ_name
      INTO ora_name
      FROM prm_ira_organ p
     where p.zone_no || p.br_no = P_ZONEBRNO
       and p.belong_bank_flag = '4'
       and p.bank_flag = '2'
        and p.end_date=to_char(to_date('99991231','YYYYMMDD'),'YYYYMMDD');
    return ora_name;
end;
//

CREATE OR REPLACE FUNCTION ctp_func_hasAuthPri(
  i_priAll in varchar2,
  i_priSelf in varchar2,
  i_priOther in varchar2,
  i_userBranchLevel in varchar2,--用户的机构级别
  i_opBranchLevel in varchar2 --被操作的对象的机构级别。如果是角色，则是角色的创建机构的级别。如果是新增机构，则是新增的机构的级别
  )
  return varchar2
is
  flag varchar2(1);
begin
  flag:=0;--0没有权限1有权限
  if i_priAll is null and i_priSelf is null and i_priOther is null then
      return flag;
  end if;
  if(i_priAll='1') then
     flag:=1;
     return flag;
  else
     if to_number(i_userBranchLevel) =to_number(i_opBranchLevel) then
      if i_priSelf='1' then
            flag:=1;
     return flag;
     end if;
     else --此时必定是i_userBranchLevel<i_opBranchLevel
      if i_priOther is not null and i_priOther!='0' and (to_number(i_opBranchLevel)-to_number(i_userBranchLevel)<=to_number(i_priOther))  then
           flag:=1;
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

CREATE OR REPLACE FUNCTION ctp_func_canbeusedbyrole(
roleBranchId IN VARCHAR2,
rolelevel    IN VARCHAR2,
																	itemlevel    IN VARCHAR2,
																	itembranchid IN VARCHAR2)
	RETURN VARCHAR2 IS
	flag     VARCHAR2(10);
	maxindex INTEGER;
	i        INTEGER;
	tmpLEVEL    VARCHAR2(100);
  intlevel INTEGER;
  tmpCount INTEGER;

BEGIN
	flag := '0'; --0不可使用1可使用
	IF (itemlevel IS NULL OR itemlevel = 'null' OR TRIM(itemlevel) IS NULL) AND
		(itembranchid IS NULL OR itembranchid = 'null' OR
		TRIM(itembranchid) IS NULL) THEN
		RETURN flag;

	ELSE
		IF rolelevel IS NOT NULL AND rolelevel != 'null' AND
			TRIM(rolelevel) IS NOT NULL THEN
			IF itemlevel IS NOT NULL AND itemlevel != 'null' AND
				TRIM(itemlevel) IS NOT NULL THEN
				maxindex := length(itemlevel);
				IF length(rolelevel) < maxindex THEN
					maxindex := length(rolelevel); --itemLevel和roleLevel两者取最小
				END IF;
				i := 1;
				FOR i IN 1 .. maxindex
				LOOP
					IF substr(itemlevel
								,i
								,1) = '1' AND substr(rolelevel
														  ,i
														  ,1) = '1' THEN
						flag := 1;
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
         	IF intlevel <= length(rolelevel) AND
						substr(rolelevel
								,intlevel
								,1) = '1' THEN
            SELECT count(*) into  tmpCount from ctp_branch where id=itembranchid and id in( select id from ctp_branch start with id=roleBranchId connect by prior id=parent_id);
            if tmpCount>0 THEN
						   flag := 1;
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
END;
//

CREATE OR REPLACE FUNCTION ctp_func_canbeusedbyuser(userbranchlevel IN VARCHAR2,
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
				IF substr(itemlevel
							,intlevel
							,1) = '1' THEN
					flag := 1;
					RETURN flag;
				END IF;
			END IF;
		ELSE
			IF itembranchid IS NOT NULL AND itembranchid != 'null' AND
				TRIM(itembranchid) IS NOT NULL THEN
				IF userbranchid = itembranchid THEN
					flag := 1;
					RETURN flag;
				END IF;
			END IF;
		END IF;
	END IF;

	RETURN flag;

EXCEPTION
	WHEN OTHERS THEN

		RETURN flag;
END;
//

CREATE OR REPLACE FUNCTION FUNC_GET_AVGPRICE(SUMVAL NUMBER,
                                             MAXVAL NUMBER,
                                             MINVAL NUMBER,
                                             CNT    NUMBER) RETURN VARCHAR2 IS
  RETVALUE varchar(30);
BEGIN
  
  if cnt = 0 then
     retvalue := '';
  elsif cnt <= 2 then
     select trim(to_char(SUMVAL/cnt, '999999999999999999999.99')) into RETVALUE from dual;
  else
    select trim(to_char((SUMVAL-maxval - minval)/(cnt - 2), '999999999999999999999.99')) into RETVALUE from dual;
  end if;  
  
  RETURN RETVALUE;

EXCEPTION
  WHEN OTHERS THEN
  
    RETURN RETVALUE;
END;
//

CREATE OR REPLACE function FUNC_CTP_LG_GETARRAY(TMPSTR IN VARCHAR2, PARAM IN VARCHAR2) RETURN ctp_type_arraytype IS
    I       INTEGER;
    POS     INTEGER;
    K       INTEGER;
    V_COUNT INTEGER := 0;
    OBJDATA ctp_type_arraytype;
  BEGIN
    K       := 1;
    POS     := 1;
    OBJDATA := ctp_type_arraytype();
    I       := INSTR(TMPSTR, PARAM, POS);

    WHILE K <> 0 LOOP
      K       := INSTR(TMPSTR, PARAM, POS);
      POS     := K + LENGTH(PARAM);
      V_COUNT := V_COUNT + 1;
    END LOOP;

    IF I <= 0 THEN
      OBJDATA.EXTEND();
      OBJDATA(1) := TMPSTR;
      RETURN OBJDATA;
    END IF;

    POS := 1;

    FOR J IN 1 .. V_COUNT - 1 LOOP
      OBJDATA.EXTEND();
      I := INSTR(TMPSTR, PARAM, POS);
      OBJDATA(J) := SUBSTR(TMPSTR, POS, I - 1 - POS + 1);
      POS := I + LENGTH(PARAM);
    END LOOP;

    OBJDATA.EXTEND();
    OBJDATA(V_COUNT) := SUBSTR(TMPSTR, POS, LENGTH(TMPSTR) - POS + 1);
    RETURN OBJDATA;

  EXCEPTION
    WHEN OTHERS THEN
      RETURN NULL;
  END;
  //
  
CREATE OR REPLACE FUNCTION ctp_func_getarrayfromstr(tmpstr IN VARCHAR2)
	RETURN ctp_type_arraytype IS
	i       INTEGER;
	pos     INTEGER;
	len     NUMBER;
	objdata ctp_type_arraytype;
BEGIN
	pos     := 1;
	objdata := ctp_type_arraytype();
	i       := instr(tmpstr
						 ,'$|$'
						 ,pos);
	IF i IS NULL OR i <= 0 THEN
		objdata.EXTEND();
		objdata(1) := tmpstr;
		RETURN objdata;
	END IF;

	len := to_number(substr(tmpstr
								  ,pos
								  ,i - 1));
	pos := i + 3;

	FOR j IN 1 .. len - 1
	LOOP
		objdata.EXTEND();
		i := instr(tmpstr
					 ,'$|$'
					 ,pos);
		IF i = 0 THEN
			RETURN NULL;
		END IF;
		objdata(j) := substr(tmpstr
								  ,pos
								  ,i - 1 - pos + 1);
		pos := i + 3;
	END LOOP;
	objdata.EXTEND();
	objdata(len) := substr(tmpstr
								 ,pos
								 ,length(tmpstr) - pos + 1);
	RETURN objdata;
EXCEPTION
	WHEN OTHERS THEN
		RETURN NULL;
END;
//

CREATE OR REPLACE TRIGGER CPMG.XSFR2_SEQ
   BEFORE INSERT
   ON CPMG.PRM_XSFR2     FOR EACH ROW
BEGIN
   SELECT SEQ_XSFR2.NEXTVAL
     INTO :NEW.SEG_SEQ
     FROM DUAL;
END;
//

CREATE OR REPLACE TRIGGER CPMG.FORECAST_ITEM
AFTER DELETE OR INSERT OR UPDATE
ON CPMG.PRM_PRODUCT REFERENCING NEW AS NEW OLD AS OLD
BEGIN
   DELETE FROM   dic_all_kind
         WHERE       operation_kind = 'FORECAST_ITEM_RMB'
                 AND SUBSTR (kind_id, 1, 4) IN ('RMB1', 'RMB2')
                 AND SUBSTR (kind_id, -1) <> 'T';

   INSERT INTO dic_all_kind (operation_kind, kind_id, kind_name)
      SELECT   'FORECAST_ITEM_RMB', 'RMB1' || product_id, product_name
        FROM   prm_product
       WHERE   parent_id = '001' AND end_date = '99991231';

   INSERT INTO dic_all_kind (operation_kind, kind_id, kind_name)
      SELECT   'FORECAST_ITEM_RMB', 'RMB2' || product_id, product_name
        FROM   prm_product
       WHERE   parent_id = '002' AND end_date = '99991231';
END;
//
delimiter ;//