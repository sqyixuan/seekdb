delimiter //;
CREATE OR REPLACE PACKAGE pckg_dams_util IS
  TYPE ref_cur IS REF CURSOR;
  TYPE r_sparea_list IS RECORD(
    sp_area      sf_card_sp_area_def.sp_area%TYPE,
    sp_area_name sf_card_sp_area_def.sp_area_name%TYPE);
  TYPE ref_sparea_list IS REF CURSOR RETURN r_sparea_list;
  
     FUNCTION func_decode(i_type   IN VARCHAR2, --字典类型,若类型后加_R,则为中文转代码
                       i_code   IN VARCHAR2, --字典代码
                       i_system IN VARCHAR2, --子系统编码
                       i_lang   IN VARCHAR2 := 'zh_CN' --语言代码
                       ) RETURN VARCHAR2;
  
  FUNCTION func_get_stru_name(i_stru_id IN VARCHAR2 --机构编码
                              ) RETURN VARCHAR2;
  
   PROCEDURE proc_dams_logger(i_logobj     IN VARCHAR2, --操作对象
                             i_logtype    IN VARCHAR2, --操作对象下的子类型
                             i_logdate    IN VARCHAR2, --操作日期
                             i_loghandler IN VARCHAR2, --操作人
                             i_loginfo    IN VARCHAR2, --概要信息
                             i_logdetail  IN CLOB := '' --详细信息（非必输）
                             );
	FUNCTION func_str_to_array(i_str        IN VARCHAR2, --需分割的字符串
                             i_split_str  IN VARCHAR2, --分隔符
                             i_split_flag IN VARCHAR2 := 1 --拆分方式,为1时,1;2拆分为[1,2],其他拆分为[1]
                             ) RETURN dams_arrytype;
	
	  FUNCTION func_gen_seq(i_prefix IN VARCHAR2, --主键前缀
                        i_seq    IN VARCHAR2, --序列器名称
                        i_len    IN NUMBER := 7 --序列器填充长度
                        ) RETURN VARCHAR2;
  
END pckg_dams_util;
//


CREATE OR REPLACE PACKAGE pckg_dams_role IS

  TYPE ret_list IS REF CURSOR;
  TYPE arrytype IS TABLE OF VARCHAR2(500);
  FUNCTION func_common_getarray(tmpstr IN VARCHAR2,
                                param  IN VARCHAR2) RETURN arrytype;

END pckg_dams_role;
//
CREATE OR REPLACE PACKAGE BODY pckg_dams_role IS
  FUNCTION func_common_getarray(tmpstr IN VARCHAR2,
                                param  IN VARCHAR2) RETURN arrytype IS
    i       INTEGER;
    pos     INTEGER;
    k       INTEGER;
    v_count INTEGER := 0;
    objdata arrytype;
  BEGIN
    k       := 1;
    pos     := 1;
    objdata := arrytype();
    i       := instr(tmpstr, param, pos);

    WHILE k <> 0
    LOOP
      k       := instr(tmpstr, param, pos);
      pos     := k + length(param);
      v_count := v_count + 1;
    END LOOP;

    IF i <= 0 THEN
      objdata.extend();
      objdata(1) := tmpstr;
      RETURN objdata;
    END IF;

    pos := 1;

    FOR j IN 1 .. v_count - 1
    LOOP
      objdata.extend();
      i := instr(tmpstr, param, pos);
      objdata(j) := substr(tmpstr, pos, i - 1 - pos + 1);
      pos := i + length(param);
    END LOOP;

    objdata.extend();
    objdata(v_count) := substr(tmpstr, pos, length(tmpstr) - pos + 1);
    RETURN objdata;

  EXCEPTION
    WHEN OTHERS THEN
      RETURN NULL;
  END;
END pckg_dams_role;
//
CREATE OR REPLACE PACKAGE BODY pckg_dams_util IS
  
  FUNCTION func_decode(i_type   IN VARCHAR2, --字典类型,若类型后加_R,则为中文转代码
                       i_code   IN VARCHAR2, --字典代码
                       i_system IN VARCHAR2, --子系统编码
                       i_lang   IN VARCHAR2 := 'zh_CN' --语言代码
                       ) RETURN VARCHAR2 IS
    v_result dams_gen_dict.dictvalue%TYPE;
    v_type   dams_gen_dict.dicttype%TYPE := upper(TRIM(i_type));
    v_count  NUMBER := NULL;
  BEGIN
    IF substr(v_type, lengthb(v_type) - 1, 2) = '_R' THEN
      --反解码  中文转代码
      SELECT COUNT(1)
        INTO v_count
        FROM dams_gen_dict
       WHERE dicttype = substr(v_type, 1, lengthb(v_type) - 2)
         AND sys_code = i_system
         AND dictlang = i_lang;
      IF v_count = 0 THEN
        RETURN i_code;
      END IF;
      SELECT dictcode
        INTO v_result
        FROM dams_gen_dict
       WHERE dicttype = substr(v_type, 1, lengthb(v_type) - 2)
         AND sys_code = i_system
         AND dictlang = i_lang
         AND dictvalue = i_code;
    ELSE
      --解码  代码转中文
      SELECT MAX(dictvalue)
        INTO v_result
        FROM dams_gen_dict
       WHERE dicttype = v_type
         AND sys_code = i_system
         AND dictlang = i_lang
         AND dictcode = i_code;
      IF v_result IS NULL THEN
        RETURN i_code;
      END IF;
    END IF;
    RETURN v_result;
  EXCEPTION
    WHEN OTHERS THEN
      RETURN NULL;
  END func_decode;
  
  FUNCTION func_get_stru_name(i_stru_id IN VARCHAR2 --机构编码
                              ) RETURN VARCHAR2 IS
    v_rlt VARCHAR2(4000);
  BEGIN
    SELECT t.stru_sname
      INTO v_rlt
      FROM dams_branch t
     WHERE t.stru_id = i_stru_id;

    RETURN v_rlt;
  EXCEPTION
    WHEN OTHERS THEN
      RETURN i_stru_id;
  END func_get_stru_name;

  PROCEDURE proc_dams_logger(i_logobj     IN VARCHAR2,
                             i_logtype    IN VARCHAR2,
                             i_logdate    IN VARCHAR2,
                             i_loghandler IN VARCHAR2,
                             i_loginfo    IN VARCHAR2,
                             i_logdetail  IN CLOB := '') IS
    PRAGMA AUTONOMOUS_TRANSACTION;
    v_key  VARCHAR2(20);
    v_info VARCHAR2(4000);
  BEGIN
    SELECT lpad(dams_syslog_seq.nextval, 20, '0') INTO v_key FROM dual;
    v_info := substrb(i_loginfo, 1, 4000);
    INSERT INTO dams_operation_loginfo
      (keyid, logobj, logtype, logdate, loghandler, loginfo, logdetail)
    VALUES
      (v_key,
       i_logobj,
       i_logtype,
       i_logdate,
       i_loghandler,
       i_loginfo,
       i_logdetail);
    COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
  END proc_dams_logger;
  
    FUNCTION func_str_to_array(i_str        IN VARCHAR2, --需分割的字符串
                             i_split_str  IN VARCHAR2, --分隔符
                             i_split_flag IN VARCHAR2 := 1 --拆分方式,为1时,1;2拆分为[1,2],其他拆分为[1]
                             ) RETURN dams_arrytype IS
    v_count1  INTEGER;
    v_count2  INTEGER;
    v_strlist dams_arrytype;
    v_node    VARCHAR2(4000);
    v_len     INTEGER;
  BEGIN
    v_count2  := 0;
    v_strlist := dams_arrytype();
    v_len     := lengthb(i_split_str);
    IF (i_str IS NULL) OR (length(i_str) <= 0) THEN
      RETURN NULL;
    END IF;
    FOR v_i IN 0 .. length(i_str)
    LOOP
      IF v_i = 0 THEN
        v_count1 := 1 - v_len; --第一个字符段
        v_count2 := instrb(i_str, i_split_str, 1, v_i + 1);
      ELSE
        v_count1 := instrb(i_str, i_split_str, 1, v_i);
        v_count2 := instrb(i_str, i_split_str, 1, v_i + 1);
      END IF;
      IF v_count2 = 0 THEN
        --若后面无分隔符,则判断标志是否添加剩余字符串
        IF i_split_flag = '1' AND v_count1 < lengthb(i_str) THEN
          v_node := substrb(i_str, v_count1 + v_len);
        ELSE
          EXIT;
        END IF;
      ELSE
        v_node := substrb(i_str, v_count1 + v_len, v_count2 - v_count1 - v_len);
      END IF;
      IF v_node IS NULL THEN
        v_node := '';
      END IF;
      v_strlist.extend();
      v_strlist(v_i + 1) := v_node;
      v_node := '';
      --若后面无分隔符,则退出循环
      IF v_count2 = 0 AND i_split_flag = '1' THEN
        EXIT;
      END IF; 
    END LOOP;
    RETURN v_strlist;
  END func_str_to_array;
  FUNCTION func_gen_seq(i_prefix IN VARCHAR2, --主键前缀
                        i_seq    IN VARCHAR2, --序列器名称
                        i_len    IN NUMBER := 7 --序列器填充长度
                        ) RETURN VARCHAR2 IS
    v_date   VARCHAR2(9) := to_char(SYSDATE, 'YYYY');
    v_result VARCHAR2(30);
    v_sql    VARCHAR2(4000);
    v_seqval VARCHAR2(20);
    v_prefix VARCHAR2(20);
    v_len    NUMBER;
  BEGIN
    v_sql := 'SELECT ' || i_seq || '.NEXTVAL FROM DUAL';
    v_len := i_len;
    EXECUTE IMMEDIATE v_sql
      INTO v_seqval;
    v_prefix := i_prefix;
    IF i_prefix = 'SA' THEN
      v_prefix := 'XXAQ';
    ELSIF i_prefix = 'SD' THEN
      v_prefix := 'BGYP';
    ELSIF i_prefix = 'SE' THEN
      v_prefix := 'WJYS';
    ELSIF i_prefix = 'SF' THEN
      v_prefix := 'CRZK';
    ELSIF i_prefix = 'SG' THEN
      v_prefix := 'ZHSP';
    ELSIF i_prefix = 'SH' THEN
      v_prefix := 'GWCK';
    ELSIF i_prefix = 'SI' THEN
      v_prefix := 'ZHSP';
    ELSIF i_prefix = 'SJ' THEN
      v_prefix := 'ZFWJ';
    ELSIF i_prefix = 'SK' THEN
      v_prefix := 'ZHSP';
    ELSIF i_prefix = 'SL' THEN
      v_prefix := 'DAJY';
    ELSIF i_prefix = 'SN' THEN
      --技术维护服务
      v_prefix := 'JSWH';
    ELSIF i_prefix = 'SO' THEN
      --设备管理
      v_prefix := 'SBSL';
    ELSIF i_prefix = 'SP' THEN
      --耗材申领管理---hcsl
      v_prefix := 'HCSL';
      --kfzx-guoq03 added
    ELSIF i_prefix = 'SR' THEN
      --参数审批管理
      v_prefix := 'CSGL';
    ELSIF i_prefix = 'SS' THEN
      --办公电话
      v_prefix := 'BGDH';
    ELSIF i_prefix = 'ST' THEN
      --保密系统
      v_prefix := 'BMXT';
    ELSIF i_prefix = 'STI' THEN
      --保密信息
      v_prefix := 'BMXX';
      v_date   := to_char(SYSDATE, 'YYYYMMDD');
    ELSIF i_prefix = 'SUO' THEN
      --加班申请
      v_prefix := 'JBSQ';
      v_date   := to_char(SYSDATE, 'YYYYMMDD');
    ELSIF i_prefix = 'SU' THEN
      --请假申请
      v_prefix := 'QJSQ';
      v_date   := to_char(SYSDATE, 'YYYYMMDD');
    ELSIF i_prefix = 'XJSQ' THEN
      --销假申请
      v_prefix := 'XJSQ';
      v_date   := to_char(SYSDATE, 'YYYYMMDD');
    ELSIF i_prefix = 'CCSQ' THEN
      --出差申请
      v_prefix := 'CCSQ';
      v_date   := to_char(SYSDATE, 'YYYYMMDD');
       v_date   := v_date || '0';
       v_len    := v_len - 1;
    ELSIF i_prefix = 'SW' THEN
      --合同申请
      v_prefix := 'HTSQ';
      v_date   := to_char(SYSDATE, 'YYYYMMDD');
    ELSIF i_prefix = 'SWI' THEN
      --合同管理
      v_prefix := 'HTGL';
      v_date   := to_char(SYSDATE, 'YYYYMMDD');
    ELSIF i_prefix = 'BXSQ' THEN
      --报销申请
      v_prefix := 'BXSQ';
      v_date   := to_char(SYSDATE, 'YYYYMMDD');
    ELSIF i_prefix = 'CCBX' THEN
      --出差报销
      v_prefix := 'CCBX';
      v_date   := to_char(SYSDATE, 'YYYYMMDD');
    ELSIF i_prefix = 'CCBL' THEN
      --出差报销补录
      v_prefix := 'CCBL';
      v_date   := to_char(SYSDATE, 'YYYYMMDD');
    ELSIF i_prefix = 'JKSQ' THEN
      --借款申请
      v_prefix := 'JKSQ';
      v_date   := to_char(SYSDATE, 'YYYYMMDD');
    ELSIF i_prefix = 'LX' THEN
      --信访事项
      v_prefix := 'LX';
      v_date   := to_char(SYSDATE, 'YYYYMMDD');
    ELSIF i_prefix = 'LF' THEN
      --信访事项
      v_prefix := 'LF';
      v_date   := to_char(SYSDATE, 'YYYYMMDD');
    ELSIF i_prefix = 'LD' THEN
      --信访事项
      v_prefix := 'LD';
      v_date   := to_char(SYSDATE, 'YYYYMMDD');
    ELSIF i_prefix = 'JA' THEN
      --信访积案
      v_prefix := 'JA';
      v_date   := to_char(SYSDATE, 'YYYYMMDD');
    ELSIF i_prefix = 'DB' THEN
      --信访督办
      v_prefix := 'DB';
      v_date   := to_char(SYSDATE, 'YYYYMMDD');
    ELSIF i_prefix = 'SZ' THEN
      --移动办公开销户申请
      v_prefix := 'YDSQ';
      v_date   := to_char(SYSDATE, 'YYYYMMDD');
    ELSIF i_prefix = 'AA' THEN
      --法审
      v_prefix := 'FLSC';
      v_date   := to_char(SYSDATE, 'YYYYMMDD');
    ELSIF i_prefix = 'XFHT' THEN
      --信访印章被退回历史
      v_prefix := 'XFHT';
      v_date   := to_char(SYSDATE, 'YYYYMMDD');
    ELSIF i_prefix = 'AB' THEN
      --声誉风险
      v_prefix := 'SYFX';
      v_date   := to_char(SYSDATE, 'YYYYMMDD');
    ELSIF i_prefix = 'YQ' THEN
      --声誉风险 舆情单号
      v_prefix := 'YQ';
      v_date   := to_char(SYSDATE, 'YYYY');
    ELSIF i_prefix = 'FXPC' THEN
      --声誉风险 风险排查
      v_prefix := 'FXPC';
      v_date   := to_char(SYSDATE, 'YYYYMMDD');
    ELSIF i_prefix = 'TZSQ' THEN
      --声誉风险 风险排查申请
      v_prefix := 'TZSQ';
      v_date   := to_char(SYSDATE, 'YYYYMMDD');
    ELSIF i_prefix = 'PCDB' THEN
      --声誉风险 风险排查申请待办
      v_prefix := 'PCDB';
      v_date   := to_char(SYSDATE, 'YYYYMMDD');
    ELSIF i_prefix = 'YJYL' THEN
      --声誉风险 应急演练
      v_prefix := 'YJYL';
      v_date   := to_char(SYSDATE, 'YYYYMMDD');
    ELSIF i_prefix = 'SYTZ' THEN
      --声誉风险 通知
      v_prefix := 'SYTZ';
      v_date   := to_char(SYSDATE, 'YYYYMMDD');
    ELSIF i_prefix = 'SYBG' THEN
      --声誉风险 报告
      v_prefix := 'SYBG';
      v_date   := to_char(SYSDATE, 'YYYYMMDD');
    ELSIF i_prefix = 'GP' THEN
      --群组号
      v_prefix := 'GP';
      v_date   := to_char(SYSDATE, 'YYYYMMDD');
    ELSIF i_prefix = 'SXOU' THEN
      --财务 系统外人员维护申请
      v_prefix := 'SXOU';
      v_date   := to_char(SYSDATE, 'YYYYMMDD');
    ELSIF i_prefix = 'SXCP' THEN
      --财务 跨部室项目维护申请
      v_prefix := 'SXCP';
      v_date   := to_char(SYSDATE, 'YYYYMMDD');
    ELSIF i_prefix = 'PROJECT' THEN
      --财务 跨部室项目编号
      v_prefix := 'PROJECT';
      v_date   := to_char(SYSDATE, 'YYYYMMDD');
    END IF;
    v_result := v_prefix || v_date || lpad(v_seqval, v_len, '0');
    RETURN v_result;
  EXCEPTION
    WHEN OTHERS THEN
      RETURN NULL;
  END func_gen_seq;
  
END pckg_dams_util;
//

CREATE OR REPLACE package PCKG_CTP_LG_PUBLIC is

  
  PROCEDURE LOG(proc_name IN VARCHAR2, info IN VARCHAR2);

end PCKG_CTP_LG_PUBLIC;
//
CREATE OR REPLACE package body PCKG_CTP_LG_PUBLIC is

  PROCEDURE log(proc_name IN VARCHAR2, info IN VARCHAR2) IS
    PRAGMA AUTONOMOUS_TRANSACTION;
    time_str VARCHAR2(100);
  BEGIN
    SELECT to_char(SYSDATE, 'mm - dd - yyyy hh24 :mi :ss')
      INTO time_str
      FROM dual;
    INSERT INTO ctp_proc_log VALUES (proc_name, time_str, info);
    COMMIT;
    RETURN;
  END;
end PCKG_CTP_LG_PUBLIC;
//
CREATE OR REPLACE PACKAGE PCKG_DAMS_LG_PERSONAL IS


  TYPE r_item_list IS RECORD(
    item_id        ctp_user_shortcut_menu.item_id%TYPE,
    item_name      ctp_item_nls.name%TYPE
    );

  TYPE ref_item_list IS REF CURSOR RETURN r_item_list;
  PROCEDURE PROC_CTP_LG_PERSONAL_DETAIL(
                i_userId         IN  VARCHAR2,
                i_Language       IN  VARCHAR2,
                o_retCode        OUT VARCHAR2,
                o_name           OUT VARCHAR2,
                o_desc           OUT VARCHAR2,
                o_email          OUT VARCHAR2,
                o_address        OUT VARCHAR2,
                o_postcode       OUT VARCHAR2,
                o_password       OUT VARCHAR2,
                o_phoneno        OUT VARCHAR2);

  PROCEDURE PROC_CTP_LG_UPDATE_PERSONAL(
                i_userId         IN  VARCHAR2,
                i_email          IN  VARCHAR2,
                i_userPostcode   IN  VARCHAR2,
                i_userPhone      IN  VARCHAR2,
                i_oldPassword    IN  VARCHAR2,
                i_password       IN  VARCHAR2,
                i_userName       IN  VARCHAR2,
                i_userDesc       IN  VARCHAR2,
                i_userAddress    IN  VARCHAR2,
                i_Language       IN  VARCHAR2,
                o_retCode        OUT VARCHAR2);


PROCEDURE PROC_CTP_LG_UP_PERSONAL_CUT(i_userid        IN VARCHAR2,
                                      i_Language        IN VARCHAR2,
																		  i_fileStream IN ctp_user_zoom.zoomcut%TYPE,
																		  o_retcode       OUT VARCHAR2);

  PROCEDURE PROC_CTP_LG_UP_PERSONAL_ZOOM(i_userid        IN VARCHAR2,
                                      i_Language        IN VARCHAR2,
																		  i_fileStream IN ctp_user_zoom.zoom%TYPE,
																		  o_retcode       OUT VARCHAR2);

  PROCEDURE PROC_CTP_LG_QRY_PERSONAL_ZOOM(i_userid        IN VARCHAR2,
                                       i_Language        IN VARCHAR2,
                                       o_retcode       OUT VARCHAR2,
																		   i_fileStream OUT ctp_user_zoom.zoom%TYPE,
                                       i_fileStream2 OUT ctp_user_zoom.zoomcut%TYPE
																		);

  PROCEDURE PROC_CTP_LG_QRY_SHORTCUT_MENU(
                i_userId         IN  VARCHAR2,
                i_defaultRoleId  IN  VARCHAR2,
                i_Language       IN  VARCHAR2,
                o_retCode        OUT VARCHAR2,
                o_itemList       OUT ref_item_list);

  PROCEDURE PROC_CTP_LG_UPSHORTCUTMENU(
                                      i_userid        IN VARCHAR2,
																		  i_defaultroleid IN VARCHAR2,
																		  i_itemid        IN VARCHAR2,
																		  o_retcode       OUT VARCHAR2);

  PROCEDURE PROC_CTP_LG_DEL_SHORTMENU(
                                      i_userid        IN VARCHAR2,
																		  i_defaultroleid IN VARCHAR2,
																		  o_retcode       OUT VARCHAR2);
END PCKG_DAMS_LG_PERSONAL;
//
CREATE OR REPLACE PACKAGE BODY PCKG_DAMS_LG_PERSONAL IS
 
  PROCEDURE PROC_CTP_LG_PERSONAL_DETAIL(
                i_userId         IN  VARCHAR2,  --用户编号
                i_Language       IN  VARCHAR2,  --语言编码
                o_retCode        OUT VARCHAR2,  --存储过程成功失败标志
                o_name           OUT VARCHAR2,  --用户名称
                o_desc           OUT VARCHAR2,  --用户描述
                o_email          OUT VARCHAR2,  --电子邮件
                o_address        OUT VARCHAR2,  --联系地址
                o_postcode       OUT VARCHAR2,  --邮政编码
                o_password       OUT VARCHAR2,  --密码
                o_phoneno        OUT VARCHAR2   --电话号码
                ) IS

  BEGIN
     o_retCode :='0';
      SELECT B.NAME,B.DESCRIPTION,A.EMAIL,B.ADDRESS,A.POSTCODE,A.PASSWORD,A.PHONE_NO
      into o_name,o_desc,o_email,o_address,o_postcode,o_password,o_phoneno
      FROM CTP_USER A, CTP_USER_NLS B
      WHERE A.ID=i_userId
      AND A.ID=B.ID
      AND B.LOCALE=i_Language;

  RETURN;
  EXCEPTION
    WHEN OTHERS THEN
      PCKG_CTP_LG_PUBLIC.log('ctp_update_menu()'
        ,SQLERRM(SQLCODE));
      o_retCode := '-1';
      RETURN;
  END;

 
  PROCEDURE PROC_CTP_LG_UPDATE_PERSONAL(
                i_userId         IN  VARCHAR2,
                i_email          IN  VARCHAR2,
                i_userPostcode   IN  VARCHAR2,
                i_userPhone      IN  VARCHAR2,
                i_oldPassword    IN  VARCHAR2,
                i_password       IN  VARCHAR2,
                i_userName       IN  VARCHAR2,
                i_userDesc       IN  VARCHAR2,
                i_userAddress    IN  VARCHAR2,
                i_Language       IN  VARCHAR2,
                o_retCode        OUT VARCHAR2) IS

  tmp_num   number;

  BEGIN
     o_retCode :='0';

     if(i_oldPassword IS NULL) then
        o_retCode :='3';
        update ctp_user A set
        A.EMAIL=i_email,
        A.POSTCODE=i_userPostcode,
        A.PHONE_NO=i_userPhone
        WHERE A.ID=i_userId;
      ELSE
           select count(*)
           into tmp_num
           from CTP_USER t
           where t.id=i_userId
           and t.password = i_oldPassword;

           if(tmp_num=0) THEN
              o_retCode :='2';
              RETURN;
           END IF;
        update ctp_user A set
        A.EMAIL=i_email,
        A.POSTCODE=i_userPostcode,
        A.PHONE_NO=i_userPhone,
        A.PASSWORD=i_password
        WHERE A.ID=i_userId;
      END IF;

      update ctp_user_nls A set
      A.NAME=i_userName,
      A.DESCRIPTION=i_userDesc,
      A.ADDRESS=i_userAddress
      WHERE A.ID=i_userId
      AND A.LOCALE=i_Language;

      commit;
  RETURN;
  EXCEPTION
    WHEN OTHERS THEN
      PCKG_CTP_LG_PUBLIC.log('PROC_CTP_LG_update_personal()'
        ,SQLERRM(SQLCODE));
      o_retCode := '-1';
      RETURN;
  END;

 
PROCEDURE PROC_CTP_LG_UP_PERSONAL_CUT(i_userid        IN VARCHAR2,
                                      i_Language        IN VARCHAR2,
																		  i_fileStream IN ctp_user_zoom.zoomcut%TYPE,
																		  o_retcode       OUT VARCHAR2) IS
  tmp_num   number;
BEGIN
	o_retcode := '0';
  SELECT COUNT(*) into tmp_num FROM ctp_user_zoom A WHERE A.ID=i_userId AND locale=i_Language;
  IF (tmp_num=0) THEN
     INSERT INTO ctp_user_zoom(id,zoomcut,locale) VALUES(i_userId,i_fileStream,i_Language);
  ELSE
     UPDATE ctp_user_zoom A SET A.ZOOMCUT=i_fileStream WHERE A.ID=i_userId AND A.locale=i_Language;
  END IF;
	RETURN;

EXCEPTION
	WHEN OTHERS THEN

		o_retCode := '-1';
		RETURN;
END;


PROCEDURE PROC_CTP_LG_UP_PERSONAL_ZOOM(i_userid        IN VARCHAR2,
                                      i_Language        IN VARCHAR2,
																		  i_fileStream IN ctp_user_zoom.zoom%TYPE,
																		  o_retcode       OUT VARCHAR2) IS

  tmp_num   number;
BEGIN
	o_retcode := '0';

  SELECT COUNT(*) into tmp_num FROM ctp_user_zoom A WHERE A.ID=i_userId AND locale=i_Language;
  IF (tmp_num=0) THEN
     INSERT INTO ctp_user_zoom(id,zoom,locale) VALUES(i_userId,i_fileStream,i_Language);
  ELSE
     UPDATE ctp_user_zoom A SET A.ZOOM=i_fileStream WHERE A.ID=i_userId AND A.locale=i_Language;
  END IF;
	RETURN;

EXCEPTION
	WHEN OTHERS THEN

		o_retCode := '-1';
		RETURN;
END;

  
PROCEDURE PROC_CTP_LG_QRY_PERSONAL_ZOOM(i_userid        IN VARCHAR2,
                                       i_Language        IN VARCHAR2,
                                       o_retcode       OUT VARCHAR2,
																		   i_fileStream OUT ctp_user_zoom.zoom%TYPE,
                                       i_fileStream2 OUT ctp_user_zoom.zoomcut%TYPE
																		) IS

BEGIN
	o_retcode := '0';

  select zoom,zoomcut into i_fileStream,i_fileStream2
    from ctp_user_zoom t
    where t.id=i_userid and t.locale=i_Language;

EXCEPTION
	WHEN OTHERS THEN

		o_retCode := '-1';
		RETURN;
END;


    PROCEDURE PROC_CTP_LG_QRY_SHORTCUT_MENU(
                i_userId         IN  VARCHAR2,
                i_defaultRoleId  IN  VARCHAR2,
                i_Language       IN  VARCHAR2,
                o_retCode        OUT VARCHAR2,
                o_itemList       OUT ref_item_list) IS

  BEGIN
     o_retCode :='0';

      open o_itemList for
      SELECT distinct a.item_id itemId, b.menu_NAME itemName
      FROM ctp_user_shortcut_menu a, dams_menu_nls b
      WHERE a.user_id = i_userId
      AND a.item_id = b.menu_id  AND b.locale=i_Language
      ORDER BY itemid ASC;

  RETURN;
  EXCEPTION
    WHEN OTHERS THEN
      --需要主动关闭游标
     if  o_itemList%isopen
     then
           close o_itemList;
     end if;

      PCKG_CTP_LG_PUBLIC.log('PROC_CTP_LG_query_shortcut_menu()'
        ,SQLERRM(SQLCODE));
      o_retCode := '-1';
      RETURN;
  END;


  PROCEDURE PROC_CTP_LG_UPSHORTCUTMENU(i_userid        IN VARCHAR2,
																		  i_defaultroleid IN VARCHAR2,
																		  i_itemid        IN VARCHAR2,
																		  o_retcode       OUT VARCHAR2) IS
	itemobj ctp_type_arraytype;
	v_num   VARCHAR2(2);
BEGIN
	o_retcode := '0';
	IF (i_defaultroleid IS NULL) THEN
					INSERT INTO ctp_user_shortcut_menu
					VALUES
						(i_userid, '', i_itemid);
	ELSE
					INSERT INTO ctp_user_shortcut_menu
					VALUES
						(i_userid, i_defaultroleid, i_itemid);

	UPDATE ctp_role_user_rel
	SET menuchg_flag = '1'
	WHERE user_id = i_userid AND role_id = i_defaultroleid;

  END IF;

EXCEPTION
	WHEN OTHERS THEN
		o_retCode := '-1';
		RETURN;
END;


  PROCEDURE PROC_CTP_LG_DEL_SHORTMENU(i_userid        IN VARCHAR2,
																		  i_defaultroleid IN VARCHAR2,
																		  o_retcode       OUT VARCHAR2) IS

BEGIN
	o_retcode := '0';

	IF (i_defaultroleid IS NULL) THEN
		DELETE FROM ctp_user_shortcut_menu
		 WHERE user_id = i_userid;

	ELSE
		DELETE FROM ctp_user_shortcut_menu
		 WHERE user_id = i_userid AND role_id = i_defaultroleid;

	END IF;
	RETURN;

EXCEPTION
	WHEN OTHERS THEN
		o_retCode := '-1';
		RETURN;
END;
END PCKG_DAMS_LG_PERSONAL;
//



CREATE OR REPLACE PACKAGE PACK_LOG AS
  START_STEP CONSTANT DB_LOG.STEP_NO%TYPE := '0';  --开始步点
  END_STEP   CONSTANT DB_LOG.STEP_NO%TYPE := '-1'; --结束步点

  START_MSG  CONSTANT VARCHAR2(10) := '开始！'; --开始信息
  END_MSG    CONSTANT VARCHAR2(10) := '结束！'; --结束信息

  DEBUG_LEVEL CONSTANT DB_LOG.LOG_LEVEL%TYPE := '2'; --调试情况
  INFO_LEVEL  CONSTANT DB_LOG.LOG_LEVEL%TYPE := '3'; --正常情况
  WARN_LEVEL  CONSTANT DB_LOG.LOG_LEVEL%TYPE := '4'; --数据错误,可预知错误
  ERR_LEVEL   CONSTANT DB_LOG.LOG_LEVEL%TYPE := '5'; --异常错误,未知错误

  PROCEDURE LOG(IN_PROCNAME IN DB_LOG.PROC_NAME%TYPE, -- 存储过程名
                IN_STEPNO   IN DB_LOG.STEP_NO%TYPE,   -- 步骤名
                IN_INFO     IN DB_LOG.INFO%TYPE,      -- 日志级别
                IN_LEVEL    IN DB_LOG.LOG_LEVEL%TYPE);-- 级别

  PROCEDURE DEBUG(IN_PROCNAME IN DB_LOG.PROC_NAME%TYPE, -- 存储过程名
                  IN_STEPNO   IN DB_LOG.STEP_NO%TYPE, -- 步骤名
                  IN_INFO     IN DB_LOG.INFO%TYPE); --日志级别

  PROCEDURE INFO(IN_PROCNAME IN DB_LOG.PROC_NAME%TYPE, -- 存储过程名
                 IN_STEPNO   IN DB_LOG.STEP_NO%TYPE, -- 步骤名
                 IN_INFO     IN DB_LOG.INFO%TYPE); --日志级别

  PROCEDURE WARN(IN_PROCNAME IN DB_LOG.PROC_NAME%TYPE, -- 存储过程名
                 IN_STEPNO   IN DB_LOG.STEP_NO%TYPE, -- 步骤名
                 IN_INFO     IN DB_LOG.INFO%TYPE); --日志级别

  PROCEDURE ERROR(IN_PROCNAME IN DB_LOG.PROC_NAME%TYPE, -- 存储过程名
                 IN_STEPNO   IN DB_LOG.STEP_NO%TYPE, -- 步骤名
                 IN_INFO     IN DB_LOG.INFO%TYPE); --日志级别

END;
//

CREATE OR REPLACE PACKAGE BODY PACK_LOG AS
  LOG_LEVEL_FILTER  DB_LOG.LOG_LEVEL%TYPE := '3';
  LOGGING_EXCEPTION EXCEPTION;
  
  PROCEDURE LOG(IN_PROCNAME IN DB_LOG.PROC_NAME%TYPE, -- 存储过程名
                IN_STEPNO   IN DB_LOG.STEP_NO%TYPE, -- 步骤名
                IN_INFO     IN DB_LOG.INFO%TYPE, --日志级别
                IN_LEVEL    IN DB_LOG.LOG_LEVEL%TYPE -- 级别
                ) IS
    PRAGMA AUTONOMOUS_TRANSACTION;
    V_ERRSTACK VARCHAR2(4000);
    V_ERROR_BACKTRACE VARCHAR2(4000);
    V_INFO VARCHAR2(4000);
  BEGIN
    IF (IN_LEVEL >= LOG_LEVEL_FILTER) THEN
      V_INFO := SUBSTRB(IN_INFO, 1, 4000);
      INSERT INTO DB_LOG
        (ID,
         PROC_NAME,
         INFO,
         LOG_LEVEL,
         TIME_STAMP,
         ERROR_BACKTRACE,
         ERR_STACK,
         STEP_NO,
         LOG_DATE)
      VALUES
        (LPAD(LOG_SEQ.NEXTVAL, 20, '0'),
         IN_PROCNAME,
         V_INFO,
         IN_LEVEL,
         TO_CHAR(SYSDATE, 'YYYYMMDDHH24MISS'),
         V_ERROR_BACKTRACE,
         V_ERRSTACK,
         IN_STEPNO,
         TO_CHAR(SYSDATE, 'YYYYMMDD'));
    END IF;
    COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      RAISE LOGGING_EXCEPTION;
  END;

  PROCEDURE DEBUG(IN_PROCNAME IN DB_LOG.PROC_NAME%TYPE, -- 存储过程名
                  IN_STEPNO   IN DB_LOG.STEP_NO%TYPE, -- 步骤名
                  IN_INFO     IN DB_LOG.INFO%TYPE --日志级别
                  ) IS
  BEGIN
    PACK_LOG.LOG(IN_PROCNAME,
                 IN_STEPNO,
                 IN_INFO,
                 PACK_LOG.DEBUG_LEVEL);
  END;

  PROCEDURE INFO(IN_PROCNAME IN DB_LOG.PROC_NAME%TYPE, -- 存储过程名
                 IN_STEPNO   IN DB_LOG.STEP_NO%TYPE, -- 步骤名
                 IN_INFO     IN DB_LOG.INFO%TYPE --日志级别
                 ) IS
  BEGIN
    PACK_LOG.LOG(IN_PROCNAME,
                 IN_STEPNO,
                 IN_INFO,
                 PACK_LOG.INFO_LEVEL);
  END;

  PROCEDURE WARN(IN_PROCNAME IN DB_LOG.PROC_NAME%TYPE, -- 存储过程名
                 IN_STEPNO   IN DB_LOG.STEP_NO%TYPE, -- 步骤名
                 IN_INFO     IN DB_LOG.INFO%TYPE --日志级别
                 ) IS
  BEGIN
    PACK_LOG.LOG(IN_PROCNAME,
                 IN_STEPNO,
                 IN_INFO,
                 PACK_LOG.WARN_LEVEL);
  END;

  PROCEDURE ERROR(IN_PROCNAME IN DB_LOG.PROC_NAME%TYPE, -- 存储过程名
                 IN_STEPNO   IN DB_LOG.STEP_NO%TYPE, -- 步骤名
                 IN_INFO     IN DB_LOG.INFO%TYPE --日志级别
                 ) IS
  BEGIN
    PACK_LOG.LOG(IN_PROCNAME,
                 IN_STEPNO,
                 IN_INFO,
                 PACK_LOG.ERR_LEVEL);
  END;
END PACK_LOG;
//



CREATE OR REPLACE PACKAGE pckg_dams_branch_property IS

  TYPE ret_list IS REF CURSOR;
  
  PROCEDURE proc_property_list(i_branch_id   IN VARCHAR2, --机构编号
                               i_user_id     IN VARCHAR2, --用户ID
                               o_flag        OUT VARCHAR2, --返回标志
                               o_branch_list OUT ret_list --机构列表
                               );
 
  PROCEDURE proc_property_upd(i_branch_id IN VARCHAR2, --机构编号
                              i_user_id   IN VARCHAR2, --用户ID
                              i_is_inner  IN VARCHAR2, --是否内设
                              o_flag      OUT VARCHAR2, --返回标志
                              o_msg       OUT VARCHAR2 --结果
                              );
 
  PROCEDURE proc_lv2subbranch_upd(i_branch_id       IN VARCHAR2, --机构编号
                                  i_user_id         IN VARCHAR2, --用户ID
                                  i_is_lv2subbranch IN VARCHAR2, --是否内设
                                  o_flag            OUT VARCHAR2, --返回标志
                                  o_msg             OUT VARCHAR2 --结果
                                  );

 
  PROCEDURE proc_gw_man_upd(i_branch_id    IN VARCHAR2, --机构编号
                            i_user_id      IN VARCHAR2, --用户ID
                            i_gw_man_type  IN VARCHAR2, --公文管理类型
                            i_dept_stru_id IN VARCHAR2,
                            o_flag         OUT VARCHAR2, --返回标志
                            o_msg          OUT VARCHAR2 --结果
                            );

 
  PROCEDURE proc_upd_gw_man_value(i_stru_id     IN VARCHAR2,
                                  i_gw_man_type IN VARCHAR2);

  PROCEDURE proc_get_gw_man_choice_list(i_stru_id     IN VARCHAR2,
                                        o_flag        OUT VARCHAR2, --返回标志
                                        o_choice_list OUT ret_list --数据列表
                                        );

  PROCEDURE proc_dept_stru_upd(i_branch_id    IN VARCHAR2, --机构编号
                               i_user_id      IN VARCHAR2, --用户ID
                               i_dept_stru_id IN VARCHAR2,
                               o_flag         OUT VARCHAR2, --返回标志
                               o_msg          OUT VARCHAR2 --结果
                               );

 
  PROCEDURE proc_dept_branch_list(i_branch_id    IN VARCHAR2, --机构编号
                                  i_user_id      IN VARCHAR2, --用户ID
                                  o_flag         OUT VARCHAR2, --返回标志
                                  o_branch_list  OUT ret_list, --机构列表
                                  o_dept_stru_id OUT VARCHAR2);
 
  PROCEDURE proc_branch_sepc_upd(i_branch_id IN VARCHAR2, --机构编号
                                 i_user_id   IN VARCHAR2, --用户ID
                                 i_is_inner  IN VARCHAR2, --是否内设
                                 i_spec_prop IN VARCHAR2, --机构属性
                                 o_flag      OUT VARCHAR2, --返回标志
                                 o_msg       OUT VARCHAR2 --结果
                                 );
 
  PROCEDURE proc_branch_lh_sepc_upd(i_branch_id IN VARCHAR2, --机构编号
                                    i_user_id   IN VARCHAR2, --用户ID
                                    i_is_inner  IN VARCHAR2, --是否内设
                                    i_spec_prop IN VARCHAR2, --机构属性
                                    o_flag      OUT VARCHAR2, --返回标志
                                    o_msg       OUT VARCHAR2 --结果
                                    );
  FUNCTION is_foreign_lvl2(i_branch_id IN VARCHAR2 --所属处室ID
                           ) RETURN NUMBER;
END pckg_dams_branch_property;
//

CREATE OR REPLACE PACKAGE BODY pckg_dams_branch_property IS

  PROCEDURE proc_property_list(i_branch_id   IN VARCHAR2, --机构编号
                               i_user_id     IN VARCHAR2, --用户ID
                               o_flag        OUT VARCHAR2, --返回标志
                               o_branch_list OUT ret_list --机构列表
                               ) IS
    v_proc_name db_log.proc_name%TYPE := 'PCKG_DAMS_BRANCH_PROPERTY.PROC_PROPERTY_LIST'; --存储过程名称
    v_step_num  db_log.step_no%TYPE; --步骤号
    v_param     db_log.info%TYPE := i_branch_id || ' ' || i_user_id; --参数

  BEGIN
   
    v_step_num := '#00';

   
    OPEN o_branch_list FOR
      SELECT p.branchid,
             p.branchname,
             p.isinnerdept,
             p.stru_lv strulv,
             p.isoriginaldept,
             p.gw_manage_type,
             p.stru_grade,
             p.up_stru_grade,
             p.lv2_sub_branch,
             p.spec_dept,
             p.sycnt,
             p.lhmicnt,
             p.lhiscnt,
             p.gjywqzybcnt
        FROM (SELECT t.stru_id branchid,
                     t.stru_sname branchname,
                     t.manage_type isinnerdept,
                     t.stru_lv, --层级用于判断是否出现“是否内设部室”,仅营业部可以修改
                     (SELECT COUNT(1)
                        FROM dams_branch_relation b1, dams_branch b2
                       WHERE b1.bnch_id = t.stru_id
                         AND b2.stru_id = b1.up_bnch_id
                         AND b2.stru_sign = '2'
                         AND b2.stru_id <> t.stru_id
                         AND rownum = 1) isoriginaldept, --是原始的机构本部下的部室处室
                     decode(t.stru_id, i_branch_id, 999,
                            decode(t.stru_sign, 2, 998, t.sort)) ord, --父机构放在最上面
                     t.sort,
                     t.stru_id,
                     t.gw_manage_type,
                     t.stru_grade,
                     up.stru_grade up_stru_grade,
                     decode(c.reserve1, NULL, '0', c.reserve1) lv2_sub_branch,
                     decode(sp.spec_prop, NULL, '0', sp.spec_prop) spec_dept, --信访系统信访部室 00001 为信访部室
                     (SELECT COUNT(1)
                        FROM dams_branch b3
                        JOIN dams_branch_spec_prop cp ON b3.stru_id = cp.stru_id
                                                     AND cp.spec_prop = '00001'
                       WHERE (b3.sup_stru = i_branch_id AND
                             b3.stru_state IN ('1', '2') AND
                             b3.stru_sign IN ('1', '2', '3', '4', '5', '8') OR
                             b3.stru_id = i_branch_id)) sycnt, --信访部室个数
                     (SELECT COUNT(1)
                        FROM dams_branch b3
                        JOIN dams_branch_spec_prop cp ON b3.stru_id = cp.stru_id
                                                     AND cp.spec_prop = '00002'
                       WHERE (b3.sup_stru = i_branch_id AND
                             b3.stru_state IN ('1', '2') AND
                             b3.stru_sign IN ('1', '2', '3', '4', '5', '8') OR
                             b3.stru_id = i_branch_id)) lhmicnt, --管信部室个数
                     (SELECT COUNT(1)
                        FROM dams_branch b3
                        JOIN dams_branch_spec_prop cp ON b3.stru_id = cp.stru_id
                                                     AND cp.spec_prop = '00003'
                       WHERE (b3.sup_stru = i_branch_id AND
                             b3.stru_state IN ('1', '2') AND
                             b3.stru_sign IN ('1', '2', '3', '4', '5', '8') OR
                             b3.stru_id = i_branch_id)) lhiscnt, --信息科技部室个数
                     (SELECT COUNT(1)
                        FROM dams_branch b3
                        JOIN dams_branch_spec_prop cp ON b3.stru_id = cp.stru_id
                                                     AND cp.spec_prop = '00004'
                       WHERE (b3.sup_stru = i_branch_id AND
                             b3.stru_state IN ('1', '2') AND
                             b3.stru_sign IN ('1', '2', '3', '4', '5', '8') OR
                             b3.stru_id = i_branch_id)) gjywqzybcnt --国际业务签字样本个数
                FROM dams_branch t
                LEFT JOIN dams_branch_cfg c ON t.stru_id = c.stru_id
                JOIN dams_branch up ON t.sup_stru = up.stru_id
                LEFT JOIN  (select sp.stru_id, listagg(sp.spec_prop,',') within group(order by sp.stru_id) spec_prop
                                   from dams_branch t left join dams_branch_spec_prop sp
                                        ON t.stru_id = sp.stru_id AND sp.spec_prop IN
                                                        ('00001', '00002', '00003', '00004')
                                          --信访部室、管信部、信息科技部、国际业务签字样本
                                          WHERE (t.sup_stru = i_branch_id AND t.stru_state IN ('1', '2') AND
                                           t.stru_sign IN ('1', '2', '3', '4', '5', '8') OR
                                            t.stru_id = i_branch_id)
                                             group by sp.stru_id) sp ON t.stru_id = sp.stru_id
               WHERE (t.sup_stru = i_branch_id AND t.stru_state IN ('1', '2') AND
                     t.stru_sign IN ('1', '2', '3', '4', '5', '8') OR
                     t.stru_id = i_branch_id)) p
       ORDER BY p.ord DESC NULLS LAST,
	   p.branchname;
    o_flag := '0';
    
  EXCEPTION
    WHEN OTHERS THEN
      --需要主动关闭游标
      IF o_branch_list%ISOPEN THEN
        CLOSE o_branch_list;
      END IF;
      --系统异常,返回错误代码-1
      pack_log.error(v_proc_name, v_step_num, '异常！' || v_param);
      o_flag := '-1';

      ROLLBACK;
      RETURN;
  END proc_property_list;

  PROCEDURE proc_property_upd(i_branch_id IN VARCHAR2, --机构编号
                              i_user_id   IN VARCHAR2, --用户ID
                              i_is_inner  IN VARCHAR2, --是否内设
                              o_flag      OUT VARCHAR2, --返回标志
                              o_msg       OUT VARCHAR2 --结果
                              ) IS
    v_proc_name   db_log.proc_name%TYPE := 'PCKG_DAMS_BRANCH_PROPERTY.PROC_PROPERTY_UPD'; --存储过程名称
    v_step_num    db_log.step_no%TYPE; --步骤号
    v_param       db_log.info%TYPE := i_branch_id || ' ' || i_user_id || ' ' ||
                                      i_is_inner; --参数
    v_end_time    VARCHAR2(14);
    v_branch_name dams_branch.stru_sname%TYPE;

  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step,
                  pack_log.start_msg || v_param);

    v_step_num := '1';
   
    UPDATE dams_branch t
       SET t.manage_type = i_is_inner
     WHERE t.stru_id = i_branch_id;
    o_flag := '0';
    o_msg  := '设置已成功';
   
    v_branch_name := pckg_dams_util.func_get_stru_name(i_branch_id);
    v_end_time    := to_char(SYSDATE, 'yyyyMMddHH24miss');
    pckg_dams_util.proc_dams_logger('BRANCH_OPERATE', '017', v_end_time,
                                    i_user_id,
                                    '机构属性设置（是否内设部室）:' || v_branch_name || '（' ||
                                     i_branch_id || '）' || '设置为“' || CASE
                                     i_is_inner WHEN '1' THEN '是' WHEN '0' THEN '否' ELSE
                                     i_is_inner END || '”');
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
     
      pack_log.error(v_proc_name, v_step_num, '异常！' || v_param);
      o_flag := '-1';
      o_msg  := '设置失败';
      ROLLBACK;
      RETURN;
  END proc_property_upd;


  PROCEDURE proc_lv2subbranch_upd(i_branch_id       IN VARCHAR2, --机构编号
                                  i_user_id         IN VARCHAR2, --用户ID
                                  i_is_lv2subbranch IN VARCHAR2, --是否内设
                                  o_flag            OUT VARCHAR2, --返回标志
                                  o_msg             OUT VARCHAR2 --结果
                                  ) IS
    v_proc_name   db_log.proc_name%TYPE := 'PCKG_DAMS_BRANCH_PROPERTY.proc_lv2subbranch_upd'; --存储过程名称
    v_step_num    db_log.step_no%TYPE; --步骤号
    v_param       db_log.info%TYPE := i_branch_id || ' ' || i_user_id || ' ' ||
                                      i_is_lv2subbranch; --参数
    v_end_time    VARCHAR2(14);
    v_branch_name dams_branch.stru_sname%TYPE;

  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step,
                  pack_log.start_msg || v_param);

    v_step_num := '1';

    IF (i_is_lv2subbranch = '1') THEN
      --设置为‘是’
      MERGE INTO dams_branch_cfg t
      USING (SELECT i_is_lv2subbranch lv2subbranch FROM dual) tmp
      ON (t.stru_id = i_branch_id)
      WHEN MATCHED THEN
        UPDATE SET t.reserve1 = i_is_lv2subbranch
      WHEN NOT MATCHED THEN
        INSERT (t.stru_id, t.reserve1) VALUES (i_branch_id, i_is_lv2subbranch);
    ELSE
      --设置为‘否’
      UPDATE dams_branch_cfg t
         SET t.reserve1 = ''
       WHERE t.stru_id = i_branch_id;
      DELETE FROM dams_branch_cfg t
       WHERE t.dept_stru_id IS NULL
         AND t.reserve2 IS NULL
         AND t.reserve3 IS NULL;
    END IF;

    MERGE INTO dams_user_daily_change t
    USING (SELECT u.ssic_id
             FROM dams_user u, dams_branch_relation br
            WHERE u.stru_id = br.bnch_id
              AND br.up_bnch_id = i_branch_id
              AND u.duty_lv IN ('A000000000000000026', 'A000000000000000027')
           UNION
           SELECT t.ssic_id
             FROM dams_user_branch_rel t, dams_user u, dams_branch_relation br
            WHERE t.stru_id = br.bnch_id
              AND br.up_bnch_id = i_branch_id
              AND t.ssic_id = u.ssic_id
              AND u.duty_lv IN ('A000000000000000026', 'A000000000000000027')) tmp
    ON (tmp.ssic_id = t.ssic_id AND t.update_time = to_char(SYSDATE, 'YYYYMMDD'))
    WHEN NOT MATCHED THEN
      INSERT
        (ssic_id, update_time, tab_src)
      VALUES
        (tmp.ssic_id, to_char(SYSDATE, 'YYYYMMDD'), 128)
    WHEN MATCHED THEN
      UPDATE SET t.tab_src = 128;

    o_flag := '0';
    o_msg  := '设置已成功';
   
    v_branch_name := pckg_dams_util.func_get_stru_name(i_branch_id);
    v_end_time    := to_char(SYSDATE, 'yyyyMMddHH24miss');
    pckg_dams_util.proc_dams_logger('BRANCH_OPERATE', '017', v_end_time,
                                    i_user_id,
                                    '机构属性设置（是否二级支行）:' || v_branch_name || '（' ||
                                     i_branch_id || '）' || '设置为“' || CASE
                                     i_is_lv2subbranch WHEN '1' THEN '是' WHEN '0' THEN '否' ELSE
                                     i_is_lv2subbranch END || '”');
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
     
      pack_log.error(v_proc_name, v_step_num, '异常！' || v_param);
      o_flag := '-1';
      o_msg  := '设置失败';
      ROLLBACK;
      RETURN;
  END proc_lv2subbranch_upd;


  PROCEDURE proc_gw_man_upd(i_branch_id    IN VARCHAR2, --机构编号
                            i_user_id      IN VARCHAR2, --用户ID
                            i_gw_man_type  IN VARCHAR2, --是否内设
                            i_dept_stru_id IN VARCHAR2,
                            o_flag         OUT VARCHAR2, --返回标志
                            o_msg          OUT VARCHAR2 --结果
                            ) IS
    v_proc_name         db_log.proc_name%TYPE := 'PCKG_DAMS_BRANCH_PROPERTY.PROC_GW_MAN_UPD'; --存储过程名称
    v_step_num          db_log.step_no%TYPE; --步骤号
    v_param             db_log.info%TYPE := i_branch_id || ' ' || i_user_id || ' ' ||
                                            i_gw_man_type || ' ' ||
                                            i_dept_stru_id; --参数
    v_end_time          VARCHAR2(14);
    v_branch_name       dams_branch.stru_sname%TYPE;
    v_stru_grade        dams_branch.stru_grade%TYPE;
    v_gw_manage_type    dams_branch.gw_manage_type%TYPE;
    v_man_grade         dams_branch.man_grade%TYPE;
    v_stru_lv           dams_branch.stru_lv%TYPE;
    v_up_stru_grade     dams_branch.stru_grade%TYPE;
    v_up_man_grade      dams_branch.man_grade%TYPE;
    v_up_stru_lv        dams_branch.stru_lv%TYPE;
    v_up_gw_manage_type dams_branch.gw_manage_type%TYPE;
    v_is_stru_sign2     dams_branch.stru_sign%TYPE; --2表示直接上级本部
    v_dept_stru         dams_branch_cfg.dept_stru_id%TYPE;
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step,
                  pack_log.start_msg || v_param);
    o_flag := '0';

    v_step_num := '0'; --获取当前机构的基本信息
    SELECT t.stru_sname, t.stru_grade, t.gw_manage_type, t.man_grade, t.stru_lv
      INTO v_branch_name,
           v_stru_grade,
           v_gw_manage_type,
           v_man_grade,
           v_stru_lv
      FROM dams_branch t
     WHERE t.stru_id = i_branch_id;
    BEGIN
      SELECT t.dept_stru_id
        INTO v_dept_stru
        FROM dams_branch_cfg t
       WHERE t.stru_id = i_branch_id;
    EXCEPTION
      WHEN OTHERS THEN
        NULL;
    END;

    IF (i_gw_man_type = v_gw_manage_type AND
       ((i_dept_stru_id IS NULL AND v_dept_stru IS NULL) OR
       (i_dept_stru_id = v_dept_stru))) THEN
      o_msg := '没有变化。';
      RETURN;
    END IF;

    UPDATE dams_branch_cfg t
       SET t.dept_stru_id = ''
     WHERE t.stru_id = i_branch_id;
    DELETE FROM dams_branch_cfg t
     WHERE t.stru_id = i_branch_id
       AND t.reserve1 IS NULL
       AND t.reserve2 IS NULL
       AND t.reserve3 IS NULL;

    --二级行一个层次更新 都要更新下属
    IF (v_man_grade = '2') THEN
      --清空branch表中下辖机构的gw
      UPDATE dams_branch t
         SET t.gw_manage_type = ''
       WHERE t.stru_id IN (SELECT r.bnch_id
                             FROM dams_branch_relation r
                            WHERE r.up_bnch_id = i_branch_id
                              AND r.up_level <> '1')
         AND t.gw_manage_type IS NOT NULL;
      --清空branch_relation表中下辖机构的gw
      UPDATE dams_branch_relation t
         SET t.gw_manage_type = ''
       WHERE t.bnch_id IN (SELECT r.bnch_id
                             FROM dams_branch_relation r
                            WHERE r.up_bnch_id = i_branch_id
                              AND r.up_level <> '1')
         AND t.gw_manage_type IS NOT NULL;
      UPDATE dams_branch_relation t
         SET t.up_gw_manage_type = ''
       WHERE t.up_bnch_id IN (SELECT r.bnch_id
                                FROM dams_branch_relation r
                               WHERE r.up_bnch_id = i_branch_id
                                 AND r.up_level <> '1')
         AND t.up_gw_manage_type IS NOT NULL;
    
      UPDATE dams_branch_cfg t
         SET t.dept_stru_id = ''
       WHERE t.stru_id IN (SELECT r.bnch_id
                             FROM dams_branch_relation r
                            WHERE r.up_bnch_id = i_branch_id
                              AND r.up_level <> '1')
         AND t.dept_stru_id IS NOT NULL;
      DELETE FROM dams_branch_cfg t
       WHERE t.stru_id IN (SELECT r.bnch_id
                             FROM dams_branch_relation r
                            WHERE r.up_bnch_id = i_branch_id
                              AND r.up_level <> '1')
         AND t.reserve1 IS NULL
         AND t.reserve2 IS NULL
         AND t.reserve3 IS NULL;
    END IF;
    BEGIN
      SELECT tt.up_bnch_grade,
             tt.up_bnch_man_grade,
             tt.up_bnch_lv,
             tt.up_gw_manage_type
        INTO v_up_stru_grade, v_up_man_grade, v_up_stru_lv, v_up_gw_manage_type
        FROM (SELECT t.up_bnch_id,
                     t.up_bnch_lv,
                     t.up_bnch_sign,
                     t.up_bnch_grade,
                     t.up_bnch_man_grade,
                     t.up_gw_manage_type
                FROM dams_branch_relation t
               WHERE t.bnch_id = i_branch_id
                 AND t.up_level <> '1'
                 AND t.up_bnch_grade IN ('2', '3', '4')
               ORDER BY t.up_level) tt
       WHERE rownum = 1;
    EXCEPTION
      WHEN OTHERS THEN
        NULL;
    END;
    BEGIN
      SELECT up.stru_sign
        INTO v_is_stru_sign2
        FROM dams_branch t, dams_branch up
       WHERE t.sup_stru = up.stru_id;
    EXCEPTION
      WHEN OTHERS THEN
        NULL;
    END;
 
    IF (i_gw_man_type = '5') THEN
      UPDATE dams_branch t
         SET t.gw_manage_type = ''
       WHERE t.stru_id IN (SELECT c.stru_id
                             FROM dams_branch_cfg c
                            WHERE c.dept_stru_id = i_branch_id)
         AND t.gw_manage_type IS NOT NULL;
      UPDATE dams_branch_relation t
         SET t.gw_manage_type = ''
       WHERE t.bnch_id IN (SELECT c.stru_id
                             FROM dams_branch_cfg c
                            WHERE c.dept_stru_id = i_branch_id)
         AND t.gw_manage_type IS NOT NULL;
      UPDATE dams_branch_relation t
         SET t.up_gw_manage_type = ''
       WHERE t.up_bnch_id IN
             (SELECT c.stru_id
                FROM dams_branch_cfg c
               WHERE c.dept_stru_id = i_branch_id)
         AND t.up_gw_manage_type IS NOT NULL;
     
      UPDATE dams_branch_cfg t
         SET t.dept_stru_id = ''
       WHERE t.stru_id = i_branch_id;
      DELETE FROM dams_branch_cfg t
       WHERE t.stru_id = i_branch_id
         AND t.reserve1 IS NULL
         AND t.reserve2 IS NULL
         AND t.reserve3 IS NULL;
    END IF;
    IF (v_gw_manage_type = '4') THEN
      UPDATE dams_branch t
         SET t.gw_manage_type = ''
       WHERE t.stru_id IN (SELECT c.stru_id
                             FROM dams_branch_cfg c
                            WHERE c.dept_stru_id = i_branch_id)
         AND t.gw_manage_type IS NOT NULL;
      UPDATE dams_branch_relation t
         SET t.gw_manage_type = ''
       WHERE t.bnch_id IN (SELECT c.stru_id
                             FROM dams_branch_cfg c
                            WHERE c.dept_stru_id = i_branch_id)
         AND t.gw_manage_type IS NOT NULL;
      UPDATE dams_branch_relation t
         SET t.up_gw_manage_type = ''
       WHERE t.up_bnch_id IN
             (SELECT c.stru_id
                FROM dams_branch_cfg c
               WHERE c.dept_stru_id = i_branch_id)
         AND t.up_gw_manage_type IS NOT NULL;
     
      UPDATE dams_branch_cfg t
         SET t.dept_stru_id = ''
       WHERE t.stru_id = i_branch_id;
      DELETE FROM dams_branch_cfg t
       WHERE t.stru_id = i_branch_id
         AND t.reserve1 IS NULL
         AND t.reserve2 IS NULL
         AND t.reserve3 IS NULL;
    END IF;
   
    IF (i_gw_man_type = '5') THEN
      IF (i_dept_stru_id IS NOT NULL) THEN
        proc_dept_stru_upd(i_branch_id, i_user_id, i_dept_stru_id, o_flag,
                           o_msg);
      ELSE
        proc_dept_stru_upd(i_branch_id, i_user_id, '-1', o_flag, o_msg);
      END IF;
      IF (o_flag != '0') THEN
        pack_log.error(v_proc_name, v_step_num,
                       '异常！' || v_param || ',' || o_msg);
        ROLLBACK;
        RETURN;
      END IF;
    END IF;

   
    v_step_num := '3';
    proc_upd_gw_man_value(i_branch_id, i_gw_man_type);

    v_step_num := '4';
    MERGE INTO dams_user_daily_change t
    USING (SELECT DISTINCT u.ssic_id
             FROM dams_user u, dams_branch_relation r
            WHERE u.stru_id = r.bnch_id
              AND u.source_type = 'HRM'
              AND r.up_bnch_id IN (i_branch_id)
           UNION
           SELECT DISTINCT u.ssic_id
             FROM dams_user_branch_rel u, dams_branch_relation r
            WHERE u.stru_id = r.bnch_id
              AND r.up_bnch_id IN (i_branch_id)) tmp
    ON (t.ssic_id = tmp.ssic_id AND t.update_time = to_char(SYSDATE, 'YYYYMMDD'))
    WHEN MATCHED THEN
      UPDATE
         SET t.tab_src = nvl(t.tab_src, 0) + 1024 -
                         bitand(nvl(t.tab_src, 0), 1024)
    WHEN NOT MATCHED THEN
      INSERT
        (ssic_id, update_time, tab_src)
      VALUES
        (tmp.ssic_id, to_char(SYSDATE, 'YYYYMMDD'), 1024);

    IF (o_flag = '0') THEN
      o_msg := '设置已成功';
    ELSE
      o_msg := '设置失败';
      ROLLBACK;
      RETURN;
    END IF;

    v_end_time := to_char(SYSDATE, 'yyyyMMddHH24miss');
    pckg_dams_util.proc_dams_logger('BRANCH_OPERATE', '017', v_end_time,
                                    i_user_id,
                                    '机构属性设置（机构管理类型）:' || v_branch_name || '（' ||
                                     i_branch_id || '）' || '设置为“' ||
                                     pckg_dams_util.func_decode('GW_MANAGE_TYPE',
                                                                i_gw_man_type,
                                                                'GW') || '”' || CASE WHEN
                                     i_gw_man_type = '5' THEN
                                     '（' ||
                                     pckg_dams_util.func_get_stru_name(i_dept_stru_id) || '）' ELSE '' END);
  EXCEPTION
    WHEN OTHERS THEN

      pack_log.error(v_proc_name, v_step_num, '异常！' || v_param);
      o_flag := '-1';
      o_msg  := '设置失败';
      ROLLBACK;
      RETURN;
  END proc_gw_man_upd;

  
  PROCEDURE proc_upd_gw_man_value(i_stru_id     IN VARCHAR2,
                                  i_gw_man_type IN VARCHAR2) IS
    v_proc_name db_log.proc_name%TYPE := 'PCKG_DAMS_BRANCH_PROPERTY.proc_upd_gw_man_value'; --存储过程名称
    v_step      db_log.step_no%TYPE;
    v_param     VARCHAR2(300) := 'i_stru_id:' || i_stru_id || ' i_gw_man_type:' ||
                                 i_gw_man_type;
  BEGIN
    UPDATE dams_branch t
       SET t.gw_manage_type = i_gw_man_type
     WHERE t.stru_id = i_stru_id
       AND t.stru_state IN ('1', '2');

    UPDATE dams_branch_relation t
       SET t.gw_manage_type = i_gw_man_type
     WHERE t.bnch_id = i_stru_id;

    UPDATE dams_branch_relation t
       SET t.up_gw_manage_type = i_gw_man_type
     WHERE t.up_bnch_id = i_stru_id;

  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, '', '异常！' || v_param);
      RAISE;
  END proc_upd_gw_man_value;

 
  PROCEDURE proc_get_gw_man_choice_list(i_stru_id     IN VARCHAR2,
                                        o_flag        OUT VARCHAR2, --返回标志
                                        o_choice_list OUT ret_list --数据列表
                                        ) IS
    v_proc_name              db_log.proc_name%TYPE := 'PCKG_DAMS_BRANCH_PROPERTY.func_get_gw_man_choice_list'; --存储过程名称
    v_param                  db_log.info%TYPE := i_stru_id; --参数
    v_sql                    VARCHAR2(10000);
    v_stru_grade             dams_branch.stru_grade%TYPE;
    v_stru_lv                dams_branch.stru_lv%TYPE;
    v_man_grade              dams_branch.man_grade%TYPE;
    v_stru_sign              dams_branch.stru_sign%TYPE;
    v_stru_sname             dams_branch.stru_sname%TYPE;
    v_gw_manage_type         dams_branch.gw_manage_type%TYPE;
    v_up_bnch_grade          dams_branch.stru_grade%TYPE;
    v_up_bnch_lv             dams_branch.stru_lv%TYPE;
    v_up_bnch_sign           dams_branch.stru_sign%TYPE;
    v_up_bnch_man_grade      dams_branch.man_grade%TYPE;
    v_up_bnch_gw_manage_type dams_branch.gw_manage_type%TYPE;
    v_ret VARCHAR2(100);    
    v_up_stru_sname          dams_branch.stru_sname%TYPE;
    v_up_stru_sign           dams_branch.stru_sign%TYPE;
    v_up_stru_grade          dams_branch.stru_grade%TYPE;
    v_up_stru_gw_manage_type dams_branch.gw_manage_type%TYPE;
  BEGIN
    o_flag := '0';

    SELECT t.stru_grade,
           t.stru_lv,
           t.man_grade,
           t.gw_manage_type,
           t.stru_sign,
           upper(t.stru_sname)
      INTO v_stru_grade,
           v_stru_lv,
           v_man_grade,
           v_gw_manage_type,
           v_stru_sign,
           v_stru_sname
      FROM dams_branch t
     WHERE t.stru_id = i_stru_id;
    BEGIN
      SELECT up.stru_sign,
             up.stru_grade,
             up.gw_manage_type,
             upper(up.stru_sname)
        INTO v_up_stru_sign,
             v_up_stru_grade,
             v_up_stru_gw_manage_type,
             v_up_stru_sname
        FROM dams_branch t, dams_branch up
       WHERE t.sup_stru = up.stru_id
         AND t.stru_id = i_stru_id;
    EXCEPTION
      WHEN OTHERS THEN
        NULL;
    END;
    BEGIN
      IF (v_stru_sign <> '8') THEN
        SELECT tt.up_bnch_grade,
               tt.up_bnch_man_grade,
               tt.up_bnch_sign,
               tt.up_bnch_lv,
               tt.up_gw_manage_type
          INTO v_up_bnch_grade,
               v_up_bnch_man_grade,
               v_up_bnch_sign,
               v_up_bnch_lv,
               v_up_bnch_gw_manage_type
          FROM (SELECT t.up_bnch_id,
                       t.up_bnch_lv,
                       t.up_bnch_sign,
                       t.up_bnch_grade,
                       t.up_bnch_man_grade,
                       t.up_gw_manage_type
                  FROM dams_branch_relation t
                 WHERE t.bnch_id = i_stru_id
                   AND t.up_level <> '1'
                   AND (t.up_bnch_sign = '4' OR
                       t.up_bnch_grade IN ('2', '3', '4'))
                 ORDER BY t.up_level) tt
         WHERE rownum = 1;
      ELSE
        BEGIN
          SELECT t.up_bnch_grade,
                 t.up_bnch_man_grade,
                 t.up_bnch_sign,
                 t.up_bnch_lv,
                 t.up_gw_manage_type
            INTO v_up_bnch_grade,
                 v_up_bnch_man_grade,
                 v_up_bnch_sign,
                 v_up_bnch_lv,
                 v_up_bnch_gw_manage_type
            FROM dams_branch_relation t
           WHERE t.bnch_id = i_stru_id
             AND t.up_gw_manage_type = '3';
        EXCEPTION
          WHEN no_data_found THEN
            SELECT tt.up_bnch_grade,
                   tt.up_bnch_man_grade,
                   tt.up_bnch_sign,
                   tt.up_bnch_lv,
                   tt.up_gw_manage_type
              INTO v_up_bnch_grade,
                   v_up_bnch_man_grade,
                   v_up_bnch_sign,
                   v_up_bnch_lv,
                   v_up_bnch_gw_manage_type
              FROM (SELECT t.up_bnch_grade,
                           t.up_bnch_man_grade,
                           t.up_bnch_sign,
                           t.up_bnch_lv,
                           t.up_gw_manage_type
                      FROM dams_branch_relation t
                     WHERE t.bnch_id = i_stru_id
                       AND (t.up_bnch_man_grade = '1' OR
                           is_foreign_lvl2(t.up_bnch_id) = 1)
                     ORDER BY t.up_level) tt
             WHERE rownum = 1;
        END;
      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        NULL;
    END;

    CASE
      WHEN v_stru_sign IN ('1', '3', '4', '5') AND v_up_bnch_sign = '1' THEN
        CASE
          WHEN v_stru_grade IN ('1', '2') THEN
            v_ret := '''''';
          WHEN v_stru_grade = '3' THEN
            --二级分行
            v_ret := ''''',''1''';
          WHEN v_stru_grade = '4' AND v_man_grade = '2' THEN
            --一级行直挂的支行
            v_ret := ''''',''3'',''5''';
          WHEN (v_stru_sign IN ('1', '3', '4', '5') AND v_stru_grade IS NULL) OR
               v_stru_grade = '5' OR
               (v_stru_grade = '4' AND v_man_grade <> '2') THEN
            --其他支行和其他机构

            IF ((v_stru_lv <> '66' OR v_stru_lv IS NULL) AND
               v_up_stru_sign = '2' AND --直挂本部下(非行长室)
               ((v_up_bnch_grade = '3' AND v_up_bnch_gw_manage_type IS NULL) OR --二级分行
               (v_up_bnch_grade = '4' AND v_up_bnch_gw_manage_type = '3') OR --调为二级分行下
               v_up_bnch_grade = '2' --一级分行下
               )) THEN
              v_ret := ''''',''5'''; -- 可调整为二级部室挂到其他一级部室（或分行:?须确认此括号内注释）下
            ELSIF (v_up_stru_sign <> '2' AND --非本部下
                  ((v_up_stru_grade = '3' AND v_up_stru_gw_manage_type IS NULL) OR --直挂二级分行下
                  (v_up_stru_gw_manage_type = '3') --直挂调为二级分行的下面
                  )) THEN
              --判断是否一级部
              v_ret := ''''',''3'',''5'''; -- 可调整为2级分行或二级部室

            ELSIF ((v_stru_lv <> '66' OR v_stru_lv IS NULL) AND
                  v_up_stru_sign <> '2' AND --非直挂本部下(非行长室)
                  ((v_up_bnch_gw_manage_type IS NULL OR
                  v_up_bnch_gw_manage_type <> '3') AND
                  (v_up_stru_grade NOT IN ('2', '3') OR
                  v_up_stru_grade IS NULL)) --非二级行下的一级部、支行
                  ) THEN
              --判断是否二级部
              v_ret := ''''',''4''';
            ELSIF ((v_stru_lv <> '66' OR v_stru_lv IS NULL) AND
                  v_up_bnch_gw_manage_type = '1' --被调整为部室的二级分行下辖机构
                  ) THEN
              --判断是否二级部
              v_ret := ''''',''4''';
            ELSIF (v_man_grade >= '4') THEN
              v_ret := ''''',''4''';
            ELSE
              v_ret := '''''';
            END IF;

          ELSE
            v_ret := '''''';
        END CASE;
      WHEN v_stru_sign IN ('1', '3', '5') AND v_up_bnch_sign = '4' THEN
        --直属机构下辖机构
        IF (v_up_stru_sign IN ('2', '4')) THEN
          v_ret := ''''',''5''';
        ELSE
          v_ret := ''''',''4''';
        END IF;
      WHEN v_stru_sign = '8' THEN
        --境外
        IF (v_man_grade = '1') THEN
          v_ret := '''''';
        ELSIF (v_stru_sname LIKE '%本部%' OR v_stru_sname LIKE '%ADMIN.OFFICE%') THEN
          v_ret := '''''';
        ELSIF (v_stru_sname NOT LIKE '%本部%' AND
              v_stru_sname NOT LIKE '%ADMIN.OFFICE%' AND v_man_grade = '2') THEN
          IF (is_foreign_lvl2(i_stru_id) = 1) THEN
            -- 是海外二级行的转为一级部室
            v_ret := ''''',''1''';
          ELSE
            --可以转为二级行或二级部室
            v_ret := ''''',''3'',''5''';
          END IF;
        ELSIF ((v_up_stru_sname LIKE '%本部%' OR
              v_up_stru_sname LIKE '%ADMIN.OFFICE%') AND
              (v_man_grade - v_up_bnch_man_grade) = 2) THEN
          --一、二级行本部下辖一级,可转二级部
          v_ret := ''''',''5''';
        ELSIF ((v_man_grade - v_up_bnch_man_grade) = 1 AND
              v_stru_sname NOT LIKE '%本部%' AND
              v_stru_sname NOT LIKE '%ADMIN.OFFICE%') THEN
          --一、二级行下一级机构是一级部可转二级部
          v_ret := ''''',''5''';
        ELSE
          v_ret := ''''',''4''';
        END IF;
      WHEN v_stru_sign IN ('2', '4', '6', '7', '18') THEN
        --本部,直属,自助,虚拟,境外,控股
        v_ret := '''''';
      ELSE
        v_ret := '''''';
    END CASE;
 
    v_sql := 'SELECT C.COLUMN_VALUE value FROM TABLE(SYS.ODCIVARCHAR2LIST(' ||
             v_ret || ')) C';
    OPEN o_choice_list FOR v_sql;

  EXCEPTION
    WHEN OTHERS THEN
      --系统异常,返回错误代码-1
      pack_log.error(v_proc_name, '', '异常！' || v_param);
      o_flag := '-1';
      RETURN;
  END proc_get_gw_man_choice_list;

 
  PROCEDURE proc_dept_stru_upd(i_branch_id    IN VARCHAR2, --机构编号
                               i_user_id      IN VARCHAR2, --用户ID
                               i_dept_stru_id IN VARCHAR2, -- 输入-1表示设定为默认,并检查是否符合有一级部室
                               o_flag         OUT VARCHAR2, --返回标志  返回-2 表示必须设定一级部室
                               o_msg          OUT VARCHAR2 --结果
                               ) IS
    v_proc_name      db_log.proc_name%TYPE := 'PCKG_DAMS_BRANCH_PROPERTY.proc_dept_stru_upd'; --存储过程名称
    v_step_num       db_log.step_no%TYPE; --步骤号
    v_param          db_log.info%TYPE := i_branch_id || ' ' || i_user_id || ' ' ||
                                         i_dept_stru_id; --参数
    v_need_dept_stru VARCHAR2(10);
  BEGIN
    --STEP 0: 初始化处理日志的相关参数
    v_step_num := '#00';
    o_flag     := '0';

    IF (i_dept_stru_id IS NOT NULL AND i_dept_stru_id <> '-1') THEN
      MERGE INTO dams_branch_cfg t
      USING (SELECT i_branch_id stru_id, i_dept_stru_id dept_stru_id FROM dual) tmp
      ON (t.stru_id = tmp.stru_id)
      WHEN MATCHED THEN
        UPDATE SET t.dept_stru_id = tmp.dept_stru_id
      WHEN NOT MATCHED THEN
        INSERT (stru_id, dept_stru_id) VALUES (tmp.stru_id, tmp.dept_stru_id);
    ELSE
      IF (i_dept_stru_id = '-1') THEN
        SELECT COUNT(1)
          INTO v_need_dept_stru
          FROM dams_branch_relation r
         WHERE r.bnch_id = i_branch_id
           AND r.up_bnch_man_grade NOT IN ('0', '1')
           AND (r.up_gw_manage_type IN ('1', '2', '4') OR
               (r.up_bnch_lv IN ('51', '99') AND r.up_gw_manage_type IS NULL))
           AND r.up_level <> '1';
        IF (v_need_dept_stru = '0') THEN
          o_flag := '-2';
          o_msg  := '设置失败,必须指定一级部室！';
          RETURN;
        END IF;
      END IF;
     
      UPDATE dams_branch_cfg t
         SET t.dept_stru_id = ''
       WHERE t.stru_id = i_branch_id;
      DELETE FROM dams_branch_cfg t
       WHERE t.stru_id = i_branch_id
         AND t.reserve1 IS NULL
         AND t.reserve2 IS NULL
         AND t.reserve3 IS NULL;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
     
      pack_log.error(v_proc_name, v_step_num, '异常！' || v_param);
      o_flag := '-1';
      o_msg  := '设置失败';
      RETURN;
  END proc_dept_stru_upd;

 
  PROCEDURE proc_dept_branch_list(i_branch_id    IN VARCHAR2, --机构编号
                                  i_user_id      IN VARCHAR2, --用户ID
                                  o_flag         OUT VARCHAR2, --返回标志
                                  o_branch_list  OUT ret_list, --机构列表
                                  o_dept_stru_id OUT VARCHAR2) IS
    v_proc_name db_log.proc_name%TYPE := 'PCKG_DAMS_BRANCH_PROPERTY.proc_dept_branch_list'; --存储过程名称
    v_step_num  db_log.step_no%TYPE; --步骤号
    v_param     db_log.info%TYPE := i_branch_id || ' ' || i_user_id;
    --v_up_is_sign2 dams_branch.stru_sign%TYPE;
    v_man_bnch  dams_branch.stru_id%TYPE;
    v_stru_sign dams_branch.stru_sign%TYPE;
  BEGIN
    o_flag := '0';
    SELECT t.stru_sign
      INTO v_stru_sign
      FROM dams_branch t
     WHERE t.stru_id = i_branch_id;
    IF (v_stru_sign = '8') THEN
      --境外机构
      OPEN o_branch_list FOR
      --平级
        SELECT t2.stru_id, t2.stru_sname
          FROM dams_branch t, dams_branch t2
         WHERE t.stru_id = i_branch_id
           AND t.sup_stru = t2.sup_stru
           AND t2.stru_state IN ('1', '2')
           AND t2.stru_id <> i_branch_id
           AND t2.stru_sname NOT LIKE '%本部%'
           AND upper(t2.stru_sname) NOT LIKE '%ADMIN.OFFICE%'
           AND t2.gw_manage_type IS NULL
        UNION ALL
        --平级有本部,找本部下一级
        SELECT t2.stru_id, t2.stru_sname
          FROM dams_branch t, dams_branch bb, dams_branch t2
         WHERE t.stru_id = i_branch_id
           AND t.sup_stru = bb.sup_stru
           AND (bb.stru_sname LIKE '%本部%' OR
               upper(bb.stru_sname) LIKE '%ADMIN.OFFICE%')
           AND t2.sup_stru = bb.stru_id
           AND t2.gw_manage_type IS NULL
           AND t2.stru_state IN ('1', '2')
        UNION ALL
        --上级是本部,找本部平级的机构
        SELECT t2.stru_id, t2.stru_sname
          FROM dams_branch t, dams_branch up, dams_branch t2
         WHERE t.stru_id = i_branch_id
           AND t.sup_stru = up.stru_id
           AND up.sup_stru = t2.sup_stru
           AND t2.stru_state IN ('1', '2')
           AND up.stru_id <> t2.stru_id
           AND (up.stru_sname LIKE '%本部%' OR
               upper(up.stru_sname) LIKE '%ADMIN.OFFICE%')
           AND t2.gw_manage_type IS NULL
        UNION ALL
        --一级境外机构下属gw_manage_type='4'
        SELECT t2.stru_id, t2.stru_sname
          FROM dams_branch_relation t, dams_branch_relation t1, dams_branch t2
         WHERE t.bnch_id = i_branch_id
           AND t.up_bnch_man_grade = '1'
           AND t1.up_bnch_id = t.up_bnch_id
           AND t2.gw_manage_type = '4'
           AND t1.bnch_id = t2.stru_id;
    ELSE
      BEGIN
        --查找上级一、二级行、直属机构
        SELECT MAX(up_bnch_id)
          INTO v_man_bnch
          FROM (SELECT t.up_bnch_id
                  FROM dams_branch_relation t
                 WHERE t.bnch_id = i_branch_id
                   AND ((t.up_gw_manage_type IS NULL AND
                       t.up_bnch_grade IN ('2', '3')) OR
                       t.up_gw_manage_type = '3' OR
                       (t.up_bnch_sign = '4' AND EXISTS
                        (SELECT 1
                            FROM dams_branch_relation rr
                           WHERE rr.bnch_id = i_branch_id
                             AND rr.up_bnch_man_grade = '1' --判断是否（直属机构）非分行内的的直属机构
                             AND rr.up_bnch_sign = '4')))
                   AND t.up_level <> '1'
                 ORDER BY t.up_level)
         WHERE rownum = 1;

      EXCEPTION
        WHEN no_data_found THEN
          NULL;
      END;

      IF (v_man_bnch IS NULL) THEN
        RETURN;
      ELSE
        OPEN o_branch_list FOR
          SELECT stru_id, stru_sname
            FROM (
                  --本部下一级
                  SELECT t.stru_id, t.stru_sname, t.sort, 1 tmp_sort, t.stru_sort
                    FROM dams_branch t, dams_branch b
                   WHERE t.sup_stru = b.stru_id
                     AND b.sup_stru = v_man_bnch
                     AND b.stru_sign = '2'
                     AND t.stru_sign NOT IN ('6', '7')
                     AND t.stru_state IN ('1', '2')
                     AND t.gw_manage_type IS NULL
                     AND t.stru_lv <> '66'
                  UNION ALL
                  --bnch下一级
                  SELECT b.stru_id, b.stru_sname, b.sort, 2 tmp_sort, b.stru_sort
                    FROM dams_branch b
                   WHERE b.sup_stru = v_man_bnch
                     AND b.stru_sign NOT IN ('2', '4', '6', '7')
                     AND b.stru_state IN ('1', '2')
                     AND b.gw_manage_type IS NULL
                  UNION ALL
                  /*
                                                                                                                                                                                                                        TODO: owner="kfzx-linlc" created="2014/3/19"
                                                                                                                                                                                                                        text="一级行内部室调整为二级部室会选择到二级行内设定的一级部室"
                                                                                                                                                                                                                        */
                  SELECT b.stru_id, b.stru_sname, b.sort, 3 tmp_sort, b.stru_sort
                    FROM dams_branch b, dams_branch_relation t
                   WHERE b.stru_id = t.bnch_id
                     AND t.up_bnch_id = v_man_bnch
                     AND t.gw_manage_type = '4')
           WHERE stru_id <> i_branch_id --不等于自身机构
           ORDER BY tmp_sort,
                    stru_sort DESC NULLS LAST,
                    sort DESC NULLS LAST,stru_sname;
      END IF;
    END IF;
    BEGIN
      SELECT t.dept_stru_id
        INTO o_dept_stru_id
        FROM dams_branch_cfg t
       WHERE t.stru_id = i_branch_id;
    EXCEPTION
      --为空
      WHEN no_data_found THEN
        NULL;
    END;
  EXCEPTION
    WHEN OTHERS THEN
      --需要主动关闭游标
      IF o_branch_list%ISOPEN THEN
        CLOSE o_branch_list;
      END IF;
      --系统异常,返回错误代码-1
      pack_log.error(v_proc_name, v_step_num, '异常！' || v_param);
      o_flag := '-1';
      ROLLBACK;
      RETURN;
  END proc_dept_branch_list;
  
  PROCEDURE proc_branch_sepc_upd(i_branch_id IN VARCHAR2, --机构编号
                                 i_user_id   IN VARCHAR2, --用户ID
                                 i_is_inner  IN VARCHAR2, --是否内设
                                 i_spec_prop IN VARCHAR2, --机构属性
                                 o_flag      OUT VARCHAR2, --返回标志
                                 o_msg       OUT VARCHAR2 --结果
                                 ) IS
    v_proc_name   db_log.proc_name%TYPE := 'PCKG_DAMS_BRANCH_PROPERTY.PROC_BRANCH_SEPC_UPD'; --存储过程名称
    v_step_num    db_log.step_no%TYPE; --步骤号
    v_param       db_log.info%TYPE := i_branch_id || ' ' || i_user_id || ' ' ||
                                      i_is_inner || ' ' || i_spec_prop; --参数
    v_end_time    VARCHAR2(14);
    v_branch_name dams_branch.stru_sname%TYPE;
    v_count       NUMBER;

  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step,
                  pack_log.start_msg || v_param);

    v_step_num := '1';
    SELECT COUNT(1)
      INTO v_count
      FROM dams_branch_spec_prop p
     WHERE p.stru_id = i_branch_id
       AND p.spec_prop = i_spec_prop;

    IF v_count > 0 AND i_is_inner = '0' THEN
      --取消该机构专业属性
      DELETE FROM dams_branch_spec_prop p
       WHERE p.stru_id --= i_branch_id
             IN (SELECT DISTINCT r.bnch_id
                   FROM dams_branch_relation r
                  WHERE r.up_bnch_id = i_branch_id)
         AND p.spec_prop = i_spec_prop;
    ELSIF v_count = 0 AND i_is_inner = '1' THEN
      MERGE INTO dams_branch_spec_prop p
      USING (SELECT DISTINCT r.bnch_id
               FROM dams_branch_relation r
              WHERE r.up_bnch_id = i_branch_id) t
      ON (t.bnch_id = p.stru_id and p.spec_prop = i_spec_prop )
      WHEN NOT MATCHED THEN
        INSERT (stru_id, spec_prop) VALUES (t.bnch_id, i_spec_prop);
    END IF;


    MERGE INTO dams_user_daily_change c
    USING (SELECT u.ssic_id
             FROM dams_user u
             JOIN dams_branch_relation r ON u.stru_id = r.bnch_id
            WHERE r.up_bnch_id = i_branch_id
           UNION
           SELECT l.ssic_id
             FROM dams_user_branch_rel l
             JOIN dams_branch_relation r ON l.stru_id = r.bnch_id
            WHERE r.up_bnch_id = i_branch_id) t1
    ON (t1.ssic_id = c.ssic_id AND c.update_time = to_char(SYSDATE, 'yyyymmdd'))
    WHEN NOT MATCHED THEN
      INSERT
        (ssic_id, update_time, tab_src)
      VALUES
        (t1.ssic_id, to_char(SYSDATE, 'yyyymmdd'), '512');

    o_flag := '0';
    o_msg  := '设置已成功';
    v_branch_name := pckg_dams_util.func_get_stru_name(i_branch_id);
    v_end_time    := to_char(SYSDATE, 'yyyyMMddHH24miss');
    pckg_dams_util.proc_dams_logger('BRANCH_OPERATE', '017', v_end_time,
                                    i_user_id,
                                    '机构属性设置（是否信访部室）:' || v_branch_name || '（' ||
                                     i_branch_id || '）' || '设置为“' || CASE
                                     i_is_inner WHEN '1' THEN '是' WHEN '0' THEN '否' ELSE
                                     i_is_inner END || '”');
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      --系统异常,返回错误代码-1
      pack_log.error(v_proc_name, v_step_num, '异常！' || v_param);
      o_flag := '-1';
      o_msg  := '设置失败';
      ROLLBACK;
      RETURN;
  END proc_branch_sepc_upd;
 
  PROCEDURE proc_branch_lh_sepc_upd(i_branch_id IN VARCHAR2, --机构编号
                                    i_user_id   IN VARCHAR2, --用户ID
                                    i_is_inner  IN VARCHAR2, --是否内设
                                    i_spec_prop IN VARCHAR2, --机构属性
                                    o_flag      OUT VARCHAR2, --返回标志
                                    o_msg       OUT VARCHAR2  --结果
                                    ) IS
    v_proc_name   db_log.proc_name%TYPE := 'PCKG_DAMS_BRANCH_PROPERTY.PROC_BRANCH_LH_SEPC_UPD'; --存储过程名称
    v_step_num    db_log.step_no%TYPE; --步骤号
    v_param       db_log.info%TYPE := i_branch_id || ' ' || i_user_id || ' ' ||
                                      i_is_inner || ' ' || i_spec_prop; --参数
    v_end_time    VARCHAR2(14);
    v_branch_name dams_branch.stru_sname%TYPE;
    v_count       NUMBER;

  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step,
                  pack_log.start_msg || v_param);

    v_step_num := '1';

    SELECT COUNT(1)
      INTO v_count
      FROM dams_branch_spec_prop p
     WHERE p.stru_id = i_branch_id
       AND p.spec_prop = i_spec_prop;

    IF v_count > 0 AND i_is_inner = '0' THEN
      --取消该机构专业属性
      DELETE FROM dams_branch_spec_prop p
       WHERE p.stru_id = i_branch_id
         AND p.spec_prop = i_spec_prop;
    ELSIF v_count = 0 AND i_is_inner = '1' THEN
      INSERT INTO dams_branch_spec_prop
        (stru_id, spec_prop)
      VALUES
        (i_branch_id, i_spec_prop);
    END IF;

   
    MERGE INTO dams_user_daily_change c
    USING (SELECT u.ssic_id
             FROM dams_user u
             JOIN dams_branch_relation r ON u.stru_id = r.bnch_id
            WHERE r.bnch_id = i_branch_id
           UNION
           SELECT l.ssic_id
             FROM dams_user_branch_rel l
             JOIN dams_branch_relation r ON l.stru_id = r.bnch_id
            WHERE r.bnch_id = i_branch_id) t1
    ON (t1.ssic_id = c.ssic_id AND c.update_time = to_char(SYSDATE, 'yyyymmdd'))
    WHEN NOT MATCHED THEN
      INSERT
        (ssic_id, update_time, tab_src)
      VALUES
        (t1.ssic_id, to_char(SYSDATE, 'yyyymmdd'), '512');

    o_flag := '0';
    o_msg  := '设置已成功';

    v_branch_name := pckg_dams_util.func_get_stru_name(i_branch_id);
    v_end_time    := to_char(SYSDATE, 'yyyyMMddHH24miss');
    pckg_dams_util.proc_dams_logger('BRANCH_OPERATE', '017', v_end_time,
                                    i_user_id,
                                    '机构属性设置（是否信访部室）:' || v_branch_name || '（' ||
                                     i_branch_id || '）' || '设置为“' || CASE
                                     i_is_inner WHEN '1' THEN '是' WHEN '0' THEN '否' ELSE
                                     i_is_inner END || '”');
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      --系统异常,返回错误代码-1
      pack_log.error(v_proc_name, v_step_num, '异常！' || v_param);
      o_flag := '-1';
      o_msg  := '设置失败';
      ROLLBACK;
      RETURN;
  END proc_branch_lh_sepc_upd;

  FUNCTION is_foreign_lvl2(i_branch_id IN VARCHAR2 --所属处室ID
                           ) RETURN NUMBER IS
    v_return NUMBER(1);
  BEGIN

    SELECT COUNT(1)
      INTO v_return
      FROM dams_branch rel
     WHERE rel.stru_id = i_branch_id
       AND rel.stru_sign = '8'
       AND rel.man_grade = '2'
       AND rel.stru_state IN ('1', '2')
       AND rel.stru_lv NOT IN ('93', '94', '96', '97')
       AND upper(rel.stru_sname) NOT LIKE '%本部%'
       AND upper(rel.stru_sname) NOT LIKE '%ADMIN.OFFICE%';
    RETURN v_return;
  END;

END pckg_dams_branch_property;
//
CREATE OR REPLACE PACKAGE pckg_dams_task IS

  FUNCTION func_get_form_id_from_pdid(i_pdid        IN VARCHAR2,
                                      i_business_id IN VARCHAR2 := '')
    RETURN VARCHAR2;
 
END pckg_dams_task;
//

CREATE OR REPLACE PACKAGE BODY pckg_dams_task IS
FUNCTION func_get_form_id_from_pdid(i_pdid        IN VARCHAR2,
                                      i_business_id IN VARCHAR2 := '')
    RETURN VARCHAR2 IS
    v_rlt dams_form.form_id%TYPE;
  BEGIN
    --20141013修改
    SELECT MAX(s.form_id)
      INTO v_rlt
      FROM dams_save_info s
     WHERE s.app_id = i_business_id;

    IF v_rlt IS NULL THEN
      SELECT MAX(t.form_id)
        INTO v_rlt
        FROM dams_wf_process_form t
       WHERE t.pdid = i_pdid;

      IF v_rlt IS NULL THEN
        RETURN i_pdid;
      END IF;

    END IF;

    RETURN v_rlt;
  END func_get_form_id_from_pdid;
end pckg_dams_task;
//

CREATE OR REPLACE PACKAGE pckg_dams_webservice IS

  
  TYPE todo_task_list IS REF CURSOR;
  TYPE record_user IS TABLE OF dams_user%ROWTYPE;
 
  PROCEDURE proc_senddata_list(o_task_list OUT todo_task_list, --机构包含员工列表
                               o_flag      OUT VARCHAR2,
                               o_msg       OUT VARCHAR2);

  PROCEDURE proc_senddata2dams_list(i_doc_id    IN VARCHAR2,
                                    i_handler   IN VARCHAR2,
                                    o_flag      OUT VARCHAR2,
                                    o_task_list OUT todo_task_list,
                                    o_msg       OUT VARCHAR2);
END pckg_dams_webservice;
//
CREATE OR REPLACE PACKAGE BODY pckg_dams_webservice IS



  PROCEDURE proc_senddata_list(o_task_list OUT todo_task_list, --返回结果列表
                               o_flag      OUT VARCHAR2,
                               o_msg       OUT VARCHAR2) IS
    v_finishnum NUMBER;
  BEGIN
  
    SELECT COUNT(1)
      INTO v_finishnum
      FROM dams_webservice_del_task t1
     WHERE t1.exception_num < 3;
    --主要逻辑为；先行传送所有已发送并且结束的代办；然后再传送新增的代办
    IF v_finishnum > 0 THEN
      UPDATE dams_webservice_del_task t
         SET t.send_flag = '1', t.exception_num = t.exception_num + 1
       WHERE t.keyid IN (SELECT keyid
                           FROM (SELECT t1.keyid,
                                        row_number() over(ORDER BY t1.create_time ASC) rn
                                   FROM dams_webservice_del_task t1
                                  WHERE t1.exception_num < 3 --异常的数据最大发送3次
                                 )
                          WHERE rn < 101);
    END IF;
  
    IF v_finishnum >= 100 THEN
      OPEN o_task_list FOR
        SELECT *
          FROM (SELECT '' filteritem,
                       substr(tw.keyid, 0, instr(tw.keyid, '_') - 1) ownerssicid,
                       7 appid,
                       tw.keyid todoid,
                       '' title,
                       'url' url,
                       to_char(SYSDATE, 'yyyy-mm-dd hh24:mi:ss') expiredate,
                       to_char(tw.create_time, 'yyyy-mm-dd hh24:mi:ss') createtime,
                       2 action,
                       to_char(SYSDATE, 'yyyy-mm-dd hh24:mi:ss') updatetime,
                       '' remark1,
                       '' remark2,
                       '' remark3
                  FROM dams_webservice_del_task tw
                 WHERE tw.send_flag = '1')
         WHERE rownum < 101
         ORDER BY todoid;
    ELSE
      OPEN o_task_list FOR
        SELECT *
          FROM (SELECT '' filteritem,
                       substr(tw.keyid, 0, instr(tw.keyid, '_') - 1) ownerssicid,
                       7 appid,
                       tw.keyid todoid,
                       '' title,
                       'url' url,
                       to_char(SYSDATE, 'yyyy-mm-dd hh24:mi:ss') expiredate,
                       to_char(tw.create_time, 'yyyy-mm-dd hh24:mi:ss') createtime,
                       2 action,
                       to_char(SYSDATE, 'yyyy-mm-dd hh24:mi:ss') updatetime,
                       '' remark1,
                       '' remark2,
                       '' remark3
                  FROM dams_webservice_del_task tw
                 WHERE tw.send_flag = '1'
                UNION ALL (SELECT *
                            FROM (SELECT p.filteritem,
                                         p.ownerssicid,
                                         p.appid,
                                         p.todoid,
                                         p.title,
                                         p.url,
                                         p.expiredate,
                                         p.createtime,
                                         p.action,
                                         p.updatetime,
                                         p.remark1,
                                         p.remark2,
                                         p.remark3
                                    FROM (SELECT t5.sys_sname filteritem,
                                                 t3.userid_ ownerssicid,
                                                 7 appid,
                                                 t1.task_id || '_' || t3.userid_ todoid,
                                                 t2.title title,
                                                 '/mainindex.jsp?method=opentodotask&from_flag=oa&business_id=' ||
                                                 t1.business_id ||
                                                 '&jbpm_task_id=' || t1.task_id || ' ' url,
                                                 '' expiredate,
                                                 to_char(nvl(t1.create_time,
                                                             SYSDATE),
                                                         'yyyy-mm-dd hh24:mi:ss') createtime,
                                                 0 action,
                                                 to_char(nvl(t1.take_time,
                                                             nvl(t1.create_time,
                                                                  SYSDATE)),
                                                         'yyyy-mm-dd hh24:mi:ss') updatetime,
                                                 '' remark1,
                                                 '' remark2,
                                                 '' remark3,
                                                 t1.create_time
                                            FROM dams_wf_task         t1,
                                                 dams_wf_process      t2,
                                                 jbpm4_participation  t3,
                                                 dams_wf_process_conf t4,
                                                 dams_sys_conf        t5
                                           WHERE t1.procinst_id = t2.procinst_id
                                             AND t3.task_ = t1.task_id
                                             AND t4.pdid = t2.process_id
                                             AND t4.sys_code = t5.sys_code
                                             AND t1.task_state = 'open'
                                             AND nvl(t1.issend, 0) = 0
                                          -- ORDER BY t1.create_time ASC
                                          
                                          --科技部考核外部待办
                                          UNION ALL
                                          SELECT t5.sys_sname filteritem,
                                                 t.ssic_id ownerssicid,
                                                 7 appid,
                                                 t.business_id || '_' || t.ssic_id todoid,
                                                 t.title title,
                                                 '/mainindex.jsp?method=openstaffkh&from_flag=mail&business_id=' ||
                                                 t.business_id ||
                                                 '&jbpm_task_id=0' || ' ' url,
                                                 '' expiredate,
                                                 to_char(nvl(to_date(t.create_time,
                                                                     'yyyymmddhh24miss'),
                                                             SYSDATE),
                                                         'yyyy-mm-dd hh24:mi:ss') createtime,
                                                 0 action,
                                                 to_char(nvl(to_date(t.take_time,
                                                                     'yyyymmddhh24miss'),
                                                             nvl(to_date(t.create_time,
                                                                          'yyyymmddhh24miss'),
                                                                  SYSDATE)),
                                                         'yyyy-mm-dd hh24:mi:ss') updatetime,
                                                 '' remark1,
                                                 '' remark2,
                                                 '' remark3,
                                                 to_date(t.create_time,
                                                         'yyyymmddhh24miss') create_time
                                            FROM dams_outer_todo_task t,
                                                 dams_sys_conf        t5
                                           WHERE t.task_state = 'open'
                                             AND t.send_flag = '0'
                                             AND t.sys_code = t5.sys_code
                                             AND t.sys_code = 'SV') p
                                   ORDER BY p.create_time ASC)
                           WHERE rownum < 101 - v_finishnum))
         ORDER BY todoid;
    END IF;
  
    UPDATE dams_webservice_del_task t
       SET t.send_flag = '0'
     WHERE t.send_flag = '1';
  
    o_flag := '0';
    o_msg  := '查询成功！';
  EXCEPTION
    WHEN OTHERS THEN
      o_flag := '-1';
      o_msg  := '查询失败！';
      RETURN;
    
  END proc_senddata_list;

  PROCEDURE proc_senddata2dams_list(i_doc_id    IN VARCHAR2,
                                    i_handler   IN VARCHAR2,
                                    o_flag      OUT VARCHAR2,
                                    o_task_list OUT todo_task_list,
                                    o_msg       OUT VARCHAR2) IS
    v_param     db_log.info%TYPE := i_doc_id || '|' || i_handler;
    v_proc_name db_log.proc_name%TYPE := 'PCKG_DAMS_WEBSERVICE.proc_senddata2dams_list';
    v_step      db_log.step_no%TYPE;
    v_existnum  NUMBER;
    v_ex_save   NUMBER;
  
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step,
                  pack_log.start_msg || v_param);
  
    SELECT COUNT(1)
      INTO v_existnum
      FROM (SELECT 1
              FROM dams_wf_task t
             WHERE t.business_id = i_doc_id
               AND rownum = 1);
  
    SELECT COUNT(1)
      INTO v_ex_save
      FROM dams_wf_task t
     WHERE t.business_id = i_doc_id
       AND t.task_state IN ('completed', 'takeback')
       AND t.is_first = '1'
       AND t.issend = '0'
       AND rownum = 1;
  
    IF i_handler = 'noticedown' THEN
      OPEN o_task_list FOR
      
        SELECT business_id, --businessid(联合主键 key)
               draft_unit_id, --creator stru
               task_id, -- task id (联合主键 key)
               creator, --创建人 id
               bussinesstype, --业务类型
               ownerssicid, --待办所属人
               ssic_id, --上一节点处理人
               task_name, --上一节点处理名称 修改为outgoing_name xiesy 0816
               title, --标题
               url, --处理地址
               url_view, --废止文档查看地址
               createtime, --创建时间
               taketime, --读取时间
               taskstate, --任务状态 ‘open’待办,‘suspended’废止
               doc_sequence,
               action, --操作项 0 新增；2删除
               syscode
          FROM --所属系统(联合主键 key)
               (SELECT '"business_id":"' || i_doc_id || '"' business_id,
                       '"creator_stru":""' draft_unit_id,
                       '"task_id":"' ||
                       decode(t1.is_first, '1', '0', t1.task_id) || '"' task_id,
                       '"creator_id":""' creator,
                       '"business_type":""' bussinesstype,
                       '"ssic_id":"' || t1.ssic_id || '"' ownerssicid,
                       '"prev_ssic_id":""' ssic_id,
                       '"prev_task_name":""' task_name,
                       '"title":""' title,
                       '"url":""' url,
                       '"url_view":""' url_view,
                       '"create_time":""' createtime,
                       '"take_time":""' taketime,
                       '"task_state":""' taskstate,
                       '"OUT_SYS_SEQ":""' doc_sequence,
                       '"action":"2"' action, --草稿删除3
                       '"sys_code":"AB"' syscode
                  FROM dams_wf_task t1
                 WHERE t1.task_state IN ('completed', 'locked', 'takeback') --包含取回
                   AND t1.issend != '3' --kfzx-guoq03 容错性只要状态不为3的 就选出来（防止平台端 遗留）
                   AND t1.business_id = i_doc_id
                
                UNION ALL
                SELECT '"business_id":"' || i_doc_id || '"' business_id,
                       '"creator_stru":""' draft_unit_id,
                       '"task_id":"' || t1.task_id || '"' task_id,
                       '"creator_id":""' creator,
                       '"business_type":""' bussinesstype,
                       '"ssic_id":"' || t1.ssic_id || '"' ownerssicid,
                       '"prev_ssic_id":""' ssic_id,
                       '"prev_task_name":""' task_name,
                       '"title":""' title,
                       '"url":""' url,
                       '"url_view":""' url_view,
                       '"create_time":""' createtime,
                       '"take_time":""' taketime,
                       '"task_state":""' taskstate,
                       '"OUT_SYS_SEQ":""' doc_sequence,
                       '"action":"2"' action,
                       '"sys_code":"AB"' syscode
                  FROM dams_wf_hist_task t1
                 WHERE t1.business_id = i_doc_id
                
                UNION ALL
                
                SELECT '"business_id":"' || business_id || '"' business_id,
                       '"creator_stru":"' || draft_unit_id || '"' draft_unit_id,
                       '"task_id":"' || task_id || '"' task_id,
                       '"creator_id":"' || creator || '"' creator,
                       '"business_type":"' || bussinesstype || '"' bussinesstype,
                       '"ssic_id":"' || ownerssicid || '"' ownerssicid,
                       '"prev_ssic_id":"' || ssic_id || '"' ssic_id,
                       '"prev_task_name":"' || outgoing_name || '"' task_name,
                       '"title":"' || title || '"' title,
                       '"url":"' ||
                       decode(taskstate, 'open',
                              'jbpm.flowc?flowActionName=open_task_flc',
                              'suspended', 'damstasklist.flowc') || url || '"' url,
                       '"url_view":"' ||
                       decode(taskstate, 'open', '', 'suspended',
                              'jbpm.flowc?flowActionName=open_task_flc' ||
                               chr(38) || 'mask=read' || urlr) || '"' url_view,
                       '"create_time":"' || createtime || '"' createtime,
                       '"take_time":"' || taketime || '"' taketime,
                       '"task_state":"' || taskstate || '"' taskstate,
                       '"OUT_SYS_SEQ":"' || doc_sequence || '"' doc_sequence,
                       '"action":"' || action || '"' action,
                       '"sys_code":"AB"' syscode
                  FROM (SELECT business_id,
                               draft_unit_id,
                               task_id,
                               creator,
                               bussinesstype,
                               ownerssicid,
                               ssic_id,
                               outgoing_name,
                               title,
                               url,
                               urlr,
                               createtime,
                               taketime,
                               taskstate,
                               doc_sequence,
                               action
                          FROM (SELECT t1.business_id,
                                       t2.creator_stru draft_unit_id,
                                       to_char(t1.task_id) task_id,
                                       t2.creator_id creator,
                                       t2.business_type bussinesstype,
                                       decode(t1.task_state, 'open', t3.userid_,
                                              'suspended', tp.ssic_id) ownerssicid,
                                       tp.ssic_id,
                                       tp.outgoing_name, --修改为outgoing_name xiesy 0816
                                       t2.title title,
                                       '' || chr(38) || 'jbpm_business_id=' ||
                                       t1.business_id || '' || chr(38) ||
                                       'jbpm_task_id=' || to_char(t1.task_id) || ' ' url,
                                       '' || chr(38) || 'jbpm_business_id=' ||
                                       t1.business_id || '' || chr(38) ||
                                       'jbpm_task_id=' || to_char(tp.task_id) || ' ' urlr, --废止查看地址
                                       to_char(nvl(t1.create_time, SYSDATE),
                                               'yyyymmddhh24miss') createtime,
                                       to_char(t1.take_time, 'yyyymmddhh24miss') taketime,
                                       t1.task_state taskstate,
                                       '' doc_sequence,
                                       '0' action
                                  FROM dams_wf_task         t1,
                                       dams_wf_task         tp,
                                       dams_wf_process      t2,
                                       jbpm4_participation  t3,
                                       dams_wf_process_conf t4
                                 WHERE t1.business_id = i_doc_id
                                   AND t1.procinst_id = t2.procinst_id
                                   AND t1.prev_task_id = tp.task_id(+)
                                   AND t2.business_id = t1.business_id
                                   AND t3.task_(+) = t1.task_id
                                   AND t4.pdid = t2.process_id
                                   AND t1.task_state = 'open'
                                UNION ALL
                                SELECT t1.business_id,
                                       t2.creator_stru draft_unit_id,
                                       to_char(t1.task_id) task_id,
                                       t2.creator_id creator,
                                       t2.business_type bussinesstype,
                                       decode(t1.task_state, 'open', t3.userid_,
                                              'suspended', tp.ssic_id) ownerssicid,
                                       tp.ssic_id,
                                       tp.outgoing_name, --修改为outgoing_name xiesy 0816
                                       t2.title title,
                                       '' || chr(38) || 'jbpm_business_id=' ||
                                       t1.business_id || '' || chr(38) ||
                                       'jbpm_task_id=' || to_char(t1.task_id) || ' ' url,
                                       '' || chr(38) || 'jbpm_business_id=' ||
                                       t1.business_id || '' || chr(38) ||
                                       'jbpm_task_id=' || to_char(tp.task_id) || ' ' urlr, --废止查看地址
                                       to_char(nvl(t1.create_time, SYSDATE),
                                               'yyyymmddhh24miss') createtime,
                                       to_char(t1.take_time, 'yyyymmddhh24miss') taketime,
                                       t1.task_state taskstate,
                                       '' doc_sequence,
                                       '0' action
                                  FROM dams_wf_task         t1,
                                       dams_wf_task         tp,
                                       dams_wf_process      t2,
                                       jbpm4_participation  t3,
                                       dams_wf_process_conf t4
                                 WHERE t1.business_id IN
                                       (SELECT sub_app_id
                                          FROM dams_noti_app_feedback
                                         WHERE business_id = i_doc_id)
                                   AND t1.procinst_id = t2.procinst_id
                                   AND t1.prev_task_id = tp.task_id(+)
                                   AND t2.business_id = t1.business_id
                                   AND t3.task_(+) = t1.task_id
                                   AND t4.pdid = t2.process_id
                                   AND t1.task_state = 'open'
                                UNION ALL
                                SELECT tp.business_id,
                                       t2.creator_stru draft_unit_id,
                                       to_char(tp.task_id) task_id,
                                       t2.creator_id creator,
                                       t2.business_type bussinesstype,
                                       decode(tp.task_state, 'open', t3.userid_,
                                              'suspended', tp.ssic_id) ownerssicid,
                                       t1.ssic_id,
                                       t1.outgoing_name, --修改为outgoing_name xiesy 0816
                                       t2.title title,
                                       '' || chr(38) || 'jbpm_business_id=' ||
                                       tp.business_id || '' || chr(38) ||
                                       'jbpm_task_id=' || to_char(tp.task_id) || ' ' url,
                                       '' || chr(38) || 'jbpm_business_id=' ||
                                       tp.business_id || '' || chr(38) ||
                                       'jbpm_task_id=' || to_char(tp.task_id) || ' ' urlr, --废止查看地址
                                       to_char(nvl(tp.create_time, SYSDATE),
                                               'yyyymmddhh24miss') createtime,
                                       to_char(tp.take_time, 'yyyymmddhh24miss') taketime,
                                       tp.task_state taskstate,
                                       '' doc_sequence,
                                       '0' action
                                  FROM dams_wf_task         t1,
                                       dams_wf_task         tp,
                                       dams_wf_process      t2,
                                       jbpm4_participation  t3,
                                       dams_wf_process_conf t4
                                 WHERE t1.business_id = i_doc_id
                                   AND tp.procinst_id = t2.procinst_id
                                   AND t1.task_id = tp.prev_task_id
                                   AND t2.business_id = tp.business_id
                                   AND t3.task_(+) = tp.task_id
                                   AND t4.pdid = t2.process_id
                                   AND t1.task_state = 'completed'
                                   AND tp.task_state IN ('open', 'suspended')) tt1));
    ELSIF i_handler = 'hbnotice' THEN
    
      OPEN o_task_list FOR
      
        SELECT business_id, --businessid(联合主键 key)
               draft_unit_id, --creator stru
               task_id, -- task id (联合主键 key)
               creator, --创建人 id
               bussinesstype, --业务类型
               ownerssicid, --待办所属人
               ssic_id, --上一节点处理人
               task_name, --上一节点处理名称 修改为outgoing_name xiesy 0816
               title, --标题
               url, --处理地址
               url_view, --废止文档查看地址
               createtime, --创建时间
               taketime, --读取时间
               taskstate, --任务状态 ‘open’待办,‘suspended’废止
               doc_sequence,
               action, --操作项 0 新增；2删除
               syscode
          FROM --所属系统(联合主键 key)
               (SELECT '"business_id":"' || business_id || '"' business_id,
                       '"creator_stru":"' || draft_unit_id || '"' draft_unit_id,
                       '"task_id":"' || task_id || '"' task_id,
                       '"creator_id":"' || creator || '"' creator,
                       '"business_type":"' || bussinesstype || '"' bussinesstype,
                       '"ssic_id":"' || ownerssicid || '"' ownerssicid,
                       '"prev_ssic_id":"' || ssic_id || '"' ssic_id,
                       '"prev_task_name":"' || outgoing_name || '"' task_name,
                       '"title":"' || title || '"' title,
                       '"url":"' || url || '"' url,
                       '"url_view":"' || urlr || '"' url_view,
                       '"create_time":"' || createtime || '"' createtime,
                       '"take_time":"' || taketime || '"' taketime,
                       '"task_state":"' || taskstate || '"' taskstate,
                       '"OUT_SYS_SEQ":"' || doc_sequence || '"' doc_sequence,
                       '"action":"' || action || '"' action,
                       '"sys_code":"AB"' syscode
                  FROM (SELECT business_id,
                               draft_unit_id,
                               task_id,
                               creator,
                               bussinesstype,
                               ownerssicid,
                               ssic_id,
                               outgoing_name,
                               title,
                               url,
                               urlr,
                               createtime,
                               taketime,
                               taskstate,
                               doc_sequence,
                               action
                          FROM (SELECT t1.business_id,
                                       t2.creator_stru draft_unit_id,
                                       to_char(t1.task_id) task_id,
                                       t2.creator_id creator,
                                       t2.business_type bussinesstype,
                                       t1.ssic_id ownerssicid,
                                       t1.prev_ssic_id ssic_id,
                                       t1.prev_task_name outgoing_name, --修改为outgoing_name xiesy 0816
                                       t1.title title,
                                       t1.url url,
                                       t1.url urlr, --废止查看地址
                                       to_char(nvl(t1.create_time, SYSDATE),
                                               'yyyymmddhh24miss') createtime,
                                       '' taketime,
                                       t1.task_state taskstate,
                                       '' doc_sequence,
                                       '0' action
                                  FROM dams_inner_todo_task t1, dams_wf_process t2
                                 WHERE t1.business_id = i_doc_id
                                   AND t2.business_id = t1.business_id
                                   AND EXISTS
                                 (SELECT 1
                                          FROM dams_assist_user tp
                                         WHERE t1.task_id = tp.task_id
                                           AND t1.ssic_id = tp.assist_user_id
                                           AND tp.status = '0')) tt1));
    ELSIF i_handler = 'hbcomplete' THEN
    
      OPEN o_task_list FOR
      
        SELECT business_id, --businessid(联合主键 key)
               draft_unit_id, --creator stru
               task_id, -- task id (联合主键 key)
               creator, --创建人 id
               bussinesstype, --业务类型
               ownerssicid, --待办所属人
               ssic_id, --上一节点处理人
               task_name, --上一节点处理名称 修改为outgoing_name xiesy 0816
               title, --标题
               url, --处理地址
               url_view, --废止文档查看地址
               createtime, --创建时间
               taketime, --读取时间
               taskstate, --任务状态 ‘open’待办,‘suspended’废止
               doc_sequence,
               action, --操作项 0 新增；2删除
               syscode
          FROM --所属系统(联合主键 key)
               (SELECT '"business_id":"' || business_id || '"' business_id,
                       '"creator_stru":"' || draft_unit_id || '"' draft_unit_id,
                       '"task_id":"' || task_id || '"' task_id,
                       '"creator_id":"' || creator || '"' creator,
                       '"business_type":"' || bussinesstype || '"' bussinesstype,
                       '"ssic_id":"' || ownerssicid || '"' ownerssicid,
                       '"prev_ssic_id":"' || ssic_id || '"' ssic_id,
                       '"prev_task_name":"' || outgoing_name || '"' task_name,
                       '"title":"' || title || '"' title,
                       '"url":"' || url || '"' url,
                       '"url_view":"' || urlr || '"' url_view,
                       '"create_time":"' || createtime || '"' createtime,
                       '"take_time":"' || taketime || '"' taketime,
                       '"task_state":"' || taskstate || '"' taskstate,
                       '"OUT_SYS_SEQ":"' || doc_sequence || '"' doc_sequence,
                       '"action":"' || action || '"' action,
                       '"sys_code":"AB"' syscode
                  FROM (SELECT business_id,
                               draft_unit_id,
                               task_id,
                               creator,
                               bussinesstype,
                               ownerssicid,
                               ssic_id,
                               outgoing_name,
                               title,
                               url,
                               urlr,
                               createtime,
                               taketime,
                               taskstate,
                               doc_sequence,
                               action
                          FROM (SELECT t1.business_id,
                                       t2.creator_stru draft_unit_id,
                                       to_char(t1.task_id) task_id,
                                       t2.creator_id creator,
                                       t2.business_type bussinesstype,
                                       tp.assist_user_id ownerssicid,
                                       tp.assist_user_id ssic_id,
                                       '' outgoing_name, --修改为outgoing_name xiesy 0816
                                       t2.title title,
                                       '' || chr(38) || 'jbpm_business_id=' ||
                                       t1.business_id || '' || chr(38) ||
                                       'jbpm_task_id=' || to_char(t1.task_id) || ' ' url,
                                       '' || chr(38) || 'jbpm_business_id=' ||
                                       t1.business_id || '' || chr(38) ||
                                       'jbpm_task_id=' || to_char(tp.task_id) || ' ' urlr, --废止查看地址
                                       to_char(nvl(t1.create_time, SYSDATE),
                                               'yyyymmddhh24miss') createtime,
                                       to_char(t1.take_time, 'yyyymmddhh24miss') taketime,
                                       t1.task_state taskstate,
                                       '' doc_sequence,
                                       '2' action
                                  FROM dams_wf_task         t1,
                                       dams_assist_user     tp,
                                       dams_wf_process      t2,
                                       dams_wf_process_conf t4
                                 WHERE t1.business_id = i_doc_id
                                   AND t1.procinst_id = t2.procinst_id
                                   AND t1.task_id = tp.task_id(+)
                                   AND t2.business_id = t1.business_id
                                   AND t4.pdid = t2.process_id
                                   AND tp.status = '1'
                                 UNION ALL
                                SELECT t1.business_id,
                                       '' draft_unit_id,
                                       to_char(t1.task_id) task_id,
                                       '' creator,
                                       '' bussinesstype,
                                       t1.ssic_id ownerssicid,
                                       t1.prev_ssic_id ssic_id,
                                       t1.prev_task_name outgoing_name, --修改为outgoing_name xiesy 0816
                                       t1.title title,
                                       t1.url url,
                                       t1.url urlr, --废止查看地址
                                       to_char(nvl(t1.create_time, SYSDATE),
                                               'yyyymmddhh24miss') createtime,
                                       '' taketime,
                                       t1.task_state taskstate,
                                       '' doc_sequence,
                                       '2' action
                                  FROM dams_inner_todo_task t1
                                 WHERE t1.business_id = i_doc_id
                                   AND EXISTS
                                 (SELECT 1
                                          FROM dams_assist_user tp
                                         WHERE t1.task_id = tp.task_id
                                           AND t1.ssic_id = tp.assist_user_id
                                           AND tp.status = '1')   
                                UNION ALL
                                SELECT t1.business_id,
                                       t2.creator_stru draft_unit_id,
                                       to_char(t1.task_id) task_id,
                                       t2.creator_id creator,
                                       t2.business_type bussinesstype,
                                       t1.ssic_id ownerssicid,
                                       t1.prev_ssic_id ssic_id,
                                       t1.prev_task_name outgoing_name, --修改为outgoing_name xiesy 0816
                                       t1.title title,
                                       t1.url url,
                                       t1.url urlr, --废止查看地址
                                       to_char(nvl(t1.create_time, SYSDATE),
                                               'yyyymmddhh24miss') createtime,
                                       '' taketime,
                                       t1.task_state taskstate,
                                       '' doc_sequence,
                                       '0' action
                                  FROM dams_inner_todo_task t1, dams_wf_process t2
                                 WHERE t1.business_id = i_doc_id
                                   AND t2.business_id = t1.business_id
                                   AND EXISTS
                                 (SELECT 1
                                          FROM dams_assist_user tp
                                         WHERE t1.task_id = tp.task_id
                                           AND t1.ssic_id = tp.assist_user_id
                                           AND tp.status = '0')
                                 UNION ALL
                                SELECT t1.business_id,
                                       t2.creator_stru draft_unit_id,
                                       to_char(t1.task_id) task_id,
                                       t2.creator_id creator,
                                       t2.business_type bussinesstype,
                                       t1.ssic_id ownerssicid,
                                       t1.prev_ssic_id ssic_id,
                                       t1.prev_task_name outgoing_name, --修改为outgoing_name xiesy 0816
                                       t1.title title,
                                       t1.url url,
                                       t1.url urlr, --废止查看地址
                                       to_char(nvl(t1.create_time, SYSDATE),
                                               'yyyymmddhh24miss') createtime,
                                       '' taketime,
                                       t1.task_state taskstate,
                                       '' doc_sequence,
                                       '0' action
                                  FROM dams_inner_todo_task t1, dams_wf_hist_process t2
                                 WHERE t1.business_id = i_doc_id
                                   AND t2.business_id = t1.business_id
                                   AND EXISTS
                                 (SELECT 1
                                          FROM dams_assist_user tp
                                         WHERE t1.task_id = tp.task_id
                                           AND t1.ssic_id = tp.assist_user_id
                                           AND tp.status = '0')           
                                UNION ALL
                                SELECT t1.business_id,
                                       t2.creator_stru draft_unit_id,
                                       to_char(t1.task_id) task_id,
                                       t2.creator_id creator,
                                       t2.business_type bussinesstype,
                                       decode(t1.task_state, 'open', t3.userid_,
                                              'suspended', tp.ssic_id) ownerssicid,
                                       tp.ssic_id,
                                       tp.outgoing_name, --修改为outgoing_name xiesy 0816
                                       t2.title title,
                                       'jbpm.flowc?flowActionName=open_task_flc' ||
                                       chr(38) || 'jbpm_business_id=' ||
                                       t1.business_id || '' || chr(38) ||
                                       'jbpm_task_id=' || to_char(t1.task_id) || ' ' url,
                                       'jbpm.flowc?flowActionName=open_task_flc' ||
                                       chr(38) || 'jbpm_business_id=' ||
                                       t1.business_id || '' || chr(38) ||
                                       'jbpm_task_id=' || to_char(tp.task_id) || ' ' urlr, --废止查看地址
                                       to_char(nvl(t1.create_time, SYSDATE),
                                               'yyyymmddhh24miss') createtime,
                                       to_char(t1.take_time, 'yyyymmddhh24miss') taketime,
                                       t1.task_state taskstate,
                                       '' doc_sequence,
                                       '0' action
                                  FROM dams_wf_task         t1,
                                       dams_wf_task         tp,
                                       dams_wf_process      t2,
                                       jbpm4_participation  t3,
                                       dams_wf_process_conf t4
                                 WHERE t1.business_id = i_doc_id
                                   AND t1.procinst_id = t2.procinst_id
                                   AND t1.prev_task_id = tp.task_id(+)
                                   AND t2.business_id = t1.business_id
                                   AND t3.task_(+) = t1.task_id
                                   AND t4.pdid = t2.process_id
                                   AND t1.task_state = 'open'
                                
                                ) tt1));
    ELSIF i_handler = 'hbtake' THEN
      OPEN o_task_list FOR
      
        SELECT business_id, --businessid(联合主键 key)
               draft_unit_id, --creator stru
               task_id, -- task id (联合主键 key)
               creator, --创建人 id
               bussinesstype, --业务类型
               ownerssicid, --待办所属人
               ssic_id, --上一节点处理人
               task_name, --上一节点处理名称 修改为outgoing_name xiesy 0816
               title, --标题
               url, --处理地址
               url_view, --废止文档查看地址
               createtime, --创建时间
               taketime, --读取时间
               taskstate, --任务状态 ‘open’待办,‘suspended’废止
               doc_sequence,
               action, --操作项 0 新增；2删除
               syscode
          FROM --所属系统(联合主键 key)
               (SELECT '"business_id":"' || business_id || '"' business_id,
                       '"creator_stru":""' draft_unit_id,
                       '"task_id":""' task_id,
                       '"creator_id":""' creator,
                       '"business_type":""' bussinesstype,
                       '"ssic_id":""' ownerssicid,
                       '"prev_ssic_id":""' ssic_id,
                       '"prev_task_name":""' task_name,
                       '"title":""' title,
                       '"url":""' url,
                       '"url_view":""' url_view,
                       '"create_time":""' createtime,
                       '"take_time":""' taketime,
                       '"task_state":""' taskstate,
                       '"OUT_SYS_SEQ":""' doc_sequence,
                       '"action":"3"' action,
                       '"sys_code":"AB"' syscode
                  FROM ab_rpt_rsk_app_wf
                 WHERE source_business_id = i_doc_id
                UNION ALL (SELECT '"business_id":"' || business_id || '"' business_id,
                                 '"creator_stru":"' || draft_unit_id || '"' draft_unit_id,
                                 '"task_id":"' || task_id || '"' task_id,
                                 '"creator_id":"' || creator || '"' creator,
                                 '"business_type":"' || bussinesstype || '"' bussinesstype,
                                 '"ssic_id":"' || ownerssicid || '"' ownerssicid,
                                 '"prev_ssic_id":"' || ssic_id || '"' ssic_id,
                                 '"prev_task_name":"' || outgoing_name || '"' task_name,
                                 '"title":"' || title || '"' title,
                                 '"url":"' || url || '"' url,
                                 '"url_view":"' || urlr || '"' url_view,
                                 '"create_time":"' || createtime || '"' createtime,
                                 '"take_time":"' || taketime || '"' taketime,
                                 '"task_state":"' || taskstate || '"' taskstate,
                                 '"OUT_SYS_SEQ":"' || doc_sequence || '"' doc_sequence,
                                 '"action":"' || action || '"' action,
                                 '"sys_code":"AB"' syscode
                            FROM (SELECT business_id,
                                         draft_unit_id,
                                         task_id,
                                         creator,
                                         bussinesstype,
                                         ownerssicid,
                                         ssic_id,
                                         outgoing_name,
                                         title,
                                         url,
                                         urlr,
                                         createtime,
                                         taketime,
                                         taskstate,
                                         doc_sequence,
                                         action
                                    FROM (SELECT DISTINCT tp.business_id,
                                                          t2.creator_stru draft_unit_id,
                                                          to_char(t1.task_id) task_id,
                                                          t2.creator_id creator,
                                                          t2.business_type bussinesstype,
                                                          tp.ssic_id ownerssicid,
                                                          t1.ssic_id,
                                                          tp.outgoing_name, --修改为outgoing_name xiesy 0816
                                                          t2.title title,
                                                          'jbpm.flowc?flowActionName=open_task_flc' ||
                                                          chr(38) ||
                                                          'jbpm_business_id=' ||
                                                          tp.business_id || '' ||
                                                          chr(38) ||
                                                          'jbpm_task_id=' ||
                                                          to_char(t1.task_id) || ' ' url,
                                                          'jbpm.flowc?flowActionName=open_task_flc' ||
                                                          chr(38) ||
                                                          'jbpm_business_id=' ||
                                                          tp.business_id || '' ||
                                                          chr(38) ||
                                                          'jbpm_task_id=' ||
                                                          to_char(t1.task_id) || ' ' urlr, --废止查看地址
                                                          to_char(nvl(tp.create_time,
                                                                      SYSDATE),
                                                                  'yyyymmddhh24miss') createtime,
                                                          to_char(t1.take_time,
                                                                  'yyyymmddhh24miss') taketime,
                                                          'open' taskstate,
                                                          '' doc_sequence,
                                                          '0' action
                                            FROM dams_wf_task    t1,
                                                 dams_wf_task    tp,
                                                 dams_wf_process t2
                                           WHERE t1.business_id = i_doc_id
                                             AND tp.procinst_id = t2.procinst_id
                                             AND t1.prev_task_id = tp.task_id
                                             AND t2.business_id = tp.business_id
                                           ORDER BY task_id DESC) tt1
                                   WHERE rownum = 1)));
    
    ELSIF i_handler = 'audit' THEN
    
      OPEN o_task_list FOR
      
        SELECT '"business_id":"' || i_doc_id || '"' business_id,
               '"creator_stru":""' draft_unit_id,
               '"task_id":"' || decode(t1.is_first, '1', '0', t1.task_id) || '"' task_id,
               '"creator_id":""' creator,
               '"business_type":""' bussinesstype,
               '"ssic_id":"' || t1.ssic_id || '"' ownerssicid,
               '"prev_ssic_id":""' ssic_id,
               '"prev_task_name":""' task_name,
               '"title":""' title,
               '"url":""' url,
               '"url_view":""' url_view,
               '"create_time":""' createtime,
               '"take_time":""' taketime,
               '"task_state":""' taskstate,
               '"OUT_SYS_SEQ":""' doc_sequence,
               '"action":"2"' action, --草稿删除3
               '"sys_code":"AB"' syscode
          FROM dams_wf_task t1
         WHERE t1.task_state IN ('completed', 'locked', 'takeback') --包含取回
           AND t1.issend != '3' --kfzx-guoq03 容错性只要状态不为3的 就选出来（防止平台端 遗留）
           AND t1.business_id = i_doc_id
        
        UNION ALL
        SELECT '"business_id":"' || i_doc_id || '"' business_id,
               '"creator_stru":""' draft_unit_id,
               '"task_id":"' || t1.task_id || '"' task_id,
               '"creator_id":""' creator,
               '"business_type":""' bussinesstype,
               '"ssic_id":"' || t1.ssic_id || '"' ownerssicid,
               '"prev_ssic_id":""' ssic_id,
               '"prev_task_name":""' task_name,
               '"title":""' title,
               '"url":""' url,
               '"url_view":""' url_view,
               '"create_time":""' createtime,
               '"take_time":""' taketime,
               '"task_state":""' taskstate,
               '"OUT_SYS_SEQ":""' doc_sequence,
               '"action":"2"' action,
               '"sys_code":"AB"' syscode
          FROM dams_wf_hist_task t1
         WHERE t1.business_id = i_doc_id
        
        UNION ALL
        
        SELECT '"business_id":"' || business_id || '"' business_id,
               '"creator_stru":"' || draft_unit_id || '"' draft_unit_id,
               '"task_id":"' || task_id || '"' task_id,
               '"creator_id":"' || creator || '"' creator,
               '"business_type":"' || bussinesstype || '"' bussinesstype,
               '"ssic_id":"' || ownerssicid || '"' ownerssicid,
               '"prev_ssic_id":"' || ssic_id || '"' ssic_id,
               '"prev_task_name":"' || outgoing_name || '"' task_name,
               '"title":"' || title || '"' title,
               '"url":"' || url || '"' url,
               '"url_view":"' || urlr || '"' url_view,
               '"create_time":"' || createtime || '"' createtime,
               '"take_time":"' || taketime || '"' taketime,
               '"task_state":"' || taskstate || '"' taskstate,
               '"OUT_SYS_SEQ":"' || doc_sequence || '"' doc_sequence,
               '"action":"' || action || '"' action,
               '"sys_code":"AB"' syscode
          FROM (SELECT business_id,
                       draft_unit_id,
                       task_id,
                       creator,
                       bussinesstype,
                       ownerssicid,
                       ssic_id,
                       outgoing_name,
                       title,
                       url,
                       urlr,
                       createtime,
                       taketime,
                       taskstate,
                       doc_sequence,
                       action
                  FROM (SELECT t1.business_id,
                               t2.creator_stru draft_unit_id,
                               to_char(t1.task_id) task_id,
                               t2.creator_id creator,
                               t2.business_type bussinesstype,
                               decode(t1.task_state, 'open', t3.userid_,
                                      'suspended', tp.ssic_id) ownerssicid,
                               tp.ssic_id,
                               tp.outgoing_name, --修改为outgoing_name xiesy 0816
                               t2.title title,
                               'jbpm.flowc?flowActionName=open_task_flc' ||
                               chr(38) || 'jbpm_business_id=' || t1.business_id || '' ||
                               chr(38) || 'jbpm_task_id=' || to_char(t1.task_id) || ' ' url,
                               'jbpm.flowc?flowActionName=open_task_flc' ||
                               chr(38) || 'jbpm_business_id=' || t1.business_id || '' ||
                               chr(38) || 'jbpm_task_id=' || to_char(tp.task_id) || ' ' urlr, --废止查看地址
                               to_char(nvl(t1.create_time, SYSDATE),
                                       'yyyymmddhh24miss') createtime,
                               to_char(t1.take_time, 'yyyymmddhh24miss') taketime,
                               t1.task_state taskstate,
                               '' doc_sequence,
                               '0' action
                          FROM dams_wf_task         t1,
                               dams_wf_task         tp,
                               dams_wf_process      t2,
                               jbpm4_participation  t3,
                               dams_wf_process_conf t4
                         WHERE t1.business_id = i_doc_id
                           AND t1.procinst_id = t2.procinst_id
                           AND t1.prev_task_id = tp.task_id(+)
                           AND t2.business_id = t1.business_id
                           AND t3.task_(+) = t1.task_id
                           AND t4.pdid = t2.process_id
                           AND t1.task_state = 'open'
                        UNION ALL
                        SELECT tp.business_id,
                               t2.creator_stru draft_unit_id,
                               to_char(tp.task_id) task_id,
                               t2.creator_id creator,
                               t2.business_type bussinesstype,
                               decode(tp.task_state, 'open', t3.userid_,
                                      'suspended', tp.ssic_id) ownerssicid,
                               t1.ssic_id,
                               t1.outgoing_name, --修改为outgoing_name xiesy 0816
                               t2.title title,
                               'jbpm.flowc?flowActionName=open_task_flc' ||
                               chr(38) || 'jbpm_business_id=' || tp.business_id || '' ||
                               chr(38) || 'jbpm_task_id=' || to_char(tp.task_id) || ' ' url,
                               'jbpm.flowc?flowActionName=open_task_flc' ||
                               chr(38) || 'jbpm_business_id=' || tp.business_id || '' ||
                               chr(38) || 'jbpm_task_id=' || to_char(tp.task_id) || ' ' urlr, --废止查看地址
                               to_char(nvl(tp.create_time, SYSDATE),
                                       'yyyymmddhh24miss') createtime,
                               to_char(tp.take_time, 'yyyymmddhh24miss') taketime,
                               tp.task_state taskstate,
                               '' doc_sequence,
                               '0' action
                          FROM dams_wf_task         t1,
                               dams_wf_task         tp,
                               dams_wf_process      t2,
                               jbpm4_participation  t3,
                               dams_wf_process_conf t4
                         WHERE t1.business_id = i_doc_id
                           AND tp.procinst_id = t2.procinst_id
                           AND t1.task_id = tp.prev_task_id
                           AND t2.business_id = tp.business_id
                           AND t3.task_(+) = tp.task_id
                           AND t4.pdid = t2.process_id
                           AND t1.task_state = 'completed'
                           AND tp.task_state IN ('open', 'suspended')
                        
                        UNION ALL
                        SELECT t1.business_id,
                               t2.creator_stru draft_unit_id,
                               to_char(t1.task_id) task_id,
                               t2.creator_id creator,
                               t2.business_type bussinesstype,
                               t1.ssic_id ownerssicid,
                               t1.prev_ssic_id ssic_id,
                               t1.prev_task_name outgoing_name, --修改为outgoing_name xiesy 0816
                               t1.title title,
                               t1.url url,
                               t1.url urlr, --废止查看地址
                               to_char(nvl(t1.create_time, SYSDATE),
                                       'yyyymmddhh24miss') createtime,
                               '' taketime,
                               t1.task_state taskstate,
                               '' doc_sequence,
                               '0' action
                          FROM dams_inner_todo_task t1,
                               dams_wf_hist_process t2,
                               dams_assist_user     tp
                         WHERE t1.business_id = i_doc_id
                           AND t2.business_id = t1.business_id
                           AND t1.task_id = tp.task_id
                           AND t1.ssic_id = tp.assist_user_id
                           AND tp.status = '0'
                        UNION ALL
                        SELECT t1.business_id,
                               t2.creator_stru draft_unit_id,
                               to_char(t1.task_id) task_id,
                               t2.creator_id creator,
                               t2.business_type bussinesstype,
                               decode(t1.task_state, 'open', t3.userid_,
                                      'suspended', tp.ssic_id) ownerssicid,
                               tp.ssic_id,
                               t1.task_name outgoing_name, --修改为outgoing_name xiesy 0816
                               t2.title title,
                               'jbpm.flowc?flowActionName=open_task_flc' ||
                               chr(38) || 'jbpm_business_id=' || t1.business_id || '' ||
                               chr(38) || 'jbpm_task_id=' || to_char(t1.task_id) || ' ' url,
                               'jbpm.flowc?flowActionName=open_task_flc' ||
                               chr(38) || 'jbpm_business_id=' || t1.business_id || '' ||
                               chr(38) || 'jbpm_task_id=' || to_char(tp.task_id) || ' ' urlr, --废止查看地址
                               to_char(nvl(t1.create_time, SYSDATE),
                                       'yyyymmddhh24miss') createtime,
                               to_char(t1.take_time, 'yyyymmddhh24miss') taketime,
                               t1.task_state taskstate,
                               '' doc_sequence,
                               '0' action
                          FROM dams_wf_task         t1,
                               dams_wf_task         tp,
                               dams_wf_process      t2,
                               jbpm4_participation  t3,
                               dams_wf_process_conf t4
                         WHERE t1.business_id IN
                               (SELECT source_business_id
                                  FROM ab_rpt_rsk_app_wf
                                 WHERE business_id = i_doc_id)
                           AND t1.procinst_id = t2.procinst_id
                           AND t1.prev_task_id = tp.task_id(+)
                           AND t2.business_id = t1.business_id
                           AND t3.task_(+) = t1.task_id
                           AND t4.pdid = t2.process_id
                           AND t1.task_state = 'open') tt1);
    ELSE
      --表示办结了和拟稿保存
      IF v_existnum = 0 THEN
      
        OPEN o_task_list FOR
          SELECT business_id, --businessid(联合主键 key)
                 draft_unit_id, --creator stru
                 task_id, -- task id (联合主键 key)
                 creator, --创建人 id
                 bussinesstype, --业务类型
                 ownerssicid, --待办所属人
                 ssic_id, --上一节点处理人
                 task_name, --上一节点处理名称 修改为outgoing_name xiesy 0816
                 title, --标题
                 url, --处理地址
                 url_view, --废止文档查看地址
                 createtime, --创建时间
                 taketime, --读取时间
                 taskstate, --任务状态 ‘open’待办,‘suspended’废止
                 action, --操作项 0 新增；2删除
                 syscode
            FROM --所属系统(联合主键 key)
                 (SELECT '"business_id":"' || i_doc_id || '"' business_id,
                         '"creator_stru":""' draft_unit_id,
                         '"task_id":""' task_id,
                         '"creator_id":""' creator,
                         '"business_type":""' bussinesstype,
                         '"ssic_id":""' ownerssicid,
                         '"prev_ssic_id":""' ssic_id,
                         '"prev_task_name":""' task_name,
                         '"title":""' title,
                         '"url":""' url,
                         '"url_view":""' url_view,
                         '"create_time":""' createtime,
                         '"take_time":""' taketime,
                         '"task_state":""' taskstate,
                         '"OUT_SYS_SEQ":""' doc_sequence,
                         '"action":"3"' action,
                         '"sys_code":"AB"' syscode
                    FROM dual
                  UNION ALL (SELECT '"business_id":"' || business_id || '"' business_id,
                                   '"creator_stru":"' || draft_unit_id || '"' draft_unit_id,
                                   '"task_id":"' || task_id || '"' task_id,
                                   '"creator_id":"' || creator || '"' creator,
                                   '"business_type":"' || bussinesstype || '"' bussinesstype,
                                   '"ssic_id":"' || ownerssicid || '"' ownerssicid,
                                   '"prev_ssic_id":"' || ssic_id || '"' ssic_id,
                                   '"prev_task_name":"' || outgoing_name || '"' task_name,
                                   '"title":"' || title || '"' title,
                                   '"url":"' ||
                                   decode(taskstate, 'open',
                                          'damstasklist.flowc?flowActionName=formURLqry',
                                          'suspended', 'damstasklist.flowc') || url || '"' url,
                                   '"url_view":"' ||
                                   decode(taskstate, 'open', '', 'suspended',
                                          'jbpm.flowc?flowActionName=open_task_flc' ||
                                           chr(38) || 'mask=read' || urlr) || '"' url_view,
                                   '"create_time":"' || createtime || '"' createtime,
                                   '"take_time":"' || taketime || '"' taketime,
                                   '"task_state":"' || taskstate || '"' taskstate,
                                   '"OUT_SYS_SEQ":"' || doc_sequence || '"' doc_sequence,
                                   '"action":"' || action || '"' action,
                                   '"sys_code":"AB"' syscode
                              FROM (SELECT business_id,
                                           draft_unit_id,
                                           task_id,
                                           creator,
                                           bussinesstype,
                                           ownerssicid,
                                           ssic_id,
                                           outgoing_name,
                                           title,
                                           url,
                                           urlr,
                                           createtime,
                                           taketime,
                                           taskstate,
                                           doc_sequence,
                                           action
                                      FROM (
                                            --草稿待办
                                            SELECT t.app_id business_id,
                                                    t.app_stru draft_unit_id,
                                                    '0' task_id,
                                                    t.applyer creator,
                                                    t.biz_type bussinesstype,
                                                    t.applyer ownerssicid,
                                                    t.applyer ssic_id,
                                                    '拟稿' outgoing_name, --修改为outgoing_name xiesy0816
                                                    t.info title, --20150821 ggc 总经理标题去掉’草稿‘
                                                    '' || chr(38) ||
                                                    'jbpm_business_id=' || t.app_id || '' ||
                                                    chr(38) || 'type=3&form_id=' ||
                                                    pckg_dams_task.func_get_form_id_from_pdid(t.pdid,
                                                                                              t.app_id) url,
                                                    '' urlr,
                                                    to_char(t.app_time,
                                                            'yyyymmddhh24miss') createtime,
                                                    to_char(t.app_time,
                                                            'yyyymmddhh24miss') taketime,
                                                    'open' taskstate, --草稿待办
                                                    '' doc_sequence,
                                                    decode(t.valid_flag, '0', '2',
                                                           '0') action
                                              FROM dams_save_info       t,
                                                    dams_wf_process_conf t1
                                             WHERE t.app_id = i_doc_id
                                               AND t1.pdid = t.pdid) tt1)));
      
      ELSE
      
        OPEN o_task_list FOR
        
          SELECT business_id, --businessid(联合主键 key)
                 draft_unit_id, --creator stru
                 task_id, -- task id (联合主键 key)
                 creator, --创建人 id
                 bussinesstype, --业务类型
                 ownerssicid, --待办所属人
                 ssic_id, --上一节点处理人
                 task_name, --上一节点处理名称 修改为outgoing_name xiesy 0816
                 title, --标题
                 url, --处理地址
                 url_view, --废止文档查看地址
                 createtime, --创建时间
                 taketime, --读取时间
                 taskstate, --任务状态 ‘open’待办,‘suspended’废止
                 doc_sequence,
                 action, --操作项 0 新增；2删除
                 syscode
            FROM --所属系统(联合主键 key)
                 (SELECT '"business_id":"' || i_doc_id || '"' business_id,
                         '"creator_stru":""' draft_unit_id,
                         '"task_id":"' ||
                         decode(t1.is_first, '1', '0', t1.task_id) || '"' task_id,
                         '"creator_id":""' creator,
                         '"business_type":""' bussinesstype,
                         '"ssic_id":"' || t1.ssic_id || '"' ownerssicid,
                         '"prev_ssic_id":""' ssic_id,
                         '"prev_task_name":""' task_name,
                         '"title":""' title,
                         '"url":""' url,
                         '"url_view":""' url_view,
                         '"create_time":""' createtime,
                         '"take_time":""' taketime,
                         '"task_state":""' taskstate,
                         '"OUT_SYS_SEQ":""' doc_sequence,
                         '"action":"2"' action, --草稿删除3
                         '"sys_code":"AB"' syscode
                    FROM dams_wf_task t1
                   WHERE t1.task_state IN ('completed', 'locked', 'takeback','drop') --包含取回
                     AND t1.issend != '3' --kfzx-guoq03 容错性只要状态不为3的 就选出来（防止平台端 遗留）
                     AND t1.business_id = i_doc_id
                  
                  UNION ALL
                  SELECT '"business_id":"' || i_doc_id || '"' business_id,
                         '"creator_stru":""' draft_unit_id,
                         '"task_id":"' || t1.task_id || '"' task_id,
                         '"creator_id":""' creator,
                         '"business_type":""' bussinesstype,
                         '"ssic_id":"' || t1.ssic_id || '"' ownerssicid,
                         '"prev_ssic_id":""' ssic_id,
                         '"prev_task_name":""' task_name,
                         '"title":""' title,
                         '"url":""' url,
                         '"url_view":""' url_view,
                         '"create_time":""' createtime,
                         '"take_time":""' taketime,
                         '"task_state":""' taskstate,
                         '"OUT_SYS_SEQ":""' doc_sequence,
                         '"action":"2"' action,
                         '"sys_code":"AB"' syscode
                    FROM dams_wf_hist_task t1
                   WHERE t1.business_id = i_doc_id
                  
                  UNION ALL
                  
                  SELECT '"business_id":"' || business_id || '"' business_id,
                         '"creator_stru":"' || draft_unit_id || '"' draft_unit_id,
                         '"task_id":"' || task_id || '"' task_id,
                         '"creator_id":"' || creator || '"' creator,
                         '"business_type":"' || bussinesstype || '"' bussinesstype,
                         '"ssic_id":"' || ownerssicid || '"' ownerssicid,
                         '"prev_ssic_id":"' || ssic_id || '"' ssic_id,
                         '"prev_task_name":"' || outgoing_name || '"' task_name,
                         '"title":"' || title || '"' title,
                         '"url":"' ||
                         decode(taskstate, 'open',
                                'jbpm.flowc?flowActionName=open_task_flc',
                                'suspended', 'damstasklist.flowc') || url || '"' url,
                         '"url_view":"' ||
                         decode(taskstate, 'open', '', 'suspended',
                                'jbpm.flowc?flowActionName=open_task_flc' ||
                                 chr(38) || 'mask=read' || urlr) || '"' url_view,
                         '"create_time":"' || createtime || '"' createtime,
                         '"take_time":"' || taketime || '"' taketime,
                         '"task_state":"' || taskstate || '"' taskstate,
                         '"OUT_SYS_SEQ":"' || doc_sequence || '"' doc_sequence,
                         '"action":"' || action || '"' action,
                         '"sys_code":"AB"' syscode
                    FROM (SELECT business_id,
                                 draft_unit_id,
                                 task_id,
                                 creator,
                                 bussinesstype,
                                 ownerssicid,
                                 ssic_id,
                                 outgoing_name,
                                 title,
                                 url,
                                 urlr,
                                 createtime,
                                 taketime,
                                 taskstate,
                                 doc_sequence,
                                 action
                            FROM (SELECT t1.business_id,
                                         t2.creator_stru draft_unit_id,
                                         to_char(t1.task_id) task_id,
                                         t2.creator_id creator,
                                         t2.business_type bussinesstype,
                                         decode(t1.task_state, 'open', t3.userid_,
                                                'suspended', tp.ssic_id) ownerssicid,
                                         tp.ssic_id,
                                         tp.outgoing_name, --修改为outgoing_name xiesy 0816
                                         t2.title title,
                                         '' || chr(38) || 'jbpm_business_id=' ||
                                         t1.business_id || '' || chr(38) ||
                                         'jbpm_task_id=' || to_char(t1.task_id) || ' ' url,
                                         '' || chr(38) || 'jbpm_business_id=' ||
                                         t1.business_id || '' || chr(38) ||
                                         'jbpm_task_id=' || to_char(tp.task_id) || ' ' urlr, --废止查看地址
                                         to_char(nvl(t1.create_time, SYSDATE),
                                                 'yyyymmddhh24miss') createtime,
                                         to_char(t1.take_time, 'yyyymmddhh24miss') taketime,
                                         t1.task_state taskstate,
                                         '' doc_sequence,
                                         '0' action
                                    FROM dams_wf_task         t1,
                                         dams_wf_task         tp,
                                         dams_wf_process      t2,
                                         jbpm4_participation  t3,
                                         dams_wf_process_conf t4
                                   WHERE t1.business_id = i_doc_id
                                     AND t1.procinst_id = t2.procinst_id
                                     AND t1.prev_task_id = tp.task_id(+)
                                     AND t2.business_id = t1.business_id
                                     AND t3.task_(+) = t1.task_id
                                     AND t4.pdid = t2.process_id
                                     AND t1.task_state = 'open'
                                  UNION ALL
                                  SELECT tp.business_id,
                                         t2.creator_stru draft_unit_id,
                                         to_char(tp.task_id) task_id,
                                         t2.creator_id creator,
                                         t2.business_type bussinesstype,
                                         decode(tp.task_state, 'open', t3.userid_,
                                                'suspended', tp.ssic_id) ownerssicid,
                                         t1.ssic_id,
                                         t1.outgoing_name, --修改为outgoing_name xiesy 0816
                                         t2.title title,
                                         '' || chr(38) || 'jbpm_business_id=' ||
                                         tp.business_id || '' || chr(38) ||
                                         'jbpm_task_id=' || to_char(tp.task_id) || ' ' url,
                                         '' || chr(38) || 'jbpm_business_id=' ||
                                         tp.business_id || '' || chr(38) ||
                                         'jbpm_task_id=' || to_char(tp.task_id) || ' ' urlr, --废止查看地址
                                         to_char(nvl(tp.create_time, SYSDATE),
                                                 'yyyymmddhh24miss') createtime,
                                         to_char(tp.take_time, 'yyyymmddhh24miss') taketime,
                                         tp.task_state taskstate,
                                         '' doc_sequence,
                                         '0' action
                                    FROM dams_wf_task         t1,
                                         dams_wf_task         tp,
                                         dams_wf_process      t2,
                                         jbpm4_participation  t3,
                                         dams_wf_process_conf t4
                                   WHERE t1.business_id = i_doc_id
                                     AND tp.procinst_id = t2.procinst_id
                                     AND t1.task_id = tp.prev_task_id
                                     AND t2.business_id = tp.business_id
                                     AND t3.task_(+) = tp.task_id
                                     AND t4.pdid = t2.process_id
                                     AND t1.task_state = 'completed'
                                     AND tp.task_state IN ('open', 'suspended')) tt1));
      
        IF v_ex_save <> 0 THEN
          --拟稿保存
          UPDATE dams_wf_task t
             SET t.issend = '3'
           WHERE t.business_id = i_doc_id
             AND t.is_first = '1';
        END IF;
      END IF;
    END IF;
    o_flag := '0';
    o_msg  := '';
  
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, v_step, v_param);
      o_flag := '-1';
      o_msg  := 'PCKG_DAMS_WEBSERVICE.proc_senddata2dams_list' || ':' ||
                SQLERRM;
      IF o_task_list%ISOPEN THEN
        CLOSE o_task_list;
      END IF;
    
  END proc_senddata2dams_list;
  
END pckg_dams_webservice;
//



CREATE OR REPLACE PACKAGE pckg_dams_role IS

  TYPE ret_list IS REF CURSOR;
  TYPE arrytype IS TABLE OF VARCHAR2(500);

  PROCEDURE proc_role_list(i_branchid IN VARCHAR2, --机构编码
                           --i_branchLevel  IN VARCHAR2, --机构级别
                           i_system    IN VARCHAR2, --??
                           i_language  IN VARCHAR2, --语言编码
                           i_begnum    IN VARCHAR2, --开始取数
                           i_fetchnum  IN VARCHAR2, --获取数
                           i_keyword   IN VARCHAR2, --角色名称关键字
                           i_syscode   IN VARCHAR2, --子系统id
                           o_flag      OUT VARCHAR2, --存储过程返回标志
                           o_totalnum  OUT VARCHAR2, --最终获取数
                           o_role_list OUT ret_list --角色列表
                           );

  PROCEDURE proc_item_list(i_branchid      IN VARCHAR2, --机构编码
                           i_branchlevel   IN VARCHAR2, --机构级别
                           i_language      IN VARCHAR2, --语言编码
                           o_flag          OUT VARCHAR2, --存储过程返回标志
                           o_ret_item_list OUT ret_list --角色列表
                           );

  PROCEDURE proc_role_save(i_name      IN VARCHAR2, --角色名称
                           i_system    IN VARCHAR2, --所属系统
                           i_level     IN VARCHAR2, --所属层级
                           i_branchid  IN VARCHAR2, --创建机构编码
                           i_detail    IN VARCHAR2, --角色描述
                           i_status    IN VARCHAR2, --角色状态
                           i_items     IN VARCHAR2, --操作项
                           i_user      IN VARCHAR2, --操作用户
                           i_language  IN VARCHAR2, --语言编码
                           i_isadmincb IN VARCHAR2, --管理员角色选择,role_type
                           o_flag      OUT VARCHAR2, --存储过程返回标志
                           o_msg       OUT VARCHAR2 --返回信息
                           );

  PROCEDURE proc_role_update(i_id        IN VARCHAR2, --角色id
                             i_name      IN VARCHAR2, --角色名称
                             i_system    IN VARCHAR2, --所属系统
                             i_level     IN VARCHAR2, --所属层级
                             i_branchid  IN VARCHAR2, --创建机构编码
                             i_detail    IN VARCHAR2, --角色描述
                             i_status    IN VARCHAR2, --角色状态
                             i_items     IN VARCHAR2, --操作项
                             i_user      IN VARCHAR2, --操作用户
                             i_isadmincb IN VARCHAR2, --管理员角色选择,role_type
                             o_flag      OUT VARCHAR2, --存储过程返回标志
                             o_msg       OUT VARCHAR2 --返回信息
                             );

  PROCEDURE proc_role_delete(i_id   IN VARCHAR2, --角色id
                             i_user IN VARCHAR2, --操作用户
                             o_flag OUT VARCHAR2, --存储过程返回标志
                             o_msg  OUT VARCHAR2 --返回信息
                             );

  PROCEDURE proc_role_query(i_id      IN VARCHAR2, --角色名称
                            o_flag    OUT VARCHAR2, --存储过程返回标志
                            o_id      OUT VARCHAR2, --id
                            o_name    OUT VARCHAR2, --所属系统
                            o_system  OUT VARCHAR2, --所属系统
                            o_isadmin OUT VARCHAR2, --角色是否管理员
                            o_detail  OUT VARCHAR2, --角色描述
                            o_status  OUT VARCHAR2, --角色状态
                            o_items   OUT VARCHAR2 --操作项

                            );

  PROCEDURE proc_user_role_list(i_syscode        IN VARCHAR2,
                                i_stru           IN VARCHAR2,
                                o_flag           OUT VARCHAR2, --存储过程返回标志
                                o_role_list      OUT ret_list, --角色列表
                                o_user_role_list OUT ret_list --人员角色列表

                                );

  PROCEDURE proc_syscode_list(o_flag         OUT VARCHAR2, --存储过程返回标志
                              o_syscode_list OUT ret_list --系统编号列表

                              );


  FUNCTION func_common_getarray(tmpstr IN VARCHAR2,
                                param  IN VARCHAR2) RETURN arrytype;

  

END pckg_dams_role;
//
CREATE OR REPLACE PACKAGE BODY pckg_dams_role IS
  PROCEDURE proc_role_list(i_branchid IN VARCHAR2, --机构编码
                           i_system    IN VARCHAR2, --??
                           i_language  IN VARCHAR2, --语言编码
                           i_begnum    IN VARCHAR2, --开始取数
                           i_fetchnum  IN VARCHAR2, --获取数
                           i_keyword   IN VARCHAR2, --角色名称关键字
                           i_syscode   IN VARCHAR2, --子系统id
                           o_flag      OUT VARCHAR2, --存储过程返回标志
                           o_totalnum  OUT VARCHAR2, --最终获取数
                           o_role_list OUT ret_list --角色列表
                           ) IS
    v_where     VARCHAR2(1500) := ' where 1=1 ';
    v_statement VARCHAR2(5000);
  BEGIN
    o_flag := '0';

    IF i_keyword IS NOT NULL THEN
      v_where := v_where || ' and rnls.role_name like ''%' || i_keyword ||
                 '%'' ';
    END IF;
   
    IF i_system IS NOT NULL THEN
      v_where := v_where || ' and role.sys_code  = ''' || i_system || ''' ';
    
    END IF;
    IF i_syscode IS NOT NULL THEN
      v_where := v_where || ' and role.sys_code = ''' || i_syscode || ''' ';
    END IF;
    IF i_language IS NOT NULL THEN
      v_where := v_where || ' and rnls.locale=''' || i_language || ''' ';
    ELSE
      v_where := v_where || ' and rnls.locale=''zh_CN''  ';
    END IF;
    v_statement := 'select role.role_id,
                     rnls.ROLE_NAME,
                     rnls.role_desc,
                     nvl2(role.role_type,''是'',''否'') isAdmin,
                     role.stru_id,
                     bnch.stru_sname,
                     sc.sys_name sysname,
                     role.creator,
                     rownum num
                from dams_role role
                left join dams_role_nls rnls
                  on role.role_id = rnls.role_id
                   left join dams_branch bnch
                  on bnch.stru_id = role.stru_id
                left join dams_sys_conf sc
                  on sc.sys_code = role.sys_code
                  ' || v_where;

    COMMIT;
    EXECUTE IMMEDIATE 'SELECT COUNT(*) from (' || v_statement || ')'
      INTO o_totalnum;

    OPEN o_role_list FOR 'select role_id,
             ROLE_NAME,
             role_desc,
             isAdmin,
             stru_id,
             stru_sname,
             sysname,
             creator
        from (' || v_statement || ')
        where num < (to_number(' || i_begnum || ') + to_number(' || i_fetchnum || '))
        and num >= to_number(' || i_begnum || ')';

  EXCEPTION
    WHEN OTHERS THEN
      IF o_role_list%ISOPEN THEN
        CLOSE o_role_list;
      END IF;

      o_flag := '-1';
      RETURN;
  END proc_role_list;


  PROCEDURE proc_item_list(i_branchid      IN VARCHAR2, --机构编码
                           i_branchlevel   IN VARCHAR2, --机构级别
                           i_language      IN VARCHAR2, --语言编码
                           o_flag          OUT VARCHAR2, --存储过程返回标志
                           o_ret_item_list OUT ret_list --角色列表
                           ) IS
    v_temp VARCHAR2(20);
  BEGIN

    IF i_language IS NULL THEN
      v_temp := 'zh_CN';
    ELSE
      v_temp := i_language;
    END IF;
    o_flag := '0';
    OPEN o_ret_item_list FOR

      SELECT a.menu_id   id,
             a.parent_id pid,
             n.menu_name NAME,
             n.menu_desc remark,
             LEVEL,
             b.cnm       childnum
        FROM dams_menu a,
             (SELECT parent_id, COUNT(1) cnm FROM dams_menu GROUP BY parent_id) b,
             dams_menu_nls n
       WHERE a.menu_id = b.parent_id(+)
         AND a.menu_id = n.menu_id
         AND n.locale = v_temp
       START WITH a.parent_id IS NULL
      CONNECT BY PRIOR a.menu_id = a.parent_id;

  EXCEPTION
    WHEN OTHERS THEN
      IF o_ret_item_list%ISOPEN THEN
        CLOSE o_ret_item_list;
      END IF;
      
      o_flag := '-1';
      RETURN;
  END proc_item_list;

  PROCEDURE proc_role_save(i_name      IN VARCHAR2, --角色名称
                           i_system    IN VARCHAR2, --所属系统
                           i_level     IN VARCHAR2, --所属层级
                           i_branchid  IN VARCHAR2, --创建机构编码
                           i_detail    IN VARCHAR2, --角色描述
                           i_status    IN VARCHAR2, --角色状态
                           i_items     IN VARCHAR2, --操作项
                           i_user      IN VARCHAR2, --操作用户
                           i_language  IN VARCHAR2, --语言编码
                           i_isadmincb IN VARCHAR2, --管理员角色选择,role_type
                           o_flag      OUT VARCHAR2, --存储过程返回标志
                           o_msg       OUT VARCHAR2 --返回信息
                           ) IS
    v_temp  VARCHAR2(20);
    v_id    VARCHAR2(20);
    v_items arrytype;
  BEGIN

    IF i_language IS NULL THEN
      v_temp := 'zh_CN';
    ELSE
      v_temp := i_language;
    END IF;

    o_flag := '0';
    o_msg  := '保存成功！';
    pack_log.debug('proc_role_save', to_char(SYSDATE, 'yyyymmdd'),
                   i_name || '|' || i_system || '|' || i_level || '|' ||
                    i_branchid || '|' || i_detail || '|' || i_status || '|' ||
                    i_items || '|' || i_user);

    --获得主键
    SELECT 'RL' || lpad(dams_role_seq.nextval, 8, 0) INTO v_id FROM dual;

    INSERT INTO dams_role
      (role_id,
       role_type,
       sys_code,
       stru_grade,
       stru_id,
       creator,
       create_time,
       lastupdate,
       updator,
       status)
    VALUES
      (v_id,
       i_isadmincb,
       i_system,
       i_level,
       i_branchid,
       i_user,
       to_char(SYSDATE, 'yyyymmdd'),
       to_char(SYSDATE, 'yyyymmdd'),
       i_user,
       i_status);

    INSERT INTO dams_role_nls
      (role_id, role_name, locale, role_desc)
    VALUES
      (v_id, i_name, v_temp, i_detail);

    v_items := func_common_getarray(i_items, '|');
    FOR i IN 1 .. v_items.count
    LOOP
      INSERT INTO dams_acl
        (resource_id, principal_id, principal_type, permission, resource_type)
      VALUES
        (v_items(i), v_id, 'role', 'r', 'menu');
    END LOOP;

  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      o_flag := '-1';
      o_msg  := '保存失败！';
      RETURN;
  END proc_role_save;
  
  PROCEDURE proc_role_update(i_id        IN VARCHAR2, --角色id
                             i_name      IN VARCHAR2, --角色名称
                             i_system    IN VARCHAR2, --所属系统
                             i_level     IN VARCHAR2, --所属层级
                             i_branchid  IN VARCHAR2, --创建机构编码
                             i_detail    IN VARCHAR2, --角色描述
                             i_status    IN VARCHAR2, --角色状态
                             i_items     IN VARCHAR2, --操作项
                             i_user      IN VARCHAR2, --操作用户
                             i_isadmincb IN VARCHAR2, --管理员角色选择,role_type
                             o_flag      OUT VARCHAR2, --存储过程返回标志
                             o_msg       OUT VARCHAR2 --返回信息
                             ) IS
    v_items arrytype;
  BEGIN
    o_flag := '0';
    o_msg  := '保存成功！';
    UPDATE dams_role
       SET sys_code   = i_system,
           stru_grade = i_level,
           stru_id    = i_branchid,
           creator    = i_user,
           lastupdate = to_char(SYSDATE, 'yyyymmdd'),
           updator    = i_user,
           status     = i_status,
           role_type  = i_isadmincb
     WHERE role_id = i_id;

    UPDATE dams_role_nls
       SET role_name = i_name, role_desc = i_detail
     WHERE role_id = i_id;
    DELETE dams_acl WHERE principal_id = i_id;
    v_items := func_common_getarray(i_items, '|');
    FOR i IN 1 .. v_items.count
    LOOP
      INSERT INTO dams_acl
        (resource_id, principal_id, principal_type, permission, resource_type)
      VALUES
        (v_items(i), i_id, 'role', 'r', 'menu');
    END LOOP;
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      o_flag := '-1';
      o_msg  := '保存失败！';
      RETURN;
  END proc_role_update;

  
  
  PROCEDURE proc_role_delete(i_id   IN VARCHAR2, --角色id
                             i_user IN VARCHAR2, --操作用户
                             o_flag OUT VARCHAR2, --存储过程返回标志
                             o_msg  OUT VARCHAR2 --返回信息
                             ) IS
    v_sysadmin VARCHAR2(20);

  BEGIN

    o_flag := '0';
    o_msg  := '删除成功！';

    SELECT t.role_type INTO v_sysadmin FROM dams_role t WHERE t.role_id = i_id;

    IF v_sysadmin = 'SYSADMIN' THEN
      o_flag := '-1';
      o_msg  := '删除失败,超级管理员不能删除！';
    ELSE
      DELETE dams_role WHERE role_id = i_id;

      DELETE dams_acl WHERE principal_id = i_id;

      DELETE dams_role_nls WHERE role_id = i_id;
    END IF;

  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
    
      o_flag := '-1';
      o_msg  := '删除失败！';
      RETURN;
  END proc_role_delete;
  
  
  PROCEDURE proc_role_query(i_id      IN VARCHAR2, --角色名称
                            o_flag    OUT VARCHAR2, --存储过程返回标志
                            o_id      OUT VARCHAR2, --id
                            o_name    OUT VARCHAR2, --所属系统
                            o_system  OUT VARCHAR2, --所属系统
                            o_isadmin OUT VARCHAR2, --角色是否管理员
                            o_detail  OUT VARCHAR2, --角色描述
                            o_status  OUT VARCHAR2, --角色状态
                            o_items   OUT VARCHAR2 --操作项

                            ) IS

  BEGIN
    o_flag := 0;

    pack_log.debug('proc_role_query', to_char(SYSDATE, 'yyyymmdd'), i_id);
    COMMIT;
    SELECT role.role_id,
           nls.role_name,
           role.sys_code,
           nvl2(role.role_type, 'ADMIN', '') isadmin,
           nls.role_desc,
           role.status,
           '1'
      INTO o_id, o_name, o_system, o_isadmin, o_detail, o_status, o_items
      FROM dams_role role, dams_role_nls nls
     WHERE role.role_id = nls.role_id
       AND role.role_id = i_id
       and nls.locale = 'zh_CN';
    SELECT listagg(acl.resource_id) within group(order by acl.resource_id)resource_id
      INTO o_items 
      FROM dams_acl acl
     WHERE acl.principal_id = i_id;
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
     
      o_flag := '-1';
      RETURN;
  END proc_role_query;

  
  PROCEDURE proc_user_role_list(i_syscode        IN VARCHAR2,
                                i_stru           IN VARCHAR2,
                                o_flag           OUT VARCHAR2, --存储过程返回标志
                                o_role_list      OUT ret_list, --角色列表
                                o_user_role_list OUT ret_list --人员角色列表
                                ) IS
    v_step      db_log.step_no%TYPE;
    v_proc_name db_log.proc_name%TYPE := 'pckg_dams_role.proc_user_role_list';
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step, pack_log.start_msg);
    o_flag := '0';
    v_step := '1';
    OPEN o_user_role_list FOR
      SELECT rel.role_id, usr.name || '  [' || bnch.stru_sname || ']'
        FROM dams_user_role_rel rel
        LEFT JOIN dams_user usr
          ON (rel.ssic_id = usr.ssic_id)
        LEFT JOIN dams_branch bnch
          ON (rel.stru_id = bnch.stru_id)
       WHERE rel.sys_code = i_syscode
         AND EXISTS
       (SELECT 1
                FROM dams_branch_relation rela, dams_branch_sys_rel sysr
               WHERE rela.bnch_id = rel.stru_id
                 AND sysr.sys_code = i_syscode
                 AND sysr.stru_id = i_stru
                 AND rela.up_bnch_id = sysr.stru_id
                 AND sysr.start_use = '1' --启用
              )
         AND NOT EXISTS
       (SELECT 1
                FROM dams_branch_relation rela, dams_branch_sys_rel sysr
               WHERE rela.bnch_id = rel.stru_id
                 AND sysr.sys_code = i_syscode
                 AND sysr.stru_id = i_stru
                 AND rela.up_bnch_id = sysr.stru_id
                 AND sysr.start_use = '0' --停用
              )
       ORDER BY rel.role_id, bnch.stru_sname;
    v_step := '2';
    OPEN o_role_list FOR
      SELECT DISTINCT rol.role_id, nls.role_name || '【' || rol.role_id || '】'
        FROM dams_role rol
        LEFT JOIN dams_role_nls nls
          ON (rol.role_id = nls.role_id)
       WHERE rol.sys_code = i_syscode
       ORDER BY rol.role_id;

    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg);
  EXCEPTION
    WHEN OTHERS THEN
      o_flag := '-1';
      IF o_role_list%ISOPEN THEN
        CLOSE o_role_list;
      END IF;
      IF o_user_role_list%ISOPEN THEN
        CLOSE o_user_role_list;
      END IF;
      pack_log.error(v_proc_name, v_step, SQLCODE);

      RETURN;
  END proc_user_role_list;

  
  
  PROCEDURE proc_syscode_list(o_flag         OUT VARCHAR2, --存储过程返回标志
                              o_syscode_list OUT ret_list --子系统列表

                              ) IS
    v_step      db_log.step_no%TYPE;
    v_proc_name db_log.proc_name%TYPE := 'pckg_dams_role.proc_syscode_list';
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step, pack_log.start_msg);
    o_flag := '0';
    v_step := '1';
    OPEN o_syscode_list FOR
      SELECT conf.sys_code, conf.sys_name FROM dams_sys_conf conf;
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg);
  EXCEPTION
    WHEN OTHERS THEN
      o_flag := '-1';
      IF o_syscode_list%ISOPEN THEN
        CLOSE o_syscode_list;
      END IF;
      pack_log.error(v_proc_name, v_step, SQLCODE);

      RETURN;
  END proc_syscode_list;
  
  
  FUNCTION func_common_getarray(tmpstr IN VARCHAR2,
                                param  IN VARCHAR2) RETURN arrytype IS
    i       INTEGER;
    pos     INTEGER;
    k       INTEGER;
    v_count INTEGER := 0;
    objdata arrytype;
  BEGIN
    k       := 1;
    pos     := 1;
    objdata := arrytype();
    i       := instr(tmpstr, param, pos);

    WHILE k <> 0
    LOOP
      k       := instr(tmpstr, param, pos);
      pos     := k + length(param);
      v_count := v_count + 1;
    END LOOP;

    IF i <= 0 THEN
      objdata.extend();
      objdata(1) := tmpstr;
      RETURN objdata;
    END IF;

    pos := 1;

    FOR j IN 1 .. v_count - 1
    LOOP
      objdata.extend();
      i := instr(tmpstr, param, pos);
      objdata(j) := substr(tmpstr, pos, i - 1 - pos + 1);
      pos := i + length(param);
    END LOOP;

    objdata.extend();
    objdata(v_count) := substr(tmpstr, pos, length(tmpstr) - pos + 1);
    RETURN objdata;

  EXCEPTION
    WHEN OTHERS THEN
      RETURN NULL;
  END;
END pckg_dams_role;
//

CREATE OR REPLACE PACKAGE pckg_dams_log_viewer IS

  PROCEDURE proc_get_handle_obj(i_ssic_id  IN VARCHAR2,
                                o_ret_code OUT VARCHAR2, --存储过程返回标志
                                o_list     OUT SYS_REFCURSOR);
  
  PROCEDURE proc_log_query(i_logobj      IN VARCHAR2,
                           i_logtype     IN VARCHAR2,
                           i_logdate_beg IN VARCHAR2,
                           i_logdate_end IN VARCHAR2,
                           i_loghandler  IN VARCHAR2,
                           i_loginfo     IN VARCHAR2,
                           i_begnum      IN VARCHAR2, --开始取数
                           i_fetchnum    IN VARCHAR2, --获取数
                           i_ssic_id     IN VARCHAR2,
                           o_flag        OUT VARCHAR2, --存储过程返回标志
                           o_totalnum    OUT VARCHAR2, --最终获取数
                           o_log_list    OUT SYS_REFCURSOR);

  
  PROCEDURE proc_log_query_detail(i_logkey   IN VARCHAR2,
                                  o_flag     OUT VARCHAR2, --存储过程返回标志
                                  o_log_info OUT CLOB);

  FUNCTION func_str2json(i_loginfo IN VARCHAR2) RETURN CLOB;
END pckg_dams_log_viewer;
//
CREATE OR REPLACE PACKAGE BODY pckg_dams_log_viewer IS

 
  PROCEDURE proc_get_handle_obj(i_ssic_id  IN VARCHAR2,
                                o_ret_code OUT VARCHAR2, --存储过程返回标志
                                o_list     OUT SYS_REFCURSOR) IS
  
    v_param     db_log.info%TYPE := i_ssic_id;
    v_proc_name db_log.proc_name%TYPE := 'pckg_dams_log_viewer.proc_get_handle_obj';
    v_step      db_log.step_no%TYPE := 0;
  
    v_count NUMBER(5);
  
  BEGIN
  
    pack_log.info(v_proc_name, pack_log.start_step,
                  pack_log.start_msg || v_param);
  
    o_ret_code := '0';
  
    SELECT COUNT(1)
      INTO v_count
      FROM dams_user_role_rel t
     WHERE t.role_id IN ('OIS098', 'OIS099')
       AND t.ssic_id = i_ssic_id
       AND t.auth_state IN ('0', '1', '2');
  
    IF (v_count > 0) THEN
      OPEN o_list FOR
        SELECT t.dictcode VALUE, t.dictvalue text
          FROM dams_gen_dict t
         WHERE t.dicttype = 'HAND_OBJ'
           AND t.dictlang = 'zh_CN'
           AND t.valid_flag = '1';
    
    ELSE
      OPEN o_list FOR
        SELECT t.dictcode VALUE, t.dictvalue text
          FROM dams_gen_dict t
         WHERE t.dicttype = 'HAND_OBJ'
           AND t.dictlang = 'zh_CN'
           AND t.valid_flag = '1'
           AND EXISTS (SELECT 1
                  FROM dams_user_role_rel rr, dams_role r
                 WHERE rr.role_id = r.role_id
                   AND r.role_type = 'ADMIN'
                   AND r.sys_code = t.sys_code
                   AND rr.ssic_id = i_ssic_id);
    
    END IF;
  
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step,
                     '异常！' || v_param || SQLERRM);
      o_ret_code := '-1';
  END proc_get_handle_obj;

 
  PROCEDURE proc_log_query(i_logobj      IN VARCHAR2,
                           i_logtype     IN VARCHAR2,
                           i_logdate_beg IN VARCHAR2,
                           i_logdate_end IN VARCHAR2,
                           i_loghandler  IN VARCHAR2,
                           i_loginfo     IN VARCHAR2,
                           i_begnum      IN VARCHAR2, --开始取数
                           i_fetchnum    IN VARCHAR2, --获取数
                           i_ssic_id     IN VARCHAR2,
                           o_flag        OUT VARCHAR2, --存储过程返回标志
                           o_totalnum    OUT VARCHAR2, --最终获取数
                           o_log_list    OUT SYS_REFCURSOR) IS
  
    v_param     db_log.info%TYPE := i_logobj || '|' || i_logtype || '|' ||
                                    i_logdate_beg || '|' || i_logdate_end || '|' ||
                                    i_loghandler || '|' || i_loginfo || '|' ||
                                    i_begnum || '|' || i_fetchnum;
    v_proc_name db_log.proc_name%TYPE := 'pckg_dams_log_viewer.proc_log_query';
    v_step      db_log.step_no%TYPE := 0;
  
    v_count NUMBER(5);
  
  BEGIN
  
    pack_log.info(v_proc_name, pack_log.start_step,
                  pack_log.start_msg || v_param);
  
    o_flag := '0';
  
    SELECT COUNT(1)
      INTO v_count
      FROM dams_user_role_rel t
     WHERE t.role_id IN ('OIS098', 'OIS099')
       AND t.ssic_id = i_ssic_id
       AND t.auth_state IN ('0', '1', '2');
  
    IF (i_logobj IS NOT NULL OR v_count > 0) THEN
    
      SELECT COUNT(1)
        INTO o_totalnum
        FROM dams_operation_loginfo t,
             dams_user              u,
             dams_gen_dict          d,
             dams_gen_dict          d1
       WHERE t.logobj LIKE '%' || i_logobj || '%'
         AND t.logtype LIKE '%' || i_logtype || '%'
         AND t.loghandler = u.ssic_id
         AND u.name LIKE '%' || i_loghandler || '%'
         AND t.loginfo LIKE '%' || i_loginfo || '%'
         AND t.logobj = d.dictcode
         AND d.dicttype = 'HAND_OBJ'
         AND t.logtype = d1.dictcode
         AND d1.dicttype = 'HAND_OBJ_SUB'
         and d.dictlang = d1.dictlang
         and d.dictlang = 'zh_CN'
         AND nvl2(i_logdate_beg, t.logdate, '1') >=
             nvl2(i_logdate_beg, rpad(i_logdate_beg, '14', '0'), '1')
         AND nvl2(i_logdate_end, t.logdate, '1') <=
             nvl2(i_logdate_end, rpad(i_logdate_end, '14', '0'), '1');
    
      OPEN o_log_list FOR
        SELECT keyid,
               logobj,
               logtype,
               loghandler,
               loginfo,
               logdate,
               logdetailflag --若detail为空返回0,否则返回1
          FROM (SELECT t.keyid,
                       d.dictvalue logobj,
                       d1.dictvalue logtype,
                       to_char(to_date(t.logdate, 'yyyymmddhh24miss'),
                               'yyyy-mm-dd hh24:mi:ss') logdate,
                       u.name loghandler,
                       t.loginfo,
                       nvl2(t.logdetail, 1, 0) logdetailflag,
                       row_number() over(ORDER BY t.logdate DESC) num
                  FROM dams_operation_loginfo t,
                       dams_user              u,
                       dams_gen_dict          d,
                       dams_gen_dict          d1
                 WHERE t.loghandler = u.ssic_id
                   AND u.name LIKE '%' || i_loghandler || '%'
                   AND t.logobj LIKE '%' || i_logobj || '%'
                   AND t.logobj = d.dictcode
                   AND d.dicttype = 'HAND_OBJ'
                   AND t.logtype LIKE '%' || i_logtype || '%'
                   AND t.logtype = d1.dictcode
                   AND d1.dicttype = 'HAND_OBJ_SUB'
                   and d.dictlang = d1.dictlang
                   and d.dictlang = 'zh_CN'
                   AND t.loginfo LIKE '%' || i_loginfo || '%'
                   AND nvl2(i_logdate_beg, t.logdate, '1') >=
                       nvl2(i_logdate_beg, rpad(i_logdate_beg, '14', '0'), '1')
                   AND nvl2(i_logdate_end, t.logdate, '1') <=
                       nvl2(i_logdate_end, rpad(i_logdate_end, '14', '0'), '1'))
         WHERE num < (to_number(i_begnum) + to_number(i_fetchnum))
           AND num >= to_number(i_begnum);
    ELSE
    
      SELECT COUNT(1)
        INTO o_totalnum
        FROM dams_operation_loginfo t,
             dams_user              u,
             dams_gen_dict          d,
             dams_gen_dict          d1
       WHERE t.logobj LIKE '%' || i_logobj || '%'
         AND t.logtype LIKE '%' || i_logtype || '%'
         AND t.loghandler = u.ssic_id
         AND u.name LIKE '%' || i_loghandler || '%'
         AND t.loginfo LIKE '%' || i_loginfo || '%'
         AND t.logobj = d.dictcode
         AND d.dicttype = 'HAND_OBJ'
         AND t.logtype = d1.dictcode
         AND d1.dicttype = 'HAND_OBJ_SUB'
         and d.dictlang = d1.dictlang
         and d.dictlang = 'zh_CN'
         AND nvl2(i_logdate_beg, t.logdate, '1') >=
             nvl2(i_logdate_beg, rpad(i_logdate_beg, '14', '0'), '1')
         AND nvl2(i_logdate_end, t.logdate, '1') <=
             nvl2(i_logdate_end, rpad(i_logdate_end, '14', '0'), '1')
         AND EXISTS (SELECT 1
                FROM dams_user_role_rel rr, dams_role r
               WHERE rr.role_id = r.role_id
                 AND r.role_type = 'ADMIN'
                 AND r.sys_code = d.sys_code
                 AND rr.ssic_id = i_ssic_id);
    
      OPEN o_log_list FOR
        SELECT keyid,
               logobj,
               logtype,
               loghandler,
               loginfo,
               logdate,
               logdetailflag --若detail为空返回0,否则返回1
          FROM (SELECT t.keyid,
                       d.dictvalue logobj,
                       d1.dictvalue logtype,
                       to_char(to_date(t.logdate, 'yyyymmddhh24miss'),
                               'yyyy-mm-dd hh24:mi:ss') logdate,
                       u.name loghandler,
                       t.loginfo,
                       nvl2(t.logdetail, 1, 0) logdetailflag,
                       row_number() over(ORDER BY t.logdate DESC) num
                  FROM dams_operation_loginfo t,
                       dams_user              u,
                       dams_gen_dict          d,
                       dams_gen_dict          d1
                 WHERE t.loghandler = u.ssic_id
                   AND u.name LIKE '%' || i_loghandler || '%'
                   AND t.logobj LIKE '%' || i_logobj || '%'
                   AND t.logobj = d.dictcode
                   AND d.dicttype = 'HAND_OBJ'
                   AND t.logtype LIKE '%' || i_logtype || '%'
                   AND t.logtype = d1.dictcode
                   AND d1.dicttype = 'HAND_OBJ_SUB'
                   and d.dictlang = d1.dictlang
                   and d.dictlang = 'zh_CN'
                   AND t.loginfo LIKE '%' || i_loginfo || '%'
                   AND nvl2(i_logdate_beg, t.logdate, '1') >=
                       nvl2(i_logdate_beg, rpad(i_logdate_beg, '14', '0'), '1')
                   AND nvl2(i_logdate_end, t.logdate, '1') <=
                       nvl2(i_logdate_end, rpad(i_logdate_end, '14', '0'), '1')
                   AND EXISTS (SELECT 1
                          FROM dams_user_role_rel rr, dams_role r
                         WHERE rr.role_id = r.role_id
                           AND r.role_type = 'ADMIN'
                           AND r.sys_code = d.sys_code
                           AND rr.ssic_id = i_ssic_id))
        
         WHERE num < (to_number(i_begnum) + to_number(i_fetchnum))
           AND num >= to_number(i_begnum);
    
    END IF;
  
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step,
                     '异常！' || v_param || SQLERRM);
      o_flag := '-1';
  END proc_log_query;

 
  PROCEDURE proc_log_query_detail(i_logkey   IN VARCHAR2,
                                  o_flag     OUT VARCHAR2, --存储过程返回标志
                                  o_log_info OUT CLOB) IS
  
    v_param     db_log.info%TYPE := i_logkey;
    v_proc_name db_log.proc_name%TYPE := 'pckg_dams_log_viewer.proc_log_query_detail';
    v_step      db_log.step_no%TYPE := 0;
  
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step,
                  pack_log.start_msg || v_param);
  
    o_flag := '0';
  
    SELECT nvl(t.logdetail, '')
      INTO o_log_info
      FROM dams_operation_loginfo t
     WHERE t.keyid = i_logkey;
  
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step,
                     '异常！' || v_param || SQLERRM);
      o_flag := '-1';
  END proc_log_query_detail;

  
  FUNCTION func_str2json(i_loginfo IN VARCHAR2) RETURN CLOB IS
    v_items  pckg_dams_role.arrytype;
    v_obj    pckg_dams_role.arrytype;
    v_obj_kv pckg_dams_role.arrytype;
  
    v_obj_info VARCHAR2(20000);
    v_obj_type VARCHAR2(200);
  
    v_ret CLOB := '[';
  
    v_obj_bd VARCHAR2(20000);
    v_temp   VARCHAR2(20000);
  
    v_numflag NUMBER := 1;
  
  BEGIN
    --v_items := pckg_dams_role.func_common_getarray(i_loginfo, ';');
  
    FOR i IN 1 .. v_items.count
    LOOP
      IF v_items(i) IS NOT NULL THEN
        --v_obj      := pckg_dams_role.func_common_getarray(v_items(i), '$');
        v_obj_info := v_obj(1);
        v_obj_type := v_obj(2);
      
        --v_obj_kv := pckg_dams_role.func_common_getarray(v_obj_info, ',');
        v_temp   := '';
        FOR j IN 1 .. v_obj_kv.count
        LOOP
          IF j = 1 THEN
            v_temp := v_temp || '{text:''' ||
                      REPLACE(REPLACE(v_obj_kv(j), ':', ''',value:'''), '|',
                              ''',oldval:''') || '''}';
          ELSE
            v_temp := v_temp || ',{text:''' ||
                      REPLACE(REPLACE(v_obj_kv(j), ':', ''',value:'''), '|',
                              ''',oldval:''') || '''}';
          END IF;
        END LOOP;
      
        IF v_numflag = 1 THEN
          v_obj_bd := '{obj:[' || v_temp || '],optype:''' || v_obj_type ||
                      '''}';
        
        ELSE
          v_obj_bd := ',{obj:[' || v_temp || '],optype:''' || v_obj_type ||
                      '''}';
        END IF;
        v_numflag := v_numflag + 1;
      
        v_ret := v_ret || v_obj_bd;
      END IF;
    END LOOP;
  
    v_ret := v_ret || ']';
  
    RETURN v_ret;
  
  EXCEPTION
    WHEN OTHERS THEN
      RETURN '';
  END func_str2json;
END pckg_dams_log_viewer;
//


CREATE OR REPLACE PACKAGE pckg_dams_jbpm_mngt IS

 

  FUNCTION func_maxid_inside_act(i_activityid IN VARCHAR2,
                                 i_tab_name   IN VARCHAR2,
                                 i_tab_id_col IN VARCHAR2,
                                 i_cfgid      IN VARCHAR2) RETURN VARCHAR2;

  
  PROCEDURE proc_gen_config_data(i_pdkey IN VARCHAR2, --流程定义ID
                                 o_flag  OUT VARCHAR2);


  PROCEDURE proc_gen_config_data_batch(o_flag OUT VARCHAR2);

 
  PROCEDURE proc_activity_key_qry(i_pdid IN VARCHAR2, --流程id
                                  o_flag OUT VARCHAR2, --最终获取数
                                  o_cur  OUT SYS_REFCURSOR);

  
  PROCEDURE proc_process_qry(i_syscode  IN VARCHAR2, --子系统编码
                             o_flag     OUT VARCHAR2,
                             o_totalnum OUT VARCHAR2, --最终获取数
                             o_cur      OUT SYS_REFCURSOR);

 
  PROCEDURE proc_wfprocfg_qry(i_pdid     IN VARCHAR2, --
                              o_flag     OUT VARCHAR2,
                              o_totalnum OUT VARCHAR2, --最终获取数
                              o_cur      OUT SYS_REFCURSOR);

 
  PROCEDURE proc_wfprocfg_oper(i_oper            IN VARCHAR2, --操作类型
                               i_pdid            IN VARCHAR2, --流程定义ID
                               i_process_name    IN VARCHAR2, --流程名称
                               i_process_key     IN VARCHAR2, -- 流程Key
                               i_process_version VARCHAR2, --流程版本
                               i_syscode         IN VARCHAR2, --子系统编号
                               i_status          IN VARCHAR2, --状态
                               i_formid          IN VARCHAR2, -- 表单ID
                               i_form_url        IN VARCHAR2,
                               i_read_url        IN VARCHAR2,
                               i_app_url         IN VARCHAR2,
                               o_flag            OUT VARCHAR2,
                               o_msg             OUT VARCHAR2);
 
  PROCEDURE proc_wfcfg_qry(i_pdid     IN VARCHAR2, --流程id
                           i_cfgid    IN VARCHAR2, --配置项id
                           o_flag     OUT VARCHAR2,
                           o_totalnum OUT VARCHAR2, --最终获取数
                           o_cur      OUT SYS_REFCURSOR);

 
  PROCEDURE proc_wfcfg_oper(i_oper          IN VARCHAR2, --操作类型
                            i_pdid          IN VARCHAR2, --流程定义ID
                            i_activity_id   IN VARCHAR2, --活动ID
                            i_activity_name IN VARCHAR2, --活动名称
                            i_cfgid         IN VARCHAR2, --配置ID
                            i_cacutype      IN VARCHAR2,
                            i_selectrange   IN VARCHAR2,
                            i_mailtype      IN VARCHAR2,
                            o_flag          OUT VARCHAR2,
                            o_msg           OUT VARCHAR2);

 
  PROCEDURE proc_wfuser_qry(i_pdid     IN VARCHAR2, --流程id
                            i_cfgid    IN VARCHAR2, --配置项id
                            o_flag     OUT VARCHAR2,
                            o_totalnum OUT VARCHAR2, --最终获取数
                            o_cur      OUT SYS_REFCURSOR);

 
  PROCEDURE proc_wfuser_oper(i_oper        IN VARCHAR2, --操作类型
                             i_id          IN VARCHAR2, --主键id
                             i_activityid  IN VARCHAR2, --活动id
                             i_pdid        IN VARCHAR2, --流程id
                             i_usertype    IN VARCHAR2, --选择类型
                             i_entityid    IN VARCHAR2, --选择类型对应参数
                             i_desc        IN VARCHAR2, --说明
                             i_preoutgoing IN VARCHAR2, --前一路径
                             i_cfgid       IN VARCHAR2, --配置项id
                             o_flag        OUT VARCHAR2, --返回值
                             o_msg         OUT VARCHAR2);

 
  PROCEDURE proc_wfdisplay_qry(i_pdid     IN VARCHAR2, --流程id
                               i_cfgid    IN VARCHAR2, --配置项id
                               o_flag     OUT VARCHAR2,
                               o_totalnum OUT VARCHAR2, --最终获取数
                               o_cur      OUT SYS_REFCURSOR);

 
  PROCEDURE proc_wfdisplay_oper(i_oper       IN VARCHAR2, --操作类型
                                i_pdid       IN VARCHAR2, --流程id
                                i_activityid IN VARCHAR2, --活动id
                                i_showtype   IN VARCHAR2, --展现方式 1. 展现到具体人 2. 展现到机构 3. 展现到角色
                                i_showaction IN VARCHAR2, --显示动作
                                i_showtask   IN VARCHAR2, --显示任务
                                i_showdealer IN VARCHAR2, --显示处理人
                                i_showoption IN VARCHAR2, --显示意见
                                i_disp_seq   IN VARCHAR2, --配置组序号
                                o_flag       OUT VARCHAR2, --返回值
                                o_msg        OUT VARCHAR2);

 
  PROCEDURE proc_wfactform_qry(i_pdid     IN VARCHAR2, --流程id
                               i_cfgid    IN VARCHAR2, --配置项id
                               o_flag     OUT VARCHAR2,
                               o_totalnum OUT VARCHAR2, --最终获取数
                               o_cur      OUT SYS_REFCURSOR);

 
  PROCEDURE proc_wfactform_oper(i_oper       IN VARCHAR2, --操作类型
                                i_pdid       IN VARCHAR2, --流程id
                                i_activityid IN VARCHAR2, --活动id
                                i_form_url   IN VARCHAR2,
                                i_read_url   IN VARCHAR2,
                                o_flag       OUT VARCHAR2, --返回值
                                o_msg        OUT VARCHAR2);

  
  PROCEDURE proc_wfactalias_qry(i_pdid     IN VARCHAR2, --流程id
                                i_cfgid    IN VARCHAR2, --配置项id
                                o_flag     OUT VARCHAR2,
                                o_totalnum OUT VARCHAR2, --最终获取数
                                o_cur      OUT SYS_REFCURSOR);

 
  PROCEDURE proc_wfactalias_oper(i_oper                IN VARCHAR2, --操作类型
                                 i_pdid                IN VARCHAR2, --流程id
                                 i_activityid          IN VARCHAR2, --活动id
                                 i_cfgid               IN VARCHAR2,
                                 i_incoming_transition IN VARCHAR2,
                                 i_alias               IN VARCHAR2,
                                 o_flag                OUT VARCHAR2, --返回值
                                 o_msg                 OUT VARCHAR2);

 
  PROCEDURE proc_wf_showdict(i_dicttype IN VARCHAR2, --字典类型
                             i_lang     IN VARCHAR2, --语言
                             o_flag     OUT VARCHAR2, --返回值
                             o_cur      OUT SYS_REFCURSOR);

 
  PROCEDURE proc_routable_procinst_qry(i_stru_id   IN VARCHAR2, --机构号
                                       i_sys_code  IN VARCHAR2, --子系统编号
                                       i_biz_type  IN VARCHAR2,
                                       i_beg_num   NUMBER,
                                       i_fetch_num NUMBER,
                                       o_flag      OUT VARCHAR2,
                                       o_total_num OUT NUMBER,
                                       o_cur       OUT SYS_REFCURSOR);

  
  PROCEDURE proc_routable_activity_qry(i_procinst_id IN NUMBER,
                                       --i_language    in varchar2,
                                       o_flag OUT VARCHAR2,
                                       o_cur  OUT SYS_REFCURSOR);
 
  PROCEDURE proc_routable_act_user_qry(i_procinst_id IN NUMBER,
                                       i_task_id     in number,
                                       o_flag        OUT VARCHAR2,
                                       o_cur         OUT SYS_REFCURSOR);

  PROCEDURE proc_routable_role_qry(i_ssic_id IN VARCHAR2,
                                   o_flag    OUT VARCHAR2,
                                   o_cur     OUT SYS_REFCURSOR);

END pckg_dams_jbpm_mngt;
//
CREATE OR REPLACE PACKAGE BODY pckg_dams_jbpm_mngt IS

  PROCEDURE log(i_pdid IN VARCHAR2, i_info IN VARCHAR2) IS
    PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN

    INSERT INTO dams_wf_sync_log
      (pdid, info, tm)
    VALUES
      (i_pdid, i_info, SYSDATE);
    COMMIT;

    dbms_output.put_line(i_info);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
  END log;

  FUNCTION func_maxid_inside_act(i_activityid IN VARCHAR2,
                                 i_tab_name   IN VARCHAR2,
                                 i_tab_id_col IN VARCHAR2,
                                 i_cfgid      IN VARCHAR2) RETURN VARCHAR2 IS
    v_preid  VARCHAR2(255);
    v_maxnum VARCHAR2(3);
    v_idx    INTEGER;
    v_id     VARCHAR2(255);
  BEGIN
    --取得最新的id

    EXECUTE IMMEDIATE 'select max(' || i_tab_id_col || ') from ' ||
                      i_tab_name || ' where activity_id = :1 '
      INTO v_id
      USING i_activityid;

    IF v_id IS NULL THEN
      v_id := v_id || '.1';
      RETURN v_id;
    END IF;

    SELECT instr(v_id, '.', -1, 1) INTO v_idx FROM dual;
    IF v_idx = 0 THEN
      SELECT instr(v_id, '-', -1, 1) INTO v_idx FROM dual;
      IF v_idx = 0 THEN
        SELECT instr(v_id, '_', -1, 1) INTO v_idx FROM dual;
      END IF;
    END IF;

    IF v_idx <> 0 THEN
      v_maxnum := substr(v_id, v_idx + 1);
      v_preid  := substr(v_id, 1, v_idx);
      v_preid  := v_preid || to_char((to_number(v_maxnum) + 1));
      RETURN v_preid;
    ELSE
      RETURN i_activityid;
    END IF;

  END;

  
  PROCEDURE proc_gen_config_data(i_pdkey IN VARCHAR2, --流程定义ID
                                 o_flag  OUT VARCHAR2) IS
    v_pdkey dams_wf_stru_config.pdkey%TYPE;
    --v_pdkey_mirror dams_wf_stru_config.pdkey_mirror%TYPE;
  BEGIN
    --删除旧配置--总分行拆离,同一个key会对应多个mirror,不带_为总行版,带_即多个不同的分行版本
    --v_pdkey := func_get_pdkey(i_pdkey);
    --v_pdkey_mirror := func_get_pdkey_mirror(i_pdid);

    DELETE FROM dams_wf_stru_config sc WHERE sc.pdkey_mirror = i_pdkey;

    --1 找出机构范围与总行的间距
    FOR item IN (SELECT t.stru_range,
                        t.cfgid cfgid,
                        t.pdkey,
                        t.pdkey_mirror,
                        t.descr,
                        r.up_level
                   FROM dams_wf_stru_range_config t, dams_branch_relation r
                  WHERE t.stru_range = r.bnch_id
                    AND r.up_bnch_id = '0010100000'
                    AND t.pdkey_mirror = i_pdkey
                    AND EXISTS (SELECT 1
                           FROM dams_wf_process_conf p
                          WHERE p.process_key = t.pdkey_mirror
                            AND p.process_key = i_pdkey)
                  ORDER BY r.up_level) LOOP
      BEGIN
        --2 循环找出所有祖先节点为range的机构

        INSERT INTO dams_wf_stru_config
          (stru_id, pdkey, cfgid, pdkey_mirror, stru_range)
          SELECT r.bnch_id,
                 item.pdkey,--i_pdkey pdkey,
                 item.cfgid,
                 item.pdkey_mirror pdkey_mirror,
                 item.stru_range
            FROM dams_branch_relation r, dams_branch b
           WHERE r.bnch_id = b.stru_id
             AND r.up_bnch_id = item.stru_range
             AND nvl(r.bnch_lv, ' ') NOT IN
                 ('71', '72', '73', '74', '75', '76', '77', '79', '80')
             AND r.bnch_sign <> '6'
             AND b.stru_sname NOT LIKE '%删除%'
             AND b.stru_state IN ('1', '2');

        log(i_pdkey,
            i_pdkey || '流程-' || item.descr || '-配置绑定设置完毕');
        COMMIT;
      EXCEPTION
        WHEN OTHERS THEN
          o_flag := '-1';
          ROLLBACK;
          log(i_pdkey,
              '!!!' || i_pdkey || '流程-' || item.descr || '-配置绑定设置失败！' ||
              substr(SQLERRM, 1, 3800));
      END;
      <<end_loop>>
      NULL;
    END LOOP;
    COMMIT;
  EXCEPTION
    WHEN no_data_found THEN
      o_flag := '-1';
      RETURN;
    WHEN OTHERS THEN
      o_flag := '-1';
      RETURN;
  END proc_gen_config_data;

  /******************************************************************************
  -- 名称       PCKG_DAMS_JBPM_MNGT.proc_gen_config_data_batch
  -- 功能     　批量生成配置数据
  -- 应用模块   JBPM流程管理-JBPM
  -- REVISIONS:
  --    Ver           Date            Author                 Description
  -- ---------      ----------      ----------        --------------------------
  --   1.0           20120925         kfzx-haozj           create this procedure.*/
  PROCEDURE proc_gen_config_data_batch(o_flag OUT VARCHAR2) IS
    v_proc_name db_log.proc_name%TYPE := 'PCKG_DAMS_JBPM_MNGT.proc_gen_config_data_batch';
    v_step      db_log.step_no%TYPE;
  BEGIN
    FOR process IN (SELECT DISTINCT p.process_key
                      FROM dams_wf_process_conf p
                     WHERE p.status = 'R') LOOP
      proc_gen_config_data(process.process_key, o_flag);
    END LOOP;

  EXCEPTION
    WHEN no_data_found THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || SQLERRM);
      RETURN;
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || SQLERRM);
      RETURN;
  END proc_gen_config_data_batch;

  /******************************************************************************
  -- 名称       PCKG_DAMS_JBPM_MNGT.proc_activity_key_qry
  -- 功能     　查询流程信息
  -- 应用模块   JBPM流程管理-JBPM
  -- REVISIONS:
  --    Ver           Date            Author                 Description
  -- ---------      ----------      ----------        --------------------------
  --   1.0           20130216        kfzx-haozj           create this procedure.*/
  -- 说明：
  PROCEDURE proc_activity_key_qry(i_pdid IN VARCHAR2, --流程id
                                  o_flag OUT VARCHAR2, --最终获取数
                                  o_cur  OUT SYS_REFCURSOR) IS

  BEGIN
    o_flag := '0';

    OPEN o_cur FOR
      SELECT DISTINCT c.id activity_id, c.activity_name
        FROM dams_wf_activity_conf c
       WHERE c.pdid = i_pdid;

  EXCEPTION
    WHEN OTHERS THEN
      o_flag := '-1';
      RETURN;
  END proc_activity_key_qry;

  /******************************************************************************
  -- 名称       PCKG_DAMS_JBPM_MNGT.proc_process_qry
  -- 功能     　查询流程信息
  -- 应用模块   JBPM流程管理-JBPM
  -- REVISIONS:
  --    Ver           Date            Author                 Description
  -- ---------      ----------      ----------        --------------------------
  --   1.0           20130216        kfzx-haozj           create this procedure.*/
  -- 说明：
  PROCEDURE proc_process_qry(i_syscode  IN VARCHAR2, --子系统编码
                             o_flag     OUT VARCHAR2,
                             o_totalnum OUT VARCHAR2, --最终获取数
                             o_cur      OUT SYS_REFCURSOR) IS

  BEGIN
    o_flag := '0';

    SELECT COUNT(*)
      INTO o_totalnum
      FROM dams_wf_process_conf c
     WHERE c.sys_code = nvl(i_syscode, c.sys_code);

    OPEN o_cur FOR
      SELECT c.pdid,
             c.process_name,
             c.process_key,
             c.process_version,
             c.sys_code,
             (SELECT s.sys_sname
                FROM dams_sys_conf s
               WHERE s.sys_code = c.sys_code) sysname,
             c.status
        FROM dams_wf_process_conf c
       WHERE c.sys_code = nvl(i_syscode, c.sys_code)
          OR c.sys_code IS NULL;

  EXCEPTION
    WHEN OTHERS THEN
      o_flag := '-1';
      RETURN;
  END proc_process_qry;

  /******************************************************************************
  -- 名称       PCKG_DAMS_JBPM_MNGT.proc_process_qry
  -- 功能     　查询流程信息
  -- 应用模块   JBPM流程管理-JBPM
  -- REVISIONS:
  --    Ver           Date            Author                 Description
  -- ---------      ----------      ----------        --------------------------
  --   1.0           20130216        kfzx-haozj           create this procedure.*/
  -- 说明：
  PROCEDURE proc_wfprocfg_qry(i_pdid     IN VARCHAR2, --
                              o_flag     OUT VARCHAR2,
                              o_totalnum OUT VARCHAR2, --最终获取数
                              o_cur      OUT SYS_REFCURSOR) IS

  BEGIN
    o_flag := '0';

    SELECT COUNT(*)
      INTO o_totalnum
      FROM dams_wf_process_conf c, dams_wf_process_form f, dams_form f2
     WHERE c.pdid = f.pdid
       AND c.sys_code = f2.sys_code
       AND f.form_id = f2.form_id
       AND c.pdid = i_pdid;

    OPEN o_cur FOR
      SELECT c.pdid,
             c.process_name,
             c.process_key,
             c.process_version,
             (SELECT s.sys_sname
                FROM dams_sys_conf s
               WHERE s.sys_code = c.sys_code) sysname,
             c.status,
             f.form_id,
             f2.form_url,
             f2.read_url,
             f2.app_url
        FROM dams_wf_process_conf c, dams_wf_process_form f, dams_form f2
       WHERE c.pdid = f.pdid
         AND c.sys_code = f2.sys_code
         AND f.form_id = f2.form_id
         AND c.pdid = i_pdid;

  EXCEPTION
    WHEN OTHERS THEN
      o_flag := '-1';
      RETURN;
  END proc_wfprocfg_qry;

  /******************************************************************************
  -- 名称       PCKG_DAMS_JBPM_MNGT.proc_gen_default_data
  -- 功能     　生成流程配置数据
  -- 应用模块   JBPM流程管理-JBPM
  -- REVISIONS:
  --    Ver           Date            Author                 Description
  -- ---------      ----------      ----------        --------------------------
  --   1.0           20130216        kfzx-haozj           create this procedure.*/

  -- 说明：
  PROCEDURE proc_wfprocfg_oper(i_oper            IN VARCHAR2, --操作类型
                               i_pdid            IN VARCHAR2, --流程定义ID
                               i_process_name    IN VARCHAR2, --流程名称
                               i_process_key     IN VARCHAR2, -- 流程Key
                               i_process_version VARCHAR2, --流程版本
                               i_syscode         IN VARCHAR2, --子系统编号
                               i_status          IN VARCHAR2, --状态
                               i_formid          IN VARCHAR2, -- 表单ID
                               i_form_url        IN VARCHAR2,
                               i_read_url        IN VARCHAR2,
                               i_app_url         IN VARCHAR2,
                               o_flag            OUT VARCHAR2,
                               o_msg             OUT VARCHAR2) IS
    v_idx     INTEGER;
    v_key     VARCHAR2(255);
    v_version VARCHAR2(255);
  BEGIN
    o_flag := '0';
    --o_msg  := '操作成功';
    IF i_oper = 'edit' THEN
      o_msg := '修改成功';
      UPDATE dams_wf_process_conf t
         SET t.sys_code     = i_syscode,
             t.process_name = i_process_name,
             t.status       = i_status
       WHERE t.pdid = i_pdid;

      --activity_form / dams_form may relatively change their formIds
      UPDATE dams_form t
         SET t.form_id  = i_formid,
             t.sys_code = i_syscode,
             t.form_url = i_form_url,
             t.read_url = i_read_url,
             t.app_url  = i_app_url
       WHERE EXISTS (SELECT 1
                FROM dams_wf_process_form t1
               WHERE t1.form_id = t.form_id
                 AND t1.pdid = i_pdid);

      UPDATE dams_wf_activity_form t
         SET t.form_id = i_formid
       WHERE EXISTS (SELECT 1
                FROM dams_wf_activity_conf c
               WHERE c.id = t.activity_id
                 AND c.pdid = i_pdid);

      UPDATE dams_wf_process_form t
         SET t.form_id = i_formid
       WHERE t.pdid = i_pdid;

    ELSIF i_oper = 'add' THEN
      o_msg := '新增成功';

      INSERT INTO dams_wf_process_conf
        (pdid,
         process_name,
         process_key,
         process_version,
         sys_code,
         status)
      VALUES
        (i_pdid,
         i_process_name,
         i_process_key,
         i_process_version,
         i_syscode,
         i_status);

      INSERT INTO dams_wf_process_form
        (id, pdid, form_id)
      VALUES
        (i_pdid, i_pdid, i_formid);

      INSERT INTO dams_form
        (form_id,
         form_name,
         form_url,
         sys_code,
         form_desc,
         read_url,
         app_url)
      VALUES
        (i_formid,
         i_process_name,
         i_form_url,
         i_syscode,
         i_process_name,
         i_read_url,
         i_app_url);
    ELSIF i_oper = 'del' THEN
      o_msg := '删除成功';
      DELETE FROM dams_form t
       WHERE EXISTS (SELECT 1
                FROM dams_wf_process_form t1
               WHERE t1.form_id = t.form_id
                 AND t1.pdid = i_pdid);
      DELETE FROM dams_wf_process_form t WHERE t.pdid = i_pdid;
      DELETE FROM dams_wf_process_conf t WHERE t.pdid = i_pdid;

    ELSIF i_oper = 'init' THEN
      o_msg := '初始化成功';
      DELETE FROM dams_form t
       WHERE EXISTS (SELECT 1
                FROM dams_wf_process_form t1
               WHERE t1.form_id = t.form_id
                 AND t1.pdid = i_pdid);
      DELETE FROM dams_wf_process_form t WHERE t.pdid = i_pdid;
      DELETE FROM dams_wf_process_conf t WHERE t.pdid = i_pdid;

      INSERT INTO dams_wf_process_conf
        (pdid,
         process_name,
         process_key,
         process_version,
         sys_code,
         status)
      VALUES
        (i_pdid,
         i_process_name,
         i_process_key,
         i_process_version,
         i_syscode,
         'R');
      INSERT INTO dams_wf_process_form
        (id, pdid, form_id)
      VALUES
        (i_pdid, i_pdid, 'autoGenForm');
      INSERT INTO dams_form
        (form_id,
         form_name,
         form_url,
         sys_code,
         form_desc,
         read_url,
         app_url)
      VALUES
        ('autoGenForm',
         i_process_name,
         '',
         i_syscode,
         i_process_name,
         '',
         '');
    END IF;

    COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      IF i_oper = 'edit' THEN
        o_msg := '修改失败:' || SQLERRM;
      ELSIF i_oper = 'add' THEN
        o_msg := '新增失败';
      ELSIF i_oper = 'del' THEN
        o_msg := '删除失败';
      END IF;
      o_flag := '-1';
      RETURN;
  END proc_wfprocfg_oper;
  /******************************************************************************
  -- 名称       PCKG_DAMS_JBPM_MNGT.proc_wfcfg_qry
  -- 功能     　查询流程信息
  -- 应用模块   JBPM流程管理-JBPM
  -- REVISIONS:
  --    Ver           Date            Author                 Description
  -- ---------      ----------      ----------        --------------------------
  --   1.0           20130216        kfzx-haozj           create this procedure.*/
  -- 说明：
  PROCEDURE proc_wfcfg_qry(i_pdid     IN VARCHAR2, --流程id
                           i_cfgid    IN VARCHAR2, --配置项id
                           o_flag     OUT VARCHAR2,
                           o_totalnum OUT VARCHAR2, --最终获取数
                           o_cur      OUT SYS_REFCURSOR) IS

  BEGIN
    o_flag := '0';

    SELECT COUNT(*)
      INTO o_totalnum
      FROM dams_wf_activity_conf c
     WHERE c.pdid = i_pdid
       AND c.cfgid = nvl(i_cfgid, c.cfgid);

    OPEN o_cur FOR
      SELECT id activity_id,
             activity_name,
             cfgid,
             --pdid,
             (SELECT dic.dictname
                FROM dams_wf_dict dic
               WHERE dic.dicttype = 'CACU_TYPE'
                 and dic.lang = 'zh_CN'
                 AND dic.dictval = cacu_type) cacu_type,
             (SELECT dic.dictname
                FROM dams_wf_dict dic
               WHERE dic.dicttype = 'SELECT_RANGE'
                 and dic.lang = 'zh_CN'
                 AND dic.dictval = select_range) select_range,
             ext_interface,
             (SELECT dic.dictname
                FROM dams_wf_dict dic
               WHERE dic.dicttype = 'MAIL_TYPE'
                 and dic.lang = 'zh_CN'
                 AND dic.dictval = mail_type) mail_type
        FROM dams_wf_activity_conf
       WHERE pdid = i_pdid
         AND cfgid = nvl(i_cfgid, cfgid);

  EXCEPTION
    WHEN OTHERS THEN
      o_flag := '-1';
      RETURN;
  END proc_wfcfg_qry;

  /******************************************************************************
  -- 名称       PCKG_DAMS_JBPM_MNGT.proc_gen_default_data
  -- 功能     　生成流程配置数据
  -- 应用模块   JBPM流程管理-JBPM
  -- REVISIONS:
  --    Ver           Date            Author                 Description
  -- ---------      ----------      ----------        --------------------------
  --   1.0           20130216        kfzx-haozj           create this procedure.*/

  -- 说明：
  PROCEDURE proc_wfcfg_oper(i_oper          IN VARCHAR2, --操作类型
                            i_pdid          IN VARCHAR2, --流程定义ID
                            i_activity_id   IN VARCHAR2, --活动ID
                            i_activity_name IN VARCHAR2, --活动名称
                            i_cfgid         IN VARCHAR2, --配置ID
                            i_cacutype      IN VARCHAR2,
                            i_selectrange   IN VARCHAR2,
                            i_mailtype      IN VARCHAR2,
                            o_flag          OUT VARCHAR2,
                            o_msg           OUT VARCHAR2) IS

  BEGIN
    o_flag := '0';
    --o_msg  := '操作成功';
    IF i_oper = 'edit' THEN
      o_msg := '修改成功';
      UPDATE dams_wf_activity_conf t
         SET t.cacu_type    = i_cacutype,
             t.select_range = i_selectrange,
             t.mail_type    = i_mailtype
       WHERE t.id = i_activity_id
         AND t.cfgid = i_cfgid;

    ELSIF i_oper = 'add' THEN
      o_msg := '新增成功';
      INSERT INTO dams_wf_activity_conf
        (pdid,
         id,
         activity_name,
         cfgid,
         cacu_type,
         select_range,
         mail_type)
      VALUES
        (i_pdid,
         i_activity_id,
         i_activity_name,
         i_cfgid,
         i_cacutype,
         i_selectrange,
         i_mailtype);

    ELSIF i_oper = 'del' THEN
      o_msg := '删除成功';
      DELETE FROM dams_wf_activity_conf t WHERE t.id = i_activity_id;

    ELSIF i_oper = 'init' THEN
      o_msg := '初始化成功';
      DELETE FROM dams_wf_activity_conf t WHERE t.id = i_activity_id;
      INSERT INTO dams_wf_activity_conf
        (pdid,
         id,
         activity_name,
         cfgid,
         cacu_type,
         select_range,
         mail_type)
      VALUES
        (i_pdid, i_activity_id, i_activity_name, '1', '1', 'N', '1');
    END IF;

    COMMIT;

  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      IF i_oper = 'edit' THEN
        o_msg := '修改失败';
      ELSIF i_oper = 'add' THEN
        o_msg := '新增失败';
      ELSIF i_oper = 'del' THEN
        o_msg := '删除失败';
      ELSIF i_oper = 'init' THEN
        o_msg := '初始化失败';
      END IF;
      o_flag := '-1';
      RETURN;
  END proc_wfcfg_oper;

  /******************************************************************************
  -- 名称       PCKG_DAMS_JBPM_MNGT.proc_wfuser_qry
  -- 功能     　查询用户信息
  -- 应用模块   JBPM流程管理-JBPM
  -- REVISIONS:
  --    Ver           Date            Author                 Description
  -- ---------      ----------      ----------        --------------------------
  --   1.0           20130216        kfzx-haozj           create this procedure.*/
  -- 说明：
  PROCEDURE proc_wfuser_qry(i_pdid     IN VARCHAR2, --流程id
                            i_cfgid    IN VARCHAR2, --配置项id
                            o_flag     OUT VARCHAR2,
                            o_totalnum OUT VARCHAR2, --最终获取数
                            o_cur      OUT SYS_REFCURSOR) IS

  BEGIN
    o_flag := '0';

    SELECT COUNT(*)
      INTO o_totalnum
      FROM dams_wf_activity_user t, dams_wf_activity_conf c
     WHERE c.id = t.activity_id
       AND c.cfgid = t.cfgid
       AND c.pdid = i_pdid
       AND c.cfgid = nvl(i_cfgid, c.cfgid);

    OPEN o_cur FOR
      SELECT t.id,
             t.activity_id,
             c.activity_name,
             t.cfgid,
             (SELECT dic.dictname
                FROM dams_wf_dict dic
               WHERE dic.dicttype = 'USER_TYPE'
                 and dic.lang = 'zh_CN'
                 AND dic.dictval = t.user_type) user_type,
             t.transition_name,
             t.entity_id,
             t.entity_name
        FROM dams_wf_activity_user t, dams_wf_activity_conf c
       WHERE c.id = t.activity_id
         AND c.cfgid = t.cfgid
         AND c.pdid = i_pdid
         AND c.cfgid = nvl(i_cfgid, c.cfgid)
       ORDER BY t.activity_id;

  EXCEPTION
    WHEN OTHERS THEN
      o_flag := '-1';
      RETURN;
  END proc_wfuser_qry;

  /******************************************************************************
  -- 名称       PCKG_DAMS_JBPM_MNGT.proc_wfuser_oper
  -- 功能     　用户配置信息
  -- 应用模块   JBPM流程管理-JBPM
  -- REVISIONS:
  --    Ver           Date            Author                 Description
  -- ---------      ----------      ----------        --------------------------
  --   1.0           20130216        kfzx-haozj           create this procedure.*/
  -- 说明：
  PROCEDURE proc_wfuser_oper(i_oper        IN VARCHAR2, --操作类型
                             i_id          IN VARCHAR2, --主键id
                             i_activityid  IN VARCHAR2, --活动id
                             i_pdid        IN VARCHAR2, --流程id
                             i_usertype    IN VARCHAR2, --选择类型
                             i_entityid    IN VARCHAR2, --选择类型对应参数
                             i_desc        IN VARCHAR2, --说明
                             i_preoutgoing IN VARCHAR2, --前一路径
                             i_cfgid       IN VARCHAR2, --配置项id
                             o_flag        OUT VARCHAR2, --返回值
                             o_msg         OUT VARCHAR2) IS
    --返回消息
    v_oper_name VARCHAR2(50) := '';
  BEGIN
    o_flag := '0';
    --o_msg  := '操作成功';
    IF i_oper = 'edit' THEN
      o_msg := '修改成功';
      UPDATE dams_wf_activity_user t
         SET t.entity_id       = i_entityid,
             t.entity_name     = i_desc,
             t.user_type       = i_usertype,
             t.transition_name = i_preoutgoing
       WHERE t.id = i_id
         AND t.cfgid = i_cfgid;

    ELSIF i_oper = 'add' THEN
      o_msg := '新增成功';
      INSERT INTO dams_wf_activity_user
        (id,
         cfgid,
         activity_id,
         user_type,
         entity_id,
         entity_name,
         transition_name)
      VALUES
        (func_maxid_inside_act(i_activityid,
                               'dams_wf_activity_user',
                               'id',
                               i_cfgid),
         i_cfgid,
         i_activityid,
         i_usertype,
         i_entityid,
         i_desc,
         i_preoutgoing);

    ELSIF i_oper = 'del' THEN
      o_msg := '删除成功';
      DELETE FROM dams_wf_activity_user t
       WHERE t.id = i_id
         AND t.cfgid = i_cfgid;

    ELSIF i_oper = 'init' THEN
      o_msg := '初始化成功';
      DELETE FROM dams_wf_activity_user t
       WHERE t.activity_id = i_activityid;
      INSERT INTO dams_wf_activity_user
        (id,
         cfgid,
         activity_id,
         user_type,
         entity_id,
         entity_name,
         transition_name)
      VALUES
        (i_activityid || '.1',
         '1',
         i_activityid,
         'R',
         '',
         '',
         'default_transition');

    END IF;

    COMMIT;

  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      IF i_oper = 'edit' THEN
        o_msg := '修改失败';
      ELSIF i_oper = 'add' THEN
        o_msg := '新增失败';
      ELSIF i_oper = 'del' THEN
        o_msg := '删除失败';
      ELSIF i_oper = 'init' THEN
        o_msg := '初始化失败';
      END IF;
      o_flag := '-1';
      RETURN;
  END proc_wfuser_oper;

  /******************************************************************************
  -- 名称       PCKG_DAMS_JBPM_MNGT.proc_wfdisplay_qry
  -- 功能     　查询展现信息
  -- 应用模块   JBPM流程管理-JBPM
  -- REVISIONS:
  --    Ver           Date            Author                 Description
  -- ---------      ----------      ----------        --------------------------
  --   1.0           20130216        kfzx-haozj           create this procedure.*/
  -- 说明：
  PROCEDURE proc_wfdisplay_qry(i_pdid     IN VARCHAR2, --流程id
                               i_cfgid    IN VARCHAR2, --配置项id
                               o_flag     OUT VARCHAR2,
                               o_totalnum OUT VARCHAR2, --最终获取数
                               o_cur      OUT SYS_REFCURSOR) IS

  BEGIN
    o_flag := '0';

    SELECT COUNT(*)
      INTO o_totalnum
      FROM dams_wf_activity_display t, dams_wf_activity_conf c
     WHERE c.id = t.id
       AND c.pdid = i_pdid;

    OPEN o_cur FOR
      SELECT t.id activity_id,
             c.activity_name,
             (SELECT dic.dictname
                FROM dams_wf_dict dic
               WHERE dic.dicttype = 'SHOW_TYPE'
                 and dic.lang = 'zh_CN'
                 AND dic.dictval = t.show_type) show_type,
             (SELECT dic.dictname
                FROM dams_wf_dict dic
               WHERE dic.dicttype = 'IS_SHOW'
               and dic.lang = 'zh_CN'
                 AND dic.dictval = t.show_action) show_action,
             (SELECT dic.dictname
                FROM dams_wf_dict dic
               WHERE dic.dicttype = 'IS_SHOW'
               and dic.lang = 'zh_CN'
                 AND dic.dictval = t.show_task) show_task,
             (SELECT dic.dictname
                FROM dams_wf_dict dic
               WHERE dic.dicttype = 'IS_SHOW'
               and dic.lang = 'zh_CN'
                 AND dic.dictval = t.show_dealer) show_dealer,
             (SELECT dic.dictname
                FROM dams_wf_dict dic
               WHERE dic.dicttype = 'IS_SHOW'
               and dic.lang = 'zh_CN'
                 AND dic.dictval = t.show_opinion) show_opinion,
             t.disp_seq
        FROM dams_wf_activity_display t, dams_wf_activity_conf c
       WHERE c.id = t.id
         AND c.pdid = i_pdid
       ORDER BY t.id;

  EXCEPTION
    WHEN OTHERS THEN
      o_flag := '-1';
      RETURN;
  END proc_wfdisplay_qry;

  /******************************************************************************
  -- 名称       PCKG_DAMS_JBPM_MNGT.proc_wfdisplay_oper
  -- 功能     　展现配置信息
  -- 应用模块   JBPM流程管理-JBPM
  -- REVISIONS:
  --    Ver           Date            Author                 Description
  -- ---------      ----------      ----------        --------------------------
  --   1.0           20130216        kfzx-haozj           create this procedure.*/
  -- 说明：
  PROCEDURE proc_wfdisplay_oper(i_oper       IN VARCHAR2, --操作类型
                                i_pdid       IN VARCHAR2, --流程id
                                i_activityid IN VARCHAR2, --活动id
                                i_showtype   IN VARCHAR2, --展现方式 1. 展现到具体人 2. 展现到机构 3. 展现到角色
                                i_showaction IN VARCHAR2, --显示动作
                                i_showtask   IN VARCHAR2, --显示任务
                                i_showdealer IN VARCHAR2, --显示处理人
                                i_showoption IN VARCHAR2, --显示意见
                                i_disp_seq   IN VARCHAR2, --配置组序号
                                o_flag       OUT VARCHAR2, --返回值
                                o_msg        OUT VARCHAR2) IS
    --返回消息
    v_oper_name VARCHAR2(50) := '';
  BEGIN
    o_flag := '0';
    --o_msg  := '操作成功';
    IF i_oper = 'edit' THEN
      o_msg := '修改成功';
      UPDATE dams_wf_activity_display t
         SET t.show_type    = i_showtype,
             t.show_action  = i_showaction,
             t.show_task    = i_showtask,
             t.show_dealer  = i_showdealer,
             t.show_opinion = i_showoption,
             t.disp_seq     = i_disp_seq
       WHERE t.id = i_activityid;

    ELSIF i_oper = 'add' THEN
      o_msg := '新增成功';
      INSERT INTO dams_wf_activity_display
        (id,
         pdid,
         show_type,
         show_action,
         show_task,
         show_dealer,
         show_opinion,
         disp_seq)
      VALUES
        (i_activityid,
         i_pdid,
         i_showtype,
         i_showaction,
         i_showtask,
         i_showdealer,
         i_showoption,
         i_disp_seq);

    ELSIF i_oper = 'del' THEN
      o_msg := '删除成功';
      DELETE FROM dams_wf_activity_display t WHERE t.id = i_activityid;

    ELSIF i_oper = 'init' THEN
      o_msg := '初始化成功';
      DELETE FROM dams_wf_activity_display t WHERE t.id = i_activityid;
      INSERT INTO dams_wf_activity_display
        (id,
         pdid,
         show_type,
         show_action,
         show_task,
         show_dealer,
         show_opinion,
         disp_seq)
      VALUES
        (i_activityid, i_pdid, '1', '0', '1', '1', '1', '1');
    END IF;
    COMMIT;

  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      IF i_oper = 'edit' THEN
        o_msg := '修改失败';
      ELSIF i_oper = 'add' THEN
        o_msg := '新增失败';
      ELSIF i_oper = 'del' THEN
        o_msg := '删除失败';
      ELSIF i_oper = 'init' THEN
        o_msg := '初始化失败';
      END IF;
      o_flag := '-1';
      RETURN;
  END proc_wfdisplay_oper;

  /******************************************************************************
  -- 名称       PCKG_DAMS_JBPM_MNGT.proc_wfactform_qry
  -- 功能     　查询展现信息
  -- 应用模块   JBPM流程管理-JBPM
  -- REVISIONS:
  --    Ver           Date            Author                 Description
  -- ---------      ----------      ----------        --------------------------
  --   1.0           20130216        kfzx-haozj           create this procedure.*/
  -- 说明：
  PROCEDURE proc_wfactform_qry(i_pdid     IN VARCHAR2, --流程id
                               i_cfgid    IN VARCHAR2, --配置项id
                               o_flag     OUT VARCHAR2,
                               o_totalnum OUT VARCHAR2, --最终获取数
                               o_cur      OUT SYS_REFCURSOR) IS
  BEGIN
    o_flag := '0';

    SELECT COUNT(*)
      INTO o_totalnum
      FROM dams_wf_activity_form t, dams_wf_activity_conf c
     WHERE c.id = t.activity_id
       AND c.pdid = i_pdid;

    OPEN o_cur FOR
      SELECT t.id activity_id, c.activity_name, t.form_url, t.read_url
        FROM dams_wf_activity_form t, dams_wf_activity_conf c
       WHERE c.id = t.activity_id
         AND c.pdid = i_pdid
       ORDER BY t.id;

  EXCEPTION
    WHEN OTHERS THEN
      o_flag := '-1';
      RETURN;
  END proc_wfactform_qry;

  /******************************************************************************
  -- 名称       PCKG_DAMS_JBPM_MNGT.proc_wfdisplay_oper
  -- 功能     　展现配置信息
  -- 应用模块   JBPM流程管理-JBPM
  -- REVISIONS:
  --    Ver           Date            Author                 Description
  -- ---------      ----------      ----------        --------------------------
  --   1.0           20130216        kfzx-haozj           create this procedure.*/
  -- 说明：
  PROCEDURE proc_wfactform_oper(i_oper       IN VARCHAR2, --操作类型
                                i_pdid       IN VARCHAR2, --流程id
                                i_activityid IN VARCHAR2, --活动id
                                i_form_url   IN VARCHAR2,
                                i_read_url   IN VARCHAR2,
                                o_flag       OUT VARCHAR2, --返回值
                                o_msg        OUT VARCHAR2) IS
    --返回消息
    v_oper_name VARCHAR2(50) := '';
    v_form_id   VARCHAR2(255);
  BEGIN
    o_flag := '0';
    --o_msg  := '操作成功';
    IF i_oper = 'edit' THEN
      o_msg := '修改成功';
      UPDATE dams_wf_activity_form t
         SET t.form_url = i_form_url, t.read_url = i_read_url
       WHERE t.activity_id = i_activityid;

    ELSIF i_oper = 'add' THEN
      o_msg := '新增成功';

      BEGIN
        SELECT t.form_id
          INTO v_form_id
          FROM dams_wf_process_form t
         WHERE t.pdid = i_pdid;
      EXCEPTION
        WHEN OTHERS THEN
          v_form_id := 'autogenForm';
      END;

      INSERT INTO dams_wf_activity_form
        (id, activity_id, form_id, form_url, read_url)
      VALUES
        (i_activityid, i_activityid, v_form_id, i_form_url, i_read_url);

    ELSIF i_oper = 'del' THEN
      o_msg := '删除成功';
      DELETE FROM dams_wf_activity_form t
       WHERE t.activity_id = i_activityid;

    ELSIF i_oper = 'init' THEN
      o_msg := '初始化成功';
      DELETE FROM dams_wf_activity_form t
       WHERE t.activity_id = i_activityid;
      INSERT INTO dams_wf_activity_form
        (id, activity_id, form_id, form_url, read_url)
      VALUES
        (i_activityid, i_activityid, 'autogenForm', '', '');

    END IF;

    COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      IF i_oper = 'edit' THEN
        o_msg := '修改失败';
      ELSIF i_oper = 'add' THEN
        o_msg := '新增失败';
      ELSIF i_oper = 'del' THEN
        o_msg := '删除失败';
      ELSIF i_oper = 'init' THEN
        o_msg := '初始化失败';
      END IF;
      o_flag := '-1';
      RETURN;
  END proc_wfactform_oper;

  /******************************************************************************
  -- 名称       PCKG_DAMS_JBPM_MNGT.proc_wfactalias_qry
  -- 功能     　查询展现信息
  -- 应用模块   JBPM流程管理-JBPM
  -- REVISIONS:
  --    Ver           Date            Author                 Description
  -- ---------      ----------      ----------        --------------------------
  --   1.0           20130216        kfzx-haozj           create this procedure.*/
  -- 说明：
  PROCEDURE proc_wfactalias_qry(i_pdid     IN VARCHAR2, --流程id
                                i_cfgid    IN VARCHAR2, --配置项id
                                o_flag     OUT VARCHAR2,
                                o_totalnum OUT VARCHAR2, --最终获取数
                                o_cur      OUT SYS_REFCURSOR) IS
  BEGIN
    o_flag := '0';

    SELECT COUNT(*)
      INTO o_totalnum
      FROM dams_wf_activity_alias t, dams_wf_activity_conf c
     WHERE t.activity_id = c.id
       AND c.pdid = i_pdid;

    OPEN o_cur FOR
      SELECT t.activity_id,
             c.activity_name,
             t.cfgid,
             t.incoming_transition pre_outgoing,
             t.activity_name_alias
        FROM dams_wf_activity_alias t, dams_wf_activity_conf c
       WHERE t.activity_id = c.id
         AND c.pdid = i_pdid
       ORDER BY t.activity_id;

  EXCEPTION
    WHEN OTHERS THEN
      o_flag := '-1';
      RETURN;
  END proc_wfactalias_qry;

  /******************************************************************************
  -- 名称       PCKG_DAMS_JBPM_MNGT.proc_wfactalias_oper
  -- 功能     　展现配置信息
  -- 应用模块   JBPM流程管理-JBPM
  -- REVISIONS:
  --    Ver           Date            Author                 Description
  -- ---------      ----------      ----------        --------------------------
  --   1.0           20130216        kfzx-haozj           create this procedure.*/
  -- 说明：
  PROCEDURE proc_wfactalias_oper(i_oper                IN VARCHAR2, --操作类型
                                 i_pdid                IN VARCHAR2, --流程id
                                 i_activityid          IN VARCHAR2, --活动id
                                 i_cfgid               IN VARCHAR2,
                                 i_incoming_transition IN VARCHAR2,
                                 i_alias               IN VARCHAR2,
                                 o_flag                OUT VARCHAR2, --返回值
                                 o_msg                 OUT VARCHAR2) IS
    --返回消息
    v_oper_name VARCHAR2(50) := '';
    v_form_id   VARCHAR2(255);
  BEGIN
    o_flag := '0';
    --o_msg  := '操作成功';
    IF i_oper = 'edit' THEN
      o_msg := '修改成功';
      UPDATE dams_wf_activity_alias t
         SET t.incoming_transition = i_incoming_transition,
             t.activity_name_alias = i_alias
       WHERE t.activity_id = i_activityid
         AND t.cfgid = i_cfgid;

    ELSIF i_oper = 'add' THEN
      o_msg := '新增成功';
      INSERT INTO dams_wf_activity_alias
        (cfgid, activity_id, incoming_transition, activity_name_alias)
      VALUES
        (i_cfgid, i_activityid, i_incoming_transition, i_alias);

    ELSIF i_oper = 'del' THEN
      o_msg := '删除成功';
      DELETE FROM dams_wf_activity_alias t
       WHERE t.activity_id = i_activityid;

    ELSIF i_oper = 'init' THEN
      o_msg := '初始化成功';
      DELETE FROM dams_wf_activity_alias t
       WHERE t.activity_id = i_activityid;
      INSERT INTO dams_wf_activity_alias
        (cfgid, activity_id, incoming_transition, activity_name_alias)
      VALUES
        ('1', i_activityid, 'default_transition', i_alias);
    END IF;
    COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      IF i_oper = 'edit' THEN
        o_msg := '修改失败';
      ELSIF i_oper = 'add' THEN
        o_msg := '新增失败';
      ELSIF i_oper = 'del' THEN
        o_msg := '删除失败';
      ELSIF i_oper = 'init' THEN
        o_msg := '初始化失败';
      END IF;
      o_flag := '-1';
      RETURN;
  END proc_wfactalias_oper;

  /******************************************************************************
  -- 名称       PCKG_DAMS_JBPM_MNGT.proc_wf_showdict
  -- 功能     　流程类字典展现
  -- 应用模块   JBPM流程管理-JBPM
  -- REVISIONS:
  --    Ver           Date            Author                 Description
  -- ---------      ----------      ----------        --------------------------
  --   1.0           20130226        kfzx-haozj           create this procedure.*/
  -- 说明：
  PROCEDURE proc_wf_showdict(i_dicttype IN VARCHAR2, --字典类型
                             i_lang     IN VARCHAR2, --语言
                             o_flag     OUT VARCHAR2, --返回值
                             o_cur      OUT SYS_REFCURSOR) IS
  BEGIN
    o_flag := '0';
    --o_msg  := '操作成功';
    OPEN o_cur FOR
      SELECT nvl(d.dictname,d1.dictname), nvl(d.dictval,d1.dictval)
        FROM dams_wf_dict d,dams_wf_dict d1
       WHERE d.dicttype(+) = i_dicttype
         AND d.lang(+) = i_lang
         and d1.dicttype = d.dicttype
         and d1.lang = 'zh_CN';
  EXCEPTION
    WHEN OTHERS THEN
      o_flag := '-1';
      RETURN;
  END proc_wf_showdict;

    /******************************************************************************
  -- 名称       PCKG_DAMS_JBPM_MNGT.proc_routable_procinst_qry
  -- 功能     　展现可重新路由的流程实例
  -- 应用模块   JBPM流程管理-JBPM
  -- REVISIONS:
  --    Ver           Date            Author                 Description
  -- ---------      ----------      ----------        --------------------------
  --   1.0           20130222        kfzx-maming1           create this procedure.*/
  -- 说明：
PROCEDURE proc_routable_procinst_qry(i_stru_id   VARCHAR2, --机构号 可能为多个逗号分隔
                                       i_sys_code  IN VARCHAR2, --子系统编号
                                       i_biz_type  IN VARCHAR2,
                                       i_beg_num   NUMBER,
                                       i_fetch_num NUMBER,
                                       o_flag      OUT VARCHAR2,
                                       o_total_num OUT NUMBER,
                                       o_cur       OUT SYS_REFCURSOR) IS
    v_param      db_log.info%TYPE := i_stru_id || '|' || i_sys_code || '|' ||
                                     i_beg_num || '|' || i_fetch_num;
    v_proc_name  db_log.proc_name%TYPE := 'PCKG_DAMS_JBPM_MNGT.proc_routable_procinst_qry';
    v_struid_arr pckg_dams_role.arrytype;
    v_struid_str varchar2(200);
  BEGIN
    o_flag := '0';

    pack_log.info(v_proc_name,
                  pack_log.start_step,
                  pack_log.start_msg || v_param);

    IF (i_beg_num < 0 OR i_fetch_num < 0) THEN
      BEGIN
        o_flag := '-2';
        RETURN;
      END;
    END IF;

    --v_struid_arr := pckg_dams_role.func_common_getarray(i_stru_id, ',');

    for i in 1 .. v_struid_arr.count loop
      if i = 1 then
        v_struid_str := '''' || v_struid_arr(i) || '''';
      else
        v_struid_str := v_struid_str || ',''' || v_struid_arr(i) || '''';
      end if;
    end loop;

    OPEN o_cur FOR 'SELECT t.procinst_id,
       t.business_id,
       t.form_id,
       t.title,
       t.subcount,
       t.current_task,
       t.start_user_name,
       t.start_stru_name
  FROM (SELECT rownum rn, innert.*
          FROM (SELECT p.procinst_id,
                       p.business_id,
                       (select max(form_id)
                          from dams_wf_task
                         where procinst_id = p.procinst_id) form_id,
                       p.title,
                       CASE
                         WHEN nvl(se.secount, 0) > 0 THEN
                          jp.activityname_ || ''[会签中]''
                         ELSE
                          jp.activityname_
                       END current_task,
                       nvl(se.secount, 0) subcount,
                       (SELECT u.name
                          FROM dams_user u
                         WHERE u.ssic_id = p.creator_id) start_user_name,
                       (SELECT bb.stru_sname
                          FROM dams_branch bb
                         WHERE bb.stru_id = p.creator_stru) start_stru_name
                  FROM dams_wf_process p,
                       jbpm4_execution jp,
                       dams_branch_relation b,
                       (SELECT COUNT(1) secount, sube.parent_ parentid
                          FROM jbpm4_execution sube
                         GROUP BY sube.parent_) se
                 WHERE p.procinst_id = jp.dbid_
                   AND b.up_bnch_id in (' || v_struid_str || ')
                   AND jp.parent_ IS NULL
                   AND jp.dbid_ = se.parentid(+)
                   AND p.creator_stru = b.bnch_id
                   and not exists (
                   select 1 from dams_wf_task r
                   where r.task_state=''drop'' and r.procinst_id=p.procinst_id
                   )
                   AND p.sys_code = nvl(:1, p.sys_code)
                   AND p.business_type = nvl(:2, p.business_type)
                 ORDER BY p.start_time DESC) innert) t
 WHERE t.rn >= :3
   AND t.rn < :4 + :5'
      using i_sys_code, i_biz_type, i_beg_num, i_beg_num, i_fetch_num;

    execute immediate 'SELECT COUNT(1)
  FROM dams_wf_process p, jbpm4_execution jp, dams_branch_relation b
 WHERE p.procinst_id = jp.dbid_
   AND jp.parent_ IS NULL
   AND p.creator_stru = b.bnch_id
    and not exists (
    select 1 from dams_wf_task r
   where r.task_state=''drop'' and r.procinst_id=p.procinst_id
    )
   AND p.sys_code = nvl(:1, p.sys_code)
   AND p.business_type = nvl(:2, p.business_type)
   AND b.up_bnch_id in (' || v_struid_str || ')'
      INTO o_total_num
      using i_sys_code, i_biz_type;

    pack_log.info(v_proc_name,
                  pack_log.end_step,
                  pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      IF o_cur%ISOPEN THEN
        CLOSE o_cur;
      END IF;
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_flag := '-1';
      RETURN;
  END proc_routable_procinst_qry;

 
  PROCEDURE proc_routable_activity_qry(i_procinst_id IN NUMBER,
                                      
                                       o_flag OUT VARCHAR2,
                                       o_cur  OUT SYS_REFCURSOR) IS
    v_param     db_log.info%TYPE := i_procinst_id;
    v_proc_name db_log.proc_name%TYPE := 'pckg_dams_jbpm.proc_routable_activity_qry';
    v_step      db_log.step_no%TYPE;
    v_subcount  number;
    --v_language  dams_wf_language_conf.zh_cn%TYPE;
  BEGIN
    o_flag := 0;
    pack_log.info(v_proc_name,
                  pack_log.start_step,
                  pack_log.start_msg || v_param);

       select count(1)
       into v_subcount
       from jbpm4_execution
       where instance_=i_procinst_id
       and state_='active-concurrent';
    IF (v_subcount = 0) THEN
      BEGIN
        OPEN o_cur FOR 'select hist.task_id
        FROM jbpm4_hist_actinst hisact,
        dams_wf_task hist
       WHERE hisact.type_ IN (''task'', ''multitask'')
         AND hisact.hproci_ = :1
         AND instr(hisact.execution_, ''.'', 1) =
             instr(hisact.execution_, ''.'', -1)
         AND hisact.end_ IS NOT NULL
         and hisact.htask_ = to_number(hist.task_id)
         and (hist.action_flag not in (''8'',''9'')
                               or hist.action_flag is null)'
          USING i_procinst_id;
      EXCEPTION
        WHEN OTHERS THEN
          o_flag := -1;
          pack_log.error(v_proc_name, pack_log.end_step, '异常');
          RETURN;
      END;
    END IF;
  END proc_routable_activity_qry;
 
  PROCEDURE proc_routable_act_user_qry(i_procinst_id IN NUMBER,
                                       i_task_id     in number, --路由至的节点名
                                       o_flag        OUT VARCHAR2,
                                       o_cur         OUT SYS_REFCURSOR) IS
    v_param     db_log.info%TYPE := i_procinst_id || '|' || i_task_id;
    v_proc_name db_log.proc_name%TYPE := 'pckg_dams_jbpm.proc_routable_act_user_qry';

  BEGIN
    o_flag := '0';
    pack_log.info(v_proc_name,
                  pack_log.start_step,
                  pack_log.start_msg || v_param);
				  /*
    OPEN o_cur FOR
      SELECT dealed_tasks.form_id jbpm_form_id,
             proc.process_id jbpm_process_id,
             proc.business_id jbpm_business_id,
             proc.title jbpm_business_title,
             current_tasks.task_id jbpm_task_id,
             '流程重新路由' jbpm_transition_name,
             '重新路由至[' ||
             (select task_name from dams_wf_task where task_id = i_task_id) || ']' jbpm_task_opinion,
             (select task_name from dams_wf_task where task_id = i_task_id) jbpm_destination_task_name,
             proc.business_type jbpm_business_type,
             proc.cfg_id jbpm_cfg_id,
             actconf.id jbpm_activity_id,
             dealed_tasks.ssic_id jbpm_nextuser_id,
             dealed_tasks.role_id jbpm_nextrole_id,
             dealed_tasks.stru_id jbpm_nextstru_id,
             proc.sys_code sys_code
        from dams_wf_process proc,
             (select max(t1.form_id) form_id,
                     dams_conn(t1.ssic_id) ssic_id,
                     dams_conn(t1.role_id) role_id,
                     dams_conn(t1.stru_id) stru_id
                from dams_wf_task t1
               where t1.task_state = 'completed'
                 and exists
               (select 1
                        from dams_wf_task t2
                       where t2.procinst_id = i_procinst_id
                         and t2.task_id = i_task_id
                         and t1.procinst_id = t2.procinst_id
                         and t1.task_name = t2.task_name
                         and t1.prev_task_id = t2.prev_task_id)) dealed_tasks, --要路由至的已完成的任务
             (select dbid_ task_id
                from jbpm4_task
               where procinst_ = i_procinst_id) current_tasks,
             dams_wf_activity_conf actconf
       where proc.procinst_id = i_procinst_id
         and proc.cfg_id = actconf.cfgid
         and actconf.pdid = proc.process_id
         and actconf.activity_name =
             (select task_name from dams_wf_task where task_id = i_task_id);
			 */
  EXCEPTION
    WHEN OTHERS THEN
      o_flag := '-1';
      pack_log.error(v_proc_name, pack_log.end_step, '异常');
      RETURN;

  END proc_routable_act_user_qry;

  PROCEDURE proc_routable_role_qry(i_ssic_id IN VARCHAR2,
                                   o_flag    OUT VARCHAR2,
                                   o_cur     OUT SYS_REFCURSOR) IS
    v_param     db_log.info%TYPE := i_ssic_id;
    v_proc_name db_log.proc_name%TYPE := 'pckg_dams_jbpm_mngt.proc_routable_role_qry';
  BEGIN
    o_flag := '0';
    pack_log.info(v_proc_name,
                  pack_log.start_step,
                  pack_log.start_msg || v_param);
	/*			  
    OPEN o_cur FOR
      select r.ssic_id, dams_conn(r.role_id), dams_conn(r.stru_id), r.sys_code
        from dams_user_role_rel r, DAMS_ROLE r2
       where r2.role_id = r.role_id
         and r2.role_type = 'ADMIN'
         and r.ssic_id = i_ssic_id
         and not exists (select 1 from dams_sys_url_cfg c where r.sys_code = c.sys_code)
       group by r.ssic_id, r.sys_code;
	   */
  EXCEPTION
    WHEN OTHERS THEN
      o_flag := '-1';
      pack_log.error(v_proc_name, pack_log.end_step, '异常');
      RETURN;
  END proc_routable_role_qry;

END pckg_dams_jbpm_mngt;
//


CREATE OR REPLACE PACKAGE pckg_dams_charge_mgt IS

  
  PROCEDURE proc_get_model_list(i_ssic_id  IN VARCHAR2, --统一认证号
                                o_ret_code OUT VARCHAR2, --返回值
                                o_list_cur OUT SYS_REFCURSOR --查询列表游标
                                );

 
  PROCEDURE proc_get_sys_list(i_ssic_id  IN VARCHAR2, --统一认证号
                              i_model_id IN VARCHAR2,
                              o_ret_code OUT VARCHAR2, --返回值
                              o_list_cur OUT SYS_REFCURSOR --查询列表游标
                              );

  
  PROCEDURE proc_get_stru_list(i_ssic_id     IN VARCHAR2, --统一认证号
                               i_model_id    IN VARCHAR2,
                               i_sys_code    IN VARCHAR2,
                               i_sup_stru_id IN VARCHAR2,
                               i_stru_name   IN VARCHAR2,
                               i_show_num    IN VARCHAR2,
                               o_ret_code    OUT VARCHAR2, --返回值
                               o_list_cur    OUT SYS_REFCURSOR --查询列表游标
                               );

  PROCEDURE proc_get_charge_list(i_ssic_id   IN VARCHAR2, --统一认证号
                                 i_model_id  IN VARCHAR2,
                                 i_sys_code  IN VARCHAR2,
                                 i_stru_id   IN VARCHAR2,
                                 i_view_type IN VARCHAR2,
                                 o_ret_code  OUT VARCHAR2, --返回值
                                 o_list_cur  OUT SYS_REFCURSOR --查询列表游标
                                 );

  
  PROCEDURE proc_get_edit_list(i_ssic_id   IN VARCHAR2, --统一认证号
                               i_model_id  IN VARCHAR2,
                               i_sys_code  IN VARCHAR2,
                               i_stru_id   IN VARCHAR2,
                               i_view_type IN VARCHAR2,
                               o_ret_code  OUT VARCHAR2, --返回值
                               o_list_cur  OUT SYS_REFCURSOR --查询列表游标
                               );

 
  PROCEDURE proc_edit_charge_rel(i_ssic_id   IN VARCHAR2, --统一认证号
                                 i_model_id  IN VARCHAR2,
                                 i_sys_code  IN VARCHAR2,
                                 i_base_id   IN VARCHAR2, --基础配置id
                                 i_edit_data IN VARCHAR2, --配置数据
                                 i_view_type IN VARCHAR2, --列表页视图类型1机构视图2领导视图
                                 o_ret_code  OUT VARCHAR2, --返回值
                                 o_msg       OUT VARCHAR2);

END pckg_dams_charge_mgt;
//

CREATE OR REPLACE PACKAGE BODY pckg_dams_charge_mgt IS


  PROCEDURE proc_get_model_list(i_ssic_id  IN VARCHAR2, --统一认证号
                                o_ret_code OUT VARCHAR2, --返回值
                                o_list_cur OUT SYS_REFCURSOR --查询列表游标
                                ) IS
    v_param     db_log.info%TYPE := i_ssic_id;
    v_proc_name db_log.proc_name%TYPE := 'pckg_dams_charge_mgt.proc_get_model_list';
    v_stru_id   dams_branch.stru_id%TYPE;
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step,
                  pack_log.start_msg || v_param);
    o_ret_code := '0';

    OPEN o_list_cur FOR
      SELECT 'M0001' AS item_value, '总行部处分管关系' AS item_text FROM dual;

  EXCEPTION
    WHEN OTHERS THEN
      IF o_list_cur%ISOPEN THEN
        CLOSE o_list_cur;
      END IF;
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_ret_code := '-1';
      RETURN;
  END proc_get_model_list;


  PROCEDURE proc_get_sys_list(i_ssic_id  IN VARCHAR2, --统一认证号
                              i_model_id IN VARCHAR2,
                              o_ret_code OUT VARCHAR2, --返回值
                              o_list_cur OUT SYS_REFCURSOR --查询列表游标
                              ) IS
    v_param     db_log.info%TYPE := i_ssic_id || '|' || i_model_id;
    v_proc_name db_log.proc_name%TYPE := 'pckg_dams_charge_mgt.proc_get_sys_list';
    v_stru_id   dams_branch.stru_id%TYPE;
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step,
                  pack_log.start_msg || v_param);
    o_ret_code := '0';

    OPEN o_list_cur FOR
      SELECT t.sys_code,
             (SELECT sc.sys_sname
                FROM dams_sys_conf sc
               WHERE sc.sys_code = t.sys_code) AS sys_name
        FROM dams_charge_model_rel t
       WHERE t.model_id = i_model_id
       GROUP BY t.sys_code;

  EXCEPTION
    WHEN OTHERS THEN
      IF o_list_cur%ISOPEN THEN
        CLOSE o_list_cur;
      END IF;
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_ret_code := '-1';
      RETURN;
  END proc_get_sys_list;


  PROCEDURE proc_get_stru_list(i_ssic_id     IN VARCHAR2, --统一认证号
                               i_model_id    IN VARCHAR2,
                               i_sys_code    IN VARCHAR2,
                               i_sup_stru_id IN VARCHAR2,
                               i_stru_name   IN VARCHAR2,
                               i_show_num    IN VARCHAR2,
                               o_ret_code    OUT VARCHAR2, --返回值
                               o_list_cur    OUT SYS_REFCURSOR --查询列表游标
                               ) IS
    v_param     db_log.info%TYPE := i_ssic_id || '|' || i_model_id || '|' ||
                                    i_sup_stru_id || '|' || i_stru_name || '|' ||
                                    i_show_num;
    v_proc_name db_log.proc_name%TYPE := 'pckg_dams_charge_mgt.proc_get_stru_list';
    v_stru_id   dams_branch.stru_id%TYPE;
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step,
                  pack_log.start_msg || v_param);
    o_ret_code := '0';


    IF i_model_id = 'M0001' THEN

      OPEN o_list_cur FOR
        SELECT b.stru_id AS item_value, b.stru_sname AS item_text
          FROM dams_branch b, dams_branch_relation br
         WHERE b.stru_id = br.bnch_id
           AND br.up_bnch_id = '0010100500' --i_sup_stru_id
           AND b.stru_sign = '3' --内设机构
           AND b.stru_lv IN ('51', '52', '99') --一级部室,二级部室
           AND b.stru_state = '1'
           AND b.stru_sname LIKE '%' || i_stru_name || '%'
           AND rownum <= i_show_num
         ORDER BY b.stru_sort DESC NULLS LAST,
                  b.sort DESC NULLS LAST,
                  b.stru_sname;
    ELSE
      OPEN o_list_cur FOR
        SELECT '' AS item_value, '' AS item_text FROM dual;
    END IF;

  EXCEPTION
    WHEN OTHERS THEN
      IF o_list_cur%ISOPEN THEN
        CLOSE o_list_cur;
      END IF;
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_ret_code := '-1';
      RETURN;
  END proc_get_stru_list;


  PROCEDURE proc_get_charge_list(i_ssic_id   IN VARCHAR2, --统一认证号
                                 i_model_id  IN VARCHAR2,
                                 i_sys_code  IN VARCHAR2,
                                 i_stru_id   IN VARCHAR2,
                                 i_view_type IN VARCHAR2,
                                 o_ret_code  OUT VARCHAR2, --返回值
                                 o_list_cur  OUT SYS_REFCURSOR --查询列表游标
                                 ) IS
    v_param     db_log.info%TYPE := i_ssic_id || '|' || i_model_id || '|' ||
                                    i_sys_code || '|' || i_stru_id || '|' ||
                                    i_view_type;
    v_proc_name db_log.proc_name%TYPE := 'pckg_dams_charge_mgt.proc_get_charge_list';
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step,
                  pack_log.start_msg || v_param);
    o_ret_code := '0';

    IF i_view_type = '1' THEN
      --机构视图,第一列出机构,第二列出分管领导

      OPEN o_list_cur FOR
        SELECT b.stru_id AS grid_row_id,
               b.stru_sname AS column1,
               (SELECT listagg(u.name, ',') within GROUP(ORDER BY u.duty_sort DESC NULLS LAST, nvl(u.if_preside_work, '502002'), u.sort DESC NULLS LAST,
         u.name ASC NULLS LAST)
                  FROM dams_user u, dams_charge_info ci
                 WHERE u.ssic_id = ci.ssic_id
                   AND ci.model_id = i_model_id
                   AND ci.sys_code = i_sys_code
                   AND ci.stru_id = b.stru_id
                   AND ci.status = '1') AS column2,
               (SELECT listagg(u.ssic_id, ',') within GROUP(ORDER BY 1)
                  FROM dams_user u, dams_charge_info ci
                 WHERE u.ssic_id = ci.ssic_id
                   AND ci.model_id = i_model_id
                   AND ci.sys_code = i_sys_code
                   AND ci.stru_id = b.stru_id
                   AND ci.status = '1') AS column2_values,
               '1' rel_state
          FROM dams_branch b, dams_branch_relation br
         WHERE b.stru_id = br.bnch_id
           AND br.up_bnch_id = i_stru_id
           AND b.stru_sign = '3' --内设机构
           AND b.stru_lv = '53' --处室
           AND b.stru_state = '1'
         ORDER BY b.stru_sort DESC NULLS LAST,
                  b.sort DESC NULLS LAST,
                  b.stru_sname;

                  null;
    ELSIF i_view_type = '2' THEN
      --领导视图,第一列出分管领导,第二列出机构
      OPEN o_list_cur FOR
        SELECT t.grid_row_id,
               t.column1,
               t.column2,
               t.column2_values,
               t.rel_state
          FROM (SELECT u.ssic_id AS grid_row_id,
                       u.name AS column1,
                       (SELECT listagg(b.stru_sname, ',') within GROUP(ORDER BY b.stru_sort DESC NULLS LAST, b.sort DESC NULLS LAST,b.stru_sname)
                          FROM dams_branch b, dams_charge_info ci
                         WHERE b.stru_id = ci.stru_id
                           AND ci.model_id = i_model_id
                           AND ci.sys_code = i_sys_code
                           AND ci.ssic_id = u.ssic_id
                           AND ci.status = '1') AS column2,
                       (SELECT listagg(b.stru_id, ',') within GROUP(ORDER BY 1)
                          FROM dams_branch b, dams_charge_info ci
                         WHERE b.stru_id = ci.stru_id
                           AND ci.model_id = i_model_id
                           AND ci.sys_code = i_sys_code
                           AND ci.ssic_id = u.ssic_id
                           AND ci.status = '1') AS column2_values,
                       '1' rel_state,
                       u.duty_sort,
                       u.if_preside_work,
                       u.sort
                  FROM dams_user            u,
                       dams_user_role_rel   ur,
                       dams_branch_relation br
                 WHERE u.ssic_id = ur.ssic_id
                   AND ur.stru_id = br.bnch_id
                   AND br.up_bnch_id = i_stru_id
                   AND ur.auth_state IN ('0', '1')
                   AND EXISTS (SELECT 1
                          FROM dams_charge_model_rel cmr
                         WHERE cmr.model_id = i_model_id
                           AND cmr.sys_code = i_sys_code
                           AND cmr.role_id = ur.role_id)
                UNION ALL
                SELECT ci.ssic_id AS grid_row_id,
                       u.name AS column1,
                       (SELECT listagg(b.stru_sname, ',') within GROUP(ORDER BY b.stru_sort DESC NULLS LAST, b.sort DESC NULLS LAST ,b.stru_sname )
                          FROM dams_branch b, dams_charge_info ci1
                         WHERE b.stru_id = ci1.stru_id
                           AND ci1.model_id = i_model_id
                           AND ci1.sys_code = i_sys_code
                           AND ci1.ssic_id = ci.ssic_id
                           AND ci1.status = '1') AS column2,
                       (SELECT listagg(b.stru_id, ',') within GROUP(ORDER BY 1)
                          FROM dams_branch b, dams_charge_info ci1
                         WHERE b.stru_id = ci1.stru_id
                           AND ci1.model_id = i_model_id
                           AND ci1.sys_code = i_sys_code
                           AND ci1.ssic_id = ci.ssic_id
                           AND ci1.status = '1') AS column2_values,
                       '0' rel_state,
                       NULL,
                       NULL,
                       NULL
                  FROM dams_charge_info ci, dams_user u, dams_branch_relation br
                 WHERE u.ssic_id = ci.ssic_id
                   AND ci.model_id = i_model_id
                   AND ci.sys_code = i_sys_code
                   AND ci.status = '1'
                   AND ci.stru_id = br.bnch_id
                   AND br.up_bnch_id = i_stru_id
                   AND NOT EXISTS (SELECT 1
                          FROM dams_user_role_rel    ur,
                               dams_charge_model_rel cmr,
                               dams_branch_relation  br
                         WHERE ur.ssic_id = ci.ssic_id
                           AND ur.stru_id = br.bnch_id
                           AND br.up_bnch_id = i_stru_id
                           AND ur.role_id = cmr.role_id
                           AND cmr.model_id = i_model_id
                           AND cmr.sys_code = i_sys_code)
                 GROUP BY ci.ssic_id, u.name) t
         ORDER BY t.rel_state DESC,
                  t.duty_sort DESC NULLS LAST,
                  nvl(t.if_preside_work, '502002'),
                  t.sort DESC NULLS LAST,
                  t.column1 ASC NULLS LAST;
    END IF;

  EXCEPTION
    WHEN OTHERS THEN
      IF o_list_cur%ISOPEN THEN
        CLOSE o_list_cur;
      END IF;
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_ret_code := '-1';
      RETURN;
  END proc_get_charge_list;

  PROCEDURE proc_get_edit_list(i_ssic_id   IN VARCHAR2, --统一认证号
                               i_model_id  IN VARCHAR2,
                               i_sys_code  IN VARCHAR2,
                               i_stru_id   IN VARCHAR2,
                               i_view_type IN VARCHAR2,
                               o_ret_code  OUT VARCHAR2, --返回值
                               o_list_cur  OUT SYS_REFCURSOR --查询列表游标
                               ) IS
    v_param     db_log.info%TYPE := i_ssic_id || '|' || i_model_id || '|' ||
                                    i_sys_code || '|' || i_stru_id || '|' ||
                                    i_view_type;
    v_proc_name db_log.proc_name%TYPE := 'pckg_dams_charge_mgt.proc_get_edit_list';
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step,
                  pack_log.start_msg || v_param);
    o_ret_code := '0';

    IF i_view_type = '1' THEN
      --列表页为机构视图时,出分管领导列表
      OPEN o_list_cur FOR
        SELECT u.ssic_id AS grid_row_id, u.name AS column1
          FROM dams_user u,
               (SELECT DISTINCT u.ssic_id
                  FROM dams_user            u,
                       dams_user_role_rel   ur,
                       dams_branch_relation br
                 WHERE u.ssic_id = ur.ssic_id
                   AND ur.stru_id = br.bnch_id
                   AND br.up_bnch_id = i_stru_id
                   AND ur.auth_state IN ('0', '1')
                   AND EXISTS (SELECT 1
                          FROM dams_charge_model_rel cmr
                         WHERE cmr.model_id = i_model_id
                           AND cmr.sys_code = i_sys_code
                           AND cmr.role_id = ur.role_id)) t
         WHERE u.ssic_id = t.ssic_id
         ORDER BY u.duty_sort DESC NULLS LAST,
                  nvl(u.if_preside_work, '502002'),
                  u.sort DESC NULLS LAST,
                  u.name ASC NULLS LAST;

    ELSIF i_view_type = '2' THEN
      --列表页为领导视图时,出分管机构列表
      OPEN o_list_cur FOR
        SELECT b.stru_id AS grid_row_id, b.stru_sname AS column1
          FROM dams_branch b,
               (SELECT DISTINCT b.stru_id
                  FROM dams_branch b, dams_branch_relation br
                 WHERE b.stru_id = br.bnch_id
                   AND br.up_bnch_id = i_stru_id
                   AND b.stru_sign = '3' --内设机构
                   AND b.stru_lv = '53' --处室
                   AND b.stru_state = '1') t
         WHERE b.stru_id = t.stru_id
         ORDER BY b.stru_sort DESC NULLS LAST,
                  b.sort DESC NULLS LAST,
                 b.stru_sname;
    END IF;

  EXCEPTION
    WHEN OTHERS THEN
      IF o_list_cur%ISOPEN THEN
        CLOSE o_list_cur;
      END IF;
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_ret_code := '-1';
      RETURN;
  END proc_get_edit_list;


  PROCEDURE proc_edit_charge_rel(i_ssic_id   IN VARCHAR2, --统一认证号
                                 i_model_id  IN VARCHAR2,
                                 i_sys_code  IN VARCHAR2,
                                 i_base_id   IN VARCHAR2, --基础配置id
                                 i_edit_data IN VARCHAR2, --配置数据
                                 i_view_type IN VARCHAR2, --列表页视图类型1机构视图2领导视图
                                 o_ret_code  OUT VARCHAR2, --返回值
                                 o_msg       OUT VARCHAR2) IS
    v_param          db_log.info%TYPE := i_ssic_id || '|' || i_model_id || '|' ||
                                         i_sys_code || '|' || i_base_id || '|' ||
                                         i_edit_data || '|' || i_view_type;
    v_proc_name      db_log.proc_name%TYPE := 'pckg_dams_charge_mgt.proc_edit_charge_rel';
    v_edit_data_list dams_arrytype;
    v_ssic_id        dams_user.ssic_id%TYPE;
    v_stru_id        dams_branch.stru_id%TYPE;
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step,
                  pack_log.start_msg || v_param);
    o_ret_code := '0';

    v_edit_data_list := pckg_dams_util.func_str_to_array(i_edit_data, ',', '1');

    IF i_view_type = '1' THEN
      --列表页为机构视图时,i_base_id为机构id,i_edit_data为配置的领导
      v_stru_id := i_base_id;

      UPDATE dams_charge_info t
         SET t.status = '0'
       WHERE t.model_id = i_model_id
         AND t.stru_id = v_stru_id
         AND t.sys_code = i_sys_code;

      IF v_edit_data_list IS NOT NULL AND i_edit_data IS NOT NULL THEN
        FOR i IN 1 .. (v_edit_data_list.count)
        LOOP
          v_ssic_id := v_edit_data_list(i);

          MERGE INTO dams_charge_info t
          USING (SELECT i_model_id AS model_id,
                        v_stru_id  AS stru_id,
                        v_ssic_id  AS ssic_id,
                        i_sys_code AS sys_code
                   FROM dual) a
          ON (t.model_id = a.model_id AND t.stru_id = a.stru_id AND t.ssic_id = a.ssic_id AND t.sys_code = a.sys_code)
          WHEN MATCHED THEN
            UPDATE
               SET t.status      = '1',
                   t.update_time = SYSDATE,
                   t.update_user = i_ssic_id
          WHEN NOT MATCHED THEN
            INSERT
              (model_id,
               stru_id,
               ssic_id,
               sys_code,
               status,
               create_time,
               create_user)
            VALUES
              (a.model_id,
               a.stru_id,
               a.ssic_id,
               a.sys_code,
               '1',
               SYSDATE,
               i_ssic_id);
        END LOOP;
      END IF;

    ELSIF i_view_type = '2' THEN

      v_ssic_id := i_base_id;

      UPDATE dams_charge_info t
         SET t.status = '0'
       WHERE t.model_id = i_model_id
         AND t.ssic_id = v_ssic_id
         AND t.sys_code = i_sys_code;

      IF v_edit_data_list IS NOT NULL AND i_edit_data IS NOT NULL THEN
        FOR i IN 1 .. (v_edit_data_list.count)
        LOOP
          v_stru_id := v_edit_data_list(i);

          MERGE INTO dams_charge_info t
          USING (SELECT i_model_id AS model_id,
                        v_stru_id  AS stru_id,
                        v_ssic_id  AS ssic_id,
                        i_sys_code AS sys_code
                   FROM dual) a
          ON (t.model_id = a.model_id AND t.stru_id = a.stru_id AND t.ssic_id = a.ssic_id AND t.sys_code = a.sys_code)
          WHEN MATCHED THEN
            UPDATE
               SET t.status      = '1',
                   t.update_time = SYSDATE,
                   t.update_user = i_ssic_id
          WHEN NOT MATCHED THEN
            INSERT
              (model_id,
               stru_id,
               ssic_id,
               sys_code,
               status,
               create_time,
               create_user)
            VALUES
              (a.model_id,
               a.stru_id,
               a.ssic_id,
               a.sys_code,
               '1',
               SYSDATE,
               i_ssic_id);
        END LOOP;
      END IF;
    END IF;

  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_ret_code := '-1';
      RETURN;
  END proc_edit_charge_rel;

END pckg_dams_charge_mgt;

//


CREATE OR REPLACE PACKAGE pckg_dams_holidayset IS

  
  PROCEDURE proc_holidayset_add(i_user_id      IN VARCHAR2, --人员id
                                i_role_id      IN VARCHAR2, --操作人员角色
                                i_branch_id    IN VARCHAR2, --机构id
                                i_native_code  IN VARCHAR2, --国家代码
                                i_holiday_time IN VARCHAR2, --节假日期
                                i_holiday_name IN VARCHAR2, --节假日名称
                                i_remark       IN VARCHAR2, --备注
                                o_flag         OUT VARCHAR2, --0  成功  1 取信息错
                                o_msg          OUT VARCHAR2 --错误信息
                                );


  PROCEDURE proc_holidayset_mod(i_user_id      IN VARCHAR2, --人员id
                                i_role_id      IN VARCHAR2, --操作人员角色
                                i_branch_id    IN VARCHAR2, --机构id
                                i_native_code  IN VARCHAR2, --国家代码
                                i_holiday_time IN VARCHAR2, --节假日期
                                i_holiday_name IN VARCHAR2, --节假日名称
                                i_remark       IN VARCHAR2, --备注
                                o_flag         OUT VARCHAR2, --0  成功  1 取信息错
                                o_msg          OUT VARCHAR2 --错误信息
                                );

  /******************************************************************************
      --存储过程名：    PROC_HOLIDAYSET_DEL
      --存储过程描述：  删除节假日
      --功能模块：      节假日管理
      --作者：          kfzx-wangjian
      --时间：          2012-10-10
      --修改历史:
  ******************************************************************************/
  PROCEDURE proc_holidayset_del(i_user_id      IN VARCHAR2, --人员id
                                i_role_id      IN VARCHAR2, --操作人员角色
                                i_branch_id    IN VARCHAR2, --机构id
                                i_native_code  IN VARCHAR2, --国家代码
                                i_holiday_time IN VARCHAR2, --节假日期
                                o_flag         OUT VARCHAR2, --0  成功  1 取信息错
                                o_msg          OUT VARCHAR2 --错误信息
                                );

  /******************************************************************************
      --存储过程名：    PROC_HOLIDAYSET_DEL
      --存储过程描述：  删除一年的节假日
      --功能模块：      节假日管理
      --作者：          kfzx-wangjian
      --时间：          2012-12-14
      --修改历史:
  ******************************************************************************/
  PROCEDURE proc_holidayset_batch_del(i_user_id      IN VARCHAR2, --人员id
                                      i_role_id      IN VARCHAR2, --操作人员角色
                                      i_branch_id    IN VARCHAR2, --机构id
                                      i_native_code  IN VARCHAR2, --国家代码
                                      i_holiday_time IN VARCHAR2, --节假日期
                                      o_flag         OUT VARCHAR2, --0  成功  1 取信息错
                                      o_msg          OUT VARCHAR2 --错误信息
                                      );

  /******************************************************************************
      --存储过程名：    PROC_HOLIDAYSET_QUERY
      --存储过程描述：  查询节假日
      --功能模块：      节假日管理
      --作者：          kfzx-wangjian
      --时间：          2012-10-10
      --修改历史:
  ******************************************************************************/
  PROCEDURE proc_holidayset_query(i_user_id      IN VARCHAR2, --人员id
                                  i_role_id      IN VARCHAR2, --操作人员角色
                                  i_branch_id    IN VARCHAR2, --机构id
                                  i_native_code  IN VARCHAR2, --国家代码
                                  i_holiday_time IN VARCHAR2, --节假日期(年月)
                                  o_totalnum     OUT VARCHAR2, --数量
                                  o_flag         OUT VARCHAR2, --0  成功  1 取信息错
                                  o_msg          OUT VARCHAR2, --错误信息
                                  o_info_list    OUT SYS_REFCURSOR, --节假日设置信息
                                  o_isadmin      OUT VARCHAR2);

  /******************************************************************************
      --存储过程名：    PROC_HOLIDAYSET_QUERY
      --存储过程描述：  条件查询节假日
      --功能模块：      节假日管理
      --作者：          kfzx-wangjian
      --时间：          2012-12-14
      --修改历史:
  ******************************************************************************/
  PROCEDURE proc_query_bycon(i_user_id      IN VARCHAR2, --人员id
                             i_role_id      IN VARCHAR2, --操作人员角色
                             i_branch_id    IN VARCHAR2, --机构id
                             i_native_code  IN VARCHAR2, --国家代码
                             i_holiday_time IN VARCHAR2, --节假日期(年月)
                             o_totalnum     OUT VARCHAR2, --数量
                             o_flag         OUT VARCHAR2, --0  成功  1 取信息错
                             o_msg          OUT VARCHAR2, --错误信息
                             o_info_list    OUT SYS_REFCURSOR --节假日设置信息
                             );

  /******************************************************************************
      --存储过程名：    PROC_HOLIDAYSET_GET_NATIVE
      --存储过程描述：  查询国家
      --功能模块：      节假日管理
      --作者：          kfzx-wangjian
      --时间：          2012-10-18
      --修改历史:
  ******************************************************************************/
  PROCEDURE proc_holidayset_get_native(o_flag        OUT VARCHAR2, --0  成功  1 取信息错
                                       o_msg         OUT VARCHAR2, --错误信息
                                       o_native_list OUT SYS_REFCURSOR --国家信息
                                       );

  /******************************************************************************
      --存储过程名：    PROC_HOLIDAYSET_BATCH_ADD
      --存储过程描述：  批次新增固定节假日
      --功能模块：      节假日管理
      --作者：          kfzx-wangjian
      --时间：          2012-10-18
      --修改历史:
  ******************************************************************************/
  PROCEDURE proc_holidayset_batch_add(i_user_id      IN VARCHAR2, --人员id
                                      i_role_id      IN VARCHAR2, --操作人员角色
                                      i_branch_id    IN VARCHAR2, --机构id
                                      i_native_code  IN VARCHAR2, --国家代码
                                      i_holiday_time IN VARCHAR2, --节假日期（年）
                                      i_year_dev     IN VARCHAR2, --是否闰年
                                      i_holiday_name IN VARCHAR2, --节假日名称
                                      i_remark       IN VARCHAR2, --备注
                                      i_weekend_date IN VARCHAR2, --周末设定
                                      o_flag         OUT VARCHAR2, --0  成功  1 取信息错
                                      o_msg          OUT VARCHAR2 --错误信息
                                      );

  /******************************************************************************
      --存储过程名：    proc_holidayset_init
      --存储过程描述：  是否总行或境外机构系统管理员 （并返回国家代码）
      --功能模块：      节假日管理
      --作者：          kfzx-wangjian
      --时间：          2012-12-26
      --修改历史:       20131122 kfzx-linlc 总行系统管理员 或 境外机构管理员
  ******************************************************************************/
  PROCEDURE proc_holidayset_init(i_user_id   IN VARCHAR2, --人员id
                                 i_stru_id   IN VARCHAR2, --机构id
                                 o_flag      OUT VARCHAR2, --0  成功  -1 取信息错
                                 o_auth_type OUT VARCHAR2, --1 有总行管理权限 0 没有管理权限 2该机构可特殊配置（一二级行）
                                 o_country   OUT VARCHAR2, --国家代码
                                 o_branch_id OUT VARCHAR2 --分支机构id
                                 );

  /******************************************************************************
      --存储过程名：    proc_enable_branch_setting
      --存储过程描述：  开启机构节假日特殊配置
      --功能模块：      节假日管理
      --作者：          kfzx-wangjian
      --时间：          2012-12-26
      --修改历史:
  ******************************************************************************/
  PROCEDURE proc_enable_branch_setting(i_user_id      IN VARCHAR2, --人员id
                                       i_branch_id    IN VARCHAR2, --机构id
                                       i_holiday_time IN VARCHAR2, --年月
                                       i_country      IN VARCHAR2, --国家
                                       o_flag         OUT VARCHAR2, --0  成功  -1 取信息错
                                       o_msg          OUT VARCHAR2);

  /******************************************************************************
      --存储过程名：    proc_disable_branch_setting
      --存储过程描述：  关闭机构节假日特殊配置
      --功能模块：      节假日管理
      --作者：          kfzx-wangjian
      --时间：          2012-12-26
      --修改历史:
  ******************************************************************************/
  PROCEDURE proc_disable_branch_setting(i_user_id      IN VARCHAR2, --人员id
                                        i_branch_id    IN VARCHAR2, --机构id
                                        i_holiday_time IN VARCHAR2, --年月
                                        i_country      IN VARCHAR2, --国家
                                        o_flag         OUT VARCHAR2, --0  成功  -1 取信息错
                                        o_msg          OUT VARCHAR2);

  /******************************************************************************
      --存储过程名：    PROC_HOLIDAYSET_BATCH_ADD
      --存储过程描述：  根据国家代码,开始日期,结束日期查找两个时间段之间的工作日
      --功能模块：      节假日管理
      --作者：          kfzx-chenl04
      --时间：          2012-10-24
      --修改历史:
  ******************************************************************************/
  FUNCTION fun_differtime(i_nativecode IN VARCHAR2, --国家代码
                          i_starttime  IN VARCHAR2, --开始日期(格式:20121012)
                          i_endtime    IN VARCHAR2 --结束日期(格式:20121012)
                          ) RETURN NUMBER;

  FUNCTION fun_branch_bank(i_branch_id IN VARCHAR2 --所属部门ID
                           ) RETURN VARCHAR2;

  /******************************************************************************
      --存储过程名：    func_holiday_check
      --存储过程描述：  检查当前时间是否合适时间（发邮件） 0非工作时间,１工作时间
      --功能模块：      节假日管理
      --作者：          kfzx-guzh02
      --时间：          2017-12-01
      --修改历史:
  ******************************************************************************/
  FUNCTION func_holiday_check(in_stru_id     IN VARCHAR2,
                              in_country     IN VARCHAR2,
                              in_sys_date    IN VARCHAR2) RETURN NUMBER;
END pckg_dams_holidayset;
//

CREATE OR REPLACE PACKAGE BODY pckg_dams_holidayset IS

  /******************************************************************************
      --存储过程名：    PROC_HOLIDAYSET_ADD
      --存储过程描述：  新增节假日
      --功能模块：      节假日管理
      --作者：          kfzx-wangjian
      --时间：          2012-10-10
      --修改历史:
  ******************************************************************************/
  PROCEDURE proc_holidayset_add(i_user_id      IN VARCHAR2, --人员id
                                i_role_id      IN VARCHAR2, --操作人员角色
                                i_branch_id    IN VARCHAR2, --机构id
                                i_native_code  IN VARCHAR2, --国家代码
                                i_holiday_time IN VARCHAR2, --节假日期
                                i_holiday_name IN VARCHAR2, --节假日名称
                                i_remark       IN VARCHAR2, --备注
                                o_flag         OUT VARCHAR2, --0  成功  1 取信息错
                                o_msg          OUT VARCHAR2 --错误信息
                                ) IS
    v_param     db_log.info%TYPE := i_branch_id || '|' || i_native_code || '|' ||
                                    i_holiday_time || '|' || i_holiday_name || '|' ||
                                    i_remark;
    v_proc_name db_log.proc_name%TYPE := 'PCKG_DAMS_HOLIDAYSET.PROC_HOLIDAYSET_ADD';
    v_step      db_log.step_no%TYPE;
    v_cur_date   VARCHAR2(8);
    v_start_time VARCHAR2(14);
    v_end_time   VARCHAR2(14);
    v_det_str    VARCHAR2(300);
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step,
                  pack_log.start_msg || v_param);
    o_flag := '0';
    o_msg  := '';
    v_step := '1';

    SELECT to_char(SYSDATE, 'yyyyMMdd') INTO v_cur_date FROM dual;
    --SELECT to_char(SYSDATE, 'yyyyMMddHH24miss') INTO v_start_time FROM dual;

    --新增节假日
    INSERT INTO dams_holiday_set
      (key_id,
       native_code,
       holiday_time,
       holiday_name,
       remark,
       cret_time,
       holiday_type,
       stru_id)
    VALUES
      (dams_holidayset_seq.nextval,
       i_native_code,
       i_holiday_time,
       i_holiday_name,
       i_remark,
       v_cur_date,
       '2',
       i_branch_id);
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);

    IF (i_branch_id IS NULL) THEN
      v_det_str := pckg_dams_util.func_decode('COUNTRY', i_native_code, 'DAMS');
    ELSE
      v_det_str := pckg_dams_util.func_get_stru_name(i_branch_id);
    END IF;
    SELECT to_char(SYSDATE, 'yyyyMMddHH24miss') INTO v_end_time FROM dual;
    pckg_dams_util.proc_dams_logger('HOLIDAY_OPERATE', '014', v_end_time,
                                    i_user_id,
                                    '节假日管理:' || i_holiday_time || ' 新增节假日:' ||
                                     i_holiday_name || ' (' || v_det_str || ')');
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, v_step, '异常！' || v_param);
      o_flag := '-1';
      o_msg  := 'PCKG_DAMS_HOLIDAYSET.PROC_HOLIDAYSET_ADD' || '模块内部错误:' ||
                SQLERRM;
      ROLLBACK;
  END proc_holidayset_add;

  /******************************************************************************
      --存储过程名：    PROC_HOLIDAYSET_MOD
      --存储过程描述：  修改节假日名称
      --功能模块：      节假日管理
      --作者：          kfzx-wangjian
      --时间：          2012-10-10
      --修改历史:
  ******************************************************************************/
  PROCEDURE proc_holidayset_mod(i_user_id      IN VARCHAR2, --人员id
                                i_role_id      IN VARCHAR2, --操作人员角色
                                i_branch_id    IN VARCHAR2, --机构id
                                i_native_code  IN VARCHAR2, --国家代码
                                i_holiday_time IN VARCHAR2, --节假日期
                                i_holiday_name IN VARCHAR2, --节假日名称
                                i_remark       IN VARCHAR2, --备注
                                o_flag         OUT VARCHAR2, --0  成功  1 取信息错
                                o_msg          OUT VARCHAR2 --错误信息
                                ) IS
    v_param     db_log.info%TYPE := i_holiday_time || '|' || i_holiday_name || '|' ||
                                    i_remark;
    v_proc_name db_log.proc_name%TYPE := 'PCKG_DAMS_HOLIDAYSET.PROC_HOLIDAYSET_MOD';
    v_step      db_log.step_no%TYPE;
    v_count     NUMBER;
    v_cur_date  VARCHAR2(8);
    --v_start_time VARCHAR2(14);
    v_end_time VARCHAR2(14);
    v_det_str  VARCHAR2(300);
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step,
                  pack_log.start_msg || v_param);
    o_flag := '0';
    o_msg  := '';
    v_step := '1';

    SELECT to_char(SYSDATE, 'yyyyMMdd') INTO v_cur_date FROM dual;
    --SELECT to_char(SYSDATE, 'yyyyMMddHH24miss') INTO v_start_time FROM dual;

    SELECT COUNT(1)
      INTO v_count
      FROM dams_holiday_set
     WHERE holiday_time = i_holiday_time
       AND (native_code = i_native_code OR stru_id = i_branch_id);

    IF v_count = 0 THEN
      --新增节假日
      INSERT INTO dams_holiday_set
        (key_id,
         native_code,
         holiday_time,
         holiday_name,
         remark,
         cret_time,
         holiday_type,
         stru_id)
      VALUES
        (dams_holidayset_seq.nextval,
         i_native_code,
         i_holiday_time,
         i_holiday_name,
         i_remark,
         v_cur_date,
         '2',
         i_branch_id);
    ELSE
      --修改节假日名称
      UPDATE dams_holiday_set
         SET holiday_name = i_holiday_name,
             remark       = i_remark,
             holiday_type = '2'
       WHERE holiday_time = i_holiday_time
         AND (native_code = i_native_code OR stru_id = i_branch_id);
    END IF;

    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);

    IF (i_branch_id IS NULL) THEN
      v_det_str := pckg_dams_util.func_decode('COUNTRY', i_native_code, 'DAMS');
    ELSE
      v_det_str := pckg_dams_util.func_get_stru_name(i_branch_id);
    END IF;
    SELECT to_char(SYSDATE, 'yyyyMMddHH24miss') INTO v_end_time FROM dual;
    pckg_dams_util.proc_dams_logger('HOLIDAY_OPERATE', '014', v_end_time,
                                    i_user_id,
                                    '节假日管理:' || i_holiday_time || ' 设为' ||
                                     i_holiday_name || ' (' || v_det_str || ')');
    /*PCKG_DAMS_LOG.proc_log_writer(i_user_id, --操作人员编码
    i_role_id, --操作人员角色
    to_char(sysdate, 'yyyyMMdd'), --交易日期（yyyyMMdd）
    i_branch_id,--机构编码
    '03',  --操作类型
    'b005',     --操作对象编码
    '节假日管理',   --操作对象名称
    '',    --过程号
    'PCKG_DAMS_HOLIDAYSET.PROC_HOLIDAYSET_MOD',  --过程名称
    v_start_time,   --交易开始时间
    v_end_time);  --交易结束时间*/

  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, v_step, '异常！' || v_param);
      o_flag := '-1';
      o_msg  := 'PCKG_DAMS_HOLIDAYSET.PROC_HOLIDAYSET_MOD' || '模块内部错误:' ||
                SQLERRM;
      ROLLBACK;
  END proc_holidayset_mod;

  /******************************************************************************
      --存储过程名：    PROC_HOLIDAYSET_DEL
      --存储过程描述：  删除节假日
      --功能模块：      节假日管理
      --作者：          kfzx-wangjian
      --时间：          2012-10-10
      --修改历史:
  ******************************************************************************/
  PROCEDURE proc_holidayset_del(i_user_id      IN VARCHAR2, --人员id
                                i_role_id      IN VARCHAR2, --操作人员角色
                                i_branch_id    IN VARCHAR2, --机构id
                                i_native_code  IN VARCHAR2, --国家代码
                                i_holiday_time IN VARCHAR2, --节假日期
                                o_flag         OUT VARCHAR2, --0  成功  1 取信息错
                                o_msg          OUT VARCHAR2 --错误信息
                                ) IS
    v_param     db_log.info%TYPE := i_holiday_time || '|' || i_native_code;
    v_proc_name db_log.proc_name%TYPE := 'PCKG_DAMS_HOLIDAYSET.PROC_HOLIDAYSET_DEL';
    v_step      db_log.step_no%TYPE;
    --v_start_time VARCHAR2(14);
    v_end_time VARCHAR2(14);
    v_det_str  VARCHAR2(300);
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step,
                  pack_log.start_msg || v_param);
    o_flag := '0';
    o_msg  := '';
    v_step := '1';
    --SELECT to_char(SYSDATE, 'yyyyMMddHH24miss') INTO v_start_time FROM dual;

    --删除节假日
    DELETE FROM dams_holiday_set
     WHERE holiday_time = i_holiday_time
       AND (native_code = i_native_code OR stru_id = i_branch_id);

    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);

    IF (i_branch_id IS NULL) THEN
      v_det_str := pckg_dams_util.func_decode('COUNTRY', i_native_code, 'DAMS');
    ELSE
      v_det_str := pckg_dams_util.func_get_stru_name(i_branch_id);
    END IF;
    SELECT to_char(SYSDATE, 'yyyyMMddHH24miss') INTO v_end_time FROM dual;
    pckg_dams_util.proc_dams_logger('HOLIDAY_OPERATE', '014', v_end_time,
                                    i_user_id,
                                    '节假日管理:' || i_holiday_time || ' 删除当天假日' || ' (' ||
                                     v_det_str || ')');
    /*PCKG_DAMS_LOG.proc_log_writer(i_user_id, --操作人员编码
    i_role_id, --操作人员角色
    to_char(sysdate, 'yyyyMMdd'), --交易日期（yyyyMMdd）
    i_branch_id,--机构编码
    '04',  --操作类型
    'b005',     --操作对象编码
    '节假日管理',   --操作对象名称
    '',    --过程号
    'PCKG_DAMS_HOLIDAYSET.PROC_HOLIDAYSET_DEL',  --过程名称
    v_start_time,   --交易开始时间
    v_end_time);  --交易结束时间*/

  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, v_step, '异常！' || v_param);
      o_flag := '-1';
      o_msg  := 'PCKG_DAMS_HOLIDAYSET.PROC_HOLIDAYSET_DEL' || '模块内部错误:' ||
                SQLERRM;
      ROLLBACK;
  END proc_holidayset_del;

  /******************************************************************************
      --存储过程名：    PROC_HOLIDAYSET_batch_DEL
      --存储过程描述：  删除一年的节假日
      --功能模块：      节假日管理
      --作者：          kfzx-wangjian
      --时间：          2012-12-14
      --修改历史:
  ******************************************************************************/
  PROCEDURE proc_holidayset_batch_del(i_user_id      IN VARCHAR2, --人员id
                                      i_role_id      IN VARCHAR2, --操作人员角色
                                      i_branch_id    IN VARCHAR2, --机构id
                                      i_native_code  IN VARCHAR2, --国家代码
                                      i_holiday_time IN VARCHAR2, --节假日期
                                      o_flag         OUT VARCHAR2, --0  成功  1 取信息错
                                      o_msg          OUT VARCHAR2 --错误信息
                                      ) IS
    v_param     db_log.info%TYPE := i_holiday_time || '|' || i_native_code;
    v_proc_name db_log.proc_name%TYPE := 'PCKG_DAMS_HOLIDAYSET.PROC_HOLIDAYSET_BATCH_DEL';
    v_step      db_log.step_no%TYPE;
    --v_start_time VARCHAR2(14);
    v_end_time    VARCHAR2(14);
    v_count       NUMBER;
    v_country_str VARCHAR2(30);
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step,
                  pack_log.start_msg || v_param);
    o_flag := '0';
    o_msg  := '';
    v_step := '1';
    --SELECT to_char(SYSDATE, 'yyyyMMddHH24miss') INTO v_start_time FROM dual;

    SELECT COUNT(1)
      INTO v_count
      FROM dams_holiday_set t
     WHERE t.holiday_time LIKE i_holiday_time || '%'
       AND t.native_code = i_native_code
       AND t.holiday_type = '1';

    IF v_count = 0 THEN
      --该年没有设置过节假日
      o_flag := '-2';
    ELSE
      --删除节假日
      DELETE FROM dams_holiday_set
       WHERE holiday_time LIKE i_holiday_time || '%'
         AND native_code = i_native_code
         AND holiday_type = '1';
    END IF;

    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);

    v_country_str := pckg_dams_util.func_decode('COUNTRY', i_native_code, 'DAMS');
    SELECT to_char(SYSDATE, 'yyyyMMddHH24miss') INTO v_end_time FROM dual;
    pckg_dams_util.proc_dams_logger('HOLIDAY_OPERATE', '014', v_end_time,
                                    i_user_id,
                                    '节假日管理: 删除' || i_holiday_time || '年双休日' || ' (' ||
                                     v_country_str || ')');
    /*
    PCKG_DAMS_LOG.proc_log_writer(i_user_id, --操作人员编码
    i_role_id, --操作人员角色
    to_char(sysdate, 'yyyyMMdd'), --交易日期（yyyyMMdd）
    i_branch_id,--机构编码
    '04',  --操作类型
    'b005',     --操作对象编码
    '节假日管理',   --操作对象名称
    '',    --过程号
    'PCKG_DAMS_HOLIDAYSET.PROC_HOLIDAYSET_BATCH_DEL',  --过程名称
    v_start_time,   --交易开始时间
    v_end_time);  --交易结束时间*/

  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, v_step, '异常！' || v_param);
      o_flag := '-1';
      o_msg  := 'PCKG_DAMS_HOLIDAYSET.PROC_HOLIDAYSET_BATCH_DEL' || '模块内部错误:' ||
                SQLERRM;
      ROLLBACK;
  END proc_holidayset_batch_del;

  /******************************************************************************
      --存储过程名：    PROC_HOLIDAYSET_QUERY
      --存储过程描述：  查询节假日
      --功能模块：      节假日管理
      --作者：          kfzx-wangjian
      --时间：          2012-10-10
      --修改历史:
  ******************************************************************************/
  PROCEDURE proc_holidayset_query(i_user_id      IN VARCHAR2, --人员id
                                  i_role_id      IN VARCHAR2, --操作人员角色
                                  i_branch_id    IN VARCHAR2, --机构id
                                  i_native_code  IN VARCHAR2, --国家代码
                                  i_holiday_time IN VARCHAR2, --节假日期(年月)
                                  o_totalnum     OUT VARCHAR2, --数量
                                  o_flag         OUT VARCHAR2, --0  成功  1 取信息错
                                  o_msg          OUT VARCHAR2, --错误信息
                                  o_info_list    OUT SYS_REFCURSOR, --节假日设置信息
                                  o_isadmin      OUT VARCHAR2) IS
    v_param     db_log.info%TYPE := i_native_code || '|' || i_holiday_time;
    v_proc_name db_log.proc_name%TYPE := 'PCKG_DAMS_HOLIDAYSET.PROC_HOLIDAYSET_QUERY';
    v_step      db_log.step_no%TYPE;
    --v_start_time VARCHAR2(14);
    --v_end_time   VARCHAR2(14);
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step,
                  pack_log.start_msg || v_param);
    o_flag     := '0';
    o_msg      := '';
    v_step     := '1';
    o_totalnum := '0';
    --SELECT to_char(SYSDATE, 'yyyyMMddHH24miss') INTO v_start_time FROM dual;
    IF (i_branch_id IS NULL) THEN
      SELECT decode(COUNT(1), '0', '0', '1')
        INTO o_isadmin
        FROM dams_user_role_rel t, dams_branch b
       WHERE t.stru_id = b.stru_id
         AND t.ssic_id = i_user_id
         AND t.role_id = 'OIS098'
         AND t.auth_state IN ('0', '2')
         AND (t.stru_id = '0010100500' OR
             (b.country = i_native_code AND i_native_code <> '156'));

      OPEN o_info_list FOR
        SELECT key_id,
               native_code,
               holiday_time,
               holiday_name,
               remark,
               cret_time,
               holiday_type
          FROM dams_holiday_set
         WHERE native_code = i_native_code
           AND holiday_time LIKE i_holiday_time || '%';
      /*
      SELECT COUNT(1)
        INTO o_totalnum
        FROM dams_holiday_set
       WHERE native_code = i_native_code
         AND holiday_time LIKE i_holiday_time || '%';*/

    ELSE
      --是否有特殊设定过
      SELECT decode(COUNT(1), '0', '0', '2')
        INTO o_isadmin
        FROM dams_holiday_set t
       WHERE t.stru_id = i_branch_id
         AND t.holiday_time LIKE i_holiday_time || '%';

      --i_branch_id IS NOT NULL
      OPEN o_info_list FOR
        WITH tmp AS
         (SELECT tt.stru_id
            FROM dams_holiday_set tt, dams_branch b
           WHERE (EXISTS (SELECT 1
                            FROM dams_branch_relation r
                           WHERE r.bnch_id = i_branch_id
                             AND tt.stru_id = r.up_bnch_id) OR
                  tt.native_code = i_native_code)
             AND tt.holiday_time LIKE i_holiday_time || '%'
             AND b.stru_id = tt.stru_id
           ORDER BY b.man_grade DESC NULLS LAST)
        SELECT t.key_id,
               t.native_code,
               t.holiday_time,
               t.holiday_name,
               t.remark,
               t.cret_time,
               t.holiday_type
          FROM dams_holiday_set t, dams_branch b
         WHERE (EXISTS (SELECT 1
                          FROM dams_branch_relation r
                         WHERE r.bnch_id = i_branch_id
                           AND t.stru_id = r.up_bnch_id) OR
                t.native_code = i_native_code)
           AND t.holiday_time LIKE i_holiday_time || '%'
           AND b.stru_id(+) = t.stru_id
           AND (EXISTS (SELECT 1
                          FROM (SELECT tmp.stru_id FROM tmp WHERE rownum = 1) t2
                         WHERE t2.stru_id = t.stru_id) OR NOT EXISTS
                (SELECT 1 FROM tmp WHERE rownum = 1) AND t.stru_id IS NULL);

    END IF;
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);

    --SELECT to_char(SYSDATE, 'yyyyMMddHH24miss') INTO v_end_time FROM dual;
    /*PCKG_DAMS_LOG.proc_log_writer(i_user_id, --操作人员编码
    i_role_id, --操作人员角色
    to_char(sysdate, 'yyyyMMdd'), --交易日期（yyyyMMdd）
    i_branch_id,--机构编码
    '01',  --操作类型
    'b005',     --操作对象编码
    '节假日管理',   --操作对象名称
    '',    --过程号
    'PCKG_DAMS_HOLIDAYSET.PROC_HOLIDAYSET_QUERY',  --过程名称
    v_start_time,   --交易开始时间
    v_end_time);  --交易结束时间*/

  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, v_step, '异常！' || v_param);
      o_flag := '-1';
      o_msg  := 'PCKG_DAMS_HOLIDAYSET.PROC_HOLIDAYSET_QUERY' || '模块内部错误:' ||
                SQLERRM;
      IF o_info_list%ISOPEN THEN
        CLOSE o_info_list;
      END IF;
  END proc_holidayset_query;

  /******************************************************************************
      --存储过程名：    PROC_HOLIDAYSET_QUERY
      --存储过程描述：  条件查询节假日
      --功能模块：      节假日管理
      --作者：          kfzx-wangjian
      --时间：          2012-12-14
      --修改历史:
  ******************************************************************************/
  PROCEDURE proc_query_bycon(i_user_id      IN VARCHAR2, --人员id
                             i_role_id      IN VARCHAR2, --操作人员角色
                             i_branch_id    IN VARCHAR2, --机构id
                             i_native_code  IN VARCHAR2, --国家代码
                             i_holiday_time IN VARCHAR2, --节假日期(年月)
                             o_totalnum     OUT VARCHAR2, --数量
                             o_flag         OUT VARCHAR2, --0  成功  1 取信息错
                             o_msg          OUT VARCHAR2, --错误信息
                             o_info_list    OUT SYS_REFCURSOR --节假日设置信息
                             ) IS
    v_param     db_log.info%TYPE := i_native_code || '|' || i_holiday_time;
    v_proc_name db_log.proc_name%TYPE := 'PCKG_DAMS_HOLIDAYSET.PROC_QUERY_BYCON';
    v_step      db_log.step_no%TYPE;
    --v_start_time VARCHAR2(14);
    --v_end_time   VARCHAR2(14);
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step,
                  pack_log.start_msg || v_param);
    o_flag := '0';
    o_msg  := '';
    v_step := '1';
    --SELECT to_char(SYSDATE, 'yyyyMMddHH24miss') INTO v_start_time FROM dual;

    /*
    TODO: owner="kfzx-linlc" category="Fix" created="2014/4/29"
    text="没有处理特殊机构"
    */
    OPEN o_info_list FOR
      SELECT t.key_id,
             t.native_code,
             t1.dictvalue,
             to_char(to_date(t.holiday_time, 'yyyy-MM-dd'), 'yyyy-MM-dd') holiday_time,
             t.holiday_name,
             t.remark,
             to_char(to_date(t.cret_time, 'yyyy-MM-dd'), 'yyyy-MM-dd') cret_time
        FROM dams_holiday_set t, dams_gen_dict t1
       WHERE t1.dicttype = 'COUNTRY'
         AND t.native_code = t1.dictcode
         AND t.native_code = i_native_code
         AND t.holiday_time LIKE i_holiday_time || '%'
         and t1.sys_code = 'DAMS'
         and t1.dictlang = 'zh_CN'
       ORDER BY t.holiday_time;

    SELECT COUNT(1)
      INTO o_totalnum
      FROM dams_holiday_set
     WHERE native_code = i_native_code
       AND holiday_time LIKE i_holiday_time || '%';

    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);

    --SELECT to_char(SYSDATE, 'yyyyMMddHH24miss') INTO v_end_time FROM dual;
    /*PCKG_DAMS_LOG.proc_log_writer(i_user_id, --操作人员编码
    i_role_id, --操作人员角色
    to_char(sysdate, 'yyyyMMdd'), --交易日期（yyyyMMdd）
    i_branch_id,--机构编码
    '01',  --操作类型
    'b005',     --操作对象编码
    '节假日管理',   --操作对象名称
    '',    --过程号
    'PCKG_DAMS_HOLIDAYSET.PROC_QUERY_BYCON',  --过程名称
    v_start_time,   --交易开始时间
    v_end_time);  --交易结束时间*/

  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, v_step, '异常！' || v_param);
      o_flag := '-1';
      o_msg  := 'PCKG_DAMS_HOLIDAYSET.PROC_QUERY_BYCON' || '模块内部错误:' || SQLERRM;
      IF o_info_list%ISOPEN THEN
        CLOSE o_info_list;
      END IF;
  END proc_query_bycon;

  /******************************************************************************
      --存储过程名：    PROC_HOLIDAYSET_GET_NATIVE
      --存储过程描述：  查询国家
      --功能模块：      节假日管理
      --作者：          kfzx-wangjian
      --时间：          2012-10-18
      --修改历史:
  ******************************************************************************/
  PROCEDURE proc_holidayset_get_native(o_flag        OUT VARCHAR2, --0  成功  1 取信息错
                                       o_msg         OUT VARCHAR2, --错误信息
                                       o_native_list OUT SYS_REFCURSOR --国家信息
                                       ) IS
    v_param     db_log.info%TYPE := '';
    v_proc_name db_log.proc_name%TYPE := 'PCKG_DAMS_HOLIDAYSET.PROC_HOLIDAYSET_GET_NATIVE';
    v_step      db_log.step_no%TYPE;
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step,
                  pack_log.start_msg || v_param);
    o_flag := '0';
    o_msg  := '';
    v_step := '1';

    OPEN o_native_list FOR
      SELECT t.dictcode native_code, t.dictvalue native_name
        FROM dams_gen_dict t
       WHERE t.dicttype = 'COUNTRY'
         and t.sys_code = 'DAMS'
         and t.dictlang = 'zh_CN'
       ORDER BY t.dictvalue;

    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);

  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, v_step, '异常！' || v_param);
      o_flag := '-1';
      o_msg  := 'PCKG_DAMS_HOLIDAYSET.PROC_HOLIDAYSET_GET_NATIVE' || '模块内部错误:' ||
                SQLERRM;
      IF o_native_list%ISOPEN THEN
        CLOSE o_native_list;
      END IF;
  END proc_holidayset_get_native;

  /******************************************************************************
      --存储过程名：    PROC_HOLIDAYSET_BATCH_ADD
      --存储过程描述：  批次新增固定节假日
      --功能模块：      节假日管理
      --作者：          kfzx-wangjian
      --时间：          2012-10-18
      --修改历史:
  ******************************************************************************/
  PROCEDURE proc_holidayset_batch_add(i_user_id      IN VARCHAR2, --人员id
                                      i_role_id      IN VARCHAR2, --操作人员角色
                                      i_branch_id    IN VARCHAR2, --机构id
                                      i_native_code  IN VARCHAR2, --国家代码
                                      i_holiday_time IN VARCHAR2, --节假日期（年）
                                      i_year_dev     IN VARCHAR2, --是否闰年(1:闰年)
                                      i_holiday_name IN VARCHAR2, --节假日名称
                                      i_remark       IN VARCHAR2, --备注
                                      i_weekend_date IN VARCHAR2, --周末设定
                                      o_flag         OUT VARCHAR2, --0  成功  1 取信息错
                                      o_msg          OUT VARCHAR2 --错误信息
                                      ) IS
    v_param     db_log.info%TYPE := i_native_code || '|' || i_holiday_time || '|' ||
                                    i_holiday_name || '|' || i_remark || '|' ||
                                    i_year_dev || '|' || i_weekend_date;
    v_proc_name db_log.proc_name%TYPE := 'PCKG_DAMS_HOLIDAYSET.PROC_HOLIDAYSET_BATCH_ADD';
    v_step      db_log.step_no%TYPE;

    v_cur_date  VARCHAR2(8);
    v_first_day VARCHAR2(8);
    v_last_day  VARCHAR2(8);
    --v_native_mode VARCHAR2(20);
    --v_temp_date   DATE;
    --v_week_day    VARCHAR2(1);
    --v_total_day   NUMBER;
    v_count NUMBER;
    --v_start_time VARCHAR2(14);
    v_end_time    VARCHAR2(14);
    v_country_str VARCHAR2(30);
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step,
                  pack_log.start_msg || v_param);
    o_flag := '0';
    o_msg  := '';
    v_step := '1';
    --SELECT to_char(SYSDATE, 'yyyyMMddHH24miss') INTO v_start_time FROM dual;

    --检查当年是否已经设置过双休日
    SELECT COUNT(1)
      INTO v_count
      FROM dams_holiday_set
     WHERE native_code = i_native_code
       AND holiday_type = '1'
       AND holiday_time LIKE i_holiday_time || '%';
    IF v_count <> 0 THEN
      o_flag := '-2';
      o_msg  := '当年已经设置过双休日,不能重复设置。';
      RETURN;
    END IF;

    v_first_day := i_holiday_time || '0101';
    v_last_day  := i_holiday_time || '1231';
    SELECT to_char(SYSDATE, 'yyyyMMdd') INTO v_cur_date FROM dual;

    MERGE INTO dams_holiday_set hs
    USING (SELECT everyday
             FROM (SELECT to_date(v_first_day, 'yyyymmdd') + LEVEL - 1 AS everyday
                     FROM dual t
                   CONNECT BY LEVEL <= (SELECT to_date(v_last_day, 'yyyymmdd') -
                                               to_date(v_first_day, 'yyyymmdd') + 1 AS dayofmon
                                          FROM dual)) t
            WHERE to_char(t.everyday, 'D') IN
                  (SELECT regexp_substr(i_weekend_date, '[^,]+', 1, rownum)
                     FROM dual
                   CONNECT BY rownum <= length(i_weekend_date) -
                              length(REPLACE(i_weekend_date, ',')) + 1)) tmp
    ON (tmp.everyday = to_date(hs.holiday_time, 'yyyymmdd') AND hs.native_code = i_native_code)
    WHEN NOT MATCHED THEN
      INSERT
        (key_id,
         native_code,
         holiday_time,
         holiday_name,
         remark,
         cret_time,
         holiday_type)
      VALUES
        (dams_holidayset_seq.nextval,
         i_native_code,
         to_char(tmp.everyday, 'yyyyMMdd'),
         i_holiday_name,
         i_remark,
         v_cur_date,
         '1');

    /*    SELECT t .native_mode
     INTO v_native_mode
     FROM dams_gen_dict t
    WHERE t.dicttype = 'COUNTRY'
      AND t.dictcode = i_native_code;*/
    /*
      v_temp_day := i_holiday_time || '0101';
      SELECT to_date(v_temp_day, 'YYYYMMDD') INTO v_temp_date FROM dual;
      IF i_year_dev = '1' THEN
        v_total_day := 366;
      ELSE
        v_total_day := 365;
      END IF;
      FOR i IN 1 .. v_total_day
      LOOP
        --dbms_output.put_line(to_char(v_temp_date , 'yyyyMMdd'));
        SELECT to_char(v_temp_date, 'D') INTO v_week_day FROM dual;
        --设置周六周日为固定节假日
        IF v_week_day = '7' OR v_week_day = '1' THEN
          --新增节假日
          INSERT INTO dams_holiday_set
            (key_id, native_code, holiday_time, holiday_name, remark, cret_time)
          VALUES
            (dams_holidayset_seq.nextval,
             i_native_code,
             to_char(v_temp_date, 'yyyyMMdd'),
             i_holiday_name,
             i_remark,
             v_cur_date);
        END IF;
        v_temp_date := v_temp_date + 1;
      END LOOP;
    */
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);

    v_country_str := pckg_dams_util.func_decode('COUNTRY', i_native_code, 'DAMS');
    SELECT to_char(SYSDATE, 'yyyyMMddHH24miss') INTO v_end_time FROM dual;
    pckg_dams_util.proc_dams_logger('HOLIDAY_OPERATE', '014', v_end_time,
                                    i_user_id,
                                    '节假日管理: 设定' || i_holiday_time || '年' ||
                                     i_holiday_name || ' (' || v_country_str || ')');
    /*PCKG_DAMS_LOG.proc_log_writer(i_user_id, --操作人员编码
    i_role_id, --操作人员角色
    to_char(sysdate, 'yyyyMMdd'), --交易日期（yyyyMMdd）
    i_branch_id,--机构编码
    '02',  --操作类型
    'b005',     --操作对象编码
    '节假日管理',   --操作对象名称
    '',    --过程号
    'PCKG_DAMS_HOLIDAYSET.PROC_HOLIDAYSET_BATCH_ADD',  --过程名称
    v_start_time,   --交易开始时间
    v_end_time);  --交易结束时间*/

  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, v_step, '异常！' || v_param);
      o_flag := '-1';
      o_msg  := 'PCKG_DAMS_HOLIDAYSET.PROC_HOLIDAYSET_BATCH_ADD' || '模块内部错误:' ||
                SQLERRM;
      ROLLBACK;
  END proc_holidayset_batch_add;

  /******************************************************************************
      --存储过程名：    proc_holidayset_init
      --存储过程描述：  是否总行或境外机构系统管理员 （并返回国家代码）
      --功能模块：      节假日管理
      --作者：          kfzx-wangjian
      --时间：          2012-12-26
      --修改历史:       20131122 kfzx-linlc 总行系统管理员 或 境外机构管理员
  ******************************************************************************/
  PROCEDURE proc_holidayset_init(i_user_id   IN VARCHAR2, --人员id
                                 i_stru_id   IN VARCHAR2, --机构id
                                 o_flag      OUT VARCHAR2, --0  成功  -1 取信息错
                                 o_auth_type OUT VARCHAR2, --1 有总行管理权限 0 没有管理权限 2该机构可特殊配置（一二级行）
                                 o_country   OUT VARCHAR2, --国家代码
                                 o_branch_id OUT VARCHAR2 --分支机构id
                                 ) IS
    v_param     db_log.info%TYPE := i_user_id || '|' || i_stru_id;
    v_proc_name db_log.proc_name%TYPE := 'PCKG_DAMS_HOLIDAYSET.PROC_HOLIDAYSET_ISADMIN';
    v_step      db_log.step_no%TYPE;
    v_count     NUMBER;
    v_branch_id VARCHAR2(20);
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step,
                  pack_log.start_msg || v_param);
    o_flag := '0';
    v_step := '1';

    IF i_stru_id = '0010100500' THEN
      --总行管理员
      o_auth_type := '1';
      o_country   := '156';
    ELSE
      --非总行本部
      o_auth_type := '0';

      --境外
      SELECT nvl(MAX(t.country), '156')
        INTO o_country
        FROM dams_branch t
       WHERE t.stru_id = i_stru_id
         AND t.stru_sign = '8';
      IF o_country = '156' THEN
        --非境外
        SELECT up_bnch_id, decode(bnch_grade, '2', '2', '3', '2', '0') --一二级行
          INTO o_branch_id, o_auth_type
          FROM (SELECT t.up_bnch_id, t.bnch_grade
                  FROM dams_branch_relation t
                 WHERE t.bnch_id = i_stru_id
                   AND t.up_bnch_man_grade IN ('0', '1', '2')
                 ORDER BY t.up_level)
         WHERE rownum = 1;
      END IF;
    END IF;

    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);

  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, v_step, '异常！' || v_param);
      o_flag := '-1';
  END proc_holidayset_init;

  /******************************************************************************
      --存储过程名：    proc_enable_branch_setting
      --存储过程描述：  开启机构节假日特殊配置
      --功能模块：      节假日管理
      --作者：          kfzx-wangjian
      --时间：          2012-12-26
      --修改历史:
  ******************************************************************************/
  PROCEDURE proc_enable_branch_setting(i_user_id      IN VARCHAR2, --人员id
                                       i_branch_id    IN VARCHAR2, --机构id
                                       i_holiday_time IN VARCHAR2, --年月
                                       i_country      IN VARCHAR2, --国家
                                       o_flag         OUT VARCHAR2, --0  成功  -1 取信息错
                                       o_msg          OUT VARCHAR2) IS
    v_param     db_log.info%TYPE := i_user_id || '|' || i_branch_id || '|' ||
                                    i_holiday_time || '|' || i_country;
    v_proc_name db_log.proc_name%TYPE := 'PCKG_DAMS_HOLIDAYSET.proc_enable_branch_setting';
    v_det_str   VARCHAR2(300);
    v_end_time  VARCHAR2(14);
  BEGIN
    o_flag := '0';
    DELETE FROM dams_holiday_set t
     WHERE t.native_code IS NULL
       AND t.holiday_time LIKE i_holiday_time || '%'
       AND t.stru_id = i_branch_id;

    INSERT INTO dams_holiday_set
      (key_id,
       native_code,
       holiday_time,
       holiday_name,
       remark,
       cret_time,
       holiday_type,
       stru_id)
      WITH tmp AS
       (SELECT tt.stru_id
          FROM dams_holiday_set tt, dams_branch b
         WHERE (EXISTS
                (SELECT 1
                   FROM dams_branch_relation r
                  WHERE r.bnch_id = i_branch_id
                    AND tt.stru_id = r.up_bnch_id) OR tt.native_code = i_country)
           AND tt.holiday_time LIKE i_holiday_time || '%'
           AND b.stru_id = tt.stru_id
         ORDER BY b.man_grade DESC NULLS LAST)
      SELECT dams_holidayset_seq.nextval,
             '',
             t.holiday_time,
             t.holiday_name,
             t.remark,
             t.cret_time,
             t.holiday_type,
             i_branch_id
        FROM dams_holiday_set t, dams_branch b
       WHERE (EXISTS
              (SELECT 1
                 FROM dams_branch_relation r
                WHERE r.bnch_id = i_branch_id
                  AND t.stru_id = r.up_bnch_id) OR t.native_code = i_country)
         AND t.holiday_time LIKE i_holiday_time || '%'
         AND b.stru_id(+) = t.stru_id
         AND (EXISTS (SELECT 1
                        FROM (SELECT tmp.stru_id FROM tmp WHERE rownum = 1) t2
                       WHERE t2.stru_id = t.stru_id) OR NOT EXISTS
              (SELECT 1 FROM tmp WHERE rownum = 1) AND t.stru_id IS NULL);

    v_det_str := pckg_dams_util.func_get_stru_name(i_branch_id);
    SELECT to_char(SYSDATE, 'yyyyMMddHH24miss') INTO v_end_time FROM dual;
    pckg_dams_util.proc_dams_logger('HOLIDAY_OPERATE', '014', v_end_time,
                                    i_user_id,
                                    '节假日管理:' || i_holiday_time || ' 开启特殊设定(' ||
                                     v_det_str || ')');
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, '', '异常！' || v_param);
      o_flag := '-1';
  END proc_enable_branch_setting;

  /******************************************************************************
      --存储过程名：    proc_enable_branch_setting
      --存储过程描述：  开启机构节假日特殊配置
      --功能模块：      节假日管理
      --作者：          kfzx-wangjian
      --时间：          2012-12-26
      --修改历史:
  ******************************************************************************/
  PROCEDURE proc_disable_branch_setting(i_user_id      IN VARCHAR2, --人员id
                                        i_branch_id    IN VARCHAR2, --机构id
                                        i_holiday_time IN VARCHAR2, --年月
                                        i_country      IN VARCHAR2, --国家
                                        o_flag         OUT VARCHAR2, --0  成功  -1 取信息错
                                        o_msg          OUT VARCHAR2) IS
    v_param     db_log.info%TYPE := i_user_id || '|' || i_branch_id || '|' ||
                                    i_holiday_time || '|' || i_country;
    v_proc_name db_log.proc_name%TYPE := 'PCKG_DAMS_HOLIDAYSET.proc_disable_branch_setting';
    v_det_str   VARCHAR2(300);
    v_end_time  VARCHAR2(14);
  BEGIN
    o_flag := '0';
    DELETE FROM dams_holiday_set t
     WHERE t.native_code IS NULL
       AND t.holiday_time LIKE i_holiday_time || '%'
       AND t.stru_id = i_branch_id;

    v_det_str := pckg_dams_util.func_get_stru_name(i_branch_id);
    SELECT to_char(SYSDATE, 'yyyyMMddHH24miss') INTO v_end_time FROM dual;
    pckg_dams_util.proc_dams_logger('HOLIDAY_OPERATE', '014', v_end_time,
                                    i_user_id,
                                    '节假日管理:' || i_holiday_time || ' 关闭特殊设定(' ||
                                     v_det_str || ')');
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, '', '异常！' || v_param);
      o_flag := '-1';
  END proc_disable_branch_setting;

  /******************************************************************************
      --存储过程名：    PROC_HOLIDAYSET_BATCH_ADD
      --存储过程描述：  根据国家代码,开始日期,结束日期查找两个时间段之间的工作日
      --功能模块：      节假日管理
      --作者：          kfzx-chenl04
      --时间：          2012-10-24
      --修改历史:
  ******************************************************************************/
  FUNCTION fun_differtime(i_nativecode IN VARCHAR2, --国家代码
                          i_starttime  IN VARCHAR2, --开始日期(格式:20121012)
                          i_endtime    IN VARCHAR2 --结束日期(格式:20121012)
                          ) RETURN NUMBER IS
    diff NUMBER;
  BEGIN
    SELECT (to_date(i_endtime, 'yyyyMMdd') - to_date(i_starttime, 'yyyyMMdd') -
           (SELECT COUNT(*)
               FROM dams_holiday_set se
              WHERE se.holiday_time BETWEEN i_starttime AND i_endtime
                AND se.native_code = i_nativecode))
      INTO diff
      FROM dual;
    RETURN diff;
  EXCEPTION
    WHEN OTHERS THEN
      RETURN - 1;
  END;

  FUNCTION fun_branch_bank(i_branch_id IN VARCHAR2 --所属部门ID
                           ) RETURN VARCHAR2 IS
    v_bank_id dams_branch.stru_id%TYPE;
  BEGIN

    SELECT nvl(MAX(up_bnch_id), i_branch_id) id --防止返回结果为空
      INTO v_bank_id
      FROM (SELECT br.up_bnch_id
              FROM dams_branch_relation br
             WHERE br.up_bnch_grade IN ('1', '2', '3')
               AND br.bnch_id = i_branch_id
             ORDER BY br.up_bnch_grade DESC)
     WHERE rownum = 1;
    RETURN v_bank_id;
  EXCEPTION
    WHEN OTHERS THEN
      RETURN '-1';
  END fun_branch_bank;

  /******************************************************************************
      --存储过程名：    func_holiday_check
      --存储过程描述：  检查当前时间是否合适时间（发邮件） 0非工作时间,１工作时间
      --功能模块：      节假日管理
      --作者：          kfzx-guzh02
      --时间：          2017-12-01
      --修改历史:
  ******************************************************************************/
  FUNCTION func_holiday_check(in_stru_id     IN VARCHAR2,
                              in_country     IN VARCHAR2,
                              in_sys_date    IN VARCHAR2) RETURN NUMBER IS
    v_count         VARCHAR2(5);
    v_time          VARCHAR2(2);
    v_native_code   dams_branch.country%TYPE := NULL;
    v_country       dams_holiday_set.native_code%TYPE;
    v_country_valid VARCHAR2(1) := '1'; --0无效 1有效
    v_date          VARCHAR2(8);

  BEGIN
    --国家代码如果为空 或者有误,则以入参机构的国家代码为主
    BEGIN
      v_country := in_country;
    EXCEPTION
      WHEN OTHERS THEN
        v_country_valid := '0';
    END;
    IF v_country IS NULL OR v_country_valid = '0' THEN
      SELECT MAX(t.country)
        INTO v_country
        FROM dams_branch t
       WHERE t.stru_id = in_stru_id;
    END IF;

    if in_sys_date is null then
      v_date := to_char(sysdate,'yyyymmdd');
      v_time := substr(to_char(SYSDATE, 'yyyymmddhh24miss'), 9, 2);
    else
      v_date := in_sys_date;
      v_time := substr(to_char(in_sys_date, 'yyyymmddhh24miss'), 9, 2);
    end if;

    SELECT COUNT(1)
      INTO v_count
      FROM (SELECT hs.holiday_time
              FROM dams_holiday_set hs, dams_branch_relation br
             WHERE br.bnch_id = in_stru_id
               AND hs.stru_id = br.up_bnch_id
               AND hs.holiday_time = v_date
            UNION ALL
            SELECT hs.holiday_time
              FROM dams_holiday_set hs
             WHERE hs.native_code = v_country
               AND hs.stru_id IS NULL
               AND hs.holiday_time = v_date);

    if v_count = 0 then
      --目前只考虑总行,不区分时区
      if v_time between 8 and 18 then
        return 1;
      end if;
    end if;

    RETURN 0;
  EXCEPTION
    WHEN OTHERS THEN
      RETURN 0;
  END func_holiday_check;

END pckg_dams_holidayset;
//


CREATE OR REPLACE PACKAGE PCKG_DAMS_AUTHORITY_APPLY IS


  TYPE t_process_task_list IS RECORD(
    task_name  dams_wf_activity_conf.activity_name%TYPE, --活动名称
    task_order dams_standard_process_model.order_code%TYPE --活动序号
    );

  TYPE t_template_list IS RECORD(
    key_id        dams_safety_control_template.key_id%TYPE,
    template_name dams_safety_control_template.template_name%TYPE,
    syscodes      VARCHAR2(400),
    syscodeids    VARCHAR2(400));

  TYPE ref_t_process_task_list IS REF CURSOR RETURN t_process_task_list; --定义流程活动列表游标

  TYPE ref_t_template_list IS REF CURSOR RETURN t_template_list;

  PROCEDURE proc_get_secure_degree(o_flag               OUT VARCHAR2, --存储过程返回标志
                                   o_secure_degree_list OUT SYS_REFCURSOR --密级列表
                                   );

  PROCEDURE proc_get_role(i_sys_type  IN VARCHAR2, --系统类型
                          o_flag      OUT VARCHAR2, --存储过程返回标志
                          o_role_list OUT SYS_REFCURSOR --角色列表
                          );

  PROCEDURE proc_get_task_describe(i_degree_id    IN VARCHAR2, --密级id
                                   i_apply_type   IN VARCHAR2, --权限类型
                                   i_duty_lv_code IN VARCHAR2, --角色id
                                   i_branch_lv    IN VARCHAR2, --机构层级
                                   o_flag         OUT VARCHAR2, --存储过程返回标志
                                   o_task_list    OUT ref_t_process_task_list --活动列表
                                   );

  PROCEDURE proc_get_final_task(i_sys_type           IN VARCHAR2, --系统类型
                                o_flag               OUT VARCHAR2, --存储过程返回标志
                                o_task_list          OUT ref_t_process_task_list, --最高活动列表
                                o_secure_degree_list OUT SYS_REFCURSOR, --密级列表
                                o_role_list          OUT SYS_REFCURSOR --角色
                                );

  PROCEDURE proc_get_basic_auth(i_sys_type           IN VARCHAR2, --系统类型
                                o_flag               OUT VARCHAR2, --存储过程返回标志
                                o_task_list          OUT ref_t_process_task_list, --活动列表
                                o_role_list          OUT SYS_REFCURSOR, --角色
                                o_secure_degree_list OUT SYS_REFCURSOR --密级列表
                                );

  PROCEDURE proc_update_authority_apply(i_auth_data     IN VARCHAR2, --权限申请字符串
                                        i_sys_type      IN VARCHAR2, --系统类型
                                        i_modify_person IN VARCHAR2, --修改人
                                        o_flag          OUT VARCHAR2 --存储过程返回标志
                                        );

  PROCEDURE proc_get_system_type(o_flag             OUT VARCHAR2, --存储过程返回标志
                                 o_totalNum         OUT VARCHAR2,
                                 o_system_type_list OUT SYS_REFCURSOR --系统类型列表
                                 );

  PROCEDURE proc_get_template_list(o_flag          OUT VARCHAR2, --存储过程返回标志
                                   o_totalNum      OUT VARCHAR2,
                                   o_template_list OUT ref_t_template_list --模板列表
                                   );

  PROCEDURE proc_new_sc_template(i_template_name IN VARCHAR2,
                                 i_syscodes      IN VARCHAR2,
                                 i_user_id       IN VARCHAR2,
                                 o_flag          OUT VARCHAR2, --存储过程返回标志
                                 o_template_id   OUT VARCHAR2);
  PROCEDURE proc_update_sc_template(i_key_id        IN VARCHAR2,
                                    i_template_name IN VARCHAR2,
                                    i_syscodes      IN VARCHAR2,
                                    i_user_id       IN VARCHAR2,
                                    o_flag          OUT VARCHAR2 --存储过程返回标志
                                    );
  PROCEDURE proc_delete_sc_template(i_key_id  IN VARCHAR2,
                                    i_user_id IN VARCHAR2,
                                    o_flag    OUT VARCHAR2 --存储过程返回标志
                                    );

  PROCEDURE proc_get_edit_auth(i_ssic_id IN VARCHAR2, --
                               o_flag    OUT VARCHAR2, --
                               o_auth    OUT VARCHAR2 --是否有编辑权限  1有,0无
                              );

END PCKG_DAMS_AUTHORITY_APPLY;
//
CREATE OR REPLACE PACKAGE BODY PCKG_DAMS_AUTHORITY_APPLY IS



  PROCEDURE proc_get_system_type(o_flag             OUT VARCHAR2, --存储过程返回标志
                                 o_totalNum         OUT VARCHAR2,
                                 o_system_type_list OUT SYS_REFCURSOR --系统类型列表
                                 ) IS
  BEGIN
    o_flag := '0';

    SELECT COUNT(1) INTO o_totalNum FROM Dams_Sys_Conf dsc;

    OPEN o_system_type_list FOR
      SELECT dsc.sys_code, dsc.sys_sname FROM Dams_Sys_Conf dsc;

  EXCEPTION
    WHEN OTHERS THEN
      IF o_system_type_list%ISOPEN THEN
        CLOSE o_system_type_list;
      END IF;
      pack_log.error('PCKG_DAMS_AUTHORITY_APPLY.proc_get_system_type',
                     pack_log.end_step,
                     '异常！');
      o_flag := '-1';
      RETURN;
  END proc_get_system_type;



  PROCEDURE proc_get_template_list(o_flag          OUT VARCHAR2, --存储过程返回标志
                                   o_totalNum      OUT VARCHAR2,
                                   o_template_list OUT ref_t_template_list --模板列表
                                   ) IS
  BEGIN
    o_flag := '0';

    SELECT COUNT(1) INTO o_totalNum FROM dams_safety_control_template sct;
 
  EXCEPTION
    WHEN OTHERS THEN
      IF o_template_list%ISOPEN THEN
        CLOSE o_template_list;
      END IF;
      pack_log.error('PCKG_DAMS_AUTHORITY_APPLY.proc_get_template_list',
                     pack_log.end_step,
                     '异常！');
      o_flag := '-1';
      RETURN;
  END proc_get_template_list;



  PROCEDURE proc_new_sc_template(i_template_name IN VARCHAR2,
                                 i_syscodes      IN VARCHAR2,
                                 i_user_id       IN VARCHAR2,
                                 o_flag          OUT VARCHAR2, --存储过程返回标志
                                 o_template_id   OUT VARCHAR2) IS
    v_template_id VARCHAR2(20);
    v_sys_items   pckg_dams_role.arrytype;
    v_logObj      dams_gen_dict.dictcode%TYPE := 'AUTHORITY_OPERATE';
    v_logType     VARCHAR2(3);
    v_logDate     VARCHAR2(14);
    v_logHandler  dams_user.ssic_id%TYPE := i_user_id;

    v_logInfo VARCHAR2(4000);
    v_temp    VARCHAR2(4000);
  BEGIN
    o_flag := '0';

    SELECT 'PL' || to_char(SYSDATE, 'yyyymmdd') ||
           lpad(DAMS_SAFETY_TEMPLATE_SEQ.NEXTVAL, 10, '0')
    INTO   v_template_id
    FROM   dual;

    o_template_id := v_template_id;

    INSERT INTO dams_safety_control_template
      (key_id,
       template_name,
       cret_date,
       cret_person,
       modify_time,
       modify_person)
    VALUES
      (v_template_id,
       i_template_name,
       to_char(SYSDATE, 'yyyymmddhh24miss'),
       i_user_id,
       '',
       '');

    INSERT INTO dams_authorityapply_set
      (key_id,
       system_type,
       duty_lv_code,
       prm_secret_id,
       apply_type,
       cret_date,
       upd_time,
       cret_person,
       upd_person,
       colume1,
       standard_id,
       branch_lv)
      SELECT 'PL' || to_char(SYSDATE, 'yyyymmdd') ||
             lpad(DAMS_AUTHORITYAPPLY_SET_SEQ.NEXTVAL, 10, '0'),
             v_template_id,
             das.duty_lv_code,
             das.prm_secret_id,
             das.apply_type,
             to_char(SYSDATE, 'yyyymmddhh24miss'),
             '99991231',
             i_user_id,
             '',
             '',
             das.standard_id,
             das.branch_lv
      FROM   dams_authorityapply_set das
      WHERE  das.system_type = '02'
             AND das.upd_time = '99991231';

    IF i_syscodes IS NOT NULL THEN
      v_sys_items := pckg_dams_role.func_common_getarray(i_syscodes, ',');

      FOR i IN 1 .. v_sys_items.COUNT
      LOOP

        DELETE FROM dams_template_sys_rel WHERE syscode = v_sys_items(i);

        INSERT INTO dams_template_sys_rel
          (key_id, template_id, syscode)
        VALUES
          ('PL' || to_char(SYSDATE, 'yyyymmdd') ||
           lpad(DAMS_TEMPLATE_SYS_REL_SEQ.NEXTVAL, 10, '0'),
           v_template_id,
           v_sys_items(i));

      END LOOP;

    END IF;

    v_logType := '005';

    SELECT to_char(SYSDATE, 'yyyymmddhh24miss') INTO v_logDate FROM dual;

    v_logInfo := '';
    SELECT t.NAME INTO v_temp FROM dams_user t WHERE t.ssic_id = i_user_id;
    v_logInfo := ' 用户:' || v_logInfo || v_temp;
    v_logInfo := v_logInfo || ' 新增安控策略模板:' || i_template_name || ' ';

    PCKG_DAMS_UTIL.proc_dams_logger(v_logObj,
                                    v_logType,
                                    v_logDate,
                                    v_logHandler,
                                    v_logInfo);

    COMMIT;

  EXCEPTION
    WHEN OTHERS THEN

      ROLLBACK;

      pack_log.error('PCKG_DAMS_AUTHORITY_APPLY.proc_new_sc_template',
                     pack_log.end_step,
                     '异常！');
      o_flag := '-1';
      RETURN;
  END proc_new_sc_template;



  PROCEDURE proc_update_sc_template(i_key_id        IN VARCHAR2,
                                    i_template_name IN VARCHAR2,
                                    i_syscodes      IN VARCHAR2,
                                    i_user_id       IN VARCHAR2,
                                    o_flag          OUT VARCHAR2 --存储过程返回标志
                                    ) IS
    v_sys_items  pckg_dams_role.arrytype;
    v_logObj     dams_gen_dict.dictcode%TYPE := 'AUTHORITY_OPERATE';
    v_logType    VARCHAR2(3);
    v_logDate    VARCHAR2(14);
    v_logHandler dams_user.ssic_id%TYPE := i_user_id;

    v_logInfo       VARCHAR2(4000);
    v_temp          VARCHAR2(4000);
    v_template_name VARCHAR2(4000);
  BEGIN
    o_flag := '0';

    SELECT MAX(template_name)
    INTO   v_template_name
    FROM   dams_safety_control_template
    WHERE  key_id = i_key_id;

    UPDATE dams_safety_control_template
    SET    template_name = i_template_name,
           modify_time   = to_char(SYSDATE, 'yyyymmddhh24miss'),
           modify_person = i_user_id
    WHERE  key_id = i_key_id;

    DELETE FROM dams_template_sys_rel tsr WHERE tsr.template_id = i_key_id;

    IF i_syscodes IS NOT NULL THEN
      v_sys_items := pckg_dams_role.func_common_getarray(i_syscodes, ',');

      FOR i IN 1 .. v_sys_items.COUNT
      LOOP

        DELETE FROM dams_template_sys_rel WHERE syscode = v_sys_items(i);

        INSERT INTO dams_template_sys_rel
          (key_id, template_id, syscode)
        VALUES
          ('PL' || to_char(SYSDATE, 'yyyymmdd') ||
           lpad(DAMS_TEMPLATE_SYS_REL_SEQ.NEXTVAL, 10, '0'),
           i_key_id,
           v_sys_items(i));

      END LOOP;

    END IF;

    v_logType := '005';

    SELECT to_char(SYSDATE, 'yyyymmddhh24miss') INTO v_logDate FROM dual;

    v_logInfo := '';
    SELECT t.NAME INTO v_temp FROM dams_user t WHERE t.ssic_id = i_user_id;
    v_logInfo := ' 用户:' || v_logInfo || v_temp;
    v_logInfo := v_logInfo || ' 修改安控策略模板:' || v_template_name || ' ';

    PCKG_DAMS_UTIL.proc_dams_logger(v_logObj,
                                    v_logType,
                                    v_logDate,
                                    v_logHandler,
                                    v_logInfo);

    COMMIT;

  EXCEPTION
    WHEN OTHERS THEN

      ROLLBACK;

      pack_log.error('PCKG_DAMS_AUTHORITY_APPLY.proc_update_sc_template',
                     pack_log.end_step,
                     '异常！');
      o_flag := '-1';
      RETURN;
  END proc_update_sc_template;



  PROCEDURE proc_delete_sc_template(i_key_id  IN VARCHAR2,
                                    i_user_id IN VARCHAR2,
                                    o_flag    OUT VARCHAR2 --存储过程返回标志
                                    ) IS
    v_sys_items  pckg_dams_role.arrytype;
    v_logObj     dams_gen_dict.dictcode%TYPE := 'AUTHORITY_OPERATE';
    v_logType    VARCHAR2(3);
    v_logDate    VARCHAR2(14);
    v_logHandler dams_user.ssic_id%TYPE := i_user_id;

    v_logInfo       VARCHAR2(4000);
    v_temp          VARCHAR2(4000);
    v_template_name VARCHAR2(4000);
  BEGIN
    o_flag := '0';
    SELECT MAX(template_name)
    INTO   v_template_name
    FROM   dams_safety_control_template
    WHERE  key_id = i_key_id;

    DELETE FROM dams_safety_control_template WHERE key_id = i_key_id;

    DELETE FROM dams_template_sys_rel tsr WHERE tsr.template_id = i_key_id;

    v_logType := '005';

    SELECT to_char(SYSDATE, 'yyyymmddhh24miss') INTO v_logDate FROM dual;

    v_logInfo := '';
    SELECT t.NAME INTO v_temp FROM dams_user t WHERE t.ssic_id = i_user_id;
    v_logInfo := ' 用户:' || v_logInfo || v_temp;
    v_logInfo := v_logInfo || ' 删除安控策略模板:' || v_template_name || ' ';

    PCKG_DAMS_UTIL.proc_dams_logger(v_logObj,
                                    v_logType,
                                    v_logDate,
                                    v_logHandler,
                                    v_logInfo);

    COMMIT;

  EXCEPTION
    WHEN OTHERS THEN

      ROLLBACK;

      pack_log.error('PCKG_DAMS_AUTHORITY_APPLY.proc_delete_sc_template',
                     pack_log.end_step,
                     '异常！');
      o_flag := '-1';
      RETURN;
  END proc_delete_sc_template;



  PROCEDURE proc_get_secure_degree(o_flag               OUT VARCHAR2, --存储过程返回标志
                                   o_secure_degree_list OUT SYS_REFCURSOR --密级列表
                                   ) IS

  BEGIN
    o_flag := '0';

    OPEN o_secure_degree_list FOR
      SELECT sd.secure_id, sd.secure_degree
      FROM   DAMS_SECURE_DEGREE_BASE sd
      ORDER  BY sd.secure_id;

  EXCEPTION
    WHEN OTHERS THEN
      IF o_secure_degree_list%ISOPEN THEN
        CLOSE o_secure_degree_list;
      END IF;
      pack_log.error('PCKG_DAMS_AUTHORITY_APPLY.proc_get_secure_degree',
                     pack_log.end_step,
                     '异常！');
      o_flag := '-1';
      RETURN;
  END proc_get_secure_degree;



  PROCEDURE proc_get_task_describe(i_degree_id    IN VARCHAR2, --密级id
                                   i_apply_type   IN VARCHAR2, --权限类型
                                   i_duty_lv_code IN VARCHAR2, --角色id
                                   i_branch_lv    IN VARCHAR2, --机构层级
                                   o_flag         OUT VARCHAR2, --存储过程返回标志
                                   o_task_list    OUT ref_t_process_task_list --活动列表
                                   ) IS
  BEGIN
    o_flag := '0';
    OPEN o_task_list FOR

      SELECT wac.activity_name, sps.order_code
      FROM   dams_standard_process_model sps, DAMS_WF_ACTIVITY_CONF wac
      WHERE  sps.key_id IN
             (SELECT das.standard_id
              FROM   dams_authorityapply_set das
              WHERE  das.duty_lv_code = i_duty_lv_code
                     AND das.system_type = '01'
                     AND das.prm_secret_id = i_degree_id
                     AND das.apply_type = i_apply_type
                     AND das.branch_lv = i_branch_lv)
             AND sps.biz_type = '009'
             AND wac.id = sps.activity_id
      ORDER  BY sps.order_code ASC;

  EXCEPTION
    WHEN OTHERS THEN
      IF o_task_list%ISOPEN THEN
        CLOSE o_task_list;
      END IF;
      pack_log.error('PCKG_DAMS_AUTHORITY_APPLY.proc_get_task_describe',
                     pack_log.end_step,
                     '异常！');
      o_flag := '-1';
      RETURN;
  END proc_get_task_describe;



  PROCEDURE proc_get_final_task(i_sys_type           IN VARCHAR2, --系统类型
                                o_flag               OUT VARCHAR2, --存储过程返回标志
                                o_task_list          OUT ref_t_process_task_list, --活动序号
                                o_secure_degree_list OUT SYS_REFCURSOR, --密级列表
                                o_role_list          OUT SYS_REFCURSOR --角色列表
                                ) IS
  BEGIN
    o_flag := '0';

    OPEN o_secure_degree_list FOR
      SELECT sd.secure_id, sd.secure_degree
      FROM   DAMS_SECURE_DEGREE_BASE sd
      ORDER  BY sd.secure_id;

    IF i_sys_type = '01'
       OR i_sys_type IS NULL THEN

      OPEN o_task_list FOR
        SELECT wac.activity_name, sps.order_code
        FROM   dams_authorityapply_set     das,
               dams_standard_process_model sps,
               DAMS_WF_ACTIVITY_CONF       wac
        WHERE  sps.key_id = das.standard_id
               AND das.system_type = '01'
               AND wac.id = sps.activity_id
               AND das.upd_time = '99991231'
        ORDER  BY das.branch_lv,
                  das.duty_lv_code DESC,
                  das.prm_secret_id,
                  das.apply_type;

      OPEN o_role_list FOR
        SELECT DISTINCT rn.role_id, rn.role_name, das.branch_lv
        FROM   dams_authorityapply_set das, dams_role_nls rn
        WHERE  das.duty_lv_code = rn.role_id
               AND das.system_type = '01'
               AND das.upd_time = '99991231'
        ORDER  BY das.branch_lv, rn.role_id DESC;
    ELSE
      OPEN o_task_list FOR
        SELECT wac1.activity_name || ';' || wac2.activity_name,
               t.min_or || ';' || t.max_or
        FROM   dams_authorityapply_set das1,
               dams_standard_process_model sps1,
               DAMS_WF_ACTIVITY_CONF wac1,
               dams_authorityapply_set das2,
               dams_standard_process_model sps2,
               DAMS_WF_ACTIVITY_CONF wac2,
               (SELECT MIN(sps.order_code) min_or,
                       MAX(sps.order_code) max_or,
                       das.branch_lv,
                       das.duty_lv_code,
                       das.prm_secret_id,
                       das.apply_type
                FROM   dams_authorityapply_set     das,
                       dams_standard_process_model sps,
                       DAMS_WF_ACTIVITY_CONF       wac
                WHERE  sps.key_id = das.standard_id
                       AND das.system_type = i_sys_type
                       AND wac.id = sps.activity_id
                       AND das.upd_time = '99991231'
                GROUP  BY das.branch_lv,
                          das.duty_lv_code,
                          das.prm_secret_id,
                          das.apply_type) t
        WHERE  sps1.key_id = das1.standard_id
               AND das1.system_type = i_sys_type
               AND wac1.id = sps1.activity_id
               AND das1.upd_time = '99991231'
               AND das1.branch_lv = t.branch_lv
               AND das1.duty_lv_code = t.duty_lv_code
               AND das1.prm_secret_id = t.prm_secret_id
               AND das1.apply_type = t.apply_type
               AND sps1.order_code = t.min_or
               AND sps2.key_id = das2.standard_id
               AND das2.system_type = i_sys_type
               AND wac2.id = sps2.activity_id
               AND das2.upd_time = '99991231'
               AND das2.branch_lv = t.branch_lv
               AND das2.duty_lv_code = t.duty_lv_code
               AND das2.prm_secret_id = t.prm_secret_id
               AND das2.apply_type = t.apply_type
               AND sps2.order_code = t.max_or
        ORDER  BY t.branch_lv,
                  t.duty_lv_code DESC,
                  t.prm_secret_id,
                  t.apply_type;

      OPEN o_role_list FOR
        SELECT DISTINCT rn.role_id, rn.role_name, das.branch_lv
        FROM   dams_authorityapply_set das, dams_role_nls rn
        WHERE  das.duty_lv_code = rn.role_id
               AND das.upd_time = '99991231'
        ORDER  BY das.branch_lv, rn.role_id DESC;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      IF o_task_list%ISOPEN THEN
        CLOSE o_task_list;
      END IF;
      IF o_secure_degree_list%ISOPEN THEN
        CLOSE o_secure_degree_list;
      END IF;
      IF o_role_list%ISOPEN THEN
        CLOSE o_role_list;
      END IF;
      pack_log.error('PCKG_DAMS_AUTHORITY_APPLY.proc_get_final_task',
                     pack_log.end_step,
                     '异常！');
      o_flag := '-1';
      RETURN;
  END proc_get_final_task;



  PROCEDURE proc_get_role(i_sys_type  IN VARCHAR2, --系统类型
                          o_flag      OUT VARCHAR2, --存储过程返回标志
                          o_role_list OUT SYS_REFCURSOR --角色
                          ) IS
  BEGIN
    o_flag := '0';
    IF i_sys_type = '01'
       OR i_sys_type IS NULL THEN

      OPEN o_role_list FOR
        SELECT DISTINCT rn.role_id, rn.role_name, das.branch_lv
        FROM   dams_authorityapply_set das, dams_role_nls rn
        WHERE  das.duty_lv_code = rn.role_id
               AND das.system_type = '01'
               AND das.upd_time = '99991231'
        ORDER  BY das.branch_lv ASC, rn.role_id DESC;
    ELSE
      OPEN o_role_list FOR
        SELECT DISTINCT rn.role_id, rn.role_name, das.branch_lv
        FROM   dams_authorityapply_set das, dams_role_nls rn
        WHERE  das.duty_lv_code = rn.role_id
               AND das.system_type = i_sys_type
               AND das.upd_time = '99991231'

        ORDER  BY das.branch_lv ASC, rn.role_id DESC;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      IF o_role_list%ISOPEN THEN
        CLOSE o_role_list;
      END IF;
      pack_log.error('PCKG_DAMS_AUTHORITY_APPLY.proc_get_role',
                     pack_log.end_step,
                     '异常！');
      o_flag := '-1';
      RETURN;
  END proc_get_role;



  PROCEDURE proc_get_basic_auth(i_sys_type           IN VARCHAR2, --系统类型
                                o_flag               OUT VARCHAR2, --存储过程返回标志
                                o_task_list          OUT ref_t_process_task_list, --活动列表
                                o_role_list          OUT SYS_REFCURSOR, --角色
                                o_secure_degree_list OUT SYS_REFCURSOR --密级列表
                                ) IS

  BEGIN

    o_flag := '0';


  EXCEPTION
    WHEN OTHERS THEN
      IF o_secure_degree_list%ISOPEN THEN
        CLOSE o_secure_degree_list;
      END IF;
      IF o_task_list%ISOPEN THEN
        CLOSE o_task_list;
      END IF;
      IF o_role_list%ISOPEN THEN
        CLOSE o_role_list;
      END IF;
      pack_log.error('PCKG_DAMS_AUTHORITY_APPLY.proc_get_basic_auth',
                     pack_log.end_step,
                     '异常！');
      o_flag := '-1';
      RETURN;
  END proc_get_basic_auth;

  PROCEDURE proc_update_authority_apply(i_auth_data     IN VARCHAR2, --权限申请字符串
                                        i_sys_type      IN VARCHAR2, --系统类型
                                        i_modify_person IN VARCHAR2, --修改人
                                        o_flag          OUT VARCHAR2 --存储过程返回标志
                                        ) IS
    v_row_items        pckg_dams_role.arrytype;
    v_auth_items       pckg_dams_role.arrytype;
    v_action_items     pckg_dams_role.arrytype;
    v_action_items_tmp pckg_dams_role.arrytype;
    v_standardid       VARCHAR2(100);
    v_standardid2      VARCHAR2(100);
    v_standardid_temp  VARCHAR2(100);
    v_standardid_temp2 VARCHAR2(100);
    v_cret_date        VARCHAR2(8);
    v_cret_person      VARCHAR2(14);
    v_key_id           VARCHAR2(20);
    v_branch_id        VARCHAR2(20);

    v_logObj     dams_gen_dict.dictcode%TYPE := 'AUTHORITY_OPERATE';
    v_logType    VARCHAR2(3);
    v_logDate    VARCHAR2(14);
    v_logHandler dams_user.ssic_id%TYPE := i_modify_person;

    v_logInfo       VARCHAR2(4000);
    v_temp          VARCHAR2(4000);
    v_template_name VARCHAR2(4000);
    v_count         VARCHAR2(4);

  BEGIN
    o_flag := '0';
    SELECT MAX(template_name)
    INTO   v_template_name
    FROM   dams_safety_control_template
    WHERE  key_id = i_sys_type;

    IF i_auth_data IS NOT NULL THEN
      v_row_items := pckg_dams_role.func_common_getarray(i_auth_data, '|');
      FOR i IN 1 .. v_row_items.COUNT
       
      LOOP
         v_auth_items   := pckg_dams_role.func_common_getarray(v_row_items(i), ',');
        v_action_items := pckg_dams_role.func_common_getarray(v_auth_items(5),';');
        v_action_items_tmp := pckg_dams_role.func_common_getarray(v_auth_items(4), ';');
        IF v_auth_items(6) = '1' THEN

          SELECT sps.key_id
          INTO   v_standardid
          FROM   dams_standard_process_model sps
          WHERE  sps.biz_type = '009'
                 AND sps.order_code = v_action_items(1)
                 AND sps.activity_id LIKE 'QXSQ-%';

        ELSIF v_auth_items(6) = '2' THEN

          SELECT sps.key_id
          INTO   v_standardid
          FROM   dams_standard_process_model sps
          WHERE  sps.biz_type = '009'
                 AND sps.order_code = v_action_items(1)
                 AND sps.activity_id LIKE 'AUTHAPPLY-%';
          SELECT sps.key_id
          INTO   v_standardid2
          FROM   dams_standard_process_model sps
          WHERE  sps.biz_type = '009'
                 AND sps.order_code = v_action_items(2)
                 AND sps.activity_id LIKE 'AUTHAPPLY-%';

          SELECT sps.key_id
          INTO   v_standardid_temp2
          FROM   dams_standard_process_model sps
          WHERE  sps.biz_type = '009'
                 AND sps.order_code = v_action_items_tmp(2)
                 AND sps.activity_id LIKE 'AUTHAPPLY-%';

        ELSIF v_auth_items(6) = '3' THEN

          SELECT sps.key_id
          INTO   v_standardid
          FROM   dams_standard_process_model sps
          WHERE  sps.biz_type = '009'
                 AND sps.order_code = v_action_items(1)
                 AND sps.activity_id LIKE 'QXSQBR-%';

        ELSE

          SELECT sps.key_id
          INTO   v_standardid
          FROM   dams_standard_process_model sps
          WHERE  sps.biz_type = '009'
                 AND sps.order_code = v_action_items(1)
                 AND sps.activity_id LIKE 'QXSQBRSUB-%';
        END IF;

        SELECT sps.key_id,
               substr(das.cret_date, 1, 8),
               das.cret_person,
               das.key_id
        INTO   v_standardid_temp, v_cret_date, v_cret_person, v_key_id
        FROM   dams_standard_process_model sps, dams_authorityapply_set das
        WHERE  sps.biz_type = '009'
               AND sps.order_code = v_action_items_tmp(1)
               AND sps.key_id = das.standard_id
               AND das.system_type = i_sys_type
               AND das.duty_lv_code = v_auth_items(2)
               AND das.prm_secret_id = v_auth_items(1)
               AND das.apply_type = v_auth_items(3)
               AND das.branch_lv = v_auth_items(6)
               AND das.upd_time = '99991231';
        IF v_action_items(1) = v_action_items(2) THEN
          UPDATE dams_authorityapply_set
          SET    upd_time = TO_CHAR(SYSDATE - 1, 'yyyymmdd')
          WHERE  system_type = i_sys_type
                 AND duty_lv_code = v_auth_items(2)
                 AND prm_secret_id = v_auth_items(1)
                 AND apply_type = v_auth_items(3)
                 AND branch_lv = v_auth_items(6)
                 AND upd_time = '99991231'
                 AND standard_id <> v_standardid_temp;
        END IF;
        IF v_cret_date = TO_CHAR(SYSDATE, 'YYYYMMDD') THEN

          UPDATE dams_authorityapply_set das
          SET    das.standard_id = v_standardid
          WHERE  das.system_type = i_sys_type
                 AND das.duty_lv_code = v_auth_items(2)
                 AND das.prm_secret_id = v_auth_items(1)
                 AND das.apply_type = v_auth_items(3)
                 AND das.branch_lv = v_auth_items(6)
                 AND das.upd_time = '99991231'
                 AND das.standard_id = v_standardid_temp;
          IF v_action_items(1) <> v_action_items(2) THEN

            SELECT COUNT(1)
            INTO   v_count
            FROM   dams_authorityapply_set das
            WHERE  das.system_type = i_sys_type
                   AND das.duty_lv_code = v_auth_items(2)
                   AND das.prm_secret_id = v_auth_items(1)
                   AND das.apply_type = v_auth_items(3)
                   AND das.branch_lv = v_auth_items(6)
                   AND das.upd_time = '99991231'
                   AND das.standard_id = v_standardid2;

            IF v_count <> '0'
               AND v_standardid_temp2 <> v_standardid THEN
              UPDATE dams_authorityapply_set das
              SET    das.standard_id = v_standardid2
              WHERE  das.system_type = i_sys_type
                     AND das.duty_lv_code = v_auth_items(2)
                     AND das.prm_secret_id = v_auth_items(1)
                     AND das.apply_type = v_auth_items(3)
                     AND das.branch_lv = v_auth_items(6)
                     AND das.upd_time = '99991231'
                     AND das.standard_id = v_standardid_temp2;
            ELSE
              UPDATE dams_authorityapply_set
              SET    upd_time = TO_CHAR(SYSDATE - 1, 'yyyymmdd')
              WHERE  standard_id = v_standardid2
                     AND system_type = i_sys_type
                     AND duty_lv_code = v_auth_items(2)
                     AND prm_secret_id = v_auth_items(1)
                     AND apply_type = v_auth_items(3)
                     AND branch_lv = v_auth_items(6)
                     AND upd_time = '99991231';

              INSERT INTO dams_authorityapply_set
                (key_id,
                 system_type,
                 duty_lv_code,
                 prm_secret_id,
                 apply_type,
                 cret_date,
                 upd_time,
                 cret_person,
                 upd_person,
                 colume1,
                 standard_id,
                 branch_lv)
              VALUES
                ('PL' || to_char(SYSDATE, 'yyyymmdd') ||
                 lpad(DAMS_AUTHORITYAPPLY_SET_SEQ.NEXTVAL, 10, '0'),
                 i_sys_type,
                 v_auth_items(2),
                 v_auth_items(1),
                 v_auth_items(3),
                 TO_CHAR(SYSDATE, 'yyyymmddhh24miss'),
                 '99991231',
                 v_cret_person,
                 i_modify_person,
                 '',
                 v_standardid2,
                 v_auth_items(6));

            END IF;

          END IF;

        ELSE
          UPDATE dams_authorityapply_set
          SET    upd_time = TO_CHAR(SYSDATE - 1, 'yyyymmdd')
          WHERE  standard_id = v_standardid_temp
                 AND system_type = i_sys_type
                 AND duty_lv_code = v_auth_items(2)
                 AND prm_secret_id = v_auth_items(1)
                 AND apply_type = v_auth_items(3)
                 AND branch_lv = v_auth_items(6)
                 AND upd_time = '99991231';
          INSERT INTO dams_authorityapply_set
            (key_id,
             system_type,
             duty_lv_code,
             prm_secret_id,
             apply_type,
             cret_date,
             upd_time,
             cret_person,
             upd_person,
             colume1,
             standard_id,
             branch_lv)
          VALUES
            (v_key_id,
             i_sys_type,
             v_auth_items(2),
             v_auth_items(1),
             v_auth_items(3),
             TO_CHAR(SYSDATE, 'yyyymmddhh24miss'),
             '99991231',
             v_cret_person,
             i_modify_person,
             '',
             v_standardid,
             v_auth_items(6));

          IF v_action_items(1) <> v_action_items(2) THEN

            SELECT COUNT(1)
            INTO   v_count
            FROM   dams_authorityapply_set das
            WHERE  das.system_type = i_sys_type
                   AND das.duty_lv_code = v_auth_items(2)
                   AND das.prm_secret_id = v_auth_items(1)
                   AND das.apply_type = v_auth_items(3)
                   AND das.branch_lv = v_auth_items(6)
                   AND das.upd_time = '99991231'
                   AND das.standard_id = v_standardid2;

            IF v_count <> '0'
               AND v_standardid_temp2 <> v_standardid THEN
              UPDATE dams_authorityapply_set das
              SET    das.standard_id = v_standardid2
              WHERE  das.system_type = i_sys_type
                     AND das.duty_lv_code = v_auth_items(2)
                     AND das.prm_secret_id = v_auth_items(1)
                     AND das.apply_type = v_auth_items(3)
                     AND das.branch_lv = v_auth_items(6)
                     AND das.upd_time = '99991231';
            ELSE

              UPDATE dams_authorityapply_set
              SET    upd_time = TO_CHAR(SYSDATE - 1, 'yyyymmdd')
              WHERE  standard_id = v_standardid2
                     AND system_type = i_sys_type
                     AND duty_lv_code = v_auth_items(2)
                     AND prm_secret_id = v_auth_items(1)
                     AND apply_type = v_auth_items(3)
                     AND branch_lv = v_auth_items(6)
                     AND upd_time = '99991231';
              INSERT INTO dams_authorityapply_set
                (key_id,
                 system_type,
                 duty_lv_code,
                 prm_secret_id,
                 apply_type,
                 cret_date,
                 upd_time,
                 cret_person,
                 upd_person,
                 colume1,
                 standard_id,
                 branch_lv)
              VALUES
                ('PL' || to_char(SYSDATE, 'yyyymmdd') ||
                 lpad(DAMS_AUTHORITYAPPLY_SET_SEQ.NEXTVAL, 10, '0'),
                 i_sys_type,
                 v_auth_items(2),
                 v_auth_items(1),
                 v_auth_items(3),
                 TO_CHAR(SYSDATE, 'yyyymmddhh24miss'),
                 '99991231',
                 v_cret_person,
                 i_modify_person,
                 '',
                 v_standardid2,
                 v_auth_items(6));

            END IF;

          END IF;
        END IF;

        SELECT u.stru_id
        INTO   v_branch_id
        FROM   dams_user u
        WHERE  u.ssic_id = i_modify_person;

      END LOOP;

      v_logType := '006';

      SELECT to_char(SYSDATE, 'yyyymmddhh24miss') INTO v_logDate FROM dual;

      v_logInfo := '';
      SELECT t.NAME
      INTO   v_temp
      FROM   dams_user t
      WHERE  t.ssic_id = i_modify_person;
      v_logInfo := ' 用户:' || v_logInfo || v_temp;
      v_logInfo := v_logInfo || ' 修改模板:' || v_template_name || '的安控策略';

      PCKG_DAMS_UTIL.proc_dams_logger(v_logObj,
                                      v_logType,
                                      v_logDate,
                                      v_logHandler,
                                      v_logInfo);
    END IF;

  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      o_flag := '-1';
      pack_log.error('PCKG_DAMS_AUTHORITY_APPLY.proc_update_authority_apply',
                     pack_log.end_step,
                     '异常！');
      PCKG_CTP_LG_PUBLIC.log('updateAuthorityApply()', SQLERRM(SQLCODE));
      RETURN;

  END proc_update_authority_apply;


  PROCEDURE proc_get_edit_auth(i_ssic_id IN VARCHAR2, --
                               o_flag    OUT VARCHAR2, --
                               o_auth    OUT VARCHAR2 --是否有编辑权限  1有,0无
                              )IS
    v_count NUMBER(8) ;
  BEGIN
    o_flag := '0';
    o_auth := '0';

    SELECT COUNT(1)
      INTO v_count
      FROM dams_user_role_rel t ,dams_branch_relation b
     WHERE t.ssic_id = i_ssic_id
       AND t.role_id IN ('DR20000099','OIS098','OIS099')
       AND t.stru_id = b.bnch_id
       AND b.up_bnch_id = '0010100500';

    IF v_count > 0 THEN
      o_auth := '1';
    END IF;

  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error('PCKG_DAMS_AUTHORITY_APPLY.proc_get_edit_auth',
                     pack_log.end_step,
                     '异常！');
      o_flag := '-1';
      o_auth := '0';
  END proc_get_edit_auth;

END PCKG_DAMS_AUTHORITY_APPLY;
//
CREATE OR REPLACE PACKAGE pckg_dams_jbpm IS
  PROCEDURE proc_jbpm_activity_alias(i_pdid       IN VARCHAR2, --流程定义ID
                                     i_cfgid      IN VARCHAR2, --配置项ID
                                     i_transition IN VARCHAR2, --流入路径名
                                     i_originname IN VARCHAR2, --图上活动名称
                                     o_flag       OUT VARCHAR2, --存储过程返回标志
                                     o_aliasname  OUT VARCHAR2 --别名
                                     );
  
 
END pckg_dams_jbpm;
//
CREATE OR REPLACE PACKAGE BODY     pckg_dams_jbpm IS
   
  
  PROCEDURE proc_jbpm_activity_alias(i_pdid       IN VARCHAR2, --流程定义ID
                                     i_cfgid      IN VARCHAR2, --配置ID
                                     i_transition IN VARCHAR2, --流入路径名
                                     i_originname IN VARCHAR2, --图上活动名称
                                     o_flag       OUT VARCHAR2, --存储过程返回标志
                                     o_aliasname  OUT VARCHAR2 --别名
                                     ) IS
    v_param     db_log.info%TYPE := i_pdid || '|' || i_cfgid || '|' ||
                                    i_transition || '|' || i_originname;
    v_proc_name db_log.proc_name%TYPE := 'PCKG_DAMS_JBPM.proc_jbpm_activity_alias';
    v_step      db_log.step_no%TYPE;
  
  BEGIN
    o_flag := '0';
  
    pack_log.info(v_proc_name, pack_log.start_step,
                  pack_log.start_msg || v_param);
  
    SELECT t.activity_name_alias
      INTO o_aliasname
      FROM dams_wf_activity_conf c, dams_wf_activity_alias t
     WHERE c.cfgid = t.cfgid
       AND c.id = t.activity_id
       AND c.cfgid = i_cfgid
       AND c.pdid = i_pdid
       AND c.activity_name = i_originname
       AND t.incoming_transition = i_transition;
  
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN no_data_found THEN
      o_flag      := '-1';
      o_aliasname := i_originname;
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      RETURN;
    WHEN OTHERS THEN
      o_flag      := '-1';
      o_aliasname := i_originname;
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      RETURN;
  END proc_jbpm_activity_alias;
  
 end;
 //

CREATE OR REPLACE PACKAGE pckg_dams_mail IS

  FUNCTION func_get_mailaddr(i_ssicid IN VARCHAR2) RETURN VARCHAR2;

  FUNCTION func_inner_subject(i_subject     IN CLOB,
                              i_title       IN VARCHAR2,
                              i_business_id IN VARCHAR2,
                              i_dest_name   IN VARCHAR2) RETURN CLOB;


  FUNCTION func_inner_content(i_content     IN CLOB,
                              i_username    IN VARCHAR2,
                              i_title       IN VARCHAR2,
                              i_business_id IN VARCHAR2,
                              i_syscode     IN VARCHAR2,
                              i_task_id     IN VARCHAR2,
                              i_dest_name   IN VARCHAR2) RETURN CLOB;

  FUNCTION func_get_mail_seq RETURN VARCHAR2;


  PROCEDURE proc_get_mail_template(i_tpl_id      IN VARCHAR2,
                                   o_subject_tpl OUT VARCHAR2,
                                   o_content_tpl OUT VARCHAR2);

  PROCEDURE proc_get_unsend_mail(i_hostip   IN VARCHAR2, --主机IP
                                 o_ret_code OUT VARCHAR2, --返回值
                                 o_cur      OUT SYS_REFCURSOR);

  PROCEDURE proc_insert_todo_mail(i_cfgid        IN VARCHAR2, --配置项编号
                                  i_pdid         IN VARCHAR2, --流程id
                                  i_language     IN VARCHAR2, --语言
                                  i_task_id      IN VARCHAR2, --任务ID
                                  i_next_task_id IN VARCHAR2, --下一任务ID
                                  i_business_id  IN VARCHAR2, --业务主键
                                  i_sys_code     IN VARCHAR2, --子模块编号
                                  i_activity_id  IN VARCHAR2, --活动定义id
                                  i_mail_tpl_id  IN VARCHAR2, --通用模板编号
                                  i_mail_to      IN VARCHAR2, --主送人
                                  i_mail_cc      IN VARCHAR2, --抄送人
                                  i_mail_bcc     IN VARCHAR2, --密送人
                                  i_title        IN VARCHAR2, --标题
                                  i_dest_name    IN VARCHAR2,
                                  o_ret_code     OUT VARCHAR2 --返回值
                                  );

  PROCEDURE proc_insert_custom_mail(i_mail_to    IN VARCHAR2, --主送人
                                    i_mail_cc    IN VARCHAR2, --抄送人
                                    i_mail_bcc   IN VARCHAR2, --密送人
                                    i_title      IN VARCHAR2, --标题
                                    i_content    IN VARCHAR2, --内容
                                    i_translated IN VARCHAR2, -- 是否需要地址转换
                                    o_ret_code   OUT VARCHAR2 --返回值
                                    );

  PROCEDURE proc_insert_custom_mail_new(i_tpl_id     IN VARCHAR2, --模板ID
                                        i_tpl_param  IN VARCHAR2, --模板参数
                                        i_mail_to    IN VARCHAR2, --发送人
                                        i_mail_cc    IN VARCHAR2, --抄送人
                                        i_mail_bcc   IN VARCHAR2, --密送人
                                        i_mail_type  IN VARCHAR2, --发送类型
                                        i_translated IN VARCHAR2, -- 是否需要地址转换
                                        o_ret_code   OUT VARCHAR2 --返回值
                                        );

  PROCEDURE proc_update_mail_status(i_mail_id  IN VARCHAR2, --邮件id
                                    i_status   IN VARCHAR2, --状态
                                    o_ret_code OUT VARCHAR2 --返回值
                                    );

  PROCEDURE proc_insert_todo_mail_vm(i_mail_to         IN VARCHAR2,
                                     i_pdid            IN VARCHAR2,
                                     i_cfgid           IN VARCHAR2,
                                     i_token           IN VARCHAR2,
                                     i_title           IN VARCHAR2,
                                     i_mail_tpl_id     IN VARCHAR2,
                                     i_dest_name       IN VARCHAR2,
                                     i_transition_name IN VARCHAR2,
                                     i_business_id     IN VARCHAR2,
                                     o_ret_code        OUT VARCHAR2);

  PROCEDURE proc_insert_st_mail(i_mail_to     IN VARCHAR2,
                                i_token       IN VARCHAR2,
                                i_title       IN VARCHAR2,
                                i_mail_tpl_id IN VARCHAR2,
                                i_business_id IN VARCHAR2,
                                o_ret_code    OUT VARCHAR2);


  FUNCTION func_gen_rndstr(i_len IN NUMBER) RETURN VARCHAR2;

  PROCEDURE proc_insert_custom_mail_token(i_tpl_id     IN VARCHAR2, --模板ID
                                          i_tpl_param  IN VARCHAR2, --模板参数
                                          i_mail_to    IN VARCHAR2, --发送人
                                          i_mail_cc    IN VARCHAR2, --抄送人
                                          i_mail_bcc   IN VARCHAR2, --密送人
                                          i_mail_type  IN VARCHAR2, --发送类型
                                          i_translated IN VARCHAR2, -- 是否需要地址转换
                                          o_ret_code   OUT VARCHAR2 --返回值
                                          );
  PROCEDURE proc_insert_inner_todo_mail(i_ssic_id     IN VARCHAR2,
                                        i_mail_to     IN VARCHAR2,
                                        i_business_id IN VARCHAR2,
                                        i_task_id     IN VARCHAR2,
                                        i_sys_code    IN VARCHAR2,
                                        i_title       in varchar2,
                                        o_ret_code    OUT VARCHAR2,
                                        o_msg         OUT VARCHAR2);

END pckg_dams_mail;
//
CREATE OR REPLACE PACKAGE BODY pckg_dams_mail IS

  FUNCTION func_get_mailaddr(i_ssicid IN VARCHAR2) RETURN VARCHAR2 IS
    v_retval CLOB := '';
    v_tmpval VARCHAR2(4000) := '';
    v_sql    VARCHAR2(4000) := '';
  BEGIN


    v_sql := 'SELECT LISTAGG(t.Notes_Id,'','') within group(order by t.notes_id) ' ||
             ' FROM dams_user t ' || ' WHERE t.ssic_id IN (SELECT column_value FROM TABLE(func_split(:1,'','')))';

    EXECUTE IMMEDIATE v_sql
      INTO v_retval
     USING i_ssicid;

    RETURN v_retval;
  EXCEPTION
    WHEN OTHERS THEN
      RETURN '';
  END func_get_mailaddr;

  FUNCTION func_inner_subject(i_subject     IN CLOB,
                              i_title       IN VARCHAR2,
                              i_business_id IN VARCHAR2,
                              i_dest_name   IN VARCHAR2) RETURN CLOB IS
    v_subject CLOB;
  BEGIN
    SELECT REPLACE(REPLACE(REPLACE(i_subject, '$subject', i_title),
                           '$businessId', i_business_id), '$destName',
                   i_dest_name)
      INTO v_subject
      FROM dual;

    RETURN v_subject;
  EXCEPTION
    WHEN OTHERS THEN
      RETURN '';
  END func_inner_subject;

  FUNCTION func_inner_content(i_content     IN CLOB,
                              i_username    IN VARCHAR2,
                              i_title       IN VARCHAR2,
                              i_business_id IN VARCHAR2,
                              i_syscode     IN VARCHAR2,
                              i_task_id     IN VARCHAR2,
                              i_dest_name   IN VARCHAR2) RETURN CLOB IS
    v_content      CLOB;
    v_finalopinion dams_wf_hist_task.outgoing_name%TYPE;
  BEGIN

    SELECT MAX(m.outgoing_name)
      INTO v_finalopinion
      FROM (SELECT decode(t.outgoing_name, 'default_transition', '审批通过',
                          t.outgoing_name) outgoing_name,
                   row_number() over(PARTITION BY t.business_id ORDER BY t.create_time DESC, t.task_id DESC) rn
              FROM dams_wf_hist_task t
             WHERE t.business_id = i_business_id) m
     WHERE m.rn = '1';

    SELECT REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(i_content,
                                                           '$userName',
                                                           i_username),
                                                   '$subject', i_title),
                                           '$businessId', i_business_id),
                                   '$taskId', i_task_id), '$destName',
                           i_dest_name), '$finalopinion', v_finalopinion)
      INTO v_content
      FROM dual;

    RETURN v_content;
  EXCEPTION
    WHEN OTHERS THEN
      RETURN '';
  END func_inner_content;


  FUNCTION func_get_mail_seq RETURN VARCHAR2 IS
    v_seq VARCHAR2(12) := '';
  BEGIN
    SELECT lpad(dams_mail_seq.nextval, 12, '0') INTO v_seq FROM dual;
    RETURN v_seq;
  EXCEPTION
    WHEN OTHERS THEN
      RETURN '';
  END func_get_mail_seq;

  PROCEDURE proc_get_mail_template(i_tpl_id      IN VARCHAR2,
                                   o_subject_tpl OUT VARCHAR2,
                                   o_content_tpl OUT VARCHAR2) IS
  BEGIN
    SELECT t.tpl_subject, t.tpl_content
      INTO o_subject_tpl, o_content_tpl
      FROM dams_mail_template t
     WHERE t.tpl_id = i_tpl_id;

  EXCEPTION
    WHEN OTHERS THEN
      o_subject_tpl := '';
      o_content_tpl := '';
  END proc_get_mail_template;

  PROCEDURE proc_get_unsend_mail(i_hostip   IN VARCHAR2, --主机IP
                                 o_ret_code OUT VARCHAR2, --返回值
                                 o_cur      OUT SYS_REFCURSOR) IS
    v_param      db_log.info%TYPE := i_hostip;
    v_proc_name  db_log.proc_name%TYPE := 'pckg_dams_mail.proc_get_unsend_mail';
    v_step       db_log.step_no%TYPE;
    v_systime    TIMESTAMP;
    unsend_mails dams_arrytype;
  BEGIN
    o_ret_code := 0;

    BEGIN
      EXECUTE IMMEDIATE 'SELECT t.dictvalue FROM ois_gen_dict t
                          WHERE t.dicttype = ''MAIL_LOCK''
                          AND t.dictcode = ''1''
              and t.sys_code = ''DAMS''
                          FOR UPDATE';
    EXCEPTION
      WHEN OTHERS THEN
	  dbms_output.put_line(55);
        pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
        o_ret_code := -1;
        RETURN;
    END;
    dbms_output.put_line(11);
    SELECT systimestamp INTO v_systime FROM dual;

    UPDATE dams_mail t
       SET t.send_flag = '1', t.load_time = v_systime, t.host_ip = i_hostip
     WHERE t.send_flag = '0'
       AND rownum <= 100
    RETURNING t.mail_id BULK COLLECT INTO unsend_mails;
    dbms_output.put_line(22);
    OPEN o_cur FOR
      SELECT m.mail_id,
             m.mail_to,
             m.mail_cc,
             m.mail_bcc,
             m.subject,
             m.content,
             m.template_id,
             m.template_data
        FROM dams_mail m, TABLE(unsend_mails) b
       WHERE b.column_value = m.mail_id;
    COMMIT;
	dbms_output.put_line(33);
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
	dbms_output.put_line(44);
      ROLLBACK;
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_ret_code := -1;
      RETURN;
  END proc_get_unsend_mail;


  PROCEDURE proc_insert_todo_mail(i_cfgid        IN VARCHAR2, --配置项编号
                                  i_pdid         IN VARCHAR2, --流程id
                                  i_language     IN VARCHAR2, --语言
                                  i_task_id      IN VARCHAR2, --任务ID
                                  i_next_task_id IN VARCHAR2, --下一任务ID
                                  i_business_id  IN VARCHAR2, --业务主键
                                  i_sys_code     IN VARCHAR2, --子模块编号
                                  i_activity_id  IN VARCHAR2, --活动定义id
                                  i_mail_tpl_id  IN VARCHAR2, --通用模板编号
                                  i_mail_to      IN VARCHAR2, --主送人
                                  i_mail_cc      IN VARCHAR2, --抄送人
                                  i_mail_bcc     IN VARCHAR2, --密送人
                                  i_title        IN VARCHAR2, --标题
                                  i_dest_name    IN VARCHAR2, --节点名称
                                  o_ret_code     OUT VARCHAR2 --返回值
                                  ) IS
    v_param       db_log.info%TYPE := i_cfgid || '|' || i_pdid || '|' ||
                                      i_language || '|' || i_task_id || '|' ||
                                      i_next_task_id || '|' || i_business_id || '|' ||
                                      i_sys_code || '|' || i_activity_id || '|' ||
                                      i_mail_tpl_id || '|' || i_sys_code || '|' ||
                                      i_mail_tpl_id || '|' || i_mail_to || '|' ||
                                      i_mail_cc || '|' || i_mail_bcc || '|' ||
                                      i_title || '|' || i_dest_name;
    v_proc_name   db_log.proc_name%TYPE := 'pckg_dams_mail.proc_insert_todo_mail';
    v_mail_seq    VARCHAR2(12);
    v_mailto_addr VARCHAR2(300);
    v_mailto_name VARCHAR2(90);
    v_taskid      VARCHAR2(2000);
    v_taskid2     VARCHAR2(19);
    v_subject     CLOB; --主题模板
    v_content     CLOB; --内容模板
    arr_mail_to   dbms_sql.varchar2_table;
    --arr_mail_cc     dbms_sql.varchar2_table;
    --arr_mail_bcc    dbms_sql.varchar2_table;
    arr_business_id dbms_sql.varchar2_table;
    arr_title       dbms_sql.varchar2_table;
    arr_taskid      dbms_sql.varchar2_table;
    v_mail_tpl_id   VARCHAR2(12);
    v_ssic_id       VARCHAR2(10);
  BEGIN

    o_ret_code := 0;

    arr_mail_to := func_dams_getarrayfromstr(i_mail_to, ',');

    arr_business_id := func_dams_getarrayfromstr(i_business_id, '$|$');
    arr_title       := func_dams_getarrayfromstr(i_title, '$|$');
    v_taskid        := nvl(i_next_task_id, i_task_id);
    arr_taskid      := func_dams_getarrayfromstr(v_taskid, ',');

    IF i_next_task_id IS NULL THEN

      v_mail_tpl_id := nvl(i_mail_tpl_id, '2');


      BEGIN

        SELECT m.subject_tpl, m.content_tpl
          INTO v_subject, v_content
          FROM dams_wf_mail_cfg m
         WHERE m.cfgid = i_cfgid
           AND m.activity_id = i_activity_id
           AND m.language = i_language;
      EXCEPTION
        WHEN no_data_found THEN
          SELECT nvl(t.tpl_subject, t1.tpl_subject),
                 nvl(t.tpl_content, t1.tpl_content)
            INTO v_subject, v_content
            FROM dams_mail_template t, dams_mail_template t1
           WHERE t.language(+) = i_language
             AND t.tpl_id(+) = t1.tpl_id
             AND t1.language = 'zh_CN'
             AND t1.tpl_id = v_mail_tpl_id; --指定通知模板
      END;

      --可以存在多条业务申请 mod by xiesy 20130328
      FOR i IN 1 .. arr_business_id.count
      LOOP

        --不指定主送人 ,则主送人为申请人
        IF i_mail_to IS NULL THEN
          SELECT MAX(u.notes_id), MAX(u.name), MAX(u.ssic_id)
            INTO v_mailto_addr, v_mailto_name, v_ssic_id
            FROM dams_wf_hist_process p, dams_user u
           WHERE p.business_id = arr_business_id(i)
             AND p.creator_id = u.ssic_id
             AND p.start_time =
                 (SELECT MIN(p2.start_time)
                    FROM dams_wf_hist_process p2
                   WHERE p2.business_id = arr_business_id(i));

        ELSE
          v_mailto_addr := func_get_mailaddr(i_mail_to);
        END IF;

        v_mail_seq := func_get_mail_seq();

        INSERT INTO dams_mail
          (mail_id, mail_to, mail_cc, mail_bcc, subject, content)
        VALUES
          (v_mail_seq, --id
           v_mailto_addr, --主送
           '', --抄送
           '', --密送
           func_inner_subject(v_subject, arr_title(i), arr_business_id(i),
                              i_dest_name), --主题
           func_inner_content(v_content, v_mailto_name, arr_title(i),
                              arr_business_id(i), i_sys_code, v_taskid,
                              i_dest_name)); --内容

        INSERT INTO dams_wf_task_mail
          (mail_id, task_id, business_id, sys_code, mail_to_ssic)
        VALUES
          (v_mail_seq, v_taskid, arr_business_id(i), i_sys_code, v_ssic_id);
      END LOOP;

    ELSE
      --暂时用单发模式,群发模式测试失败
      FOR i IN 1 .. arr_mail_to.count
      LOOP
        --主送人ssicId不为空,则为审批人通知
        IF arr_mail_to(i) IS NOT NULL THEN

          v_mail_tpl_id := nvl(i_mail_tpl_id, '1'); --标准审批通知模板

          --主题 , 内容, 链接
          BEGIN
            --没有传入activity_id时的临时处理方式
            SELECT m.subject_tpl, m.content_tpl
              INTO v_subject, v_content
              FROM dams_wf_mail_cfg m
             WHERE m.cfgid = i_cfgid
               AND m.activity_id =
                   (SELECT c.id
                      FROM dams_wf_task t, dams_wf_activity_conf c
                     WHERE t.task_id = i_task_id
                       AND t.task_name = c.activity_name
                       AND c.cfgid = m.cfgid)
               AND m.language = i_language;
          EXCEPTION
            WHEN no_data_found THEN
              SELECT t.tpl_subject, t.tpl_content
                INTO v_subject, v_content
                FROM dams_mail_template t
               WHERE t.language = i_language
                 AND t.tpl_id = v_mail_tpl_id; --指定通知模板
          END;

          --主送人地址
          SELECT MAX(u.notes_id), MAX(u.name)
            INTO v_mailto_addr, v_mailto_name
            FROM dams_user u
           WHERE u.ssic_id = arr_mail_to(i);


          FOR j IN 1 .. arr_business_id.count
          LOOP

            --判断是否有多条taskid
            IF arr_taskid IS NOT NULL AND arr_taskid.count > 1 THEN
              --判断是否多条businessId
              IF arr_business_id.count > 1 THEN
                --多个businessId,则是一个单子需要拆流程的,task序号和businessId序号对应
                v_taskid2 := arr_taskid(j);
              ELSE
                --单个businessId,多个taskId,则fork分支情况,暂不处理
                NULL;
              END IF;
            ELSE
              --只有一个taskId,直接赋值
              v_taskid2 := arr_taskid(1);
            END IF;
            v_mail_seq := func_get_mail_seq();

            INSERT INTO dams_mail
              (mail_id, mail_to, mail_cc, mail_bcc, subject, content)
            VALUES
              (v_mail_seq, --id
               v_mailto_addr, --主送
               '', --抄送
               '', --密送
               func_inner_subject(v_subject, arr_title(j), arr_business_id(j),
                                  i_dest_name), --主题
               func_inner_content(v_content, v_mailto_name, arr_title(j),
                                  arr_business_id(j), i_sys_code, v_taskid2,
                                  i_dest_name)); --内容

            INSERT INTO dams_wf_task_mail
              (mail_id, task_id, business_id, sys_code, mail_to_ssic)
            VALUES
              (v_mail_seq,
               v_taskid2,
               arr_business_id(j),
               i_sys_code,
               arr_mail_to(i));
          END LOOP;

        END IF;
      END LOOP;

    END IF;

    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step,
                     '异常！' || SQLERRM || v_param);
      o_ret_code := -1;
      RETURN;
  END proc_insert_todo_mail;

  -- Author  : kfzx-haozj
  -- Created : 2012/9/6 14:05:16
  -- Module :  插入自定义邮件
  -- Purpose :  插入自定义邮件
  PROCEDURE proc_insert_custom_mail(i_mail_to    IN VARCHAR2, --主送人
                                    i_mail_cc    IN VARCHAR2, --抄送人
                                    i_mail_bcc   IN VARCHAR2, --密送人
                                    i_title      IN VARCHAR2, --标题
                                    i_content    IN VARCHAR2, --内容
                                    i_translated IN VARCHAR2, -- 是否需要地址转换
                                    o_ret_code   OUT VARCHAR2 --返回值
                                    ) IS
    v_param        db_log.info%TYPE := substrb(i_mail_to || '|' || i_mail_cc || '|' ||
                                               i_mail_bcc || '|' || i_title || '|' ||
                                               i_content, 1, 3900);
    v_proc_name    db_log.proc_name%TYPE := 'pckg_dams_mail.proc_insert_custom_mail';
    v_mail_seq     VARCHAR2(12);
    v_mailto_addr  VARCHAR2(300);
    v_mailcc_addr  VARCHAR2(300);
    v_mailbcc_addr VARCHAR2(300);
    v_mailto_name  VARCHAR2(90);
    v_processkey   VARCHAR2(50);
    v_taskid       VARCHAR2(19);
    v_idx          INTEGER;
    v_subject      CLOB; --主题模板
    v_content      CLOB; --内容模板
    v_url          VARCHAR2(1000);
    arr_mail_to    dbms_sql.varchar2_table;
    arr_mail_cc    dbms_sql.varchar2_table;
    arr_mail_bcc   dbms_sql.varchar2_table;
    v_mail_tpl_id  VARCHAR2(12);
    v_ssic_id      VARCHAR2(10);
  BEGIN
    o_ret_code := 0;
    IF i_translated <> '1' THEN
      v_mailto_addr  := func_get_mailaddr(i_mail_to);
      v_mailcc_addr  := func_get_mailaddr(i_mail_cc);
      v_mailbcc_addr := func_get_mailaddr(i_mail_bcc);
    ELSE
      v_mailto_addr  := i_mail_to;
      v_mailcc_addr  := i_mail_cc;
      v_mailbcc_addr := i_mail_bcc;
    END IF;

    v_mail_seq := func_get_mail_seq();

    --群发用,尝试失败
    /*INSERT INTO dams_mail
      (mail_id, mail_to, mail_cc, mail_bcc, subject, content)
    VALUES
      (v_mail_seq,
       v_mailto_addr,
       v_mailcc_addr,
       v_mailbcc_addr,
       i_title,
       i_content);*/

    --单条发送解析用
    arr_mail_to := func_dams_getarrayfromstr(v_mailto_addr, ',');
    --arr_mail_cc  := func_dams_getarrayfromstr(v_mailcc_addr, ',');
    --arr_mail_bcc := func_dams_getarrayfromstr(v_mailbcc_addr, ',');

    FOR i IN 1 .. arr_mail_to.count
    LOOP

      v_mail_seq := func_get_mail_seq();

      INSERT INTO dams_mail
        (mail_id, mail_to, mail_cc, mail_bcc, subject, content)
      VALUES
        (v_mail_seq,
         arr_mail_to(i),
         v_mailcc_addr,
         v_mailbcc_addr,
         i_title,
         i_content);

    END LOOP;

    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_ret_code := -1;
      RETURN;
  END proc_insert_custom_mail;

  -- Author  : kfzx-haozj
  -- Created : 2012/9/6 14:05:16
  -- Module :  插入自定义邮件
  -- Purpose :  插入自定义邮件
  PROCEDURE proc_insert_custom_mail_new(i_tpl_id     IN VARCHAR2, --模板ID
                                        i_tpl_param  IN VARCHAR2, --模板参数
                                        i_mail_to    IN VARCHAR2, --发送人
                                        i_mail_cc    IN VARCHAR2, --抄送人
                                        i_mail_bcc   IN VARCHAR2, --密送人
                                        i_mail_type  IN VARCHAR2, --发送类型
                                        i_translated IN VARCHAR2, -- 是否需要地址转换
                                        o_ret_code   OUT VARCHAR2 --返回值
                                        ) IS
    v_param        db_log.info%TYPE := i_tpl_id || '|' || i_mail_to || '|' ||
                                       i_mail_type || '|' || i_translated;
    v_proc_name    db_log.proc_name%TYPE := 'pckg_dams_mail.proc_insert_custom_mail_new';
    v_step         db_log.step_no%TYPE;
    v_mail_seq     VARCHAR2(12);
    v_mailto_addr  VARCHAR2(4000);
    v_mailcc_addr  VARCHAR2(4000);
    v_mailbcc_addr VARCHAR2(4000);
    v_mailto_name  VARCHAR2(90);
    v_processkey   VARCHAR2(50);
    v_taskid       VARCHAR2(19);
    v_idx          INTEGER;
    v_subject      CLOB; --主题模板
    v_content      CLOB; --内容模板
    v_url          VARCHAR2(1000);
    arr_mail_to    dams_arrytype;
    arr_mail_cc    dams_arrytype;
    arr_mail_bcc   dams_arrytype;
    v_mail_tpl_id  VARCHAR2(12);
    v_ssic_id      VARCHAR2(10);
  BEGIN
    o_ret_code := 0;
    IF nvl(i_translated, '0') <> '1' THEN
      v_mailto_addr  := func_get_mailaddr(i_mail_to);
      v_mailcc_addr  := func_get_mailaddr(i_mail_cc);
      v_mailbcc_addr := func_get_mailaddr(i_mail_bcc);
    ELSE
      v_mailto_addr  := i_mail_to;
      v_mailcc_addr  := i_mail_cc;
      v_mailbcc_addr := i_mail_bcc;
    END IF;

 --单条发送解析用
    arr_mail_to := pckg_dams_util.func_str_to_array(v_mailto_addr, ',');

    FOR i IN 1 .. arr_mail_to.count
    LOOP

      v_mail_seq := func_get_mail_seq();

      INSERT INTO dams_mail
        (mail_id,
         mail_type,
         mail_to,
         mail_cc,
         mail_bcc,
         template_id,
         template_data)
      VALUES
        (v_mail_seq,
         i_mail_type,
         arr_mail_to(i),
         v_mailcc_addr,
         v_mailbcc_addr,
         i_tpl_id,
         i_tpl_param);

    END LOOP;

    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_ret_code := -1;
      RETURN;
  END proc_insert_custom_mail_new;

  -- Author  : kfzx-haozj
  -- Created : 2012/9/6 14:05:16
  -- Module :  置发送状态
  -- Purpose :  置发送状态
  PROCEDURE proc_update_mail_status(i_mail_id  IN VARCHAR2, --邮件id
                                    i_status   IN VARCHAR2, --状态
                                    o_ret_code OUT VARCHAR2 --返回值
                                    ) IS
    v_param     db_log.info%TYPE := i_mail_id;
    v_proc_name db_log.proc_name%TYPE := 'pckg_dams_mail.proc_update_mail_status';
    v_step      db_log.step_no%TYPE;
    v_idx       INTEGER;
  BEGIN
    o_ret_code := 0;

    IF i_status = '0' THEN
      UPDATE dams_mail t
         SET t.send_flag = '8', --发送成功
             t.send_time = systimestamp
       WHERE t.mail_id = i_mail_id;
    ELSE
      UPDATE dams_mail t
         SET t.send_flag = '9', --发送失败
             t.send_time = systimestamp
       WHERE t.mail_id = i_mail_id;
    END IF;

    COMMIT;

    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_ret_code := -1;
      RETURN;
  END proc_update_mail_status;

  -- Author  : kfzx-maming
  -- Module :  插入vm待办邮件
  -- Purpose :  插入vm代办邮件
  PROCEDURE proc_insert_todo_mail_vm(i_mail_to         IN VARCHAR2,
                                     i_pdid            IN VARCHAR2,
                                     i_cfgid           IN VARCHAR2,
                                     i_token           IN VARCHAR2,
                                     i_title           IN VARCHAR2,
                                     i_mail_tpl_id     IN VARCHAR2,
                                     i_dest_name       IN VARCHAR2,
                                     i_transition_name IN VARCHAR2,
                                     i_business_id     IN VARCHAR2,
                                     o_ret_code        OUT VARCHAR2) IS
    v_param       db_log.info%TYPE := i_mail_to || '|' || i_pdid || '|' ||
                                      i_cfgid || '|' || i_token || '|' ||
                                      i_title || '|' || i_dest_name || '|' ||
                                      i_mail_tpl_id || '|' || i_transition_name || '|' ||
                                      i_business_id;
    v_proc_name   db_log.proc_name%TYPE := 'pckg_dams_mail.proc_insert_todo_mail_vm';
    v_dest_name   VARCHAR2(255);
    v_mail_seq    VARCHAR(12);
    v_mail_tpl_id VARCHAR(12);
    v_mailto_addr VARCHAR(100);
    v_mailtype    VARCHAR(12);
    v_applyer     dams_user.name%TYPE;
    v_data        CLOB;
  BEGIN
    o_ret_code := 0;

    --获取申请人/流程发起人/经办人
    SELECT MAX(u.name)
      INTO v_applyer
      FROM dams_user u, dams_wf_process p
     WHERE u.ssic_id = p.creator_id
       AND p.business_id = i_business_id
       AND p.cfg_id = nvl(i_cfgid, '1')
       AND p.process_id = i_pdid;
    --下一任务的别名
    pckg_dams_jbpm.proc_jbpm_activity_alias(i_pdid, nvl(i_cfgid, '1'),
                                            i_transition_name, i_dest_name,
                                            o_ret_code, v_dest_name);
    v_mailtype := '1';

    v_mail_seq := func_get_mail_seq();

    v_mail_tpl_id := nvl(i_mail_tpl_id, '1');

    SELECT MAX(t.notes_id)
      INTO v_mailto_addr
      FROM dams_user t
     WHERE t.ssic_id = i_mail_to;

    IF v_mailto_addr IS NOT NULL THEN
      v_data := 'destName=' || v_dest_name || '=' || v_applyer ||
                '=' || i_title || '=' || i_business_id ||
                '=' || i_token;

      INSERT INTO dams_mail
        (mail_id, mail_type, mail_to, template_id, template_data)
      VALUES
        (v_mail_seq, v_mailtype, v_mailto_addr, v_mail_tpl_id, v_data);

    END IF;
    /*    INSERT INTO dams_wf_task_mail
      (mail_id, task_id, business_id, sys_code, mail_to_ssic)
    VALUES
      (v_mail_seq, i_next_task_id, i_business_id, i_sys_code, i_mail_to);*/

    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_ret_code := -1;
      RETURN;
  END proc_insert_todo_mail_vm;

  -- Author  : kfzx-jiangke
  -- Module :  插入st通知邮件
  -- Purpose :  插入st通知邮件
  PROCEDURE proc_insert_st_mail(i_mail_to     IN VARCHAR2,
                                i_token       IN VARCHAR2,
                                i_title       IN VARCHAR2,
                                i_mail_tpl_id IN VARCHAR2,
                                i_business_id IN VARCHAR2,
                                o_ret_code    OUT VARCHAR2) IS
    v_param       db_log.info%TYPE := i_mail_to || '|' || i_token || '|' ||
                                      i_title || '|' || i_mail_tpl_id || '|' ||
                                      i_business_id;
    v_proc_name   db_log.proc_name%TYPE := 'pckg_dams_mail.proc_insert_st_mail';
    v_mail_seq    VARCHAR(12);
    v_mail_tpl_id VARCHAR(12);
    v_mailto_addr VARCHAR(100);
    v_title       VARCHAR(1000);
    v_mailtype    VARCHAR(12);
    v_data        CLOB;
  BEGIN
    o_ret_code := 0;

    v_mailtype := '1';

    v_mail_seq := func_get_mail_seq();

    v_mail_tpl_id := nvl(i_mail_tpl_id, '1');

    SELECT MAX(t.notes_id)
      INTO v_mailto_addr
      FROM dams_user t
     WHERE t.ssic_id = i_mail_to;
    SELECT t.title
      INTO v_title
      FROM dams_wf_hist_process t
     WHERE t.business_id = i_business_id;

    v_data := '=' || v_title || '=' || i_business_id ||
              '=' || i_token;

    INSERT INTO dams_mail
      (mail_id, mail_type, mail_to, template_id, template_data)
    VALUES
      (v_mail_seq, v_mailtype, v_mailto_addr, v_mail_tpl_id, v_data);

    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_ret_code := -1;
      RETURN;
  END proc_insert_st_mail;

  -- Purpose : 生成token
  FUNCTION func_gen_rndstr(i_len IN NUMBER) RETURN VARCHAR2 IS
    v_ret        VARCHAR2(1000);
    v_candidates VARCHAR2(62) := '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ';
    v_chr        VARCHAR2(1);
  BEGIN
    FOR i IN 1 .. i_len
    LOOP
      BEGIN
        SELECT substr(v_candidates, floor(dbms_random.value(0, 62)), 1)
          INTO v_chr
          FROM dual;
        v_ret := v_ret || v_chr;
      END;
    END LOOP;
    RETURN v_ret;
  EXCEPTION
    WHEN OTHERS THEN
      RETURN '';
  END func_gen_rndstr;

  -- Author  : kfzx-gugc
  -- Created : 20151021
  -- Module :  插入自定义邮件和token表
  -- Purpose :  插入自定义邮件和token表
  PROCEDURE proc_insert_custom_mail_token(i_tpl_id     IN VARCHAR2, --模板ID
                                          i_tpl_param  IN VARCHAR2, --模板参数
                                          i_mail_to    IN VARCHAR2, --发送人
                                          i_mail_cc    IN VARCHAR2, --抄送人
                                          i_mail_bcc   IN VARCHAR2, --密送人
                                          i_mail_type  IN VARCHAR2, --发送类型
                                          i_translated IN VARCHAR2, -- 是否需要地址转换
                                          o_ret_code   OUT VARCHAR2 --返回值
                                          ) IS
    v_param        db_log.info%TYPE := i_tpl_id || '|' || i_tpl_param || '|' ||
                                       i_mail_to || '|' || i_mail_cc || '|' ||
                                       i_mail_bcc || '|' || i_mail_type || '|' ||
                                       i_translated;
    v_proc_name    db_log.proc_name%TYPE := 'pckg_dams_mail.proc_insert_custom_mail_token';
    v_step         db_log.step_no%TYPE;
    v_mail_seq     VARCHAR2(12);
    v_mailto_addr  VARCHAR2(4000);
    v_mailcc_addr  VARCHAR2(4000);
    v_mailbcc_addr VARCHAR2(4000);
    v_mailto_name  VARCHAR2(90);
    v_processkey   VARCHAR2(50);
    v_taskid       VARCHAR2(19);
    v_idx          INTEGER;
    v_subject      CLOB; --主题模板
    v_content      CLOB; --内容模板
    v_url          VARCHAR2(1000);
    arr_mail_to    pckg_dams_role.arrytype;
    arr_mail_cc    dams_arrytype;
    arr_mail_bcc   dams_arrytype;
    v_mail_tpl_id  VARCHAR2(12);
    v_ssic_id      VARCHAR2(10);

    v_token     VARCHAR2(40) := ''; --token字段
    v_url_param VARCHAR2(4000) := '';
    arr_param   pckg_dams_role.arrytype;

    c_done_task_tpl_id CONSTANT VARCHAR2(20) := '34,131';
    v_is_done_task VARCHAR2(1);
  BEGIN
    o_ret_code := 0;


    SELECT COUNT(1)
      INTO v_is_done_task
      FROM TABLE(func_split2(c_done_task_tpl_id))
     WHERE column_value = i_tpl_id;

    --arr_param := pckg_dams_role.func_common_getarray(i_tpl_param, '&');
    FOR i IN 1 .. arr_param.count
    LOOP
      IF instr(arr_param(i), 'businessId') > 0 THEN
        v_url_param := v_url_param || '&' ||
                       REPLACE(arr_param(i), 'businessId', 'business_id');
      END IF;

      IF instr(arr_param(i), 'taskId') > 0 THEN
        v_url_param := v_url_param || '&' ||
                       REPLACE(arr_param(i), 'taskId', 'jbpm_task_id');

      END IF;
    END LOOP;

    --单条发送解析用
    --arr_mail_to := pckg_dams_role.func_common_getarray(i_mail_to, ',');

    FOR i IN 1 .. arr_mail_to.count
    LOOP

      v_mail_seq := func_get_mail_seq();
      v_token    := func_gen_rndstr(40);

      IF nvl(i_translated, '0') <> '1' THEN

        INSERT INTO dams_mail_token
          (token_id,
           ssic_id,
           method,
           url_param,
           create_time,
           dealed_time,
           expire_time)
        VALUES
          (v_token,
           arr_mail_to(i),
           decode(v_is_done_task, '1', 'opendonetask', 'opentodotask'),
           v_url_param,
           to_char(SYSDATE, 'yyyy-mm-dd hh24:mi:ss'),
           NULL,
           to_char(SYSDATE + 3, 'yyyy-mm-dd hh24:mi:ss'));

        INSERT INTO dams_mail
          (mail_id,
           mail_type,
           mail_to,
           mail_cc,
           mail_bcc,
           template_id,
           template_data)
          SELECT v_mail_seq,
                 i_mail_type,
                 u.notes_id,
                 v_mailcc_addr,
                 v_mailbcc_addr,
                 i_tpl_id,
                 i_tpl_param || '=' || v_token
            FROM dams_user u
           WHERE u.ssic_id = arr_mail_to(i);
      ELSE
        BEGIN
          SELECT u.ssic_id
            INTO v_ssic_id
            FROM dams_user u
           WHERE u.notes_id = arr_mail_to(i)
             AND rownum = 1;
        EXCEPTION
          WHEN OTHERS THEN
            v_ssic_id := '';
        END;
        INSERT INTO dams_mail_token
          (token_id,
           ssic_id,
           method,
           url_param,
           create_time,
           dealed_time,
           expire_time)
        VALUES
          (v_token,
           v_ssic_id,
           decode(v_is_done_task, '1', 'opendonetask', 'opentodotask'),
           v_url_param,
           to_char(SYSDATE, 'yyyy-mm-dd hh24:mi:ss'),
           NULL,
           to_char(SYSDATE + 3, 'yyyy-mm-dd hh24:mi:ss'));

        INSERT INTO dams_mail
          (mail_id,
           mail_type,
           mail_to,
           mail_cc,
           mail_bcc,
           template_id,
           template_data)
        VALUES
          (v_mail_seq,
           i_mail_type,
           arr_mail_to(i),
           v_mailcc_addr,
           v_mailbcc_addr,
           i_tpl_id,
           i_tpl_param || '=' || v_token);
      END IF;

    END LOOP;

    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_ret_code := -1;
      RETURN;
  END proc_insert_custom_mail_token;

  -- 发送inner_todo 待办邮件
  PROCEDURE proc_insert_inner_todo_mail(i_ssic_id     IN VARCHAR2,
                                        i_mail_to     IN VARCHAR2,
                                        i_business_id IN VARCHAR2,
                                        i_task_id     IN VARCHAR2,
                                        i_sys_code    IN VARCHAR2,
                                        i_title       IN VARCHAR2,
                                        o_ret_code    OUT VARCHAR2,
                                        o_msg         OUT VARCHAR2) IS
    v_param     db_log.info%TYPE := i_ssic_id || '|' || i_mail_to || '|' ||
                                    i_business_id || '|' || i_task_id || '|' ||
                                    i_sys_code;
    v_proc_name db_log.proc_name%TYPE := 'pckg_dams_mail.proc_insert_inner_todo_mail';

    arr_mail_to pckg_dams_role.arrytype;
    v_mail_seq  VARCHAR2(12);

    v_token     VARCHAR2(40) := ''; --token字段
    v_url_param VARCHAR2(4000) := '';

    c_tpl_id NUMBER(5) := 35;
    c_method VARCHAR2(20) := 'openinnertodo';

  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step,
                  pack_log.start_msg || v_param);
    o_ret_code := '0';

    v_url_param := '=' || i_business_id || '=' ||
                   i_task_id;

    --单条发送解析用
    --arr_mail_to := pckg_dams_role.func_common_getarray(i_mail_to, ',');

    FOR i IN 1 .. arr_mail_to.count
    LOOP
      v_mail_seq := func_get_mail_seq();
      v_token    := func_gen_rndstr(40);
      INSERT INTO dams_mail_token
        (token_id,
         ssic_id,
         method,
         url_param,
         create_time,
         dealed_time,
         expire_time)
      VALUES
        (v_token,
         arr_mail_to(i),
         c_method,
         v_url_param,
         to_char(SYSDATE, 'yyyy-mm-dd hh24:mi:ss'),
         NULL,
         to_char(SYSDATE + 3, 'yyyy-mm-dd hh24:mi:ss'));

      INSERT INTO dams_mail
        (mail_id,
         mail_type,
         mail_to,
         mail_cc,
         mail_bcc,
         template_id,
         template_data)
        SELECT v_mail_seq,
               '1',
               u.notes_id,
               '',
               '',
               c_tpl_id,
               '=' || v_token || '=' || i_title
          FROM dams_user u
         WHERE u.ssic_id = arr_mail_to(i);
    END LOOP;

  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_ret_code := '-1';
  END proc_insert_inner_todo_mail;

END pckg_dams_mail;

//

CREATE OR REPLACE PACKAGE pckg_dams_authapply_process IS

  TYPE t_item_action IS RECORD(
    action_order dams_standard_process_model.order_code%TYPE, --活动序号
    action_name  dams_wf_activity_conf.activity_name%TYPE,
    open_state   dams_standard_process_model.open_state%TYPE); --活动名称

  TYPE t_item_step IS RECORD(
    showname       dams_wf_activity_conf.activity_name%TYPE, --活动名称
    transitionname dams_wf_activity_user.transition_name%TYPE, --活动序号
    activityid     dams_wf_activity_conf.id%TYPE); --活动名称

  TYPE t_item_option IS RECORD(
    option_user    dams_user.name%TYPE, --留言人
    option_tele    dams_user.office_phone1%TYPE, --留言人电话
    option_content dams_authorityapply_comment.coment%TYPE, --留言内容
    option_date    VARCHAR2(24)); --留言时间

  TYPE ref_t_item_action IS REF CURSOR RETURN t_item_action;
  TYPE ref_t_item_step IS REF CURSOR RETURN t_item_step;
  TYPE ref_t_item_option IS REF CURSOR RETURN t_item_option;

  PROCEDURE proc_get_authapply_info(i_key_id          IN VARCHAR2,
                                    i_task_id         IN VARCHAR2,
                                    i_finish          IN VARCHAR2,
                                    i_process_id      IN VARCHAR2,
                                    o_flag            OUT VARCHAR2,
                                    o_apply_num       OUT VARCHAR2,
                                    o_branch_id       OUT VARCHAR2,
                                    o_branch_name     OUT VARCHAR2,
                                    o_doc_id          OUT VARCHAR2,
                                    o_doc_type        OUT VARCHAR2,
                                    o_doc_title       OUT VARCHAR2,
                                    o_doc_num         OUT VARCHAR2,
                                    o_doc_sec         OUT VARCHAR2,
                                    o_doc_secid       OUT VARCHAR2,
                                    o_read_order      OUT VARCHAR2,
                                    o_copy_order      OUT VARCHAR2,
                                    o_print_order     OUT VARCHAR2,
                                    o_read_auth       OUT VARCHAR2,
                                    o_copy_auth       OUT VARCHAR2,
                                    o_print_auth      OUT VARCHAR2,
                                    o_auth_read_id    OUT VARCHAR2,
                                    o_auth_copy_id    OUT VARCHAR2,
                                    o_auth_print_id   OUT VARCHAR2,
                                    o_auth_read_name  OUT VARCHAR2,
                                    o_auth_copy_name  OUT VARCHAR2,
                                    o_auth_print_name OUT VARCHAR2,
                                    o_apply_start     OUT VARCHAR2,
                                    o_apply_end       OUT VARCHAR2,
                                    o_print_time      OUT VARCHAR2,
                                    o_apply_reason    OUT VARCHAR2,
                                    o_pre_role        OUT VARCHAR2,
                                    o_pre_option      OUT VARCHAR2,
                                    o_pre_date        OUT VARCHAR2,
                                    o_pre_task_name   OUT VARCHAR2,
                                    o_activity_id     OUT VARCHAR2,
                                    o_sys_code        OUT VARCHAR2,
                                    o_own_auth        OUT VARCHAR2,
                                    o_has_doc         OUT VARCHAR2,
                                    o_encrypt_type    OUT VARCHAR2,
                                    o_ic2_print_times OUT VARCHAR2,
                                    o_ret_list        OUT SYS_REFCURSOR,
                                    o_option_chief    OUT ref_t_item_option,
                                    o_option_manager  OUT ref_t_item_option,
                                    o_option_governor OUT ref_t_item_option);


  PROCEDURE proc_insert_auth_apply(i_edit_type        IN VARCHAR2,
                                   i_change_auth      IN VARCHAR2,
                                   i_bussiness_id     IN VARCHAR2,
                                   i_move_flag        IN VARCHAR2,
                                   i_file_type        IN VARCHAR2,
                                   i_apply_person_id  IN VARCHAR2,
                                   i_branch_id        IN VARCHAR2,
                                   i_doc_type         IN VARCHAR2,
                                   i_doc_id           IN VARCHAR2,
                                   i_sec_id           IN VARCHAR2,
                                   i_doc_num          IN VARCHAR2,
                                   i_doc_title        IN VARCHAR2,
                                   i_file_header      IN CLOB,
                                   i_read_order       IN VARCHAR2,
                                   i_copy_order       IN VARCHAR2,
                                   i_print_order      IN VARCHAR2,
                                   i_read_auth        IN VARCHAR2,
                                   i_copy_auth        IN VARCHAR2,
                                   i_print_auth       IN VARCHAR2,
                                   i_ar_person        IN VARCHAR2,
                                   i_ac_person        IN VARCHAR2,
                                   i_ap_person        IN VARCHAR2,
                                   i_apply_start      IN VARCHAR2,
                                   i_apply_end        IN VARCHAR2,
                                   i_print_time       IN VARCHAR2,
                                   i_apply_reason     IN VARCHAR2,
                                   i_upd_person       IN VARCHAR2,
                                   i_option           IN VARCHAR2,
                                   i_sys_code         IN VARCHAR2,
                                   i_finish           IN VARCHAR2,
                                   i_point_name       IN VARCHAR2,
                                   i_com_role_id      IN VARCHAR2,
                                   i_drop_flag        IN VARCHAR2,
                                   i_own_auth         IN VARCHAR2,
                                   i_has_doc          IN VARCHAR2,
                                   i_ic2_print_times  IN VARCHAR2,
                                   i_encrypt_type     IN VARCHAR2,
                                   i_from_flag        IN VARCHAR2,
                                   o_flag             OUT VARCHAR2,
                                   o_m_flag           OUT VARCHAR2,
                                   o_jbpm_business_id OUT VARCHAR2);


  PROCEDURE proc_get_pre_taskinfo(i_task_id             IN VARCHAR2,
                                  o_flag                OUT VARCHAR2,
                                  o_pre_task_name       OUT VARCHAR2,
                                  o_pre_task_transition OUT VARCHAR2);


  PROCEDURE proc_get_process_auth(i_sec_id           IN VARCHAR2,
                                  i_role_id          IN VARCHAR2,
                                  i_process_id       IN VARCHAR2,
                                  i_branch_lv        IN VARCHAR2,
                                  i_sys_type         IN VARCHAR2,
                                  o_flag             OUT VARCHAR2,
                                  o_task_order_read  OUT ref_t_item_action,
                                  o_task_order_copy  OUT ref_t_item_action,
                                  o_task_order_print OUT ref_t_item_action);


  PROCEDURE proc_get_role_info(i_starter_id    IN VARCHAR2, --申请人ID
                               i_branch_id     IN VARCHAR2, --申请人机构
                               i_cur_id        IN VARCHAR2, --当前处理人ID
                               i_cur_task      IN VARCHAR2, --当前活动ID
                               i_sec_id        IN VARCHAR2, --文档ID
                               o_flag          OUT VARCHAR2, --返回标志
                               o_doc_sec       OUT VARCHAR2, --密级
                               o_branch_lv     OUT VARCHAR2, --机构层级
                               o_start_lv      OUT VARCHAR2, --申请人职务层级编码
                               o_start_ad      OUT VARCHAR2, --申请人AD帐号
                               o_start_notesid OUT VARCHAR2, --申请人notesID
                               o_start_tele    OUT VARCHAR2, --申请人电话
                               o_stru_name     OUT VARCHAR2, --拟稿人机构部室名称
                               o_stru_id       OUT VARCHAR2, --拟稿人机构部室ID
                               o_cur_lv        OUT VARCHAR2, --当前处理人职务层级编码
                               o_cur_order     OUT VARCHAR2, --当前处理序号
                               o_process_id    OUT VARCHAR2, --流程名称
                               o_apply_stru    OUT VARCHAR2 --申请机构ID
                               );

  PROCEDURE proc_get_next_steps(i_edit_type      IN VARCHAR2, --是否可编辑
                                i_process_id     IN VARCHAR2, --流程ID
                                i_task_name      IN VARCHAR2, --活动名称
                                i_min_order      IN VARCHAR2, --开始审批序号
                                i_max_order      IN VARCHAR2, --最高审批序号
                                i_max_state      IN VARCHAR2, --最高审判状态
                                o_flag           OUT VARCHAR2, --返回标志
                                o_next_step_list OUT ref_t_item_step --下一活动列表
                                );

  PROCEDURE proc_has_auth(i_ssic_id    IN VARCHAR2, --用户ID
                          i_sec_id     IN VARCHAR2, --密级ID
                          i_sys_type   IN VARCHAR2, --活动名称
                          o_flag       OUT VARCHAR2, --返回标志
                          o_read_auth  OUT VARCHAR2,
                          o_copy_auth  OUT VARCHAR2, --复制最高审批序号
                          o_print_auth OUT VARCHAR2 --打印最高审批序号
                          );

  PROCEDURE proc_get_pre_taskid(i_business_id IN VARCHAR2, --业务主键ID
                                o_flag        OUT VARCHAR2, --返回标志
                                o_task_id     OUT VARCHAR2 --taskID
                                );

  PROCEDURE proc_get_subsys_url(i_sys_code IN VARCHAR2, --系统编码
                                o_flag     OUT VARCHAR2, --返回标志
                                o_url      OUT VARCHAR2 --url
                                );
  PROCEDURE proc_insert_dams_mail(i_mail_to  IN VARCHAR2, --主送
                                  i_mail_cc  IN VARCHAR2, --抄送
                                  i_mail_bcc IN VARCHAR2, --密送
                                  i_subject  IN VARCHAR2, --主题
                                  i_content  IN VARCHAR2, --内容
                                  o_flag     OUT VARCHAR2 --0  成功  1 失败
                                  );

  FUNCTION fun_branch_bank(i_branch_id IN VARCHAR2 --所属部门ID
                           ) RETURN VARCHAR2;

  FUNCTION fun_get_branch_lv(i_branch_id IN VARCHAR2) RETURN VARCHAR2;

  PROCEDURE proc_export_auth_apply(i_appstru    IN VARCHAR2,
                                   i_query_type IN VARCHAR2,
                                   i_start_date IN VARCHAR2,
                                   i_end_date   IN VARCHAR2,
                                   i_branch_id  IN VARCHAR2,
                                   o_ret_code   OUT VARCHAR2,
                                   o_apply_stru OUT VARCHAR2,
                                   o_start_date OUT VARCHAR2,
                                   o_end_date   OUT VARCHAR2,
                                   o_auth_apply OUT SYS_REFCURSOR);
END pckg_dams_authapply_process;
//
CREATE OR REPLACE PACKAGE BODY pckg_dams_authapply_process IS

  PROCEDURE proc_get_authapply_info(i_key_id          IN VARCHAR2,
                                    i_task_id         IN VARCHAR2,
                                    i_finish          IN VARCHAR2,
                                    i_process_id      IN VARCHAR2,
                                    o_flag            OUT VARCHAR2,
                                    o_apply_num       OUT VARCHAR2,
                                    o_branch_id       OUT VARCHAR2,
                                    o_branch_name     OUT VARCHAR2,
                                    o_doc_id          OUT VARCHAR2,
                                    o_doc_type        OUT VARCHAR2,
                                    o_doc_title       OUT VARCHAR2,
                                    o_doc_num         OUT VARCHAR2,
                                    o_doc_sec         OUT VARCHAR2,
                                    o_doc_secid       OUT VARCHAR2,
                                    o_read_order      OUT VARCHAR2,
                                    o_copy_order      OUT VARCHAR2,
                                    o_print_order     OUT VARCHAR2,
                                    o_read_auth       OUT VARCHAR2,
                                    o_copy_auth       OUT VARCHAR2,
                                    o_print_auth      OUT VARCHAR2,
                                    o_auth_read_id    OUT VARCHAR2,
                                    o_auth_copy_id    OUT VARCHAR2,
                                    o_auth_print_id   OUT VARCHAR2,
                                    o_auth_read_name  OUT VARCHAR2,
                                    o_auth_copy_name  OUT VARCHAR2,
                                    o_auth_print_name OUT VARCHAR2,
                                    o_apply_start     OUT VARCHAR2,
                                    o_apply_end       OUT VARCHAR2,
                                    o_print_time      OUT VARCHAR2,
                                    o_apply_reason    OUT VARCHAR2,
                                    o_pre_role        OUT VARCHAR2,
                                    o_pre_option      OUT VARCHAR2,
                                    o_pre_date        OUT VARCHAR2,
                                    o_pre_task_name   OUT VARCHAR2,
                                    o_activity_id     OUT VARCHAR2,
                                    o_sys_code        OUT VARCHAR2,
                                    o_own_auth        OUT VARCHAR2,
                                    o_has_doc         OUT VARCHAR2,
                                    o_encrypt_type    OUT VARCHAR2,
                                    o_ic2_print_times OUT VARCHAR2,
                                    o_ret_list        OUT SYS_REFCURSOR,
                                    o_option_chief    OUT ref_t_item_option,
                                    o_option_manager  OUT ref_t_item_option,
                                    o_option_governor OUT ref_t_item_option) IS
    v_count       VARCHAR2(4);
    v_old_type    VARCHAR2(20);
    v_old_doc_id  VARCHAR2(20);
    v_business_id VARCHAR2(20);
    v_header      clob;

  BEGIN
    o_flag := '0';

    SELECT task_name, to_char(create_time, 'yyyy-mm-dd hh24:mi:ss'), NAME,business_id
      INTO o_pre_task_name, o_pre_date, o_pre_role,v_business_id
      FROM (SELECT wt.task_name, wt.create_time, u.name,wt.business_id
              FROM dams_wf_task wt, dams_user u
             WHERE wt.task_id = i_task_id
               AND wt.ssic_id = u.ssic_id

            UNION
            SELECT wht.task_name, wht.create_time, u.name,wht.business_id
              FROM dams_wf_hist_task wht, dams_user u
             WHERE wht.task_id = i_task_id
               AND wht.ssic_id = u.ssic_id) t;

    IF i_finish = '1' THEN

      o_activity_id := '';

      SELECT ai.apply_id,
             ai.branch_id,
             b1.stru_sname,
             ai.doc_id,
             ai.doc_title,
             ai.doc_num,
             ai.secure_id,
             sdb.secure_degree,
             ai.max_order_c,
             ai.max_order_p,
             ai.copy_auth,
             ai.print_auth,
             ai.authorized_person_c,
             ai.authorized_person_p,
             ai.apply_start_date,
             ai.apply_end_date,
             ai.print_time,
             ai.apply_reason,
             ai.file_header,
             ai.doc_type,
             ai.read_auth,
             ai.authorized_person_r,
             ai.max_order_r,
             ai.sys_code,
             ai.own_auth,
             ai.has_doc,
             ai.encrypt_type,
             ai.ic2_print_times
        INTO o_apply_num,
             o_branch_id,
             o_branch_name,
             o_doc_id,
             o_doc_title,
             o_doc_num,
             o_doc_secid,
             o_doc_sec,
             o_copy_order,
             o_print_order,
             o_copy_auth,
             o_print_auth,
             o_auth_copy_id,
             o_auth_print_id,
             o_apply_start,
             o_apply_end,
             o_print_time,
             o_apply_reason,
             v_header,
             o_doc_type,
             o_read_auth,
             o_auth_read_id,
             o_read_order,
             o_sys_code,
             o_own_auth,
             o_has_doc,
             o_encrypt_type,
             o_ic2_print_times
        FROM dams_authorityapply_archve ai,
             dams_branch                b1,
             dams_secure_degree_base    sdb
       WHERE ai.key_id = i_key_id
         AND b1.stru_id = ai.branch_id
         AND sdb.secure_id = ai.secure_id;

    ELSE

      SELECT wac.id
        INTO o_activity_id
        FROM dams_wf_activity_conf wac,
             (SELECT wt.task_name
                FROM dams_wf_task wt
               WHERE wt.prev_task_id = i_task_id
                 AND wt.business_id = v_business_id
              UNION
              SELECT wht.task_name
                FROM dams_wf_hist_task wht
               WHERE wht.prev_task_id = i_task_id
                 AND wht.business_id = v_business_id) t
       WHERE t.task_name = wac.activity_name
         AND wac.pdid = i_process_id;
      SELECT ai.apply_id,
             ai.branch_id,
             b1.stru_sname,
             ai.doc_id,
             ai.doc_title,
             ai.doc_num,
             ai.secure_id,
             sdb.secure_degree,
             ai.max_order_c,
             ai.max_order_p,
             ai.copy_auth,
             ai.print_auth,
             ai.authorized_person_c,
             ai.authorized_person_p,
             ai.apply_start_date,
             ai.apply_end_date,
             ai.print_time,
             ai.apply_reason,
             ai.file_header,
             ai.doc_type,
             ai.read_auth,
             ai.authorized_person_r,
             ai.max_order_r,
             ai.sys_code,
             ai.own_auth,
             ai.has_doc,
             ai.encrypt_type,
             ai.ic2_print_times
        INTO o_apply_num,
             o_branch_id,
             o_branch_name,
             o_doc_id,
             o_doc_title,
             o_doc_num,
             o_doc_secid,
             o_doc_sec,
             o_copy_order,
             o_print_order,
             o_copy_auth,
             o_print_auth,
             o_auth_copy_id,
             o_auth_print_id,
             o_apply_start,
             o_apply_end,
             o_print_time,
             o_apply_reason,
             v_header,
             o_doc_type,
             o_read_auth,
             o_auth_read_id,
             o_read_order,
             o_sys_code,
             o_own_auth,
             o_has_doc,
             o_encrypt_type,
             o_ic2_print_times
        FROM dams_authorityapply_info ai,
             dams_branch              b1,
             dams_secure_degree_base  sdb
       WHERE ai.key_id = i_key_id
         AND b1.stru_id = ai.branch_id
         AND sdb.secure_id = ai.secure_id;

    END IF;



    open o_ret_list for
      select v_header from dual;

    IF o_auth_copy_id IS NULL THEN
      o_auth_copy_name := '';
    ELSE
      SELECT u.name
        INTO o_auth_copy_name
        FROM dams_user u
       WHERE u.ssic_id = o_auth_copy_id;
    END IF;

    IF o_auth_print_id IS NULL THEN
      o_auth_print_name := '';
    ELSE
      SELECT u.name
        INTO o_auth_print_name
        FROM dams_user u
       WHERE u.ssic_id = o_auth_print_id;
    END IF;

    IF o_auth_read_id IS NULL THEN
      o_auth_read_name := '';
    ELSE
      SELECT u.name
        INTO o_auth_read_name
        FROM dams_user u
       WHERE u.ssic_id = o_auth_read_id;
    END IF;

    SELECT MAX(coment)
      INTO o_pre_option
      FROM (SELECT ac.coment, row_number() over(ORDER BY ac.cret_date DESC) rn
              FROM dams_authorityapply_comment ac
             WHERE ac.doc_id = i_key_id
               AND ac.point_name = o_pre_task_name)
     WHERE rn = 1;
    --end

    IF substr(i_process_id, 1, length(i_process_id) - 1) = 'QXSQ-' THEN
      OPEN o_option_chief FOR
        SELECT u.name,
               u.office_phone1,
               ac.coment,
               to_char(to_date(ac.cret_date, 'yyyymmddhh24miss'),
                       'yyyy-mm-dd hh24:mi:ss') option_date
          FROM dams_authorityapply_comment ac, dams_user u
         WHERE ac.doc_id = i_key_id
           AND ac.role_id = 'DR20000013'
           AND u.ssic_id = ac.user_id
           AND ac.coment IS NOT NULL
           AND ac.point_name <> '提交申请'
        UNION ALL
        SELECT u.name,
               u.office_phone1,
               ac.coment,
               to_char(to_date(ac.cret_date, 'yyyymmddhh24miss'),
                       'yyyy-mm-dd hh24:mi:ss') option_date
          FROM dams_authorityapply_comment ac, dams_user u
         WHERE ac.doc_id = i_key_id
           AND ac.role_id = 'DR20000014'
           AND ac.point_name <> '提交申请'
           AND ac.coment IS NOT NULL
           AND u.ssic_id = ac.user_id
         ORDER BY option_date;
      OPEN o_option_manager FOR
        SELECT u.name,
               u.office_phone1,
               ac.coment,
               to_char(to_date(ac.cret_date, 'yyyymmddhh24miss'),
                       'yyyy-mm-dd hh24:mi:ss') option_date
          FROM dams_authorityapply_comment ac, dams_user u
         WHERE ac.doc_id = i_key_id
           AND ac.role_id = 'DR20000022'
           AND ac.point_name <> '提交申请'
           AND ac.coment IS NOT NULL
           AND u.ssic_id = ac.user_id
        UNION ALL
        SELECT u.name,
               u.office_phone1,
               ac.coment,
               to_char(to_date(ac.cret_date, 'yyyymmddhh24miss'),
                       'yyyy-mm-dd hh24:mi:ss') option_date
          FROM dams_authorityapply_comment ac, dams_user u
         WHERE ac.doc_id = i_key_id
           AND ac.role_id = 'DR20000023'
           AND ac.point_name <> '提交申请'
           AND ac.coment IS NOT NULL
           AND u.ssic_id = ac.user_id
         ORDER BY option_date;
      OPEN o_option_governor FOR
        SELECT u.name,
               u.office_phone1,
               ac.coment,
               to_char(to_date(ac.cret_date, 'yyyymmddhh24miss'),
                       'yyyy-mm-dd hh24:mi:ss') option_date
          FROM dams_authorityapply_comment ac, dams_user u
         WHERE ac.doc_id = i_key_id
           AND ac.role_id = 'DR20000024'
           AND ac.point_name <> '提交申请'
           AND ac.coment IS NOT NULL
           AND u.ssic_id = ac.user_id
        UNION ALL
        SELECT u.name,
               u.office_phone1,
               ac.coment,
               to_char(to_date(ac.cret_date, 'yyyymmddhh24miss'),
                       'yyyy-mm-dd hh24:mi:ss') option_date
          FROM dams_authorityapply_comment ac, dams_user u
         WHERE ac.doc_id = i_key_id
           AND ac.role_id = 'DR20000025'
           AND ac.point_name <> '提交申请'
           AND ac.coment IS NOT NULL
           AND u.ssic_id = ac.user_id
         ORDER BY option_date;
    ELSIF substr(i_process_id, 1, length(i_process_id) - 1) = 'AUTHAPPLY-' THEN
      OPEN o_option_manager FOR
        SELECT u.name,
               u.office_phone1,
               ac.coment,
               to_char(to_date(ac.cret_date, 'yyyymmddhh24miss'),
                       'yyyy-mm-dd hh24:mi:ss') option_date
          FROM dams_authorityapply_comment ac, dams_user u
         WHERE ac.doc_id = i_key_id
           AND ac.role_id = 'DR20000015'
           AND ac.point_name <> '提交申请'
           AND ac.coment IS NOT NULL
           AND u.ssic_id = ac.user_id
        UNION ALL
        SELECT u.name,
               u.office_phone1,
               ac.coment,
               to_char(to_date(ac.cret_date, 'yyyymmddhh24miss'),
                       'yyyy-mm-dd hh24:mi:ss') option_date
          FROM dams_authorityapply_comment ac, dams_user u
         WHERE ac.doc_id = i_key_id
           AND ac.role_id = 'DR20000016'
           AND ac.point_name <> '提交申请'
           AND ac.coment IS NOT NULL
           AND u.ssic_id = ac.user_id
         ORDER BY option_date;
      OPEN o_option_governor FOR
        SELECT u.name,
               u.office_phone1,
               ac.coment,
               to_char(to_date(ac.cret_date, 'yyyymmddhh24miss'),
                       'yyyy-mm-dd hh24:mi:ss') option_date
          FROM dams_authorityapply_comment ac, dams_user u
         WHERE ac.doc_id = i_key_id
           AND ac.role_id = 'DR20000020'
           AND ac.point_name <> '提交申请'
           AND ac.coment IS NOT NULL
           AND u.ssic_id = ac.user_id
        UNION ALL
        SELECT u.name,
               u.office_phone1,
               ac.coment,
               to_char(to_date(ac.cret_date, 'yyyymmddhh24miss'),
                       'yyyy-mm-dd hh24:mi:ss') option_date
          FROM dams_authorityapply_comment ac, dams_user u
         WHERE ac.doc_id = i_key_id
           AND ac.role_id = 'DR20000021'
           AND ac.point_name <> '提交申请'
           AND ac.coment IS NOT NULL
           AND u.ssic_id = ac.user_id
         ORDER BY option_date;

    ELSIF substr(i_process_id, 1, length(i_process_id) - 1) = 'QXSQBR-' THEN
      OPEN o_option_manager FOR
        SELECT u.name,
               u.office_phone1,
               ac.coment,
               to_char(to_date(ac.cret_date, 'yyyymmddhh24miss'),
                       'yyyy-mm-dd hh24:mi:ss') option_date
          FROM dams_authorityapply_comment ac, dams_user u
         WHERE ac.doc_id = i_key_id
           AND ac.role_id IN ('DR20000007', 'DR20000005')
           AND ac.point_name <> '提交申请'
           AND ac.coment IS NOT NULL
           AND u.ssic_id = ac.user_id
        UNION ALL
        SELECT u.name,
               u.office_phone1,
               ac.coment,
               to_char(to_date(ac.cret_date, 'yyyymmddhh24miss'),
                       'yyyy-mm-dd hh24:mi:ss') option_date
          FROM dams_authorityapply_comment ac, dams_user u
         WHERE ac.doc_id = i_key_id
           AND ac.role_id IN ('DR20000008', 'DR20000006')
           AND ac.point_name <> '提交申请'
           AND ac.coment IS NOT NULL
           AND u.ssic_id = ac.user_id
         ORDER BY option_date;
      OPEN o_option_governor FOR
        SELECT u.name,
               u.office_phone1,
               ac.coment,
               to_char(to_date(ac.cret_date, 'yyyymmddhh24miss'),
                       'yyyy-mm-dd hh24:mi:ss') option_date
          FROM dams_authorityapply_comment ac, dams_user u
         WHERE ac.doc_id = i_key_id
           AND ac.role_id = 'DR20000011'
           AND ac.point_name <> '提交申请'
           AND ac.coment IS NOT NULL
           AND u.ssic_id = ac.user_id
        UNION ALL
        SELECT u.name,
               u.office_phone1,
               ac.coment,
               to_char(to_date(ac.cret_date, 'yyyymmddhh24miss'),
                       'yyyy-mm-dd hh24:mi:ss') option_date
          FROM dams_authorityapply_comment ac, dams_user u
         WHERE ac.doc_id = i_key_id
           AND ac.role_id = 'DR20000012'
           AND ac.point_name <> '提交申请'
           AND ac.coment IS NOT NULL
           AND u.ssic_id = ac.user_id
         ORDER BY option_date;

    ELSE

      OPEN o_option_manager FOR
        SELECT u.name,
               u.office_phone1,
               ac.coment,
               to_char(to_date(ac.cret_date, 'yyyymmddhh24miss'),
                       'yyyy-mm-dd hh24:mi:ss') option_date
          FROM dams_authorityapply_comment ac, dams_user u
         WHERE ac.doc_id = i_key_id
           AND ac.role_id IN ('DR20000035', 'DR20000036')
           AND ac.point_name <> '提交申请'
           AND ac.coment IS NOT NULL
           AND u.ssic_id = ac.user_id;
      OPEN o_option_governor FOR
        SELECT u.name,
               u.office_phone1,
               ac.coment,
               to_char(to_date(ac.cret_date, 'yyyymmddhh24miss'),
                       'yyyy-mm-dd hh24:mi:ss') option_date
          FROM dams_authorityapply_comment ac, dams_user u
         WHERE ac.doc_id = i_key_id
           AND ac.role_id IN ('DR20000037', 'DR20000038')
           AND ac.point_name <> '提交申请'
           AND ac.coment IS NOT NULL
           AND u.ssic_id = ac.user_id
         ORDER BY option_date;

     

    END IF;

  EXCEPTION
    WHEN OTHERS THEN

      IF o_option_chief%ISOPEN THEN
        CLOSE o_option_chief;
      END IF;
      IF o_option_manager%ISOPEN THEN
        CLOSE o_option_manager;
      END IF;
      IF o_option_governor%ISOPEN THEN
        CLOSE o_option_governor;
      END IF;
      o_flag := -1;
      pack_log.error('PCKG_DAMS_AUTHAPPLY_PROCESS.proc_get_authapply_info',
                     pack_log.end_step, '异常！');
      RETURN;
  END proc_get_authapply_info;

 
  PROCEDURE proc_insert_auth_apply(i_edit_type        IN VARCHAR2,
                                   i_change_auth      IN VARCHAR2,
                                   i_bussiness_id     IN VARCHAR2,
                                   i_move_flag        IN VARCHAR2,
                                   i_file_type        IN VARCHAR2,
                                   i_apply_person_id  IN VARCHAR2,
                                   i_branch_id        IN VARCHAR2,
                                   i_doc_type         IN VARCHAR2,
                                   i_doc_id           IN VARCHAR2,
                                   i_sec_id           IN VARCHAR2,
                                   i_doc_num          IN VARCHAR2,
                                   i_doc_title        IN VARCHAR2,
                                   i_file_header      IN CLOB,
                                   i_read_order       IN VARCHAR2,
                                   i_copy_order       IN VARCHAR2,
                                   i_print_order      IN VARCHAR2,
                                   i_read_auth        IN VARCHAR2,
                                   i_copy_auth        IN VARCHAR2,
                                   i_print_auth       IN VARCHAR2,
                                   i_ar_person        IN VARCHAR2,
                                   i_ac_person        IN VARCHAR2,
                                   i_ap_person        IN VARCHAR2,
                                   i_apply_start      IN VARCHAR2,
                                   i_apply_end        IN VARCHAR2,
                                   i_print_time       IN VARCHAR2,
                                   i_apply_reason     IN VARCHAR2,
                                   i_upd_person       IN VARCHAR2,
                                   i_option           IN VARCHAR2,
                                   i_sys_code         IN VARCHAR2,
                                   i_finish           IN VARCHAR2,
                                   i_point_name       IN VARCHAR2,
                                   i_com_role_id      IN VARCHAR2,
                                   i_drop_flag        IN VARCHAR2,
                                   i_own_auth         IN VARCHAR2,
                                   i_has_doc          IN VARCHAR2,
                                   i_ic2_print_times  IN VARCHAR2,
                                   i_encrypt_type     IN VARCHAR2,
                                   i_from_flag        IN VARCHAR2,
                                   o_flag             OUT VARCHAR2,
                                   o_m_flag           OUT VARCHAR2,
                                   o_jbpm_business_id OUT VARCHAR2) IS
    v_apply_id        dams_authorityapply_archve.apply_id%TYPE;
    v_file_type       dams_authorityapply_archve.file_type%TYPE;
    v_apply_person_id dams_authorityapply_archve.apply_person_id%TYPE;
    v_branch_id       dams_authorityapply_archve.branch_id%TYPE;
    v_doc_type        dams_authorityapply_archve.doc_type%TYPE;
    v_doc_id          dams_authorityapply_archve.doc_id%TYPE;
    v_apply_reason    dams_authorityapply_archve.apply_reason%TYPE;
    v_doc_title       dams_authorityapply_archve.doc_title%TYPE;
    v_doc_num         dams_authorityapply_archve.doc_num%TYPE;
    v_sec_id          dams_authorityapply_archve.secure_id%TYPE;
    v_file_header     dams_authorityapply_archve.file_header%TYPE;
    v_read_order      dams_authorityapply_archve.max_order_r%TYPE;
    v_copy_order      dams_authorityapply_archve.max_order_c%TYPE;
    v_print_order     dams_authorityapply_archve.max_order_p%TYPE;
    v_ar_person       dams_authorityapply_archve.authorized_person_r%TYPE;
    v_ac_person       dams_authorityapply_archve.authorized_person_c%TYPE;
    v_ap_person       dams_authorityapply_archve.authorized_person_p%TYPE;
    v_has_doc         dams_authorityapply_archve.has_doc%TYPE;
    v_encrypt_type    dams_authorityapply_archve.encrypt_type%TYPE;
    v_branch          VARCHAR2(20);
    v_msg             VARCHAR2(20);
    v_sys_code        VARCHAR2(20);
    v_own_auth        VARCHAR2(20);
    v_dams_conut      VARCHAR2(20);
  BEGIN
    o_flag   := '0';
    o_m_flag := i_move_flag;
    IF i_edit_type = '0' THEN
      SELECT 'PL' || to_char(SYSDATE, 'yyyymmdd') ||
             lpad(dams_authorityapply_info_seq.nextval, 10, '0')
        INTO o_jbpm_business_id
        FROM dual;

     
      v_apply_id := pckg_dams_util.func_gen_seq('QXSQ', 'DAMS_QXSQ_SEQ');

      INSERT INTO dams_authorityapply_info
        (key_id,
         apply_id,
         file_type,
         apply_person_id,
         branch_id,
         doc_type,
         doc_id,
         read_auth,
         copy_auth,
         print_auth,
         apply_start_date,
         apply_end_date,
         print_time,
         apply_reason,
         cret_date,
         upd_date,
         upd_user,
         secure_id,
         doc_title,
         doc_num,
         file_header,
         max_order_r,
         max_order_c,
         max_order_p,
         authorized_person_r,
         authorized_person_c,
         authorized_person_p,
         sys_code,
         own_auth,
         has_doc,
         encrypt_type,
         ic2_print_times)
      VALUES
        (o_jbpm_business_id,
         v_apply_id,
         i_file_type,
         i_apply_person_id,
         i_branch_id,
         i_doc_type,
         i_doc_id,
         i_read_auth,
         i_copy_auth,
         i_print_auth,
         i_apply_start,
         i_apply_end,
         i_print_time,
         i_apply_reason,
         to_char(SYSDATE, 'yyyymmddhh24miss'),
         '',
         '',
         i_sec_id,
         i_doc_title,
         i_doc_num,
         i_file_header,
         i_read_order,
         i_copy_order,
         i_print_order,
         i_ar_person,
         i_ac_person,
         i_ap_person,
         i_sys_code,
         i_own_auth,
         i_has_doc,
         i_encrypt_type,
         i_ic2_print_times);
      INSERT INTO dams_authorityapply_comment
        (key_id,
         user_id,
         doc_id,
         point_name,
         role_id,
         bnch_id,
         coment,
         type_code,
         cret_date)
      VALUES
        ('PL' || to_char(SYSDATE, 'yyyymmdd') ||
         lpad(dams_authapply_comment_seq.nextval, 10, '0'),
         i_apply_person_id,
         o_jbpm_business_id,
         substr(i_point_name, 1, 100),
         i_com_role_id,
         i_branch_id,
         i_option,
         '01',
         to_char(SYSDATE, 'yyyymmddhh24miss'));

      IF i_finish = '1' THEN

        SELECT ai.apply_id,
               ai.file_type,
               ai.apply_person_id,
               ai.branch_id,
               ai.doc_type,
               ai.doc_id,
               ai.apply_reason,
               ai.secure_id,
               ai.doc_title,
               ai.doc_num,
               ai.file_header,
               ai.max_order_r,
               ai.max_order_c,
               ai.max_order_p,
               ai.authorized_person_r,
               ai.authorized_person_c,
               ai.authorized_person_p,
               ai.sys_code,
               ai.own_auth,
               ai.encrypt_type,
               ai.has_doc
          INTO v_apply_id,
               v_file_type,
               v_apply_person_id,
               v_branch_id,
               v_doc_type,
               v_doc_id,
               v_apply_reason,
               v_sec_id,
               v_doc_title,
               v_doc_num,
               v_file_header,
               v_read_order,
               v_copy_order,
               v_print_order,
               v_ar_person,
               v_ac_person,
               v_ap_person,
               v_sys_code,
               v_own_auth,
               v_encrypt_type,
               v_has_doc
          FROM dams_authorityapply_info ai
         WHERE ai.key_id = o_jbpm_business_id;
        INSERT INTO dams_authorityapply_archve
          (key_id,
           apply_id,
           file_type,
           apply_person_id,
           branch_id,
           doc_type,
           doc_id,
           read_auth,
           copy_auth,
           print_auth,
           apply_start_date,
           apply_end_date,
           print_time,
           apply_reason,
           cret_date,
           upd_date,
           upd_user,
           secure_id,
           doc_title,
           doc_num,
           file_header,
           max_order_r,
           max_order_c,
           max_order_p,
           authorized_person_r,
           authorized_person_c,
           authorized_person_p,
           sys_code,
           own_auth,
           has_doc,
           encrypt_type,
           ic2_print_times)
        VALUES
          (o_jbpm_business_id,
           v_apply_id,
           v_file_type,
           v_apply_person_id,
           v_branch_id,
           v_doc_type,
           v_doc_id,
           i_read_auth,
           i_copy_auth,
           i_print_auth,
           i_apply_start,
           i_apply_end,
           i_print_time,
           v_apply_reason,
           to_char(SYSDATE, 'yyyymmddhh24miss'),
           '',
           '',
           v_sec_id,
           v_doc_title,
           v_doc_num,
           v_file_header,
           v_read_order,
           v_copy_order,
           v_print_order,
           i_ar_person,
           i_ac_person,
           i_ap_person,
           v_sys_code,
           v_own_auth,
           v_has_doc,
           v_encrypt_type,
           i_ic2_print_times);

        --同步公文数据
        if i_ic2_print_times > 0 then
          SELECT nvl(t.dept_stru_id,t.stru_id)
            INTO v_branch_id
            FROM dams_branch_catalog t
           WHERE t.stru_id = v_branch_id
             and rownum = 1;

          IF v_doc_type = '003' or v_doc_type = '004' or v_has_doc = '0' THEN
            --收文增加打印权限
            select count(1) into v_dams_conut from dams_print_times_user_rel t where t.doc_id = v_doc_id and t.doc_type = '2' and t.ssic_id = v_apply_person_id and t.dept_id = v_branch_id;
            if v_dams_conut > 0 then
              --已有权限,累加
              update dams_print_times_user_rel t
               set t.print_times = i_ic2_print_times + t.print_times,
                   t.update_time = sysdate,
                   t.update_user = v_apply_person_id
             where t.doc_id = v_doc_id
               and t.ssic_id = v_apply_person_id
               and t.dept_id = v_branch_id
               and t.doc_type = '2';
            else
              --没有权限,新增
              --是否有收文人员、综合岗角色
              select count(1)
                into v_dams_conut
                from dams_user_role_rel t
               where t.ssic_id = v_apply_person_id
                 and t.role_id in ('DR00000017','DR00000005');

              if v_dams_conut > 0 then
                INSERT INTO dams_print_times_user_rel
                VALUES
                (v_doc_id, '2', v_apply_person_id,v_branch_id, i_ic2_print_times + 1,sysdate,sysdate,v_apply_person_id);
              else
                INSERT INTO dams_print_times_user_rel
                  VALUES
                (v_doc_id, '2', v_apply_person_id,v_branch_id, i_ic2_print_times,sysdate,sysdate,v_apply_person_id);
              end if;
            end if;
          else
            --发文增加打印权限
            select count(1) into v_dams_conut from dams_print_times_user_rel t where t.doc_id = v_doc_id and t.doc_type = '1' and t.ssic_id = v_apply_person_id and t.dept_id = v_branch_id;
            if v_dams_conut > 0 then
              --已有权限,累加
              update dams_print_times_user_rel t
                 set t.print_times = i_ic2_print_times + t.print_times,
                     t.update_time = sysdate,
                     t.update_user = v_apply_person_id
               where t.doc_id = v_doc_id
                 and t.ssic_id = v_apply_person_id
                 and t.dept_id = v_branch_id
                 and t.doc_type = '1';
            else
              --没有权限,新增
              --是否有发文人员角色
              select count(1)
                into v_dams_conut
                from dams_user_role_rel t
               where t.ssic_id = v_apply_person_id
                 and t.role_id in ('DR00000025');

              if v_dams_conut > 0 then
                INSERT INTO dams_print_times_user_rel
                VALUES
                (v_doc_id, '1', v_apply_person_id,v_branch_id, i_ic2_print_times + 1,sysdate,sysdate,v_apply_person_id);
              else
                INSERT INTO dams_print_times_user_rel
                  VALUES
                (v_doc_id, '1', v_apply_person_id,v_branch_id, i_ic2_print_times,sysdate,sysdate,v_apply_person_id);
              end if;
            end if;
          end if;
        end if;
      END IF;
    ELSE
      o_jbpm_business_id := i_bussiness_id;

      --写入修改日志
      IF i_change_auth = '1' THEN
        INSERT INTO dams_authapply_log
          (key_id,
           business_id,
           point_name,
           copy_auth,
           print_auth,
           print_time,
           start_date,
           end_date,
           upd_person,
           cret_date)
        VALUES
          ('PL' || to_char(SYSDATE, 'yyyymmdd') ||
           lpad(dams_authapply_log_seq.nextval, 10, '0'),
           o_jbpm_business_id,
           substr(i_point_name, 1, 100),
           i_copy_auth,
           i_print_auth,
           i_print_time,
           i_apply_start,
           i_apply_end,
           i_upd_person,
           to_char(SYSDATE, 'yyyymmddhh24miss'));

      END IF;

      IF i_drop_flag = '0' THEN
        UPDATE dams_authorityapply_info
           SET copy_auth           = i_copy_auth,
               file_type           = i_file_type,
               read_auth           = i_read_auth,
               print_auth          = i_print_auth,
               authorized_person_r = i_ar_person,
               authorized_person_c = i_ac_person,
               authorized_person_p = i_ap_person,
               apply_start_date    = i_apply_start,
               apply_end_date      = i_apply_end,
               print_time          = i_print_time,
               upd_date            = to_char(SYSDATE, 'yyyymmddhh24miss'),
               upd_user            = i_upd_person,
               ic2_print_times     = i_ic2_print_times
         WHERE key_id = i_bussiness_id;

        INSERT INTO dams_authorityapply_comment
          (key_id,
           user_id,
           doc_id,
           point_name,
           role_id,
           bnch_id,
           coment,
           type_code,
           cret_date)
        VALUES
          ('PL' || to_char(SYSDATE, 'yyyymmdd') ||
           lpad(dams_authapply_comment_seq.nextval, 10, '0'),
           i_upd_person,
           o_jbpm_business_id,
           substr(i_point_name, 1, 100),
           i_com_role_id,
           i_branch_id,
           i_option,
           '01',
           to_char(SYSDATE, 'yyyymmddhh24miss'));
      ELSE
        UPDATE dams_authorityapply_info
           SET file_type           = i_file_type,
               authorized_person_r = i_ar_person,
               authorized_person_c = i_ac_person,
               authorized_person_p = i_ap_person,
               max_order_r         = i_read_order,
               max_order_c         = i_copy_order,
               max_order_p         = i_print_order
         WHERE key_id = i_bussiness_id;

        INSERT INTO dams_authorityapply_comment
          (key_id,
           user_id,
           doc_id,
           point_name,
           role_id,
           bnch_id,
           coment,
           type_code,
           cret_date)
        VALUES
          ('PL' || to_char(SYSDATE, 'yyyymmdd') ||
           lpad(dams_authapply_comment_seq.nextval, 10, '0'),
           i_upd_person,
           o_jbpm_business_id,
           substr(i_point_name, 1, 100),
           i_com_role_id,
           i_branch_id,
           i_option,
           '01',
           to_char(SYSDATE, 'yyyymmddhh24miss'));

      END IF;

      IF i_finish = '1' THEN

        SELECT ai.apply_id,
               ai.file_type,
               ai.apply_person_id,
               ai.branch_id,
               ai.doc_type,
               ai.doc_id,
               ai.apply_reason,
               ai.secure_id,
               ai.doc_title,
               ai.doc_num,
               ai.file_header,
               ai.max_order_r,
               ai.max_order_c,
               ai.max_order_p,
               ai.authorized_person_r,
               ai.authorized_person_c,
               ai.authorized_person_p,
               ai.sys_code,
               ai.own_auth,
               ai.encrypt_type,
               ai.has_doc
          INTO v_apply_id,
               v_file_type,
               v_apply_person_id,
               v_branch_id,
               v_doc_type,
               v_doc_id,
               v_apply_reason,
               v_sec_id,
               v_doc_title,
               v_doc_num,
               v_file_header,
               v_read_order,
               v_copy_order,
               v_print_order,
               v_ar_person,
               v_ac_person,
               v_ap_person,
               v_sys_code,
               v_own_auth,
               v_encrypt_type,
               v_has_doc
          FROM dams_authorityapply_info ai
         WHERE ai.key_id = o_jbpm_business_id;
        INSERT INTO dams_authorityapply_archve
          (key_id,
           apply_id,
           file_type,
           apply_person_id,
           branch_id,
           doc_type,
           doc_id,
           read_auth,
           copy_auth,
           print_auth,
           apply_start_date,
           apply_end_date,
           print_time,
           apply_reason,
           cret_date,
           upd_date,
           upd_user,
           secure_id,
           doc_title,
           doc_num,
           file_header,
           max_order_r,
           max_order_c,
           max_order_p,
           authorized_person_r,
           authorized_person_c,
           authorized_person_p,
           sys_code,
           own_auth,
           has_doc,
           encrypt_type,
           ic2_print_times)
        VALUES
          (o_jbpm_business_id,
           v_apply_id,
           v_file_type,
           v_apply_person_id,
           v_branch_id,
           v_doc_type,
           v_doc_id,
           i_read_auth,
           i_copy_auth,
           i_print_auth,
           i_apply_start,
           i_apply_end,
           i_print_time,
           v_apply_reason,
           to_char(SYSDATE, 'yyyymmddhh24miss'),
           '',
           '',
           v_sec_id,
           v_doc_title,
           v_doc_num,
           v_file_header,
           v_read_order,
           v_copy_order,
           v_print_order,
           i_ar_person,
           i_ac_person,
           i_ap_person,
           v_sys_code,
           v_own_auth,
           v_has_doc,
           v_encrypt_type,
           i_ic2_print_times);

        --同步公文数据
        if i_ic2_print_times > 0 then
          SELECT nvl(t.dept_stru_id,t.stru_id)
            INTO v_branch_id
            FROM dams_branch_catalog t
           WHERE t.stru_id = v_branch_id
             and rownum = 1;

          IF v_doc_type = '003' or v_doc_type = '004' or v_has_doc = '0' THEN
            --收文增加打印权限
            select count(1) into v_dams_conut from dams_print_times_user_rel t where t.doc_id = v_doc_id and t.doc_type = '2'and t.ssic_id = v_apply_person_id and t.dept_id = v_branch_id;
            if v_dams_conut > 0 then
              --已有权限,累加
              update dams_print_times_user_rel t
               set t.print_times = i_ic2_print_times + t.print_times,
                   t.update_time = sysdate,
                   t.update_user = i_upd_person
             where t.doc_id = v_doc_id
               and t.ssic_id = v_apply_person_id
               and t.dept_id = v_branch_id
               and t.doc_type = '2';
            else
              --没有权限,新增
              --是否有收文人员、综合岗角色
              select count(1)
                into v_dams_conut
                from dams_user_role_rel t
               where t.ssic_id = v_apply_person_id
                 and t.role_id in ('DR00000017','DR00000005');

              if v_dams_conut > 0 then
                INSERT INTO dams_print_times_user_rel
                VALUES
                (v_doc_id, '2', v_apply_person_id,v_branch_id, i_ic2_print_times + 1,sysdate,sysdate,i_upd_person);
              else
                INSERT INTO dams_print_times_user_rel
                  VALUES
                (v_doc_id, '2', v_apply_person_id,v_branch_id, i_ic2_print_times,sysdate,sysdate,i_upd_person);
              end if;
            end if;
          else
            --发文增加打印权限
            select count(1) into v_dams_conut from dams_print_times_user_rel t where t.doc_id = v_doc_id and t.doc_type = '1' and t.ssic_id = v_apply_person_id and t.dept_id = v_branch_id;
            if v_dams_conut > 0 then
              --已有权限,累加
              update dams_print_times_user_rel t
                 set t.print_times = i_ic2_print_times + t.print_times,
                     t.update_time = sysdate,
                     t.update_user = i_upd_person
               where t.doc_id = v_doc_id
                 and t.ssic_id = v_apply_person_id
                 and t.dept_id = v_branch_id
                 and t.doc_type = '1';
            else
              --没有权限,新增
              --是否有发文人员角色
              select count(1)
                into v_dams_conut
                from dams_user_role_rel t
               where t.ssic_id = v_apply_person_id
                 and t.role_id in ('DR00000025');

              if v_dams_conut > 0 then
                INSERT INTO dams_print_times_user_rel
                VALUES
                (v_doc_id, '1', v_apply_person_id,v_branch_id, i_ic2_print_times + 1,sysdate,sysdate,i_upd_person);
              else
                INSERT INTO dams_print_times_user_rel
                  VALUES
                (v_doc_id, '1', v_apply_person_id,v_branch_id, i_ic2_print_times,sysdate,sysdate,i_upd_person);
              end if;
            end if;
          end if;
        end if;
      END IF;
    END IF;

    COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      o_flag := '-1';
      pckg_ctp_lg_public.log('insertAuthApply()', SQLERRM(SQLCODE));
      pack_log.error('PCKG_DAMS_AUTHAPPLY_PROCESS.proc_insert_auth_apply',
                     pack_log.end_step, '异常！');
      RETURN;
  END proc_insert_auth_apply;

 
  PROCEDURE proc_get_pre_taskinfo(i_task_id             IN VARCHAR2,
                                  o_flag                OUT VARCHAR2,
                                  o_pre_task_name       OUT VARCHAR2,
                                  o_pre_task_transition OUT VARCHAR2) IS

  BEGIN
    o_flag := '0';
    SELECT wt1.task_name, wt2.outgoing_name
      INTO o_pre_task_name, o_pre_task_transition
      FROM dams_wf_task wt1, dams_wf_task wt2
     WHERE wt1.task_id = i_task_id
       AND wt2.task_id = wt1.prev_task_id;
  EXCEPTION
    WHEN OTHERS THEN
      o_flag := '-1';
      pack_log.error('PCKG_DAMS_AUTHAPPLY_PROCESS.proc_get_pre_taskinfo',
                     pack_log.end_step, '异常！');
      RETURN;
  END proc_get_pre_taskinfo;

  
  PROCEDURE proc_get_process_auth(i_sec_id           IN VARCHAR2,
                                  i_role_id          IN VARCHAR2,
                                  i_process_id       IN VARCHAR2,
                                  i_branch_lv        IN VARCHAR2,
                                  i_sys_type         IN VARCHAR2,
                                  o_flag             OUT VARCHAR2,
                                  o_task_order_read  OUT ref_t_item_action,
                                  o_task_order_copy  OUT ref_t_item_action,
                                  o_task_order_print OUT ref_t_item_action) IS
    v_minc        VARCHAR2(10);
    v_minp        VARCHAR2(10);
    v_minr        VARCHAR2(10);
    v_maxr        VARCHAR2(10);
    v_maxc        VARCHAR2(10);
    v_maxp        VARCHAR2(10);
    v_minc_tmp    VARCHAR2(10);
    v_minp_tmp    VARCHAR2(10);
    v_minr_tmp    VARCHAR2(10);
    v_template_id VARCHAR2(20);

    v_branch_lv VARCHAR2(2) := i_branch_lv; --20160307 ggc
  BEGIN
    o_flag := '0';

    SELECT nvl(MAX(tsr.template_id), '02')
      INTO v_template_id
      FROM dams_template_sys_rel tsr
     WHERE tsr.syscode = i_sys_type;

    IF i_process_id = 'AUTHAPPLY-1' AND (i_sec_id = '2' OR i_sec_id = '3') THEN

      SELECT MIN(sps.order_code)
        INTO v_minr
        FROM dams_authorityapply_set das, dams_standard_process_model sps
       WHERE das.prm_secret_id = i_sec_id
         AND das.duty_lv_code = i_role_id
         AND das.standard_id = sps.key_id
         AND das.system_type = '01'
         AND das.branch_lv = v_branch_lv
         AND das.apply_type = '1'
         AND sps.biz_type = '009'
         AND sps.order_code <> '0';

      SELECT MIN(sps.order_code)
        INTO v_minc
        FROM dams_authorityapply_set das, dams_standard_process_model sps
       WHERE das.prm_secret_id = i_sec_id
         AND das.duty_lv_code = i_role_id
         AND das.standard_id = sps.key_id
         AND das.system_type = '01'
         AND das.branch_lv = v_branch_lv
         AND das.apply_type = '2'
         AND sps.biz_type = '009'
         AND sps.order_code <> '0';

      SELECT MIN(sps.order_code)
        INTO v_minp
        FROM dams_authorityapply_set das, dams_standard_process_model sps
       WHERE das.prm_secret_id = i_sec_id
         AND das.duty_lv_code = i_role_id
         AND das.standard_id = sps.key_id
         AND das.branch_lv = v_branch_lv
         AND das.system_type = '01'
         AND das.apply_type = '3'
         AND sps.biz_type = '009'
         AND sps.order_code <> '0';

      SELECT sps.order_code
        INTO v_maxr
        FROM dams_authorityapply_set     das,
             dams_standard_process_model sps,
             dams_wf_activity_conf       wac
       WHERE das.prm_secret_id = i_sec_id
         AND das.duty_lv_code = i_role_id
         AND das.standard_id = sps.key_id
         AND das.system_type = v_template_id
         AND das.apply_type = '1'
         AND das.branch_lv = v_branch_lv
         AND sps.biz_type = '009'
         AND sps.open_state <> '3'
         AND sps.order_code <> '999'
         AND wac.id = sps.activity_id
         AND das.upd_time = '99991231';

      IF v_maxr < v_minr THEN
        v_minr_tmp := v_maxr;
      ELSE
        v_minr_tmp := v_minr;
      END IF;

      OPEN o_task_order_read FOR
        SELECT t.order_code, t.activity_name, t.open_state
          FROM (SELECT sps1.order_code, wac.activity_name, sps1.open_state
                  FROM dams_standard_process_model sps1,
                       dams_wf_activity_conf       wac
                 WHERE sps1.biz_type = '009'
                   AND sps1.activity_id = wac.id
                   AND sps1.activity_id LIKE i_process_id || '%'
                   AND sps1.order_code = v_minr_tmp
                UNION ALL
                SELECT sps.order_code, wac.activity_name, sps.open_state
                  FROM dams_authorityapply_set     das,
                       dams_standard_process_model sps,
                       dams_wf_activity_conf       wac
                 WHERE das.prm_secret_id = i_sec_id
                   AND das.duty_lv_code = i_role_id
                   AND das.standard_id = sps.key_id
                   AND das.system_type = v_template_id
                   AND das.apply_type = '1'
                   AND das.branch_lv = v_branch_lv
                   AND sps.biz_type = '009'
                   AND sps.order_code <> '999'
                   AND wac.id = sps.activity_id
                   AND das.upd_time = '99991231') t
         ORDER BY t.order_code;

      SELECT sps.order_code
        INTO v_maxc
        FROM dams_authorityapply_set     das,
             dams_standard_process_model sps,
             dams_wf_activity_conf       wac
       WHERE das.prm_secret_id = i_sec_id
         AND das.duty_lv_code = i_role_id
         AND das.standard_id = sps.key_id
         AND das.system_type = v_template_id
         AND das.apply_type = '2'
         AND das.branch_lv = v_branch_lv
         AND sps.biz_type = '009'
         AND sps.open_state <> '3'
         AND sps.order_code <> '999'
         AND wac.id = sps.activity_id
         AND das.upd_time = '99991231';

      IF v_maxc < v_minc THEN
        v_minc_tmp := v_maxc;
      ELSE
        v_minc_tmp := v_minc;
      END IF;

      OPEN o_task_order_copy FOR
        SELECT t.order_code, t.activity_name, t.open_state
          FROM (SELECT sps1.order_code, wac.activity_name, sps1.open_state
                  FROM dams_standard_process_model sps1,
                       dams_wf_activity_conf       wac
                 WHERE sps1.biz_type = '009'
                   AND sps1.activity_id = wac.id
                   AND sps1.activity_id LIKE i_process_id || '%'
                   AND sps1.order_code = v_minc_tmp
                UNION
                SELECT decode(sps.order_code, '98',
                              to_char(to_number(v_minc_tmp) + 1), sps.order_code) order_code,
                       wac.activity_name,
                       sps.open_state
                  FROM dams_authorityapply_set     das,
                       dams_standard_process_model sps,
                       dams_wf_activity_conf       wac
                 WHERE das.prm_secret_id = i_sec_id
                   AND das.duty_lv_code = i_role_id
                   AND das.standard_id = sps.key_id
                   AND das.system_type = v_template_id
                   AND das.apply_type = '2'
                   AND das.branch_lv = v_branch_lv
                   AND sps.biz_type = '009'
                   AND sps.order_code <> '999'
                   AND wac.id = sps.activity_id
                   AND das.upd_time = '99991231') t
         ORDER BY t.order_code;

      SELECT sps.order_code
        INTO v_maxp
        FROM dams_authorityapply_set     das,
             dams_standard_process_model sps,
             dams_wf_activity_conf       wac
       WHERE das.prm_secret_id = i_sec_id
         AND das.duty_lv_code = i_role_id
         AND das.standard_id = sps.key_id
         AND das.system_type = v_template_id
         AND das.apply_type = '3'
         AND das.branch_lv = v_branch_lv
         AND sps.biz_type = '009'
         AND sps.open_state <> '3'
         AND sps.order_code <> '999'
         AND wac.id = sps.activity_id
         AND das.upd_time = '99991231';

      IF v_maxp < v_minp THEN
        v_minp_tmp := v_maxp;
      ELSE
        v_minp_tmp := v_minp;
      END IF;

      OPEN o_task_order_print FOR
        SELECT t.order_code, t.activity_name, t.open_state
          FROM (SELECT sps1.order_code, wac.activity_name, sps1.open_state
                  FROM dams_standard_process_model sps1,
                       dams_wf_activity_conf       wac
                 WHERE sps1.biz_type = '009'
                   AND sps1.activity_id = wac.id
                   AND sps1.activity_id LIKE i_process_id || '%'
                   AND sps1.order_code = v_minp_tmp
                UNION
                SELECT decode(sps.order_code, '98',
                              to_char(to_number(v_minp_tmp) + 1), sps.order_code) order_code,
                       wac.activity_name,
                       sps.open_state
                  FROM dams_authorityapply_set     das,
                       dams_standard_process_model sps,
                       dams_wf_activity_conf       wac
                 WHERE das.prm_secret_id = i_sec_id
                   AND das.duty_lv_code = i_role_id
                   AND das.standard_id = sps.key_id
                   AND das.system_type = v_template_id
                   AND das.apply_type = '3'
                   AND das.branch_lv = v_branch_lv
                   AND sps.biz_type = '009'
                   AND sps.order_code <> '999'
                   AND wac.id = sps.activity_id
                   AND das.upd_time = '99991231') t
         ORDER BY t.order_code;

    ELSE

      --20160307 ggc: 境外机构 branch_lv = 2
      IF i_process_id = 'QXSQOS-1' THEN
        v_branch_lv := '2';
      END IF;

      SELECT MIN(sps.order_code)
        INTO v_minr
        FROM dams_authorityapply_set das, dams_standard_process_model sps
       WHERE das.prm_secret_id = i_sec_id
         AND das.duty_lv_code = i_role_id
         AND das.standard_id = sps.key_id
         AND das.system_type = '01'
         AND das.branch_lv = v_branch_lv
         AND das.apply_type = '1'
         AND sps.biz_type = '009'
         AND sps.order_code <> '0';

      SELECT MIN(sps.order_code)
        INTO v_minc
        FROM dams_authorityapply_set das, dams_standard_process_model sps
       WHERE das.prm_secret_id = i_sec_id
         AND das.duty_lv_code = i_role_id
         AND das.standard_id = sps.key_id
         AND das.system_type = '01'
         AND das.branch_lv = v_branch_lv
         AND das.apply_type = '2'
         AND sps.biz_type = '009'
         AND sps.order_code <> '0';
      SELECT MIN(sps.order_code)
        INTO v_minp
        FROM dams_authorityapply_set das, dams_standard_process_model sps
       WHERE das.prm_secret_id = i_sec_id
         AND das.duty_lv_code = i_role_id
         AND das.standard_id = sps.key_id
         AND das.branch_lv = v_branch_lv
         AND das.system_type = '01'
         AND das.apply_type = '3'
         AND sps.biz_type = '009'
         AND sps.order_code <> '0';

      SELECT sps.order_code
        INTO v_maxr
        FROM dams_authorityapply_set     das,
             dams_standard_process_model sps,
             dams_wf_activity_conf       wac
       WHERE das.prm_secret_id = i_sec_id
         AND das.duty_lv_code = i_role_id
         AND das.standard_id = sps.key_id
         AND das.system_type = v_template_id
         AND das.apply_type = '1'
         AND das.branch_lv = v_branch_lv
         AND sps.biz_type = '009'
         AND sps.open_state <> '3'
         AND sps.order_code <> '999'
         AND wac.id = sps.activity_id
         AND das.upd_time = '99991231';

      IF v_maxr < v_minr THEN
        v_minr_tmp := v_maxr;
      ELSE
        v_minr_tmp := v_minr;
      END IF;

      SELECT sps.order_code
        INTO v_maxc
        FROM dams_authorityapply_set     das,
             dams_standard_process_model sps,
             dams_wf_activity_conf       wac
       WHERE das.prm_secret_id = i_sec_id
         AND das.duty_lv_code = i_role_id
         AND das.standard_id = sps.key_id
         AND das.system_type = v_template_id
         AND das.apply_type = '2'
         AND das.branch_lv = v_branch_lv
         AND sps.biz_type = '009'
         AND sps.open_state <> '3'
         AND sps.order_code <> '999'
         AND wac.id = sps.activity_id
         AND das.upd_time = '99991231';

      IF v_maxc < v_minc THEN
        v_minc_tmp := v_maxc;
      ELSE
        v_minc_tmp := v_minc;
      END IF;

      SELECT sps.order_code
        INTO v_maxp
        FROM dams_authorityapply_set     das,
             dams_standard_process_model sps,
             dams_wf_activity_conf       wac
       WHERE das.prm_secret_id = i_sec_id
         AND das.duty_lv_code = i_role_id
         AND das.standard_id = sps.key_id
         AND das.system_type = v_template_id
         AND das.apply_type = '3'
         AND das.branch_lv = v_branch_lv
         AND sps.biz_type = '009'
         AND sps.open_state <> '3'
         AND sps.order_code <> '999'
         AND wac.id = sps.activity_id
         AND das.upd_time = '99991231';

      IF v_maxp < v_minp THEN
        v_minp_tmp := v_maxp;
      ELSE
        v_minp_tmp := v_minp;
      END IF;

      SELECT nvl(MAX(tsr.template_id), '02')
        INTO v_template_id
        FROM dams_template_sys_rel tsr
       WHERE tsr.syscode = i_sys_type;

      OPEN o_task_order_read FOR
        SELECT t.order_code, t.activity_name, t.open_state
          FROM (SELECT sps1.order_code, wac.activity_name, sps1.open_state
                  FROM dams_standard_process_model sps1,
                       dams_wf_activity_conf       wac
                 WHERE sps1.biz_type = '009'
                   AND sps1.activity_id = wac.id
                   AND sps1.activity_id LIKE i_process_id || '%'
                   AND sps1.order_code = v_minr_tmp
                UNION ALL
                SELECT sps.order_code, wac.activity_name, sps.open_state
                  FROM dams_authorityapply_set     das,
                       dams_standard_process_model sps,
                       dams_wf_activity_conf       wac
                 WHERE das.prm_secret_id = i_sec_id
                   AND das.duty_lv_code = i_role_id
                   AND das.standard_id = sps.key_id
                   AND das.system_type = v_template_id
                   AND das.apply_type = '1'
                   AND das.branch_lv = v_branch_lv
                   AND sps.biz_type = '009'
                   AND sps.order_code <> '999'
                   AND wac.id = sps.activity_id
                   AND das.upd_time = '99991231') t
         ORDER BY t.order_code;

      OPEN o_task_order_copy FOR
        SELECT t.order_code, t.activity_name, t.open_state
          FROM (SELECT sps1.order_code, wac.activity_name, sps1.open_state
                  FROM dams_standard_process_model sps1,
                       dams_wf_activity_conf       wac
                 WHERE sps1.biz_type = '009'
                   AND sps1.activity_id = wac.id
                   AND sps1.activity_id LIKE i_process_id || '%'
                   AND sps1.order_code = v_minc_tmp
                UNION ALL
                SELECT sps.order_code, wac.activity_name, sps.open_state
                  FROM dams_authorityapply_set     das,
                       dams_standard_process_model sps,
                       dams_wf_activity_conf       wac
                 WHERE das.prm_secret_id = i_sec_id
                   AND das.duty_lv_code = i_role_id
                   AND das.standard_id = sps.key_id
                   AND das.system_type = v_template_id
                   AND das.apply_type = '2'
                   AND das.branch_lv = v_branch_lv
                   AND sps.biz_type = '009'
                   AND sps.order_code <> '999'
                   AND wac.id = sps.activity_id
                   AND das.upd_time = '99991231') t
         ORDER BY t.order_code;

      OPEN o_task_order_print FOR
        SELECT t.order_code, t.activity_name, t.open_state
          FROM (SELECT sps1.order_code, wac.activity_name, sps1.open_state
                  FROM dams_standard_process_model sps1,
                       dams_wf_activity_conf       wac
                 WHERE sps1.biz_type = '009'
                   AND sps1.activity_id = wac.id
                   AND sps1.activity_id LIKE i_process_id || '%'
                   AND sps1.order_code = v_minp_tmp
                UNION ALL
                SELECT sps.order_code, wac.activity_name, sps.open_state
                  FROM dams_authorityapply_set     das,
                       dams_standard_process_model sps,
                       dams_wf_activity_conf       wac
                 WHERE das.prm_secret_id = i_sec_id
                   AND das.duty_lv_code = i_role_id
                   AND das.standard_id = sps.key_id
                   AND das.system_type = v_template_id
                   AND das.apply_type = '3'
                   AND das.branch_lv = v_branch_lv
                   AND sps.biz_type = '009'
                   AND sps.order_code <> '999'
                   AND wac.id = sps.activity_id
                   AND das.upd_time = '99991231') t
         ORDER BY t.order_code;

    END IF;

  EXCEPTION
    WHEN OTHERS THEN
      IF o_task_order_read%ISOPEN THEN
        CLOSE o_task_order_read;
      END IF;
      IF o_task_order_copy%ISOPEN THEN
        CLOSE o_task_order_copy;
      END IF;
      IF o_task_order_print%ISOPEN THEN
        CLOSE o_task_order_print;
      END IF;
      o_flag := '-1';
      pack_log.error('PCKG_DAMS_AUTHAPPLY_PROCESS.proc_get_process_auth',
                     pack_log.end_step, '异常！');
      RETURN;
  END proc_get_process_auth;

  
  PROCEDURE proc_get_role_info(i_starter_id    IN VARCHAR2, --申请人ID
                               i_branch_id     IN VARCHAR2, --申请人机构
                               i_cur_id        IN VARCHAR2, --当前处理人ID
                               i_cur_task      IN VARCHAR2, --当前活动ID
                               i_sec_id        IN VARCHAR2, --文档ID
                               o_flag          OUT VARCHAR2, --返回标志
                               o_doc_sec       OUT VARCHAR2, --密级
                               o_branch_lv     OUT VARCHAR2, --机构层级
                               o_start_lv      OUT VARCHAR2, --申请人职务层级编码
                               o_start_ad      OUT VARCHAR2, --申请人AD帐号
                               o_start_notesid OUT VARCHAR2, --申请人notesID
                               o_start_tele    OUT VARCHAR2, --申请人电话
                               o_stru_name     OUT VARCHAR2, --拟稿人机构部室名称
                               o_stru_id       OUT VARCHAR2, --拟稿人机构部室ID
                               o_cur_lv        OUT VARCHAR2, --当前处理人职务层级编码
                               o_cur_order     OUT VARCHAR2, --当前处理序号
                               o_process_id    OUT VARCHAR2, --流程名称
                               o_apply_stru    OUT VARCHAR2 --申请机构ID
                               ) IS
    v_branch_lv VARCHAR2(3);
    v_branch_id VARCHAR2(20);
    v_sign      VARCHAR2(20);
    v_cur_stru  dams_branch.stru_id%TYPE;
    v_param     db_log.info%TYPE := i_starter_id || '|' || i_branch_id || '|' ||
                                    i_sec_id || '|' || i_cur_id || '|' ||
                                    i_cur_task;

  BEGIN

    o_flag := '0';

    SELECT MAX(t.stru_id)
      INTO o_apply_stru
      FROM (SELECT stru_id
              FROM dams_user
             WHERE ssic_id = i_starter_id
            UNION
            SELECT stru_id
              FROM dams_user_branch_rel
             WHERE ssic_id = i_starter_id) t,
           dams_branch_catalog bc,
           dams_branch_catalog bc1
     WHERE t.stru_id = bc.stru_id
       AND bc.dept_stru_id = bc1.dept_stru_id
       AND bc1.stru_id = i_branch_id;

    IF o_apply_stru IS NULL THEN
      SELECT MAX(t.stru_id)
        INTO o_apply_stru
        FROM (SELECT stru_id
                FROM dams_user
               WHERE ssic_id = i_starter_id
              UNION
              SELECT stru_id
                FROM dams_user_branch_rel
               WHERE ssic_id = i_starter_id) t,
             dams_branch_catalog bc,
             dams_branch_catalog bc1
       WHERE t.stru_id = bc.stru_id
         AND bc.major_stru_id = bc1.major_stru_id
         AND bc1.stru_id = i_branch_id;
    END IF;

    IF o_apply_stru IS NULL THEN
      o_apply_stru := i_branch_id;
    END IF;

    SELECT MAX(nvl(bc.major_stru_id, bc.major_stru_id)),
           MAX(nvl(bc.stru_level, bc.stru_level)),
           MAX(nvl(b.stru_sname, b1.stru_sname)),
           MAX(nvl(b.stru_sign, b1.stru_sign)),
           MAX(nvl(b.stru_id, b1.stru_id))
      INTO v_branch_id, v_branch_lv, o_stru_name, v_sign, o_stru_id
      FROM dams_branch_catalog bc, dams_branch b, dams_branch b1
     WHERE bc.stru_id = o_apply_stru
       --AND b.stru_id(+) = bc.dept_stru_id
       AND bc.lead_stru_id = b1.stru_id(+);

    IF v_branch_lv > '3' THEN
      v_branch_lv := '3';
    END IF;

    o_branch_lv := v_branch_lv;
    IF v_sign IN ('8', '18') THEN
      --20160223 ggc: 境外机构都走QXQSOS流程
      o_process_id := 'QXSQOS';
    ELSE
      IF v_branch_lv = '1' THEN
        o_process_id := 'QXSQ';
      ELSIF v_branch_lv = '2' THEN
        IF v_sign IN ('8', '18') THEN
          o_process_id := 'QXSQOS';
        ELSE
          o_process_id := 'AUTHAPPLY';
        END IF;
      ELSIF v_branch_lv = '3' THEN
        o_process_id := 'QXSQBR';
      ELSE
        o_process_id := 'QXSQBRSUB';
      END IF;
    END IF;

    IF i_cur_task IS NOT NULL THEN
      SELECT sps.order_code
        INTO o_cur_order
        FROM dams_standard_process_model sps
       WHERE sps.activity_id = i_cur_task
         AND sps.biz_type = '009';
    END IF;

    SELECT MAX(sdb.secure_degree)
      INTO o_doc_sec
      FROM dams_secure_degree_base sdb
     WHERE sdb.secure_id = i_sec_id;

    SELECT coalesce(MAX(u.office_phone1), MAX(u.mobile_phone1),
                    MAX(u.office_phone2)) mobile_phone1,

           CASE
             --WHEN pckg_doc.fun_branch_authtype(i_branch_id) = '1' THEN
             WHEN o_start_tele= '1' THEN
              MAX(u.ad)
             ELSE
              MAX(u.ssic_id)
           END,

           MAX(u.notes_id)
      INTO o_start_tele, o_start_ad, o_start_notesid
      FROM dams_user u
     WHERE u.ssic_id = i_starter_id;

    SELECT nvl(MAX(urr.role_id),
               decode(v_branch_lv, '3', 'DR20000003', '2',
                       decode(v_sign, '8', 'DR20000034', '18', 'DR20000034',
                               'DR20000001'), 'DR20000001'))
      INTO o_start_lv
      FROM dams_user_role_rel urr
     WHERE urr.stru_id = o_apply_stru
       AND urr.sys_code = 'QXSQ'
       AND urr.auth_state IN ('0', '1')
       AND urr.role_id NOT IN ('DR20000099', 'DR20000026', 'DR20000027')
       AND urr.ssic_id = i_starter_id
       AND EXISTS
     (SELECT 1 FROM dams_qxsq_role qr WHERE urr.role_id = qr.role_id);

    SELECT MAX(t.stru_id)
      INTO v_cur_stru
      FROM (SELECT stru_id
              FROM dams_user
             WHERE ssic_id = i_cur_id
            UNION
            SELECT stru_id
              FROM dams_user_branch_rel
             WHERE ssic_id = i_cur_id) t,
           dams_branch_catalog bc,
           dams_branch_catalog bc1
     WHERE t.stru_id = bc.stru_id
       AND bc.major_stru_id = bc1.major_stru_id
       AND bc1.stru_id = i_branch_id;

    SELECT nvl(MAX(urr.role_id), 'DR20000001')
      INTO o_cur_lv
      FROM dams_user_role_rel urr
     WHERE urr.ssic_id = i_cur_id
       AND urr.stru_id = v_cur_stru
       AND urr.sys_code = 'QXSQ'
       AND urr.auth_state IN ('0', '1')
       AND urr.role_id NOT IN ('DR20000099', 'DR20000026', 'DR20000027')
       AND EXISTS
     (SELECT 1 FROM dams_qxsq_role qr WHERE qr.role_id = urr.role_id);

  EXCEPTION
    WHEN OTHERS THEN
      o_flag := '-1';
      pack_log.error('PCKG_DAMS_AUTHAPPLY_PROCESS.proc_get_role_info',
                     pack_log.end_step, '异常！' || v_param);
      RETURN;
  END proc_get_role_info;

  PROCEDURE proc_get_next_steps(i_edit_type      IN VARCHAR2, --是否可编辑
                                i_process_id     IN VARCHAR2, --流程ID
                                i_task_name      IN VARCHAR2, --活动名称
                                i_min_order      IN VARCHAR2, --开始审批序号
                                i_max_order      IN VARCHAR2, --最高审批序号
                                i_max_state      IN VARCHAR2, --最高审判状态
                                o_flag           OUT VARCHAR2, --返回标志
                                o_next_step_list OUT ref_t_item_step) IS
    v_order     VARCHAR2(10);
    v_count     VARCHAR2(4);
    v_max_order VARCHAR2(10);
  BEGIN
    o_flag := '0';

    IF i_process_id = 'AUTHAPPLY-1' AND i_max_state = '3' THEN
      v_max_order := to_char(to_number(i_max_order - 1));
    ELSE
      v_max_order := i_max_order;
    END IF;

    IF i_edit_type = '0' THEN
      v_order := i_min_order;

      SELECT COUNT(1)
        INTO v_count
        FROM dams_standard_process_model sps,
             dams_wf_activity_conf       wac,
             dams_wf_activity_user       wau
       WHERE sps.activity_id = wac.id
         AND wac.pdid = i_process_id
         AND wau.activity_id = wac.id
         AND sps.open_state <> '3'
         AND to_number(v_order) <= to_number(sps.order_code)
         AND to_number(sps.order_code) <= to_number(v_max_order);
      IF v_count <> '0' THEN
        OPEN o_next_step_list FOR
          SELECT DISTINCT wac.activity_name, wau.transition_name, wac.id
            FROM dams_standard_process_model sps,
                 dams_wf_activity_conf       wac,
                 dams_wf_activity_user       wau
           WHERE sps.activity_id = wac.id
             AND wac.pdid = i_process_id
             AND wau.activity_id = wac.id
             AND to_number(v_order) <= to_number(sps.order_code)
             AND to_number(sps.order_code) <= to_number(v_max_order)
             AND sps.open_state <> '3'
           ORDER BY wac.id;

      END IF;
    ELSE
      SELECT nvl(sps.order_code, i_min_order - 1)
        INTO v_order
        FROM dams_standard_process_model sps, dams_wf_activity_conf wac
       WHERE sps.activity_id = wac.id
         AND wac.pdid = i_process_id
         AND sps.open_state <> '3'
         AND wac.activity_name = i_task_name;

      SELECT COUNT(1)
        INTO v_count
        FROM dams_standard_process_model sps,
             dams_wf_activity_conf       wac,
             dams_wf_activity_user       wau
       WHERE sps.activity_id = wac.id
         AND wac.pdid = i_process_id
         AND wau.activity_id = wac.id
         AND sps.open_state <> '3'
         AND to_number(v_order) < to_number(sps.order_code)
         AND to_number(sps.order_code) <= to_number(v_max_order);

      IF v_count <> '0' THEN

        OPEN o_next_step_list FOR
          SELECT DISTINCT wac.activity_name, wau.transition_name, wac.id
            FROM dams_standard_process_model sps,
                 dams_wf_activity_conf       wac,
                 dams_wf_activity_user       wau
           WHERE sps.activity_id = wac.id
             AND wac.pdid = i_process_id
             AND wau.activity_id = wac.id
             AND sps.open_state <> '3'
             AND to_number(v_order) < to_number(sps.order_code)
             AND to_number(sps.order_code) <= to_number(v_max_order)
           ORDER BY wac.id;
      END IF;
    END IF;

    IF v_count = '0' THEN

      IF i_process_id = 'AUTHAPPLY-1' AND i_max_state = '3' THEN
        OPEN o_next_step_list FOR
          SELECT DISTINCT wac.activity_name, wau.transition_name, wac.id
            FROM dams_standard_process_model sps,
                 dams_wf_activity_conf       wac,
                 dams_wf_activity_user       wau
           WHERE sps.activity_id = wac.id
             AND wac.pdid = i_process_id
             AND wau.activity_id = wac.id
             AND sps.open_state = '3'
             AND wac.activity_name <> i_task_name;

      END IF;
    END IF;

  EXCEPTION
    WHEN OTHERS THEN
      o_flag := '-1';
      pack_log.error('PCKG_DAMS_AUTHAPPLY_PROCESS.proc_get_next_steps',
                     pack_log.end_step, '异常！');
      IF o_next_step_list%ISOPEN THEN
        CLOSE o_next_step_list;
      END IF;
      RETURN;

  END proc_get_next_steps;

  PROCEDURE proc_has_auth(i_ssic_id    IN VARCHAR2, --用户ID
                          i_sec_id     IN VARCHAR2, --密级ID
                          i_sys_type   IN VARCHAR2, --活动名称
                          o_flag       OUT VARCHAR2, --返回标志
                          o_read_auth  OUT VARCHAR2, --查阅权限
                          o_copy_auth  OUT VARCHAR2, --复制权限
                          o_print_auth OUT VARCHAR2 --打印权限
                          ) IS
    v_branch_lv   VARCHAR2(20);
    v_role_id     VARCHAR2(20);
    v_template_id VARCHAR2(20);
  BEGIN
    o_flag := '0';

    SELECT nvl(MAX(tsr.template_id), '02')
      INTO v_template_id
      FROM dams_template_sys_rel tsr
     WHERE tsr.syscode = i_sys_type;

    SELECT nvl(MAX(b.stru_grade), MAX(b.man_grade))
      INTO v_branch_lv
      FROM dams_branch_relation br, dams_user u, dams_branch b
     WHERE br.bnch_id = u.stru_id
       AND u.ssic_id = i_ssic_id
       AND b.stru_id = br.up_bnch_id
       AND (br.up_bnch_sign = '1' OR br.up_bnch_sign = '4');

    SELECT nvl(MAX(urr.role_id), 'DR20000001')
      INTO v_role_id
      FROM dams_user u, dams_user_role_rel urr
     WHERE u.ssic_id = i_ssic_id
       AND urr.ssic_id(+) = u.ssic_id
       --AND urr.auth_state(+) IN ('0', '1')
       AND urr.sys_code(+) = 'QXSQ'
       AND urr.role_id(+) <> 'DR20000099'
       AND EXISTS
     (SELECT 1 FROM dams_qxsq_role qr WHERE qr.role_id = urr.role_id);

    SELECT decode(sps.order_code, '0', 'R', '')
      INTO o_read_auth
      FROM dams_authorityapply_set das, dams_standard_process_model sps
     WHERE das.prm_secret_id = i_sec_id
       AND das.duty_lv_code = v_role_id
       AND das.standard_id = sps.key_id
       AND das.system_type = v_template_id
       AND das.apply_type = '1'
       AND das.branch_lv = v_branch_lv
       AND sps.biz_type = '009'
       AND sps.order_code <> '999'
       AND das.upd_time = '99991231';

    SELECT decode(sps.order_code, '0', 'C', '')
      INTO o_copy_auth
      FROM dams_authorityapply_set das, dams_standard_process_model sps
     WHERE das.prm_secret_id = i_sec_id
       AND das.duty_lv_code = v_role_id
       AND das.standard_id = sps.key_id
       AND das.system_type = v_template_id
       AND das.apply_type = '2'
       AND das.branch_lv = v_branch_lv
       AND sps.biz_type = '009'
       AND sps.order_code <> '999'
       AND das.upd_time = '99991231';

    SELECT decode(sps.order_code, '0', 'P', '')
      INTO o_print_auth
      FROM dams_authorityapply_set das, dams_standard_process_model sps
     WHERE das.prm_secret_id = i_sec_id
       AND das.duty_lv_code = v_role_id
       AND das.standard_id = sps.key_id
       AND das.system_type = v_template_id
       AND das.apply_type = '3'
       AND das.branch_lv = v_branch_lv
       AND sps.biz_type = '009'
       AND das.upd_time = '99991231';

  EXCEPTION
    WHEN OTHERS THEN
      o_flag := '-1';

      pack_log.error('PCKG_DAMS_AUTHAPPLY_PROCESS.proc_has_auth',
                     pack_log.end_step, '异常！');

      RETURN;
  END proc_has_auth;

  PROCEDURE proc_get_pre_taskid(i_business_id IN VARCHAR2, --业务主键ID
                                o_flag        OUT VARCHAR2, --返回标志
                                o_task_id     OUT VARCHAR2 --taskID
                                ) IS
  BEGIN
    o_flag := '0';
    SELECT MAX(t.task_id)
      INTO o_task_id
      FROM (SELECT MAX(wt.task_id) task_id
              FROM dams_wf_task wt
             WHERE wt.business_id = i_business_id
            UNION ALL
            SELECT MAX(wht.task_id) task_id
              FROM dams_wf_hist_task wht
             WHERE wht.business_id = i_business_id) t;

  EXCEPTION
    WHEN OTHERS THEN
      o_flag := '-1';

      pack_log.error('PCKG_DAMS_AUTHAPPLY_PROCESS.proc_get_pre_taskid',
                     pack_log.end_step, '异常！');

      RETURN;

  END proc_get_pre_taskid;

 

  PROCEDURE proc_get_subsys_url(i_sys_code IN VARCHAR2, --系统编码
                                o_flag     OUT VARCHAR2, --返回标志
                                o_url      OUT VARCHAR2 --url
                                ) IS
    v_count VARCHAR2(4);

  BEGIN
    o_flag := '0';
    SELECT COUNT(1)
      INTO v_count
      FROM dams_sys_url_cfg suc
     WHERE suc.sys_code = i_sys_code;

    IF v_count = '0' THEN
      o_url := '';
    ELSE

      SELECT suc.sysurl || '/' || suc.context
        INTO o_url
        FROM dams_sys_url_cfg suc
       WHERE suc.sys_code = i_sys_code;

    END IF;

  EXCEPTION
    WHEN OTHERS THEN
      o_flag := '-1';

      pack_log.error('PCKG_DAMS_AUTHAPPLY_PROCESS.proc_get_subsys_url',
                     pack_log.end_step, '异常！');

      RETURN;

  END proc_get_subsys_url;

 
  PROCEDURE proc_insert_dams_mail(i_mail_to  IN VARCHAR2, --主送
                                  i_mail_cc  IN VARCHAR2, --抄送
                                  i_mail_bcc IN VARCHAR2, --密送
                                  i_subject  IN VARCHAR2, --主题
                                  i_content  IN VARCHAR2, --内容
                                  o_flag     OUT VARCHAR2 --0  成功  1 失败
                                  ) IS
    v_param     db_log.info%TYPE := i_mail_to || '|' || i_mail_cc || '|' ||
                                    i_mail_bcc;
    v_proc_name db_log.proc_name%TYPE := 'PCKG_DAMS_OPINION.PROC_INSERT_DAMS_MAIL';
    v_step      db_log.step_no%TYPE;

   
    v_mail_seq VARCHAR2(20);
    v_msg      VARCHAR2(400);
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step,
                  pack_log.start_msg || v_param);
    o_flag := '0';
    v_step := '1';

   

    v_mail_seq := pckg_dams_mail.func_get_mail_seq();

    INSERT INTO dams_mail
      (mail_id, mail_to, mail_cc, mail_bcc, subject, content)
    VALUES
      (v_mail_seq, i_mail_to, i_mail_cc, i_mail_bcc, i_subject, i_content);

    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);

    COMMIT;

  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, v_step, '异常！' || v_param);
      o_flag := '-1';
      ROLLBACK;
  END proc_insert_dams_mail;

 
  FUNCTION fun_branch_bank(i_branch_id IN VARCHAR2 --所属部门ID
                           ) RETURN VARCHAR2 IS
    v_bank_id dams_branch.stru_id%TYPE;
  BEGIN
    SELECT MAX(c.major_stru_id)
      INTO v_bank_id
      FROM dams_branch_catalog c
     WHERE c.stru_id = i_branch_id;
   
    RETURN v_bank_id;
  EXCEPTION
    WHEN OTHERS THEN
      RETURN '-1';
  END fun_branch_bank;

  /******************************************************************************
      --函数名称：      FUN_BRANCH_LV2
      --存储过程描述：  根据机构返回机构层级
      --功能模块：      参数公共模块
      --作者：          kfzx-fuyl
      --时间：          2013-04-16
      --修改历史:
  ******************************************************************************/
  FUNCTION fun_get_branch_lv(i_branch_id IN VARCHAR2) RETURN VARCHAR2 IS
    v_bank_lv   VARCHAR2(10);
    v_param     db_log.info%TYPE := i_branch_id;
    v_proc_name db_log.proc_name%TYPE := 'PCKG_DAMS_PARAMETER_SET.FUN_GET_BRANCH_LV';
  BEGIN

    SELECT MAX(bc.stru_level)
      INTO v_bank_lv
      FROM dams_branch_catalog bc
     WHERE bc.stru_id = i_branch_id;

    RETURN v_bank_lv;
  EXCEPTION
    WHEN no_data_found THEN
      RETURN i_branch_id;
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, v_param);
      RETURN '-1';
  END fun_get_branch_lv;


  PROCEDURE proc_export_auth_apply(i_appstru    IN VARCHAR2,
                                   i_query_type IN VARCHAR2,
                                   i_start_date IN VARCHAR2,
                                   i_end_date   IN VARCHAR2,
                                   i_branch_id  IN VARCHAR2,
                                   o_ret_code   OUT VARCHAR2,
                                   o_apply_stru OUT VARCHAR2,
                                   o_start_date OUT VARCHAR2,
                                   o_end_date   OUT VARCHAR2,
                                   o_auth_apply OUT SYS_REFCURSOR) IS
    v_param     db_log.info%TYPE := i_appstru || '|' || i_start_date || '|' ||
                                    i_end_date || '|' || i_branch_id;
    v_proc_name db_log.proc_name%TYPE := 'PCKG_DAMS_PARAMETER_SET.FUN_GET_BRANCH_LV';

  BEGIN
    o_ret_code := '0';

    o_start_date := substr(i_start_date, 1, 4) || '年' ||
                    substr(i_start_date, 6, 2) || '月' ||
                    substr(i_start_date, 9, 2) || '日';
    o_end_date   := substr(i_end_date, 1, 4) || '年' || substr(i_end_date, 6, 2) || '月' ||
                    substr(i_end_date, 9, 2) || '日';

    IF i_query_type = '1' THEN

      o_apply_stru := '所辖机构';

      OPEN o_auth_apply FOR
        SELECT app_stru_name,
               app_user_name,
               app_user_role,
               doc_title,
               doc_num,
               secure_degree,
               app_auth,
               apply_reason,
               available_date,
               auth_user_name,
               auth_user_role,
               auth_date,
               applying_date,
               app_result
          FROM (SELECT app_stru_name,
                       app_user_name,
                       app_user_role,
                       doc_title,
                       doc_num,
                       secure_degree,
                       app_auth,
                       apply_reason,
                       available_date,
                       auth_user_name,
                       auth_user_role,
                       auth_date,
                       applying_date,
                       app_result,
                       app_date,
                       stru_level,
                       major_stru_id
                  FROM (SELECT app_stru_name,
                               app_user_name,
                               app_user_role,
                               doc_title,
                               doc_num,
                               secure_degree,
                               app_auth,
                               apply_reason,
                               available_date,
                               auth_user_name,
                               auth_user_role,
                               auth_date,
                               CASE
                                 WHEN app_day = '0' THEN
                                  CASE
                                    WHEN app_hour = '0' THEN
                                     app_sec || '分钟'
                                    ELSE
                                     app_hour || '小时' || app_sec || '分钟'
                                  END
                                 WHEN app_day IS NULL THEN
                                  NULL
                                 ELSE
                                  app_day || '天' || app_hour || '小时' || app_sec || '分钟'
                               END applying_date,
                               app_result,
                               app_date,
                               stru_level,
                               major_stru_id
                          FROM (SELECT DISTINCT b.stru_sname app_stru_name,
                                                u1.name app_user_name,
                                                pckg_dams_util.func_decode('DUTY_LV',
                                                                           u1.duty_lv,
                                                                           'DAMS',
                                                                           'zh_CN') app_user_role,
                                                aa.doc_title doc_title,
                                                aa.doc_num doc_num,
                                                sdb.secure_degree secure_degree,
                                                decode(aa.read_auth, '1', '查阅 ', '') ||
                                                decode(aa.copy_auth, '1', '复制 ', '') ||
                                                decode(aa.print_auth, '1', '打印 ',
                                                       '') app_auth,
                                                aa.apply_reason apply_reason,
                                                decode(aa.apply_end_date,
                                                       '99991231', '无限制',
                                                       substr(aa.apply_start_date, 1,
                                                               4) || '年' ||
                                                        substr(aa.apply_start_date, 5,
                                                               2) || '月' ||
                                                        substr(aa.apply_start_date, 7,
                                                               2) || '日' || '-' ||
                                                        substr(aa.apply_end_date, 1,
                                                               4) || '年' ||
                                                        substr(aa.apply_end_date, 5,
                                                               2) || '月' ||
                                                        substr(aa.apply_end_date, 7,
                                                               2) || '日') available_date,
                                                nvl(nvl(nvl((SELECT u3.name
                                                              FROM dams_user u3
                                                             WHERE aa.authorized_person_c =
                                                                   u3.ssic_id),
                                                            (SELECT u4.name
                                                                FROM dams_user u4
                                                               WHERE aa.authorized_person_p =
                                                                     u4.ssic_id)),
                                                        (SELECT u2.name
                                                            FROM dams_user u2
                                                           WHERE aa.authorized_person_r =
                                                                 u2.ssic_id)),
                                                    (SELECT u2.name
                                                        FROM dams_user u2
                                                       WHERE wht.ssic_id = u2.ssic_id)) auth_user_name,
                                                wht.task_name1 auth_user_role,
                                                wht.end_time auth_date,
                                                wht2.create_time app_date,
                                                br.stru_level,
                                                br.major_stru_id,
                                                extract(DAY FROM(wht.end_time -
                                                             wht2.create_time)) app_day,
                                                extract(hour
                                                        FROM(wht.end_time -
                                                             wht2.create_time)) app_hour,
                                                extract(minute
                                                        FROM(wht.end_time -
                                                             wht2.create_time)) ||
                                                substr(substr(extract(SECOND
                                                                      FROM(wht.end_time -
                                                                           wht2.create_time)),
                                                              1,
                                                              instr(extract(SECOND
                                                                             FROM(wht.end_time -
                                                                                  wht2.create_time)),
                                                                     '.') - 1) / 60,
                                                       1, 3) app_sec,

                                                decode(wht.ssic_id, NULL, NULL,
                                                       decode(aa.read_auth, '1',
                                                               decode(aa.max_order_r,
                                                                       '0', '不同意', '同意'),
                                                               decode(aa.print_auth,
                                                                       '1',
                                                                       decode(aa.max_order_p,
                                                                               '0', '不同意',
                                                                               '同意'),
                                                                       decode(aa.copy_auth,
                                                                               '1',
                                                                               decode(aa.max_order_c,
                                                                                       '0',
                                                                                       '不同意',
                                                                                       '同意'),
                                                                               '不同意')))) app_result
                                  FROM dams_secure_degree_base  sdb,
                                       dams_branch_catalog      br,
                                       dams_branch              b,
                                       dams_user                u1,
                                       viw_dams_wf_all_task     wht,
                                       viw_dams_wf_all_task     wht2,
                                       dams_branch_catalog      bc,
                                       dams_authorityapply_info aa
                                 WHERE aa.secure_id = sdb.secure_id
                                   AND aa.branch_id = br.stru_id
                                   AND aa.branch_id = b.stru_id
                                   AND bc.stru_id = i_branch_id
                                   AND br.major_stru_id = bc.major_stru_id
                                   AND aa.apply_person_id = u1.ssic_id
                                   AND wht.business_id(+) = aa.key_id
                                   --AND (wht.action_flag(+) = '1' OR wht.outgoing_name(+) = '不同意')
                                   AND wht.sys_code(+) = 'QXSQ'
                                   AND wht2.business_id = aa.key_id
                                   AND wht2.is_first = '1'
                                   AND wht2.sys_code = 'QXSQ'
                                   AND aa.cret_date >
                                       REPLACE(i_start_date, '-', '') || '000000'
                                   AND aa.cret_date <
                                       REPLACE(i_end_date, '-', '') || '235959') t
                        UNION ALL
                        SELECT app_stru_name,
                               app_user_name,
                               app_user_role,
                               doc_title,
                               doc_num,
                               secure_degree,
                               app_auth,
                               apply_reason,
                               available_date,
                               auth_user_name,
                               auth_user_role,
                               auth_date,
                               CASE
                                 WHEN app_day = '0' THEN
                                  CASE
                                    WHEN app_hour = '0' THEN
                                     app_sec || '分钟'
                                    ELSE
                                     app_hour || '小时' || app_sec || '分钟'
                                  END
                                 WHEN app_day IS NULL THEN
                                  NULL
                                 ELSE
                                  app_day || '天' || app_hour || '小时' || app_sec || '分钟'
                               END applying_date,
                               app_result,
                               app_date,
                               stru_level,
                               major_stru_id
                          FROM (SELECT DISTINCT b.stru_sname app_stru_name,
                                                u1.name app_user_name,
                                                pckg_dams_util.func_decode('DUTY_LV',
                                                                           u1.duty_lv,
                                                                           'DAMS',
                                                                           'zh_CN') app_user_role,
                                                aa.doc_title doc_title,
                                                aa.doc_num doc_num,
                                                sdb.secure_degree secure_degree,
                                                decode(aa.read_auth, '1', '查阅 ', '') ||
                                                decode(aa.copy_auth, '1', '复制 ', '') ||
                                                decode(aa.print_auth, '1', '打印 ',
                                                       '') app_auth,
                                                aa.apply_reason apply_reason,
                                                decode(aa.apply_end_date,
                                                       '99991231', '无限制',
                                                       substr(aa.apply_start_date, 1,
                                                               4) || '年' ||
                                                        substr(aa.apply_start_date, 5,
                                                               2) || '月' ||
                                                        substr(aa.apply_start_date, 7,
                                                               2) || '日' || '-' ||
                                                        substr(aa.apply_end_date, 1,
                                                               4) || '年' ||
                                                        substr(aa.apply_end_date, 5,
                                                               2) || '月' ||
                                                        substr(aa.apply_end_date, 7,
                                                               2) || '日') available_date,
                                                nvl(nvl(nvl((SELECT u3.name
                                                              FROM dams_user u3
                                                             WHERE aa.authorized_person_c =
                                                                   u3.ssic_id),
                                                            (SELECT u4.name
                                                                FROM dams_user u4
                                                               WHERE aa.authorized_person_p =
                                                                     u4.ssic_id)),
                                                        (SELECT u2.name
                                                            FROM dams_user u2
                                                           WHERE aa.authorized_person_r =
                                                                 u2.ssic_id)),
                                                    (SELECT u2.name
                                                        FROM dams_user u2
                                                       WHERE wht.ssic_id = u2.ssic_id)) auth_user_name,
                                                wht.task_name1 auth_user_role,
                                                wht.end_time auth_date,
                                                wht2.create_time app_date,
                                                br.stru_level,
                                                br.major_stru_id,
                                                extract(DAY FROM(wht.end_time -
                                                             wht2.create_time)) app_day,
                                                extract(hour
                                                        FROM(wht.end_time -
                                                             wht2.create_time)) app_hour,
                                                extract(minute
                                                        FROM(wht.end_time -
                                                             wht2.create_time)) ||
                                                substr(substr(extract(SECOND
                                                                      FROM(wht.end_time -
                                                                           wht2.create_time)),
                                                              1,
                                                              instr(extract(SECOND
                                                                             FROM(wht.end_time -
                                                                                  wht2.create_time)),
                                                                     '.') - 1) / 60,
                                                       1, 3) app_sec,

                                                decode(wht.ssic_id, NULL, NULL,
                                                       decode(aa.read_auth, '1',
                                                               decode(aa.max_order_r,
                                                                       '0', '不同意', '同意'),
                                                               decode(aa.print_auth,
                                                                       '1',
                                                                       decode(aa.max_order_p,
                                                                               '0', '不同意',
                                                                               '同意'),
                                                                       decode(aa.copy_auth,
                                                                               '1',
                                                                               decode(aa.max_order_c,
                                                                                       '0',
                                                                                       '不同意',
                                                                                       '同意'),
                                                                               '不同意')))) app_result
                                  FROM dams_secure_degree_base  sdb,
                                       dams_branch_catalog      br,
                                       dams_branch              b,
                                       dams_user                u1,
                                       viw_dams_wf_all_task     wht,
                                       viw_dams_wf_all_task     wht2,
                                       dams_branch_catalog      bc,
                                       dams_branch              b1,
                                       dams_authorityapply_info aa
                                 WHERE aa.secure_id = sdb.secure_id
                                   AND aa.branch_id = br.stru_id
                                   AND aa.branch_id = b.stru_id
                                   AND bc.stru_id = i_branch_id
                                   AND b1.sup_stru = bc.major_stru_id
                                   AND br.major_stru_id = b1.stru_id
                                   AND aa.apply_person_id = u1.ssic_id
                                   AND wht.business_id(+) = aa.key_id
                                   --AND (wht.action_flag(+) = '1' OR wht.outgoing_name(+) = '不同意')
                                   AND wht.sys_code(+) = 'QXSQ'
                                   AND wht2.business_id = aa.key_id
                                   AND wht2.is_first = '1'
                                   AND wht2.sys_code = 'QXSQ'
                                   AND aa.cret_date >
                                       REPLACE(i_start_date, '-', '') || '000000'
                                   AND aa.cret_date <
                                       REPLACE(i_end_date, '-', '') || '235959') t)
                 ORDER BY stru_level,
                          major_stru_id,
                          app_stru_name ASC NULLS LAST,
                          app_date);

    ELSE

      IF i_appstru = 'all' THEN

        o_apply_stru := '所有部门';

        OPEN o_auth_apply FOR
          SELECT app_stru_name,
                 app_user_name,
                 app_user_role,
                 doc_title,
                 doc_num,
                 secure_degree,
                 app_auth,
                 apply_reason,
                 available_date,
                 auth_user_name,
                 auth_user_role,
                 auth_date,
                 CASE
                   WHEN app_day = '0' THEN
                    CASE
                      WHEN app_hour = '0' THEN
                       app_sec || '分钟'
                      ELSE
                       app_hour || '小时' || app_sec || '分钟'
                    END
                   WHEN app_day IS NULL THEN
                    NULL
                   ELSE
                    app_day || '天' || app_hour || '小时' || app_sec || '分钟'
                 END applying_date,
                 app_result
            FROM (SELECT DISTINCT b.stru_sname app_stru_name,
                                  u1.name app_user_name,
                                  pckg_dams_util.func_decode('DUTY_LV',
                                                             u1.duty_lv, 'DAMS',
                                                             'zh_CN') app_user_role,
                                  aa.doc_title doc_title,
                                  aa.doc_num doc_num,
                                  sdb.secure_degree secure_degree,
                                  decode(aa.read_auth, '1', '查阅 ', '') ||
                                  decode(aa.copy_auth, '1', '复制 ', '') ||
                                  decode(aa.print_auth, '1', '打印 ', '') app_auth,
                                  aa.apply_reason apply_reason,
                                  decode(aa.apply_end_date, '99991231', '无限制',
                                         substr(aa.apply_start_date, 1, 4) || '年' ||
                                          substr(aa.apply_start_date, 5, 2) || '月' ||
                                          substr(aa.apply_start_date, 7, 2) || '日' || '-' ||
                                          substr(aa.apply_end_date, 1, 4) || '年' ||
                                          substr(aa.apply_end_date, 5, 2) || '月' ||
                                          substr(aa.apply_end_date, 7, 2) || '日') available_date,
                                  nvl(nvl(nvl((SELECT u3.name
                                                FROM dams_user u3
                                               WHERE aa.authorized_person_c =
                                                     u3.ssic_id),
                                              (SELECT u4.name
                                                  FROM dams_user u4
                                                 WHERE aa.authorized_person_p =
                                                       u4.ssic_id)),
                                          (SELECT u2.name
                                              FROM dams_user u2
                                             WHERE aa.authorized_person_r =
                                                   u2.ssic_id)),
                                      (SELECT u2.name
                                          FROM dams_user u2
                                         WHERE wht.ssic_id = u2.ssic_id)) auth_user_name,
                                  wht.task_name1 auth_user_role,
                                  wht.end_time auth_date,
                                  wht2.create_time app_date,
                                  extract(DAY
                                          FROM(wht.end_time - wht2.create_time)) app_day,
                                  extract(hour
                                          FROM(wht.end_time - wht2.create_time)) app_hour,
                                  extract(minute
                                          FROM(wht.end_time - wht2.create_time)) ||
                                  substr(substr(extract(SECOND
                                                        FROM(wht.end_time -
                                                             wht2.create_time)), 1,
                                                instr(extract(SECOND
                                                               FROM(wht.end_time -
                                                                    wht2.create_time)),
                                                       '.') - 1) / 60, 1, 3) app_sec,

                                  decode(wht.ssic_id, NULL, NULL,
                                         decode(aa.read_auth, '1',
                                                 decode(aa.max_order_r, '0', '不同意',
                                                         '同意'),
                                                 decode(aa.print_auth, '1',
                                                         decode(aa.max_order_p, '0',
                                                                 '不同意', '同意'),
                                                         decode(aa.copy_auth, '1',
                                                                 decode(aa.max_order_c,
                                                                         '0', '不同意', '同意'),
                                                                 '不同意')))) app_result
                    FROM dams_secure_degree_base  sdb,
                         dams_branch_catalog      br,
                         dams_branch              b,
                         dams_user                u1,
                         viw_dams_wf_all_task     wht,
                         viw_dams_wf_all_task     wht2,
                         dams_branch_catalog      bc,
                         dams_authorityapply_info aa
                   WHERE aa.secure_id = sdb.secure_id
                     AND aa.branch_id = br.stru_id
                     AND aa.branch_id = b.stru_id
                     AND bc.stru_id = i_branch_id
                     AND br.major_stru_id = bc.major_stru_id
                     AND aa.apply_person_id = u1.ssic_id
                     AND wht.business_id(+) = aa.key_id
                     --AND (wht.action_flag(+) = '1' OR wht.outgoing_name(+) = '不同意')
                     AND wht.sys_code(+) = 'QXSQ'
                     AND wht2.business_id = aa.key_id
                     AND wht2.is_first = '1'
                     AND wht2.sys_code = 'QXSQ'
                     AND aa.cret_date >
                         REPLACE(i_start_date, '-', '') || '000000'
                     AND aa.cret_date < REPLACE(i_end_date, '-', '') || '235959') t
           ORDER BY 
		  app_stru_name ASC NULLS LAST,
                    app_date;

      ELSE

        SELECT MAX(b.stru_sname)
          INTO o_apply_stru
          FROM dams_branch b
         WHERE b.stru_id = i_appstru;

        OPEN o_auth_apply FOR
          SELECT app_stru_name,
                 app_user_name,
                 app_user_role,
                 doc_title,
                 doc_num,
                 secure_degree,
                 app_auth,
                 apply_reason,
                 available_date,
                 auth_user_name,
                 auth_user_role,
                 auth_date,
                 CASE
                   WHEN app_day = '0' THEN
                    CASE
                      WHEN app_hour = '0' THEN
                       app_sec || '分钟'
                      ELSE
                       app_hour || '小时' || app_sec || '分钟'
                    END
                   WHEN app_day IS NULL THEN
                    NULL
                   ELSE
                    app_day || '天' || app_hour || '小时' || app_sec || '分钟'
                 END applying_date,
                 app_result
            FROM (SELECT DISTINCT b.stru_sname app_stru_name,
                                  u1.name app_user_name,
                                  pckg_dams_util.func_decode('DUTY_LV',
                                                             u1.duty_lv, 'DAMS',
                                                             'zh_CN') app_user_role,
                                  aa.doc_title doc_title,
                                  aa.doc_num doc_num,
                                  sdb.secure_degree secure_degree,
                                  decode(aa.read_auth, '1', '查阅 ', '') ||
                                  decode(aa.copy_auth, '1', '复制 ', '') ||
                                  decode(aa.print_auth, '1', '打印 ', '') app_auth,
                                  aa.apply_reason apply_reason,
                                  decode(aa.apply_end_date, '99991231', '无限制',
                                         substr(aa.apply_start_date, 1, 4) || '年' ||
                                          substr(aa.apply_start_date, 5, 2) || '月' ||
                                          substr(aa.apply_start_date, 7, 2) || '日' || '-' ||
                                          substr(aa.apply_end_date, 1, 4) || '年' ||
                                          substr(aa.apply_end_date, 5, 2) || '月' ||
                                          substr(aa.apply_end_date, 7, 2) || '日') available_date,
                                  nvl(nvl(nvl((SELECT u3.name
                                                FROM dams_user u3
                                               WHERE aa.authorized_person_c =
                                                     u3.ssic_id),
                                              (SELECT u4.name
                                                  FROM dams_user u4
                                                 WHERE aa.authorized_person_p =
                                                       u4.ssic_id)),
                                          (SELECT u2.name
                                              FROM dams_user u2
                                             WHERE aa.authorized_person_r =
                                                   u2.ssic_id)),
                                      (SELECT u2.name
                                          FROM dams_user u2
                                         WHERE wht.ssic_id = u2.ssic_id)) auth_user_name,
                                  wht.task_name1 auth_user_role,
                                  wht.end_time auth_date,
                                  wht2.create_time app_date,
                                  extract(DAY
                                          FROM(wht.end_time - wht2.create_time)) app_day,
                                  extract(hour
                                          FROM(wht.end_time - wht2.create_time)) app_hour,
                                  extract(minute
                                          FROM(wht.end_time - wht2.create_time)) ||
                                  substr(substr(extract(SECOND
                                                        FROM(wht.end_time -
                                                             wht2.create_time)), 1,
                                                instr(extract(SECOND
                                                               FROM(wht.end_time -
                                                                    wht2.create_time)),
                                                       '.') - 1) / 60, 1, 3) app_sec,

                                  decode(wht.ssic_id, NULL, NULL,
                                         decode(aa.read_auth, '1',
                                                 decode(aa.max_order_r, '0', '不同意',
                                                         '同意'),
                                                 decode(aa.print_auth, '1',
                                                         decode(aa.max_order_p, '0',
                                                                 '不同意', '同意'),
                                                         decode(aa.copy_auth, '1',
                                                                 decode(aa.max_order_c,
                                                                         '0', '不同意', '同意'),
                                                                 '不同意')))) app_result
                    FROM dams_secure_degree_base  sdb,
                         dams_branch_catalog      br,
                         dams_branch              b,
                         dams_user                u1,
                         viw_dams_wf_all_task     wht,
                         viw_dams_wf_all_task     wht2,
                         dams_branch_catalog      bc,
                         dams_authorityapply_info aa
                   WHERE aa.secure_id = sdb.secure_id
                     AND aa.branch_id = br.stru_id
                     AND aa.branch_id = b.stru_id
                     AND bc.stru_id = i_appstru
                     AND br.dept_stru_id = bc.dept_stru_id
                     AND aa.apply_person_id = u1.ssic_id
                     AND wht.business_id(+) = aa.key_id
                     --AND (wht.action_flag(+) = '1' OR wht.outgoing_name(+) = '不同意')
                     AND wht.sys_code(+) = 'QXSQ'
                     AND wht2.business_id = aa.key_id
                     AND wht2.is_first = '1'
                     AND wht2.sys_code = 'QXSQ'
                     AND aa.cret_date >
                         REPLACE(i_start_date, '-', '') || '000000'
                     AND aa.cret_date < REPLACE(i_end_date, '-', '') || '235959') t
           ORDER BY 
		   app_stru_name ASC NULLS LAST,
                    app_date;

      END IF;

    END IF;

  EXCEPTION
    WHEN OTHERS THEN

      o_ret_code := '-1';

      IF o_auth_apply%ISOPEN THEN
        CLOSE o_auth_apply;
      END IF;

      pack_log.error(v_proc_name, pack_log.end_step, v_param);

  END proc_export_auth_apply;

END pckg_dams_authapply_process;

//





CREATE OR REPLACE PACKAGE pckg_ab_rsk_chk IS

  PROCEDURE proc_prepare_data(i_ssic_id    IN VARCHAR2,
                              i_role_stru  IN VARCHAR2,
                              o_ret_code   OUT VARCHAR2, --返回值
                              o_role_stru  OUT VARCHAR2,
                              o_bnch_id    OUT VARCHAR2,
                              o_bnch_name  OUT VARCHAR2,
                              o_bnch_grade OUT VARCHAR2,
                              o_sup_stru   OUT VARCHAR2);

  PROCEDURE proc_get_info_list(i_ssic_id          IN VARCHAR2,
                               i_stru_id          IN VARCHAR2,
                               i_status           IN VARCHAR2,
                               i_event_stru       IN VARCHAR2,
                               i_submit_state     IN VARCHAR2,
                               i_process_state    IN VARCHAR2,
                               i_adopt_status     IN VARCHAR2, --纳入标识(0未纳入,1已纳入)
                               i_adopt_stru_grade IN VARCHAR2, --纳入层级(1总行,2一级行)
                               i_beg_num          IN VARCHAR2,
                               i_fetch_num        IN VARCHAR2,
                               o_ret_code         OUT VARCHAR2,
                               o_msg              OUT VARCHAR2,
                               o_total_num        OUT VARCHAR2,
                               o_info_list        OUT SYS_REFCURSOR);

  PROCEDURE proc_get_sub_list(i_ssic_id      IN VARCHAR2, --当前操作用户
                              i_stru_id      IN VARCHAR2, --所属机构
                              i_sub_stru_id  IN VARCHAR2,
                              i_status       IN VARCHAR2, --状态1有效2销账3删除
                              i_event_stru   IN VARCHAR2,
                              i_adopt_status IN VARCHAR2, --纳入标识(0未纳入,1已纳入)
                              i_beg_num      IN VARCHAR2, --
                              i_fetch_num    IN VARCHAR2, --
                              o_ret_code     OUT VARCHAR2,
                              o_msg          OUT VARCHAR2,
                              o_total_num    OUT VARCHAR2,
                              o_info_list    OUT SYS_REFCURSOR);

  PROCEDURE proc_get_rsk_chk_detail(i_ssic_id     IN VARCHAR2,
                                    i_rsk_chk_id  IN VARCHAR2,
                                    o_ret_code    OUT VARCHAR2, --返回值
                                    o_app_num     OUT VARCHAR2, --关联的APP的数量,用于标识改台账是否走过流程
                                    o_list_cur    OUT SYS_REFCURSOR, --查询列表游标
                                    o_source_list OUT SYS_REFCURSOR --来源台账信息列表(取下级行的专业机构及负责人)
                                    );
  PROCEDURE proc_get_submit_detail(i_ssic_id     IN VARCHAR2,
                                   i_rsk_chk_id  IN VARCHAR2,
                                   i_submit_id   IN VARCHAR2,
                                   o_ret_code    OUT VARCHAR2, --返回值
                                   o_list_cur    OUT SYS_REFCURSOR, --查询列表游标
                                   o_source_list OUT SYS_REFCURSOR --来源台账信息列表(取下级行的专业机构及负责人)
                                   );

  PROCEDURE save_rsk_chk_info(io_rsk_chk_id        IN OUT VARCHAR2, --台账id
                              i_ssic_id            IN VARCHAR2, --当前操作用户
                              i_stru_id            IN VARCHAR2, --所属机构
                              i_status             IN VARCHAR2, --状态1有效2销账3删除
                              i_event_stru         IN VARCHAR2, --当事机构
                              i_event_type         IN VARCHAR2, --事件类型
                              i_description        IN CLOB, --风险隐患事件描述
                              i_host_user          IN VARCHAR2, --当事机构管理责任人
                              i_host_tel           IN VARCHAR2, --当事机构管理责任人联系方式
                              i_rsk_point          IN CLOB, --风险点及存在的问题
                              i_solution_point     IN CLOB, --应对预案要点
                              i_response_method    IN CLOB, --媒体应对口径
                              i_treatment_result   IN CLOB, --处置情况
                              i_remark             IN VARCHAR2, --备注
                              i_source_rsk_chk_id  IN VARCHAR2, --来源风险排查id
                              i_attach_file_ref_id IN VARCHAR2, --附件关联id
                              i_pro_stru           IN VARCHAR2, --主管专业部门id
                              i_pro_host_user      IN VARCHAR2, --主管专业部门责任人id
                              i_pro_host_tel       IN VARCHAR2 --主管专业部门责任人联系方式
                              );

  PROCEDURE proc_insert_rsk_chk_info(i_rsk_chk_id         IN VARCHAR2, --台账ID
                                     i_ssic_id            IN VARCHAR2, --当前操作用户
                                     i_stru_id            IN VARCHAR2, --所属机构
                                     i_status             IN VARCHAR2, --状态1有效2销账3删除
                                     i_event_stru         IN VARCHAR2, --当事机构
                                     i_event_type         IN VARCHAR2, --事件类型
                                     i_description        IN CLOB, --风险隐患事件描述
                                     i_host_user          IN VARCHAR2, --当事机构管理责任人
                                     i_host_tel           IN VARCHAR2, --当事机构管理责任人联系方式
                                     i_rsk_point          IN CLOB, --风险点及存在的问题
                                     i_solution_point     IN CLOB, --应对预案要点
                                     i_response_method    IN CLOB, --媒体应对口径
                                     i_treatment_result   IN CLOB, --处置情况
                                     i_remark             IN VARCHAR2, --备注
                                     i_source_rsk_chk_id  IN VARCHAR2, --来源风险排查id
                                     i_attach_file_ref_id IN VARCHAR2, --附件关联id
                                     i_pro_stru           IN VARCHAR2, --主管专业部门id
                                     i_pro_host_user      IN VARCHAR2, --主管专业部门责任人id
                                     i_pro_host_tel       IN VARCHAR2, --主管专业部门责任人联系方式
                                     o_ret_code           OUT VARCHAR2,
                                     o_msg                OUT VARCHAR2,
                                     o_info_id            OUT VARCHAR2);

  PROCEDURE proc_update_rsk_chk_info(i_rsk_chk_id         IN VARCHAR2, --台账ID
                                     i_ssic_id            IN VARCHAR2, --当前操作用户
                                     i_stru_id            IN VARCHAR2, --所属机构
                                     i_status             IN VARCHAR2, --状态1有效2销账3删除
                                     i_event_stru         IN VARCHAR2, --当事机构
                                     i_event_type         IN VARCHAR2, --事件类型
                                     i_description        IN CLOB, --风险隐患事件描述
                                     i_host_user          IN VARCHAR2, --当事机构管理责任人
                                     i_host_tel           IN VARCHAR2, --当事机构管理责任人联系方式
                                     i_rsk_point          IN CLOB, --风险点及存在的问题
                                     i_solution_point     IN CLOB, --应对预案要点
                                     i_response_method    IN CLOB, --媒体应对口径
                                     i_treatment_result   IN CLOB, --处置情况
                                     i_remark             IN VARCHAR2, --备注
                                     i_source_rsk_chk_id  IN VARCHAR2, --来源风险排查id
                                     i_attach_file_ref_id IN VARCHAR2, --附件关联id
                                     i_pro_stru           IN VARCHAR2, --主管专业部门id
                                     i_pro_host_user      IN VARCHAR2, --主管专业部门责任人id
                                     i_pro_host_tel       IN VARCHAR2, --主管专业部门责任人联系方式
                                     o_ret_code           OUT VARCHAR2,
                                     o_msg                OUT VARCHAR2,
                                     o_info_id            OUT VARCHAR2);

  PROCEDURE proc_submit_to_sup_app(i_rsk_chk_ids   IN VARCHAR2, --台账ID
                                   i_ssic_id       IN VARCHAR2, --当前操作用户
                                   i_audit_state   IN VARCHAR2, --审核状态(01被否决02等待复核人审核03复核人审批通过04等待负责人审批05负责人审批通过99全部审批通过)
                                   i_process_state IN VARCHAR2, --流程类型(1提交上级的内部审批流程2销账流程3删除流程)
                                   i_stru_id       IN VARCHAR2, --创建申请的机构
                                   o_ret_code      OUT VARCHAR2,
                                   o_msg           OUT VARCHAR2,
                                   o_info_id       OUT VARCHAR2);

  PROCEDURE insert_rsk_chk_app(i_rsk_chk_ids   IN VARCHAR2, --台账ID
                               i_ssic_id       IN VARCHAR2, --当前操作用户
                               i_audit_state   IN VARCHAR2, --审核状态(01被否决02等待复核人审核03复核人审批通过04等待负责人审批05负责人审批通过99全部审批通过)
                               i_process_state IN VARCHAR2, --流程类型(1提交上级的内部审批流程2销账流程3删除流程)
                               i_stru_id       IN VARCHAR2, --创建申请的机构
                               o_ret_code      OUT VARCHAR2,
                               o_app_id        OUT VARCHAR2);

  PROCEDURE proc_get_sup_manager(i_ssic_id        IN VARCHAR2, --当前操作用户
                                 i_stru_id        IN VARCHAR2, -- 当前用户所属机构
                                 i_process_status IN VARCHAR2,
                                 i_role_id        IN VARCHAR2,
                                 o_ret_code       OUT VARCHAR2,
                                 o_msg            OUT VARCHAR2,
                                 o_total_num      OUT VARCHAR2,
                                 o_user_list      OUT SYS_REFCURSOR --查询列表游标
                                 );

  PROCEDURE proc_update_process_status(i_info_json   IN CLOB, --台账信息json[{rsk_chk_id:***,reason:×××},{}...]
                                       i_app_id      IN VARCHAR2, --台账申请表id
                                       i_task_id     IN VARCHAR2, --task表id
                                       i_veto_ids    IN VARCHAR2, --被否决的台账id串
                                       i_ssic_id     IN VARCHAR2, --当前操作用户
                                       i_destination IN VARCHAR2, --下一任务名称
                                       o_ret_code    OUT VARCHAR2,
                                       o_msg         OUT VARCHAR2,
                                       o_mail_user   OUT VARCHAR2);

  PROCEDURE proc_get_app_info_list(i_app_id    IN VARCHAR2, --台账申请表id
                                   i_ssic_id   IN VARCHAR2, --当前操作用户
                                   i_stru_id   IN VARCHAR2, --当前操作用户所属机构
                                   i_beg_num   IN VARCHAR2,
                                   i_fetch_num IN VARCHAR2,
                                   o_ret_code  OUT VARCHAR2,
                                   o_msg       OUT VARCHAR2,
                                   o_total_num OUT VARCHAR2,
                                   o_info_list OUT SYS_REFCURSOR);

  PROCEDURE proc_get_audit_prepare_data(i_ssic_id          IN VARCHAR2, --用户id
                                        i_app_id           IN VARCHAR2, --台账申请id
                                        i_procinst_id      IN VARCHAR2, --待办表id
                                        i_role             IN VARCHAR2,
                                        o_ret_code         OUT VARCHAR2, --返回值
                                        o_role_stru        OUT VARCHAR2, --用户角色机构
                                        o_bnch_id          OUT VARCHAR2,
                                        o_bnch_name        OUT VARCHAR2,
                                        o_bnch_grade       OUT VARCHAR2,
                                        o_sup_stru         OUT VARCHAR2, --上级机构
                                        o_app_creator_user OUT VARCHAR2, --创建台账申请单的用户
                                        o_task_title       OUT VARCHAR2, --待办信息显示标题
                                        o_app_process      OUT VARCHAR2, --流程类型(1提交上级的内部审批流程2销账流程3删除流程)
                                        o_number           OUT VARCHAR2 --当前环节可操作的台账数量
                                        );

  FUNCTION func_format_content(i_content IN CLOB) RETURN CLOB;
  FUNCTION func_get_pro_info(i_rsk_chk_id IN VARCHAR2,
                             i_type       IN VARCHAR2) RETURN VARCHAR2;
  PROCEDURE proc_rsk_chk_export(i_ssic_id          IN VARCHAR2,
                                i_stru_id          IN VARCHAR2,
                                i_status           IN VARCHAR2,
                                i_event_stru       IN VARCHAR2,
                                i_submit_state     IN VARCHAR2,
                                i_process_state    IN VARCHAR2,
                                i_adopt_status     IN VARCHAR2, --纳入标识(0未纳入,1已纳入)
                                i_adopt_stru_grade IN VARCHAR2, --纳入层级(1总行,2一级行)
                                i_begin_time       IN VARCHAR2,
                                i_end_time         IN VARCHAR2,
                                o_ret_code         OUT VARCHAR2, --返回值
                                o_list             OUT SYS_REFCURSOR --查询列表游标
                                );

  PROCEDURE proc_add_remind_mail(i_ssic_id     IN VARCHAR2, --当前用户
                                 i_target_user IN VARCHAR2, --目标用户
                                 i_app_id      IN VARCHAR2, --app表app_id
                                 i_task_id     IN VARCHAR2, --待办表id
                                 i_title       IN VARCHAR2, --待办显示标题
                                 i_remind_type IN VARCHAR2, --流程步骤名
                                 i_audit_state IN VARCHAR2, --审核状态(01被否决02等待复核人审核03复核人审批通过04等待负责人审批05负责人审批通过99全部审批通过)
                                 --用于取台账信息列表
                                 i_sys_code IN VARCHAR2, --子系统编码
                                 o_ret_code OUT VARCHAR2,
                                 o_msg      OUT VARCHAR2);



  PROCEDURE proc_add_comment(i_app_id   IN VARCHAR2, --台账申请表id
                             i_veto_ids IN VARCHAR2, --被否决的台账id串
                             i_ssic_id  IN VARCHAR2, --当前操作用户
                             i_comments IN VARCHAR2,
                             o_ret_code OUT VARCHAR2,
                             o_msg      OUT VARCHAR2);

  PROCEDURE proc_add_operation_info(i_app_id       IN VARCHAR2, --台账申请表id
                                    i_veto_ids     IN VARCHAR2, --被否决的台账id串
                                    i_ssic_id      IN VARCHAR2, --当前操作用户
                                    i_target_user  IN VARCHAR2, --下一经办人
                                    i_process_step IN VARCHAR2, --当前流程的步骤：01提交到复核,02复核结束,提交到审批,03审批结束；11提交到审批
                                    o_ret_code     OUT VARCHAR2,
                                    o_msg          OUT VARCHAR2);

  PROCEDURE proc_get_comment_list(i_app_id       IN VARCHAR2, --台账申请表id
                                  i_ssic_id      IN VARCHAR2, --当前操作用户
                                  o_ret_code     OUT VARCHAR2,
                                  o_msg          OUT VARCHAR2,
                                  o_comment_list OUT SYS_REFCURSOR);

  PROCEDURE proc_get_comments_by_info(i_info_id       IN VARCHAR2, --台账申请表id
                                      i_app_id        IN VARCHAR2, --台账申请表id
                                      i_ssic_id       IN VARCHAR2, --当前操作用户
                                      i_flag          IN VARCHAR2, --标识符,用于判定取appid的数量,1取最新的一次申请,2取所有
                                      i_audit_state   IN VARCHAR2, --审核状态,一般为2审批中,4被否决
                                      i_process_state IN VARCHAR2, --流程状态类型,0非流程,1上报,2销账,3删除
                                      o_ret_code      OUT VARCHAR2,
                                      o_msg           OUT VARCHAR2,
                                      o_comment_list  OUT SYS_REFCURSOR,
                                      o_task_list     OUT SYS_REFCURSOR);

  PROCEDURE proc_sub_add_to_sup(i_rsk_chk_ids IN VARCHAR2, --台账ID串
                                i_ssic_id     IN VARCHAR2, --当前操作用户
                                i_stru_id     IN VARCHAR2, --所属机构
                                o_ret_code    OUT VARCHAR2,
                                o_msg         OUT VARCHAR2);



  PROCEDURE proc_update_info_status(i_ssic_id     IN VARCHAR2, --当前操作用户
                                    i_status      IN VARCHAR2, --台账状态,1有效2销账3删除
                                    i_rsk_chk_ids IN VARCHAR2, --申请涉及所有台账表id串
                                    o_ret_code    OUT VARCHAR2,
                                    o_msg         OUT VARCHAR2);


  PROCEDURE proc_submit_application(i_info_json     IN CLOB, --台账ID串
                                    i_cur_ssic_id   IN VARCHAR2, --当前操作用户
                                    i_process_state IN VARCHAR2, --流程类型(1提交上级的内部审批流程2销账流程3删除流程)
                                    o_ret_code      OUT VARCHAR2,
                                    o_msg           OUT VARCHAR2,
                                    o_app_id        OUT VARCHAR2);


  PROCEDURE proc_update_app_task(i_app_id      IN VARCHAR2, --申请表id
                                 i_cur_ssic_id IN VARCHAR2, --当前操作用户
                                 o_ret_code    OUT VARCHAR2,
                                 o_msg         OUT VARCHAR2);

  PROCEDURE proc_get_audit_info_list(i_app_id    IN VARCHAR2, --台账申请表id
                                     i_ssic_id   IN VARCHAR2, --当前操作用户
                                     i_stru_id   IN VARCHAR2, --当前操作用户所属机构
                                     i_is_over   IN VARCHAR2,
                                     i_beg_num   IN VARCHAR2,
                                     i_fetch_num IN VARCHAR2,
                                     o_ret_code  OUT VARCHAR2,
                                     o_msg       OUT VARCHAR2,
                                     o_total_num OUT VARCHAR2,
                                     o_info_list OUT SYS_REFCURSOR);


  PROCEDURE proc_get_app_list(i_ssic_id    IN VARCHAR2, --当前操作用户
                              i_rsk_chk_id IN VARCHAR2, --台账信息表infoId
                              i_beg_num    IN VARCHAR2,
                              i_fetch_num  IN VARCHAR2,
                              o_ret_code   OUT VARCHAR2,
                              o_msg        OUT VARCHAR2,
                              o_total_num  OUT VARCHAR2,
                              o_info_list  OUT SYS_REFCURSOR);


  PROCEDURE proc_validate_info_state(i_rsk_chk_ids   IN VARCHAR2, --台账ID
                                     i_ssic_id       IN VARCHAR2, --当前操作用户
                                     i_process_state IN VARCHAR2, --流程类型(1提交上级的内部审批流程2销账流程3删除流程)
                                     i_stru_id       IN VARCHAR2, --创建申请的机构
                                     o_ret_code      OUT VARCHAR2,
                                     o_msg           OUT VARCHAR2,
                                     o_z_count       OUT VARCHAR2, --总行纳入条数
                                     o_y_count       OUT VARCHAR2, --一级行纳入条数
                                     o_z_submit      OUT VARCHAR2, --提交总行条数
                                     o_y_submit      OUT VARCHAR2 --提交一级行条数
                                     );

  PROCEDURE proc_direct_delete(i_info_json   IN CLOB, --台账信息json[{rsk_chk_id:***,reason:×××},{}...]
                               i_cur_ssic_id IN VARCHAR2, --当前操作用户
                               --i_process_state IN VARCHAR2, --流程类型(3直接删除,置状态位,不用走流程)
                               o_ret_code OUT VARCHAR2,
                               o_msg      OUT VARCHAR2,
                               o_app_id   OUT VARCHAR2);

  PROCEDURE proc_fxpc_deptuser_qry(i_ssic_id          IN VARCHAR2,
                                   i_stru_id          IN VARCHAR2,
                                   i_jbpm_business_id IN VARCHAR2,
                                   o_ret_code         OUT VARCHAR2,
                                   o_msg              OUT VARCHAR2,
                                   o_user_list        OUT SYS_REFCURSOR --人员列表
                                   );

  PROCEDURE proc_fxpc_qry_yz_user(i_rsk_chk_ids IN VARCHAR2, --台账ID
                                  i_stru_id     IN VARCHAR2,
                                  o_ret_code    OUT VARCHAR2,
                                  o_yz_users    OUT VARCHAR2,
                                  o_msg         OUT VARCHAR2);


  PROCEDURE proc_sup_return_to_sub(i_info_json IN CLOB, --台账信息json[{rsk_chk_id:***,reason:×××},{}...]
                                   i_ssic_id   IN VARCHAR2, --当前操作用户
                                   i_stru_id   IN VARCHAR2, --所属机构
                                   o_ret_code  OUT VARCHAR2,
                                   o_msg       OUT VARCHAR2,
                                   o_mail_user OUT VARCHAR2);

  PROCEDURE proc_get_submit_history_list(i_rsk_chk_id IN VARCHAR2, --台账信息表infoId
                                         i_beg_num    IN VARCHAR2,
                                         i_fetch_num  IN VARCHAR2,
                                         o_ret_code   OUT VARCHAR2,
                                         o_msg        OUT VARCHAR2,
                                         o_total_num  OUT VARCHAR2,
                                         o_info_list  OUT SYS_REFCURSOR);

  PROCEDURE proc_get_submit_differences(i_rsk_chk_id IN VARCHAR2, --台账信息表infoId
                                        o_ret_code   OUT VARCHAR2,
                                        o_msg        OUT VARCHAR2,
                                        o_new_list   OUT SYS_REFCURSOR,
                                        o_old_list   OUT SYS_REFCURSOR);

END pckg_ab_rsk_chk;
//

CREATE OR REPLACE PACKAGE pckg_ab_report IS


  PROCEDURE proc_prepare_data(i_ssic_id        IN VARCHAR2,
                              i_role_stru      IN VARCHAR2,
                              o_ret_code       OUT VARCHAR2, --返回值
                              o_role_stru      OUT VARCHAR2,
                              o_role_stru_name OUT VARCHAR2,
                              o_bnch_id        OUT VARCHAR2,
                              o_bnch_name      OUT VARCHAR2,
                              o_bnch_grade     OUT VARCHAR2);

  PROCEDURE proc_save(i_applicant          IN VARCHAR2, --申请人
                      i_tel                IN VARCHAR2, --联系电话
                      i_app_stru           IN VARCHAR2, --申请部门
                      i_business_id        IN VARCHAR2,
                      i_title              IN VARCHAR2,
                      i_content            IN CLOB,
                      i_attach_file_ref_id IN VARCHAR2,
                      o_ret_code           OUT VARCHAR2,
                      o_msg                OUT VARCHAR2,
                      o_appid              OUT VARCHAR2);

  PROCEDURE proc_get_detail(i_ssic_id     IN VARCHAR2, --统一认证号
                            i_business_id IN VARCHAR2,
                            o_ret_code    OUT VARCHAR2, --返回值
                            o_list_cur    OUT SYS_REFCURSOR);

  PROCEDURE proc_submit(i_applicant          IN VARCHAR2, --申请人
                        i_tel                IN VARCHAR2, --联系电话
                        i_app_stru           IN VARCHAR2, --申请部门
                        i_business_id        IN VARCHAR2,
                        i_title              IN VARCHAR2,
                        i_content            IN CLOB,
                        i_attach_file_ref_id IN VARCHAR2,
                        o_ret_code           OUT VARCHAR2,
                        o_msg                OUT VARCHAR2,
                        o_appid              OUT VARCHAR2);

  PROCEDURE proc_audit(i_jbpm_business_id           IN VARCHAR2, --业务ID
                       i_jbpm_next_activity_id      IN VARCHAR2, --目标任务
                       i_jbpm_destination_task_name IN VARCHAR2,
                       i_jbpm_activity_id           IN VARCHAR2,
                       i_jbpm_task_id               IN VARCHAR2,
                       i_audit_user                 IN VARCHAR2,
                       i_jbpm_transition_name       IN VARCHAR2,
                       o_ret_code                   OUT VARCHAR2,
                       o_msg                        OUT VARCHAR2,
                       o_jbpm_business_title        OUT VARCHAR2);

END pckg_ab_report;
//
CREATE OR REPLACE PACKAGE BODY pckg_ab_report IS

  /******************************************************************************
      --存储过程名：    proc_prepare_data
      --存储过程描述：  准备数据
      --功能模块：      准备数据
      --作者：          kfzx-linlc
      --时间：          2015-11-27
      --修改历史:
  ******************************************************************************/
  PROCEDURE proc_prepare_data(i_ssic_id        IN VARCHAR2,
                              i_role_stru      IN VARCHAR2,
                              o_ret_code       OUT VARCHAR2, --返回值
                              o_role_stru      OUT VARCHAR2,
                              o_role_stru_name OUT VARCHAR2,
                              o_bnch_id        OUT VARCHAR2,
                              o_bnch_name      OUT VARCHAR2,
                              o_bnch_grade     OUT VARCHAR2) IS
    v_param     db_log.info%TYPE := i_ssic_id || '|' || i_role_stru;
    v_proc_name db_log.proc_name%TYPE := 'pckg_ab_report.proc_prepare_data';

  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step,
                  pack_log.start_msg || v_param);
    o_ret_code := 0;

    IF i_role_stru IS NOT NULL THEN
      -- 多机构
      o_role_stru := i_role_stru;
    ELSE
      SELECT MAX(t.stru_id)
        INTO o_role_stru
        FROM dams_user_role_rel t
       WHERE t.ssic_id = i_ssic_id
         AND t.role_id = 'AB000'
         AND t.sys_code = 'AB';
    END IF;
    SELECT b.stru_sname
      INTO o_role_stru_name
      FROM dams_branch b
     WHERE b.stru_id = o_role_stru;

    -- 获取所属行 及 stru_grade
    SELECT c.major_stru_id, b.stru_grade, b.stru_sname
      INTO o_bnch_id, o_bnch_grade, o_bnch_name
      FROM dams_branch_catalog c, dams_branch b
     WHERE c.stru_id = o_role_stru
       AND c.major_stru_id = b.stru_id(+);

  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_ret_code := '-1';
      RETURN;
  END proc_prepare_data;

  /******************************************************************************
      --存储过程名：    proc_save
      --存储过程描述：  保存
      --功能模块：      保存
      --作者：          kfzx-linlc
      --时间：          2015-09-17
      --修改历史:
  ******************************************************************************/
  PROCEDURE proc_save(i_applicant          IN VARCHAR2, --申请人
                      i_tel                IN VARCHAR2, --联系电话
                      i_app_stru           IN VARCHAR2, --申请部门
                      i_business_id        IN VARCHAR2,
                      i_title              IN VARCHAR2,
                      i_content            IN CLOB,
                      i_attach_file_ref_id IN VARCHAR2,
                      o_ret_code           OUT VARCHAR2,
                      o_msg                OUT VARCHAR2,
                      o_appid              OUT VARCHAR2) IS
    v_param       db_log.info%TYPE := substr(i_applicant || '|' || i_tel || '|' ||
                                             i_app_stru || '|' || i_business_id || '|' ||
                                             i_title || '|' || i_content || '|' ||
                                             i_attach_file_ref_id, 1, 3000);
    v_proc_name   db_log.proc_name%TYPE := 'pckg_ab_report.proc_save';
    v_business_id VARCHAR2(20) := i_business_id;
    v_sysdate     DATE := SYSDATE;
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step,
                  pack_log.start_msg || v_param);
    o_ret_code := 0;

    IF v_business_id IS NULL THEN
      v_business_id := pckg_dams_util.func_gen_seq('SYBG', 'AB_REPORT_SEQ', 8);
	 
    END IF;

    MERGE INTO dams_notice_report_drf t
    USING (SELECT v_business_id business_id FROM dual) tmp
    ON (t.business_id = tmp.business_id)
    WHEN MATCHED THEN
      UPDATE
         SET t.applicant          = i_applicant,
             t.app_stru           = i_app_stru,
             t.tel                = i_tel,
             t.save_time          = v_sysdate,
             t.title              = i_title,
             t.content            = i_content,
             t.attach_file_ref_id = i_attach_file_ref_id
    WHEN NOT MATCHED THEN
      INSERT
        (business_id,
         valid_flag,
         applicant,
         app_stru,
         tel,
         save_time,
         title,
         content,
         attach_file_ref_id,
         sys_code,
         biz_type)
      VALUES
        (v_business_id,
         '',
         i_applicant,
         i_app_stru,
         i_tel,
         v_sysdate,
         i_title,
         i_content,
         i_attach_file_ref_id,
         'AB',
         'AB05');

    DELETE FROM dams_save_info t WHERE t.app_id = v_business_id;
    INSERT INTO dams_save_info
      (app_id, applyer, app_stru, app_time, biz_type, info, pdid, form_id)
    VALUES
      (v_business_id,
       i_applicant,
       i_app_stru,
       SYSDATE,
       'AB05',
       '[草稿]中国工商银行声誉风险报告单',
       'ab_report-1',
       'abReportForm');

    o_appid := v_business_id;

    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_ret_code := -1;
  END proc_save;

  /******************************************************************************
      --存储过程名：    proc_get_detail
      --存储过程描述：
      --功能模块：      申请查询
      --作者：          kfzx-linlc
      --时间：          2015-09-17
      --修改历史:
  ******************************************************************************/
  PROCEDURE proc_get_detail(i_ssic_id     IN VARCHAR2, --统一认证号
                            i_business_id IN VARCHAR2,
                            o_ret_code    OUT VARCHAR2, --返回值
                            o_list_cur    OUT SYS_REFCURSOR) IS
    v_param     db_log.info%TYPE := i_ssic_id || '|' || i_business_id;
    v_proc_name db_log.proc_name%TYPE := 'pckg_ab_report.proc_get_detail';
    v_count     NUMBER(5);
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step,
                  pack_log.start_msg || v_param);
    o_ret_code := '0';

    SELECT COUNT(1)
      INTO v_count
      FROM dams_notice_report_app t
     WHERE t.business_id = i_business_id;

    IF v_count > 0 THEN
      OPEN o_list_cur FOR
        SELECT business_id,
               applicant,
               (SELECT u.name FROM dams_user u WHERE u.ssic_id = t.applicant) applicant_name,
               app_stru,
               (SELECT b1.stru_sname
                  FROM dams_branch b1
                 WHERE b1.stru_id = t.app_stru) app_stru_name,
               c.major_stru_id bnch_id,
               db.stru_sname bnch_name,
               db.stru_grade bnch_grade,
               tel,
               title,
               content,
               attach_file_ref_id
          FROM dams_notice_report_app t, dams_branch_catalog c, dams_branch db
         WHERE t.business_id = i_business_id
           AND t.app_stru = c.stru_id(+)
           AND c.major_stru_id = db.stru_id(+);

    ELSE
      OPEN o_list_cur FOR
        SELECT business_id,
               applicant,
               (SELECT u.name FROM dams_user u WHERE u.ssic_id = t.applicant) applicant_name,
               app_stru,
               (SELECT b1.stru_sname
                  FROM dams_branch b1
                 WHERE b1.stru_id = t.app_stru) app_stru_name,
               c.major_stru_id bnch_id,
               db.stru_sname bnch_name,
               db.stru_grade bnch_grade,
               tel,
               title,
               content,
               attach_file_ref_id
          FROM dams_notice_report_drf t, dams_branch_catalog c, dams_branch db
         WHERE t.business_id = i_business_id
           AND t.app_stru = c.stru_id(+)
           AND c.major_stru_id = db.stru_id(+);

    END IF;

  EXCEPTION
    WHEN OTHERS THEN
      IF o_list_cur%ISOPEN THEN
        CLOSE o_list_cur;
      END IF;
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_ret_code := '-1';
      RETURN;
  END proc_get_detail;

  /******************************************************************************
      --存储过程名：    proc_submit
      --存储过程描述：  保存
      --功能模块：      保存
      --作者：          kfzx-linlc
      --时间：          2015-09-17
      --修改历史:
  ******************************************************************************/
  PROCEDURE proc_submit(i_applicant          IN VARCHAR2, --申请人
                        i_tel                IN VARCHAR2, --联系电话
                        i_app_stru           IN VARCHAR2, --申请部门
                        i_business_id        IN VARCHAR2,
                        i_title              IN VARCHAR2,
                        i_content            IN CLOB,
                        i_attach_file_ref_id IN VARCHAR2,
                        o_ret_code           OUT VARCHAR2,
                        o_msg                OUT VARCHAR2,
                        o_appid              OUT VARCHAR2) IS
    v_param       db_log.info%TYPE := substr(i_applicant || '|' || i_tel || '|' ||
                                             i_app_stru || '|' || i_business_id || '|' ||
                                             i_title || '|' || i_content || '|' ||
                                             i_attach_file_ref_id, 1, 3000);
    v_proc_name   db_log.proc_name%TYPE := 'pckg_ab_report.proc_submit';
    v_business_id VARCHAR2(20) := i_business_id;
    v_sysdate     DATE := SYSDATE;
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step,
                  pack_log.start_msg || v_param);
    o_ret_code := 0;

    IF v_business_id IS NULL THEN
      v_business_id := pckg_dams_util.func_gen_seq('SYBG', 'AB_REPORT_SEQ', 8);
	  
    END IF;

    -- 删除草稿
    DELETE FROM dams_save_info a WHERE a.app_id = v_business_id;
    DELETE FROM dams_notice_report_drf t WHERE t.business_id = v_business_id;

    MERGE INTO dams_notice_report_app t
    USING (SELECT v_business_id business_id FROM dual) tmp
    ON (t.business_id = tmp.business_id)
    WHEN MATCHED THEN
      UPDATE
         SET t.applicant          = i_applicant,
             t.app_stru           = i_app_stru,
             t.tel                = i_tel,
             t.update_time        = v_sysdate,
             t.title              = i_title,
             t.content            = i_content,
             t.attach_file_ref_id = i_attach_file_ref_id
    WHEN NOT MATCHED THEN
      INSERT
        (business_id,
         valid_flag,
         applicant,
         app_stru,
         tel,
         create_time,
         update_time,
         title,
         content,
         attach_file_ref_id,
         sys_code,
         biz_type)
      VALUES
        (v_business_id,
         '',
         i_applicant,
         i_app_stru,
         i_tel,
         v_sysdate,
         v_sysdate,
         i_title,
         i_content,
         i_attach_file_ref_id,
         'AB',
         'AB05');

    o_appid := v_business_id;

    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_ret_code := -1;
  END proc_submit;

  /******************************************************************************
      --存储过程名：    proc_audit
      --存储过程描述：  审批
      --功能模块：      审批
      --作者：          kfzx-linlc
      --时间：          2015-09-17
      --修改历史:
  ******************************************************************************/
  PROCEDURE proc_audit(i_jbpm_business_id           IN VARCHAR2, --业务ID
                       i_jbpm_next_activity_id      IN VARCHAR2, --目标任务
                       i_jbpm_destination_task_name IN VARCHAR2,
                       i_jbpm_activity_id           IN VARCHAR2,
                       i_jbpm_task_id               IN VARCHAR2,
                       i_audit_user                 IN VARCHAR2,
                       i_jbpm_transition_name       IN VARCHAR2,
                       o_ret_code                   OUT VARCHAR2,
                       o_msg                        OUT VARCHAR2,
                       o_jbpm_business_title        OUT VARCHAR2) IS
    v_param     db_log.info%TYPE := substr(i_jbpm_business_id || '|' ||
                                           i_jbpm_next_activity_id || '|' ||
                                           i_jbpm_destination_task_name || '|' ||
                                           i_jbpm_activity_id || '|' ||
                                           i_jbpm_task_id || '|' ||
                                           i_audit_user || '|' ||
                                           i_jbpm_transition_name, 1, 3000);
    v_proc_name db_log.proc_name%TYPE := 'pckg_ab_report.proc_audit';
    v_sysdate   DATE := SYSDATE;
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step,
                  pack_log.start_msg || v_param);
    o_ret_code := 0;

    -- 结束更新close_time
    IF i_jbpm_destination_task_name = '结束' THEN
      UPDATE dams_notice_report_app t
         SET t.close_time = v_sysdate
       WHERE t.business_id = i_jbpm_business_id;
    END IF;

    SELECT t.title
      INTO o_jbpm_business_title
      FROM dams_wf_process t
     WHERE t.business_id = i_jbpm_business_id;

    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_ret_code := -1;
  END proc_audit;

END pckg_ab_report;
//




CREATE OR REPLACE PACKAGE pckg_ab_exercise IS

  PROCEDURE proc_prepare_data(i_ssic_id        IN VARCHAR2,
                              i_role_stru      IN VARCHAR2,
                              o_ret_code       OUT VARCHAR2, --返回值
                              o_role_stru      OUT VARCHAR2,
                              o_role_stru_name OUT VARCHAR2,
                              o_bnch_id        OUT VARCHAR2,
                              o_bnch_name      OUT VARCHAR2,
                              o_bnch_grade     OUT VARCHAR2);

  PROCEDURE proc_save(i_applicant            IN VARCHAR2, --申请人
                      i_tel                  IN VARCHAR2, --联系电话
                      i_app_stru             IN VARCHAR2, --申请部门
                      i_business_id          IN VARCHAR2,
                      i_stru_id              IN VARCHAR2,
                      i_exe_time             IN VARCHAR2,
                      i_exe_place            IN VARCHAR2,
                      i_people_or_num        IN VARCHAR2,
                      i_content              IN VARCHAR2,
                      i_purpose              IN VARCHAR2,
                      i_procedure            IN VARCHAR2,
                      i_summery              IN VARCHAR2,
                      i_problem_and_solution IN VARCHAR2,
                      i_attach_file_ref_id   IN VARCHAR2,
                      i_remark               IN VARCHAR2,
                      i_attend_stru          IN CLOB,
                      o_ret_code             OUT VARCHAR2,
                      o_msg                  OUT VARCHAR2,
                      o_appid                OUT VARCHAR2);

  PROCEDURE proc_get_detail(i_ssic_id     IN VARCHAR2, --统一认证号
                            i_business_id IN VARCHAR2,
                            o_ret_code    OUT VARCHAR2, --返回值
                            o_list_cur    OUT SYS_REFCURSOR, --查询列表游标
                            o_stru_list   OUT SYS_REFCURSOR);
  PROCEDURE proc_submit(i_applicant            IN VARCHAR2, --申请人
                        i_tel                  IN VARCHAR2, --联系电话
                        i_app_stru             IN VARCHAR2, --申请部门
                        i_business_id          IN VARCHAR2,
                        i_stru_id              IN VARCHAR2,
                        i_exe_time             IN VARCHAR2,
                        i_exe_place            IN VARCHAR2,
                        i_people_or_num        IN VARCHAR2,
                        i_content              IN VARCHAR2,
                        i_purpose              IN VARCHAR2,
                        i_procedure            IN VARCHAR2,
                        i_summery              IN VARCHAR2,
                        i_problem_and_solution IN VARCHAR2,
                        i_attach_file_ref_id   IN VARCHAR2,
                        i_remark               IN VARCHAR2,
                        i_attend_stru          IN CLOB,
                        o_ret_code             OUT VARCHAR2,
                        o_msg                  OUT VARCHAR2,
                        o_appid                OUT VARCHAR2);
  PROCEDURE proc_audit(i_jbpm_business_id           IN VARCHAR2, --业务ID
                       i_jbpm_next_activity_id      IN VARCHAR2, --目标任务
                       i_jbpm_destination_task_name IN VARCHAR2,
                       i_jbpm_activity_id           IN VARCHAR2,
                       i_jbpm_task_id               IN VARCHAR2,
                       i_audit_user                 IN VARCHAR2,
                       i_jbpm_transition_name       IN VARCHAR2,
                       o_ret_code                   OUT VARCHAR2,
                       o_msg                        OUT VARCHAR2,
                       o_jbpm_business_title        OUT VARCHAR2);
END pckg_ab_exercise;
//
CREATE OR REPLACE PACKAGE BODY pckg_ab_exercise IS


  PROCEDURE proc_prepare_data(i_ssic_id        IN VARCHAR2,
                              i_role_stru      IN VARCHAR2,
                              o_ret_code       OUT VARCHAR2, --返回值
                              o_role_stru      OUT VARCHAR2,
                              o_role_stru_name OUT VARCHAR2,
                              o_bnch_id        OUT VARCHAR2,
                              o_bnch_name      OUT VARCHAR2,
                              o_bnch_grade     OUT VARCHAR2) IS
    v_param     db_log.info%TYPE := i_ssic_id || '|' || i_role_stru;
    v_proc_name db_log.proc_name%TYPE := 'pckg_ab_exercise.proc_prepare_data';

  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step,
                  pack_log.start_msg || v_param);
    o_ret_code := 0;

    IF i_role_stru IS NOT NULL THEN
      -- 多机构
      o_role_stru := i_role_stru;
    ELSE
      SELECT MAX(t.stru_id)
        INTO o_role_stru
        FROM dams_user_role_rel t
       WHERE t.ssic_id = i_ssic_id
         AND t.role_id = 'AB000'
         AND t.sys_code = 'AB';
    END IF;
    SELECT b.stru_sname
      INTO o_role_stru_name
      FROM dams_branch b
     WHERE b.stru_id = o_role_stru;

    -- 获取所属行 及 stru_grade
    SELECT c.major_stru_id, b.stru_grade, b.stru_sname
      INTO o_bnch_id, o_bnch_grade, o_bnch_name
      FROM dams_branch_catalog c, dams_branch b
     WHERE c.stru_id = o_role_stru
       AND c.major_stru_id = b.stru_id(+);

  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_ret_code := '-1';
      RETURN;
  END proc_prepare_data;

  /******************************************************************************
      --存储过程名：    proc_save
      --存储过程描述：  保存
      --功能模块：      保存
      --作者：          kfzx-linlc
      --时间：          2015-09-17
      --修改历史:
  ******************************************************************************/
  PROCEDURE proc_save(i_applicant            IN VARCHAR2, --申请人
                      i_tel                  IN VARCHAR2, --联系电话
                      i_app_stru             IN VARCHAR2, --申请部门
                      i_business_id          IN VARCHAR2,
                      i_stru_id              IN VARCHAR2,
                      i_exe_time             IN VARCHAR2,
                      i_exe_place            IN VARCHAR2,
                      i_people_or_num        IN VARCHAR2,
                      i_content              IN VARCHAR2,
                      i_purpose              IN VARCHAR2,
                      i_procedure            IN VARCHAR2,
                      i_summery              IN VARCHAR2,
                      i_problem_and_solution IN VARCHAR2,
                      i_attach_file_ref_id   IN VARCHAR2,
                      i_remark               IN VARCHAR2,
                      i_attend_stru          IN CLOB,
                      o_ret_code             OUT VARCHAR2,
                      o_msg                  OUT VARCHAR2,
                      o_appid                OUT VARCHAR2) IS
    v_param       db_log.info%TYPE := substr(i_applicant || '|' || i_tel || '|' ||
                                             i_app_stru || '|' || i_business_id || '|' ||
                                             i_stru_id || '|' || i_exe_time || '|' ||
                                             i_exe_place || '|' ||
                                             i_people_or_num || '|' ||
                                             i_content || '|' || i_purpose || '|' ||
                                             i_procedure || '|' || i_summery || '|' ||
                                             i_problem_and_solution || '|' ||
                                             i_attach_file_ref_id || '|' ||
                                             i_remark, 1, 3000);
    v_proc_name   db_log.proc_name%TYPE := 'pckg_ab_exercise.proc_save';
    v_business_id VARCHAR2(20) := i_business_id;
    v_sysdate     DATE := SYSDATE;
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step,
                  pack_log.start_msg || v_param);
    o_ret_code := 0;

    IF v_business_id IS NULL THEN
      v_business_id := pckg_dams_util.func_gen_seq('YJYL', 'AB_EXERCISE_SEQ', 8);
	  NULL;
    END IF;

    MERGE INTO ab_exercise_drf t
    USING (SELECT v_business_id business_id FROM dual) tmp
    ON (t.business_id = tmp.business_id)
    WHEN MATCHED THEN
      UPDATE
         SET t.applicant            = i_applicant,
             t.app_stru             = i_app_stru,
             t.save_time            = v_sysdate,
             t.stru_id              = i_stru_id,
             t.exe_time             = to_date(i_exe_time, 'YYYY-MM-DD'),
             t.exe_place            = i_exe_place,
             t.people_or_num        = i_people_or_num,
             t.content              = i_content,
             t.purpose              = i_purpose,
             t.procedure            = i_procedure,
             t.summery              = i_summery,
             t.problem_and_solution = i_problem_and_solution,
             t.attach_file_ref_id   = i_attach_file_ref_id,
             t.remark               = i_remark,
             t.attend_stru          = i_attend_stru
    WHEN NOT MATCHED THEN
      INSERT
        (business_id,
         valid_flag,
         applicant,
         app_stru,
         save_time,
         stru_id,
         exe_time,
         exe_place,
         people_or_num,
         content,
         purpose,
         PROCEDURE,
         summery,
         problem_and_solution,
         attach_file_ref_id,
         remark,
         attend_stru)
      VALUES
        (v_business_id,
         '',
         i_applicant,
         i_app_stru,
         v_sysdate,
         i_stru_id,
         to_date(i_exe_time, 'YYYY-MM-DD'),
         i_exe_place,
         i_people_or_num,
         i_content,
         i_purpose,
         i_procedure,
         i_summery,
         i_problem_and_solution,
         i_attach_file_ref_id,
         i_remark,
         i_attend_stru);

    /*
    -- 参与机构
    UPDATE ab_exercise_stru t
       SET t.status = '0', t.update_time = v_sysdate
     WHERE t.business_id = v_business_id
       AND t.status = '1'
       AND t.stru_id NOT IN
           (SELECT column_value FROM TABLE(func_split2(i_attend_stru)));
    MERGE INTO ab_exercise_stru t
    USING (SELECT column_value stru_id FROM TABLE(func_split2(i_attend_stru))) tmp
    ON (t.business_id = v_business_id AND t.stru_id = tmp.stru_id)
    WHEN MATCHED THEN
      UPDATE SET t.status = '1', t.update_time = v_sysdate WHERE t.status = '0'
    WHEN NOT MATCHED THEN
      INSERT
        (business_id, stru_id, status, update_time)
      VALUES
        (v_business_id, tmp.stru_id, '1', v_sysdate);
        */

    --
    DELETE FROM dams_save_info t WHERE t.app_id = v_business_id;
    INSERT INTO dams_save_info
      (app_id, applyer, app_stru, app_time, biz_type, info, pdid, form_id)
    VALUES
      (v_business_id,
       i_applicant,
       i_app_stru,
       SYSDATE,
       'AB03',
       '[草稿]中国工商银行应急预案演练记录表',
       'ab_exercise-1',
       'abExerciseForm');

    o_appid := v_business_id;

    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_ret_code := -1;
  END proc_save;

  /******************************************************************************
      --存储过程名：    proc_get_detail
      --存储过程描述：
      --功能模块：      申请查询
      --作者：          kfzx-linlc
      --时间：          2015-09-17
      --修改历史:
  ******************************************************************************/
  PROCEDURE proc_get_detail(i_ssic_id     IN VARCHAR2, --统一认证号
                            i_business_id IN VARCHAR2,
                            o_ret_code    OUT VARCHAR2, --返回值
                            o_list_cur    OUT SYS_REFCURSOR, --查询列表游标
                            o_stru_list   OUT SYS_REFCURSOR) IS
    v_param     db_log.info%TYPE := i_ssic_id || '|' || i_business_id;
    v_proc_name db_log.proc_name%TYPE := 'pckg_ab_exercise.proc_get_detail';
    v_count     NUMBER(5);
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step,
                  pack_log.start_msg || v_param);
    o_ret_code := '0';

    SELECT COUNT(1)
      INTO v_count
      FROM ab_exercise_app t
     WHERE t.business_id = i_business_id;

    IF v_count > 0 THEN
      OPEN o_list_cur FOR
        SELECT business_id,
               applicant,
               (SELECT u.name FROM dams_user u WHERE u.ssic_id = t.applicant) applicant_name,
               app_stru,
               (SELECT b1.stru_sname
                  FROM dams_branch b1
                 WHERE b1.stru_id = t.app_stru) app_stru_name,
               c.major_stru_id bnch_id,
               db.stru_sname bnch_name,
               db.stru_grade bnch_grade,
               t.stru_id,
               (SELECT b2.stru_sname
                  FROM dams_branch b2
                 WHERE b2.stru_id = t.stru_id) stru_name,
               to_char(exe_time, 'YYYY-MM-DD') exe_time,
               exe_place,
               people_or_num,
               content,
               purpose,
               PROCEDURE,
               summery,
               problem_and_solution,
               attach_file_ref_id,
               remark,
               t.attend_stru
          FROM ab_exercise_app t, dams_branch_catalog c, dams_branch db
         WHERE t.business_id = i_business_id
           AND t.app_stru = c.stru_id(+)
           AND c.major_stru_id = db.stru_id(+);
    ELSE
      OPEN o_list_cur FOR
        SELECT business_id,
               applicant,
               (SELECT u.name FROM dams_user u WHERE u.ssic_id = t.applicant) applicant_name,
               app_stru,
               (SELECT b1.stru_sname
                  FROM dams_branch b1
                 WHERE b1.stru_id = t.app_stru) app_stru_name,
               c.major_stru_id bnch_id,
               db.stru_sname bnch_name,
               db.stru_grade bnch_grade,
               t.stru_id,
               (SELECT b2.stru_sname
                  FROM dams_branch b2
                 WHERE b2.stru_id = t.stru_id) stru_name,
               to_char(exe_time, 'YYYY-MM-DD') exe_time,
               exe_place,
               people_or_num,
               content,
               purpose,
               PROCEDURE,
               summery,
               problem_and_solution,
               attach_file_ref_id,
               remark,
               t.attend_stru
          FROM ab_exercise_drf t, dams_branch_catalog c, dams_branch db
         WHERE t.business_id = i_business_id
           AND t.app_stru = c.stru_id(+)
           AND c.major_stru_id = db.stru_id(+);
    END IF;

    /*
      OPEN o_stru_list FOR
        SELECT stru_id,
               (SELECT b.stru_sname FROM dams_branch b WHERE b.stru_id = t.stru_id) stru_name
          FROM ab_exercise_stru t
         WHERE t.business_id = i_business_id
           AND status = '1';
    */
  EXCEPTION
    WHEN OTHERS THEN
      IF o_list_cur%ISOPEN THEN
        CLOSE o_list_cur;
      END IF;
      IF o_stru_list%ISOPEN THEN
        CLOSE o_stru_list;
      END IF;
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_ret_code := '-1';
      RETURN;
  END proc_get_detail;

  /******************************************************************************
      --存储过程名：    proc_submit
      --存储过程描述：  保存
      --功能模块：      保存
      --作者：          kfzx-linlc
      --时间：          2015-09-17
      --修改历史:
  ******************************************************************************/
  PROCEDURE proc_submit(i_applicant            IN VARCHAR2, --申请人
                        i_tel                  IN VARCHAR2, --联系电话
                        i_app_stru             IN VARCHAR2, --申请部门
                        i_business_id          IN VARCHAR2,
                        i_stru_id              IN VARCHAR2,
                        i_exe_time             IN VARCHAR2,
                        i_exe_place            IN VARCHAR2,
                        i_people_or_num        IN VARCHAR2,
                        i_content              IN VARCHAR2,
                        i_purpose              IN VARCHAR2,
                        i_procedure            IN VARCHAR2,
                        i_summery              IN VARCHAR2,
                        i_problem_and_solution IN VARCHAR2,
                        i_attach_file_ref_id   IN VARCHAR2,
                        i_remark               IN VARCHAR2,
                        i_attend_stru          IN CLOB,
                        o_ret_code             OUT VARCHAR2,
                        o_msg                  OUT VARCHAR2,
                        o_appid                OUT VARCHAR2) IS
    v_param       db_log.info%TYPE := substr(i_applicant || '|' || i_tel || '|' ||
                                             i_app_stru || '|' || i_business_id || '|' ||
                                             i_stru_id || '|' || i_exe_time || '|' ||
                                             i_exe_place || '|' ||
                                             i_people_or_num || '|' ||
                                             i_content || '|' || i_purpose || '|' ||
                                             i_procedure || '|' || i_summery || '|' ||
                                             i_problem_and_solution || '|' ||
                                             i_attach_file_ref_id || '|' ||
                                             i_remark, 1, 3000);
    v_proc_name   db_log.proc_name%TYPE := 'pckg_ab_exercise.proc_submit';
    v_business_id VARCHAR2(20) := i_business_id;
    v_sysdate     DATE := SYSDATE;
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step,
                  pack_log.start_msg || v_param);
    o_ret_code := 0;

    IF v_business_id IS NULL THEN
      v_business_id := pckg_dams_util.func_gen_seq('YJYL', 'AB_EXERCISE_SEQ', 8);

    END IF;

    -- 删除草稿
    DELETE FROM dams_save_info a WHERE a.app_id = v_business_id;
    DELETE FROM ab_exercise_drf t WHERE t.business_id = v_business_id;

    MERGE INTO ab_exercise_app t
    USING (SELECT v_business_id business_id FROM dual) tmp
    ON (t.business_id = tmp.business_id)
    WHEN MATCHED THEN
      UPDATE
         SET t.applicant            = i_applicant,
             t.app_stru             = i_app_stru,
             t.update_time          = v_sysdate,
             t.stru_id              = i_stru_id,
             t.exe_time             = to_date(i_exe_time, 'YYYY-MM-DD'),
             t.exe_place            = i_exe_place,
             t.people_or_num        = i_people_or_num,
             t.content              = i_content,
             t.purpose              = i_purpose,
             t.procedure            = i_procedure,
             t.summery              = i_summery,
             t.problem_and_solution = i_problem_and_solution,
             t.attach_file_ref_id   = i_attach_file_ref_id,
             t.remark               = i_remark
    WHEN NOT MATCHED THEN
      INSERT
        (business_id,
         valid_flag,
         applicant,
         app_stru,
         create_time,
         update_time,
         stru_id,
         exe_time,
         exe_place,
         people_or_num,
         content,
         purpose,
         PROCEDURE,
         summery,
         problem_and_solution,
         attach_file_ref_id,
         remark,
         attend_stru)
      VALUES
        (v_business_id,
         '',
         i_applicant,
         i_app_stru,
         v_sysdate,
         v_sysdate,
         i_stru_id,
         to_date(i_exe_time, 'YYYY-MM-DD'),
         i_exe_place,
         i_people_or_num,
         i_content,
         i_purpose,
         i_procedure,
         i_summery,
         i_problem_and_solution,
         i_attach_file_ref_id,
         i_remark,
         i_attend_stru);
    /*
      -- 参与机构
      UPDATE ab_exercise_stru t
         SET t.status = '0', t.update_time = v_sysdate
       WHERE t.business_id = v_business_id
         AND t.status = '1'
         AND t.stru_id NOT IN
             (SELECT column_value FROM TABLE(func_split2(i_attend_stru)));
      MERGE INTO ab_exercise_stru t
      USING (SELECT column_value stru_id FROM TABLE(func_split2(i_attend_stru))) tmp
      ON (t.business_id = v_business_id AND t.stru_id = tmp.stru_id)
      WHEN MATCHED THEN
        UPDATE SET t.status = '1', t.update_time = v_sysdate WHERE t.status = '0'
      WHEN NOT MATCHED THEN
        INSERT
          (business_id, stru_id, status, update_time)
        VALUES
          (v_business_id, tmp.stru_id, '1', v_sysdate);
    */
    o_appid := v_business_id;

    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_ret_code := -1;
  END proc_submit;

  PROCEDURE proc_audit(i_jbpm_business_id           IN VARCHAR2, --业务ID
                       i_jbpm_next_activity_id      IN VARCHAR2, --目标任务
                       i_jbpm_destination_task_name IN VARCHAR2,
                       i_jbpm_activity_id           IN VARCHAR2,
                       i_jbpm_task_id               IN VARCHAR2,
                       i_audit_user                 IN VARCHAR2,
                       i_jbpm_transition_name       IN VARCHAR2,
                       o_ret_code                   OUT VARCHAR2,
                       o_msg                        OUT VARCHAR2,
                       o_jbpm_business_title        OUT VARCHAR2) IS
    v_param     db_log.info%TYPE := substr(i_jbpm_business_id || '|' ||
                                           i_jbpm_next_activity_id || '|' ||
                                           i_jbpm_destination_task_name || '|' ||
                                           i_jbpm_activity_id || '|' ||
                                           i_jbpm_task_id || '|' ||
                                           i_audit_user || '|' ||
                                           i_jbpm_transition_name, 1, 3000);
    v_proc_name db_log.proc_name%TYPE := 'pckg_ab_exercise.proc_audit';
    v_sysdate   DATE := SYSDATE;
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step,
                  pack_log.start_msg || v_param);
    o_ret_code := 0;

    -- 结束更新close_time
    IF i_jbpm_destination_task_name = '结束' THEN
      UPDATE ab_exercise_app t
         SET t.close_time = v_sysdate
       WHERE t.business_id = i_jbpm_business_id;
    END IF;

    SELECT t.title
      INTO o_jbpm_business_title
      FROM dams_wf_process t
     WHERE t.business_id = i_jbpm_business_id;

    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_ret_code := -1;
  END proc_audit;

END pckg_ab_exercise;
//




CREATE OR REPLACE PACKAGE pckg_dams_dict IS

  PROCEDURE proc_gendict_query(i_table_name    IN VARCHAR2, --通用表名
                               i_typecolumn    IN VARCHAR2, --字典大类字段名
                               i_codecolumn    IN VARCHAR2, --字典小类字段名
                               i_valuecolumn   IN VARCHAR2, --字典项值字段名
                               i_langcolumn    IN VARCHAR2, --多语言字段名
                               i_filtercolumns IN VARCHAR2, --过滤字段名,分号分隔
                               i_filtervalues  IN VARCHAR2, --过滤值,分行分隔
                               o_flag          OUT VARCHAR2, --0  成功  1 取角色信息错
                               o_msg           OUT VARCHAR2, --具体错误信息
                               o_gendict       OUT SYS_REFCURSOR --通用字典表查询结果集
                               );

  PROCEDURE proc_dict_chain(i_sys_code    IN VARCHAR2, --系统编号
                            i_dict_type   IN VARCHAR2, --字典类型
                            i_parent_code IN VARCHAR2, --父字典编号
                            i_lang        IN VARCHAR2, --语言
                            o_retcode     OUT VARCHAR2, --返回值
                            o_list_cur    OUT SYS_REFCURSOR --查询列表游标
                            );
END pckg_dams_dict;
//
CREATE OR REPLACE PACKAGE BODY pckg_dams_dict IS

  PROCEDURE proc_gendict_query(i_table_name    IN VARCHAR2, --通用表名
                               i_typecolumn    IN VARCHAR2, --字典大类字段名
                               i_codecolumn    IN VARCHAR2, --字典小类字段名
                               i_valuecolumn   IN VARCHAR2, --字典项值字段名
                               i_langcolumn    IN VARCHAR2, --多语言字段名
                               i_filtercolumns IN VARCHAR2, --过滤字段名,分号分隔
                               i_filtervalues  IN VARCHAR2, --过滤值,分行分隔
                               o_flag          OUT VARCHAR2, --0  成功  1 取角色信息错
                               o_msg           OUT VARCHAR2, --具体错误信息
                               o_gendict       OUT SYS_REFCURSOR --通用字典表查询结果集
                               ) IS
    v_sql           VARCHAR2(2000); --存放动态SQL语句
    v_sql1          VARCHAR2(1000); --存放动态SQL语句
    v_sql2          VARCHAR2(4000); --存放过滤条件
    v_sql3          VARCHAR2(4000); --存放排序条件
    v_filtercolumns dams_arrytype;
    v_filtervalues  dams_arrytype;
    v_count         NUMBER(2);
    v_proc_name     VARCHAR2(100);
    v_inparam       VARCHAR2(4000);
    v_outparam      VARCHAR2(4000);
  BEGIN
    o_flag      := '0';
    o_msg       := '';
    v_sql       := '';
    v_sql1      := '';
    v_proc_name := 'pckg_dams_dict.proc_gendict_query';
    v_inparam   := '开始!' || '|' || i_table_name || '|' || i_typecolumn || '|' ||
                   i_codecolumn || '|' || i_valuecolumn || '|' || i_langcolumn || '|' ||
                   i_filtercolumns || '|' || i_filtervalues;
    pack_log.info(v_proc_name, '', v_inparam);
    IF i_table_name IS NOT NULL THEN
      v_sql := v_sql || '''' || i_table_name || '''';
    ELSE
      o_flag := '1';
      o_msg  := '系统启动初始化字典表失败,通用字典表表名为空!' || SQLCODE || ' ' || SQLERRM;
      RETURN;
    END IF;

    IF i_typecolumn IS NOT NULL THEN
      v_sql1 := v_sql1 || i_typecolumn || ',';
    ELSE
      v_sql1 := v_sql1 || '''DEFAULT'',';

    END IF;

    IF i_codecolumn IS NOT NULL AND i_valuecolumn IS NOT NULL THEN
      v_sql1 := v_sql1 || i_codecolumn || ',
             ' || i_valuecolumn;

    ELSE
      o_flag := '1';
      o_msg  := '系统启动初始化字典表失败,通用字典表字段名为空!' || SQLCODE || ' ' || SQLERRM;
      RETURN;
    END IF;
    --支持国际化
    IF i_langcolumn IS NOT NULL THEN
      v_sql1 := v_sql1 || ',' || i_langcolumn;
    ELSE
      v_sql1 := v_sql1 || ',NULL';
    END IF;
    v_sql := 'SELECT ' || v_sql1 || ' FROM ' || REPLACE(v_sql, '''', '');

    --字典过滤
    IF i_filtercolumns IS NOT NULL AND i_filtervalues IS NOT NULL THEN

      v_filtercolumns := pckg_dams_util.func_str_to_array(i_filtercolumns, ';');
      v_filtervalues  := pckg_dams_util.func_str_to_array(i_filtervalues, ';');

      FOR i IN 1 .. v_filtercolumns.count
      LOOP
        IF i = 1 THEN
          v_sql2 := ' WHERE ';
        ELSE
          v_sql2 := v_sql2 || ' AND ';
        END IF;
        v_sql2 := v_sql2 || v_filtercolumns(i) || ' = ''' || v_filtervalues(i) || '''';

      END LOOP;

    END IF;
    SELECT COUNT(1)
      INTO v_count
      FROM user_tab_columns t
     WHERE t.table_name = upper(i_table_name)
       AND t.column_name IN ('ORDER_NO', 'DICTCODE');

    IF v_count >= 2 THEN
      v_sql3 := ' ORDER BY ORDER_NO,DICTCODE';
    ELSE
      v_sql3 := '';
    END IF;

    OPEN o_gendict FOR v_sql || v_sql2 || v_sql3;
    v_outparam := '结束!' || '|' || o_flag || '|' || o_msg;
    pack_log.info(v_proc_name, '', v_outparam);
  EXCEPTION
    WHEN OTHERS THEN
      o_flag := '1';
      o_msg  := o_msg || '系统启动初始化字典表发生错误:' || SQLCODE || ' ' || SQLERRM;
  END;

  PROCEDURE proc_dict_chain(i_sys_code    IN VARCHAR2, --系统编号
                            i_dict_type   IN VARCHAR2, --字典类型
                            i_parent_code IN VARCHAR2, --父字典编号
                            i_lang        IN VARCHAR2, --语言
                            o_retcode     OUT VARCHAR2, --返回值
                            o_list_cur    OUT SYS_REFCURSOR --查询列表游标
                            ) IS
    v_param     db_log.info%TYPE := i_sys_code || '|' || i_dict_type || '|' ||
                                    i_parent_code;
    v_proc_name db_log.proc_name%TYPE := 'PCKG_DAMS_DICT.PROC_DICT_CHAIN';
    v_step      db_log.step_no%TYPE := 0;
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step,
                  pack_log.start_msg || v_param);

    o_retcode := '0';

    --查询字典项
    OPEN o_list_cur FOR
      SELECT t.dictcode, t.dictvalue
        FROM dams_gen_dict t
       WHERE t.sys_code = i_sys_code
         AND ((i_parent_code IS NULL AND t.parent_code IS NULL) OR
             t.parent_code = i_parent_code)
         AND t.dicttype = i_dict_type
         AND t.valid_flag = 1
         AND t.dictlang = i_lang
       ORDER BY t.dictcode;

    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_retcode := '-1';
      ROLLBACK;
      RETURN;
  END proc_dict_chain;

END pckg_dams_dict;
//



CREATE OR REPLACE PACKAGE pckg_ab_rpt_rsk_hb IS

  PROCEDURE proc_deptuser_qry(i_ssic_id   IN VARCHAR2,
                              i_stru_id   IN VARCHAR2,
                              o_ret_code  OUT VARCHAR2,
                              o_msg       OUT VARCHAR2,
                              o_user_list OUT SYS_REFCURSOR 
                              );

  PROCEDURE proc_add_assist(i_condition IN CLOB,
                            i_task_id   IN VARCHAR2, 
                            o_ret_code  OUT VARCHAR2,
                            o_msg       OUT VARCHAR2,
                             o_appid     OUT VARCHAR2);
 
  PROCEDURE proc_hb_complete(i_condition IN VARCHAR2,
                             o_ret_code  OUT VARCHAR2,
                             o_msg       OUT VARCHAR2,
			     o_appid     OUT VARCHAR2);
 
  PROCEDURE proc_qry_hb_user(i_business_id IN VARCHAR2,
                             i_task_id     IN VARCHAR2,
                             o_ret_code    OUT VARCHAR2,
                             o_hb_users    OUT VARCHAR2,
                             o_msg         OUT VARCHAR2);
END pckg_ab_rpt_rsk_hb;
//


CREATE OR REPLACE PACKAGE pckg_ab_notice IS

  PROCEDURE proc_save(i_applicant          IN VARCHAR2, --申请人
                      i_tel                IN VARCHAR2, --联系电话
                      i_app_stru           IN VARCHAR2, --申请部门
                      i_business_id        IN VARCHAR2,
                      i_title              IN VARCHAR2,
                      i_content            IN CLOB,
                      i_attach_file_ref_id IN VARCHAR2,
                      i_need_feed_back     IN VARCHAR2,
                      i_stru_ids           IN CLOB,
                      o_ret_code           OUT VARCHAR2,
                      o_msg                OUT VARCHAR2,
                      o_appid              OUT VARCHAR2);
  PROCEDURE proc_get_detail(i_ssic_id     IN VARCHAR2, --统一认证号
                            i_business_id IN VARCHAR2,
                            o_ret_code    OUT VARCHAR2, --返回值
                            o_list_cur    OUT SYS_REFCURSOR,
                            o_stru_cur    OUT SYS_REFCURSOR);
  PROCEDURE proc_bnch_get_detail(i_ssic_id              IN VARCHAR2, --统一认证号
                                 i_business_id          IN VARCHAR2,
                                 o_ret_code             OUT VARCHAR2, --返回值
                                 o_feedback_routine     OUT VARCHAR2,
                                 o_list_cur             OUT SYS_REFCURSOR,
                                 o_stru_cur             OUT SYS_REFCURSOR,
                                 o_sub_list_cur         OUT SYS_REFCURSOR);
  PROCEDURE proc_submit(i_applicant          IN VARCHAR2, --申请人
                        i_tel                IN VARCHAR2, --联系电话
                        i_app_stru           IN VARCHAR2, --申请部门
                        i_business_id        IN VARCHAR2,
                        i_title              IN VARCHAR2,
                        i_content            IN CLOB,
                        i_attach_file_ref_id IN VARCHAR2,
                        i_need_feed_back     IN VARCHAR2,
                        i_stru_ids           IN CLOB,
                        o_ret_code           OUT VARCHAR2,
                        o_msg                OUT VARCHAR2,
                        o_appid              OUT VARCHAR2);
  PROCEDURE proc_audit(i_jbpm_business_id           IN VARCHAR2, --业务ID
                       i_jbpm_next_activity_id      IN VARCHAR2, --目标任务
                       i_jbpm_destination_task_name IN VARCHAR2,
                       i_jbpm_activity_id           IN VARCHAR2,
                       i_jbpm_task_id               IN VARCHAR2,
                       i_audit_user                 IN VARCHAR2,
                       i_jbpm_transition_name       IN VARCHAR2,
                       o_ret_code                   OUT VARCHAR2,
                       o_msg                        OUT VARCHAR2,
                       o_jbpm_business_title        OUT VARCHAR2);

  PROCEDURE proc_bnch_audit(i_jbpm_business_id           IN VARCHAR2, --业务ID
                            i_jbpm_next_activity_id      IN VARCHAR2, --目标任务
                            i_jbpm_destination_task_name IN VARCHAR2,
                            i_jbpm_activity_id           IN VARCHAR2,
                            i_jbpm_task_id               IN VARCHAR2,
                            i_audit_user                 IN VARCHAR2,
                            i_jbpm_transition_name       IN VARCHAR2,
                            i_need_save_feedback         IN VARCHAR2,
                            i_content                    IN VARCHAR2,
                            i_attach_file_ref_id         IN VARCHAR2,
                            o_ret_code                   OUT VARCHAR2,
                            o_msg                        OUT VARCHAR2,
                            o_jbpm_business_title        OUT VARCHAR2);

  PROCEDURE proc_prepare_start_info(i_source_business_id IN VARCHAR2,
                                    i_stru_ids           IN VARCHAR2,
                                    o_ret_code           OUT VARCHAR2, --返回标识
                                    o_msg                OUT VARCHAR2,
                                    o_info_cur           OUT SYS_REFCURSOR,
                                    o_pre_task_id        OUT VARCHAR2);

  PROCEDURE proc_query_next_dealer(i_stru_id     IN VARCHAR2, --机构ID
                                   i_role_id     IN VARCHAR2,
                                   i_sys_code    IN VARCHAR2,
                                   o_retcode     OUT VARCHAR2, --返回标识
                                   o_dealer_list OUT SYS_REFCURSOR);

  PROCEDURE proc_get_urge_info(i_source_business_id IN VARCHAR2,
                               o_ret_code           OUT VARCHAR2, --返回标识
                               o_msg                OUT VARCHAR2,
                               o_need_feedback      OUT VARCHAR2,
                               o_feedback_state     OUT SYS_REFCURSOR);
  PROCEDURE proc_urge(i_source_business_id IN VARCHAR2,
                      i_stru_ids           IN VARCHAR2,
                      o_ret_code           OUT VARCHAR2, --返回标识
                      o_msg                OUT VARCHAR2);

  PROCEDURE proc_get_jbpm_param(i_ssic_id   IN VARCHAR2,
                                i_role_id   IN VARCHAR2,
                                i_stru_id   IN VARCHAR2,
                                o_ret_code  OUT VARCHAR2, --返回标志
                                o_msg       OUT VARCHAR2,
                                o_next_user OUT VARCHAR2);

END pckg_ab_notice;
//
CREATE OR REPLACE PACKAGE BODY pckg_ab_notice IS


  PROCEDURE proc_save(i_applicant          IN VARCHAR2, --申请人
                      i_tel                IN VARCHAR2, --联系电话
                      i_app_stru           IN VARCHAR2, --申请部门
                      i_business_id        IN VARCHAR2,
                      i_title              IN VARCHAR2,
                      i_content            IN CLOB,
                      i_attach_file_ref_id IN VARCHAR2,
                      i_need_feed_back     IN VARCHAR2,
                      i_stru_ids           IN CLOB,
                      o_ret_code           OUT VARCHAR2,
                      o_msg                OUT VARCHAR2,
                      o_appid              OUT VARCHAR2) IS
    v_param       db_log.info%TYPE := substr(i_applicant || '|' || i_tel || '|' ||
                                             i_app_stru || '|' || i_business_id || '|' ||
                                             i_title || '|' ||
                                             substr(i_content, 1, 1000) || '|' ||
                                             i_attach_file_ref_id || '|' ||
                                             i_need_feed_back || '|' ||
                                             substr(i_stru_ids, 1, 1000), 1, 3000);
    v_proc_name   db_log.proc_name%TYPE := 'pckg_ab_notice.proc_save';
    v_business_id VARCHAR2(20) := i_business_id;
    v_sysdate     DATE := SYSDATE;
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step,
                  pack_log.start_msg || v_param);
    o_ret_code := 0;

    IF v_business_id IS NULL THEN
      v_business_id := pckg_dams_util.func_gen_seq('SYTZ', 'AB_NOTICE_SEQ', 8);
    END IF;

    MERGE INTO dams_notice_report_drf t
    USING (SELECT v_business_id business_id FROM dual) tmp
    ON (t.business_id = tmp.business_id)
    WHEN MATCHED THEN
      UPDATE
         SET t.applicant          = i_applicant,
             t.app_stru           = i_app_stru,
             t.tel                = i_tel,
             t.save_time          = v_sysdate,
             t.title              = i_title,
             t.content            = i_content,
             t.attach_file_ref_id = i_attach_file_ref_id,
             t.need_feed_back     = i_need_feed_back
    WHEN NOT MATCHED THEN
      INSERT
        (business_id,
         valid_flag,
         applicant,
         app_stru,
         tel,
         save_time,
         title,
         content,
         attach_file_ref_id,
         need_feed_back,
         sys_code,
         biz_type)
      VALUES
        (v_business_id,
         '',
         i_applicant,
         i_app_stru,
         i_tel,
         v_sysdate,
         i_title,
         i_content,
         i_attach_file_ref_id,
         i_need_feed_back,
         'AB',
         'AB04');

    DELETE FROM dams_noti_app_feedback t WHERE t.business_id = v_business_id;
    INSERT INTO dams_noti_app_feedback
      (business_id, stru_id, update_time)
      SELECT v_business_id business_id, column_value stru_id, v_sysdate
        FROM TABLE(func_split2_clob(i_stru_ids));

    DELETE FROM dams_save_info t WHERE t.app_id = v_business_id;
    INSERT INTO dams_save_info
      (app_id, applyer, app_stru, app_time, biz_type, info, pdid, form_id)
    VALUES
      (v_business_id,
       i_applicant,
       i_app_stru,
       SYSDATE,
       'AB04',
       '[草稿]中国工商银行声誉风险管理通知单',
       'ab_notice_head-1',
       'abNoticeHeadForm');

    o_appid := v_business_id;

    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_ret_code := -1;
  END proc_save;

  /******************************************************************************
      --存储过程名：    proc_get_detail
      --存储过程描述：
      --功能模块：      申请查询
      --作者：          kfzx-linlc
      --时间：          2015-09-17
      --修改历史:
  ******************************************************************************/
  PROCEDURE proc_get_detail(i_ssic_id     IN VARCHAR2, --统一认证号
                            i_business_id IN VARCHAR2,
                            o_ret_code    OUT VARCHAR2, --返回值
                            o_list_cur    OUT SYS_REFCURSOR,
                            o_stru_cur    OUT SYS_REFCURSOR) IS
    v_param     db_log.info%TYPE := i_ssic_id || '|' || i_business_id;
    v_proc_name db_log.proc_name%TYPE := 'pckg_ab_notice.proc_get_detail';
    v_count     NUMBER(5);
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step,
                  pack_log.start_msg || v_param);
    o_ret_code := '0';

    SELECT COUNT(1)
      INTO v_count
      FROM dams_notice_report_app t
     WHERE t.business_id = i_business_id;

    IF v_count > 0 THEN
      OPEN o_list_cur FOR
        SELECT business_id,
               applicant,
               (SELECT u.name FROM dams_user u WHERE u.ssic_id = t.applicant) applicant_name,
               app_stru,
               (SELECT b1.stru_sname
                  FROM dams_branch b1
                 WHERE b1.stru_id = t.app_stru) app_stru_name,
               c.major_stru_id bnch_id,
               db.stru_sname bnch_name,
               db.stru_grade bnch_grade,
               tel,
               title,
               content,
               attach_file_ref_id,
               need_feed_back
          FROM dams_notice_report_app t, dams_branch_catalog c, dams_branch db
         WHERE t.business_id = i_business_id
           AND t.app_stru = c.stru_id(+)
           AND c.major_stru_id = db.stru_id(+);

    ELSE
      OPEN o_list_cur FOR
        SELECT business_id,
               applicant,
               (SELECT u.name FROM dams_user u WHERE u.ssic_id = t.applicant) applicant_name,
               app_stru,
               (SELECT b1.stru_sname
                  FROM dams_branch b1
                 WHERE b1.stru_id = t.app_stru) app_stru_name,
               c.major_stru_id bnch_id,
               db.stru_sname bnch_name,
               db.stru_grade bnch_grade,
               tel,
               title,
               content,
               attach_file_ref_id,
               need_feed_back
          FROM dams_notice_report_drf t, dams_branch_catalog c, dams_branch db
         WHERE t.business_id = i_business_id
           AND t.app_stru = c.stru_id(+)
           AND c.major_stru_id = db.stru_id(+);

    END IF;

    OPEN o_stru_cur FOR
      SELECT business_id,
             stru_id,
             (SELECT b.stru_sname FROM dams_branch b WHERE b.stru_id = t.stru_id) stru_name,
             to_char(update_time, 'YYYY-MM-DD hh24:mi') update_time,
             sub_app_id,
             content,
             attach_file_ref_id,
             t.status
        FROM dams_noti_app_feedback t
       WHERE t.business_id LIKE i_business_id;

  EXCEPTION
    WHEN OTHERS THEN
      IF o_list_cur%ISOPEN THEN
        CLOSE o_list_cur;
      END IF;
      IF o_stru_cur%ISOPEN THEN
        CLOSE o_stru_cur;
      END IF;
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_ret_code := '-1';
      RETURN;
  END proc_get_detail;

  /******************************************************************************
      --存储过程名：    proc_bnch_get_detail
      --存储过程描述：  proc_bnch_get_detail
      --功能模块：      申请查询
      --作者：          kfzx-linlc
      --时间：          2015-09-17
      --修改历史:
  ******************************************************************************/
 PROCEDURE proc_bnch_get_detail(i_ssic_id          IN VARCHAR2, --统一认证号
                                i_business_id      IN VARCHAR2,
                                o_ret_code         OUT VARCHAR2, --返回值
                                o_feedback_routine OUT VARCHAR2, --同意并反馈的路径选择
                                o_list_cur         OUT SYS_REFCURSOR,
                                o_stru_cur         OUT SYS_REFCURSOR,
                                o_sub_list_cur     OUT SYS_REFCURSOR) IS
   v_param             db_log.info%TYPE := i_ssic_id || '|' || i_business_id;
   v_proc_name         db_log.proc_name%TYPE := 'pckg_ab_notice.proc_bnch_get_detail';
   v_count             NUMBER(5);
   v_feedback_routine1 VARCHAR2(1); --１表示是总行部室,分行部室,０不是
   v_feedback_routine2 VARCHAR2(1); --１表示支行,０不是
   v_sup_business_id VARCHAR2(20);

 BEGIN
   pack_log.info(v_proc_name, pack_log.start_step,
                 pack_log.start_msg || v_param);
   o_ret_code := '0';
   --反馈机构为 　总行部室、分行部室、支行　走　　　       同意并反馈办公室            0
   --反馈机构为　其他　　　　　　　　　　　走　　　       同意并反馈上级机构          1
   o_feedback_routine := '1';

   SELECT COUNT(1)
     INTO v_feedback_routine1
     FROM dams_branch_catalog bc, dams_noti_app_feedback af
    WHERE af.sub_app_id = i_business_id
      AND bc.stru_id = af.stru_id
      AND bc.stru_id != bc.major_stru_id
      AND bc.stru_level IN ('1', '2', '3');
   SELECT COUNT(1)
     INTO v_feedback_routine2
     FROM dams_branch_catalog bc, dams_noti_app_feedback af
    WHERE af.sub_app_id = i_business_id
      AND bc.stru_id = af.stru_id
      AND bc.stru_id = bc.major_stru_id
      AND bc.stru_level = '4';

   IF v_feedback_routine1 = '1' OR v_feedback_routine2 = '1' THEN
     o_feedback_routine := '0';
   END IF;

   SELECT t.business_id
     INTO v_sup_business_id
     FROM dams_noti_app_feedback t
    WHERE t.sub_app_id = i_business_id;

   OPEN o_list_cur FOR
     SELECT business_id,
            applicant,
            (SELECT u.name FROM dams_user u WHERE u.ssic_id = t.applicant) applicant_name,
            app_stru,
            (SELECT b1.stru_sname
               FROM dams_branch b1
              WHERE b1.stru_id = t.app_stru) app_stru_name,
            c.major_stru_id bnch_id,
            db.stru_sname bnch_name,
            db.stru_grade bnch_grade,
            tel,
            title,
            content,
            attach_file_ref_id,
            need_feed_back
       FROM dams_notice_report_app t, dams_branch_catalog c, dams_branch db
      WHERE t.business_id = v_sup_business_id
        AND t.app_stru = c.stru_id(+)
        AND c.major_stru_id = db.stru_id(+);

   OPEN o_stru_cur FOR
     SELECT business_id,
            stru_id,
            (SELECT b.stru_sname FROM dams_branch b WHERE b.stru_id = t.stru_id) stru_name
       FROM dams_noti_app_feedback t
      WHERE t.business_id = v_sup_business_id;

   OPEN o_sub_list_cur FOR
     SELECT business_id,
            stru_id,
            (SELECT b.stru_sname FROM dams_branch b WHERE b.stru_id = t.stru_id) stru_name,
            (SELECT b.stru_grade FROM dams_branch b WHERE b.stru_id = t.stru_id) stru_grade,
            to_char(update_time, 'YYYY-MM-DD hh24:mi') update_time,
            sub_app_id,
            content,
            attach_file_ref_id
       FROM dams_noti_app_feedback t
      WHERE t.sub_app_id = i_business_id;

 EXCEPTION
   WHEN OTHERS THEN
     IF o_list_cur%ISOPEN THEN
       CLOSE o_list_cur;
     END IF;
     IF o_stru_cur%ISOPEN THEN
       CLOSE o_stru_cur;
     END IF;
     IF o_sub_list_cur%ISOPEN THEN
       CLOSE o_sub_list_cur;
     END IF;
     pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
     o_ret_code := '-1';
     RETURN;
 END proc_bnch_get_detail;

  
  PROCEDURE proc_submit(i_applicant          IN VARCHAR2, --申请人
                        i_tel                IN VARCHAR2, --联系电话
                        i_app_stru           IN VARCHAR2, --申请部门
                        i_business_id        IN VARCHAR2,
                        i_title              IN VARCHAR2,
                        i_content            IN CLOB,
                        i_attach_file_ref_id IN VARCHAR2,
                        i_need_feed_back     IN VARCHAR2,
                        i_stru_ids           IN CLOB,
                        o_ret_code           OUT VARCHAR2,
                        o_msg                OUT VARCHAR2,
                        o_appid              OUT VARCHAR2) IS
    v_param       db_log.info%TYPE := substr(i_applicant || '|' || i_tel || '|' ||
                                             i_app_stru || '|' || i_business_id || '|' ||
                                             i_title || '|' ||
                                             substr(i_content, 1, 1000) || '|' ||
                                             i_attach_file_ref_id || '|' ||
                                             i_need_feed_back || '|' ||
                                             substr(i_stru_ids, 1, 1000), 1, 3000);
    v_proc_name   db_log.proc_name%TYPE := 'pckg_ab_notice.proc_submit';
    v_business_id VARCHAR2(20) := i_business_id;
    v_sysdate     DATE := SYSDATE;
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step,
                  pack_log.start_msg || v_param);
    o_ret_code := 0;

    IF v_business_id IS NULL THEN
      v_business_id := '234556';
    END IF;

    -- 删除草稿
    DELETE FROM dams_save_info a WHERE a.app_id = v_business_id;
    DELETE FROM dams_notice_report_drf t WHERE t.business_id = v_business_id;

    MERGE INTO dams_notice_report_app t
    USING (SELECT v_business_id business_id FROM dual) tmp
    ON (t.business_id = tmp.business_id)
    WHEN MATCHED THEN
      UPDATE
         SET t.tel                = i_tel,
             t.update_time        = v_sysdate,
             t.title              = i_title,
             t.content            = i_content,
             t.attach_file_ref_id = i_attach_file_ref_id,
             t.need_feed_back     = i_need_feed_back
    WHEN NOT MATCHED THEN
      INSERT
        (business_id,
         valid_flag,
         applicant,
         app_stru,
         tel,
         create_time,
         update_time,
         title,
         content,
         attach_file_ref_id,
         need_feed_back,
         sys_code,
         biz_type)
      VALUES
        (v_business_id,
         '',
         i_applicant,
         i_app_stru,
         i_tel,
         v_sysdate,
         v_sysdate,
         i_title,
         i_content,
         i_attach_file_ref_id,
         i_need_feed_back,
         'AB',
         'AB04');

    DELETE FROM dams_noti_app_feedback t WHERE t.business_id = v_business_id;
    INSERT INTO dams_noti_app_feedback
      (business_id, stru_id, update_time)
      SELECT v_business_id business_id, column_value stru_id, v_sysdate
        FROM TABLE(func_split2_clob(i_stru_ids));

    o_appid := v_business_id;

    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_ret_code := -1;
  END proc_submit;


  PROCEDURE proc_audit(i_jbpm_business_id           IN VARCHAR2, --业务ID
                       i_jbpm_next_activity_id      IN VARCHAR2, --目标任务
                       i_jbpm_destination_task_name IN VARCHAR2,
                       i_jbpm_activity_id           IN VARCHAR2,
                       i_jbpm_task_id               IN VARCHAR2,
                       i_audit_user                 IN VARCHAR2,
                       i_jbpm_transition_name       IN VARCHAR2,
                       o_ret_code                   OUT VARCHAR2,
                       o_msg                        OUT VARCHAR2,
                       o_jbpm_business_title        OUT VARCHAR2) IS
    v_param     db_log.info%TYPE := substr(i_jbpm_business_id || '|' ||
                                           i_jbpm_next_activity_id || '|' ||
                                           i_jbpm_destination_task_name || '|' ||
                                           i_jbpm_activity_id || '|' ||
                                           i_jbpm_task_id || '|' ||
                                           i_audit_user || '|' ||
                                           i_jbpm_transition_name, 1, 3000);
    v_proc_name db_log.proc_name%TYPE := 'pckg_ab_notice.proc_audit';
    v_sysdate   DATE := SYSDATE;
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step,
                  pack_log.start_msg || v_param);
    o_ret_code := 0;

    -- 结束更新close_time
    IF i_jbpm_destination_task_name = '结束' THEN
      UPDATE dams_notice_report_app t
         SET t.close_time = v_sysdate
       WHERE t.business_id = i_jbpm_business_id;
    END IF;

    SELECT t.title
      INTO o_jbpm_business_title
      FROM dams_wf_process t
     WHERE t.business_id = i_jbpm_business_id;

    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_ret_code := -1;
  END proc_audit;


  PROCEDURE proc_add_yuezhi(i_businessid IN VARCHAR2,
                            i_userid     IN CLOB,
                            i_cretperson IN VARCHAR2,
                            i_taskid     IN NUMBER,
                            i_param      IN VARCHAR2, -- 其它参数
                            o_flag       OUT VARCHAR2, --存储过程成功失败标志
                            o_errmsg     OUT VARCHAR2,
                            o_mail_user  OUT VARCHAR2) IS

    v_param     db_log.info%TYPE := i_businessid || '|' ||
                                    substr(i_userid, 0, 3000) || '|' ||
                                    i_cretperson || '|' || i_taskid;
    v_proc_name db_log.proc_name%TYPE := 'pckg_ab_notice.proc_add_yuezhi';

    v_count         NUMBER;
    v_sql           VARCHAR2(2024);
    v_str           VARCHAR2(32000);
    v_length        INTEGER := 0;
    v_begnum        INTEGER := 1;
    v_postion       INTEGER := 1;
    v_cnt           INTEGER := 0;
    v_userarray     pckg_dams_role.arrytype;
    v_readuserarray pckg_dams_role.arrytype;
    v_title         dams_wf_process.title%TYPE;
    v_tpl_param     VARCHAR2(4000) := '';
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step,
                  pack_log.start_msg || v_param);

    o_flag := 0;

    IF i_userid IS NULL THEN
      RETURN;
    END IF;

    v_length := dbms_lob.getlength(i_userid);

    IF v_length <= 8000 THEN
      dbms_lob.read(i_userid, v_length, 1, v_str);

      --v_userarray := pckg_dams_role.func_common_getarray(v_str, ',');
      FOR i IN 1 .. v_userarray.count
      LOOP
        v_count := 1;
        v_sql   := 'select count(*) from dams_read_empower t where t.bussiness_id=:i_businessId and t.user_id=:i_userId';

        --v_readuserarray := pckg_dams_role.func_common_getarray(v_userarray(i), '|');

        EXECUTE IMMEDIATE v_sql
          INTO v_count
          USING i_businessid, v_readuserarray(1);

        o_mail_user := o_mail_user || v_readuserarray(1) || ',';
        --如果已插过阅知表,则不用再插
        IF v_count = 0 THEN

          INSERT INTO dams_read_empower
            (key_id,
             bussiness_id,
             user_id,
             create_time,
             create_user,
             state,
             task_id)
          VALUES
            (seq_read_empower.nextval,
             i_businessid,
             v_readuserarray(1),
             SYSDATE,
             i_cretperson,
             '0',
             i_taskid);
        END IF;
      END LOOP;

    ELSE
      LOOP
        EXIT WHEN v_length - v_postion < 8000;
        v_postion := dbms_lob.instr(i_userid, ',', v_postion, 100);
        v_str     := dbms_lob.substr(i_userid, v_postion - v_begnum, v_begnum);

        --v_userarray := pckg_dams_role.func_common_getarray(v_str, ',');
        FOR i IN 1 .. v_userarray.count
        LOOP
          v_count := 1;

          --v_readuserarray := pckg_dams_role.func_common_getarray(v_userarray(i),'|');

          v_sql := 'select count(*) from dams_read_empower t where t.bussiness_id=:i_businessId and t.user_id=:i_userId';
          EXECUTE IMMEDIATE v_sql
            INTO v_count
            USING i_businessid, v_readuserarray(1);

          o_mail_user := o_mail_user || v_readuserarray(1) || ',';
          --如果已插过阅知表,则不用再插
          IF v_count = 0 THEN
            INSERT INTO dams_read_empower
              (key_id,
               bussiness_id,
               user_id,
               create_time,
               create_user,
               state,
               task_id)
            VALUES
              (seq_read_empower.nextval,
               i_businessid,
               v_readuserarray(1),
               SYSDATE,
               i_cretperson,
               '0',
               i_taskid);
          END IF;
        END LOOP;

        v_begnum := v_postion;
        IF v_length - v_postion < 8000 THEN
          v_str := dbms_lob.substr(i_userid, v_length - v_begnum + 1, v_begnum);

          --v_userarray := pckg_dams_role.func_common_getarray(v_str, '|');
          FOR i IN 1 .. v_userarray.count
          LOOP
            v_count := 1;
            v_sql   := 'select count(*) from dams_read_empower t where t.bussiness_id=:i_businessId and t.user_id=:i_userId';

            --v_readuserarray := pckg_dams_role.func_common_getarray(v_userarray(i), ',');

            EXECUTE IMMEDIATE v_sql
              INTO v_count
              USING i_businessid, v_readuserarray(1);

            --如果已插过阅知表,则不用再插
            IF v_count = 0 THEN
              o_mail_user := o_mail_user || v_readuserarray(1) || ',';
              INSERT INTO dams_read_empower
                (key_id,
                 bussiness_id,
                 user_id,
                 create_time,
                 create_user,
                 state,
                 task_id)
              VALUES
                (seq_read_empower.nextval,
                 i_businessid,
                 v_readuserarray(1),
                 SYSDATE,
                 i_cretperson,
                 '0',
                 i_taskid);
            END IF;
          END LOOP;

        END IF;

        v_cnt := v_cnt + 1;
      END LOOP;
    END IF;

    --发阅知邮件
    o_mail_user := substr(o_mail_user, 1, length(o_mail_user) - 1);

    SELECT MAX(p.title)
      INTO v_title
      FROM viw_dams_wf_all_process p
     WHERE p.business_id = i_businessid;

    v_tpl_param := 'businessId=' || i_businessid || '1=' || v_title ||
                   '2=' || i_taskid || '&' || i_param;

    pckg_dams_mail.proc_insert_custom_mail_token('131', v_tpl_param,
                                                 o_mail_user, '', '', '2', '',
                                                 o_flag);

  EXCEPTION
    WHEN OTHERS THEN
      o_flag := -1;
      pack_log.error(v_proc_name, pack_log.end_step,
                     '异常: ' || v_param || ',error code：' || SQLERRM(SQLCODE));
      o_errmsg := v_proc_name || ':' || SQLERRM(SQLCODE);
      ROLLBACK;
  END proc_add_yuezhi;

  PROCEDURE proc_bnch_audit(i_jbpm_business_id           IN VARCHAR2, --业务ID
                            i_jbpm_next_activity_id      IN VARCHAR2, --目标任务
                            i_jbpm_destination_task_name IN VARCHAR2,
                            i_jbpm_activity_id           IN VARCHAR2,
                            i_jbpm_task_id               IN VARCHAR2,
                            i_audit_user                 IN VARCHAR2,
                            i_jbpm_transition_name       IN VARCHAR2,
                            i_need_save_feedback         IN VARCHAR2,
                            i_content                    IN VARCHAR2,
                            i_attach_file_ref_id         IN VARCHAR2,
                            o_ret_code                   OUT VARCHAR2,
                            o_msg                        OUT VARCHAR2,
                            o_jbpm_business_title        OUT VARCHAR2) IS
    v_param              db_log.info%TYPE := substr(i_jbpm_business_id || '|' ||
                                                    i_jbpm_next_activity_id || '|' ||
                                                    i_jbpm_destination_task_name || '|' ||
                                                    i_jbpm_activity_id || '|' ||
                                                    i_jbpm_task_id || '|' ||
                                                    i_audit_user || '|' ||
                                                    i_jbpm_transition_name || '|' ||
                                                    substr(i_content, 1, 1000) || '|' ||
                                                    i_attach_file_ref_id, 1, 3000);
    v_proc_name          db_log.proc_name%TYPE := 'pckg_ab_notice.proc_bnch_audit';
    v_sysdate            DATE := SYSDATE;
    v_need_feedback      dams_notice_report_app.need_feed_back%TYPE;
    v_tpl_param          VARCHAR2(4000);
    v_token              dams_mail_token.token_id%TYPE;
    v_head_business_id   VARCHAR2(20);
    v_yz_user            VARCHAR2(4000);
    v_mail_user          VARCHAR2(4000);
    v_feedback_stru_name dams_branch.stru_sname %TYPE;
    v_task_id            dams_wf_task.task_id%TYPE;

  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step,
                  pack_log.start_msg || v_param);
    o_ret_code := 0;

    SELECT t.title
      INTO o_jbpm_business_title
      FROM dams_wf_process t
     WHERE t.business_id = i_jbpm_business_id;

    IF i_need_save_feedback = '1' THEN
      UPDATE dams_noti_app_feedback t
         SET t.content            = i_content,
             t.attach_file_ref_id = i_attach_file_ref_id,
             t.status             = '1'
       WHERE t.sub_app_id = i_jbpm_business_id;
    END IF;

    -- 结束更新close_time
    IF i_jbpm_destination_task_name = '结束' THEN
      UPDATE dams_noti_app_feedback t
         SET t.status = '2'
       WHERE t.sub_app_id = i_jbpm_business_id;
   
    END IF;

    IF i_jbpm_activity_id = 'ab_notice_bnch-1-1' THEN
      UPDATE dams_noti_app_feedback t
         SET t.sub_applicant = i_audit_user
       WHERE t.sub_app_id = i_jbpm_business_id;
    END IF;

    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_ret_code := -1;
  END proc_bnch_audit;


  PROCEDURE proc_prepare_start_info(i_source_business_id IN VARCHAR2,
                                    i_stru_ids           IN VARCHAR2,
                                    o_ret_code           OUT VARCHAR2, --返回标识
                                    o_msg                OUT VARCHAR2,
                                    o_info_cur           OUT SYS_REFCURSOR,
                                    o_pre_task_id        OUT VARCHAR2) IS
    v_param     db_log.info%TYPE := substr('', 1, 3000);
    v_proc_name db_log.proc_name%TYPE := 'pckg_ab_notice.proc_prepare_start_info';
    v_sysdate   DATE := SYSDATE;
  
    v_no_dealer_bnch_str VARCHAR2(4000);
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step,
                  pack_log.start_msg || v_param);
    o_ret_code := 0;
  
    SELECT listagg((SELECT b.stru_sname
                     FROM dams_branch b
                    WHERE b.stru_id = column_value), ',') within GROUP(ORDER BY column_value)
      INTO v_no_dealer_bnch_str
      FROM TABLE(func_split2(i_stru_ids)) strus1
     WHERE NOT EXISTS (SELECT 1
              FROM dams_user_role_rel t, dams_branch_catalog c
             WHERE t.stru_id = c.stru_id
               AND (c.major_stru_id = strus1.column_value OR
                   c.dept_stru_id = strus1.column_value)
               AND t.sys_code = 'AB'
               AND t.role_id = 'AB001'
               AND t.auth_state IN ('0', '2'));
    IF v_no_dealer_bnch_str IS NOT NULL THEN
      o_ret_code := '1';
      o_msg      := '以下机构没有配置声誉风险联系人：' || v_no_dealer_bnch_str;
      ROLLBACK;
      RETURN;
    END IF;
  
    UPDATE dams_noti_app_feedback t
       SET t.sub_app_id  = pckg_dams_util.func_gen_seq('SYTZ', 'AB_NOTICE_SEQ', 8),
           t.update_time = v_sysdate
     WHERE t.business_id = i_source_business_id;
  
    OPEN o_info_cur FOR
      SELECT t.stru_id, t.sub_app_id
        FROM dams_noti_app_feedback t
       WHERE t.business_id = i_source_business_id;
  
    SELECT task_id
      INTO o_pre_task_id
      FROM (SELECT a.task_id,
                   row_number() over(PARTITION BY a.procinst_id ORDER BY a.create_time DESC) rn
              FROM viw_dams_wf_all_task a
             WHERE a.business_id = i_source_business_id)
     WHERE rn = 1;
  
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  
  EXCEPTION
    WHEN OTHERS THEN
      IF o_info_cur%ISOPEN THEN
        CLOSE o_info_cur;
      END IF;
      ROLLBACK;
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_ret_code := -1;
  END proc_prepare_start_info;

 
  PROCEDURE proc_query_next_dealer(i_stru_id     IN VARCHAR2, --机构ID
                                   i_role_id     IN VARCHAR2,
                                   i_sys_code    IN VARCHAR2,
                                   o_retcode     OUT VARCHAR2, --返回标识
                                   o_dealer_list OUT SYS_REFCURSOR) IS
    v_param      db_log.info%TYPE := i_stru_id || '|' || i_role_id || '|' ||
                                     i_sys_code;
    v_proc_name  db_log.proc_name%TYPE := 'pckg_ab_notice.proc_query_next_dealer';
    v_stru_range dams_branch.stru_id%TYPE;
    v_stru_sign  dams_branch.stru_sign%TYPE;
    v_count      NUMBER(19);
    v_man_grade  dams_branch.man_grade%TYPE;
    v_stru_grade dams_branch.stru_grade%TYPE;

    v_role_id dams_role.role_id %TYPE := i_role_id;
    v_spcific_branch     VARCHAR2(2000):='';
  BEGIN
    o_retcode := '0';
    pack_log.info(v_proc_name, pack_log.start_step,
                  pack_log.start_msg || v_param);

    -- 先判断是否境外控股 作为部门级流程走
    SELECT t.stru_sign, t.man_grade, t.stru_grade
      INTO v_stru_sign, v_man_grade, v_stru_grade
      FROM dams_branch t
     WHERE t.stru_id = i_stru_id;
    IF v_stru_sign IN ('8', '18') AND v_man_grade = '1' THEN
      v_stru_range := i_stru_id;

    ELSE
      SELECT COUNT(1)
        INTO v_count
        FROM dams_branch_catalog t
       WHERE t.stru_id = i_stru_id
         AND t.major_stru_id = i_stru_id;
      IF v_count >= 1 THEN
        IF v_stru_grade IN ('1', '2', '3') OR v_stru_sign = '4' THEN
          -- 一二级行
          SELECT t.stru_id
            INTO v_stru_range
            FROM dams_branch t
           WHERE t.sup_stru = i_stru_id
             AND t.stru_sign = '2';
          IF v_stru_grade IN ('1', '2') THEN
            v_role_id := 'AB000';
          END IF;
          IF instr(v_spcific_branch,i_stru_id)>0 THEN
            v_role_id := 'AB000';
          END IF;
        ELSE
          v_stru_range := i_stru_id;
        END IF;
      ELSE
        -- 部门
        v_stru_range := i_stru_id;
      END IF;
    END IF;

    OPEN o_dealer_list FOR
      SELECT listagg(ssic_id, ',') within GROUP(ORDER BY ssic_id) user_id,
             listagg(role_id, ',') within GROUP(ORDER BY ssic_id) role_id,
             listagg(stru_id, ',') within GROUP(ORDER BY ssic_id) stru_id,
             listagg(auth_state, ',') within GROUP(ORDER BY ssic_id) auth_state,
             REPLACE((listagg(ssic_tran_id, ',') within GROUP(ORDER BY ssic_id)),
                     's', '') ssic_id_tran
        FROM (SELECT aa.ssic_id,
                     bb.name user_name,
                     aa.role_id,
                     aa.stru_id,
                     decode(bb.ad, NULL, 'n', 'y') user_ad,
                     nvl(aa.ssic_tran_id, 's') ssic_tran_id,
                     aa.user_tran_name,
                     aa.auth_state
                FROM (SELECT decode(c.source_user_id, NULL, a.ssic_id,
                                    c.current_user_id) ssic_id,
                             a.role_id role_id,
                             a.stru_id,
                             decode(c.source_user_id, NULL, '', c.source_user_id) ssic_tran_id,
                             (SELECT u.name
                                FROM dams_user u
                               WHERE u.ssic_id = c.current_user_id) user_tran_name,
                             decode(c.source_user_id, NULL, '0', '1') auth_state
                        FROM dams_user_role_rel   a,
                             dams_accredit_main   c,
                             dams_branch_relation r
                       WHERE a.role_id = v_role_id
                         AND a.auth_state NOT IN ('2', '3')
                         AND a.stru_id = r.bnch_id
                         AND a.sys_code = i_sys_code
                         AND r.up_bnch_id = v_stru_range
                         AND a.ssic_id = c.source_user_id(+)
                         AND c.sys_code(+) = i_sys_code
                         AND c.role_id(+) = v_role_id
                         AND c.state(+) = '1') aa,
                     dams_user bb,
                     dams_branch bch
               WHERE aa.ssic_id = bb.ssic_id
                 AND bb.stru_id = bch.stru_id);

  EXCEPTION
    WHEN OTHERS THEN
      o_retcode := '-1';
      pack_log.error(v_proc_name, 1, v_param);
  END;

  PROCEDURE proc_get_urge_info(i_source_business_id IN VARCHAR2,
                               o_ret_code           OUT VARCHAR2, --返回标识
                               o_msg                OUT VARCHAR2,
                               o_need_feedback      OUT VARCHAR2,
                               o_feedback_state     OUT SYS_REFCURSOR) IS
    v_param     db_log.info%TYPE := substr(i_source_business_id, 1, 3000);
    v_proc_name db_log.proc_name%TYPE := 'pckg_ab_notice.proc_get_urge_info';
  BEGIN
    o_ret_code := '0';
    SELECT need_feed_back
      INTO o_need_feedback
      FROM dams_notice_report_app t
     WHERE t.business_id = i_source_business_id;
    IF o_need_feedback = '1' THEN
      -- 是否存在已经下发并没有反馈的
      SELECT decode(COUNT(1), 0, 0, 1)
        INTO o_need_feedback
        FROM dams_noti_app_feedback t
       WHERE t.business_id = i_source_business_id
         AND t.sub_app_id IS NOT NULL
         AND (t.status IS NULL OR t.status <> '2');
    END IF;

    IF o_need_feedback = '1' THEN
      OPEN o_feedback_state FOR
        SELECT t.sub_app_id business_id,
               t.stru_id,
               (SELECT b.stru_sname
                  FROM dams_branch b
                 WHERE b.stru_id = t.stru_id) stru_name,
               to_char(t.urge_time, 'YYYY-MM-DD hh24:mi:ss') urge_time,
               decode(t.status, '2', '1', '0') has_feedback
          FROM dams_noti_app_feedback t
         WHERE t.business_id = i_source_business_id
         ORDER BY urge_time NULLS FIRST;
    END IF;

  EXCEPTION
    WHEN OTHERS THEN
      o_ret_code := '-1';
      pack_log.error(v_proc_name, 1, v_param);
  END;

  PROCEDURE proc_send_urge_mail(i_business_id IN VARCHAR2,
                                i_sysdate     IN DATE) IS
    v_param     db_log.info%TYPE := '';
    v_proc_name db_log.proc_name%TYPE := 'pckg_ab_notice.proc_send_urge_mail';
    v_tokenid   dams_mail_token.token_id%TYPE;
    v_array     dams_arrytype;
    v_tpl_param VARCHAR2(4000);
    v_task_id   dams_wf_task.task_id%TYPE;
    v_applicant dams_user.ssic_id%TYPE;
    v_title     dams_wf_process.title%TYPE;
    o_ret_code  VARCHAR2(10);
    v_user_str  VARCHAR2(4000);
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step,
                  pack_log.start_msg || v_param);

    SELECT MAX(t.ssic_id), MAX(t.task_id)
      INTO v_applicant, v_task_id
      FROM dams_wf_task t
     WHERE t.business_id = i_business_id
       AND t.create_time =
           (SELECT MIN(t1.create_time)
              FROM dams_wf_task t1
             WHERE t1.business_id = i_business_id);
    SELECT t.title
      INTO v_title
      FROM dams_wf_process t
     WHERE t.business_id = i_business_id;
    IF v_applicant IS NOT NULL THEN
      -- 查找到经办人
      v_tokenid := pckg_dams_mail.func_gen_rndstr(20);
      INSERT INTO dams_mail_token
        (token_id, state, ssic_id, method, url_param, create_time, expire_time)
      VALUES
        (v_tokenid,
         0,
         v_applicant,
         'opentodotask',
         '3=' || i_business_id || '4=' || v_task_id,
         to_char(i_sysdate, 'yyyy-mm-dd hh24:mi:ss'),
         to_char(i_sysdate + 3, 'yyyy-mm-dd hh24:mi:ss'));
      v_tpl_param := 'subject=' || v_title || '5=' || i_business_id ||
                     'aaaaaaaa=' || v_tokenid;

      pckg_dams_mail.proc_insert_custom_mail_new('135', v_tpl_param,
                                                 v_applicant, '', '', '4', '0',
                                                 o_ret_code);
    ELSE
      -- 查询处理人
      SELECT listagg(p.userid_, ',') within GROUP(ORDER BY p.userid_)
        INTO v_user_str
        FROM jbpm4_participation p
       WHERE p.task_ = v_task_id;

      v_array := func_split2(v_user_str);

      FOR i IN 1 .. v_array.count
      LOOP
        BEGIN
          v_tokenid := pckg_dams_mail.func_gen_rndstr(20);
          INSERT INTO dams_mail_token
            (token_id,
             state,
             ssic_id,
             method,
             url_param,
             create_time,
             expire_time)
          VALUES
            (v_tokenid,
             0,
             v_array(i),
             'opentodotask',
             '3=' || i_business_id || '4=' || v_task_id,
             to_char(i_sysdate, 'yyyy-mm-dd hh24:mi:ss'),
             to_char(i_sysdate + 3, 'yyyy-mm-dd hh24:mi:ss'));
          v_tpl_param := 'subject=' || v_title || '5=' ||
                         i_business_id || 'aaaaaaaa=' || v_tokenid;

          pckg_dams_mail.proc_insert_custom_mail_new('135', v_tpl_param,
                                                     v_array(i), '', '', '4',
                                                     '0', o_ret_code);
        EXCEPTION
          WHEN OTHERS THEN
            -- 放弃此次循环
            NULL;
        END;
      END LOOP;
    END IF;

    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      RAISE;
  END proc_send_urge_mail;

  PROCEDURE proc_urge(i_source_business_id IN VARCHAR2,
                      i_stru_ids           IN VARCHAR2,
                      o_ret_code           OUT VARCHAR2, --返回标识
                      o_msg                OUT VARCHAR2) IS
    v_param        db_log.info%TYPE := substr(i_source_business_id, 1, 3000);
    v_proc_name    db_log.proc_name%TYPE := 'pckg_ab_notice.proc_urge';
    v_sysdate      DATE := SYSDATE;
    c_interval_min NUMBER(5) := 5;

    CURSOR c_app IS
      SELECT t.sub_app_id business_id
        FROM dams_noti_app_feedback t
       WHERE t.business_id = i_source_business_id
         AND t.stru_id IN
             (SELECT column_value FROM TABLE(func_split2(i_stru_ids)))
         AND (t.urge_time IS NULL OR
             (v_sysdate - t.urge_time) * 24 * 60 > c_interval_min);
    c_row c_app%ROWTYPE;

    v_str VARCHAR2(4000);
  BEGIN
    o_ret_code := '0';
    FOR c_row IN c_app
    LOOP
      BEGIN
        proc_send_urge_mail(c_row.business_id, v_sysdate);
      EXCEPTION
        WHEN OTHERS THEN
          -- 放弃此次循环
          NULL;
      END;
    END LOOP;

    SELECT listagg(b.stru_sname, ',') within GROUP(ORDER BY t.business_id)
      INTO v_str
      FROM dams_noti_app_feedback t, dams_branch b
     WHERE t.business_id = i_source_business_id
       AND t.stru_id = b.stru_id
       AND t.stru_id IN
           (SELECT column_value FROM TABLE(func_split2(i_stru_ids)))
       AND (v_sysdate - t.urge_time) * 24 * 60 <= c_interval_min;
    IF v_str IS NOT NULL THEN
      o_msg := v_str || '的催办间隔不能小于' || c_interval_min || '分钟';
    END IF;

    UPDATE dams_noti_app_feedback t
       SET t.urge_time = v_sysdate
     WHERE t.business_id = i_source_business_id
       AND t.stru_id IN
           (SELECT column_value FROM TABLE(func_split2(i_stru_ids)))
       AND (t.urge_time IS NULL OR
           (v_sysdate - t.urge_time) * 24 * 60 > c_interval_min);

  EXCEPTION
    WHEN OTHERS THEN
      o_ret_code := '-1';
      pack_log.error(v_proc_name, 1, v_param);
  END;


  PROCEDURE proc_get_jbpm_param(i_ssic_id   IN VARCHAR2,
                                i_role_id   IN VARCHAR2,
                                i_stru_id   IN VARCHAR2,
                                o_ret_code  OUT VARCHAR2, --返回标志
                                o_msg       OUT VARCHAR2,
                                o_next_user OUT VARCHAR2) IS

    v_proc_name  db_log.proc_name%TYPE := 'pckg_ab_notice.proc_get_jbpm_param'; --存储过程名称
    v_step_num   db_log.step_no%TYPE; --步骤号
    v_param      db_log.info%TYPE := i_ssic_id || '|' || i_role_id || '|' ||
                                     i_stru_id;
    v_stru_id    dams_branch.stru_id%TYPE;
    v_stru_grade dams_branch.stru_grade%TYPE;
    v_stru_level dams_branch_catalog.stru_level%TYPE;
    v_stru_sign  dams_branch.stru_sign%TYPE;
    v_count      NUMBER;
    v_count1     NUMBER;
  BEGIN
    --STEP 0: 初始化处理日志的相关参数
    pack_log.info(v_proc_name, pack_log.start_step,
                  pack_log.start_msg || v_param);
    v_step_num := '#00';

    -- 获取所属行 及 stru_grade
    SELECT c.major_stru_id,
           (SELECT b.stru_grade
              FROM dams_branch b
             WHERE b.stru_id = c.major_stru_id)
      INTO v_stru_id, v_stru_grade
      FROM dams_branch_catalog c
     WHERE c.stru_id = i_stru_id;
    IF v_stru_id = '0010100000' THEN
      --总行
      SELECT COUNT(1)
        INTO v_count
        FROM dams_user_role_rel ur, dams_branch_catalog bc
       WHERE ur.stru_id = bc.stru_id
         AND ur.role_id = 'AB002'
         AND bc.major_stru_id = '0010100000'
         AND ur.ssic_id = i_ssic_id
         AND ur.auth_state <> '3';
      IF v_count > 0 THEN
        o_next_user := '1'; --是负责人,直接发送
      ELSE
        o_next_user := '10'; --不是负责人,送总行复核人
      END IF;
    ELSE
      --分行
      SELECT t.stru_level, b.stru_sign
        INTO v_stru_level, v_stru_sign
        FROM dams_branch_catalog t, dams_branch b
       WHERE b.stru_id = t.stru_id
         AND t.stru_id = v_stru_id;

      -- 二级分行一下直接取机构,一二级行取部室
      IF v_stru_level >= 4 OR v_stru_sign = '8' OR v_stru_sign = '18' THEN
        SELECT t.major_stru_id
          INTO v_stru_id
          FROM dams_branch_catalog t
         WHERE t.stru_id = i_stru_id;
      ELSE
        SELECT b.stru_id
          INTO v_stru_id
          FROM dams_branch_catalog t, dams_branch b
         WHERE b.sup_stru = t.major_stru_id
           AND t.stru_id = i_stru_id
           AND b.stru_sign = '2';
      END IF;
      SELECT COUNT(1)
        INTO v_count
        FROM dams_user_role_rel ur, dams_branch_relation br
       WHERE ur.stru_id = br.bnch_id
         AND ur.role_id = 'AB003'
         AND br.up_bnch_id = v_stru_id
         AND ur.ssic_id = i_ssic_id
         AND ur.auth_state <> '3';
      IF v_count > 0 THEN
        o_next_user := '1'; --是负责人,直接发送
      ELSE
        o_next_user := '20'; --不是负责人,送总行复核人
      END IF;

    END IF;
    o_ret_code := '0';

  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, v_step_num, '异常！' || v_param);
      o_ret_code := '-1';
      ROLLBACK;
      RETURN;
  END proc_get_jbpm_param;
END pckg_ab_notice;
//


CREATE OR REPLACE PACKAGE CTP_MX_PCKG_INIT AS

  type ref_cursor IS ref CURSOR;
  --rcuror ref_cursor;

  PROCEDURE proc_get_appinfo(appinfo OUT ref_cursor);

  PROCEDURE proc_get_servinfo(appname IN varchar2, servinfo OUT ref_cursor);
  --end proc_get_servinfo;

  PROCEDURE proc_get_monitor_switch(appname    IN varchar2,
                                    switchinfo OUT ref_cursor);
  --end proc_get_monitor_switch;

  PROCEDURE proc_get_useablity_info(checkers OUT ref_cursor);
  --end proc_get_useablity_info;

  PROCEDURE proc_get_trade_define(trades OUT ref_cursor);
  --end proc_get_trade_define;

  PROCEDURE proc_get_resource_define(resources OUT ref_cursor);
  --end proc_get_resource_define;
  PROCEDURE proc_get_trade_info(tradeRef OUT ref_cursor);

  PROCEDURE proc_get_resource_info(resourceRef OUT ref_cursor);
END;
//

CREATE OR REPLACE PACKAGE BODY CTP_MX_PCKG_INIT AS

  PROCEDURE proc_get_appinfo(appinfo OUT ref_cursor) AS
  BEGIN
    OPEN appinfo FOR
      SELECT appname_en, version, type FROM ctp_mx_app_info;
  END;

  PROCEDURE proc_get_servinfo(appname IN varchar2, servinfo OUT ref_cursor) AS
  BEGIN
    OPEN servinfo FOR
      SELECT a.ip ip, b.port port
        FROM ctp_mx_server_ip a, ctp_mx_server_port b
       WHERE a.hostname = b.hostname
         AND a.hostName = appname;
  END;

  --end proc_get_servinfo;

  PROCEDURE proc_get_monitor_switch(appname    IN varchar2,
                                    switchinfo OUT ref_cursor) AS
  BEGIN
    OPEN switchinfo FOR
      SELECT switchtype, channeltype, action
        FROM ctp_mx_monitor_flag
       WHERE hostname = appname
       ORDER BY hostname, switchtype;
  END;

  --end proc_get_monitor_switch;

  PROCEDURE proc_get_useablity_info(checkers OUT ref_cursor) AS
  BEGIN
    OPEN checkers FOR
      SELECT name, modulecode, submodulecode, checker, status, msg
        FROM ctp_mx_usability_info
       WHERE ismonitor = '1'
       ORDER BY name;
  END;

  --end proc_get_useablity_info;

  PROCEDURE proc_get_trade_define(trades OUT ref_cursor) AS
  BEGIN
    OPEN trades FOR
      SELECT b.eventCode  eventCode,
             b.eventName  eventName,
             a.eventlevel eventlevel,
             a.trancode   trancode,
             a.occurtime  occurtime
        FROM ctp_mx_trans_error_level a, ctp_mx_trans_error_info b
       WHERE a.eventCode = b.eventCode
         AND b.ismonitor = '1'
       ORDER BY eventCode, trancode, occurtime;
  END;

  PROCEDURE proc_get_resource_define(resources OUT ref_cursor) AS
  BEGIN
    OPEN resources FOR
      SELECT b.resourcecode,
             b.resourcerate,
             b.type,
             b.monitor,
             a.occurtime,
             a.eventlevel
        FROM ctp_mx_resource_level_define a, ctp_mx_resource_info b
       WHERE b.resourcecode = a.resourcecode
         AND b.ismonitor = '1'
       ORDER BY resourcecode, a.occurtime, a.eventlevel;
  END;

--end proc_get_trade_define;
  PROCEDURE proc_get_trade_info(tradeRef OUT ref_cursor) AS
  BEGIN
    OPEN tradeRef FOR
      SELECT eventCode, eventName FROM ctp_mx_trans_error_info where ismonitor = '1';
  END;

  PROCEDURE proc_get_resource_info(resourceRef OUT ref_cursor) AS
  BEGIN
    OPEN resourceRef FOR
      SELECT resourceCode, resourceName FROM ctp_mx_resource_info where ismonitor = '1';
  END;
END CTP_MX_PCKG_INIT;
//



CREATE OR REPLACE PACKAGE pckg_dams_branch_sys_rel IS
  TYPE ret_list IS REF CURSOR;
 
  PROCEDURE proc_branch_sys_rel_qry(i_syscode IN VARCHAR2,
                                    o_flag    OUT VARCHAR2, --存储过程返回标志
                                    o_br_list OUT ret_list --机构列表
                                    );
 
  PROCEDURE proc_sys_rel_qry(i_ssic    IN VARCHAR2,
                             o_flag    OUT VARCHAR2, --存储过程返回标志
                             o_br_list OUT ret_list --机构列表
                             );
END pckg_dams_branch_sys_rel;
//
CREATE OR REPLACE PACKAGE BODY pckg_dams_branch_sys_rel IS

  /******************************************************************************
  -- 名称       pckg_dams_branch_sys_rel.proc_branch_sys_rel_qry
  -- 功能     　查询子系统下的所有已投产机构
  -- 应用模块   平台子系统管理
  -- 参数
                i_syscode        IN VARCHAR2,  --子系统编号
                o_flag           OUT VARCHAR2  --存储过程返回标志
                o_br_list      OUT ret_list, --机构列表

  -- REVISIONS:
  --    Ver           Date            Author                 Description
  -- ---------      ----------      ----------        --------------------------
  --   1.0           20130130         kfzx-saqirg            create this procedure.
  ******************************************************************************/
  PROCEDURE proc_branch_sys_rel_qry(i_syscode IN VARCHAR2,
                                    o_flag    OUT VARCHAR2, --存储过程返回标志
                                    o_br_list OUT ret_list --机构列表
                                    ) IS
    V_STEP      DB_LOG.STEP_NO%TYPE;
    V_PARAMS    DB_LOG.INFO%TYPE;
    V_PROC_NAME DB_LOG.PROC_NAME%TYPE := 'pckg_dams_branch_sys_rel.proc_branch_sys_rel_qry';
  BEGIN
    PACK_LOG.INFO(V_PROC_NAME,
                  PACK_LOG.START_STEP,
                  PACK_LOG.START_MSG || V_PARAMS);
    V_PARAMS := i_syscode;
    V_STEP   := '1';
    o_flag   := '0';
    OPEN o_br_list FOR
      SELECT stru_id
      FROM   dams_branch_sys_rel
      WHERE  start_use = '1' --启用
             AND sys_code = i_syscode;
    PACK_LOG.INFO(V_PROC_NAME, PACK_LOG.END_STEP, PACK_LOG.END_MSG || V_PARAMS);
  EXCEPTION
    WHEN OTHERS THEN
      o_flag := '-1';
      PACK_LOG.ERROR(V_PROC_NAME, V_STEP, V_PARAMS);
  END;
  /******************************************************************************
  -- 名称       pckg_dams_branch_sys_rel.proc_sys_rel_qry
  -- 功能     　查询已投产机构
  -- 应用模块   平台子系统管理
  -- 参数
                i_ssic         IN VARCHAR2,  --统一认证号
                o_flag         OUT VARCHAR2  --存储过程返回标志
                o_br_list      OUT o_br_list, --机构列表

  -- REVISIONS:
  --    Ver           Date            Author                 Description
  -- ---------      ----------      ----------        --------------------------
  --   1.0           20130130         kfzx-saqirg            create this procedure.
  ******************************************************************************/
  PROCEDURE proc_sys_rel_qry(i_ssic    IN VARCHAR2,
                             o_flag    OUT VARCHAR2, --存储过程返回标志
                             o_br_list OUT ret_list --机构列表
                             ) IS
    V_STEP      DB_LOG.STEP_NO%TYPE;
    V_PARAMS    DB_LOG.INFO%TYPE;
    V_PROC_NAME DB_LOG.PROC_NAME%TYPE := 'pckg_dams_branch_sys_rel.proc_sys_rel_qry';
  BEGIN
    PACK_LOG.INFO(V_PROC_NAME,
                  PACK_LOG.START_STEP,
                  PACK_LOG.START_MSG || V_PARAMS);
    V_PARAMS := i_ssic;
    V_STEP   := '1';
    o_flag   := '0';
    OPEN o_br_list FOR
      SELECT DISTINCT SR.STRU_ID, SR.SYS_CODE
      FROM   DAMS_USER_ROLE_REL RR, DAMS_BRANCH_SYS_REL SR
      WHERE  DECODE(RR.STRU_ID, '0010100000', '0010100500', RR.STRU_ID) = SR.STRU_ID
             AND RR.SSIC_ID = i_ssic
             AND RR.ROLE_ID = 'OIS099'
             AND SR.START_USE = '1'; --启用
    PACK_LOG.INFO(V_PROC_NAME, PACK_LOG.END_STEP, PACK_LOG.END_MSG || V_PARAMS);
  EXCEPTION
    WHEN OTHERS THEN
      o_flag := '-1';
      PACK_LOG.ERROR(V_PROC_NAME, V_STEP, V_PARAMS);
  END;
END pckg_dams_branch_sys_rel;
//


CREATE OR REPLACE PACKAGE pckg_branch_selector IS

  -- Author  : KFZX-LINLC
  -- Created : 2016/7/15 9:14:16
  -- Purpose :

  PROCEDURE proc_get_simple_tree(i_stru_id  IN VARCHAR2, --申请人
                                 i_level    IN VARCHAR2,
                                 o_ret_code OUT VARCHAR2,
                                 o_list     OUT SYS_REFCURSOR);

  PROCEDURE proc_get_rules(i_stru_id  IN VARCHAR2,
                           i_func     IN VARCHAR2,
                           o_ret_code OUT VARCHAR2,
                           o_list     OUT SYS_REFCURSOR);

END pckg_branch_selector;
//
CREATE OR REPLACE PACKAGE BODY pckg_branch_selector IS

  /******************************************************************************
      --存储过程名：    proc_get_simple_tree
      --存储过程描述：
      --功能模块：
      --作者：          kfzx-linlc
      --时间：          2015-09-17
      --修改历史:
  ******************************************************************************/
  PROCEDURE proc_get_simple_tree(i_stru_id  IN VARCHAR2,
                                 i_level    IN VARCHAR2,
                                 o_ret_code OUT VARCHAR2,
                                 o_list     OUT SYS_REFCURSOR) IS
    v_param     db_log.info%TYPE := i_stru_id || '|' || i_level;
    v_proc_name db_log.proc_name%TYPE := 'pckg_branch_selector.proc_get_simple_tree';
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step,
                  pack_log.start_msg || v_param);
    o_ret_code := 0;

    OPEN o_list FOR

      SELECT LEVEL,
             t.stru_id,
             t.stru_fname,
             t.stru_sname,
             t.stru_sign,
             t.sup_stru,
             t.man_grade,
             t.stru_sort,
             t.stru_lv,
             t.stru_grade
        FROM dams_branch t
       START WITH stru_id = i_stru_id
      CONNECT BY PRIOR stru_id = sup_stru
             AND stru_state IN ('1', '2')
             AND LEVEL <= i_level
       ORDER SIBLINGS BY stru_sort DESC NULLS LAST;

    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_ret_code := -1;
  END proc_get_simple_tree;

  /******************************************************************************
      --存储过程名：    proc_get_rules
      --存储过程描述：
      --功能模块：
      --作者：          kfzx-linlc
      --时间：          2015-09-17
      --修改历史:
  ******************************************************************************/
  PROCEDURE proc_get_rules(i_stru_id  IN VARCHAR2,
                           i_func     IN VARCHAR2,
                           o_ret_code OUT VARCHAR2,
                           o_list     OUT SYS_REFCURSOR) IS
    v_param     db_log.info%TYPE := i_stru_id || '|' || i_func;
    v_proc_name db_log.proc_name%TYPE := 'pckg_branch_selector.proc_get_rules';

    v_count NUMBER(5);
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step,
                  pack_log.start_msg || v_param);
    o_ret_code := 0;

    SELECT COUNT(1)
      INTO v_count
      FROM dams_stru_select_range t
     WHERE t.func = i_func
       AND stru_id = i_stru_id;

    IF v_count = 0 THEN
      OPEN o_list FOR
        SELECT t.rule_id, t.rule_type, t.rule_content, t.max_level
          FROM dams_stru_select_range t
         WHERE t.func = i_func
           AND stru_id = 'default'
         ORDER BY t.rule_id;
    ELSE
      OPEN o_list FOR
        SELECT t.rule_id, t.rule_type, t.rule_content, t.max_level
          FROM dams_stru_select_range t
         WHERE t.func = i_func
           AND stru_id = i_stru_id
         ORDER BY t.rule_id;
    END IF;

    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_ret_code := -1;
  END proc_get_rules;

END pckg_branch_selector;
//




CREATE OR REPLACE package CTP_MX_PCKG_STAT is

  -- Author  : KFZX-ZHANGFAN
  -- Created : 2010-5-24 9:02:15
  -- Purpose :

  TYPE r_peak_list IS RECORD(
    indextype ctp_mx_stat_peak_info.indextype%TYPE,
    peakvalue number);
  TYPE cur_peak_list IS REF CURSOR RETURN r_peak_list;

  TYPE r_avgop_time IS RECORD(
    opname  ctp_mx_stat_opavgtime_info.opname%TYPE,
    avgtime number);
  TYPE cur_avgop_time IS REF CURSOR RETURN r_avgop_time;
  --插入统计峰值
  procedure proc_update_peak_value(i_hostname  in varchar2,
                                   i_indextype in varchar2,
                                   i_peakvalue number);
  --查询统计峰值
  procedure proc_qry_peak_value(o_peak_list OUT cur_peak_list);
  --删除统计峰值
  procedure proc_del_peak_value;

  --查询统计交易平均时间
  procedure proc_qry_avgop_time(o_avgop_time OUT cur_avgop_time);
  --删除统计峰值
  procedure proc_del_avgop_time;
  procedure proc_update_monitor_flag(i_switchType  in varchar2,
                                     i_channelType in varchar2,
                                     i_action      in varchar2,
                                     i_modifyuser  in varchar2,
                                     i_hostname    in varchar2);
end CTP_MX_PCKG_STAT;

//
CREATE OR REPLACE package body CTP_MX_PCKG_STAT is

  --插入统计峰值
  procedure proc_update_peak_value(i_hostname  in varchar2,
                                   i_indextype in varchar2,
                                   i_peakvalue number) is
  begin
    insert into ctp_mx_stat_peak_info
      (hostname, indextype, peakvalue, statDate)
    values
      (i_hostname, i_indextype, i_peakvalue, sysdate);
    commit;
  end proc_update_peak_value;

  --查询统计峰值
  procedure proc_qry_peak_value(o_peak_list OUT cur_peak_list) is
  begin
    OPEN o_peak_list FOR
      select indextype,
             ceil(sum(peakvalue) / count(peakvalue)) as peakvalue
        from (select indextype, hostname, max(peakvalue) as peakvalue
                from ctp_mx_stat_peak_info
               where indextype is not null
               group by indextype, hostname)
       group by indextype;

  end proc_qry_peak_value;

  --删除统计峰值
  procedure proc_del_peak_value is
  begin
    delete from ctp_mx_stat_peak_info;
    commit;
  end proc_del_peak_value;

  --查询统计峰值
  procedure proc_qry_avgop_time(o_avgop_time OUT cur_avgop_time) is
  begin
    OPEN o_avgop_time FOR
      select opname, avgtime
        from (select opname, ceil(sum(opTime) / sum(opcount)) as avgtime
                from ctp_mx_stat_opavgtime_info
               group by opname
               order by avgtime desc)
       where rownum <= 10;

  end proc_qry_avgop_time;

  --删除统计峰值
  procedure proc_del_avgop_time is
  begin
    delete from ctp_mx_stat_opavgtime_info;
    commit;
  end proc_del_avgop_time;

  --更新监控开关

  procedure proc_update_monitor_flag(i_switchType  in varchar2,
                                     i_channelType in varchar2,
                                     i_action      in varchar2,
                                     i_modifyuser  in varchar2,
                                     i_hostname    in varchar2) is
  begin
    if i_switchtype = '0' then
      if i_channelType = '0' then
        update ctp_mx_monitor_flag
           set action     = i_action,
               modifyuser = i_modifyuser,
               midifydate = sysdate
         where hostname = i_hostname;
      else
        update ctp_mx_monitor_flag
           set action     = i_action,
               modifyuser = i_modifyuser,
               midifydate = sysdate
         where hostname = i_hostname
           and channelType = i_channelType;
      end if;
    else
      if i_channelType = '0' then
        update ctp_mx_monitor_flag
           set action     = i_action,
               modifyuser = i_modifyuser,
               midifydate = sysdate
         where hostname = i_hostname
           and switchType = i_switchType;
      else
        update ctp_mx_monitor_flag
           set action     = i_action,
               modifyuser = i_modifyuser,
               midifydate = sysdate
         where hostname = i_hostname
           and switchType = i_switchType
           and channelType = i_channelType;
      end if;
    end if;

    commit;
  end proc_update_monitor_flag;
end CTP_MX_PCKG_STAT;
//



CREATE OR REPLACE PACKAGE ctpext_pkg_file IS



  TYPE ref_cur IS REF CURSOR;


  PROCEDURE proc_file_upload(i_user_id          IN VARCHAR2, --操作用户
                             i_file_name        IN VARCHAR2, --文件名
                             i_file_data        IN BLOB, --文件blob数据
                             i_ref_id           IN VARCHAR2, --文件引用编号
                             i_table_name       IN VARCHAR2, --存储文件表表名
                             i_fileid_colname   IN VARCHAR2, --文件id字段名
                             i_filename_colname IN VARCHAR2, --文件名字段名
                             i_fileref_colname  IN VARCHAR2, --文件引用编号字段名
                             i_blob_colname     IN VARCHAR2, --文件blob字段名
                             i_suff_colname     IN VARCHAR2, --后缀字段名
                             i_fileid_seq       IN VARCHAR2, --文件id序列器
                             i_ext_colname      IN VARCHAR2, --额外的字段名
                             i_max_files        IN VARCHAR2, --上传的最大文件数
                             o_retcode          OUT VARCHAR2,
                             o_file_id          OUT VARCHAR2);

  PROCEDURE proc_new_file_ref(i_user_id     IN VARCHAR2,
                              i_fileref_seq IN VARCHAR2, --文件编号序列器
                              o_retcode     OUT VARCHAR2,
                              o_ref_id      OUT VARCHAR2);

  PROCEDURE proc_file_download(i_user_id          IN VARCHAR2,
                               i_table_name       IN VARCHAR2, --存储文件表表名
                               i_fileid_colname   IN VARCHAR2, --文件id字段名
                               i_file_id          IN VARCHAR2, --需要下载的文件id
                               i_filename_colname IN VARCHAR2, --文件名字段名
                               i_suff_colname     IN VARCHAR2, --后缀字段名
                               i_blob_colname     IN VARCHAR2, --文件blob字段名
                               o_retcode          OUT VARCHAR2,
                               o_file_name        OUT VARCHAR2, --需要下载的文件名
                               o_file_suff        OUT VARCHAR2, --需要下载的文件后缀
                               o_file_data        OUT BLOB);


  PROCEDURE proc_file_list(i_user_id          IN VARCHAR2,
                           i_table_name       IN VARCHAR2, --存储文件表表名
                           i_fileref_colname  IN VARCHAR2, --文件引用编号字段名
                           i_ref_id           IN VARCHAR2, --文件引用编号
                           i_fileid_colname   IN VARCHAR2, --文件id字段名
                           i_filename_colname IN VARCHAR2, --文件名字段名
                           i_suff_colname     IN VARCHAR2, --后缀字段名
                           i_ext_colname      IN VARCHAR2, --额外的字段名
                           i_valid_also       IN VARCHAR2, --是否只取有效数据
                           o_retcode          OUT VARCHAR2,
                           o_total_num        OUT VARCHAR2, --文件个数
                           o_file_detail      OUT ref_cur);

  PROCEDURE proc_file_delete(i_user_id        IN VARCHAR2,
                             i_table_name     IN VARCHAR2, --存储文件表表名
                             i_fileid_colname IN VARCHAR2, --文件id字段名
                             i_file_ids       IN VARCHAR2, --需要删除的的文件id序列 例如1,2,3
                             o_retcode        OUT VARCHAR2);


  PROCEDURE proc_file_updateflag(i_user_id        IN VARCHAR2,
                                 i_table_name     IN VARCHAR2, --存储文件表表名
                                 i_fileid_colname IN VARCHAR2, --文件id字段名
                                 i_file_ids       IN VARCHAR2, --需要更新的的文件id序列 例如1,2,3
                                 i_flag_colname   IN VARCHAR2,
                                 i_flag_value     IN VARCHAR2,
                                 o_retcode        OUT VARCHAR2);

  PROCEDURE proc_files_upload(i_user_id          IN VARCHAR2, --操作用户
                             i_file1_name        IN VARCHAR2, --文件名
                             i_file1_data        IN BLOB, --文件blob数据
                             i_file2_name        IN VARCHAR2, --文件名
                             i_file2_data        IN BLOB, --文件blob数据
                             i_file3_name        IN VARCHAR2, --文件名
                             i_file3_data        IN BLOB, --文件blob数据
                             i_file4_name        IN VARCHAR2, --文件名
                             i_file4_data        IN BLOB, --文件blob数据
                             i_file5_name        IN VARCHAR2, --文件名
                             i_file5_data        IN BLOB, --文件blob数据                                                                                                                 
                             i_ref_id           IN VARCHAR2, --文件引用编号
                             i_table_name       IN VARCHAR2, --存储文件表表名
                             i_fileid_colname   IN VARCHAR2, --文件id字段名
                             i_filename_colname IN VARCHAR2, --文件名字段名
                             i_fileref_colname  IN VARCHAR2, --文件引用编号字段名
                             i_blob_colname     IN VARCHAR2, --文件blob字段名
                             i_suff_colname     IN VARCHAR2, --后缀字段名
                             i_fileid_seq       IN VARCHAR2, --文件id序列器
                             i_ext_colname      IN VARCHAR2, --额外的字段名
                             i_max_files        IN VARCHAR2, --上传的最大文件数
                             o_retcode          OUT VARCHAR2,
                             o_file_id          OUT VARCHAR2);

END ctpext_pkg_file;
//

CREATE OR REPLACE PACKAGE BODY ctpext_pkg_file IS
  /******************************************************************************
    NAME:      proc_file_upload
    MODULE:    BLOB文件上传、下载
    PURPOSE:   文件上传
    REVISIONS:
    REMARK:    文件上传：需传入引用编号（若没有,需要生成一个引用编号）,因此不需要传入引用引用编号序列器
    Ver        Date        Author           Description
    ---------  ----------  ---------------  ------------------------------------
    1.0        2012-11-20  KFZX-ZHANGT02    新建
  ******************************************************************************/
  PROCEDURE proc_file_upload(i_user_id          IN VARCHAR2, --操作用户
                             i_file_name        IN VARCHAR2, --文件名
                             i_file_data        IN BLOB, --文件blob数据
                             i_ref_id           IN VARCHAR2, --文件引用编号
                             i_table_name       IN VARCHAR2, --存储文件表表明
                             i_fileid_colname   IN VARCHAR2, --文件id字段名
                             i_filename_colname IN VARCHAR2, --文件名字段名
                             i_fileref_colname  IN VARCHAR2, --文件引用编号字段名
                             i_blob_colname     IN VARCHAR2, --文件blob字段名
                             i_suff_colname     IN VARCHAR2, --后缀字段名
                             i_fileid_seq       IN VARCHAR2, --文件id序列器
                             i_ext_colname      IN VARCHAR2, --额外的字段名
                             i_max_files        IN VARCHAR2, --上传的最大文件数
                             o_retcode          OUT VARCHAR2,
                             o_file_id          OUT VARCHAR2) AS

    v_dynamic_sql VARCHAR2(4000);

    v_userid_colname   VARCHAR2(50);
    v_updtime_colname  VARCHAR2(50);
    v_isvalid_colname  VARCHAR2(50);
    i_ext_colname_temp VARCHAR2(1000);

    empty_data EXCEPTION;
    empty_field EXCEPTION;
    error_extcol EXCEPTION;

    v_num_temp NUMBER(10);
    v_point    NUMBER(10);

    v_file_id   VARCHAR2(50);
    v_file_suff VARCHAR2(100);
    v_msg       VARCHAR2(1000);
  BEGIN
    --初始化处理日志的相关参数

    IF i_ref_id IS NULL OR i_file_name IS NULL THEN
      --OR i_file_data IS NULL
      v_msg := '部分文件数据为空';
      RAISE empty_data;
    END IF;

    IF i_table_name IS NULL OR i_fileid_colname IS NULL OR
       i_filename_colname IS NULL OR i_fileref_colname IS NULL OR
       i_blob_colname IS NULL OR i_suff_colname IS NULL OR
       i_fileid_seq IS NULL OR i_ext_colname IS NULL THEN
      v_msg := '部分文件存储表表数据为空';
      RAISE empty_field;
    END IF;


    --分析数据,分析额外字段名,以“,”分割
    i_ext_colname_temp := i_ext_colname || ',';
    v_point            := instr(i_ext_colname_temp, ',');

    v_userid_colname   := substr(i_ext_colname_temp, 1, v_point - 1); --操作用户ID字段名
    i_ext_colname_temp := substr(i_ext_colname_temp, v_point + 1);

    IF i_ext_colname_temp IS NULL THEN
      v_msg := '错误的额外字段名';
      RAISE error_extcol;
    END IF;

    v_point            := instr(i_ext_colname_temp, ',');
    v_updtime_colname  := substr(i_ext_colname_temp, 1, v_point - 1);
    i_ext_colname_temp := substr(i_ext_colname_temp, v_point + 1);

    IF i_ext_colname_temp IS NULL THEN
      v_msg := '错误的额外字段名';
      RAISE error_extcol;
    END IF;

    v_point            := instr(i_ext_colname_temp, ',');
    v_isvalid_colname  := substr(i_ext_colname_temp, 1, v_point - 1);
    i_ext_colname_temp := substr(i_ext_colname_temp, v_point + 1);


    v_file_suff := substr(i_file_name, instr(i_file_name, '.', -1, 1) + 1);


    v_dynamic_sql := 'SELECT ''' || to_char(SYSDATE, 'YYYYMMDD') ||
                     ''' || lpad(' || i_fileid_seq ||
                     '.nextval,10,0) FROM dual';
    EXECUTE IMMEDIATE v_dynamic_sql
      INTO v_file_id;


    v_dynamic_sql := 'INSERT INTO ' || i_table_name || '(' ||
                     i_fileid_colname || ',' || i_filename_colname || ',' ||
                     i_suff_colname || ',' || i_fileref_colname || ',' ||
                     v_userid_colname || ',' || v_updtime_colname || ',' ||
                     i_blob_colname || ',' || v_isvalid_colname ||
                     ') VALUES(:1, :2, :3, :4, :5, :6, :7, :8)';
    EXECUTE IMMEDIATE v_dynamic_sql
      USING v_file_id, i_file_name, v_file_suff, i_ref_id, i_user_id, to_date(SYSDATE), i_file_data, '0';
    o_file_id := v_file_id;


    o_retcode := '0';

  EXCEPTION
    WHEN empty_data THEN
      ROLLBACK;
      o_retcode := '-10';

    WHEN empty_field THEN
      ROLLBACK;
      o_retcode := '-11';

    WHEN OTHERS THEN
      ROLLBACK;
      o_retcode := '-1';

  END proc_file_upload;

  /******************************************************************************
    NAME:      proc_new_file_ref
    MODULE:    BLOB文件上传、下载
    PURPOSE:   生成文件引用编号
    REVISIONS:
    REMARK:
    Ver        Date        Author           Description
    ---------  ----------  ---------------  ------------------------------------
    1.0        2012-11-20  KFZX-ZHANGT02    新建
  ******************************************************************************/
  PROCEDURE proc_new_file_ref(i_user_id     IN VARCHAR2,
                              i_fileref_seq IN VARCHAR2, --文件编号序列器
                              o_retcode     OUT VARCHAR2,
                              o_ref_id      OUT VARCHAR2) AS


    v_dynamic_sql VARCHAR2(4000);
    v_msg         VARCHAR2(4000);
  BEGIN
    --初始化处理日志的相关参数



    v_dynamic_sql := 'SELECT ''' || to_char(SYSDATE, 'YYYYMMDD') ||
                     ''' ||lpad(' || i_fileref_seq ||
                     '.nextval,10,0) FROM dual';
    EXECUTE IMMEDIATE v_dynamic_sql
      INTO o_ref_id;


    o_retcode := '0';

  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      o_retcode := '-1';

  END proc_new_file_ref;

  /******************************************************************************
    NAME:      proc_file_download
    MODULE:    BLOB文件上传、下载
    PURPOSE:   下载BLOB文件
    REVISIONS:
    REMARK:
    Ver        Date        Author           Description
    ---------  ----------  ---------------  ------------------------------------
    1.0        2012-11-20  KFZX-ZHANGT02    新建
  ******************************************************************************/
  PROCEDURE proc_file_download(i_user_id          IN VARCHAR2,
                               i_table_name       IN VARCHAR2, --存储文件表表名
                               i_fileid_colname   IN VARCHAR2, --文件id字段名
                               i_file_id          IN VARCHAR2, --需要下载的文件id
                               i_filename_colname IN VARCHAR2, --文件名字段名
                               i_suff_colname     IN VARCHAR2, --后缀字段名
                               i_blob_colname     IN VARCHAR2, --文件blob字段名
                               o_retcode          OUT VARCHAR2,
                               o_file_name        OUT VARCHAR2, --需要下载的文件名
                               o_file_suff        OUT VARCHAR2, --需要下载的文件后缀
                               o_file_data        OUT BLOB) AS

    v_dynamic_sql VARCHAR2(4000);
    v_msg         VARCHAR2(4000);
  BEGIN
    --初始化处理日志的相关参数



    v_dynamic_sql := ' SELECT ' || i_filename_colname || ',' ||
                     i_suff_colname || ',' || i_blob_colname || ' FROM ' ||
                     i_table_name || ' WHERE ' || i_fileid_colname || '=''' ||
                     i_file_id || '''';
    EXECUTE IMMEDIATE v_dynamic_sql
      INTO o_file_name, o_file_suff, o_file_data;
    o_retcode := '0';

  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      o_retcode := '-1';

  END proc_file_download;

  /******************************************************************************
    NAME:      proc_file_list
    MODULE:    BLOB文件上传、下载
    PURPOSE:   获取文件列表
    REVISIONS:
    REMARK:    i_valid_also："1"：获取所有文件（有效,非有效）,否则：删除掉无效文件,
               只取有效文件列表
    Ver        Date        Author           Description
    ---------  ----------  ---------------  ------------------------------------
    1.0        2012-11-20  KFZX-ZHANGT02    新建
  ******************************************************************************/
  PROCEDURE proc_file_list(i_user_id          IN VARCHAR2,
                           i_table_name       IN VARCHAR2, --存储文件表表名
                           i_fileref_colname  IN VARCHAR2, --文件引用编号字段名
                           i_ref_id           IN VARCHAR2, --文件引用编号
                           i_fileid_colname   IN VARCHAR2, --文件id字段名
                           i_filename_colname IN VARCHAR2, --文件名字段名
                           i_suff_colname     IN VARCHAR2, --后缀字段名
                           i_ext_colname      IN VARCHAR2, --额外的字段名
                           i_valid_also       IN VARCHAR2, --是否只取有效数据
                           o_retcode          OUT VARCHAR2,
                           o_total_num        OUT VARCHAR2, --文件个数
                           o_file_detail      OUT ref_cur) AS
  
    v_msg         VARCHAR2(4000);
    v_dynamic_sql VARCHAR2(4000);
    v_is_valid    VARCHAR2(10);
    v_ext_colname VARCHAR2(200);
  
    v_p NUMBER(3, 0);
    v_q NUMBER(3, 0);
    error_extcol EXCEPTION;
  
  BEGIN
    --初始化处理日志的相关参数
  
    v_ext_colname := i_ext_colname || ',';
    v_p           := instr(v_ext_colname, ',', 1, 2);
    v_q           := instr(v_ext_colname, ',', 1, 3);
    IF v_p IS NULL OR v_q IS NULL THEN
      RAISE error_extcol;
    END IF;
    v_is_valid := substr(v_ext_colname, v_p + 1, v_q - v_p - 1);
  
    IF i_valid_also != '1' THEN
      --删除无效数据
      v_dynamic_sql := 'DELETE FROM ' || i_table_name || ' WHERE ' ||
                       i_fileref_colname || '=:1 AND ' || v_is_valid ||
                       ' = :2 ';
      EXECUTE IMMEDIATE v_dynamic_sql
        USING i_ref_id, '0';
    END IF;
  
    v_dynamic_sql := 'SELECT COUNT(1) FROM ' || i_table_name || ' WHERE ' ||
                     i_fileref_colname || '= :1 ';
  
    EXECUTE IMMEDIATE v_dynamic_sql
      INTO o_total_num
      USING i_ref_id;
  
    v_dynamic_sql := ' SELECT ' || i_fileid_colname || ' FILE_ID,' ||
                     i_fileref_colname || ' REF_ID,' || i_filename_colname ||
                     ' FILE_NAME,' || i_suff_colname || ' FILE_SUFF' ||
                     ' FROM ' || i_table_name || ' WHERE ' || i_fileref_colname ||
                     '= :1 AND ' || v_is_valid || ' IS NOT NULL ORDER BY ' ||
                     i_fileid_colname;
    OPEN o_file_detail FOR v_dynamic_sql
      USING i_ref_id;
  
    o_retcode := '0';
  
  EXCEPTION
    WHEN error_extcol THEN
      ROLLBACK;
      o_retcode := '-10';
    
    WHEN OTHERS THEN
      ROLLBACK;
      o_retcode := '-1';
    
  END proc_file_list;
  /******************************************************************************
    NAME:      proc_file_delete
    MODULE:    BLOB文件上传、下载
    PURPOSE:   文件删除
    REVISIONS:
    REMARK:
    Ver        Date        Author           Description
    ---------  ----------  ---------------  ------------------------------------
    1.0        2012-11-20  KFZX-ZHANGT02    新建
  ******************************************************************************/
  PROCEDURE proc_file_delete(i_user_id        IN VARCHAR2,
                             i_table_name     IN VARCHAR2, --存储文件表表名
                             i_fileid_colname IN VARCHAR2, --文件id字段名
                             i_file_ids       IN VARCHAR2, --需要删除的的文件id序列 例如1,2,3
                             o_retcode        OUT VARCHAR2) AS

    sqlstr       VARCHAR2(4000);
    v_msg        VARCHAR2(4000);
  BEGIN
    --初始化处理日志的相关参数



    sqlstr := 'delete from ' || i_table_name || ' where ' ||
              i_fileid_colname || ' in ( ' || i_file_ids || ')';
    EXECUTE IMMEDIATE sqlstr;

    o_retcode := '0';

  EXCEPTION
    WHEN OTHERS THEN
      o_retcode := '-1';
      ROLLBACK;

  END proc_file_delete;
  /******************************************************************************
    NAME:      proc_file_updateflag
    MODULE:    BLOB文件上传、下载
    PURPOSE:   更新文件标识位
    REVISIONS:
    REMARK:    更新标识位 这里用来将新上传的文件更新为可用的
    Ver        Date        Author           Description
    ---------  ----------  ---------------  ------------------------------------
    1.0        2012-11-20  KFZX-ZHANGT02    新建
  ******************************************************************************/
  PROCEDURE proc_file_updateflag(i_user_id        IN VARCHAR2,
                                 i_table_name     IN VARCHAR2, --存储文件表表名
                                 i_fileid_colname IN VARCHAR2, --文件id字段名
                                 i_file_ids       IN VARCHAR2, --需要更新的文件id序列 例如1,2,3
                                 i_flag_colname   IN VARCHAR2, ----标识位字段名
                                 i_flag_value     IN VARCHAR2, ---标识值
                                 o_retcode        OUT VARCHAR2) IS

    v_sqlstr     VARCHAR2(4000);
    v_msg        VARCHAR2(4000);
  BEGIN
    --初始化处理日志的相关参数



    v_sqlstr := 'UPDATE ' || i_table_name || ' SET ' || i_flag_colname ||
                ' = ' || i_flag_value || ' WHERE ' || i_fileid_colname ||
                ' in (' || i_file_ids || ')';
    EXECUTE IMMEDIATE v_sqlstr;

    o_retcode := '0';

  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      o_retcode := '-2';

  END proc_file_updateflag;
  
    /******************************************************************************
    NAME:      proc_files_upload
    MODULE:    五个BLOB文件上传、下载
    PURPOSE:   文件上传
    REVISIONS:
    REMARK:    文件上传：需传入引用编号（若没有,需要生成一个引用编号）,因此不需要传入引用引用编号序列器
    Ver        Date        Author           Description
    ---------  ----------  ---------------  ------------------------------------
    1.0        2016-5-23  KFZX-XUYJ01    新建
  ******************************************************************************/
  PROCEDURE proc_files_upload(i_user_id          IN VARCHAR2, --操作用户
                             i_file1_name        IN VARCHAR2, --文件名
                             i_file1_data        IN BLOB, --文件blob数据
                             i_file2_name        IN VARCHAR2, --文件名
                             i_file2_data        IN BLOB, --文件blob数据
                             i_file3_name        IN VARCHAR2, --文件名
                             i_file3_data        IN BLOB, --文件blob数据
                             i_file4_name        IN VARCHAR2, --文件名
                             i_file4_data        IN BLOB, --文件blob数据
                             i_file5_name        IN VARCHAR2, --文件名
                             i_file5_data        IN BLOB, --文件blob数据                                                                                                                 
                             i_ref_id           IN VARCHAR2, --文件引用编号
                             i_table_name       IN VARCHAR2, --存储文件表表名
                             i_fileid_colname   IN VARCHAR2, --文件id字段名
                             i_filename_colname IN VARCHAR2, --文件名字段名
                             i_fileref_colname  IN VARCHAR2, --文件引用编号字段名
                             i_blob_colname     IN VARCHAR2, --文件blob字段名
                             i_suff_colname     IN VARCHAR2, --后缀字段名
                             i_fileid_seq       IN VARCHAR2, --文件id序列器
                             i_ext_colname      IN VARCHAR2, --额外的字段名
                             i_max_files        IN VARCHAR2, --上传的最大文件数
                             o_retcode          OUT VARCHAR2,
                             o_file_id          OUT VARCHAR2)AS

    v_dynamic_sql VARCHAR2(4000);

    v_userid_colname   VARCHAR2(50);
    v_updtime_colname  VARCHAR2(50);
    v_isvalid_colname  VARCHAR2(50);
    i_ext_colname_temp VARCHAR2(1000);

    empty_data EXCEPTION;
    empty_field EXCEPTION;
    error_extcol EXCEPTION;

    v_num_temp NUMBER(10);
    v_point    NUMBER(10);

    v_file_id   VARCHAR2(50);
    v_file_suff VARCHAR2(100);
    v_msg       VARCHAR2(1000);
    v_proc_name     VARCHAR2(100);
    v_inparam       VARCHAR2(4000);
  BEGIN
    --初始化处理日志的相关参数

    IF i_ref_id IS NULL OR i_file1_name IS NULL THEN
      --OR i_file_data IS NULL
      v_msg := '部分文件数据为空';
      RAISE empty_data;
    END IF;
    v_proc_name := 'ctpext_pkg_file.proc_files_upload';
    v_inparam   := '开始!' || '|' || i_table_name || '|' || i_fileid_colname || '|' ||
                   i_filename_colname || '|' || i_fileref_colname || '|' || i_blob_colname || '|' ||
                   i_suff_colname || '|' || i_fileid_seq || '|' || i_ext_colname;
    pack_log.info(v_proc_name, '', v_inparam);
    IF i_table_name IS NULL OR i_fileid_colname IS NULL OR
       i_filename_colname IS NULL OR i_fileref_colname IS NULL OR
       i_blob_colname IS NULL OR i_suff_colname IS NULL OR
       i_fileid_seq IS NULL OR i_ext_colname IS NULL THEN
      v_msg := '部分文件存储表表数据为空';
      RAISE empty_field;
    END IF;


    --分析数据,分析额外字段名,以“,”分割
    i_ext_colname_temp := i_ext_colname || ',';
    v_point            := instr(i_ext_colname_temp, ',');

    v_userid_colname   := substr(i_ext_colname_temp, 1, v_point - 1); --操作用户ID字段名
    i_ext_colname_temp := substr(i_ext_colname_temp, v_point + 1);

    IF i_ext_colname_temp IS NULL THEN
      v_msg := '错误的额外字段名';
      RAISE error_extcol;
    END IF;

    v_point            := instr(i_ext_colname_temp, ',');
    v_updtime_colname  := substr(i_ext_colname_temp, 1, v_point - 1);
    i_ext_colname_temp := substr(i_ext_colname_temp, v_point + 1);

    IF i_ext_colname_temp IS NULL THEN
      v_msg := '错误的额外字段名';
      RAISE error_extcol;
    END IF;

    v_point            := instr(i_ext_colname_temp, ',');
    v_isvalid_colname  := substr(i_ext_colname_temp, 1, v_point - 1);
    i_ext_colname_temp := substr(i_ext_colname_temp, v_point + 1);
    
    IF i_file1_name IS NOT NULL THEN
      v_file_suff := substr(i_file1_name, instr(i_file1_name, '.', -1, 1) + 1);


      v_dynamic_sql := 'SELECT ''' || to_char(SYSDATE, 'YYYYMMDD') ||
                       ''' || lpad(' || i_fileid_seq ||
                       '.nextval,10,0) FROM dual';
      EXECUTE IMMEDIATE v_dynamic_sql
        INTO v_file_id;


      v_dynamic_sql := 'INSERT INTO ' || i_table_name || '(' ||
                       i_fileid_colname || ',' || i_filename_colname || ',' ||
                       i_suff_colname || ',' || i_fileref_colname || ',' ||
                       v_userid_colname || ',' || v_updtime_colname || ',' ||
                       i_blob_colname || ',' || v_isvalid_colname ||
                       ') VALUES(:1, :2, :3, :4, :5, :6, :7, :8)';
      EXECUTE IMMEDIATE v_dynamic_sql
        USING v_file_id, i_file1_name, v_file_suff, i_ref_id, i_user_id, to_date(SYSDATE), i_file1_data, '1';
        o_file_id := v_file_id;
        
    END IF;
    IF i_file2_name IS NOT NULL THEN
      v_file_suff := substr(i_file2_name, instr(i_file2_name, '.', -1, 1) + 1);


      v_dynamic_sql := 'SELECT ''' || to_char(SYSDATE, 'YYYYMMDD') ||
                       ''' || lpad(' || i_fileid_seq ||
                       '.nextval,10,0) FROM dual';
      EXECUTE IMMEDIATE v_dynamic_sql
        INTO v_file_id;


      v_dynamic_sql := 'INSERT INTO ' || i_table_name || '(' ||
                       i_fileid_colname || ',' || i_filename_colname || ',' ||
                       i_suff_colname || ',' || i_fileref_colname || ',' ||
                       v_userid_colname || ',' || v_updtime_colname || ',' ||
                       i_blob_colname || ',' || v_isvalid_colname ||
                       ') VALUES(:1, :2, :3, :4, :5, :6, :7, :8)';
      EXECUTE IMMEDIATE v_dynamic_sql
        USING v_file_id, i_file2_name, v_file_suff, i_ref_id, i_user_id, to_date(SYSDATE), i_file2_data, '1';
       o_file_id := o_file_id || v_file_id;
    END IF;
    IF i_file3_name IS NOT NULL THEN
      v_file_suff := substr(i_file3_name, instr(i_file3_name, '.', -1, 1) + 1);


      v_dynamic_sql := 'SELECT ''' || to_char(SYSDATE, 'YYYYMMDD') ||
                       ''' || lpad(' || i_fileid_seq ||
                       '.nextval,10,0) FROM dual';
      EXECUTE IMMEDIATE v_dynamic_sql
        INTO v_file_id;


      v_dynamic_sql := 'INSERT INTO ' || i_table_name || '(' ||
                       i_fileid_colname || ',' || i_filename_colname || ',' ||
                       i_suff_colname || ',' || i_fileref_colname || ',' ||
                       v_userid_colname || ',' || v_updtime_colname || ',' ||
                       i_blob_colname || ',' || v_isvalid_colname ||
                       ') VALUES(:1, :2, :3, :4, :5, :6, :7, :8)';
      EXECUTE IMMEDIATE v_dynamic_sql
        USING v_file_id, i_file3_name, v_file_suff, i_ref_id, i_user_id, to_date(SYSDATE), i_file3_data, '1';
      o_file_id := o_file_id || v_file_id;
    END IF;
    IF i_file4_name IS NOT NULL  THEN
          v_file_suff := substr(i_file4_name, instr(i_file4_name, '.', -1, 1) + 1);


      v_dynamic_sql := 'SELECT ''' || to_char(SYSDATE, 'YYYYMMDD') ||
                       ''' || lpad(' || i_fileid_seq ||
                       '.nextval,10,0) FROM dual';
      EXECUTE IMMEDIATE v_dynamic_sql
        INTO v_file_id;


      v_dynamic_sql := 'INSERT INTO ' || i_table_name || '(' ||
                       i_fileid_colname || ',' || i_filename_colname || ',' ||
                       i_suff_colname || ',' || i_fileref_colname || ',' ||
                       v_userid_colname || ',' || v_updtime_colname || ',' ||
                       i_blob_colname || ',' || v_isvalid_colname ||
                       ') VALUES(:1, :2, :3, :4, :5, :6, :7, :8)';
      EXECUTE IMMEDIATE v_dynamic_sql
        USING v_file_id, i_file4_name, v_file_suff, i_ref_id, i_user_id, to_date(SYSDATE), i_file4_data, '1';
       o_file_id := o_file_id || v_file_id;
    END IF;
    IF  i_file5_name IS NOT NULL  THEN
      v_file_suff := substr(i_file5_name, instr(i_file5_name, '.', -1, 1) + 1);


      v_dynamic_sql := 'SELECT ''' || to_char(SYSDATE, 'YYYYMMDD') ||
                       ''' || lpad(' || i_fileid_seq ||
                       '.nextval,10,0) FROM dual';
      EXECUTE IMMEDIATE v_dynamic_sql
        INTO v_file_id;


      v_dynamic_sql := 'INSERT INTO ' || i_table_name || '(' ||
                       i_fileid_colname || ',' || i_filename_colname || ',' ||
                       i_suff_colname || ',' || i_fileref_colname || ',' ||
                       v_userid_colname || ',' || v_updtime_colname || ',' ||
                       i_blob_colname || ',' || v_isvalid_colname ||
                       ') VALUES(:1, :2, :3, :4, :5, :6, :7, :8)';
      EXECUTE IMMEDIATE v_dynamic_sql
        USING v_file_id, i_file5_name, v_file_suff, i_ref_id, i_user_id, to_date(SYSDATE), i_file5_data, '1';
       o_file_id := o_file_id || v_file_id;
    END IF;


    o_retcode := '0';

  EXCEPTION
    WHEN empty_data THEN
      ROLLBACK;
      o_retcode := '-10';

    WHEN empty_field THEN
      ROLLBACK;
      o_retcode := '-11';

    WHEN OTHERS THEN
      ROLLBACK;
      o_retcode := '-1';

  END proc_files_upload;

END ctpext_pkg_file;
//



CREATE OR REPLACE PACKAGE pckg_dams_file_upload IS

  
  TYPE ref_cur IS REF CURSOR;

  PROCEDURE proc_file_upload(i_user_id   IN VARCHAR2, --操作用户
                             i_file_name IN VARCHAR2, --文件名
                             i_file_data IN BLOB, --文件blob数据
                             i_ref_id    IN VARCHAR2, --文件引用编号
                             i_max_files IN VARCHAR2, --上传的最大文件数
                             o_retcode   OUT VARCHAR2,
                             o_file_id   OUT VARCHAR2);

  PROCEDURE proc_new_file_ref(i_user_id IN VARCHAR2,
                              o_retcode OUT VARCHAR2,
                              o_ref_id  OUT VARCHAR2);

  PROCEDURE proc_file_download(i_user_id   IN VARCHAR2,
                               i_file_id   IN VARCHAR2, --需要下载的文件id
                               o_retcode   OUT VARCHAR2,
                               o_file_name OUT VARCHAR2, --需要下载的文件名
                               o_file_data OUT BLOB);

  PROCEDURE proc_file_list(i_user_id      IN VARCHAR2,
                           i_ref_id       IN VARCHAR2, --文件引用编号
                           i_unvalid_also IN VARCHAR2, --是否只取有效数据
                           o_retcode      OUT VARCHAR2,
                           o_total_num    OUT VARCHAR2, --文件个数
                           o_file_detail  OUT ref_cur);

  PROCEDURE proc_file_delete(i_user_id      IN VARCHAR2,
                             i_file_ref_id  IN VARCHAR2, --文件关联编号
                             i_file_ids     IN VARCHAR2, --需要删除的的文件id序列 例如1,2,3
                             i_is_clear_all IN VARCHAR2, -- 是否删除文件关联下的所有文件
                             o_retcode      OUT VARCHAR2);

  PROCEDURE proc_file_updateflag(i_user_id    IN VARCHAR2,
                                 i_file_ref   IN VARCHAR2,
                                 i_file_ids   IN VARCHAR2, --需要更新的文件id序列 例如1,2,3
                                 i_flag_value IN VARCHAR2, ---标识值
                                 o_retcode    OUT VARCHAR2);
  PROCEDURE proc_copy_with_ref(i_user_id      IN VARCHAR2,
                               i_file_ref     IN VARCHAR2,
                               i_paste_ref_id IN VARCHAR2,
                               o_retcode      OUT VARCHAR2,
                               o_ref_id       OUT VARCHAR2);
  PROCEDURE proc_copy_with_file_id(i_user_id      IN VARCHAR2,
                                   i_file_ids     IN VARCHAR2,
                                   i_paste_ref_id IN VARCHAR2,
                                   o_retcode      OUT VARCHAR2,
                                   o_ref_id       OUT VARCHAR2);
END pckg_dams_file_upload;
//
CREATE OR REPLACE PACKAGE BODY pckg_dams_file_upload IS
  /******************************************************************************
    NAME:      proc_file_upload
    MODULE:    BLOB文件上传、下载
    PURPOSE:   文件上传
    REVISIONS:
    REMARK:    文件上传：需传入引用编号（若没有,需要生成一个引用编号）,因此不需要传入引用引用编号序列器
    Ver        Date        Author           Description
    ---------  ----------  ---------------  ------------------------------------
    1.0        2015-11-09  KFZX-linlc    新建
  ******************************************************************************/
  PROCEDURE proc_file_upload(i_user_id   IN VARCHAR2, --操作用户
                             i_file_name IN VARCHAR2, --文件名
                             i_file_data IN BLOB, --文件blob数据
                             i_ref_id    IN VARCHAR2, --文件引用编号
                             i_max_files IN VARCHAR2, --上传的最大文件数
                             o_retcode   OUT VARCHAR2,
                             o_file_id   OUT VARCHAR2) AS
    empty_data EXCEPTION;

    v_file_id VARCHAR2(50);
    v_msg     VARCHAR2(1000);

    v_sysdate DATE := SYSDATE;
  BEGIN
    /*
    TODO: owner="kfzx-linlc" created="2015/11/9"
    text="valid（是否需要）"
    */

    IF i_ref_id IS NULL OR i_file_name IS NULL THEN
      v_msg := '部分文件数据为空';
      RAISE empty_data;
    END IF;

    SELECT lpad(dams_file_id_seq.nextval, 20, '0') INTO v_file_id FROM dual;

    -- 插入文件
    INSERT INTO dams_file
      (file_id, file_name, file_content, create_time, create_user)
    VALUES
      (v_file_id, i_file_name, i_file_data, v_sysdate, i_user_id);

    -- 文件关联
    INSERT INTO dams_file_ref
      (file_ref_id, file_id, create_time, create_user)
    VALUES
      (i_ref_id, v_file_id, v_sysdate, i_user_id);

    o_file_id := v_file_id;
    o_retcode := '0';

  EXCEPTION
    WHEN empty_data THEN
      o_retcode := '-1';

    WHEN OTHERS THEN
      o_retcode := '-1';

  END proc_file_upload;

  /******************************************************************************
    NAME:      proc_new_file_ref
    MODULE:    BLOB文件上传、下载
    PURPOSE:   生成文件引用编号
    REVISIONS:
    REMARK:
    Ver        Date        Author           Description
    ---------  ----------  ---------------  ------------------------------------
    1.0        2012-11-20  KFZX-ZHANGT02    新建
    2.0        2015-11-09  KFZX-linlc    新建
  ******************************************************************************/
  PROCEDURE proc_new_file_ref(i_user_id IN VARCHAR2,
                              o_retcode OUT VARCHAR2,
                              o_ref_id  OUT VARCHAR2) AS
  BEGIN
    --初始化处理日志的相关参数
    SELECT to_char(SYSDATE, 'YYYYMMDD') ||
           lpad(dams_file_ref_seq.nextval, 12, '0')
      INTO o_ref_id
      FROM dual;

    o_retcode := '0';
  EXCEPTION
    WHEN OTHERS THEN
      o_retcode := '-1';
  END proc_new_file_ref;

  /******************************************************************************
    NAME:      proc_file_download
    MODULE:    BLOB文件上传、下载
    PURPOSE:   下载BLOB文件
    REVISIONS:
    REMARK:
    Ver        Date        Author           Description
    ---------  ----------  ---------------  ------------------------------------
    1.0        2012-11-20  KFZX-ZHANGT02    新建
  ******************************************************************************/
  PROCEDURE proc_file_download(i_user_id   IN VARCHAR2,
                               i_file_id   IN VARCHAR2, --需要下载的文件id
                               o_retcode   OUT VARCHAR2,
                               o_file_name OUT VARCHAR2, --需要下载的文件名
                               o_file_data OUT BLOB) AS
    v_msg VARCHAR2(4000);
  BEGIN
    --初始化处理日志的相关参数

    SELECT t.file_name, t.file_content
      INTO o_file_name, o_file_data
      FROM dams_file t
     WHERE t.file_id = i_file_id;
    o_retcode := '0';

  EXCEPTION
    WHEN OTHERS THEN
      o_retcode := '-1';

  END proc_file_download;

  /******************************************************************************
    NAME:      proc_file_list
    MODULE:    BLOB文件上传、下载
    PURPOSE:   获取文件列表
    REVISIONS:
    REMARK:    i_valid_also："1"：获取所有文件（有效,非有效）,否则：删除掉无效文件,
               只取有效文件列表
    Ver        Date        Author           Description
    ---------  ----------  ---------------  ------------------------------------
    1.0        2012-11-20  KFZX-ZHANGT02    新建
    2.0        2015-11-09  kfzx-linlc
  ******************************************************************************/
  PROCEDURE proc_file_list(i_user_id      IN VARCHAR2,
                           i_ref_id       IN VARCHAR2, --文件引用编号
                           i_unvalid_also IN VARCHAR2, --是否读取无效数据
                           o_retcode      OUT VARCHAR2,
                           o_total_num    OUT VARCHAR2, --文件个数
                           o_file_detail  OUT ref_cur) AS

    v_msg VARCHAR2(4000);

  BEGIN
    --初始化处理日志的相关参数
    /*
    TODO: owner="kfzx-linlc" created="2015/11/9"
    text="是否获取无效文件（是否需要）"
    */

    SELECT COUNT(1)
      INTO o_total_num
      FROM dams_file_ref r, dams_file t
     WHERE r.file_id = t.file_id
       AND r.file_ref_id = i_ref_id;

    OPEN o_file_detail FOR
      SELECT t.file_id file_id, r.file_ref_id ref_id, t.file_name file_name
        FROM dams_file_ref r, dams_file t
       WHERE r.file_id = t.file_id
         AND r.file_ref_id = i_ref_id;

    o_retcode := '0';

  EXCEPTION
    WHEN OTHERS THEN
      o_retcode := '-1';

  END proc_file_list;
  /******************************************************************************
    NAME:      proc_file_delete
    MODULE:    BLOB文件上传、下载
    PURPOSE:   文件删除
    REVISIONS:
    REMARK:
    Ver        Date        Author           Description
    ---------  ----------  ---------------  ------------------------------------
    1.0        2012-11-20  KFZX-ZHANGT02    新建
    2.0        2015-11-09  KFZX-linlc
  ******************************************************************************/
  PROCEDURE proc_file_delete(i_user_id      IN VARCHAR2,
                             i_file_ref_id  IN VARCHAR2, --文件关联编号
                             i_file_ids     IN VARCHAR2, --需要删除的的文件id序列 例如1,2,3
                             i_is_clear_all IN VARCHAR2, -- 是否删除文件关联下的所有文件
                             o_retcode      OUT VARCHAR2) AS
    v_msg VARCHAR2(4000);
  BEGIN
    --初始化处理日志的相关参数

    -- 删除
    /*
    TODO: owner="kfzx-linlc" created="2015/11/9"
    text="（是否需要用valid）"
    */
    DELETE FROM dams_file_ref r
     WHERE r.file_ref_id = i_file_ref_id
       AND r.file_id IN
           (SELECT column_value FROM TABLE(func_split2(i_file_ids)));

    o_retcode := '0';

  EXCEPTION
    WHEN OTHERS THEN
      o_retcode := '-1';

  END proc_file_delete;

  /******************************************************************************
    NAME:      proc_file_updateflag
    MODULE:    BLOB文件上传、下载
    PURPOSE:   更新文件标识位
    REVISIONS:
    REMARK:    更新标识位 这里用来将新上传的文件更新为可用的
    Ver        Date        Author           Description
    ---------  ----------  ---------------  ------------------------------------
    1.0        2012-11-20  KFZX-ZHANGT02    新建
  ******************************************************************************/
  PROCEDURE proc_file_updateflag(i_user_id    IN VARCHAR2,
                                 i_file_ref   IN VARCHAR2,
                                 i_file_ids   IN VARCHAR2, --需要更新的文件id序列 例如1,2,3
                                 i_flag_value IN VARCHAR2, ---标识值
                                 o_retcode    OUT VARCHAR2) IS

    v_sqlstr VARCHAR2(4000);
    v_msg    VARCHAR2(4000);
  BEGIN
    /*
      --初始化处理日志的相关参数

      v_sqlstr := 'UPDATE ' || i_table_name || ' SET ' || i_flag_colname || ' = ' ||
                  i_flag_value || ' WHERE ' || i_fileid_colname || ' in (' ||
                  i_file_ids || ')';
      EXECUTE IMMEDIATE v_sqlstr;
    */
    o_retcode := '0';

  EXCEPTION
    WHEN OTHERS THEN
      o_retcode := '-1';
  END proc_file_updateflag;

  /******************************************************************************
    NAME:      proc_copy_with_ref
    MODULE:    BLOB文件上传、下载
    PURPOSE:   copy file
    REVISIONS:
    REMARK:
    Ver        Date        Author           Description
    ---------  ----------  ---------------  ------------------------------------
    1.0        2015-11-10  KFZX-LINLC    新建
  ******************************************************************************/
  PROCEDURE proc_copy_with_ref(i_user_id      IN VARCHAR2,
                               i_file_ref     IN VARCHAR2,
                               i_paste_ref_id IN VARCHAR2,
                               o_retcode      OUT VARCHAR2,
                               o_ref_id       OUT VARCHAR2) IS
    v_paste_ref_id dams_file_ref.file_ref_id%TYPE;
    new_ref_error EXCEPTION;
    v_sysdate DATE := SYSDATE;
  BEGIN

    IF i_paste_ref_id IS NULL THEN
      -- 生成ref_id
      proc_new_file_ref(i_user_id, o_retcode, v_paste_ref_id);
      IF o_retcode <> 0 THEN
        RAISE new_ref_error;
      END IF;
    ELSE
      v_paste_ref_id := i_paste_ref_id;
    END IF;

    -- 插入
    MERGE INTO dams_file_ref t
    USING (SELECT v_paste_ref_id file_ref_id, t.file_id
             FROM dams_file_ref t
            WHERE t.file_ref_id = i_file_ref) tmp
    ON (tmp.file_id = t.file_id AND tmp.file_ref_id = t.file_ref_id)
    WHEN NOT MATCHED THEN
      INSERT
        (file_ref_id, file_id, create_time, create_user)
      VALUES
        (tmp.file_ref_id, tmp.file_id, v_sysdate, i_user_id);
    o_retcode := '0';
    o_ref_id  := v_paste_ref_id;
  EXCEPTION
    WHEN OTHERS THEN
      o_retcode := '-1';
  END proc_copy_with_ref;

  /******************************************************************************
    NAME:      proc_copy_with_ref
    MODULE:    BLOB文件上传、下载
    PURPOSE:   copy file
    REVISIONS:
    REMARK:
    Ver        Date        Author           Description
    ---------  ----------  ---------------  ------------------------------------
    1.0        2015-11-10  KFZX-LINLC    新建
  ******************************************************************************/
  PROCEDURE proc_copy_with_file_id(i_user_id      IN VARCHAR2,
                                   i_file_ids     IN VARCHAR2,
                                   i_paste_ref_id IN VARCHAR2,
                                   o_retcode      OUT VARCHAR2,
                                   o_ref_id       OUT VARCHAR2) IS
    v_paste_ref_id dams_file_ref.file_ref_id%TYPE;
    new_ref_error EXCEPTION;
    v_sysdate DATE := SYSDATE;
  BEGIN
    IF i_paste_ref_id IS NULL THEN
      -- 生成ref_id
      proc_new_file_ref(i_user_id, o_retcode, v_paste_ref_id);
      IF o_retcode <> 0 THEN
        RAISE new_ref_error;
      END IF;
    ELSE
      v_paste_ref_id := i_paste_ref_id;
    END IF;

    -- 插入
    MERGE INTO dams_file_ref t
    USING (SELECT v_paste_ref_id file_ref_id, column_value file_id
             FROM TABLE(func_split2(i_file_ids))) tmp
    ON (tmp.file_id = t.file_id AND tmp.file_ref_id = t.file_ref_id)
    WHEN NOT MATCHED THEN
      INSERT
        (file_ref_id, file_id, create_time, create_user)
      VALUES
        (tmp.file_ref_id, tmp.file_id, v_sysdate, i_user_id);
    o_retcode := '0';
    o_ref_id  := v_paste_ref_id;
  EXCEPTION
    WHEN OTHERS THEN
      o_retcode := '-1';
  END proc_copy_with_file_id;

END pckg_dams_file_upload;
//




CREATE OR REPLACE PACKAGE pckg_su_vacation_job IS
  /******************************************************************************
      --存储过程名：    proc_vacation_upd_save
      --存储过程描述：  出差申请审批完成存入推送表
      --功能模块：      联机接口 
      --作者：          kfzx-renhq 
      --时间：          2019-07-29
      --修改历史:
  ******************************************************************************/
  PROCEDURE proc_vacation_upd_save(i_app_id     IN VARCHAR2, --日期
                                   i_valid_flag IN VARCHAR2, --是否有效(正常情况下为1,当撤销时为2)
                                   i_type       IN VARCHAR2, --是否是特殊数据
                                   o_retcode    OUT VARCHAR2 --返回值
                                   );

  /******************************************************************************
      --存储过程名：    proc_vacation_upd_chg
      --存储过程描述：  推送之后更新标志位
      --功能模块：      联机接口
      --作者：          kfzx-renhq
      --时间：          2019-07-29
      --修改历史:
  ******************************************************************************/
  PROCEDURE proc_vacation_upd_chg(i_app_id    IN VARCHAR2,
                                  i_send_flag IN VARCHAR2, --发送成功标识
                                  o_retcode   OUT VARCHAR2 --返回值
                                  );
  /******************************************************************************
      --存储过程名：    proc_vacation_upd_qry
      --存储过程描述：  查询推送列表
      --功能模块：      联机接口
      --作者：          kfzx-renhq
      --时间：          2019-07-29
      --修改历史:
  ******************************************************************************/
  PROCEDURE proc_vacation_upd_qry(o_retcode OUT VARCHAR2,
                                  o_list    OUT SYS_REFCURSOR);

  /******************************************************************************
      --方法名：    proc_get_vacation_upd_peer
      --方法描述：  查询同行人员列表
      --功能模块：      联机接口
      --作者：          kfzx-renhq
      --时间：          2019-07-29
      --修改历史:
  ******************************************************************************/
  PROCEDURE proc_get_vacation_upd_peer(i_ssid_ids IN VARCHAR2,
                                       i_app_id   IN VARCHAR2,
                                       o_retcode  OUT VARCHAR2,
                                       o_list     OUT SYS_REFCURSOR);
  /******************************************************************************
      --方法名：    proc_get_vacation_upd_dest
      --方法描述：  查询同行目的的列表
      --功能模块：      联机接口
      --作者：          kfzx-renhq
      --时间：          2019-07-29
      --修改历史:
  ******************************************************************************/
  PROCEDURE proc_get_vacation_upd_dest(i_app_id  IN VARCHAR2,
                                       o_retcode OUT VARCHAR2,
                                       o_list    OUT SYS_REFCURSOR);
  /******************************************************************************
      --方法名：    proc_get_vacation_upd_destmanal
      --方法描述：  查询同行目的的列表
      --功能模块：      联机接口
      --作者：          kfzx-renhq
      --时间：          2019-07-29
      --修改历史:
  ******************************************************************************/
  PROCEDURE proc_get_vacation_upd_dmanal(i_app_id  IN VARCHAR2,
                                         o_retcode OUT VARCHAR2,
                                         o_dest    OUT VARCHAR2);
  /******************************************************************************
      --方法名：    proc_get_switch
      --方法描述：  查询ois财务报销开关是否开启
      --功能模块：      联机接口
      --作者：          kfzx-renhq
      --时间：          2019-08-23
      --修改历史:
  ******************************************************************************/
  PROCEDURE proc_get_switch(o_retcode   OUT VARCHAR2,
                            o_sx_switch OUT VARCHAR2);

END pckg_su_vacation_job;

//

CREATE OR REPLACE PACKAGE BODY pckg_su_vacation_job IS
  /******************************************************************************
      --存储过程名：    proc_vacation_upd_save
      --存储过程描述：  出差申请审批完成存入推送表
      --功能模块：      联机接口
      --作者：          kfzx-renhq
      --时间：          2019-07-29
      --修改历史:
  ******************************************************************************/
  PROCEDURE proc_vacation_upd_save(i_app_id     IN VARCHAR2, --日期
                                   i_valid_flag IN VARCHAR2, --是否有效(正常情况下为1,当撤销时为2)
                                   i_type       IN VARCHAR2, --是否是特殊数据
                                   o_retcode    OUT VARCHAR2 --返回值
                                   ) IS
    v_param     db_log.info%TYPE := i_app_id || '|' || i_valid_flag || '|' ||
                                    i_type;
    v_proc_name db_log.proc_name%TYPE := 'pckg_su_vacation_job.proc_vacation_upd_save';
    v_seq       VARCHAR2(20);
    v_switch    VARCHAR2(1);
    v_type      VARCHAR2(1);
    --v_tmp_seq   VARCHAR2(20);
  BEGIN
    o_retcode := '0';
    v_type    := i_type;
    IF v_type IS NULL THEN
      v_type := '0';
    END IF;
    --在ois侧的财务报销开关关闭,存入数据以供推送到财务开支平台供报销
    pckg_su_vacation_job.proc_get_switch(o_retcode, v_switch);
  
    IF v_switch = '0' THEN
      pack_log.info(v_proc_name, pack_log.start_step,
                    pack_log.start_msg || v_param);
      v_seq := pckg_dams_util.func_gen_seq('', 'seq_su_vacation_upd', '10');
    
      INSERT INTO su_vacation_info_upd
        (id, app_id, upd_times, upd_flag, valid_flag, typ)
      VALUES
        (v_seq, i_app_id, 0, '0', i_valid_flag, v_type);
      pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_retcode := '-1';
      ROLLBACK;
      RETURN;
  END proc_vacation_upd_save;
  /******************************************************************************
      --存储过程名：    proc_vacation_upd_chg
      --存储过程描述：  推送之后更新标志位
      --功能模块：      联机接口
      --作者：          kfzx-renhq
      --时间：          2019-07-29
      --修改历史:
  ******************************************************************************/
  PROCEDURE proc_vacation_upd_chg(i_app_id    IN VARCHAR2,
                                  i_send_flag IN VARCHAR2, --推送标识  1 成功
                                  o_retcode   OUT VARCHAR2 --返回值
                                  ) IS
    v_param     db_log.info%TYPE := i_app_id;
    v_proc_name db_log.proc_name%TYPE := 'pckg_su_vacation_job.proc_vacation_upd_chg';
  BEGIN
    o_retcode := '0';
  
    UPDATE su_vacation_info_upd t
       SET t.upd_end_time = SYSDATE, t.upd_flag = i_send_flag
     WHERE t.id = i_app_id;
    -- 如果推送成功,将数据移至历史表
    IF i_send_flag = '1' THEN
      INSERT INTO su_vacation_info_upd_his
        (id,
         app_id,
         upd_times,
         upd_start_time,
         upd_end_time,
         upd_flag,
         valid_flag,
         typ)
        SELECT id,
               app_id,
               upd_times,
               upd_start_time,
               upd_end_time,
               upd_flag,
               valid_flag,
               typ
          FROM su_vacation_info_upd
         WHERE id = i_app_id;
      DELETE FROM su_vacation_info_upd WHERE id = i_app_id;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_retcode := '-1';
      ROLLBACK;
      RETURN;
  END proc_vacation_upd_chg;
  /******************************************************************************
      --存储过程名：    proc_vacation_upd_qry
      --存储过程描述：  查询推送列表
      --功能模块：      联机接口
      --作者：          kfzx-renhq
      --时间：          2019-07-29
      --修改历史:
  ******************************************************************************/
  PROCEDURE proc_vacation_upd_qry(o_retcode OUT VARCHAR2,
                                  o_list    OUT SYS_REFCURSOR) IS
    v_param     db_log.info%TYPE := '';
    v_proc_name db_log.proc_name%TYPE := 'pckg_su_vacation_job.proc_vacation_upd_qry';
  BEGIN
    o_retcode := '0';
    OPEN o_list FOR
      SELECT DISTINCT vi.app_id apply_no,
                      decode(ve.busi_type, '02', '03', '03', '04', ve.busi_type) apply_type,
                      '' apply_reason,
                      to_char(vi.apply_time, 'YYYYMMDD') apply_time,
                      vi.start_date start_date,
                      vi.end_date end_date,
                      vi.operator operator_user,
                      vi.operator_stru operator_stru,
                      vi.operator_stru operator_dept,
                      vi.remark apply_reasondesc,
                      decode(vi.start_peroid, '01', 1, '02', 2, vi.start_peroid) begin_time,
                      decode(vi.end_peroid, '01', 1, '02', 2, vi.end_peroid) end_time,
                      --decode(vi.valid_flag, '2', vi.complete_time, '') revocation_time,
                      '' update_user,
                      ve.parter peer,
                      vi.applyer main_apply_user,
                      u.name main_apply_name,
                      vi.applyer_stru main_apply_stru,
                      decode(vd.valid_flag, '1', '2', '2', '3', '2') approstatus,
                      '0' oa_rei_flag,
                      '' var1,
                      '' var2,
                      vd.id,
                      vd.valid_flag, -- 是否有效,当为 0 时表示需要删除数据
                      to_char(vi.complete_time, 'YYYY-MM-DD HH24:MM:SS'), --取消休假时的撤销时间
                      vd.typ --数据类型
        FROM su_vacation_info_upd vd,
             su_vacation_info     vi,
             su_vacation_info_det ve,
             su_vacation_location vl,
             dams_user            u
       WHERE vd.app_id = vi.app_id
         AND u.ssic_id = vi.applyer
         AND vl.app_id = vd.app_id
         AND ve.app_id = vd.app_id
            --AND vd.typ = '0' -- 正常数据
            --AND vd.valid_flag = '1'
         AND vd.upd_times < 3
         AND vd.upd_flag = '0';
    -- 置开始推送时间
    UPDATE su_vacation_info_upd t
       SET t.upd_start_time = SYSDATE
     WHERE t.upd_start_time IS NULL;
    --修改推送次数
    UPDATE su_vacation_info_upd t
       SET t.upd_times = t.upd_times + 1
     WHERE t.upd_times < 3
       AND t.upd_flag = '0';
    -- 设置推送三次都未成功的数据的推送结束时间,并修改推送状态
    UPDATE su_vacation_info_upd t
       SET t.upd_end_time = SYSDATE, t.upd_flag = '2'
     WHERE t.upd_flag = '0'
       AND t.upd_times > 2;
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_retcode := '-1';
      ROLLBACK;
      RETURN;
  END proc_vacation_upd_qry;
  /******************************************************************************
      --方法名：    proc_get_vacation_upd_peer
      --方法描述：  查询同行人员字列表
      --功能模块：      联机接口
      --作者：          kfzx-renhq
      --时间：          2019-07-29
      --修改历史:
  ******************************************************************************/
  PROCEDURE proc_get_vacation_upd_peer(i_ssid_ids IN VARCHAR2,
                                       i_app_id   IN VARCHAR2,
                                       o_retcode  OUT VARCHAR2,
                                       o_list     OUT SYS_REFCURSOR) IS
    v_proc_name db_log.proc_name%TYPE := 'pckg_su_vacation_job.proc_get_vacation_upd_peer';
    v_param     db_log.info%TYPE := i_ssid_ids || '|' || i_app_id;
    v_ssic_id   VARCHAR2(10); --主出差人统一认证编号
  BEGIN
    o_retcode := '0';
    SELECT applyer
      INTO v_ssic_id
      FROM su_vacation_info
     WHERE app_id = i_app_id;
    OPEN o_list FOR
      SELECT u.ssic_id peer_id, u.name peer_name, v.applyer_stru peer_fancode
        FROM dams_user u, su_vacation_info v
       WHERE u.ssic_id = v.applyer
         AND v.valid_flag = '1'
         AND v.orgapp_id = i_app_id
         AND TRIM(v.payment_state) IS NULL --null  未报销  1 已报销 2 报销中 3 无需报销
         AND u.ssic_id IN
             (SELECT column_value
                FROM TABLE(pckg_dams_util.func_str_to_array(i_ssid_ids, ',')))
         AND u.ssic_id <> v_ssic_id
       ORDER BY u.ssic_id;
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_retcode := '-1';
      ROLLBACK;
      RETURN;
  END proc_get_vacation_upd_peer;
  /******************************************************************************
      --方法名：    proc_get_vacation_upd_dest
      --方法描述：  查询同行目的的列表
      --功能模块：      联机接口
      --作者：          kfzx-renhq
      --时间：          2019-07-29
      --修改历史:
  ******************************************************************************/
  PROCEDURE proc_get_vacation_upd_dest(i_app_id  IN VARCHAR2,
                                       o_retcode OUT VARCHAR2,
                                       o_list    OUT SYS_REFCURSOR) IS
    v_proc_name db_log.proc_name%TYPE := 'pckg_su_vacation_job.proc_get_vacation_upd_dest';
    v_param     db_log.info%TYPE := i_app_id;
  BEGIN
    o_retcode := '0';
    OPEN o_list FOR
      SELECT decode(t1.area_id, '110000', '110100', '310000', '310100', '120000',
                    '120100', '500000', '500100', t1.area_id) station
        FROM su_vacation_location t, dams_payment_position t1
       WHERE t.end_location = t1.city_id
         AND t.app_id = i_app_id;
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_retcode := '-1';
      ROLLBACK;
      RETURN;
  END proc_get_vacation_upd_dest;
  /******************************************************************************
      --方法名：    proc_get_vacation_upd_destmanal
      --方法描述：  查询同行目的的列表
      --功能模块：      联机接口
      --作者：          kfzx-renhq
      --时间：          2019-07-29
      --修改历史:
  ******************************************************************************/
  PROCEDURE proc_get_vacation_upd_dmanal(i_app_id  IN VARCHAR2,
                                         o_retcode OUT VARCHAR2,
                                         o_dest    OUT VARCHAR2) IS
    v_proc_name db_log.proc_name%TYPE := 'pckg_su_vacation_job.proc_get_vacation_upd_dmanal';
    v_param     db_log.info%TYPE := i_app_id;
  BEGIN
    o_retcode := '0';
    SELECT l.end_location
      INTO o_dest
      FROM su_vacation_location l
     WHERE l.app_id = i_app_id;
    IF TRIM(o_dest) IS NULL THEN
      o_dest := '北京'; --手输目的地只做判空校验
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_retcode := '-1';
      ROLLBACK;
      RETURN;
  END proc_get_vacation_upd_dmanal;
  /******************************************************************************
      --方法名：    proc_get_switch
      --方法描述：  查询ois财务报销开关是否开启
      --功能模块：      联机接口
      --作者：          kfzx-renhq
      --时间：          2019-08-23
      --修改历史:
  ******************************************************************************/
  PROCEDURE proc_get_switch(o_retcode   OUT VARCHAR2,
                            o_sx_switch OUT VARCHAR2) IS
    v_proc_name db_log.proc_name%TYPE := 'pckg_su_vacation_job.proc_get_switch';
  
  BEGIN
    o_retcode := '0';
    /*SELECT t.dictcode
     INTO o_sx_switch
     FROM dams_gen_dict t
    WHERE t.dicttype = 'SX_PAY_STAT'
      AND t.sys_code = 'SX'
      AND t.dictlang = 'zh_CN';*/
    o_sx_switch := '0';
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！');
      o_retcode   := '-1';
      o_sx_switch := '0';
      ROLLBACK;
      RETURN;
  END proc_get_switch;

END pckg_su_vacation_job;
//




CREATE OR REPLACE PACKAGE pkg_ckeditor IS

  -- Author  : KFZX-HUANGXL
  -- Created : 2011-12-21 10:51
  -- Purpose : for ckeditor blob field usage

  TYPE ref_cursor IS REF CURSOR;

  PROCEDURE create_ckeditorblob(i_content_id   IN VARCHAR2,
                                i_content_type IN VARCHAR2,
                                i_content_blob IN BLOB,
                                i_file_name    IN VARCHAR2,
                                i_content_desc IN VARCHAR2,
                                i_reserved1    IN VARCHAR2,
                                i_reserved2    IN VARCHAR2,
                                o_flag         OUT VARCHAR2,
                                o_msg          OUT VARCHAR2,
                                o_blob_uid     OUT VARCHAR2);

  PROCEDURE update_ckeditorblob(i_blob_uid     IN VARCHAR2,
                                i_content_blob IN BLOB,
                                i_file_name    IN VARCHAR2,
                                i_content_desc IN VARCHAR2,
                                i_reserved1    IN VARCHAR2,
                                i_reserved2    IN VARCHAR2,
                                o_flag         OUT VARCHAR2,
                                o_msg          OUT VARCHAR2,
                                o_new_blob_uid OUT VARCHAR2);

  PROCEDURE delete_ckeditorblob(i_blob_uid IN VARCHAR2,
                                o_flag     OUT VARCHAR2,
                                o_msg      OUT VARCHAR2);

  PROCEDURE get_ckeditorblob(i_blob_uid         IN VARCHAR2,
                             o_flag             OUT VARCHAR2,
                             o_msg              OUT VARCHAR2,
                             o_content_blob     OUT BLOB,
                             o_content_filename OUT VARCHAR2);

  PROCEDURE update_ckeditor_rels(i_blob_uid_str IN VARCHAR2,
                                 i_content_id   IN VARCHAR2,
                                 i_content_type IN VARCHAR2,
                                 i_reserved1    IN VARCHAR2,
                                 i_reserved2    IN VARCHAR2,
                                 o_flag         OUT VARCHAR2,
                                 o_msg          OUT VARCHAR2);
END pkg_ckeditor;
//
CREATE OR REPLACE PACKAGE BODY pkg_ckeditor IS

  PROCEDURE create_ckeditorblob(i_content_id   IN VARCHAR2,
                                i_content_type IN VARCHAR2,
                                i_content_blob IN BLOB,
                                i_file_name    IN VARCHAR2,
                                i_content_desc IN VARCHAR2,
                                i_reserved1    IN VARCHAR2,
                                i_reserved2    IN VARCHAR2,
                                o_flag         OUT VARCHAR2,
                                o_msg          OUT VARCHAR2,
                                o_blob_uid     OUT VARCHAR2) IS
    v_proc_name db_log.proc_name%TYPE; -- 存储过程名
    v_step_no   db_log.step_no%TYPE; -- 步骤名
  BEGIN
    --置初始标志
    --==============================================================================================================
    --STEP1  日志变量赋值
    --==============================================================================================================
    v_step_no   := '1';
    v_proc_name := 'pkg_ckeditor.create_ckeditorblob';
    o_flag      := 1;

    SELECT seq_com_blob_uid.NEXTVAL INTO o_blob_uid FROM dual;

    v_step_no := '2';

    INSERT INTO com_blob_contents
      (blob_uid,
       content_id,
       content_type,
       content_blob,
       file_name,
       content_desc,
       content_date,
       reserved1,
       reserved2)
    VALUES
      (o_blob_uid,
       i_content_id,
       i_content_type,
       i_content_blob,
       i_file_name,
       i_content_desc,
       SYSDATE,
       i_reserved1,
       i_reserved2);

    v_step_no := '3';
    o_flag := '0';
    o_msg := v_proc_name || '模块处理完成!';
    COMMIT;
    pack_log.log(v_proc_name, v_step_no, o_msg, '2');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      o_msg := v_proc_name || '模块内部错误:' || o_msg || SQLERRM;
      pack_log.log(v_proc_name, v_step_no, o_msg, '4');
      RAISE;
  END;

  PROCEDURE update_ckeditorblob(i_blob_uid     IN VARCHAR2,
                                i_content_blob IN BLOB,
                                i_file_name    IN VARCHAR2,
                                i_content_desc IN VARCHAR2,
                                i_reserved1    IN VARCHAR2,
                                i_reserved2    IN VARCHAR2,
                                o_flag         OUT VARCHAR2,
                                o_msg          OUT VARCHAR2,
                                o_new_blob_uid OUT VARCHAR2) IS
    v_proc_name db_log.proc_name%TYPE; -- 存储过程名
    v_step_no   db_log.step_no%TYPE; -- 步骤名
  BEGIN
    --置初始标志
    --==============================================================================================================
    --STEP1  日志变量赋值
    --==============================================================================================================
    v_step_no   := '1';
    v_proc_name := 'pkg_ckeditor.update_ckeditorblob';
    o_flag      := 1;

    SELECT seq_com_blob_uid.NEXTVAL INTO o_new_blob_uid FROM dual;

    v_step_no := '2';

    INSERT INTO com_blob_contents
      (blob_uid,
       content_id,
       content_type,
       content_blob,
       file_name,
       content_desc,
       content_date,
       reserved1,
       reserved2)
      SELECT o_new_blob_uid,
             content_id,
             content_type,
             i_content_blob,
             i_file_name,
             i_content_desc,
             SYSDATE,
             i_reserved1,
             i_reserved2
        FROM com_blob_contents t
       WHERE t.blob_uid = i_blob_uid;

    v_step_no := '3';
    o_flag := '0';
    o_msg := v_proc_name || '模块处理完成!';
    COMMIT;
    pack_log.log(v_proc_name, v_step_no, o_msg, '2');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      o_msg := v_proc_name || '模块内部错误:' || o_msg || SQLERRM;
      pack_log.log(v_proc_name, v_step_no, o_msg, '4');
      RAISE;
  END;

  PROCEDURE delete_ckeditorblob(i_blob_uid IN VARCHAR2,
                                o_flag     OUT VARCHAR2,
                                o_msg      OUT VARCHAR2) IS
    v_proc_name db_log.proc_name%TYPE; -- 存储过程名
    v_step_no   db_log.step_no%TYPE; -- 步骤名
  BEGIN
    --置初始标志
    --==============================================================================================================
    --STEP1  日志变量赋值
    --==============================================================================================================
    v_step_no   := '1';
    v_proc_name := 'pkg_ckeditor.delete_ckeditorblob';
    o_flag      := 1;

    DELETE FROM com_blob_contents t WHERE t.blob_uid = i_blob_uid;

    v_step_no := '2';
    o_flag := '0';
    o_msg := v_proc_name || '模块处理完成!';
    COMMIT;
    pack_log.log(v_proc_name, v_step_no, o_msg, '2');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      o_msg := v_proc_name || '模块内部错误:' || o_msg || SQLERRM;
      pack_log.log(v_proc_name, v_step_no, o_msg, '4');
      RAISE;
  END;

  PROCEDURE get_ckeditorblob(i_blob_uid         IN VARCHAR2,
                             o_flag             OUT VARCHAR2,
                             o_msg              OUT VARCHAR2,
                             o_content_blob     OUT BLOB,
                             o_content_filename OUT VARCHAR2) IS
    v_proc_name db_log.proc_name%TYPE; -- 存储过程名
  BEGIN
    --置初始标志
    --==============================================================================================================
    --STEP1  日志变量赋值
    --==============================================================================================================
    v_proc_name := 'pkg_ckeditor.get_ckeditorblob';
    o_flag      := 1;

    SELECT t.content_blob, t.file_name
      INTO o_content_blob, o_content_filename
      FROM com_blob_contents t
     WHERE t.blob_uid = i_blob_uid;
    o_flag := '0';
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      o_msg := v_proc_name || '模块内部错误:' || o_msg || SQLERRM;
      pack_log.log(v_proc_name, '1', o_msg, '4');
      RAISE;
  END;

  PROCEDURE update_ckeditor_rels(i_blob_uid_str IN VARCHAR2,
                                 i_content_id   IN VARCHAR2,
                                 i_content_type IN VARCHAR2,
                                 i_reserved1    IN VARCHAR2,
                                 i_reserved2    IN VARCHAR2,
                                 o_flag         OUT VARCHAR2,
                                 o_msg          OUT VARCHAR2) IS
    i_blob_uid_array TP_ARRAY_TABLE;
    v_proc_name db_log.proc_name%TYPE; -- 存储过程名
    v_step_no   db_log.step_no%TYPE; -- 步骤名
  BEGIN
    --置初始标志
    --==============================================================================================================
    --STEP1  日志变量赋值
    --==============================================================================================================
    v_step_no   := '1';
    v_proc_name := 'pkg_ckeditor.update_ckeditor_rels';
    o_flag      := 1;

    IF i_blob_uid_str IS NOT NULL THEN
      i_blob_uid_array := func_split(i_blob_uid_str,'$|$');
      FOR i IN 1 .. i_blob_uid_array.COUNT LOOP
        v_step_no := v_step_no || to_char(i);
        UPDATE com_blob_contents
           SET content_id   = i_content_id,
               content_type = i_content_type,
               content_date = SYSDATE,
               reserved1    = i_reserved1,
               reserved2    = i_reserved2
         WHERE blob_uid = i_blob_uid_array(i);
      END LOOP;
    END IF;

    v_step_no := '2';
    o_flag := '0';
    o_msg := v_proc_name || '模块处理完成!';
    COMMIT;
    pack_log.log(v_proc_name, v_step_no, o_msg, '2');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      o_msg := v_proc_name || '模块内部错误:' || o_msg ||
               '参数i_blob_uid_str=' || i_blob_uid_str ||
               '参数i_content_id=' || i_content_id ||
               '参数i_content_type=' || i_content_type ||
               '参数i_reserved1=' || i_reserved1 ||
               '参数i_reserved2=' || i_reserved2 ||
               SQLERRM;
      pack_log.log(v_proc_name, v_step_no, o_msg, '4');
      RAISE;
  END;
END pkg_ckeditor;
//




CREATE OR REPLACE PACKAGE pckg_ois_group IS
 
  PROCEDURE proc_add_group(i_ssic_id     IN VARCHAR2,
                           i_func        IN VARCHAR2, --功能类型
                           i_group_name  IN VARCHAR2, --群组名称
                           i_group_value IN VARCHAR2, --群组值
                           o_ret_code    OUT VARCHAR2,
                           o_msg         OUT VARCHAR2);
  
  PROCEDURE proc_delete_group(i_group_id IN VARCHAR2,
                              o_ret_code OUT VARCHAR2,
                              o_msg      OUT VARCHAR2);
  
  PROCEDURE proc_qry_group(i_ssic_id  IN VARCHAR2,
                           i_func     IN VARCHAR2,
                           o_ret_code OUT VARCHAR2,
                           o_msg      OUT VARCHAR2,
                           o_list     OUT SYS_REFCURSOR);
END pckg_ois_group;
//
CREATE OR REPLACE PACKAGE BODY pckg_ois_group IS
  /******************************************************************************
      --存储过程名：    proc_add_group
      --存储过程描述：  群组新增
      --功能模块：      群组模块
      --作者：          kfzx-jiangke
      --时间：          20151030
      --修改历史:
  ******************************************************************************/
  PROCEDURE proc_add_group(i_ssic_id     IN VARCHAR2,
                           i_func        IN VARCHAR2, --功能类型
                           i_group_name  IN VARCHAR2, --群组名称
                           i_group_value IN VARCHAR2, --群组值
                           o_ret_code    OUT VARCHAR2,
                           o_msg         OUT VARCHAR2) IS
    v_param     db_log.info%TYPE := i_ssic_id || '|' || i_func || '|' ||
                                    i_group_name || '|' || i_group_value;
    v_group_id  VARCHAR2(20);
    v_count     NUMBER;
    v_proc_name db_log.proc_name%TYPE := 'pckg_ois_group.proc_add_group';
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step,
                  pack_log.start_msg || v_param);
    o_ret_code := 0;

    SELECT COUNT(1)
      INTO v_count
      FROM ois_group t
     WHERE t.group_name = i_group_name
       AND t.ssic_id = i_ssic_id
       AND t.func = i_func;

    IF v_count > 0 THEN
      SELECT t.group_id
        INTO v_group_id
        FROM ois_group t
       WHERE t.group_name = i_group_name
         AND t.ssic_id = i_ssic_id
         AND t.func = i_func;

      DELETE FROM ois_group_content t WHERE t.group_id = v_group_id;
      INSERT INTO ois_group_content t
        SELECT v_group_id, column_value, rownum
          FROM TABLE(pckg_dams_util.func_str_to_array(i_group_value, ','));
    ELSE
      v_group_id := pckg_dams_util.func_gen_seq('GP', 'GROUP_SEQ');
      INSERT INTO ois_group
      VALUES
        (v_group_id, i_group_name, i_ssic_id, i_func, i_ssic_id, SYSDATE);
      INSERT INTO ois_group_content t
        SELECT v_group_id, column_value, rownum
          FROM TABLE(pckg_dams_util.func_str_to_array(i_group_value, ','));
    END IF;

  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_ret_code := '-1';
  END proc_add_group;

  /******************************************************************************
      --存储过程名：    proc_delete_group
      --存储过程描述：  群组删除
      --功能模块：      群组模块
      --作者：          kfzx-jiangke
      --时间：          20151030
      --修改历史:
  ******************************************************************************/
  PROCEDURE proc_delete_group(i_group_id IN VARCHAR2,
                              o_ret_code OUT VARCHAR2,
                              o_msg      OUT VARCHAR2) IS
    v_param     db_log.info%TYPE := i_group_id;
    v_group_id  VARCHAR2(20);
    v_count     NUMBER;
    v_proc_name db_log.proc_name%TYPE := 'pckg_ois_group.proc_delete_group';
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step,
                  pack_log.start_msg || v_param);
    o_ret_code := 0;

    DELETE FROM ois_group t WHERE t.group_id = i_group_id;
    DELETE FROM ois_group_content t WHERE t.group_id = i_group_id;

  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_ret_code := '-1';
  END proc_delete_group;

  /******************************************************************************
      --存储过程名：    proc_qry_group
      --存储过程描述：  群组查询
      --功能模块：      群组模块
      --作者：          kfzx-jiangke
      --时间：          20151030
      --修改历史:
  ******************************************************************************/
  PROCEDURE proc_qry_group(i_ssic_id  IN VARCHAR2,
                           i_func     IN VARCHAR2,
                           o_ret_code OUT VARCHAR2,
                           o_msg      OUT VARCHAR2,
                           o_list     OUT SYS_REFCURSOR) IS
    v_param     db_log.info%TYPE := i_ssic_id || '|' || i_func;
    v_group_id  VARCHAR2(20);
    v_count     NUMBER;
    v_proc_name db_log.proc_name%TYPE := 'pckg_ois_group.proc_qry_group';
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step,
                  pack_log.start_msg || v_param);
    o_ret_code := 0;

    OPEN o_list FOR
      SELECT t.group_name title,
             t.group_id key,
             listagg(t1.group_content, ',') within GROUP(ORDER BY t1.sort) VALUE
        FROM ois_group t, ois_group_content t1
       WHERE t.group_id = t1.group_id
         AND t.ssic_id = i_ssic_id
         AND t.func = i_func
       GROUP BY t.group_id, t.group_name;

  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_ret_code := '-1';
  END proc_qry_group;

END pckg_ois_group;
//



CREATE OR REPLACE PACKAGE pckg_dams_system IS

  -- Author  : kfzx-gecc1
  -- Created :
  -- Purpose :

  TYPE t_item_list IS RECORD(
    sys_code      dams_sys_conf.sys_code%TYPE, --系统编号
    sys_name      dams_sys_conf.sys_name%TYPE, --系统名称
    sys_sname     dams_sys_conf.sys_sname%TYPE, --系统简称
    sys_group     dams_sys_conf.sys_group%TYPE, --8?
    ssic_sys_code dams_sys_conf.ssic_sys_code%TYPE); --统一认证系统编码

  TYPE ref_t_item_list IS REF CURSOR RETURN t_item_list; --定义子系统列表游标

  PROCEDURE proc_dams_system(i_sys_code      IN dams_sys_conf.sys_code%TYPE, --系统编号
                             i_sys_name      IN dams_sys_conf.sys_name%TYPE, --系统名称
                             i_sys_sname     IN dams_sys_conf.sys_sname%TYPE, --系统简称
                             i_sys_group     IN dams_sys_conf.sys_group%TYPE, --8?
                             i_ssic_sys_code IN dams_sys_conf.ssic_sys_code%TYPE, --统一认证系统编码
                             i_begnum        IN VARCHAR2, --开始取数?
                             i_fetchnum      IN VARCHAR2, --获取数？
                             o_retcode       OUT VARCHAR2, --代表成功失败
                             o_totalnum      OUT VARCHAR2, --最终获取数
                             o_item_list     OUT ref_t_item_list);

  PROCEDURE proc_dams_system_detail(i_sys_code           IN dams_sys_conf.sys_code%TYPE, --系统编号
                                    o_retcode            OUT VARCHAR2,
                                    o_sys_code           OUT dams_sys_conf.sys_code%TYPE,
                                    o_sys_name           OUT dams_sys_conf.sys_name%TYPE, --所属系统名称
                                    o_sys_sname          OUT dams_sys_conf.sys_sname%TYPE, --所属系统简称
                                    o_sys_desc           OUT dams_sys_conf.sys_desc%TYPE, --所属系统描述
                                    o_sys_server_ip      OUT dams_sys_conf.sys_server_ip%TYPE, --服务器IP地址
                                    o_sys_server_ctxpath OUT dams_sys_conf.sys_server_ctxpath%TYPE, --上下文路径
                                    o_sys_mainpage_url   OUT dams_sys_conf.sys_mainpage_url%TYPE, --首页url
                                    o_sys_group          OUT dams_sys_conf.sys_group%TYPE, --所属栏目
                                    o_ssic_sys_code      OUT dams_sys_conf.ssic_sys_code%TYPE, --统一认证系统编码
                                    o_sysorder           OUT NUMBER);

  PROCEDURE proc_dams_system_renew(i_sys_code  IN dams_sys_conf.sys_code%TYPE, --系统编号,更新的主键
                                   i_sys_name  IN dams_sys_conf.sys_name%TYPE, --系统名称,UPDATE的数据
                                   i_sys_sname IN dams_sys_conf.sys_sname%TYPE, --系统简称,UPDATE的数据
                                   i_sys_group IN dams_sys_conf.sys_group%TYPE, --栏目,UPDATE的数据
                                   i_sys_desc  IN dams_sys_conf.sys_desc%TYPE, --描述,UPDATE的数据
                                   i_sysorder  IN NUMBER,
                                   i_op_user   IN VARCHAR2, --操作人
                                   o_retcode   OUT VARCHAR2);

  PROCEDURE proc_orderflag(i_sys_code IN VARCHAR2,
                           i_sysorder IN VARCHAR2,
                           o_retcode  OUT VARCHAR2,
                           o_flag     OUT VARCHAR2); --0:不重复；1：重复
END pckg_dams_system;
//

CREATE OR REPLACE PACKAGE BODY pckg_dams_system IS
  PROCEDURE proc_dams_system(i_sys_code      IN dams_sys_conf.sys_code%TYPE, --系统编号
                             i_sys_name      IN dams_sys_conf.sys_name%TYPE, --系统名称
                             i_sys_sname     IN dams_sys_conf.sys_sname%TYPE, --系统简称
                             i_sys_group     IN dams_sys_conf.sys_group%TYPE, --8?
                             i_ssic_sys_code IN dams_sys_conf.ssic_sys_code%TYPE, --统一认证系统编码
                             i_begnum        IN VARCHAR2, --开始取数?
                             i_fetchnum      IN VARCHAR2, --获取数？
                             o_retcode       OUT VARCHAR2, --代表成功失败
                             o_totalnum      OUT VARCHAR2, --最终获取数
                             o_item_list     OUT ref_t_item_list) IS

    v_param     db_log.info%TYPE := i_begnum || '|' || i_fetchnum || '|' ||
                                    i_sys_code || '|' || i_sys_name || '|' ||
                                    i_sys_sname || '|' || i_sys_group || '|' ||
                                    i_ssic_sys_code;
    v_proc_name db_log.proc_name%TYPE := 'DAMS_PKG_SYSTEM.PROC_DAMS_SYSTEM';
    --V_STEP      DB_LOG.STEP_NO%TYPE;

  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step,
                  pack_log.start_msg || v_param);
    o_retcode := 0;
    --V_STEP    := 1;
    SELECT COUNT(1)
      INTO o_totalnum
      FROM dams_sys_conf t
     WHERE t.sys_code LIKE
           decode(i_sys_code, NULL, t.sys_code, '%' || i_sys_code || '%')
       AND t.sys_name LIKE
           decode(i_sys_name, NULL, t.sys_name, '%' || i_sys_name || '%')
       AND t.sys_sname LIKE
           decode(i_sys_sname, NULL, t.sys_sname, '%' || i_sys_sname || '%')
       AND (nvl(t.sys_group, 1)) LIKE
           decode(i_sys_group, NULL, (nvl(t.sys_group, 1)),
                  '%' || i_sys_group || '%')
       AND (nvl(t.ssic_sys_code, 1) LIKE
           decode(i_ssic_sys_code, NULL, nvl(t.ssic_sys_code, 1),
                   '%' || i_ssic_sys_code || '%'));
    --V_STEP := 2;
    IF i_begnum IS NULL THEN
      OPEN o_item_list FOR
        SELECT sys_code, sys_name, sys_sname, sys_group, sys_order
          FROM dams_sys_conf t
         WHERE t.sys_code LIKE
               decode(i_sys_code, NULL, t.sys_code, '%' || i_sys_code || '%')
           AND t.sys_name LIKE
               decode(i_sys_name, NULL, t.sys_name, '%' || i_sys_name || '%')
           AND t.sys_sname LIKE
               decode(i_sys_sname, NULL, t.sys_sname, '%' || i_sys_sname || '%')
           AND (nvl(t.sys_group, 1)) LIKE
               decode(i_sys_group, NULL, (nvl(t.sys_group, 1)),
                      '%' || i_sys_group || '%')
           AND (nvl(t.ssic_sys_code, 1) LIKE
               decode(i_ssic_sys_code, NULL, nvl(t.ssic_sys_code, 1),
                       '%' || i_ssic_sys_code || '%'))
         ORDER BY sys_order;
    ELSE
      OPEN o_item_list FOR
        SELECT sys_code, sys_name, sys_sname, sys_group, sys_order
          FROM (SELECT sys_code,
                       sys_name,
                       sys_sname,
                       sys_group,
                       sys_order,
                       row_number() over(ORDER BY sys_code) rn
                  FROM dams_sys_conf t
                 WHERE t.sys_code LIKE
                       decode(i_sys_code, NULL, t.sys_code,
                              '%' || i_sys_code || '%')
                   AND t.sys_name LIKE
                       decode(i_sys_name, NULL, t.sys_name,
                              '%' || i_sys_name || '%')
                   AND t.sys_sname LIKE
                       decode(i_sys_sname, NULL, t.sys_sname,
                              '%' || i_sys_sname || '%')
                   AND (nvl(t.sys_group, 1)) LIKE
                       decode(i_sys_group, NULL, (nvl(t.sys_group, 1)),
                              '%' || i_sys_group || '%')
                   AND (nvl(t.ssic_sys_code, 1) LIKE
                       decode(i_ssic_sys_code, NULL, nvl(t.ssic_sys_code, 1),
                               '%' || i_ssic_sys_code || '%'))
                 ORDER BY sys_order)
         WHERE rn >= to_number(i_begnum)
           AND rn < to_number(i_begnum) + to_number(i_fetchnum);
    END IF;
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
    RETURN;
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      IF o_item_list%ISOPEN THEN
        CLOSE o_item_list;
      END IF;
      o_retcode := -1;
      RETURN;
  END;

  PROCEDURE proc_dams_system_detail(i_sys_code           IN dams_sys_conf.sys_code%TYPE, --系统编号
                                    o_retcode            OUT VARCHAR2,
                                    o_sys_code           OUT dams_sys_conf.sys_code%TYPE,
                                    o_sys_name           OUT dams_sys_conf.sys_name%TYPE, --所属系统名称
                                    o_sys_sname          OUT dams_sys_conf.sys_sname%TYPE, --所属系统简称
                                    o_sys_desc           OUT dams_sys_conf.sys_desc%TYPE, --所属系统描述
                                    o_sys_server_ip      OUT dams_sys_conf.sys_server_ip%TYPE, --服务器IP地址
                                    o_sys_server_ctxpath OUT dams_sys_conf.sys_server_ctxpath%TYPE, --上下文路径
                                    o_sys_mainpage_url   OUT dams_sys_conf.sys_mainpage_url%TYPE, --首页url
                                    o_sys_group          OUT dams_sys_conf.sys_group%TYPE, --所属栏目
                                    o_ssic_sys_code      OUT dams_sys_conf.ssic_sys_code%TYPE, --统一认证系统编码
                                    o_sysorder           OUT NUMBER) IS

    v_param     db_log.info%TYPE := i_sys_code;
    v_proc_name db_log.proc_name%TYPE := 'DAMS_PKG_SYSTEM.PROC_DAMS_SYSTEM_DETAIL';
    --V_STEP      DB_LOG.STEP_NO%TYPE;
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step,
                  pack_log.start_msg || v_param);
    o_retcode := 0;
    SELECT sys_code,
           sys_name,
           sys_sname,
           sys_desc,
           sys_server_ip,
           sys_server_ctxpath,
           sys_mainpage_url,
           sys_group,
           ssic_sys_code,
           sys_order
      INTO o_sys_code,
           o_sys_name,
           o_sys_sname,
           o_sys_desc,
           o_sys_server_ip,
           o_sys_server_ctxpath,
           o_sys_mainpage_url,
           o_sys_group,
           o_ssic_sys_code,
           o_sysorder
      FROM dams_sys_conf
     WHERE sys_code = i_sys_code;
    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_retcode := -1;
  END;
  PROCEDURE proc_dams_system_renew(i_sys_code  IN dams_sys_conf.sys_code%TYPE, --系统编号,更新的主键
                                   i_sys_name  IN dams_sys_conf.sys_name%TYPE, --系统名称,UPDATE的数据
                                   i_sys_sname IN dams_sys_conf.sys_sname%TYPE, --系统简称,UPDATE的数据
                                   i_sys_group IN dams_sys_conf.sys_group%TYPE, --栏目,UPDATE的数据
                                   i_sys_desc  IN dams_sys_conf.sys_desc%TYPE, --描述,UPDATE的数据
                                   i_sysorder  IN NUMBER,
                                   i_op_user   IN VARCHAR2, --操作人
                                   o_retcode   OUT VARCHAR2) IS
    v_param     db_log.info%TYPE := i_sys_code || '|' || i_sys_name || '|' ||
                                    i_sys_sname || '|' || i_sys_group || '|' ||
                                    i_sys_desc || '|' || i_sysorder;
    v_proc_name db_log.proc_name%TYPE := 'DAMS_PKG_SYSTEM.PROC_RENEW';
    v_menuid    dams_menu_nls.menu_id%TYPE; --我的工作中子系统id
    v_menuid1   dams_menu_nls.menu_id%TYPE; --业务申请中的id
    --V_STEP      DB_LOG.STEP_NO%TYPE;

    v_logobj  dams_gen_dict.dictcode%TYPE := 'SYSTEM_MNGT';
    v_logtype VARCHAR2(3) := '015';
    v_logdate VARCHAR2(14);

    v_loginfo  VARCHAR2(4000) := '';
    v_oldvalue VARCHAR2(4000);
    v_logtemp  VARCHAR2(4000) := '';
    v_logdet   CLOB;

    v_count       NUMBER(5);
    old_sys_code  VARCHAR2(200) := '';
    old_sys_name  VARCHAR2(200) := '';
    old_sys_sname VARCHAR2(200) := '';
    old_sys_group VARCHAR2(200) := '';
    old_sys_desc  VARCHAR2(200) := '';
    old_sysorder  VARCHAR2(200) := '';
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step,
                  pack_log.start_msg || v_param);
    o_retcode := 0;
    v_menuid  := '1';
    v_menuid1 := '1';

    SELECT to_char(SYSDATE, 'yyyymmddhh24miss') INTO v_logdate FROM dual;

    SELECT COUNT(1)
      INTO v_count
      FROM dams_sys_conf
     WHERE sys_code = i_sys_code;

    IF v_count = 1 THEN
      SELECT sys_code, sys_name, sys_sname, sys_group, sys_desc, sys_order
        INTO old_sys_code,
             old_sys_name,
             old_sys_sname,
             old_sys_group,
             old_sys_desc,
             old_sysorder
        FROM dams_sys_conf
       WHERE sys_code = i_sys_code;

      v_loginfo := '修改子系统:' || old_sys_name || '(' || i_sys_code || ')';
      v_logtemp := '所属系统名称:' || i_sys_name || '|' || old_sys_name || ',所属系统简称:' ||
                   i_sys_sname || '|' || old_sys_sname || ',所属栏目:' ||
                   i_sys_group || '|' || old_sys_group || ',所属系统描述:' ||
                   i_sys_desc || '|' || old_sys_desc || ',顺序:' || i_sysorder || '|' ||
                   old_sysorder || '$修改';
    END IF;

    --更新子系统表
    UPDATE dams_sys_conf
       SET sys_name  = i_sys_name,
           sys_sname = i_sys_sname,
           sys_group = i_sys_group,
           sys_desc  = i_sys_desc,
           sys_order = i_sysorder
     WHERE sys_code = i_sys_code;

    --更新菜单次序
    UPDATE dams_menu t
       SET t.sortno = i_sysorder
     WHERE t.menu_id IN (SELECT s.menu_id
                           FROM dams_sub_sys_menu_sort s
                          WHERE s.sys_code = i_sys_code);
    --更新菜单名称
    UPDATE dams_menu_nls t
       SET t.menu_name     = i_sys_name,
           t.default_label = i_sys_name,
           t.menu_desc     = i_sys_name
     WHERE t.menu_id IN (SELECT s.menu_id
                           FROM dams_sub_sys_menu_sort s
                          WHERE s.sys_code = i_sys_code)
       AND t.locale = 'zh_CN';

    IF v_loginfo IS NOT NULL THEN
      v_logdet := pckg_dams_log_viewer.func_str2json(v_logtemp);
      pckg_dams_util.proc_dams_logger(v_logobj, v_logtype, v_logdate, i_op_user,
                                      v_loginfo, v_logdet);
    END IF;

    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_retcode := -1;
  END;
 
  PROCEDURE proc_orderflag(i_sys_code IN VARCHAR2,
                           i_sysorder IN VARCHAR2,
                           o_retcode  OUT VARCHAR2,
                           o_flag     OUT VARCHAR2) IS
    --0:不重复；1：重复
    v_param     db_log.info%TYPE := i_sys_code;
    v_proc_name db_log.proc_name%TYPE := 'pcke_dams_system.proc_orderflag';
    v_sysorder  dams_sys_conf.sys_order%TYPE;
    v_tmp       NUMBER(10);
    --V_STEP      DB_LOG.STEP_NO%TYPE;
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step,
                  pack_log.start_msg || v_param);
    o_retcode := 0;

    SELECT t.sys_order
      INTO v_sysorder
      FROM dams_sys_conf t
     WHERE t.sys_code = i_sys_code;

    IF v_sysorder = i_sysorder THEN
      o_flag := '0';
    END IF;

    IF v_sysorder <> i_sysorder THEN
      SELECT COUNT(1)
        INTO v_tmp
        FROM dams_sys_conf t
       WHERE t.sys_order = i_sysorder;

      IF v_tmp = 0 THEN
        o_flag := '0';
      ELSE
        o_flag := '1';
      END IF;
    END IF;

    pack_log.info(v_proc_name, pack_log.end_step, pack_log.end_msg || v_param);
  EXCEPTION
    WHEN OTHERS THEN
      pack_log.error(v_proc_name, pack_log.end_step, '异常！' || v_param);
      o_retcode := -1;
  END;
END pckg_dams_system;
//

 
 CREATE OR REPLACE PACKAGE pckg_dams_inner_todo IS



  PROCEDURE proc_insert_inner_todo(i_ssic_id        IN VARCHAR2,
                                   i_next_ssic_ids  IN VARCHAR2,
                                   i_business_id    IN VARCHAR2,
                                   i_task_id        IN VARCHAR2,
                                   i_prev_task_name IN VARCHAR2,
                                   i_sys_code       IN VARCHAR2,
                                   i_biz_type       IN VARCHAR2,
                                   i_url            IN VARCHAR2,
                                   i_title          IN VARCHAR2,
                                   i_send_mail      IN VARCHAR2,
                                   i_in_sys_seq     IN VARCHAR2,
                                   o_ret_code       OUT VARCHAR2,
                                   o_msg            OUT VARCHAR2);
END pckg_dams_inner_todo;
//
CREATE OR REPLACE PACKAGE BODY pckg_dams_inner_todo IS

  PROCEDURE proc_insert_inner_todo(i_ssic_id        IN VARCHAR2,
                                   i_next_ssic_ids  IN VARCHAR2,
                                   i_business_id    IN VARCHAR2,
                                   i_task_id        IN VARCHAR2,
                                   i_prev_task_name IN VARCHAR2,
                                   i_sys_code       IN VARCHAR2,
                                   i_biz_type       IN VARCHAR2,
                                   i_url            IN VARCHAR2,
                                   i_title          IN VARCHAR2,
                                   i_send_mail      IN VARCHAR2,
                                   i_in_sys_seq     IN VARCHAR2,
                                   o_ret_code       OUT VARCHAR2,
                                   o_msg            OUT VARCHAR2) IS
    v_param     db_log.info%TYPE := i_ssic_id;
    v_proc_name db_log.proc_name%TYPE := 'pckg_dams_inner_todo.proc_insert_inner_todo';
    v_count     NUMBER(4);
    v_ret_code  VARCHAR2(10);
  BEGIN
    pack_log.info(v_proc_name, pack_log.start_step,
                  pack_log.start_msg || v_param);
    o_ret_code := '0';

    -- 插入待办
    MERGE INTO dams_inner_todo_task t
    USING (SELECT column_value ssic_id, i_business_id business_id
             FROM TABLE(func_split2(i_next_ssic_ids))) d
    ON (t.business_id = d.business_id AND t.ssic_id = d.ssic_id)
    WHEN MATCHED THEN
      UPDATE
         SET t.create_time  = SYSDATE,
             t.prev_ssic_id = i_ssic_id,
             t.take_time    = ''
    WHEN NOT MATCHED THEN
      INSERT
        (task_id,
         ssic_id,
         prev_task_name,
         prev_ssic_id,
         create_time,
         take_time,
         end_time,
         task_state,
         business_id,
         business_type,
         title,
         creator_id,
         sys_code,
         url,
         in_sys_seq)
      VALUES
        (i_task_id,
         d.ssic_id,
         i_prev_task_name,
         i_ssic_id,
         SYSDATE,
         '',
         '',
         'open',
         d.business_id,
         i_biz_type,
         i_title,
         i_ssic_id,
         i_sys_code,
         i_url,
         i_in_sys_seq);

    IF i_send_mail = '1' THEN
      -- 需要发送文件
      pckg_dams_mail.proc_insert_inner_todo_mail(i_ssic_id, i_next_ssic_ids,
                                                 i_business_id, i_task_id,
                                                 i_sys_code, i_title, v_ret_code,
                                                 o_msg);
    END IF;

  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      pack_log.error(v_proc_name, pack_log.END_STEP, '异常！' || v_param);
      o_ret_code := '-1';
  END proc_insert_inner_todo;

END pckg_dams_inner_todo;
//
delimiter ;//