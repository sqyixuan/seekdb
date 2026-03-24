


delimiter $;

CREATE OR REPLACE FUNCTION "AF_GET_TOKEN" (v_source IN OUT varchar2, v_split IN varchar2) RETURN varchar2
IS
    v_ret   varchar2(1000);
    v_b     number;
    v_pos   number;
BEGIN
    v_b := length(v_split);
    v_ret := '';
    v_pos := instr(v_source,v_split,1,1);
    if v_pos > 0 then
        v_ret := substr(v_source,1,v_pos-1);
        v_source := substr(v_source,v_pos+v_b);
    else
        v_ret := substr(v_source,1);
        v_source := '';
    end if;
    return v_ret;
END ;


$

 CREATE OR REPLACE FUNCTION "AF_GET_TARIFF_SN" return varchar2
is
   v_sn   varchar2(20);
begin
    select  to_char(to_date('20201103010101','yyyymmddhh24miss'),'yymmddhh24miss') || lpad(SEQ_TARIFF_SN.NEXTVAL,8,'0') into v_sn from dual;
    return v_sn;
end;


$




CREATE OR REPLACE PROCEDURE "EXECUTE_IMMEDIATE" ( p_sql_text VARCHAR2 ) IS

   COMPILATION_ERROR EXCEPTION;
   PRAGMA EXCEPTION_INIT(COMPILATION_ERROR,-24344);

   l_cursor INTEGER DEFAULT 0;
   rc       INTEGER DEFAULT 0;
   stmt     VARCHAR2(1000);

BEGIN

   l_cursor := DBMS_SQL.OPEN_CURSOR;
   DBMS_SQL.PARSE(l_cursor, p_sql_text, DBMS_SQL.NATIVE);
   rc := DBMS_SQL.EXECUTE(l_cursor);
   DBMS_SQL.CLOSE_CURSOR(l_cursor);
   EXCEPTION WHEN COMPILATION_ERROR THEN DBMS_SQL.CLOSE_CURSOR(l_cursor);
       WHEN OTHERS THEN
          BEGIN
              DBMS_SQL.CLOSE_CURSOR(l_cursor);
              raise_application_error(-20101,sqlerrm || '  when executing ''' || p_sql_text || '''   ');
          END;
END;

$

  CREATE OR REPLACE PACKAGE "BILL_TEMPLATE" is

  -- Author  : LKF15094
  -- Created : 2009-12-29 8:52:47
  -- Purpose :

   function getKeyFieldName(a_template_id number) return varchar2;
   function getIDfromInfoTable(a_instid number, a_template_id number,
            a_tablename varchar2, a_source_id varchar2) return varchar2;
   procedure getInstID(MaxORFree in number,InstID out number);--1: max 2 :free
   procedure Instantiation(a_template_type in varchar2,a_param in varchar2,a_instid in number ,a_template_id in number,a_instCnt out number);
   procedure move_tariff_plan(a_instid in number,a_template_id in number);
   procedure move_tariff_plan_item(a_new_plan_id in number,a_instid in number,a_template_id in number);
   procedure move_tariff_schema(a_instid in number,a_template_id in number);
   procedure move_tariff_schema_item(a_new_schema_id in number,a_old_schema_id in number,a_instid in number,a_template_id in number);
   procedure generateid(maxorfree in number,whoseid in varchar2,newid out varchar2);
   procedure update_param_value(a_instid in number ,a_template_id in number);
   procedure justify_data_type(a_table_name in varchar2 ,a_column_name in varchar2,a_lquotation out varchar2,a_rquotation out varchar2);
   procedure clearInstTable(a_instid in number);
   procedure ret_inst_recmsg(a_instid in number ,a_template_id in number ,a_start in number, a_end in number, a_retMsg out varchar2);
   procedure export_insert_sql(a_instid in number ,a_template_id in number ,a_source_rule_table_name in varchar2,a_dest_rule_id in varchar2,a_ret_sql out varchar2);
end BILL_TEMPLATE;


$

  CREATE OR REPLACE PACKAGE "BP" 
as
	procedure p_generate_billingcycle(n_cycle_schema_id	in	billingcycle_schema.cycle_schema_id%type);
	procedure p_generate_all_billingcycle;
end;

$

  CREATE OR REPLACE PACKAGE "CDRFILELOGCHECK" is
procedure checkfilelog;
end;
  

$

  CREATE OR REPLACE PACKAGE "PKG_CDRBALANCE_CHECK" is

  -- Author  : W39883
  -- Created : 2006-4-30 17:34:23
  -- Purpose : 话单平衡性检查相关包
  --按文件名统计
  procedure PROC_BYNAME(file_name varchar2);
  --判断文件名是否匹配指定业务类型串
  function  FILENAME_MATCH(file_name varchar2, service_code varchar2) return number;
  --对平衡检查结果二次处理并显示
  procedure AUDIT_SHOW(file_name varchar2, sect_str varchar2,begindate number,
  						enddate number,service_code varchar2);
  --按日期统计
  procedure PROC_BYDATE(proc_date varchar2);
end PKG_CDRBALANCE_CHECK;



$

  CREATE OR REPLACE PACKAGE "PKG_CHECK_PARAM" is

  -- Author  : J39853
  -- Created : 2007-5-22 14:52:30
  -- Purpose :

  -- Public type declarations
  --type <TypeName> is <Datatype>;

  -- Public constant declarations
  --<ConstantName> constant <Datatype> := <Value>;

  -- Public variable declarations
  --<VariableName> <Datatype>;
  PROCEDURE ap_check_param(a_result out varchar2);

  --检查同一个class对应一个实体的多个属性的情况
  function af_check_entity_class return number ;

  --检查累计量属性在赠送量定义中有配置
  function af_check_cust_entity_attr return number;

  --检查赠送量属性在累计量定义中有配置

  --对计费事件的属性进行检查
  function af_check_chargeevent_continue return number;

  --写数据到表中
  function af_insert_data_tab(a_serialno number, a_msg varchar2) return number;


  -- Public function and procedure declarations
  --function <FunctionName>(<Parameter> <Datatype>) return <Datatype>;

end pkg_check_param;


$

  CREATE OR REPLACE PACKAGE "PKG_DATAFILE" is

  -- Author  : created by w39883
  -- Created : 2006-1-6 19:28:12
  -- Purpose : 局数据相关处理包
procedure DATAPROC(filetype number,v_result in out varchar2);
procedure DATA2DB(filetype number,applyDate varchar2,v_result in out varchar2);

procedure PROC_MAPPING_LIST_SP(v_map_id number, v_result in out varchar2);
procedure DB_MAPPING_LIST_SP(v_map_id number, v_result  in out  varchar2);

procedure PROC_MAPPING_LIST_OPER(v_map_id number, v_result in out varchar2);
procedure DB_MAPPING_LIST_OPER(v_map_id number, v_result  in out  varchar2);

procedure PROC_MAPPING_LIST_SNSP(v_map_id number, v_result  in out  varchar2);
procedure DB_MAPPING_LIST_SNSP(v_map_id number, v_result  in out  varchar2);


procedure PROC_HIGHCOSTEXCHANGERATE(v_result  in out  varchar2);
procedure DB_HIGHCOSTEXCHANGERATE(v_result  in out  varchar2);

end PKG_DATAFILE;

$


  CREATE OR REPLACE PACKAGE "PKG_GET_SPLIT_REGION_LIST" is

  -- Author  : Z00197892
  -- Created : 2017/7/31 16:51:36
  -- Purpose :

  TYPE REGION_TYPE IS TABLE OF VARCHAR2(32) INDEX BY VARCHAR2(32);
  V_REGION_LIST REGION_TYPE;

  -- Public function and procedure declarations
  function get(v_param_name in VARCHAR2, v_region in VARCHAR2)
    return REGION_TYPE;

end pkg_get_split_region_list;


$


  CREATE OR REPLACE PACKAGE "PKG_MASKCHECK" IS
--echo ##type ty_str_split是function maskcheck中找到并置于此处
type ty_str_split IS TABLE OF VARCHAR2 (64);
 ---十进制转化为二进制
 function d2b(n in number) return varchar2;

 ---某个字符串是否为数字字符串
 function isNumber(p in varchar2) return number;

 ---将分隔符分隔的字符串拆分
 function fn_split (p_str IN VARCHAR2, p_delimiter IN VARCHAR2) RETURN ty_str_split PIPELINED;

 ---判断登录IP是否在IP_RANGE_LIST中定义
 function maskcheck(a_ipmask in varchar2, a_checkip in varchar2 := null) return varchar2;

end pkg_MaskCheck;


$


  CREATE OR REPLACE PACKAGE "PKG_RERATING" AS
  /*******************************************************************************
  作    者:裘学欣39824
  创建时间:2008-06-16  CCBS CHG V300R001C01B07
  修改时间:
  --------------------------------------------------------------------------------
  主要功能:SRS.FUNC.006.001 话单回收控制改造
  *******************************************************************************/
  g_Result_Path VARCHAR2(128); --日志文件路径
  g_Operator    VARCHAR2(32); --操作员

  /*******************************************************************************
  Function:       p_Distask(a_Taskid NUMBER，a_RowCount out NUMBER)
  Description:    回收子任务分配
  Input:          a_Taskid 回收任务编号
  Output:         a_RowCount 分配的用户个数
  Return:         N/A
  *******************************************************************************/
  PROCEDURE p_Distask(a_Taskid   NUMBER,
                      a_Rowcount OUT NUMBER);

  /*******************************************************************************
  Function:       p_Genparam(a_Taskid NUMBER， a_Param out Varchar2)
  Description:    根据回收任务和回收类型生成回收参数
  Input:          a_Taskid回收任务编号
  Output:         a_Param 回收参数
  Return:         N/A
  *******************************************************************************/
  PROCEDURE p_Genparam(a_Taskid NUMBER,
                       a_Param  OUT VARCHAR2);

  /*******************************************************************************
  Function:       p_Genwhere(a_Taskid NUMBER，a_Childid NUMBER，
                  a_Nodeid NUMBER, a_Where out Varchar2)
  Description:    根据回收任务、回收子任务、回收用户生成回收条件
  Input:          a_Taskid 回收任务编号，a_Childid 回收子任务编号，a_Nodeid 回收节点
  Output:         a_Where 回收条件
  Return:         N/A
  *******************************************************************************/
  PROCEDURE p_Genwhere(a_Taskid  NUMBER,
                       a_Childid NUMBER,
                       a_Nodeid  NUMBER,
                       a_Where   IN OUT VARCHAR2);

  /*******************************************************************************
  Function:       f_Get_Nodeid(a_Region NUMBER)
  Description:    取计费节点号函数
  Input:          a_Region 地区
  Output:         N/A
  Return:        节点号
  *******************************************************************************/
  FUNCTION f_Get_Nodeid(a_Region NUMBER) RETURN NUMBER;

  /*******************************************************************************
  Function:       f_Get_Tablename(a_Taskid NUMBER)
  Description:    取回收话单表函数
  Input:          a_Taskid 回收任务编号
  Output:         N/A
  Return:        回收话单表名
  *******************************************************************************/
  FUNCTION f_Get_Tablename(a_Taskid NUMBER) RETURN VARCHAR2;

  /*******************************************************************************
  Function:       f_Get_Operator()
  Description:    取操作员函数
  Input:          N/A
  Output:         N/A
  Return:        操作员
  *******************************************************************************/
  FUNCTION f_Get_Operator RETURN VARCHAR2;

  /*******************************************************************************
  Function:       p_Set_Operator(Operator VARCHAR2)
  Description:    设置操作员函数
  Input:          操作员
  Output:         N/A
  Return:         N/A
  *******************************************************************************/
  PROCEDURE p_Set_Operator(a_Operator VARCHAR2);

END;


$

  CREATE OR REPLACE PACKAGE "PKG_ROAM" IS

  -- Author  : 徐国栋
  -- Created : 2008-5-30 15:19:40
  -- Purpose :  为了新的五项考核指标

  -- Public type declarations
  PROCEDURE af_judge(v_date IN DATE);
  PROCEDURE ap_gencdr(s_seq VARCHAR2 DEFAULT NULL);
END pkg_roam;

$





  CREATE OR REPLACE PACKAGE "PKG_TEMPALTE_DEF" is
PROCEDURE AP_TempalteDataImport(p_Template_ID IN NUMBER, --模板编号
                                            p_Tempalte_Type IN varchar2,
                                            p_Rule_ID   IN VARCHAR2,
                                            n_Success out number,
                                            s_OutInfo out varchar2);

PROCEDURE AP_CreateTemplateInfoID(n_Template_id in out number);
PROCEDURE AP_CreateTemplateCatalogId(n_tree_node_id IN OUT NUMBER);

PROCEDURE AP_TariffPlan(p_TariffPlanId IN NUMBER, --资费政策编号
                                          p_TemplateId   IN NUMBER);
PROCEDURE AP_TariffSchema(p_TariffSchemaId IN NUMBER, --资费模式编号
                                            p_TemplateId     IN NUMBER);
PROCEDURE AP_TariffExport(p_TemplateId IN NUMBER, --模板编号
                                            p_TariffType IN NUMBER DEFAULT 1, --资费类型（1：资费政策；2：资费政策项；3：资费模式；4：资费模式项）
                                            p_TariffId   IN VARCHAR);
end PKG_TEMPALTE_DEF;


$

  CREATE OR REPLACE PACKAGE "PKG_VALIDUSER" AS
  Logonid       NUMBER(10) := 0; --2006-12-15 V2R2C05B30 39824
  Userid        VARCHAR2(100) := '_SYS_';
  Istriggerneed NUMBER := 1;
  Usertype      NUMBER := 0; --V3R1C01B01 39824 2007-3-19
  ClientInfo    varchar2(1024) := NULL;
  LogonSeq      VARCHAR2(32) := '0';

  PROCEDURE Setuser(v_User          IN VARCHAR2,
                    v_Istriggerneed NUMBER := 1,
                    v_clientinfo in varchar2 := '');
  PROCEDURE Defaultuser(v_Istriggerneed NUMBER := 1);
  PROCEDURE Setusertype(v_Usertype NUMBER := 0); --V3R1C01B01 39824 2007-3-19
  PROCEDURE SetLogonid(v_Logonid NUMBER := 0); --V3R1C12L07n11 sKF42558 2012-07-10
  --V3R1C01B01 39885 2007-03-15
  SUBTYPE Auditlogtype IS Audit_Log%ROWTYPE;
  FUNCTION Beginauditlog(a_Auditlog IN OUT Auditlogtype) RETURN NUMBER;
  PROCEDURE Writeauditlog(a_Auditlog  IN Auditlogtype,
                          a_Fieldname VARCHAR2,
                          a_Oldvalue  VARCHAR2,
                          a_Newvalue  VARCHAR2);
  PROCEDURE Writeauditlog(a_Auditlog  IN Auditlogtype,
                          a_Fieldname VARCHAR2,
                          a_Oldvalue  DATE,
                          a_Newvalue  DATE);
  PROCEDURE Endauditlog(a_Auditlog IN OUT Auditlogtype);
  --V3R1C01B01 39885 2007-03-15
  FUNCTION  GetUserid(a_userid in varchar2) return varchar2;
  PROCEDURE GetLogonSeq(a_LogonSeq OUT VARCHAR2);
END Pkg_Validuser;


$

  CREATE OR REPLACE PACKAGE "PKG_WORKORDER" is

  -- Author  : Y46156
  -- Created : 2008-9-4 11:30:58
  -- Purpose :

  TARIFF_PLAN_ID_MAX            number(6) := 999999;         --资费政策最大值
  TARIFF_SCHEMA_ID_MAX          number(9) := 999999999;      --资费模式最大值

  TARIFF_PLAN_ID_START          number(6) := 1000;           --资费政策起始序号，生成资费政策编号时，从改值开始查询，
                                                             --找到目前系统中未用的资费政策编号作为新的资费政策编号
  TARIFF_SCHEMA_ID_START        number(9) := 1000;           --资费模式起始序号

  procedure ap_workorder_to_tariff(
        a_workorder_id      in varchar2,    --work order id
        a_region            in number,      --region
        a_result            out number,     -- 1 - success, -1 - failed
        a_msg               out varchar2    --result message
    );

    --generate tariff plan item
    procedure ap_gen_tariff_plan_item(
        a_workorder_id          in  varchar2,   --work order id
        a_region                in  number,     --region
        a_tariff_plan_id        in  number,     --tariff plan id
        a_new_tariff_plan_id    in  number,     --new tariff plan id
        a_result                out number,     --1 - success, -1 - failure
        a_msg out varchar2                      --result message
    );

    --generate tariff schema
    procedure ap_gen_tariff_schema(
        a_workorder_id          in  varchar2,   --work order id
        a_region                in  number,     --region
        a_tariff_plan_id        in  number,     --tariff plan id
        a_tariff_schema_sn      in  number,     --tariff schema serial number
        a_charging_event_id     in  number,     --charging event id
        a_tariff_schema_id      in  number,     --tariff schema id
        a_result                out number,     --1-success, -1-failure
        a_msg                   out varchar2    --result message
    );

    --generate tariff schema item
    procedure ap_gen_tariff_schema_item(
        a_workorder_id          in  varchar2,   --work order id
        a_region                in  number,     --region
        a_tariff_plan_id        in  number,     --tariff plan id
        a_tariff_schema_sn      in  number,     --tariff schema serial number
        a_charging_event_id     in  number,     --charging event id
        a_tariff_schema_id      in  number,     --tariff schema id
        a_result                out number,     --1- success, -1- failure
        a_msg                   out varchar2   --result message
    );

    --generate ucd tariff plan
    procedure ap_gen_tariff_plan_ucd(
        a_workorder_id          in  varchar2, --work order id
        a_tariff_plan_id        in  number,   --tariff plan id
        a_new_tariff_plan_id    in  number,   --new tariff plan id
        a_source_type           in  number,   --0 from table of tariff_plan
                                              --1 from table of tariff_plan_ucd
        a_result                out number,   --1-success, -1-failurre
        a_msg                   out varchar2  --result message
    );

    procedure ap_gen_tariff_plan_item_ucd(
        a_workorder_id          in  varchar2, --work order id
        a_tariff_plan_id        in  number,   --tariff plan id
        a_new_tariff_plan_id    in  number,   --new tariff plan id
        a_tariff_plan_item_id   in  number,   --tariff paln item;
        a_result                out number,   --1-success, -1-failurre
        a_msg                   out varchar2  --result message
    );

    --generate ucd tariff schema
    procedure ap_gen_tariff_schema_ucd(
        a_workorder_id          in  varchar2,   --work order id
        a_tariff_plan_id        in  number,     --tariff plan id
        a_new_tariff_plan_id    in  number,     --new tariff plan id
        a_tariff_schema_sn      in  number,     --tariff schema serial number
        a_tariff_schema_id      in  number,     --tariff schema id
        a_attr_class            out number,     --attr class
        a_result                out number,     --1-success, -1-failurre
        a_msg out varchar2                      --result message
    );

    --copy ucd tariff plan item
    procedure ap_cp_tariff_plan_item_ucd(
        a_workorder_id          in  varchar2, --work order id
        a_new_workorder_id      in  varchar2, --new work order id
        a_tariff_plan_id        in  number,   --tariff plan id
        a_new_tariff_plan_id    in  number,   --new tariff plan id
        a_tariff_schema_sn      in  number,   --tariff schema serial number;
        a_result                out number,   --1-success, -1-failurre
        a_msg                   out varchar2  --result message
    );

    --copy ucd tariff schema
    procedure ap_cp_tariff_schema_ucd(
        a_workorder_id          in  varchar2,   --old work order id
        a_new_workorder_id      in  varchar2,   --new work order id
        a_tariff_plan_id        in  number,     --old tariff plan id
        a_new_tariff_plan_id    in  number,     --new tariff plan id
        a_tariff_schema_sn      in  number,     --old tariff schema serial number
        a_new_tariff_schema_sn  in  number,     --new tariff schema serial number
        a_result                out number,     --1-success, -1-failurre
        a_msg out varchar2                      --result message
    );

    --check work order and tariff plan
    procedure ap_check_workorder(
        a_workorder_id          in  varchar2, --work order id
        a_tariff_plan_id        in  number,   --tariff plan id
        a_result                out number,   --1-success, -1-failurre
        a_msg                   out varchar2  --result message
    );

    procedure ap_get_workorder_id(
        a_region_id             in  number,    --region id
        a_workorder_id          out varchar2   -- work order id
    );

    --delete work order and related info
    procedure ap_del_workorder(
        a_workorder_id          in varchar2,   -- work order id
        a_result                out number     --1-success, -1-failurre
    );

    --delete tariff plan
    procedure ap_del_tariff_plan(
        a_workorder_id          in varchar2,   -- work order id
        a_plan_id               in number,     -- tariff plan id
        a_result                out number     --1-success, -1-failurre
    );

    --delete ucd tariff plan item
    procedure ap_del_plan_item_ucd(
        a_workorder_id          in varchar2,   -- work order id
        a_tariff_plan_id        in number,     -- tariff plan id
        a_tariff_schema_sn      in number,     -- tariff schema serial number
        a_result                out number     --1-success, -1-failurre
    );

    --del ucd tariff schema
    procedure ap_del_tariff_schema_ucd(
        a_workorder_id          in  varchar2,   --work order id
        a_tariff_plan_id        in  number,     --tariff plan id
        a_tariff_schema_sn      in  number,     --tariff schema serial number
        a_result                out number     --1-success, -1-failurre
    );

    --add process log
    procedure ap_add_process_log(
        a_workorder_id          in varchar2,   -- work order id
        a_operator              in varchar2,   -- operator
        a_status                in number,     -- work order status
        a_reason                in varchar2,   -- reject reason
        a_result                out number     --1-success, -1-failurre
    );
    --judge tariff plan id
    procedure ap_judge_plan_id(
        a_rowid                 in varchar2,   -- rowid
        a_plan_id               in number,     -- tariff plan id
        a_result                out number     --1-success, -1-failurre
    );

     --update tarriff plan id
    procedure ap_update_plan_id(
        a_workorder_id          in varchar2,   -- work order id
        a_old_plan_id           in number,     -- old tariff plan id
        a_new_plan_id           in number,     -- new tariff plan id
        a_result                out number     --1-success, -1-failurre
    );

    --获取资费政策编号
    procedure ap_get_plan_id(
        a_plan_id               out number,    -- tariff plan id
        a_result                out number,    --1-success, -1-failurre
        a_msg                   out varchar2  --result message
    );

    --获取资费模式编号
    procedure ap_get_schema_id(
        a_schema_id             out number,    -- tariff schema id
        a_result                out number,    --1-success, -1-failurre
        a_msg                   out varchar2   --result message
    );

     --save ucd tariff schema
   procedure ap_save_tariff_schema_ucd(
        a_workorder_id	        in  varchar2,       --工单ID
        a_tariff_plan_id   	    in  number,         --资费政策ID
        a_tariff_schema_sn	    in  number,         --资费模式序号
        a_tariff_name 	        in  varchar2,       --资费模式名称
        a_tariff_type     	    in  number,         --资费模式类型
        a_is_combination	    in  number,         --是否组合资费模式
        a_field_def       	    in  varchar2,       --参数字段定义
        a_fieldcount      	    in  number,         --参数字段定义数
        a_match_order     	    in  number,         --参数匹配次序
        a_match_type      	    in  number,         --匹配类型
        a_apply_method    	    in  number,         --应用时间判断方法
        a_refid           	    in  number,         --资费标记位置
        a_ref_offset      	    in  number,         --资费标记偏移
        a_discount_fee_id 	    in  number,         --优惠费用
        a_g_field_def     	    in  varchar2,       --中文参数定义
        a_round_method    	    in  number,         --取整方法
        a_round_scale     	    in  number,         --取整精度
        a_billcode_order	    in  number,         --账目编码取值顺序
        a_rowid                 out varchar2,       --返回rowid
        a_result                out number,         --1-成功, -1-失败
        a_msg                   out varchar2        --结果信息
    );

     --save ucd tariff item
     procedure ap_save_tariff_item(
        a_workorder_id	        in    varchar2,		    --工单ID
        a_tariff_plan_id   	    in    number,     	    --资费政策ID
        a_tariff_schema_sn	    in    number,	    	--资费模式序号
        a_applytime	            in    date,         	--生效时间
        a_expiretime	        in    date,         	--失效时间
        a_tariff_criteria 	    in    varchar2,		    --资费定义条件
        a_subtariff_type  	    in    number,    		--下级资费类型
        a_subtariff_schema_sn	in    number,		    --下级资费模式序号
        a_precedence      	    in    number,    		--优先级
        a_g_criteria      	    in    varchar2,		    --资费定义条件描述
        a_node_name	            in    varchar2,		    --节点名称
        a_ratio	                in    number,		    --比率
        a_attr_class	        in    number,     	    --计价属性
        a_rowid                 out   varchar2,         --返回rowid
        a_result                out   number,           --1-成功, -1-失败
        a_msg                   out   varchar2          --结果信息
    );

end PKG_WORKORDER;


$

  CREATE OR REPLACE PACKAGE "ROAMPROC" 
is

	function toAlarmLevel( nFee number,sRoamType varchar2,sImsi varchar2 ) return varchar2;
	pragma restrict_references(toAlarmLevel,WNDS,WNPS);

	procedure stat_highcost( sRoamType varchar2, sReportDay varchar2, sReportNum varchar2 );

	procedure newdaytask( sNetType varchar2 );

	function	getHighRate(imsi varchar2) return number;
	pragma 		restrict_references(getHighRate,WNDS,WNPS);

  --获取高额汇率
	function GetSdrRate( sRoamType varchar2 ) return number;

  --获取高额门限值
  function	getThreshold(imsi varchar2) return number;

end;



$

  CREATE OR REPLACE PACKAGE "TU" IS

  -- Author  : Junqiang Luo
  -- Created : 2005-07-07 17:08:08 2005-07-07 17:08:08
  -- Purpose : tariff utilities

  -- Public type declarations
  --type <TypeName> is <Datatype>;

  -- Public constant declarations
  --<ConstantName> constant <Datatype> := <Value>;

  -- Public variable declarations
  --<VariableName> <Datatype>;

  -- Public function and procedure declarations
  --function <FunctionName>(<Parameter> <Datatype>) return <Datatype>;

  PROCEDURE add_default_plan_item(a_plan_id NUMBER);
  PROCEDURE copy_remind_limit_para(from_plan_id NUMBER, to_plan_id NUMBER);
  PROCEDURE copy_plan_items(a_from_plan_id NUMBER, a_to_plan_id NUMBER);
  PROCEDURE copy_schema(a_from_schema_id NUMBER, a_to_schema_id NUMBER);
  PROCEDURE copy_usage_attr(a_from_entityid NUMBER,a_from_attr NUMBER, a_to_attr NUMBER);
  PROCEDURE ap_copy_custinfo_def(a_from_entityid NUMBER,a_from_attr NUMBER, a_count NUMBER);
  PROCEDURE copy_month_priprivilege(a_from_plan_id NUMBER,
                                    a_to_plan_id   NUMBER);
  PROCEDURE copy_schema2(a_from_schema_id NUMBER, a_to_schema_id NUMBER);

  FUNCTION new_cdrtype(cdrtype VARCHAR2) RETURN VARCHAR2;
  FUNCTION new_calltype(callflag VARCHAR2) RETURN VARCHAR2;
  FUNCTION get_ratecode(a_rate NUMBER, a_unit NUMBER) RETURN NUMBER;
  FUNCTION correct_date(a_date DATE) RETURN DATE;

  PROCEDURE deep_copy_tariff_plan_vpn(a_from_plan NUMBER, a_to_plan NUMBER);
  FUNCTION af_getstr(str VARCHAR2, v_split VARCHAR2, pos NUMBER)
    RETURN VARCHAR2;
  PROCEDURE ap_do_inter(v_imsi      IN VARCHAR2,
                        v_roamfee   IN NUMBER, --0 不需要维护 国际漫游费用，单位元  如果是有主被叫，是主叫的漫游费用
                        v_troamfee  IN NUMBER, --0 不需要维护 国际漫游费用，单位元  如果是有主被叫，是被叫的漫游费用
                        v_lfee      IN NUMBER, -- 0 不需要维护 拨打非本国长途 单位元计费单元是分钟的
                        v_lfee2     IN NUMBER, -- 0 不需要维护，如果是0 单位元，计费单元是分钟的
                        v_smsfee    IN NUMBER, -- 不需要维护,如果是0，单位是元，短信费用
                        v_gprsfee   IN NUMBER, -- 不需要维护，如果是0，单位是分，GPRS费用.
                        v_applytime IN DATE);
  /*拷贝资费政策*/
  PROCEDURE ap_copy_tariff_plan(from_plan_id NUMBER, to_plan_id NUMBER);
  /*拷贝账单分拆定义*/
  PROCEDURE ap_copy_bill_split(from_plan_id NUMBER, to_plan_id NUMBER);
  PROCEDURE ap_do_base(v_tariff_plan_id IN NUMBER,
                       v_attr_id        IN NUMBER,
                       v_freevolum      IN NUMBER);
  /*拷贝帐务优惠*/
  PROCEDURE ap_copy_account_priv(from_plan_id NUMBER, to_plan_id NUMBER);
  PROCEDURE ap_gen_billcycle(v_event_id IN NUMBER);
END tu;

$

  CREATE OR REPLACE PACKAGE "TU_BAK" IS

  -- Author  : Junqiang Luo
  -- Created : 2005-07-07 17:08:08 2005-07-07 17:08:08 
  -- Purpose : tariff utilities

  -- Public type declarations
  --type <TypeName> is <Datatype>;

  -- Public constant declarations
  --<ConstantName> constant <Datatype> := <Value>;

  -- Public variable declarations
  --<VariableName> <Datatype>;

  -- Public function and procedure declarations
  --function <FunctionName>(<Parameter> <Datatype>) return <Datatype>;

  PROCEDURE add_default_plan_item(a_plan_id NUMBER);
  PROCEDURE copy_plan_items(a_from_plan_id NUMBER, a_to_plan_id NUMBER);
  PROCEDURE copy_schema(a_from_schema_id NUMBER, a_to_schema_id NUMBER);
  PROCEDURE copy_usage_attr(a_from_attr NUMBER, a_to_attr NUMBER);
  PROCEDURE copy_month_priprivilege(a_from_plan_id NUMBER,
                                    a_to_plan_id   NUMBER);
  PROCEDURE copy_schema2(a_from_schema_id NUMBER, a_to_schema_id NUMBER);

  FUNCTION new_cdrtype(cdrtype VARCHAR2) RETURN VARCHAR2;
  FUNCTION new_calltype(callflag VARCHAR2) RETURN VARCHAR2;
  FUNCTION get_ratecode(a_rate NUMBER, a_unit NUMBER) RETURN NUMBER;
  FUNCTION correct_date(a_date DATE) RETURN DATE;

  PROCEDURE deep_copy_tariff_plan_vpn(a_from_plan NUMBER, a_to_plan NUMBER);
  FUNCTION af_getstr(str VARCHAR2, v_split VARCHAR2, pos NUMBER)
    RETURN VARCHAR2;
  PROCEDURE ap_do_inter(v_imsi      IN VARCHAR2,
                        v_roamfee   IN NUMBER, --0 不需要维护 国际漫游费用，单位元  如果是有主被叫，是主叫的漫游费用
                        v_troamfee  IN NUMBER, --0 不需要维护 国际漫游费用，单位元  如果是有主被叫，是被叫的漫游费用
                        v_lfee      IN NUMBER, -- 0 不需要维护 拨打非本国长途 单位元计费单元是分钟的
                        v_lfee2     IN NUMBER, -- 0 不需要维护，如果是0 单位元，计费单元是分钟的
                        v_smsfee    IN NUMBER, -- 不需要维护,如果是0，单位是元，短信费用 
                        v_gprsfee   IN NUMBER, -- 不需要维护，如果是0，单位是分，GPRS费用.
                        v_applytime IN DATE);
  /*拷贝资费政策*/
  PROCEDURE ap_copy_tariff_plan(from_plan_id NUMBER, to_plan_id NUMBER);
  PROCEDURE ap_do_base(v_tariff_plan_id IN NUMBER,
                       v_attr_id        IN NUMBER,
                       v_freevolum      IN NUMBER);
  /*拷贝帐务优惠*/
  PROCEDURE ap_copy_account_priv(from_plan_id NUMBER, to_plan_id NUMBER);
  PROCEDURE ap_gen_billcycle(v_event_id IN NUMBER);
END tu_bak;

$

  CREATE OR REPLACE PACKAGE "UTIL" is

    procedure print_clob(a_script clob);

    --分析计费事件字符串
    procedure parse_event_str(a_str varchar2, a_trim boolean);

    --生成参数表触发器
    function get_audit_trigger_script(a_table_name varchar2) return clob;

    --生成并创建参数表触发器
    procedure create_audit_trigger(a_table_name varchar2, a_execute boolean);

    function get_event_script(a_event_id number, a_table_name varchar2 := 'event') return clob;

    type PlanUsedTariffRec is record
    (
    plan_id number(8),
    tariff_type varchar2(20),
    tariff_id number(8)
    );
    type PlanUsedTariffSet is table of PlanUsedTariffRec;
    --function get_tariff_plan_used_tariffs(a_planlist sys_refcursor) return PlanUsedTariffSet pipelined;

    type PlanUsageAttrRec is record
    (
    plan_id number(8),
    usage_type number(1),
    attr_id number(6)
    );
    type PlanUsageAttrSet is table of PlanUsageAttrRec;
    --function get_tariff_plan_usage_attr(a_planlist sys_refcursor) return PlanUsageAttrSet pipelined;

    type BillingCheckRec is record
    (
    event_id number(3),
    attr_id varchar2(4)
    );
    type BillingCheckSet is table of BillingCheckRec;
    function get_billingchecks(a_billingdef sys_refcursor) return BillingCheckSet pipelined;

end util;

$



  CREATE OR REPLACE PACKAGE BODY "BILL_TEMPLATE" is
  TYPE REF_CURSOR IS REF CURSOR;
  TestException Exception;
  FormatError Exception;
  function getKeyFieldName(a_template_id number) return varchar2
  is
    key_field_name varchar2(100);
    tmp_type varchar2(100);
  begin
      select TEMPLATE_RULE_DEF_TYPE into  tmp_type from template_info where template_id=a_template_id;
      case lower(tmp_type)
      when 'tariff_plan' then
          key_field_name:='tariff_plan_id';
		  dbms_output.put_line('1');
      when 'tariff_plan_item' then
          key_field_name:='tariff_plan_item_sn';
		  dbms_output.put_line('2');
      when 'tariff_schema' then
          key_field_name:='tariff_schema_id';
		  dbms_output.put_line('3');
      when 'tariff_item' then
          key_field_name:='tariff_item_sn';
		  dbms_output.put_line('4');
      end case;
      return key_field_name;
  end;

  function getIDfromInfoTable(a_instid number, a_template_id number,
            a_tablename varchar2, a_source_id varchar2) return varchar2
  is
    new_id varchar2(100) := null;
    cnt number;
  begin
      select count(*) into cnt from TEMPLATE_INSTANTE_INFO
      where instantiation_id = a_instid
        and template_id = a_template_id
        and source_rule_table_name = a_tablename
        and source_rule_id = a_source_id;
      if cnt <> 0 then
            select dest_rule_id into new_id from TEMPLATE_INSTANTE_INFO
            where instantiation_id = a_instid
              and template_id = a_template_id
              and source_rule_table_name = a_tablename
              and source_rule_id = a_source_id;
			  dbms_output.put_line('5');
      end if;
      return new_id;
  end;

   procedure getInstID(maxorfree in number,instid out number)
   is
   cnt number(8);
   begin
     select count(*)  into cnt from template_instantiation ;
     if maxorfree = 1 then
        select max(INSTANTIATION_ID)+1 into instid from TEMPLATE_INSTANTIATION;
        if instid > 99999999 then
            select rownum_ into instid from (select rownum rownum_,b.* from (select a.* from template_instantiation a order by instantiation_id )b) where rownum_< instantiation_id and rownum=1;
        dbms_output.put_line('6');
		end if;
        instid:=nvl(instid,1);
     elsif maxorfree =2 then
        select rownum_ into instid from (select rownum rownum_,b.* from (select a.* from template_instantiation a order by instantiation_id )b) where rownum_< instantiation_id and rownum=1;
     dbms_output.put_line('7');
	 end if;
     exception
     when no_data_found then
        if maxorfree=2 then
           if cnt>0 then
              select max(INSTANTIATION_ID)+1 into instid from TEMPLATE_INSTANTIATION;
			  dbms_output.put_line('8');
           else
              instid := 1;
			  dbms_output.put_line('9');
           end if;
        end if;
     when others then
        raise_application_error(-20001,'error when generate instantiation_id');
   end getInstID;
   --实例化
   procedure Instantiation(a_template_type in varchar2,a_param in varchar2,a_instid in number ,a_template_id in number,a_instCnt out number)
   is
   template_type varchar2(40);
   begin
       template_type := lower(a_template_type);
       if template_type= 'tariff_plan' then
            move_tariff_plan(a_instid,a_template_id);
			dbms_output.put_line('10');
       elsif template_type= 'tariff_plan_item' then
           move_tariff_schema(a_instid,a_template_id);
           move_tariff_plan_item(to_number(a_param),a_instid,a_template_id);
		   dbms_output.put_line('11');
       elsif template_type= 'tariff_schema' then
           move_tariff_schema(a_instid,a_template_id);
		   dbms_output.put_line('12');
       elsif template_type= 'tariff_item' then
           move_tariff_schema_item(to_number(a_param),to_number(a_param),a_instid,a_template_id);
		   dbms_output.put_line('13');
       end if;

       update_param_value(a_instid,a_template_id);
       commit;
       select count(*) into a_instCnt from template_instante_info a where a.instantiation_id=a_instid and a.template_id=a_template_id;
       exception
          when others then
		  dbms_output.put_line('14');
            rollback;
            raise_application_error(-20001,'error when move the data from template table');
   end Instantiation;


   procedure justify_data_type(a_table_name in varchar2 ,a_column_name in varchar2,a_lquotation out varchar2,a_rquotation out varchar2)
   is
   m_data_type varchar2(20);
   begin
       select  data_type into m_data_type from user_tab_columns where table_name=a_table_name and column_name=a_column_name;
       case lower(m_data_type)
       when 'number'then
          a_lquotation:='';
          a_rquotation:='';
		  dbms_output.put_line('15');
       when 'varchar2'then
          a_lquotation:='''';
          a_rquotation:='''';
		  dbms_output.put_line('16');
       when 'date' then
          a_lquotation:='to_date(''';
          a_rquotation:=''', ''yyyy-mm-dd hh24:mi:ss'')';
		  dbms_output.put_line('17');
       else
          a_lquotation:='''';
          a_rquotation:='''';
		  dbms_output.put_line('18');
       end case;
       Exception
          when others then
             raise_application_error(-20001,'don''t exist '||a_table_name||'.'||a_column_name);
   end ;
   procedure update_param_value(a_instid in number ,a_template_id in number)
   is
       recPtr template_param_value%rowtype;
       recPtrMap TEMPLATE_PARAM_MAP%rowtype;
       sql_text varchar2(2000);
       lquotation varchar2(30);
       rquotation varchar2(30);
       val varchar2(1024);
       sep varchar2(4);
       idx number(2);
       idx1 number(2);
       new_map_cond varchar2(256);
       new_rule_id varchar2(20);
       pos number(3);
       col_index number(3);
       trd_table_name varchar2(40);
       real_table_name varchar2(40);
   begin
       for recPtr in (select * from template_param_value where INSTANTIATION_ID=a_instid and template_id=a_template_id)
       loop

           for recPtrMap in (select * from template_param_map where template_id=recPtr.Template_Id and param_id=recPtr.Param_Id)
           loop
              val:='';
              trd_table_name:=recPtrMap.Mapping_Table_Name;
              real_table_name:=substr(recPtrMap.Mapping_Table_Name,5,length(recPtrMap.Mapping_Table_Name)-4);
              if recPtrMap.Mapping_Col_Index > 0 then
                  sql_text:='select '||recPtrMap.Mapping_Col_Name||' from '||real_table_name||' where '||recPtrMap.Mapping_Cond;
                  execute immediate sql_text into val;
                  sep:=recPtrMap.Mapping_Sep_Sign;
                  if recPtrMap.Mapping_Col_Index = 1 then--头部分
                     idx1:=instr(val,sep,1,1);
                     if idx1 = 0 then
                        val:=recPtr.Param_Value ;
						dbms_output.put_line('19');
                     else
                        val:=recPtr.Param_Value||substr(val,idx1,length(val)-idx1+1);
						dbms_output.put_line('20');
                     end if;
                  elsif instr(val,sep,1,recPtrMap.Mapping_Col_Index) = 0 then --尾部分
                     col_index :=recPtrMap.Mapping_Col_Index-1;
                     loop
                        idx:=instr(val,sep,1,col_index);
                        exit when idx <>0 ;
                        col_index:=col_index-1;
                        if col_index <=0 then
						dbms_output.put_line('21');
                           raise FormatError;
                        end if;
						dbms_output.put_line('22');
                     end loop;
                     val:=substr(val,1,idx)||recPtr.Param_Value ;
                  else       --中间部分
                    idx:=instr(val,sep,1,recPtrMap.Mapping_Col_Index-1);
                    idx1:=instr(val,sep,1,recPtrMap.Mapping_Col_Index);
                    val:=substr(val,1,idx)||recPtr.Param_Value||substr(val,idx1,length(val)-idx1+1);
					dbms_output.put_line('23');
                  end if;
              else
                  val :=recPtr.Param_Value;
				  dbms_output.put_line('24');
              end if;
              --得到新的id
              pos:=instr(recPtrMap.Mapping_Cond,'=',1);
              select DEST_RULE_ID
                into new_rule_id
                from TEMPLATE_INSTANTE_INFO
               where INSTANTIATION_ID = a_instid
                 and TEMPLATE_ID = a_template_id
                 and SOURCE_RULE_TABLE_NAME = trd_table_name
                 and SOURCE_RULE_ID =
                     substr(recPtrMap.Mapping_Cond,
                            pos + 2,
                            length(rtrim(recPtrMap.Mapping_Cond)) - pos - 2);
              new_map_cond:=substr(recPtrMap.Mapping_Cond,1,pos)||new_rule_id;
              justify_data_type(real_table_name,recPtrMap.Mapping_Col_Name,lquotation,rquotation);
              sql_text:='update '||real_table_name||' set '||recPtrMap.Mapping_Col_Name||'='||lquotation||val||rquotation ||' where '||new_map_cond;
              execute_immediate(sql_text);
           end loop;
       end loop;
       exception
          when FormatError then
		  dbms_output.put_line('25');
            raise_application_error(-20001,'param format exception');
          when no_data_found then
		  dbms_output.put_line('26');
            raise_application_error(-20001,'no data found');
          when others then
            raise_application_error(-20001,'update param error');
   end;


   procedure move_tariff_plan(a_instid in number,a_template_id in number)
   is

   new_plan_id varchar2(20);
   recPtr trd_tariff_plan%rowtype;
   begin
        select * into recPtr from trd_tariff_plan where template_id=a_template_id;
        new_plan_id := getIDfromInfoTable(a_instid,a_template_id,upper('trd_tariff_plan'),to_char(recPtr.tariff_plan_id));
        if new_plan_id is null then
           generateid(1,'tariff_plan',new_plan_id);
		   dbms_output.put_line('27');
        end if;
        insert into tariff_plan
        (
          TARIFF_PLAN_ID     ,
          TARIFF_PLAN_NAME   ,
          IS_RECURSIVE       ,
          PRECEDENCE         ,
          PROCESS_BEFORE     ,
          PROCESS_AFTER      ,
          PLANTYPE           ,
          NOTE               ,
          PRECEDENCE_ACCDISC ,
          PRICEPLANTYPE      ,
          REGION
        )
         values
         (
         to_number(new_plan_id),
         recPtr.TARIFF_PLAN_NAME,
         recPtr.IS_RECURSIVE,
         recPtr.PRECEDENCE,
         recPtr.PROCESS_BEFORE,
         recPtr.PROCESS_AFTER,
         recPtr.PLANTYPE,
         recPtr.NOTE,
         recPtr.PRECEDENCE_ACCDISC,
         recPtr.PRICEPLANTYPE,
         recPtr.REGION
         );
        --插入记录表TEMPLATE_INSTANTE_INFO
        insert into TEMPLATE_INSTANTE_INFO (INSTANTIATION_ID,TEMPLATE_ID,SOURCE_RULE_TABLE_NAME,SOURCE_RULE_ID,DEST_RULE_ID)
        values(a_instid,a_template_id,upper('trd_tariff_plan'),to_char(recPtr.tariff_plan_id),new_plan_id);
        move_tariff_schema(a_instid,a_template_id);
        move_tariff_plan_item(to_number(new_plan_id),a_instid,a_template_id);
        exception
          when others then
             raise_application_error(-20001,'move tariff plan  error');
   end move_tariff_plan;

   procedure move_tariff_schema(a_instid in number,a_template_id in number)
   is
   recPtr trd_tariff_schema%rowtype;
   recInfo TEMPLATE_INSTANTE_INFO%rowtype;
   a_new_schema_id varchar2(20);
   begin
       for recPtr in (select * from trd_tariff_schema where template_id=a_template_id)
       loop
           a_new_schema_id := getIDfromInfoTable(a_instid,a_template_id,upper('trd_tariff_schema'),to_char(recPtr.tariff_schema_id));
            if a_new_schema_id is null then
               generateid(1,'tariff_schema',a_new_schema_id);
			   dbms_output.put_line('28');
            end if;
           insert into tariff_schema
           (
              TARIFF_SCHEMA_ID,
              TARIFF_NAME     ,
              TARIFF_TYPE     ,
              FIELDCOUNT      ,
              FIELD_DEF       ,
              MATCH_ORDER     ,
              MATCH_TYPE      ,
              APPLY_METHOD    ,
              REFID           ,
              DISCOUNT_FEE_ID ,
              G_FIELD_DEF     ,
              ROUND_METHOD    ,
              ROUND_SCALE     ,
              REF_OFFSET      ,
              EVENT_ID        ,
              BILLCODE_ORDER  ,
              CREATETIME      ,
              REGION
            )
           values
           (
              to_number(a_new_schema_id),
              recPtr.TARIFF_NAME    ,
              recPtr.TARIFF_TYPE    ,
              recPtr.FIELDCOUNT     ,
              recPtr.FIELD_DEF      ,
              recPtr.MATCH_ORDER    ,
              recPtr.MATCH_TYPE     ,
              recPtr.APPLY_METHOD   ,
              recPtr.REFID          ,
              recPtr.DISCOUNT_FEE_ID,
              recPtr.G_FIELD_DEF    ,
              recPtr.ROUND_METHOD   ,
              recPtr.ROUND_SCALE    ,
              recPtr.REF_OFFSET     ,
              recPtr.EVENT_ID       ,
              recPtr.BILLCODE_ORDER ,
              recPtr.CREATETIME     ,
              recPtr.REGION);
            insert into TEMPLATE_INSTANTE_INFO(INSTANTIATION_ID,TEMPLATE_ID,SOURCE_RULE_TABLE_NAME,SOURCE_RULE_ID,DEST_RULE_ID)
             values(a_instid,a_template_id,upper('trd_tariff_schema'),to_char(recPtr.tariff_schema_id),a_new_schema_id);
         end loop;
         for recInfo in (select * from TEMPLATE_INSTANTE_INFO where INSTANTIATION_ID =a_instid
                     and TEMPLATE_ID = a_template_id  and SOURCE_RULE_TABLE_NAME ='TRD_TARIFF_SCHEMA')
         loop
             move_tariff_schema_item(recInfo.Dest_Rule_Id,recInfo.Source_Rule_Id,a_instid,a_template_id);
			 dbms_output.put_line('29');
         end loop;
       exception
          when others then
             raise_application_error(-20001,'move tariff schema  error');
   end move_tariff_schema;


   procedure move_tariff_plan_item(a_new_plan_id in number,a_instid in number,a_template_id in number)
   is

   new_tariff_plan_item_sn varchar2(20);
   recPtr trd_tariff_plan_item%rowtype;
   a_new_schema_id varchar2(20);
   begin
      for recPtr in  (select * from trd_tariff_plan_item where template_id=a_template_id)
      loop

          generateid(1,'tariff_plan_item',new_tariff_plan_item_sn);
          a_new_schema_id := getIDfromInfoTable(a_instid,a_template_id,upper('trd_tariff_schema'),to_char(recPtr.tariff_schema_id));
            if a_new_schema_id is null then
               generateid(1,'tariff_schema',a_new_schema_id);
			   dbms_output.put_line('30');
            end if;
          insert into tariff_plan_item(
                TARIFF_PLAN_ID,CALCTYPE,FEE_ID,TARIFF_SCHEMA_ID,CHARGING_EVENT_ID,
                PRECEDENCE,APPLYTIME,EXPIRETIME,APPLY_METHOD,DISCOUNT_TYPE,BASE_FEE_ID ,
                DISCOUNT_FEE_ID,CHARGING_COND,SWITCH_METHOD,SWITCH_UNIT,G_CHARGING_COND,
                BILLCODE,SELECT_METHOD,TARIFF_PLAN_ITEM_NAME,SERV_ID,TARIFF_PLAN_ITEM_SN)
           values(
                  to_number(a_new_plan_id),
                  recPtr.Calctype,
                  recPtr.Fee_Id,
                  to_number(a_new_schema_id),
                  recPtr.Charging_Event_Id,
                  recPtr.Precedence,
                  recPtr.Applytime,
                  recPtr.Expiretime,
                  recPtr.Apply_Method,
                  recPtr.Discount_Type,
                  recPtr.Base_Fee_Id,
                  recPtr.Discount_Fee_Id,
                  recPtr.Charging_Cond,
                  recPtr.Switch_Method,
                  recPtr.Switch_Unit,
                  recPtr.g_Charging_Cond,
                  recPtr.Billcode,
                  recPtr.Select_Method,
                  recPtr.Tariff_Plan_Item_Name,
                  recPtr.Serv_Id,
                  new_tariff_plan_item_sn
                  );
           --插入记录表
           insert into TEMPLATE_INSTANTE_INFO(INSTANTIATION_ID,TEMPLATE_ID,SOURCE_RULE_TABLE_NAME,SOURCE_RULE_ID,DEST_RULE_ID)
           values(a_instid,a_template_id,upper('trd_tariff_plan_item'),to_char(recPtr.tariff_plan_item_sn),new_tariff_plan_item_sn);
       end loop;
       exception
          when others then
             raise_application_error(-20001,'move tariff plan item error');
   end move_tariff_plan_item;


   procedure move_tariff_schema_item(a_new_schema_id in number,a_old_schema_id in number,a_instid in number,a_template_id in number)
   is
      rec REF_CURSOR;
      new_schema_item_sn varchar(20);
      recPtr trd_tariff_item%rowtype;
      new_schema_id varchar2(20);

   begin
         if a_new_schema_id <> a_old_schema_id then
             open rec for 'select * from trd_tariff_item where template_id='||to_char(a_template_id)
                      ||' and tariff_schema_id ='||to_char(a_old_schema_id);
					   dbms_output.put_line('31');
         else
              open rec for 'select * from trd_tariff_item where template_id='||to_char(a_template_id);
			   dbms_output.put_line('32');
         end if;

         loop
             fetch rec into recPtr;
             exit when rec%notfound;


             generateid(1,'tariff_item',new_schema_item_sn);
             if recPtr.Subtariff_Type = 2 then
                 new_schema_id := getIDfromInfoTable(a_instid,a_template_id,upper('trd_tariff_schema'),to_char(recPtr.Tariff_Id));
                 if new_schema_id is null then
                    generateid(1,'tariff_schema',new_schema_id);
					 dbms_output.put_line('33');
                 end if;
                 insert into tariff_item
                 (
                  TARIFF_SCHEMA_ID,
                  APPLYTIME,
                  EXPIRETIME ,
                  TARIFF_CRITERIA,
                  SUBTARIFF_TYPE ,
                  TARIFF_ID,
                  RATIO  ,
                  RATETYPE,
                  PARAM_STRING,
                  PRECEDENCE,
                  EXPR_ID,
                  G_PARAM ,
                  IS_DYNAMIC,
                  G_CRITERIA,
                  BILLCODE,
                  TARIFF_ITEM_SN ,
                  TARIFF_ITEM_NAME
                )
                 values (a_new_schema_id,
                         recPtr.Applytime,
                         recPtr.Expiretime,
                         recPtr.Tariff_Criteria,
                         recPtr.Subtariff_Type,--2：资费模式
                         to_number(new_schema_id),
                         recPtr.Ratio,
                         recPtr.Ratetype,
                         recPtr.Param_String,
                         recPtr.Precedence,
                         recPtr.Expr_Id,
                         recPtr.g_Param,
                         recPtr.Is_Dynamic,
                         recPtr.g_Criteria,
                         recPtr.Billcode,
                         new_Schema_item_sn,
                         recPtr.Tariff_Item_Name
                        );
                 --插入记录表
                 insert into  TEMPLATE_INSTANTE_INFO(INSTANTIATION_ID,TEMPLATE_ID,SOURCE_RULE_TABLE_NAME,SOURCE_RULE_ID,DEST_RULE_ID)
                  values(a_instid,a_template_id,upper('trd_tariff_item'),recPtr.Tariff_Item_Sn,new_schema_item_sn);
             else
                 insert into tariff_item
                 (
                  TARIFF_SCHEMA_ID,
                  APPLYTIME,
                  EXPIRETIME ,
                  TARIFF_CRITERIA,
                  SUBTARIFF_TYPE ,
                  TARIFF_ID,
                  RATIO  ,
                  RATETYPE,
                  PARAM_STRING,
                  PRECEDENCE,
                  EXPR_ID,
                  G_PARAM ,
                  IS_DYNAMIC,
                  G_CRITERIA,
                  BILLCODE,
                  TARIFF_ITEM_SN ,
                  TARIFF_ITEM_NAME
                )
                  values
                  (
                   to_number(a_new_schema_id),
                   recPtr.Applytime,
                   recPtr.Expiretime,
                   recPtr.Tariff_Criteria,
                   recPtr.Subtariff_Type,--2：资费模式
                   recPtr.Tariff_Id,
                   recPtr.Ratio,
                   recPtr.Ratetype,
                   recPtr.Param_String,
                   recPtr.Precedence,
                   recPtr.Expr_Id,
                   recPtr.g_Param,
                   recPtr.Is_Dynamic,
                   recPtr.g_Criteria,
                   recPtr.Billcode,
                   new_Schema_item_sn,
                   recPtr.Tariff_Item_Name
                  );
                  --将数据插入实例表
                  insert into TEMPLATE_INSTANTE_INFO(INSTANTIATION_ID,TEMPLATE_ID,SOURCE_RULE_TABLE_NAME,SOURCE_RULE_ID,DEST_RULE_ID)
                   values(a_instid,a_template_id,upper('trd_tariff_item'),recPtr.Tariff_Item_Sn,new_schema_item_sn);
				    dbms_output.put_line('34');
              end if;
          end loop;
          close rec;
          Exception
              when others then
                  raise_application_error(-20001,'the tariff_item template was destroyed');
   end move_tariff_schema_item;


   procedure generateid(maxorfree in number,whoseid in varchar2,newid out varchar2)
   is
   cnt number(6);
   begin
       case lower(whoseid)
       when 'tariff_plan' then
           select count(*) into cnt from tariff_plan ;
           if maxorfree = 1 then
               select to_char(max(tariff_plan_id)+1) into newid from tariff_plan;
               if length(newid)>6 then
                  select to_char(rownum_) into newid from (select rownum rownum_,b.* from (select tariff_plan_id from tariff_plan a order by tariff_plan_id )b) where rownum_< tariff_plan_id and rownum=1;
                dbms_output.put_line('35');
			   end if;

           else
               select to_char(rownum_) into newid from (select rownum rownum_,b.* from (select tariff_plan_id from tariff_plan a order by tariff_plan_id )b) where rownum_< tariff_plan_id and rownum=1;
            dbms_output.put_line('36');
		   end if;
       when 'tariff_plan_item' then
           if maxorfree = 1 then
               select to_char(to_date('20201103010101','yyyymmddhh24miss'),'yymmddhh24miss')||lpad(to_char(seq_recid8.nextval ),8,0) into newid from dual;
			    dbms_output.put_line('37');
           else
               null;
			    dbms_output.put_line('38');
           end if;
       when 'tariff_schema' then
           select count(*) into cnt from tariff_schema ;
           if maxorfree = 1 then
                select to_char(max(tariff_schema_id)+1) into newid from tariff_schema;
                if length(newid)>9 then
                   select to_char(rownum_) into newid
                       from (select rownum rownum_,b.*
                           from (select tariff_schema_id
                               from tariff_schema a order by tariff_schema_id )b) where rownum_< tariff_schema_id and rownum=1;
							    dbms_output.put_line('39');
                end if;

           else
                select to_char(rownum_) into newid
                from (select rownum rownum_,b.*
                       from (select tariff_schema_id
                             from tariff_schema a order by tariff_schema_id )b) where rownum_< tariff_schema_id and rownum=1;
							  dbms_output.put_line('40');
           end if;
       when 'tariff_item' then
           if maxorfree = 1 then
               select to_char(to_date('20201103010101','yyyymmddhh24miss'),'yymmddhh24miss')||lpad(to_char(seq_recid8.nextval ),8,0) into newid from dual;
			   dbms_output.put_line('41');
           else
               null;
			   dbms_output.put_line('42');
           end if;
       end case;
       newid := nvl(newid,'1');
       exception
           when no_data_found then
             if maxorfree=2 then
               if cnt>0 then
                  case lower(whoseid)
                  when 'tariff_plan' then
                      select to_char(max(tariff_plan_id)+1) into newid from tariff_plan;
					  dbms_output.put_line('43');
                  when 'tariff_schema' then
                      select to_char(max(tariff_schema_id)+1) into newid from tariff_schema;
					  dbms_output.put_line('44');
                  end case;
               else
                  newid := '1';
				  dbms_output.put_line('45');
               end if;
             end if;
           when others then
             raise_application_error(-20001,'generate record id error');
   end generateid;

   procedure clearInstTable(a_instid in number)
   is
   begin
       delete from TEMPLATE_INSTANTIATION where INSTANTIATION_ID=a_instid;
       delete from TEMPLATE_PARAM_VALUE where  INSTANTIATION_ID=a_instid;
	   dbms_output.put_line('46');
       commit;
       exception
       when others then
          rollback;
   end clearInstTable;

   procedure ret_inst_recmsg(a_instid in number ,a_template_id in number,a_start in number, a_end in number, a_retMsg out varchar2)
   is
   tmp_tab_name varchar2(128);
   begin
     for rec in ( select dest_rule_id,Source_Rule_Table_Name from(select rownum rownum_,a.* from template_instante_info a where instantiation_id=a_instid and template_id=a_template_id order by dest_rule_id) where rownum_ between a_start and a_end ) loop
        tmp_tab_name:='';
        select name into tmp_tab_name from code_dict a where a.catalog='TEMPLATE_TABLE_NAME' and a.code=upper(rec.Source_Rule_Table_Name);
        tmp_tab_name:=nvl(tmp_tab_name,rec.Source_Rule_Table_Name);
        a_retMsg:=a_retMsg||tmp_tab_name||' : ['||rec.Dest_Rule_Id||']'||chr(13)||chr(10);
		dbms_output.put_line('47');
     end loop;

     exception
         when others then
            raise_application_error(-20001,'return insert message error');
   end ret_inst_recmsg;


   procedure export_insert_sql(a_instid in number ,a_template_id in number ,a_source_rule_table_name in varchar2,a_dest_rule_id in varchar2,a_ret_sql out varchar2)
   is
     singal_sql_text1 varchar2(2000);
     singal_sql_text2 varchar2(2000);
     real_tab_name varchar2(128);
     exec_sql varchar2(500);
     col_val varchar2(2000);
     index_col_name varchar2(128);
     lquotation varchar2(40);--
     rquotation varchar2(40);
     ilquotation varchar2(30);--index_column 的左右
     irquotation varchar2(30);
     d2c_lquotation varchar2(30);
     d2c_rquotation varchar2(30);

   begin
      for recPtr in (select * from template_instante_info where instantiation_id = a_instid and template_id = a_template_id and source_rule_table_name = a_source_rule_table_name and dest_rule_id=a_dest_rule_id)
      loop
        real_tab_name:=substr(recPtr.source_rule_table_name,5,length(recPtr.source_rule_table_name)-4);
        singal_sql_text1:='insert into '||real_tab_name||' (';
        singal_sql_text2:=' values(';
        select note into index_col_name from code_dict where upper(catalog)='TEMPLATE_TABLE_NAME' and upper(code)=upper(recPtr.source_rule_table_name);
        justify_data_type(real_tab_name,index_col_name,ilquotation,irquotation);
        for tabColPtr in (select * from user_tab_columns where table_name=real_tab_name order by column_name)
        loop
            singal_sql_text1:=singal_sql_text1||tabColPtr.column_name||',';
            justify_data_type(real_tab_name,tabColPtr.column_name,lquotation,rquotation);
            --将时间类型转换成yyyy-mm-dd hh24:mi:ss 格式
            if lquotation='to_date(''' then
              d2c_lquotation:='to_char(';
              d2c_rquotation:=',''yyyy-mm-dd hh24:mi:ss'')';
			  dbms_output.put_line('48');
            else
              d2c_lquotation:='';
              d2c_rquotation:='';
			  dbms_output.put_line('49');
            end if;
            exec_sql:='select '||d2c_lquotation||tabColPtr.column_name||d2c_rquotation||' from '||real_tab_name||' where '||index_col_name||' = '||ilquotation||upper(recPtr.dest_rule_id)||irquotation;

            execute immediate exec_sql into col_val;
            singal_sql_text2:=singal_sql_text2||lquotation||col_val||rquotation||',';
        end loop;
        singal_sql_text1:=substr(singal_sql_text1,1,length(singal_sql_text1)-1)||')'||chr(13)||chr(10);
        singal_sql_text2:=substr(singal_sql_text2,1,length(singal_sql_text2)-1)||');'||chr(13)||chr(10);
        a_ret_sql:=a_ret_sql||singal_sql_text1||singal_sql_text2;

      end loop;

      exception
        when others then
          raise_application_error(-20001,'export sql text error');
   end export_insert_sql;

begin
  -- Initialization
  --<Statement>;
  null;
  dbms_output.put_line('50');
end BILL_TEMPLATE;


$

  CREATE OR REPLACE PACKAGE BODY "BP" AS

FUNCTION f_check_validdate(v_date IN VARCHAR2,d_lastdate OUT DATE)RETURN DATE
IS
d_date DATE;
v_lastdate VARCHAR2(16);
BEGIN
 --JUDGE CLOSE DATE IS 29 TO 31  BY W54999 ON 20080129
    v_lastdate := to_char(last_day(to_date(substr(v_date,1,6),'YYYYMM')),'YYYYMMDD')||substr(v_date,length(v_date)-5,6);
    IF v_date>v_lastdate THEN
       d_lastdate := to_date(v_lastdate,'YYYYMMDDHH24MISS');
       d_date := to_date(to_char(trunc(add_months(to_date(v_lastdate,'YYYYMMDDHH24MISS'),1),'mm'),'YYYYMMDD')||substr(v_date,length(v_date)-5,6),'YYYYMMDDHH24MISS');
	   dbms_output.put_line('1');
    ELSE
       d_date := to_date(v_date,'YYYYMMDDHH24MISS');
       d_lastdate := d_date;
	   dbms_output.put_line('2');
    END IF;   
RETURN d_date;
END;

FUNCTION f_get_closedate(d_odate IN DATE,v_cdate IN VARCHAR2,n_unit IN NUMBER,d_ldate OUT DATE) RETURN DATE
IS
n_month NUMBER;
d_cdate DATE;
v_lastdate VARCHAR2(16);
BEGIN
    --get valid date for string 
    v_lastdate := to_char(last_day(to_date(substr(v_cdate,1,6),'YYYYMM')),'YYYYMMDD')||substr(v_cdate,length(v_cdate)-5,6);
    IF v_cdate>v_lastdate THEN
       d_cdate := to_date(v_lastdate,'YYYYMMDDHH24MISS');
	   dbms_output.put_line('3');
    ELSE
       d_cdate := to_date(v_cdate,'YYYYMMDDHH24MISS');
	   dbms_output.put_line('4');
    END IF;   
    --check whether the month between close date and open date exceed the month unit
    SELECT round(months_between(d_cdate,d_odate)) INTO n_month FROM dual;
    IF n_month<>n_unit THEN
	dbms_output.put_line('5');
      RETURN f_check_validdate(to_char(add_months(d_cdate,-(n_month-n_unit)),'YYYYMM')||substr(v_cdate,length(v_cdate)-7,8),d_ldate);
    ELSE
	dbms_output.put_line('6');
      RETURN f_check_validdate(v_cdate,d_ldate);
    END IF;
END;

function f_create_new_billingcycle(	theSchema 		in 		billingcycle_schema%rowtype,
									newOpenDate		in		date,
                  v_flag IN VARCHAR)
return billingcycle%rowtype
is
	theNewCycle		billingcycle%rowtype;
  d_opendate DATE;
  d_closedate DATE;
  v_seq  NUMBER;
begin
	--帐务周期类型。每种帐务周期对应特定的帐务周期命名格式。
  theNewCycle.opendate := newOpenDate;
	--1:	year：	YYYY
	if(theSchema.cycle_unit = 1)then
		--format new_open_date
    IF v_flag='0' THEN
		theNewCycle.opendate := to_date(to_char(newOpenDate,'YYYY')||substr(theSchema.opendate,length(theSchema.opendate)-9,10),'YYYYMMDDHH24MISS');
		dbms_output.put_line('7');
		END IF;
    --generate close date
		theNewCycle.closedate := add_months(theNewCycle.opendate,12 * theSchema.unit_count);
		--generate invoice date
		theNewCycle.invoicedate := theNewCycle.closedate + theSchema.invoice_delay/24;
    IF theSchema.Cycleid_Base=1 THEN
		--generate new cycle name
		theNewCycle.cycle_name := to_char(theNewCycle.opendate,'YYYY');
		--generate new cycle id
		theNewCycle.cycle_id := to_char(theNewCycle.opendate,'YYYY');
		dbms_output.put_line('8');
    ELSE 
		--generate new cycle name
		theNewCycle.cycle_name := to_char(theNewCycle.closedate,'YYYY');
		--generate new cycle id
		theNewCycle.cycle_id := to_char(theNewCycle.closedate,'YYYY');
		dbms_output.put_line('9');
    END IF;
	--2:	season：	YYYYQ
	elsif(theSchema.cycle_unit = 2)then
		--format new_open_date
    IF v_flag='0' THEN
		theNewCycle.opendate := to_date(to_char(newOpenDate,'YYYYMM')||substr(theSchema.opendate,length(theSchema.opendate)-7,8),'YYYYMMDDHH24MISS');
		dbms_output.put_line('10');
		END IF;
    --generate close date
		theNewCycle.closedate := add_months(theNewCycle.opendate,3 * theSchema.unit_count);
		--generate invoice date
		theNewCycle.invoicedate := theNewCycle.closedate + theSchema.invoice_delay/24;
    IF theSchema.Cycleid_Base=1 THEN
		--generate new cycle name
		theNewCycle.cycle_name := to_char(theNewCycle.opendate,'YYYYQ');
		--generate new cycle id
		theNewCycle.cycle_id := to_char(theNewCycle.opendate,'YYYYQ');
		dbms_output.put_line('11');
    ELSE 
		--generate new cycle name
		theNewCycle.cycle_name := to_char(theNewCycle.closedate,'YYYYQ');
		--generate new cycle id
		theNewCycle.cycle_id := to_char(theNewCycle.closedate,'YYYYQ');
		dbms_output.put_line('12');
    END IF;
	--3:	month：	YYYYMM
	elsif(theSchema.cycle_unit = 3)then
		--format new_open_date
    IF v_flag='0' THEN
		theNewCycle.opendate := f_check_validdate(to_char(newOpenDate,'YYYYMM')||substr(theSchema.opendate,length(theSchema.opendate)-7,8),d_opendate);
		dbms_output.put_line('13');
		END IF;
    
    --generate close date
    --JUDGE CLOSE DATE IS 29 TO 31  BY W54999 ON 20080129
		theNewCycle.Closedate := f_get_closedate(theNewCycle.opendate,to_char(add_months(theNewCycle.opendate,1 * theSchema.unit_count),'YYYYMM')||substr(theSchema.opendate,length(theSchema.opendate)-7,8),1*theSchema.unit_count,d_closedate);
    --generate invoice date
		theNewCycle.invoicedate := theNewCycle.closedate + theSchema.invoice_delay/24;
    IF v_flag='0' THEN
      IF theSchema.Cycleid_Base=1 THEN 
		  --generate new cycle name
      theNewCycle.cycle_name := to_char(d_opendate,'YYYY')||lpad(ceil(to_number(to_char(d_opendate,'mm'))/theschema.unit_count),2,'0');
		  --generate new cycle id
      theNewCycle.cycle_id := to_char(d_opendate,'YYYY')||lpad(ceil(to_number(to_char(d_opendate,'mm'))/theschema.unit_count),2,'0');
	  dbms_output.put_line('14');
	    ELSE
		  --generate new cycle name
      theNewCycle.cycle_name := to_char(d_closedate,'YYYY')||lpad(ceil(to_number(to_char(d_closedate,'mm'))/theschema.unit_count),2,'0');
		  --generate new cycle id
      theNewCycle.cycle_id := to_char(d_closedate,'YYYY')||lpad(ceil(to_number(to_char(d_closedate,'mm'))/theschema.unit_count),2,'0');
	  dbms_output.put_line('15');
	    END IF;
    ELSE
      IF theSchema.Cycleid_Base=1 THEN
    	--generate new cycle name
      SELECT to_number(nvl(substr(MAX(cycle_id),5,2),'0')) INTO v_seq  FROM billingcycle t 
      WHERE cycle_schema_id=theSchema.Cycle_Schema_Id AND substr(cycle_id,1,4)=to_char(theNewCycle.opendate,'YYYY');
    
		  theNewCycle.cycle_name := to_char(theNewCycle.opendate,'YYYY')||lpad(to_char(v_seq+1),2,'0');
		  --generate new cycle id
		  theNewCycle.cycle_id := to_char(theNewCycle.opendate,'YYYY')||lpad(to_char(v_seq+1),2,'0');
		  dbms_output.put_line('16');
      ELSE
    	--generate new cycle name
      SELECT to_number(nvl(substr(MAX(cycle_id),5,2),'0')) INTO v_seq  FROM billingcycle t 
      WHERE cycle_schema_id=theSchema.Cycle_Schema_Id AND substr(cycle_id,1,4)=to_char(theNewCycle.closedate,'YYYY');
    
		  theNewCycle.cycle_name := to_char(theNewCycle.closedate,'YYYY')||lpad(to_char(v_seq+1),2,'0');
		  --generate new cycle id
		  theNewCycle.cycle_id := to_char(theNewCycle.closedate,'YYYY')||lpad(to_char(v_seq+1),2,'0');
		  dbms_output.put_line('17');
	    END IF;
    END IF;	
  --4:	10 day：	YYYYMMDD
	elsif(theSchema.cycle_unit = 4)then
		--format new_open_date
    IF v_flag='0' THEN
		theNewCycle.opendate := to_date(to_char(newOpenDate,'YYYYMMDD')||substr(theSchema.opendate,length(theSchema.opendate)-5,6),'YYYYMMDDHH24MISS');
		dbms_output.put_line('18');
		END IF;
    --generate close date
		theNewCycle.closedate := theNewCycle.opendate + 10 * theSchema.unit_count;
		--generate invoice date
		theNewCycle.invoicedate := theNewCycle.closedate + theSchema.invoice_delay/24;
    IF theSchema.Cycleid_Base=1 THEN
		--generate new cycle name
		theNewCycle.cycle_name := to_char(theNewCycle.opendate,'YYYYMMDD');
		--generate new cycle id
		theNewCycle.cycle_id := to_char(theNewCycle.opendate,'YYYYMMDD');
		dbms_output.put_line('19');
    ELSE
		--generate new cycle name
		theNewCycle.cycle_name := to_char(theNewCycle.closedate,'YYYYMMDD');
		--generate new cycle id
		theNewCycle.cycle_id := to_char(theNewCycle.closedate,'YYYYMMDD');
		dbms_output.put_line('20');
    END IF;
	--5:	week：	YYYYMMDD
	elsif(theSchema.cycle_unit = 5)then
		--format new_open_date
    IF v_flag='0' THEN
		theNewCycle.opendate := to_date(to_char(newOpenDate,'YYYYMMDD')||substr(theSchema.opendate,length(theSchema.opendate)-5,6),'YYYYMMDDHH24MISS');
		dbms_output.put_line('21');
		END IF;
    --generate close date
		theNewCycle.closedate := theNewCycle.opendate + 7 * theSchema.unit_count;
		--generate invoice date
		theNewCycle.invoicedate := theNewCycle.closedate + theSchema.invoice_delay;
		--generate new cycle name
    IF theSchema.Cycleid_Base=1 THEN
		SELECT to_number(nvl(substr(MAX(cycle_id),5,2),'0')) INTO v_seq  FROM billingcycle t 
    WHERE cycle_schema_id=theSchema.Cycle_Schema_Id AND substr(cycle_id,1,4)=to_char(theNewCycle.opendate,'YYYY');
    
    --theNewCycle.cycle_name := to_char(theNewCycle.opendate,'YYYYMMDD');
		theNewCycle.cycle_name := to_char(theNewCycle.opendate,'YYYY')||lpad(to_char(v_seq+1),2,'0');
		--generate new cycle id
		--theNewCycle.cycle_id := to_char(theNewCycle.opendate,'YYYYMMDD');
		theNewCycle.cycle_id := to_char(theNewCycle.opendate,'YYYY')||lpad(to_char(v_seq+1),2,'0');
		dbms_output.put_line('22');
    ELSE
		SELECT to_number(nvl(substr(MAX(cycle_id),5,2),'0')) INTO v_seq  FROM billingcycle t 
    WHERE cycle_schema_id=theSchema.Cycle_Schema_Id AND substr(cycle_id,1,4)=to_char(theNewCycle.closedate,'YYYY');
    
    --theNewCycle.cycle_name := to_char(theNewCycle.opendate,'YYYYMMDD');
		theNewCycle.cycle_name := to_char(theNewCycle.closedate,'YYYY')||lpad(to_char(v_seq+1),2,'0');
		--generate new cycle id
		--theNewCycle.cycle_id := to_char(theNewCycle.opendate,'YYYYMMDD');
		theNewCycle.cycle_id := to_char(theNewCycle.closedate,'YYYY')||lpad(to_char(v_seq+1),2,'0');
		dbms_output.put_line('23');
	  END IF;
	--6:	day：	YYYYMMDD
	else	--6
		--format new_open_date
    IF v_flag='0' THEN
		theNewCycle.opendate := to_date(to_char(newOpenDate,'YYYYMMDD')||substr(theSchema.opendate,length(theSchema.opendate)-5,6),'YYYYMMDDHH24MISS');
		dbms_output.put_line('24');
		END IF;
    --generate close date
		theNewCycle.closedate := theNewCycle.opendate + 1 * theSchema.unit_count;
		--generate invoice date
		theNewCycle.invoicedate := theNewCycle.closedate + theSchema.invoice_delay/24;
		--generate new cycle name
    IF theSchema.Cycleid_Base=1 THEN
		SELECT to_number(nvl(substr(MAX(cycle_id),5,2),'0')) INTO v_seq  FROM billingcycle t 
    WHERE cycle_schema_id=theSchema.Cycle_Schema_Id AND substr(cycle_id,1,4)=to_char(theNewCycle.opendate,'YYYY');
    
    --theNewCycle.cycle_name := to_char(theNewCycle.opendate,'YYYYMMDD');
		theNewCycle.cycle_name := to_char(theNewCycle.opendate,'YYYY')||lpad(to_char(v_seq+1),2,'0');
		--generate new cycle id
		--theNewCycle.cycle_id := to_char(theNewCycle.opendate,'YYYYMMDD');
		theNewCycle.cycle_id := to_char(theNewCycle.opendate,'YYYY')||lpad(to_char(v_seq+1),2,'0');
		dbms_output.put_line('25');
    ELSE
		SELECT to_number(nvl(substr(MAX(cycle_id),5,2),'0')) INTO v_seq  FROM billingcycle t 
    WHERE cycle_schema_id=theSchema.Cycle_Schema_Id AND substr(cycle_id,1,4)=to_char(theNewCycle.closedate,'YYYY');
    --theNewCycle.cycle_name := to_char(theNewCycle.opendate,'YYYYMMDD');
		theNewCycle.cycle_name := to_char(theNewCycle.closedate,'YYYY')||lpad(to_char(v_seq+1),2,'0');
		--generate new cycle id
		--theNewCycle.cycle_id := to_char(theNewCycle.opendate,'YYYYMMDD');
		theNewCycle.cycle_id := to_char(theNewCycle.closedate,'YYYY')||lpad(to_char(v_seq+1),2,'0');
		dbms_output.put_line('26');
    END IF;
	end if;
	return theNewCycle;
end;

function f_billingcycle_translate(theCond in varchar2,theBillingCycle in billingcycle%rowtype) return varchar2
is
	s_cond					varchar2(500);
	s_replace				varchar2(20);
begin
	s_cond := theCond;
	s_replace := to_char(theBillingCycle.cycle_id);
	s_cond := replace(s_cond,chr(3)||'0008-.0001'||chr(4),s_replace);
	s_cond := replace(s_cond,chr(3)||'0008->0001'||chr(4),s_replace);
	s_cond := replace(s_cond,chr(3)||'0008=.0001'||chr(4),s_replace);
	s_cond := replace(s_cond,chr(3)||'0008=>0001'||chr(4),s_replace);
	s_cond := replace(s_cond,chr(3)||'0008!.0001'||chr(4),s_replace);
	s_cond := replace(s_cond,chr(3)||'0008!>0001'||chr(4),s_replace);

	s_replace := ''''||to_char(theBillingCycle.opendate,'YYYYMMDDHH24MISS')||'''';
	s_cond := replace(s_cond,chr(3)||'0008-.0002'||chr(4),s_replace);
	s_cond := replace(s_cond,chr(3)||'0008->0002'||chr(4),s_replace);
	s_cond := replace(s_cond,chr(3)||'0008=.0002'||chr(4),s_replace);
	s_cond := replace(s_cond,chr(3)||'0008=>0002'||chr(4),s_replace);
	s_cond := replace(s_cond,chr(3)||'0008!.0002'||chr(4),s_replace);
	s_cond := replace(s_cond,chr(3)||'0008!>0002'||chr(4),s_replace);

	s_replace := ''''||to_char(theBillingCycle.closedate,'YYYYMMDDHH24MISS')||'''';
	s_cond := replace(s_cond,chr(3)||'0008-.0003'||chr(4),s_replace);
	s_cond := replace(s_cond,chr(3)||'0008->0003'||chr(4),s_replace);
	s_cond := replace(s_cond,chr(3)||'0008=.0003'||chr(4),s_replace);
	s_cond := replace(s_cond,chr(3)||'0008=>0003'||chr(4),s_replace);
	s_cond := replace(s_cond,chr(3)||'0008!.0003'||chr(4),s_replace);
	s_cond := replace(s_cond,chr(3)||'0008!>0003'||chr(4),s_replace);

	s_replace := ''''||to_char(theBillingCycle.invoicedate,'YYYYMMDDHH24MISS')||'''';
	s_cond := replace(s_cond,chr(3)||'0008-.0004'||chr(4),s_replace);
	s_cond := replace(s_cond,chr(3)||'0008->0004'||chr(4),s_replace);
	s_cond := replace(s_cond,chr(3)||'0008=.0004'||chr(4),s_replace);
	s_cond := replace(s_cond,chr(3)||'0008=>0004'||chr(4),s_replace);
	s_cond := replace(s_cond,chr(3)||'0008!.0004'||chr(4),s_replace);
	s_cond := replace(s_cond,chr(3)||'0008!>0004'||chr(4),s_replace);
dbms_output.put_line('27');
	return s_cond;
end;

function f_billingcycle_translate_name(theCond in varchar2,theBillingCycle in billingcycle%rowtype) return varchar2
is
	s_cond					varchar2(500);
	s_replace				varchar2(20);
begin
	s_cond := theCond;
	s_replace := to_char(theBillingCycle.cycle_id);
	s_cond := replace(s_cond,'帐务周期.帐务周期 ID',s_replace);
	s_replace := ''''||to_char(theBillingCycle.opendate,'YYYYMMDDHH24MISS')||'''';
	s_cond := replace(s_cond,'帐务周期.帐务周期开始时间',s_replace);
	s_replace := ''''||to_char(theBillingCycle.closedate,'YYYYMMDDHH24MISS')||'''';
	s_cond := replace(s_cond,'帐务周期.帐务周期结束时间',s_replace);
	s_replace := ''''||to_char(theBillingCycle.invoicedate,'YYYYMMDDHH24MISS')||'''';
	s_cond := replace(s_cond,'帐务周期.帐务周期出帐时间',s_replace);
	if s_cond is null then
		s_cond := 'xx' ;
		dbms_output.put_line('28');
	end if;
	return s_cond;
end;


procedure p_generate_billingcycle
	(n_cycle_schema_id	in	billingcycle_schema.cycle_schema_id%type)
is
	n_cycle_origin_count	pls_integer	:= 0;
	n_pre_built_num			pls_integer;
	n_new_cycle_status		pls_integer;
	d_new_open_date			date;
	d_last_open_date		date;
	d_last_close_date		date;
  v_cycle_id           billingcycle.cycle_id%type;
	thisSchema				billingcycle_schema%rowtype;
	theNewCycle				billingcycle%rowtype;
	s_new_billing_cond		varchar2(400);
	s_g_billing_cond 		charging_cycle.g_billing_cond%type :=' ';
	WRONG_STATUS			exception;
	ldt_today				date ;
begin
	--Get ServerDate
--echo ##手动将不稳定的to_date('20201103010101','yyyymmddhh24miss')改为稳定值
--	select to_date('20201103010101','yyyymmddhh24miss') into ldt_today from dual ;
select to_date('20201020172433','YYYYMMDDHH24MISS')  into ldt_today from dual ;
	select * into thisSchema from billingcycle_schema where cycle_schema_id = n_cycle_schema_id;
	--check status and fixed_cycle
	if(thisSchema.status<>1 or thisSchema.fixed_cycle<>1) then
	dbms_output.put_line('29');
		raise WRONG_STATUS;
	end if;
	select count(*) into n_cycle_origin_count from billingcycle where cycle_schema_id = n_cycle_schema_id;
	--以前没有任何此类（CycleSchemaId）的帐务周期
	if(n_cycle_origin_count = 0) THEN
  --delete void data from charging cycle by w54999 on 20080129
  delete from charging_cycle where cycle_schema_id=n_cycle_schema_id; 
		-- 建立第一个帐务周期并激活之(status=2)
		dbms_output.put_line('no origin record.');
		d_new_open_date := to_date(thisSchema.firstcycle_opendate,'YYYYMMDDHH24MISS');
		Thenewcycle := f_create_new_billingcycle(thisSchema,d_new_open_date,'0');
		--当前激活
		insert into billingcycle
			(cycle_schema_id,cycle_id,cycle_name,cycle_status,opendate,closedate,invoicedate)
			values
			(n_cycle_schema_id,theNewCycle.cycle_id,theNewCycle.cycle_name,2,theNewCycle.opendate,theNewCycle.closedate,theNewCycle.invoicedate);
		--generate charging_cycle
		for thisCycleSchema in (select * from charging_cycle_schema where cycle_schema_id=n_cycle_schema_id) loop
			s_new_billing_cond := f_billingcycle_translate(thisCycleSchema.billing_cond,theNewCycle);
			--Get Charging_Cycle_Schema G_Billing_Cond
			s_g_billing_cond := f_billingcycle_translate_name(thisCycleSchema.g_billing_cond,theNewCycle);
			insert into charging_cycle
				(cycle_schema_id,cycle_id,charging_event_id,billing_cond,g_billing_cond)
				values
				(n_cycle_schema_id,theNewCycle.cycle_id,thisCycleSchema.charging_event_id,s_new_billing_cond,' '||s_g_billing_cond);
				dbms_output.put_line('30');
		end loop;
		--Update Param_Table_Def LastChangeTime
		update param_table_def set lastchangetime=ldt_today where upper(table_name)='BILLINGCYCLE' ;
		update param_table_def set lastchangetime=ldt_today where upper(table_name)='CHARGING_CYCLE' ;
	end if;
  --add judge whether current bill cycle exist by w54999 on 20080129
BEGIN
	select cycle_id,opendate,closedate INTO v_cycle_id,d_last_open_date,d_last_close_date	from billingcycle where cycle_schema_id = n_cycle_schema_id and cycle_status=2;
	dbms_output.put_line('31');
EXCEPTION
  WHEN no_data_found THEN
  dbms_output.put_line('32');
     raise_application_error(-20100,'Can not find the current billingcycle for cycle_schema_id = '||n_cycle_schema_id||'!');
END;
	--delete old data  Modified 2003.12.16
	delete from charging_cycle where cycle_schema_id=n_cycle_schema_id and
	  		cycle_id >= v_cycle_id;
  
  --refenerate charging_cycle according to billingcycle fixed by weidh 20071211
  FOR thebillingcycle IN (SELECT * FROM billingcycle 
  WHERE cycle_schema_id = n_cycle_schema_id  and opendate >= d_last_open_date ORDER BY cycle_id) LOOP
    --generate charging_cycle
		for thisCycleSchema in (select * from charging_cycle_schema where cycle_schema_id=n_cycle_schema_id) loop
			s_new_billing_cond := f_billingcycle_translate(thisCycleSchema.billing_cond,thebillingcycle);
			s_g_billing_cond := f_billingcycle_translate_name(thisCycleSchema.g_billing_cond,thebillingcycle);
			insert into charging_cycle
				(cycle_schema_id,cycle_id,charging_event_id,billing_cond,g_billing_cond)
				values
				(n_cycle_schema_id,thebillingcycle.cycle_id,thisCycleSchema.charging_event_id,s_new_billing_cond,s_g_billing_cond);
				dbms_output.put_line('33');
		end loop;		
  END LOOP;  

	select count(*) into n_pre_built_num from billingcycle where cycle_schema_id = n_cycle_schema_id and opendate > d_last_open_date;
  select max(opendate),max(closedate) into d_last_open_date,d_last_close_date
		from billingcycle where cycle_schema_id = n_cycle_schema_id;
	dbms_output.put_line('get max: opendate='||to_char(d_last_open_date,'YYYYMMDD HH24MISS')||' closedate='||to_char(d_last_close_date,'YYYYMMDD HH24MISS'));

	if(thisSchema.multiactivation = 1) then
		n_new_cycle_status := 1;
		dbms_output.put_line('34');
	else
		n_new_cycle_status := 5;
		dbms_output.put_line('35');
	end if;

	dbms_output.put_line('count of cycles need build = '||to_char(thisSchema.prebuilt_cycles-n_pre_built_num));
	for i in 1..(thisSchema.prebuilt_cycles-n_pre_built_num) loop
		theNewCycle := f_create_new_billingcycle(thisSchema,d_last_close_date,'1');
		dbms_output.put_line('new cycle: id='||theNewCycle.cycle_id||' opendate='||to_char(theNewCycle.opendate,'yyyymmdd hh24miss')||' closedate='||to_char(theNewCycle.closedate,'yyyymmdd hh24miss'));
		insert into billingcycle
			(cycle_schema_id,cycle_id,cycle_name,cycle_status,opendate,closedate,invoicedate)
			values
			(n_cycle_schema_id,theNewCycle.cycle_id,theNewCycle.cycle_name,n_new_cycle_status,d_last_close_date,theNewCycle.closedate,theNewCycle.invoicedate);
		--generate charging_cycle
		for thisCycleSchema in (select * from charging_cycle_schema where cycle_schema_id=n_cycle_schema_id) loop
			s_new_billing_cond := f_billingcycle_translate(thisCycleSchema.billing_cond,theNewCycle);
			s_g_billing_cond := f_billingcycle_translate_name(thisCycleSchema.g_billing_cond,theNewCycle);
			insert into charging_cycle
				(cycle_schema_id,cycle_id,charging_event_id,billing_cond,g_billing_cond)
				values
				(n_cycle_schema_id,theNewCycle.cycle_id,thisCycleSchema.charging_event_id,s_new_billing_cond,s_g_billing_cond);
				dbms_output.put_line('36');
		end loop;
		d_last_close_date := theNewCycle.closedate;
	end loop;

	if (thisSchema.prebuilt_cycles - n_pre_built_num) > 0 then
		--Update Param_Table_Def LastChangeTime
		update param_table_def set lastchangetime=ldt_today where upper(table_name)='BILLINGCYCLE' ;
		update param_table_def set lastchangetime=ldt_today where upper(table_name)='CHARGING_CYCLE' ;
		dbms_output.put_line('37');
	else
		update param_table_def set lastchangetime=ldt_today where upper(table_name)='CHARGING_CYCLE' ;
		dbms_output.put_line('38');
	end if ;

end;

procedure p_generate_all_billingcycle
is
begin
	for theSchemaId in (select cycle_schema_id from billingcycle_schema where status=1 and fixed_cycle=1) loop
		p_generate_billingcycle(theSchemaId.cycle_schema_id);
		dbms_output.put_line('39');
	end loop;
end;

end; --end package

$

  CREATE OR REPLACE PACKAGE BODY "CDRFILELOGCHECK" as
/*=====================================================================================================*/
function get_number(cFilename varchar2, nStart integer, aLength integer) return integer
is
	nRet	integer := -1;
begin
	nRet := to_number(substr(cFilename,nStart,aLength));
	dbms_output.put_line('1');
	return nRet;
exception when others then
	return -1;
end;
/*=====================================================================================================*/
procedure checkgsmfilelogbyname( rFormatRow	in cdrfilename_format%rowtype)
is
    v_max_filename varchar2(256);
	lastfile	sepfilelog%rowtype:=null;
	lastfilenum	integer:=null;
	curfilenum	integer:=null;

	-- ��������������D����������a����2������������t
	cursor curGsmFileLogFiles( aFileNameFormat varchar2, aFileName varchar2)
	 is select * from sepfilelog
	 where filename like aFileNameFormat and filename >= aFileName
       and filename <= v_max_filename
	 order by filename;

begin
	dbms_output.put_line('name check '||rFormatRow.filenameformat);
	-- ����3y����������������D��o����3����D������
	delete filelog_check
    	where  nettype = rFormatRow.nettype and errortype='S'
	      and filename2 like rFormatRow.filenameformat
          and filename2 >= rFormatRow.lastfilename;

    --����3��������������2��������������������t����
    select nvl(max(filename),rFormatRow.lastfilename) into v_max_filename from sepfilelog
	 where filename like rFormatRow.filenameformat and filename >= rFormatRow.lastfilename
       and endprocessdate < to_date('20201103010101','yyyymmddhh24miss')-3/24 ;

	-- ����2����������DD
	for "file" in curGsmFileLogFiles(rFormatRow.filenameformat,rFormatRow.lastfilename) loop
		if length("file".filename) <> rFormatRow.filenamelen and instr("file".filename,'.dat')=0 then
			if lastfile.filename is not null then
				insert into filelog_check(nettype,errortype,filename1,
					processtime1,filename2,processtime2)
				values(rFormatRow.nettype,'L',lastfile.filename,
					lastfile.beginprocessdate,"file".filename,"file".beginprocessdate);
					dbms_output.put_line('2');
			end if;
			lastfilenum := -1;
		else	--file name length ok
			curfilenum := get_number("file".filename,rFormatRow.checkpos,rFormatRow.checklen);
			if lastfilenum is null then
				lastfilenum := curfilenum;
				dbms_output.put_line('3');
			else	--last file num exists
				if (lastfilenum <> -1) and ( curfilenum = mod(lastfilenum + 1,
						power(10,rFormatRow.filenamelen - rFormatRow.checkpos + 1)) ) then
					lastfilenum := curfilenum;
					dbms_output.put_line('4');
				else -- error found
					insert into filelog_check(nettype,errortype,filename1,
						processtime1,filename2,processtime2)
					values(rFormatRow.nettype,'S',lastfile.filename,
						lastfile.beginprocessdate,"file".filename,"file".beginprocessdate);
					lastfilenum := curfilenum;
					dbms_output.put_line('5');
				end if;
			end if;
		end if;
		lastfile := "file";
	end loop;

	-- ��������������������2����������������������
    if lastfile.filename is not null then
    	update cdrfilename_format
    	 set lastfilename = lastfile.filename, lastnamecheckfiletime = lastfile.endprocessdate,
          lastchecktime=to_date('20201021','yyyymmdd')
    	 where nettype = rFormatRow.nettype and filenameformat = rFormatRow.filenameformat;
		 dbms_output.put_line('6');
    end if;

	commit;
	return;
	exception when others then
		dbms_output.put_line('error in check gsm file log.');
		dbms_output.put_line(sqlerrm);
		rollback;
		return;
end;
/*=====================================================================================================*/
procedure checkfilelog
is
	cursor curCdrFileNameFormat is select * from cdrfilename_format;
	rFileNameFormat		cdrfilename_format%rowtype;
begin
	for "row" in curCdrFileNameFormat loop
		rFileNameFormat := "row";
		dbms_output.put_line("row".filenameformat);
		checkgsmfilelogbyname(rFileNameFormat);
		dbms_output.put_line('7');
	end loop;
exception when others then
	dbms_output.put_line('error in check filelog');
	return;
end;
/*=====================================================================================================*/
end;
  

$

  CREATE OR REPLACE PACKAGE BODY "PKG_CDRBALANCE_CHECK" is

procedure PROC_BYNAME(
file_name varchar2  --话单文件名
) as
v_sect_src      varchar2(64)    := '-';      --源阶段
v_sect_dst      varchar2(64)    := '-';      --目标阶段
v_sect_tmp      varchar2(64)    := '-';      --临时阶段,用来翻转源目标阶段用
v_log_field    varchar2(64)    := '-';       --event_flag对应的表中的字段
v_update_sql    varchar2(1024)  := '-';      --动态更新统计表的SQL
v_reverse_flag  number(1)       := 0;       --翻转标志,对某阶段为源的分流数据要翻转处理(重处理,话单回收)
v_calc_factor   number(1)       := 1;       --计算因子,对于翻转运算的分流,要将其数据翻转,即*(-1)
v_flow_type     number(4)       := 0;
v_event_flag    varchar2(20)    := '-';
v_sumcount		number(14)		:= 0;
v_cmp_result	number(1)		:= 0;--标志当前文件是否平衡
begin
    begin
        --清理历史数据
       delete cdr_balancecheck where filename=file_name;
       delete cdr_balancecheck_detail where filename=file_name;
	   dbms_output.put_line('1');
       commit;
   exception when others then
        rollback;
        raise_application_error(-20101,sqlerrm);
        return;
   end;
   --遍历分拣/漫游来访文件
   for c1 in(
            select filename,recordnum,stdcount,errcount,to_number(to_char(beginprocessdate,'yyyymmdd')) proc_date from sepfilelog
                where filename=file_name
            union
            select filename,billcount recordnum,billcount stdcount,errcount,to_number(to_char(processtime,'yyyymmdd')) proc_date from visittranslog
                where filename=file_name)
    loop
        begin
            --从分拣/漫游来访日志文件得到总量数据，插入相应表中
            insert into cdr_balancecheck(procdate,filename,recordnum,stdcount,errcount)
                values(c1.proc_date, c1.filename, c1.recordnum, c1.stdcount, c1.errcount);

            --从分流日志表统计分流数据，按模块，分流ID，分流类型，事件分类
            for flow_row in (select entry_module_id,module_id,flow_id,file_event_count sumcount
                                from v_event_flow_log_sourfile where source_name=c1.filename)
            loop
                 --取源阶段/目标阶段数据,如果源为空,则取模块号;如果目标为空,则取"模块号,规则号"
                select nvl(max(flow_section),flow_row.module_id) into v_sect_src
                    from event_flow_module where module_id=flow_row.entry_module_id;
                select event_flag,flow_type,nvl(next_section,flow_row.module_id||','||flow_row.flow_id)
                    into v_event_flag,v_flow_type,v_sect_dst
                        from event_flow_def where module_id=flow_row.module_id and flow_id=flow_row.flow_id;
                --判断源阶段是否是翻转阶段
                select count(*) into v_reverse_flag from code_dict
                    where catalog='T_SECTION_REVERSE' and code=v_sect_src;
                if(v_reverse_flag > 0) then
					if(v_event_flag='CHOME') then
						select case v_sect_src
							when '90_REPROC_EMPTY' then 'EMPTY'
							when '91_REPROC_ERROR' then 'ERROR'
							when '93_REPROC_NOCHARGE' then 'WAIFS'
							else 'CHOME' end into v_event_flag from dual;
							dbms_output.put_line('2');
					end if;
                    --实现阶段翻转
                    v_sect_tmp      := v_sect_dst;
                    v_sect_dst      := v_sect_src;
                    v_sect_src      := v_sect_tmp;
                    v_calc_factor   := -1;  --计算因子取-1,实现后面统计数据取反值
                else
                    v_calc_factor  := 1;
					dbms_output.put_line('3');
                end if;
                --取日志字段,动态组织SQL,更新数据到统计表
                begin
                    select logfield into v_log_field from EVENT_FLAG_DEF where code=v_event_flag;
                    v_update_sql := 'update CDR_BALANCECHECK_DETAIL set '
                        ||v_log_field||'='||v_log_field||'+'||flow_row.sumcount*(v_calc_factor)||' '
                        ||'where procdate='''||c1.proc_date||''' and filename='''||c1.filename||''' '
                        ||'and sect_src='''||v_sect_src||''' and sect_dst='''||v_sect_dst||''' '
                        ||'and flow_type='||v_flow_type;
                    execute immediate(v_update_sql);
                    --如果目标记录不存在，则先增加记录再更新
                    if sql%rowcount=0 then
                         insert into CDR_BALANCECHECK_DETAIL(procdate,filename,sect_src,sect_dst,flow_type)
                                values(c1.proc_date,c1.filename, v_sect_src, v_sect_dst, v_flow_type);
                         execute immediate(v_update_sql);
						 dbms_output.put_line('4');
                    end if;
                exception when others then
                    null;
                end;
            end loop;
        	select nvl(sum(HOMECOUNT),0)+nvl(sum(EMPTYCOUNT),0)+nvl(sum(ERRORCOUNT),0)
				+nvl(sum(RMHOMECOUNT),0)+nvl(sum(RMEMPTYCOUNT),0)+nvl(sum(RMERRORCOUNT),0)
				+nvl(sum(NOCHARGECOUNT),0)+nvl(sum(REJECTCOUNT),0)
				 into v_sumcount from CDR_BALANCECHECK_DETAIL
        	where procdate=c1.proc_date and filename=c1.filename and flow_type=1
            	and sect_dst in(select code from code_dict where catalog='T_SECTION_TERMS');
			if(v_sumcount = c1.stdcount) then
				v_cmp_result := 1;
				dbms_output.put_line('5');
			else
				v_cmp_result := 0;
				dbms_output.put_line('6');
			end if;
			update cdr_balancecheck set finalcount=v_sumcount,isbalance=v_cmp_result
				where procdate=c1.proc_date and filename=c1.filename;
            commit;
        exception when others then
            rollback;
            raise_application_error(-20101,sqlerrm);
            return;
        end;
    end loop;
end PROC_BYNAME;

function  FILENAME_MATCH(file_name varchar2, service_code varchar2)
return number is
	v_tmp_pos number(4);
	v_servicetype varchar2(32);
	v_service_code varchar2(2048);
begin
	if(service_code='-1') then
	dbms_output.put_line('7');
		return 1;
	end if;
	v_service_code := service_code;
		--遍历判断配置表中是否存在匹配的业务类型
		for c1 in (select filenameformat,servicetype from servicetype_format
			where file_name like filenameformat and inuse=1 order by filenameformat desc)
		loop
			--如果匹配成功，看是否有符合的业务类型
			v_tmp_pos := instr(v_service_code,',');
			while(v_tmp_pos > 0)
			loop
				--遍历取出每个业务类型
				v_servicetype := substr(v_service_code, 1, v_tmp_pos-1);
				--匹配到业务类型，返回1
				if(v_servicetype=c1.servicetype) then
				dbms_output.put_line('8');
					return 1;
				end if;
				v_service_code := substr(v_service_code,v_tmp_pos+1);
				v_tmp_pos := instr(v_service_code,',');
			end loop;
			--如果未匹配到合适的业务类型，返回0
			dbms_output.put_line('9');
			return 0;
		end loop;
		return 0;
end FILENAME_MATCH;

procedure AUDIT_SHOW(
file_name varchar2, --文件名,带%模糊查询
sect_str varchar2,  --阶段串,"-1"汇总稽核,分阶段稽核串型如"'sect1','sect2','sect3'"
begindate number,   --开始日期
enddate number,      --结束日期
service_code varchar2
)
as
v_file_name varchar2(128);
v_tmp_sect  varchar2(1024) := '';
v_tmp_pos   number(4) := -1;
v_sect_param varchar2(1024) := '';
begin
  v_file_name := file_name;
  if(file_name is null) then
    v_file_name := '%';
	dbms_output.put_line('10');
  end if;
  if(trim(sect_str)='-1') then  --汇总稽核
    --遍历汇总表
    for c1 in(select * from CDR_BALANCECHECK where filename like v_file_name
		and  procdate>=begindate and procdate<=enddate
			and pkg_cdrbalance_check.FILENAME_MATCH(filename, service_code)=1)
    loop
        --针对汇总表记录,求其分阶段统计数据插入临时表
        insert into WORK_CDR_BALANCECHECK(procdate,filename,sect_code,all_count,
            HOMECOUNT,EMPTYCOUNT,ERRORCOUNT,RMHOMECOUNT,RMEMPTYCOUNT,RMERRORCOUNT,
            NOCHARGECOUNT,REJECTCOUNT,DIFFVAL)
        select c1.procdate,c1.filename,'-1',c1.stdcount,nvl(sum(HOMECOUNT),0),nvl(sum(EMPTYCOUNT),0),
			nvl(sum(ERRORCOUNT),0),nvl(sum(RMHOMECOUNT),0),nvl(sum(RMEMPTYCOUNT),0),nvl(sum(RMERRORCOUNT),0),
			nvl(sum(NOCHARGECOUNT),0),nvl(sum(REJECTCOUNT),0),c1.stdcount-(
			nvl(sum(HOMECOUNT),0)+nvl(sum(EMPTYCOUNT),0)+nvl(sum(ERRORCOUNT),0)+nvl(sum(RMHOMECOUNT),0)
			+nvl(sum(RMEMPTYCOUNT),0)+nvl(sum(RMERRORCOUNT),0)+nvl(sum(NOCHARGECOUNT),0)+nvl(sum(REJECTCOUNT),0))
		from CDR_BALANCECHECK_DETAIL
        where procdate=c1.procdate and filename=c1.filename and flow_type=1
            and sect_dst in(select code from code_dict where catalog='T_SECTION_TERMS');
			dbms_output.put_line('11');
    end loop;
  else  --分阶段稽核
    v_tmp_sect  := sect_str;
    --如果v_tmp_sect不是以','结尾,则主动添加一个,方便下面的算法
    if(substr(v_tmp_sect,length(v_tmp_sect)-1,1)<>',') then
        v_tmp_sect := v_tmp_sect||',';
		dbms_output.put_line('12');
    end if;
    v_tmp_pos   := instr(v_tmp_sect,',');
    while(v_tmp_pos > 0)
    loop
        v_sect_param := substr(v_tmp_sect,1,v_tmp_pos-1);
        --出口数据统计
        for c2 in(select procdate,filename,sect_src,nvl(sum(HOMECOUNT),0) HOMECOUNT,nvl(sum(EMPTYCOUNT),0) EMPTYCOUNT,
            nvl(sum(ERRORCOUNT),0) ERRORCOUNT,nvl(sum(RMHOMECOUNT),0) RMHOMECOUNT,nvl(sum(RMEMPTYCOUNT),0) RMEMPTYCOUNT,
            nvl(sum(RMERRORCOUNT),0) RMERRORCOUNT,nvl(sum(NOCHARGECOUNT),0) NOCHARGECOUNT,nvl(sum(REJECTCOUNT),0) REJECTCOUNT
            from CDR_BALANCECHECK_DETAIL where procdate>=begindate and procdate<=enddate and filename like v_file_name
            and sect_src=v_sect_param and flow_type=1
			and pkg_cdrbalance_check.FILENAME_MATCH(filename, service_code)=1
			group by procdate,filename,sect_src)
        loop
            insert into WORK_CDR_BALANCECHECK(procdate,filename,sect_code,all_count,
              HOMECOUNT,EMPTYCOUNT,ERRORCOUNT,RMHOMECOUNT,RMEMPTYCOUNT,RMERRORCOUNT,
              NOCHARGECOUNT,REJECTCOUNT,DIFFVAL)
            values(c2.procdate,c2.filename,c2.sect_src,0,c2.HOMECOUNT,c2.EMPTYCOUNT,c2.ERRORCOUNT,c2.RMHOMECOUNT,c2.RMEMPTYCOUNT,
            c2.RMERRORCOUNT,c2.NOCHARGECOUNT,c2.REJECTCOUNT,(c2.HOMECOUNT+c2.EMPTYCOUNT+c2.ERRORCOUNT+c2.RMHOMECOUNT+c2.RMEMPTYCOUNT+
            c2.RMERRORCOUNT+c2.NOCHARGECOUNT+c2.REJECTCOUNT)*(-1));
			dbms_output.put_line('13');
        end loop;
		--入口数据更新
		--首阶段入口特殊处理
        if(v_sect_param='01_SECTION_SEP') then
            for c3 in(select * from CDR_BALANCECHECK
                    where procdate>=begindate and procdate<=enddate and filename like v_file_name
						and pkg_cdrbalance_check.FILENAME_MATCH(filename, service_code)=1)
            loop
                update WORK_CDR_BALANCECHECK set all_count=c3.stdcount,diffval=diffval+c3.stdcount
                    where procdate=c3.procdate and filename=c3.filename and sect_code='01_SECTION_SEP';
					dbms_output.put_line('14');
            end loop;
        end if;
		--其他入口数据更新
        for c1 in(select procdate,filename,sect_dst,nvl(sum(HOMECOUNT),0)+nvl(sum(EMPTYCOUNT),0)+nvl(sum(ERRORCOUNT),0)+
        nvl(sum(RMHOMECOUNT),0)+nvl(sum(RMEMPTYCOUNT),0)+nvl(sum(RMERRORCOUNT),0)+nvl(sum(NOCHARGECOUNT),0)+nvl(sum(REJECTCOUNT),0)
        all_count from CDR_BALANCECHECK_DETAIL where filename like v_file_name and procdate>=begindate
        and procdate<=enddate and sect_dst=v_sect_param
		and pkg_cdrbalance_check.FILENAME_MATCH(filename, service_code)=1
        group by procdate,filename,sect_dst)
        loop
            update WORK_CDR_BALANCECHECK set all_count=c1.all_count,diffval=diffval+c1.all_count
                where procdate=c1.procdate and filename=c1.filename and sect_code=c1.sect_dst;
				dbms_output.put_line('15');
        end loop;
        v_tmp_sect   := substr(v_tmp_sect,v_tmp_pos+1);
        v_tmp_pos    := instr(v_tmp_sect,',');
    end loop;
  end if;
end AUDIT_SHOW;

procedure PROC_BYDATE(
proc_date varchar2  --处理日期,型如'20050601',不带时间
) as
v_sect_src      varchar2(64)    := '-';      --源阶段
v_sect_dst      varchar2(64)    := '-';      --目标阶段
v_sect_tmp      varchar2(64)    := '-';      --临时阶段,用来翻转源目标阶段用
v_log_field    varchar2(64)    := '-';       --event_flag对应的表中的字段
v_update_sql    varchar2(1024)  := '-';      --动态更新统计表的SQL
v_reverse_flag  number(1)       := 0;       --翻转标志,对某阶段为源的分流数据要翻转处理(重处理,话单回收)
v_calc_factor   number(1)       := 1;       --计算因子,对于翻转运算的分流,要将其数据翻转,即*(-1)
v_flow_type     number(4)       := 0;
v_event_flag    varchar2(20)    := '-';
v_sumcount		number(14)		:= 0;
v_cmp_result	number(1)		:= 0;	--标志当前文件是否平衡
begin
    begin
        --清理历史数据
       delete cdr_balancecheck where procdate=proc_date;
       delete cdr_balancecheck_detail where procdate=proc_date;
	   dbms_output.put_line('16');
       commit;
   exception when others then
        rollback;
        raise_application_error(-20101,sqlerrm);
        return;
   end;
   --遍历分拣/漫游来访文件
   for c1 in(
            select filename,recordnum,stdcount,errcount from sepfilelog
                where trunc(beginprocessdate)=to_date(proc_date,'yyyymmdd')
            union
            select filename,billcount recordnum,billcount stdcount,errcount from visittranslog
                where trunc(processtime)=to_date(proc_date,'yyyymmdd'))
    loop
        begin
            --从分拣/漫游来访日志文件得到总量数据，插入相应表中
            insert into cdr_balancecheck(procdate,filename,recordnum,stdcount,errcount)
                values(proc_date, c1.filename, c1.recordnum, c1.stdcount, c1.errcount);

            --从分流日志表统计分流数据，按模块，分流ID，分流类型，事件分类
            for flow_row in (select entry_module_id,module_id,flow_id,file_event_count sumcount
                                from v_event_flow_log_sourfile where source_name=c1.filename)
            loop
                 --取源阶段/目标阶段数据,如果源为空,则取模块号;如果目标为空,则取"模块号,规则号"
                select nvl(max(flow_section),flow_row.module_id) into v_sect_src
                    from event_flow_module where module_id=flow_row.entry_module_id;
                select event_flag,flow_type,nvl(next_section,flow_row.module_id||','||flow_row.flow_id)
                    into v_event_flag,v_flow_type,v_sect_dst
                        from event_flow_def where module_id=flow_row.module_id and flow_id=flow_row.flow_id;
                --判断源阶段是否是翻转阶段
                select count(*) into v_reverse_flag from code_dict
                    where catalog='T_SECTION_REVERSE' and code=v_sect_src;
                if(v_reverse_flag > 0) then
					if(v_event_flag='CHOME') then
						select case v_sect_src
							when '90_REPROC_EMPTY' then 'EMPTY'
							when '91_REPROC_ERROR' then 'ERROR'
							when '93_REPROC_NOCHARGE' then 'WAIFS'
							else 'CHOME' end into v_event_flag from dual;
							dbms_output.put_line('17');
					end if;
                    --实现阶段翻转
                    v_sect_tmp      := v_sect_dst;
                    v_sect_dst      := v_sect_src;
                    v_sect_src      := v_sect_tmp;
                    v_calc_factor   := -1;  --计算因子取-1,实现后面统计数据取反值
                else
                    v_calc_factor  := 1;
					dbms_output.put_line('18');
                end if;
                --取日志字段,动态组织SQL,更新数据到统计表
                begin
                    select logfield into v_log_field from EVENT_FLAG_DEF where code=v_event_flag;
                    v_update_sql := 'update CDR_BALANCECHECK_DETAIL set '
                        ||v_log_field||'='||v_log_field||'+'||flow_row.sumcount*(v_calc_factor)||' '
                        ||'where procdate='''||proc_date||''' and filename='''||c1.filename||''' '
                        ||'and sect_src='''||v_sect_src||''' and sect_dst='''||v_sect_dst||''' '
                        ||'and flow_type='||v_flow_type;
                    execute immediate(v_update_sql);
                    --如果目标记录不存在，则先增加记录再更新
                    if sql%rowcount=0 then
                         insert into CDR_BALANCECHECK_DETAIL(procdate,filename,sect_src,sect_dst,flow_type)
                                values(proc_date,c1.filename, v_sect_src, v_sect_dst, v_flow_type);
                         execute immediate(v_update_sql);
						 dbms_output.put_line('19');
                    end if;
                exception when others then
                    null;
                end;
            end loop;
        	select nvl(sum(HOMECOUNT),0)+nvl(sum(EMPTYCOUNT),0)+nvl(sum(ERRORCOUNT),0)
				+nvl(sum(RMHOMECOUNT),0)+nvl(sum(RMEMPTYCOUNT),0)+nvl(sum(RMERRORCOUNT),0)
				+nvl(sum(NOCHARGECOUNT),0)+nvl(sum(REJECTCOUNT),0)
				 into v_sumcount from CDR_BALANCECHECK_DETAIL
        	where procdate=proc_date and filename=c1.filename and flow_type=1
            	and sect_dst in(select code from code_dict where catalog='T_SECTION_TERMS');
			if(v_sumcount = c1.stdcount) then
				v_cmp_result := 1;
				dbms_output.put_line('20');
			else
				v_cmp_result := 0;
				dbms_output.put_line('21');
			end if;
			update cdr_balancecheck set finalcount=v_sumcount,isbalance=v_cmp_result
				where procdate=proc_date and filename=c1.filename;
			commit;
        exception when others then
            rollback;
            raise_application_error(-20101,sqlerrm);
            return;
        end;
    end loop;
end PROC_BYDATE;

end PKG_CDRBALANCE_CHECK;



$

  CREATE OR REPLACE PACKAGE BODY "PKG_CHECK_PARAM" is

/*  --创建临时表
  create global temporary table wrk_check_param_infotab(
    serialno number(8),
    content varchar2(1024)
  )
  on commit preserve rows;
*/
  g_serialno number(8) := 0; --序列号

  PROCEDURE ap_check_param(a_result out varchar2) is
  v_ret number(1);
  v_count number(1);
  v_sql varchar2(256);
  begin
      a_result := '1';
      v_ret := 1;
      --检查存放错误信息表是否存在
      select count(*) into v_count from user_tables where table_name='WRK_CHECK_PARAM_INFOTAB';
      if v_count < 1 then
      begin
          v_sql := 'create global temporary table wrk_check_param_infotab(serialno number(8),content varchar2(2048)) on commit preserve rows';
          execute immediate v_sql;
		  dbms_output.put_line('1');
      exception when others then
          a_result := '-1';
          return;
      end;
      end if;

      --检查同一实体定义中是否有同一有效属性类对应多个属性
      v_ret := af_check_entity_class();

      if v_ret < 0 then
          a_result := '-1';
		  dbms_output.put_line('2');
          return;
      end if;

      if v_ret <> 1 then
          a_result := to_char(v_ret);
		  dbms_output.put_line('3');
      end if;

      --检查累计量属性在赠送量定义中有配置
      v_ret := af_check_cust_entity_attr();
      if v_ret <> 1 then
          a_result := to_char(v_ret);
		  dbms_output.put_line('4');
      end if;


      --检查赠送量属性在累计量定义中有配置

      --对计费事件的属性进行检查
      v_ret := af_check_chargeevent_continue();
      if v_ret <> 1 then
          a_result := to_char(v_ret);
		  dbms_output.put_line('5');
      end if;

      if a_result = 0 then
          dbms_output.put_line('parameter have problem, please check-look table wrk_check_param_infotab ');
		  dbms_output.put_line('6');
      end if;
  end;

  --检查同一个class对应一个实体的多个属性的情况
  function af_check_entity_class return number is
  v_result number(1);  --返回结果
  v_msg varchar2(1024);
  v_rst number(1);
  v_tmpserialno number(8);
  begin
    v_result := 1;

    --检查同一个class对应一个实体的多个属性的情况
    v_tmpserialno := g_serialno;
    g_serialno := g_serialno + 1;

    --取出有问题的实体号和属性号
    for c1 in (select entity_id, attr_class, attr_id from entity_attr_def where entity_id||attr_class in
        (select entity_id||attr_class from entity_attr_def where attr_class>0
          group by entity_id,attr_class having count(*)>1) order by entity_id,attr_class
    ) loop
    begin
        v_msg := '      entity_id='||to_char(c1.entity_id)||'  attr_class='||to_char(c1.attr_class)||'  attr_id='||to_char(c1.attr_id);

        v_rst := af_insert_data_tab(g_serialno, v_msg);
        if v_rst = -1 then
		dbms_output.put_line('7');
            return -1;
        end if;
        g_serialno := g_serialno + 1;
        v_result := 0;
    end;
    end loop;
    if v_result = 1 then
        dbms_output.put_line('In the entity_attr_def table, attr_class check-up have no problem.');
		dbms_output.put_line('8');
    else
        v_msg := 'A attribute class have many attribute in a entity, as follows:';
        v_rst := af_insert_data_tab(v_tmpserialno, v_msg);
        if v_rst = -1 then
		dbms_output.put_line('9');
            return -1;
        end if;
		dbms_output.put_line('10');
    end if;
    return v_result;
  end;

function af_check_cust_entity_attr return number is
	v_count number(8):= 0;
	v_result number(1);
  v_ret boolean;
	function checkit(a_row entity_attr_def%rowtype, a_result out number, a_info varchar2) return boolean
	is
  v_msg varchar2(1024);
  v_rst number(1);
	begin
      v_msg := 'entity_id='||a_row.entity_id||', attr_id='||a_row.attr_id||a_info;
      v_rst := af_insert_data_tab(g_serialno, v_msg);
      if v_rst = -1 then
	  dbms_output.put_line('11');
          return false;
      end if;
      g_serialno := g_serialno + 1;
      a_result := 0;
	  dbms_output.put_line('12');
			return true;
	end;
  begin
    v_result := 1;
    v_ret := true;
    --检查累计量属性在赠送量定义中有配置
	  for r in ( select * from entity_attr_def where attr_type=3 order by entity_id,attr_id )
	  loop
		  select count(*) into v_count from total_usage_attr_def where entity_id=r.entity_id and attr_id=r.attr_id;
      if v_count > 1 then
          v_ret := checkit(r, v_result, '  total usage attr has more than 1 defination in table total_usage_attr_def.');
		  dbms_output.put_line('13');
      end if;

      select count(*) into v_count from free_usage_attr_def where entity_id=r.entity_id and attr_id=r.attr_id;
      if v_count > 0 then
          v_ret := checkit(r, v_result, '  total usage attr has defination in table free_usage_attr_def.');
		  dbms_output.put_line('14');
      end if;
      if v_ret = false then
	  dbms_output.put_line('15');
          return -1;
      end if;
	  end loop;

  --检查赠送量属性在累计量定义中有配置
	  for r in ( select * from entity_attr_def where attr_type=4 order by entity_id,attr_id )
	  loop
      select count(*) into v_count from free_usage_attr_def where entity_id=r.entity_id and attr_id=r.attr_id;
      if v_count > 1 then
          v_ret := checkit(r, v_result, '  free usage attr has more than 1 defination in table free_usage_attr_def.');
		  dbms_output.put_line('16');
      end if;

      select count(*) into v_count from total_usage_attr_def where entity_id=r.entity_id and attr_id=r.attr_id;
      if v_count > 0 then
          v_ret := checkit(r, v_result, '  free usage attr has defination in table total_usage_attr_def.');
		  dbms_output.put_line('17');
      end if;
      if v_ret = false then
	  dbms_output.put_line('18');
          return -1;
      end if;

	  end loop;
    return v_result;
  end;


  --对计费事件的属性进行检查
  function af_check_chargeevent_continue return number is
  v_result number(1);
  v_rst number(1);
  --v_totallocation number(6); --3R1C01B05 AR.FUNC.004.002 z39863 2007-12-21 delete
  v_msg varchar2(1024);
  v_tmpserialno number(8);
  begin
    v_result := 1;
    --V3R1C01B05 AR.FUNC.004.002 z39863 2007-12-21 delete begin
    /*
    v_tmpserialno := g_serialno;
    g_serialno := g_serialno + 1;
    for c1 in (select * from charging_event_def order by event_id) loop
    begin
        --对计费事件的属性进行处理，
        v_totallocation := 3;

        for c2 in (select * from event_attr_def where event_id = c1.event_id order by event_id,attr_id) loop
        begin
            --比较，若不相等，则返回attr_id和位置
            if v_totallocation <> c2.position then
                v_msg := '      event_id='||to_char(c2.event_id)||',  attr_id='||to_char(c2.attr_id);

                --写表
                v_rst := af_insert_data_tab(g_serialno, v_msg);
                if v_rst = -1 then
                    return -1;
                end if;
                g_serialno := g_serialno + 1;

                v_result := 0;
                exit;
            end if;
            v_totallocation := c2.position + c2.length;
        end ;
        end loop;
    end;
    end loop;
    */
    --V3R1C01B05 AR.FUNC.004.002 z39863 2007-12-21 delete end
    --检查没有问题，打印正确的提示，否则返回值为0
    if v_result = 1 then
        dbms_output.put_line('Chaging event definition have no problem.');
		dbms_output.put_line('19');
    else
        v_msg := 'Check charging_event attribute continuity ，event problem as follows:';
        v_rst := af_insert_data_tab(v_tmpserialno, v_msg);
        if v_rst = -1 then
		dbms_output.put_line('20');
            return -1;
        end if;
    end if;
    return v_result;
  end;

  function af_insert_data_tab(a_serialno number, a_msg varchar2) return number is
  begin
      insert into wrk_check_param_infotab(serialno , content) values(a_serialno, a_msg);
	  dbms_output.put_line('21');
      return 1;
  exception when others then
      return -1;
  end;

end pkg_check_param;

$


 CREATE OR REPLACE PACKAGE "PKG_DATAFILE" is

  -- Author  : created by w39883
  -- Created : 2006-1-6 19:28:12
  -- Purpose : 局数据相关处理包
procedure DATAPROC(filetype number,v_result in out varchar2);
procedure DATA2DB(filetype number,applyDate varchar2,v_result in out varchar2);

procedure PROC_MAPPING_LIST_SP(v_map_id number, v_result in out varchar2);
procedure DB_MAPPING_LIST_SP(v_map_id number, v_result  in out  varchar2);

procedure PROC_MAPPING_LIST_OPER(v_map_id number, v_result in out varchar2);
procedure DB_MAPPING_LIST_OPER(v_map_id number, v_result  in out  varchar2);

procedure PROC_MAPPING_LIST_SNSP(v_map_id number, v_result  in out  varchar2);
procedure DB_MAPPING_LIST_SNSP(v_map_id number, v_result  in out  varchar2);


procedure PROC_HIGHCOSTEXCHANGERATE(v_result  in out  varchar2);
procedure DB_HIGHCOSTEXCHANGERATE(v_result  in out  varchar2);

end PKG_DATAFILE;

$

  CREATE OR REPLACE PACKAGE BODY "PKG_DATAFILE" is
v_note          varchar2(200);
v_ret_imp       varchar2(100) := '导入记录条数：';
v_ret_new       varchar2(100) := '，新增记录条数：';
v_ret_old       varchar2(100) := '，已存在记录数：';
v_ret_diff      varchar2(100) := '，不一致记录数：';
v_ret_del       varchar2(100) := '，禁用记录条数：';
v_ret_update    varchar2(100) := '，更新记录条数：';
v_ret_dup       varchar2(100) := '，重复纪录条数：';
v_ret_ask   varchar2(100) := '要求处理记录条数：';
v_ret_done   varchar2(100) := '，正确处理记录条数：';
v_ret_error varchar2(100) := '，未处理记录条数：';
v_ret_delete varchar2(100) := '，删除记录条数：';

procedure DATAPROC(filetype number,v_result  in out  varchar2) as
/*
    基本流程（单表更新）：
        遍历临时表记录
            判断数据是否存在，如果不存在（或标志状态为禁用）标记为新增记录1，直接插入数据
            如果数据已存在，判断数据是否一致，如果不一致标记为不一致记录3，更新数据
            如果数据一致，标记为已存在2，不做进一步处理
*/
begin
    case
        when filetype=1 then PROC_MAPPING_LIST_SP(60,v_result);    --梦网短信企业局数据
		dbms_output.put_line('1');
        when filetype=3 then PROC_MAPPING_LIST_SP(62,v_result);    --WAP企业局数据
		dbms_output.put_line('2');
        when filetype=5 then PROC_MAPPING_LIST_SP(64,v_result);    --PDA企业局数据     
        dbms_output.put_line('3');
		when filetype=7 then PROC_MAPPING_LIST_SP(66,v_result);    --MMS企业局数据
        dbms_output.put_line('4');
		when filetype=9 then PROC_MAPPING_LIST_SP(68,v_result);    --KJAVA企业局数据
        dbms_output.put_line('5');
		when filetype=11 then PROC_MAPPING_LIST_SP(73,v_result);    --流媒体企业局数据
        dbms_output.put_line('6');
		when filetype=13 then PROC_MAPPING_LIST_SP(75,v_result);    --手机动画企业局数据
        dbms_output.put_line('7');
		when filetype=15 then PROC_MAPPING_LIST_SP(77,v_result);    --自有短信企业局数据
        dbms_output.put_line('8');
		when filetype=17 then PROC_MAPPING_LIST_SP(79,v_result);    --捐款短信企业局数据
        dbms_output.put_line('9');
		when filetype=19 then PROC_MAPPING_LIST_SP(81,v_result);    --手机邮箱企业局数据
dbms_output.put_line('10');
        when filetype=21 then PROC_MAPPING_LIST_SNSP(83,v_result);    --省内梦网短信企业局数据
        dbms_output.put_line('11');
		when filetype=23 then PROC_MAPPING_LIST_SNSP(84,v_result);    --省内梦网彩信企业局数据
        dbms_output.put_line('12');
		when filetype=25 then PROC_MAPPING_LIST_SNSP(85,v_result);    --省内WAP企业局数据
dbms_output.put_line('13');
        when filetype=22 then PROC_MAPPING_LIST_SNSP(-83,v_result);    --省内梦网短信业务局数据
        dbms_output.put_line('14');
		when filetype=24 then PROC_MAPPING_LIST_SNSP(-84,v_result);    --省内梦网彩信业务局数据
        dbms_output.put_line('15');
		when filetype=26 then PROC_MAPPING_LIST_SNSP(-85,v_result);    --省内WAP业务局数据
        dbms_output.put_line('16');
        when filetype=2 then PROC_MAPPING_LIST_OPER(61,v_result);    --梦网短信业务局数据
        dbms_output.put_line('17');
		when filetype=4 then PROC_MAPPING_LIST_OPER(63,v_result);    --WAP业务局数据
        dbms_output.put_line('18');
		when filetype=6 then PROC_MAPPING_LIST_OPER(65,v_result);    --PDA业务局数据     
        dbms_output.put_line('19');
		when filetype=8 then PROC_MAPPING_LIST_OPER(67,v_result);    --MMS业务局数据
        dbms_output.put_line('20');
		when filetype=10 then PROC_MAPPING_LIST_OPER(69,v_result);    --KJAVA业务局数据
        dbms_output.put_line('21');
		when filetype=12 then PROC_MAPPING_LIST_OPER(74,v_result);    --流媒体业务局数据
        dbms_output.put_line('22');
		when filetype=14 then PROC_MAPPING_LIST_OPER(76,v_result);    --手机动画业务局数据
       dbms_output.put_line('23');
		when filetype=16 then PROC_MAPPING_LIST_OPER(78,v_result);    --自有短信业务局数据
        dbms_output.put_line('24');
		when filetype=18 then PROC_MAPPING_LIST_OPER(80,v_result);    --捐款短信业务局数据
        dbms_output.put_line('25');
		when filetype=20 then PROC_MAPPING_LIST_OPER(82,v_result);    --手机邮箱业务局数据
dbms_output.put_line('26');
        when filetype=50 then PROC_HIGHCOSTEXCHANGERATE(v_result);    --高额汇率
        dbms_output.put_line('27');
        else v_result := '未找到该类型文件处理方法，请确认已定义';
		dbms_output.put_line('28');
    end case;
   
end;
procedure DATA2DB(filetype number,applydate varchar2,v_result  in out  varchar2) as
/*
    基本流程（单表更新）：
        遍历临时表记录（STATUS in（1，2）；PROCFLAG=0)
            如果是新增记录，判断是否禁用，
                是则更新失效日期使其禁用
                否则直接插入记录数据
            如果是更新记录，则更新记录数据
            操作成功，当前记录PROCFLAG置为1
*/
begin
    case
        --以下数据业务企业局数据同步：
        when filetype=1 then DB_MAPPING_LIST_SP(60,v_result);    --梦网短信企业局数据
        dbms_output.put_line('29');
		when filetype=3 then DB_MAPPING_LIST_SP(62,v_result);    --WAP企业局数据
        dbms_output.put_line('30');
		when filetype=5 then DB_MAPPING_LIST_SP(64,v_result);    --PDA企业局数据     
        dbms_output.put_line('31');
		when filetype=7 then DB_MAPPING_LIST_SP(66,v_result);    --MMS企业局数据
        dbms_output.put_line('32');
		when filetype=9 then DB_MAPPING_LIST_SP(68,v_result);    --KJAVA企业局数据
        dbms_output.put_line('33');
		when filetype=11 then DB_MAPPING_LIST_SP(73,v_result);    --流媒体企业局数据
        dbms_output.put_line('34');
		when filetype=13 then DB_MAPPING_LIST_SP(75,v_result);    --手机动画企业局数据
        dbms_output.put_line('35');
		when filetype=15 then DB_MAPPING_LIST_SP(77,v_result);    --自有短信企业局数据
        dbms_output.put_line('36');
		when filetype=17 then DB_MAPPING_LIST_SP(79,v_result);    --捐款短信企业局数据
        dbms_output.put_line('37');
		when filetype=19 then DB_MAPPING_LIST_SP(81,v_result);    --手机邮箱企业局数据
dbms_output.put_line('38');
        when filetype=21 then DB_MAPPING_LIST_SNSP(83,v_result);    --省内梦网短信企业局数据
        dbms_output.put_line('39');
		when filetype=23 then DB_MAPPING_LIST_SNSP(84,v_result);    --省内梦网彩信企业局数据
        dbms_output.put_line('40');
		when filetype=25 then DB_MAPPING_LIST_SNSP(85,v_result);    --省内WAP企业局数据
        dbms_output.put_line('41');
		when filetype=22 then DB_MAPPING_LIST_SNSP(-83,v_result);    --省内梦网短信企业局数据
       dbms_output.put_line('42');
		when filetype=24 then DB_MAPPING_LIST_SNSP(-84,v_result);    --省内梦网彩信企业局数据
        dbms_output.put_line('43');
		when filetype=26 then DB_MAPPING_LIST_SNSP(-85,v_result);    --省内WAP企业局数据
dbms_output.put_line('44');
        --以下数据业务业务局数据都是全量同步：
        when filetype=2 then DB_MAPPING_LIST_OPER(61, v_result);    --梦网短信业务局数据
        dbms_output.put_line('45');
		when filetype=4 then DB_MAPPING_LIST_OPER(63, v_result);    --WAP业务局数据
        dbms_output.put_line('46');
		when filetype=6 then DB_MAPPING_LIST_OPER(65, v_result);    --PDA业务局数据    
        dbms_output.put_line('47');
		when filetype=8 then DB_MAPPING_LIST_OPER(67, v_result);    --MMS业务局数据
        dbms_output.put_line('48');
		when filetype=10 then DB_MAPPING_LIST_OPER(69, v_result);    --KJAVA业务局数据
        dbms_output.put_line('49');
		when filetype=12 then DB_MAPPING_LIST_OPER(74,v_result);    --流媒体业务局数据
        dbms_output.put_line('50');
		when filetype=14 then DB_MAPPING_LIST_OPER(76,v_result);    --手机动画业务局数据
        dbms_output.put_line('51');
		when filetype=16 then DB_MAPPING_LIST_OPER(78,v_result);    --自有短信业务局数据
        dbms_output.put_line('52');
		when filetype=18 then DB_MAPPING_LIST_OPER(80,v_result);    --捐款短信业务局数据
        dbms_output.put_line('53');
		when filetype=20 then DB_MAPPING_LIST_OPER(82,v_result);    --手机邮箱业务局数据
        dbms_output.put_line('54');
		--when filetype=1 then DB_SET_VALUE_SP(applydate, v_result);    --梦网SP局数据
        /*
        when filetype=22 then DB_MAPPING_LIST_SNOPER(83,v_result);    --省内梦网短信业务局数据
        when filetype=24 then DB_MAPPING_LIST_SNOPER(84,v_result);    --省内梦网彩信业务局数据
        when filetype=26 then DB_MAPPING_LIST_SNOPER(85,v_result);    --省内WAP业务局数据
        */
        when filetype=50 then DB_HIGHCOSTEXCHANGERATE(v_result);    --高额汇率
        dbms_output.put_line('55');
		else    v_result := '未找到该类型文件处理方法，请确认已定义';
		dbms_output.put_line('56');
    end case;
end;



procedure PROC_MAPPING_LIST_SP(v_map_id number, v_result  in out  varchar2) as
v_spcode        varchar2(100);
v_spname        varchar2(100);
v_provcode      varchar2(100);
v_hprovcode     varchar2(100);
v_gateway       varchar2(100);
v_servcode      varchar2(100);
v_applytime     varchar2(100);
v_expiretime    varchar2(100);

v_sour          varchar2(200);
v_dest          varchar2(200);

v_count         number(6) := 0;
v_delnum        number(6) := 0;
v_newnum        number(6) := 0;
v_oldnum        number(6) := 0;
v_diffnum       number(6) := 0;

begin
    for c1 in(select F1,F2,F3,F4,F5,F6,F7,F8,F9,rowid from WORK_DATAFILE where STATUS=0 order by ID)
    loop
        if (v_map_id=60 or v_map_id=66 or v_map_id=79 or v_map_id=81)  then--梦网短信,mms,捐款短信(PM),手机邮箱(YX)企业局数据
            v_spcode     := c1.F1;
            v_spname     := c1.F2;
            v_provcode   := c1.F3;
            v_gateway    := c1.F4;
            v_servcode   := c1.F5;
            v_applytime  := c1.F6;
            v_expiretime := c1.F7;
            v_sour :=v_spcode || ',' || v_servcode;
            v_dest :='1,'|| v_spname|| ','|| v_provcode;            
            if(v_applytime is null) then
                v_applytime := to_char(to_date('20201021','yyyymmdd'),'yyyy-mm-dd hh24:mi:ss');
                update work_datafile set F6=v_applytime where rowid=c1.rowid;
				dbms_output.put_line('57');
            end if;
            if((v_expiretime is null) or (to_date(v_expiretime,'yyyy-mm-dd hh24:mi:ss')>to_date('2037-01-01','yyyy-mm-dd'))) then
                --v_expiretime := '20370101000000';
                v_expiretime := null;
                update work_datafile set F7=v_expiretime where rowid=c1.rowid;
				dbms_output.put_line('58');
            end if;
        
        elsif (v_map_id=62 or v_map_id=64 or v_map_id=75) then	--wap,pda,flh企业局数据
            v_spcode     := c1.F1;
            v_spname     := c1.F2;
            v_applytime  := c1.F3;
            v_expiretime := c1.F4;
            v_sour :=v_spcode;
            v_dest :='1,'|| v_spname|| ','|| v_provcode;            
            
            if(v_applytime is null) then
               v_applytime := to_char(to_date('20201021','yyyymmdd'),'yyyy-mm-dd hh24:mi:ss');
               update work_datafile set F3=v_applytime where rowid=c1.rowid;
			   dbms_output.put_line('59');
            end if;
            if((v_expiretime is null) or (to_date(v_expiretime,'yyyy-mm-dd hh24:mi:ss')>to_date('2037-01-01','yyyy-mm-dd'))) then
               --v_expiretime := '20370101000000';
               v_expiretime := null;
               update work_datafile set F4=v_expiretime where rowid=c1.rowid;
			   dbms_output.put_line('60');
            end if;       
      
        elsif (v_map_id=77) then   --自有短信(CM)    
            v_spcode     := c1.F1;
            v_spname     := c1.F2;
            v_provcode   := c1.F3;
            v_hprovcode   := c1.F4;
            v_gateway    := c1.F5;
            v_servcode   := c1.F6;
            v_applytime  := c1.F7;
            v_expiretime := c1.F8;
            
            v_sour :=v_spcode || ',' || v_servcode;
            v_dest :='1,'|| v_spname|| ','|| v_provcode|| ',';            
            
            if(v_applytime is null) then
                v_applytime := to_char(to_date('20201021','yyyymmdd'),'yyyy-mm-dd hh24:mi:ss');
                update work_datafile set F7=v_applytime where rowid=c1.rowid;
				dbms_output.put_line('61');
            end if;
            if((v_expiretime is null) or (to_date(v_expiretime,'yyyy-mm-dd hh24:mi:ss')>to_date('2037-01-01','yyyy-mm-dd'))) then
                --v_expiretime := '20370101000000';
                v_expiretime := null;
                update work_datafile set F8=v_expiretime where rowid=c1.rowid;
				dbms_output.put_line('62');
            end if;
            
        elsif (v_map_id=73) then   --流媒体企业局数据    
            v_spcode     := c1.F1;
            v_spname     := c1.F2;
            v_servcode   := c1.F3;
            v_provcode   := c1.F4;
            v_hprovcode  := c1.F5;
            v_applytime  := c1.F6;
            v_expiretime := c1.F7;
            
            v_sour :=v_spcode||','||v_servcode;
            v_dest :=v_spname|| ','|| v_provcode||',';            
            
            if(v_applytime is null) then
                v_applytime := to_char(to_date('20201021','yyyymmdd'),'yyyy-mm-dd hh24:mi:ss');
                update work_datafile set F6=v_applytime where rowid=c1.rowid;
				dbms_output.put_line('63');
            end if;
            if((v_expiretime is null) or (to_date(v_expiretime,'yyyy-mm-dd hh24:mi:ss')>to_date('2037-01-01','yyyy-mm-dd'))) then
                --v_expiretime := '20370101000000';
                v_expiretime := null;
                update work_datafile set F7=v_expiretime where rowid=c1.rowid;
				dbms_output.put_line('64');
            end if;    
            
        elsif (v_map_id=68) then   --Kjava企业局数据    
            v_spcode     := c1.F1;
            v_spname     := c1.F2;
            v_provcode   := c1.F3;
            v_gateway    := c1.F4;
            v_applytime  := c1.F5;
            v_expiretime := c1.F6;
            v_sour :=v_spcode;
            v_dest :='1,'|| v_spname|| ',';            
         
           if(v_applytime is null) then
               v_applytime := to_char(to_date('20201021','yyyymmdd'),'yyyy-mm-dd hh24:mi:ss');
               update work_datafile set F5=v_applytime where rowid=c1.rowid;
       			dbms_output.put_line('65');
           end if;
            if((v_expiretime is null) or (to_date(v_expiretime,'yyyy-mm-dd hh24:mi:ss')>to_date('2037-01-01','yyyy-mm-dd'))) then
                --v_expiretime := '20370101000000';
                v_expiretime := null;
                update work_datafile set F6=v_expiretime where rowid=c1.rowid;
				dbms_output.put_line('66');
            end if;
            
        else
             v_result := '未找到该映射id:' || v_map_id ||'，请确认已定义';
			 dbms_output.put_line('67');
             return;
        end if;
        --通过v_note传递参数存在标志，不作要求就是0，新增1，存在2，不一致3
        if (v_spcode is not null) then            
            select count(*) into v_count from mapping_list where mapping_id=v_map_id 
                and mapping_sour=v_sour;
                --and to_date('20201021','yyyymmdd') >=applytime and (to_date('20201021','yyyymmdd') <=expiretime or expiretime is null);
            if(v_count > 0)then          
                select count(*) into v_count from mapping_list where mapping_id=v_map_id 
                   and mapping_sour=v_sour
                   and mapping_dest=v_dest
                   and to_char(applytime,'yyyymmdd')=substr(v_applytime,1,8)
                   and (decode(expiretime,null,'20370101',to_char(expiretime,'yyyymmdd'))=
                   decode(v_expiretime,null,'20370101',substr(v_expiretime,1,8)));
                   --and to_date('20201021','yyyymmdd') >=applytime and (to_date('20201021','yyyymmdd') <=expiretime or expiretime is null);
                 if(v_count > 0)then
                    v_note := '2';
					dbms_output.put_line('68');
                else       
                   v_note := '3';  
dbms_output.put_line('69');				   
                end if;
                
            else
                v_note := '1';
				dbms_output.put_line('70');
            end if;
        else
            v_note := '0';
			dbms_output.put_line('71');
        end if;
       
        --更新参数标志
        if(substr(v_note,1,1)='2')  then
            update work_datafile set status=2,notes=v_note where rowid=c1.rowid;
            v_oldnum := v_oldnum+1;
			dbms_output.put_line('72');
        else
            if(substr(v_note,1,1)='3')  then
                update work_datafile set status=3,notes=v_note where rowid=c1.rowid;
                v_diffnum := v_diffnum+1;
				dbms_output.put_line('73');
            else              
                if(substr(v_note,1)<>'0')then
                    update work_datafile set status=1,notes=v_note where rowid=c1.rowid;
                    v_newnum := v_newnum+1;
					dbms_output.put_line('74');
                end if;
            end if;
        end if;
    end loop;
    v_result := v_ret_imp||(v_newnum+v_oldnum+v_diffnum+v_delnum)
               ||v_ret_new||v_newnum||v_ret_old||v_oldnum
               ||v_ret_diff||v_diffnum||v_ret_del||v_delnum;

end;


procedure DB_MAPPING_LIST_SP(v_map_id number, v_result  in out  varchar2) as
v_count         number(8);
v_dupnum        number(8);
v_note          varchar2(200);
v_sptype        varchar2(20);
begin
    
    case 
        when v_map_id=60 then v_note:='梦网短信企业局数据-'; v_sptype:='SMS';
		dbms_output.put_line('75');
        when v_map_id=62 then v_note:='WAP企业局数据-'; v_sptype:='WAP';
        dbms_output.put_line('76');
        when v_map_id=64 then v_note:='PDA企业局数据-'; v_sptype:='PDA';
        dbms_output.put_line('77');
        when v_map_id=66 then v_note:='MMS企业局数据-'; v_sptype:='MMS';
        dbms_output.put_line('78');
        when v_map_id=68 then v_note:='KJAVA企业局数据-'; v_sptype:='KJAVA';
        dbms_output.put_line('79');
        when v_map_id=73 then v_note:='流媒体企业局数据'; v_sptype:='STREAM';
        dbms_output.put_line('80');
        when v_map_id=75 then v_note:='手机动画企业局数据'; v_sptype:='FLASH';
        dbms_output.put_line('81');
        when v_map_id=77 then v_note:='自有短信企业局数据'; v_sptype:='CM';
        dbms_output.put_line('82');
        when v_map_id=79 then v_note:='捐款短信企业局数据'; v_sptype:='PM';
        dbms_output.put_line('83');
        when v_map_id=81 then v_note:='手机邮箱企业局数据'; v_sptype:='CX';
        dbms_output.put_line('84');
        
        else 
             v_result := '未找到该映射id:' || v_map_id ||'，请确认已定义';
        dbms_output.put_line('85');
             return;
    end case;

    delete from mapping_list where mapping_id=v_map_id;
    delete from STL_SP_CODE where sp_type=v_sptype;
    
    if (v_map_id=60  or v_map_id=66 or v_map_id=79 or v_map_id=81)  then--梦网短信,mms,捐款短信(PM),手机邮箱(YX)企业局数据
        insert into mapping_list (MAPPING_SOUR,MAPPING_DEST,APPLYTIME,EXPIRETIME,MAPPING_ID,NOTE)
            select a.F1||','||a.F5,  '1,'||a.F2||','||a.F3, 
            to_date(substr(a.F6,1,8),'yyyymmdd'), to_date(substr(a.F7,1,8),'yyyymmdd'),
            v_map_id, v_note||to_char(to_date('20201021','yyyymmdd'),'yyyymmddhh24miss') from WORK_DATAFILE a;
        --修改同步表
        
        insert into STL_SP_CODE (SP_CODE,SP_TYPE,SP_NAME,VPROV,SERV_CODE,
            STL_MODE,VALID_CYCLES,STL_LEVEL,VREGION,SERV_PHONE,
            FEE_TYPE,FEE_FUCTION,EFF_DATE,EXP_DATE,CYCLE1,CYCLE2,REMARK)
            select a.F1, v_sptype, a.F2, a.F3, a.F5,'1',1,1,'000',null,null,null, 
            to_date(substr(a.F6,1,8),'yyyymmdd'), to_date(substr(a.F7,1,8),'yyyymmdd'), null, null,
            v_note||to_char(to_date('20201021','yyyymmdd'),'yyyymmddhh24miss') from WORK_DATAFILE a;
        dbms_output.put_line('86');

    elsif (v_map_id=62 or v_map_id=64 or v_map_id=75) then	--wap,pda,flh企业局数据
        insert into mapping_list (MAPPING_SOUR,MAPPING_DEST,APPLYTIME,EXPIRETIME,MAPPING_ID,NOTE)
            select a.F1,  '1,'||a.F2||',', 
            to_date(substr(a.F3,1,8),'yyyymmdd'), to_date(substr(a.F4,1,8),'yyyymmdd'),
            v_map_id, v_note||to_char(to_date('20201021','yyyymmdd'),'yyyymmddhh24miss') from WORK_DATAFILE a;   
        --修改同步表
            
            
        insert into STL_SP_CODE (SP_CODE,SP_TYPE,SP_NAME,VPROV,SERV_CODE,
            STL_MODE,VALID_CYCLES,STL_LEVEL,VREGION,SERV_PHONE,
            FEE_TYPE,FEE_FUCTION,EFF_DATE,EXP_DATE,CYCLE1,CYCLE2,REMARK)
            select a.F1, v_sptype, a.F2, 0, null, '1',1,1, '000',null,null,null,
            to_date(substr(a.F3,1,8),'yyyymmdd'), to_date(substr(a.F4,1,8),'yyyymmdd'), null, null,
            v_note||to_char(to_date('20201021','yyyymmdd'),'yyyymmddhh24miss') from WORK_DATAFILE a;         
    dbms_output.put_line('87');
    elsif (v_map_id=77) then   --自有短信(CM)   
        insert into mapping_list (MAPPING_SOUR,MAPPING_DEST,APPLYTIME,EXPIRETIME,MAPPING_ID,NOTE)
            select a.F1||','||a.F6, '1,'||a.F2||','||a.F3||',',  
            to_date(substr(a.F7,1,8),'yyyymmdd'), to_date(substr(a.F8,1,8),'yyyymmdd'),
            v_map_id, v_note||to_char(to_date('20201021','yyyymmdd'),'yyyymmddhh24miss') from WORK_DATAFILE a;     
        --修改同步表
            
        insert into STL_SP_CODE (SP_CODE,SP_TYPE,SP_NAME,VPROV,SERV_CODE,
            STL_MODE,VALID_CYCLES,STL_LEVEL,VREGION,SERV_PHONE,
            FEE_TYPE,FEE_FUCTION,EFF_DATE,EXP_DATE,CYCLE1,CYCLE2,REMARK) 
            select a.F1, v_sptype, a.F2, a.F3, a.F6, '1',1,1, '000',null,null,null,
            to_date(substr(a.F7,1,8),'yyyymmdd'), to_date(substr(a.F8,1,8),'yyyymmdd'), null, null,
            v_note||to_char(to_date('20201021','yyyymmdd'),'yyyymmddhh24miss') from WORK_DATAFILE a; 
          dbms_output.put_line('88');
    elsif (v_map_id=73) then   --流媒体企业局数据  
        insert into mapping_list (MAPPING_SOUR,MAPPING_DEST,APPLYTIME,EXPIRETIME,MAPPING_ID,NOTE)
            select a.F1||','||a.F3, a.F2||','||a.F4||',',  
            to_date(substr(a.F6,1,8),'yyyymmdd'), to_date(substr(a.F7,1,8),'yyyymmdd'),
            v_map_id, v_note||to_char(to_date('20201021','yyyymmdd'),'yyyymmddhh24miss') from WORK_DATAFILE a;     
        --修改同步表
        insert into STL_SP_CODE (SP_CODE,SP_TYPE,SP_NAME,VPROV,SERV_CODE,
            STL_MODE,VALID_CYCLES,STL_LEVEL,VREGION,SERV_PHONE,
            FEE_TYPE,FEE_FUCTION,EFF_DATE,EXP_DATE,CYCLE1,CYCLE2,REMARK) 
            select a.F1, v_sptype, a.F2, a.F4,a.F3, '1',1,1, '000',null,null,null,
            to_date(substr(a.F6,1,8),'yyyymmdd'), to_date(substr(a.F7,1,8),'yyyymmdd'), null, null,
            v_note||to_char(to_date('20201021','yyyymmdd'),'yyyymmddhh24miss') from WORK_DATAFILE a;  
      dbms_output.put_line('89');      
    elsif (v_map_id=68) then--Kjava企业局数据   
        insert into mapping_list (MAPPING_SOUR,MAPPING_DEST,APPLYTIME,EXPIRETIME,MAPPING_ID,NOTE)
            select a.F1, '1,'||a.F2||',',  
            to_date(substr(a.F5,1,8),'yyyymmdd'), to_date(substr(a.F6,1,8),'yyyymmdd'),
            v_map_id, v_note||to_char(to_date('20201021','yyyymmdd'),'yyyymmddhh24miss') from WORK_DATAFILE a;     
        --修改同步表
    
        insert into STL_SP_CODE (SP_CODE,SP_TYPE,SP_NAME,VPROV,SERV_CODE,
            STL_MODE,VALID_CYCLES,STL_LEVEL,VREGION,SERV_PHONE,
            FEE_TYPE,FEE_FUCTION,EFF_DATE,EXP_DATE,CYCLE1,CYCLE2,REMARK)
            select a.F1, v_sptype, a.F2, a.F3, null, '1',1,1, '000',null,null,null,
            to_date(substr(a.F5,1,8),'yyyymmdd'), to_date(substr(a.F6,1,8),'yyyymmdd'), null, null,
            v_note||to_char(to_date('20201021','yyyymmdd'),'yyyymmddhh24miss') from WORK_DATAFILE a; 
			dbms_output.put_line('90');
    end if;

    select count(*) into v_count from WORK_DATAFILE;
    --删除重复纪录      
    delete from mapping_list a where 
        a.rowid!= (select max(b.rowid) from mapping_list b  
        where a.mapping_id=b.mapping_id and a.mapping_sour=b.mapping_sour and a.mapping_dest=b.mapping_dest
        and a.applytime=b.applytime and a.expiretime=b.expiretime) and a.mapping_id=v_map_id;
    
    v_dupnum :=sql%rowcount;
                     
    v_result := v_ret_imp||v_count||v_ret_update||(v_count-v_dupnum)
               ||v_ret_dup||v_dupnum ;
              
end;







procedure PROC_MAPPING_LIST_OPER(v_map_id number, v_result  in out  varchar2) as
v_spcode        varchar2(100);
v_channel       varchar2(100);
v_opercode      varchar2(100);
v_opername      varchar2(100);
v_feetype       varchar2(10);
v_fee           varchar2(20);
v_applytime     varchar2(100);
v_expiretime    varchar2(100);
v_rate          varchar2(20);
v_download      varchar2(20);

            
v_count         number(6) := 0;
v_delnum        number(6) := 0;
v_newnum        number(6) := 0;
v_oldnum        number(6) := 0;
v_diffnum       number(6) := 0;

begin
    for c1 in(select F1,F2,F3,F4,F5,F6,F7,F8,F9,rowid from WORK_DATAFILE where STATUS=0 order by ID)
    loop
        if (v_map_id=61 or v_map_id=63 or v_map_id=67 or v_map_id=78)  then--梦网短信,wap,mms,自有短信(CM)业务局数据
            v_spcode     := c1.F1;
            v_channel    := c1.F2;
            v_opercode   := c1.F3;
            v_opername   := c1.F4;
            v_feetype    := c1.F5;
            v_fee        := c1.F6;
            v_applytime  := c1.F7;
            v_expiretime := c1.F8;
            v_rate   := c1.F9;
        
            if(v_applytime is null) then
                v_applytime := to_char(to_date('20201021','yyyymmdd'),'yyyy-mm-dd hh24:mi:ss');
                update work_datafile set F7=v_applytime where rowid=c1.rowid;
				dbms_output.put_line('91');
            end if;
            if((v_expiretime is null) or (to_date(v_expiretime,'yyyy-mm-dd hh24:mi:ss')>to_date('2037-01-01','yyyy-mm-dd'))) then
            --v_expiretime := '20370101000000';
                v_expiretime := null;
                update work_datafile set F8=v_expiretime where rowid=c1.rowid;
				dbms_output.put_line('92');
            end if;
            --修正v_feetype为双字符串
            if(v_feetype is not null)then
                v_feetype := lpad(v_feetype,2,'0');
                update work_datafile set F5=v_feetype where rowid=c1.rowid;
				dbms_output.put_line('93');
            end if;
            
        elsif (v_map_id=65 or  v_map_id=80 or v_map_id=82) then	--pda,捐款短信(PM),手机邮箱(YX)业务局数据
            v_spcode     := c1.F1;
            v_opercode   := c1.F2;
            v_opername   := c1.F3;
            v_feetype    := c1.F4;
            v_fee        := c1.F5;
            v_applytime  := c1.F6;
            v_expiretime := c1.F7;
                       
            if(v_applytime is null) then
                v_applytime := to_char(to_date('20201021','yyyymmdd'),'yyyy-mm-dd hh24:mi:ss');
                update work_datafile set F6=v_applytime where rowid=c1.rowid;
				dbms_output.put_line('94');
            end if;
            if((v_expiretime is null) or (to_date(v_expiretime,'yyyy-mm-dd hh24:mi:ss')>to_date('2037-01-01','yyyy-mm-dd'))) then
               --v_expiretime := '20370101000000';
               v_expiretime := null;
               update work_datafile set F7=v_expiretime where rowid=c1.rowid;
			   dbms_output.put_line('95');
            end if;
            --修正v_feetype为双字符串
            if(v_feetype is not null)then
                v_feetype := lpad(v_feetype,2,'0');
                update work_datafile set F4=v_feetype where rowid=c1.rowid;
				dbms_output.put_line('96');
            end if;
                
        elsif (v_map_id=69) then             --Kjava业务局数据    
            v_spcode     := c1.F1;
            v_channel    := c1.F2;
            v_opercode    := c1.F3;
            v_opername     := c1.F4;
            v_feetype   := c1.F5;
            v_fee        := c1.F6;
            v_download   := c1.F7;
            v_applytime  := c1.F8;
            v_expiretime := c1.F9;
                
            if(v_applytime is null) then
                v_applytime := to_char(to_date('20201021','yyyymmdd'),'yyyy-mm-dd hh24:mi:ss');
                update work_datafile set F8=v_applytime where rowid=c1.rowid;
				dbms_output.put_line('97');
            end if;
            if((v_expiretime is null) or (to_date(v_expiretime,'yyyy-mm-dd hh24:mi:ss')>to_date('2037-01-01','yyyy-mm-dd'))) then
                --v_expiretime := '20370101000000';
                v_expiretime := null;
                update work_datafile set F9=v_expiretime where rowid=c1.rowid;
				dbms_output.put_line('98');
            end if;
            --修正v_feetype为双字符串
            if(v_feetype is not null)then
                v_feetype := lpad(v_feetype,2,'0');
                update work_datafile set F5=v_feetype where rowid=c1.rowid;
				dbms_output.put_line('99');
            end if;    
        
        elsif (v_map_id=74) then             --流媒体业务局数据    
            v_spcode     := c1.F1;
            v_opercode   := c1.F2;
            v_opername   := c1.F3;
            v_feetype    := c1.F4;
            v_fee        := c1.F5;
            v_rate       := c1.F6;
            v_applytime  := c1.F7;
            v_expiretime := c1.F8;
                
            if(v_applytime is null) then
                v_applytime := to_char(to_date('20201021','yyyymmdd'),'yyyy-mm-dd hh24:mi:ss');
                update work_datafile set F7=v_applytime where rowid=c1.rowid;
				dbms_output.put_line('100');
            end if;
            if((v_expiretime is null) or (to_date(v_expiretime,'yyyy-mm-dd hh24:mi:ss')>to_date('2037-01-01','yyyy-mm-dd'))) then
                --v_expiretime := '20370101000000';
                v_expiretime := null;
                update work_datafile set F8=v_expiretime where rowid=c1.rowid;
				dbms_output.put_line('101');
            end if;
            --修正v_feetype为双字符串
            if(v_feetype is not null)then
                v_feetype := lpad(v_feetype,2,'0');
                update work_datafile set F4=v_feetype where rowid=c1.rowid;
				dbms_output.put_line('102');
            end if;    
        
        elsif (v_map_id=76) then	--手机动画业务局数据
            v_spcode     := c1.F1;
            v_channel    := c1.F2;
            v_opercode   := c1.F3;
            v_opername   := c1.F4;
            v_feetype    := c1.F5;
            v_fee        := c1.F6;
            v_rate       := c1.F7;
            v_applytime  := c1.F8;
            v_expiretime := c1.F9;
            
            if(v_applytime is null) then
                v_applytime := to_char(to_date('20201021','yyyymmdd'),'yyyy-mm-dd hh24:mi:ss');
                update work_datafile set F8=v_applytime where rowid=c1.rowid;
				dbms_output.put_line('103');
            end if;
            if((v_expiretime is null) or (to_date(v_expiretime,'yyyy-mm-dd hh24:mi:ss')>to_date('2037-01-01','yyyy-mm-dd'))) then
            --v_expiretime := '20370101000000';
                v_expiretime := null;
                update work_datafile set F9=v_expiretime where rowid=c1.rowid;
				dbms_output.put_line('104');
            end if;
            --修正v_feetype为双字符串
            if(v_feetype is not null)then
                v_feetype := lpad(v_feetype,2,'0');
                update work_datafile set F5=v_feetype where rowid=c1.rowid;
				dbms_output.put_line('105');
            end if;
          
        else
            v_result := '未找到该映射id:' || v_map_id ||'，请确认已定义';
			dbms_output.put_line('106');
            return;
                
        end if;
        
        --通过v_note传递参数存在标志，不作要求就是0，新增1，存在2，不一致3
        if (v_spcode is not null) then            
            select count(*) into v_count from mapping_list where mapping_id=v_map_id 
                and mapping_sour=v_spcode || ',' || v_opercode;
                --and to_date('20201021','yyyymmdd') >=applytime and (to_date('20201021','yyyymmdd') <=expiretime or expiretime is null);
            if(v_count > 0)then          
                select count(*) into v_count from mapping_list where mapping_id=v_map_id 
                   and mapping_sour=v_spcode || ',' || v_opercode
                   and mapping_dest=v_opername || ',' || v_feetype || '.' || v_fee || ',1|85|1'
                   and to_char(applytime,'yyyymmdd')=substr(v_applytime,1,8)
                   and (decode(expiretime,null,'20370101',to_char(expiretime,'yyyymmdd'))=
                   decode(v_expiretime,null,'20370101',substr(v_expiretime,1,8)));
                   --and to_date('20201021','yyyymmdd') >=applytime and (to_date('20201021','yyyymmdd') <=expiretime or expiretime is null);
                
                if(v_count > 0)then
                    v_note := '2';
					dbms_output.put_line('107');
                else       
                   v_note := '3';  
dbms_output.put_line('108');				   
                end if;
                
            else
                v_note := '1';
				dbms_output.put_line('109');
            end if;
        else
            v_note := '0';
			dbms_output.put_line('110');
        end if;
       
        --更新参数标志
        if(substr(v_note,1,1)='2')  then
            update work_datafile set status=2,notes=v_note where rowid=c1.rowid;
            v_oldnum := v_oldnum+1;
			dbms_output.put_line('111');
        else
            if(substr(v_note,1,1)='3')  then
                update work_datafile set status=3,notes=v_note where rowid=c1.rowid;
                v_diffnum := v_diffnum+1;
				dbms_output.put_line('112');
            else              
                if(substr(v_note,1)<>'0')then
                    update work_datafile set status=1,notes=v_note where rowid=c1.rowid;
                    v_newnum := v_newnum+1;
					dbms_output.put_line('113');
                end if;
            end if;
        end if;
    end loop;
    v_result := v_ret_imp||(v_newnum+v_oldnum+v_diffnum+v_delnum)
               ||v_ret_new||v_newnum||v_ret_old||v_oldnum
               ||v_ret_diff||v_diffnum||v_ret_del||v_delnum;

end;

procedure DB_MAPPING_LIST_OPER(v_map_id number, v_result  in out  varchar2) as
v_count         number(8);
v_dupnum        number(8);
v_note          varchar2(200);
v_sptype        varchar2(20);
begin
    
    delete from mapping_list where mapping_id=v_map_id;
    case 
        when v_map_id=61 then v_note:='梦网短信业务局数据-'; v_sptype:='SMS';
		dbms_output.put_line('114');
        when v_map_id=63 then v_note:='WAP业务局数据-'; v_sptype:='WAP';
        dbms_output.put_line('115');
        when v_map_id=65 then v_note:='PDA业务局数据-'; v_sptype:='PDA';
        dbms_output.put_line('116');
        when v_map_id=67 then v_note:='MMS业务局数据-'; v_sptype:='MMS';
        dbms_output.put_line('117');
        when v_map_id=69 then v_note:='KJAVA业务局数据-'; v_sptype:='KJAVA';
        dbms_output.put_line('118');
        when v_map_id=74 then v_note:='流媒体企业局数据'; v_sptype:='STREAM';
        dbms_output.put_line('119');
        when v_map_id=76 then v_note:='手机动画企业局数据'; v_sptype:='FLASH';
        dbms_output.put_line('120');
        when v_map_id=78 then v_note:='自有短信企业局数据'; v_sptype:='CM';
        dbms_output.put_line('121');
        when v_map_id=80 then v_note:='捐款短信企业局数据'; v_sptype:='PM';
        dbms_output.put_line('122');
        when v_map_id=82 then v_note:='手机邮箱企业局数据'; v_sptype:='CX';
        dbms_output.put_line('123');
        else 
             v_result := '未找到该映射id:' || v_map_id ||'，请确认已定义';
         dbms_output.put_line('124');
            return;
    end case;
    if (v_map_id=61 or v_map_id=63 or v_map_id=67 or v_map_id=78)  then--梦网短信,wap,mms,自有短信(CM)业务局数据
       
        /*
        delete  from work_datafile  where F8 is not null and F8 <to_date('20201021','yyyymmdd') 
        v_dupnum :=sql%rowcount;
        */
        delete  from work_datafile a where 
        a.rowid!= (select max(b.rowid) from work_datafile b  
        where a.F1=b.F1 and a.F3=b.F3 
        and a.F5=b.F5 and a.F7=b.F7)  ;
        
        v_dupnum :=sql%rowcount;
                
        insert into mapping_list (MAPPING_SOUR,MAPPING_DEST,APPLYTIME,EXPIRETIME,MAPPING_ID,NOTE)
        select a.F1||','||a.F3,  a.F4||','||a.F5||'.'||a.F6||',1|85|1', 
        to_date(substr(a.F7,1,8),'yyyymmdd'), to_date(substr(a.F8,1,8),'yyyymmdd'),
        v_map_id, v_note||to_char(to_date('20201021','yyyymmdd'),'yyyymmddhh24miss') from WORK_DATAFILE a;
        --修改同步表
        delete from STL_SP_OPER where sp_type=v_sptype;
        
        insert into STL_SP_OPER(SP_CODE,SP_TYPE,OPER_CODE,OPER_NAME,CHRG_TYPE,FEE_RATE,
        SP_RATIO,FILTER_FLAG,EFF_DATE,EXP_DATE,CYCLE1,CYCLE2,REMARK) 
        select a.F1, v_sptype, a.F3, a.F4, a.F5, a.F6, a.F9, null, 
        to_date(substr(a.F7,1,8),'yyyymmdd'), to_date(substr(a.F8,1,8),'yyyymmdd'), null, null,
        v_note||to_char(to_date('20201021','yyyymmdd'),'yyyymmddhh24miss') from WORK_DATAFILE a;
dbms_output.put_line('125');
    elsif (v_map_id=65 or  v_map_id=80 or v_map_id=82) then	--pda,捐款短信(PM),手机邮箱(YX)业务局数据
        delete  from work_datafile a where 
        a.rowid!= (select max(b.rowid) from work_datafile b  
        where a.F1=b.F1 and a.F2=b.F2 and a.F3=b.F3 
        and a.F4=b.F4 and a.F5=b.F5  and a.F6=b.F6)  ;
        v_dupnum :=sql%rowcount;
        
        insert into mapping_list (MAPPING_SOUR,MAPPING_DEST,APPLYTIME,EXPIRETIME,MAPPING_ID,NOTE)
        select a.F1||','||a.F2,  a.F3||','||a.F4||'.'||a.F5||',1|85|1', 
        to_date(substr(a.F6,1,8),'yyyymmdd'), to_date(substr(a.F7,1,8),'yyyymmdd'),
        v_map_id, v_note||to_char(to_date('20201021','yyyymmdd'),'yyyymmddhh24miss') from WORK_DATAFILE a;   
        --修改同步表
        delete from STL_SP_OPER where sp_type=v_sptype;
        insert into STL_SP_OPER select a.F1, v_sptype, a.F2, a.F3, a.F4, a.F5, 85, null, 
        to_date(substr(a.F6,1,8),'yyyymmdd'), to_date(substr(a.F7,1,8),'yyyymmdd'), null, null,
        v_note||to_char(to_date('20201021','yyyymmdd'),'yyyymmddhh24miss') from WORK_DATAFILE a;               
         dbms_output.put_line('126');   
    elsif (v_map_id=69) then             --Kjava业务局数据    
        delete  from work_datafile a where 
        a.rowid!= (select max(b.rowid) from work_datafile b  
        where a.F1=b.F1 and a.F3=b.F3 
        and a.F4=b.F4 and a.F5=b.F5  and a.F6=b.F6 and a.F8=b.F8)  ;
        v_dupnum :=sql%rowcount;
          
        insert into mapping_list (MAPPING_SOUR,MAPPING_DEST,APPLYTIME,EXPIRETIME,MAPPING_ID,NOTE)
        select a.F1||','||a.F3,  a.F4||','||a.F5||'.'||a.F6||',1|85|1', 
        to_date(substr(a.F8,1,8),'yyyymmdd'), to_date(substr(a.F9,1,8),'yyyymmdd'),
        v_map_id, v_note||to_char(to_date('20201021','yyyymmdd'),'yyyymmddhh24miss') from WORK_DATAFILE a;     
        --修改同步表
        delete from STL_SP_OPER where sp_type=v_sptype;
        insert into STL_SP_OPER select a.F1, v_sptype, a.F3, a.F4, a.F5, a.F6, 85, null, 
        to_date(substr(a.F8,1,8),'yyyymmdd'), to_date(substr(a.F9,1,8),'yyyymmdd'), null, null,
        v_note||to_char(to_date('20201021','yyyymmdd'),'yyyymmddhh24miss') from WORK_DATAFILE a;      
dbms_output.put_line('127');
    elsif (v_map_id=74) then	--流媒体业务局数据    
        delete  from work_datafile a where 
        a.rowid!= (select max(b.rowid) from work_datafile b  
        where a.F1=b.F1 and a.F2=b.F2 and a.F3=b.F3 
        and a.F4=b.F4 and a.F5=b.F5  and a.F6=b.F6 and a.F7=b.F7)  ;
        v_dupnum :=sql%rowcount;
         
        insert into mapping_list (MAPPING_SOUR,MAPPING_DEST,APPLYTIME,EXPIRETIME,MAPPING_ID,NOTE)
        select a.F1||','||a.F2,  a.F3||',,'||substr(a.F4,2,1)||a.F5, 
        to_date(substr(a.F7,1,8),'yyyymmdd'), to_date(substr(a.F8,1,8),'yyyymmdd'),
        v_map_id, v_note||to_char(to_date('20201021','yyyymmdd'),'yyyymmddhh24miss') from WORK_DATAFILE a;     
        --修改同步表
        delete from STL_SP_OPER where sp_type=v_sptype;
        insert into STL_SP_OPER select a.F1, v_sptype, a.F2, a.F3, a.F4, a.F5, a.F6, null, 
        to_date(substr(a.F7,1,8),'yyyymmdd'), to_date(substr(a.F8,1,8),'yyyymmdd'), null, null,
        v_note||to_char(to_date('20201021','yyyymmdd'),'yyyymmddhh24miss') from WORK_DATAFILE a;      
dbms_output.put_line('128');
    elsif (v_map_id=76) then	--手机动画业务局数据   
        delete  from work_datafile a where 
        a.rowid!= (select max(b.rowid) from work_datafile b  
        where a.F1=b.F1 and a.F3=b.F3 
        and a.F4=b.F4 and a.F5=b.F5  and a.F6=b.F6 and a.F8=b.F8)  ;
        v_dupnum :=sql%rowcount; 
        
        insert into mapping_list (MAPPING_SOUR,MAPPING_DEST,APPLYTIME,EXPIRETIME,MAPPING_ID,NOTE)
        select a.F1||','||a.F3,  a.F4||','||a.F5||'.'||a.F6||',1|85|1', 
        to_date(substr(a.F8,1,8),'yyyymmdd'), to_date(substr(a.F9,1,8),'yyyymmdd'),
        v_map_id, v_note||to_char(to_date('20201021','yyyymmdd'),'yyyymmddhh24miss') from WORK_DATAFILE a;     
        --修改同步表
        delete from STL_SP_OPER where sp_type=v_sptype;
        insert into STL_SP_OPER select a.F1, v_sptype, a.F3, a.F4, a.F5, a.F6, a.F7, null, 
        to_date(substr(a.F8,1,8),'yyyymmdd'), to_date(substr(a.F9,1,8),'yyyymmdd'), null, null,
        v_note||to_char(to_date('20201021','yyyymmdd'),'yyyymmddhh24miss') from WORK_DATAFILE a;      
dbms_output.put_line('129');
    end if;
    
    select count(*) into v_count from WORK_DATAFILE;
   /* --删除重复纪录      
    delete from mapping_list a where 
        a.rowid!= (select max(b.rowid) from mapping_list b  
        where a.mapping_id=b.mapping_id and a.mapping_sour=b.mapping_sour and a.mapping_dest=b.mapping_dest
        and a.applytime=b.applytime and a.expiretime=b.expiretime) and a.mapping_id=v_map_id;
    
    v_dupnum :=sql%rowcount;
                     */
    v_result := v_ret_imp||(v_count+v_dupnum)||v_ret_update||(v_count)
               ||v_ret_dup||v_dupnum ;
              
end;

/******************************************************************************
******************************************************************************/
procedure PROC_MAPPING_LIST_SNSP(v_map_id number, v_result  in out  varchar2) as
v_spcode        varchar2(100);
v_spname        varchar2(100);
v_servcode      varchar2(100);
v_servtype      varchar2(100);
v_applytime     varchar2(100);
v_expiretime    varchar2(100);
v_infofeemode   varchar2(100);
v_billcycle     varchar2(100);
v_feemode       varchar2(100);
v_feefuction    varchar2(100);
v_city          varchar2(100);

v_rate          varchar2(10);
v_feetype       varchar2(10);
v_fee           varchar2(20);
v_opercode      varchar2(100);
v_opername      varchar2(100);
v_selience_judge  varchar2(20);
v_hige_judge      varchar2(20);
v_sour          varchar2(200);
v_dest          varchar2(200);

v_sptype        varchar2(20);
v_count         number(6) := 0;
v_delnum        number(6) := 0;
v_newnum        number(6) := 0;
v_oldnum        number(6) := 0;
v_diffnum       number(6) := 0;

begin
    for c1 in(select F1,F2,F3,F4,F5,F6,F7,F8,F9,F10,F11,rowid from WORK_DATAFILE where STATUS=0 order by ID)
    loop
        if (v_map_id=83 or v_map_id=84 or v_map_id=85)  then--省内梦网短信,mms,wap企业局数据
            v_spcode     := c1.F2;
            v_spname     := c1.F3;
            v_servcode   := c1.F4;
            v_servtype   := c1.F5;
            v_infofeemode    := c1.F6;
            v_billcycle   := c1.F7;
            v_feemode  := c1.F8;
            v_feefuction := c1.F9;
            v_city       :=c1.F10;
            v_applytime      :=c1.F11;
            
            v_sour :=v_spcode || ',' || v_servcode;
            v_dest :='1,'|| v_spname|| ','|| v_city;            
            if(v_applytime is null) then
                v_applytime := to_char(to_date('20201021','yyyymmdd'),'yyyy-mm-dd hh24:mi:ss');
                update work_datafile set F11=v_applytime where rowid=c1.rowid;
				dbms_output.put_line('130');
            end if;
            v_expiretime :=null;
           
           -----------
        elsif (v_map_id=-83 or v_map_id=-84 or v_map_id=-85)  then--省内梦网短信,mms,wap业务局数据
            case     
                when v_map_id=-83 then v_note:='省内梦网短信企业局数据'; v_sptype:='SNSMS';
				dbms_output.put_line('131');
                when v_map_id=-84 then v_note:='省内梦网彩信企业局数据'; v_sptype:='SNMMS'; 
				dbms_output.put_line('132');
                when v_map_id=-85 then v_note:='省内WAP企业局数据'; v_sptype:='SNWAP'; 
				dbms_output.put_line('133');
            end case;

            v_spcode     := c1.F2;
            v_opercode   := c1.F3;
            v_opername   := c1.F4;
            v_feetype    := c1.F5;
            v_fee        := c1.F6;
            v_rate       := c1.F7;
            v_selience_judge  := c1.F8;
            v_hige_judge := c1.F9;
            v_applytime      :=c1.F10;
            
            if(v_applytime is null) then
                v_applytime := to_char(to_date('20201021','yyyymmdd'),'yyyy-mm-dd hh24:mi:ss');
                update work_datafile set F10=v_applytime where rowid=c1.rowid;
				dbms_output.put_line('134');
            end if;
            v_expiretime :=null;       
        -----------         
        else
             v_result := '未找到该映射id:' || v_map_id ||'，请确认已定义';
			 dbms_output.put_line('135');
             return;
        end if;
        
        --通过v_note传递参数存在标志，不作要求就是0，新增1，存在2，不一致3
        if( v_map_id >0) then     --企业局数据
            if (v_spcode is not null) then            
                select count(*) into v_count from mapping_list where mapping_id=v_map_id 
                    and mapping_sour=v_sour
                    and to_date('20201021','yyyymmdd') >=applytime and (to_date('20201021','yyyymmdd') <=expiretime or expiretime is null);
                if(v_count > 0)then          
                    select count(*) into v_count from mapping_list where mapping_id=v_map_id 
                        and mapping_sour=v_sour
                        and mapping_dest=v_dest
                        and to_char(applytime,'yyyymmdd')=substr(v_applytime,1,8)
                        and (decode(expiretime,null,'20370101',to_char(expiretime,'yyyymmdd'))=
                        decode(v_expiretime,null,'20370101',substr(v_expiretime,1,8)));
                    if(v_count > 0)then
                        v_note := '2';
						dbms_output.put_line('136');
                    else       
                       v_note := '3';    
					   dbms_output.put_line('137');
                    end if;
                else
                    v_note := '1';
					dbms_output.put_line('138');
                end if;
            else
                v_note := '0';
				dbms_output.put_line('139');
            end if;
        else  --业务局数据
            if (v_spcode is not null) then
                select count(*) into v_count from STL_SP_OPER where sp_type=v_sptype 
                    and sp_code=v_spcode and nvl(oper_code,0)=nvl(v_opercode,0)
                    and CHRG_TYPE=v_feetype 
                    and to_date('20201021','yyyymmdd') >=eff_date and (to_date('20201021','yyyymmdd') <=exp_date or exp_date is null);
                if(v_count > 0)then          
                    select count(*) into v_count from STL_SP_OPER where sp_type=v_sptype 
                        and sp_code=v_spcode and nvl(oper_code,0)=nvl(v_opercode,0)
                        and CHRG_TYPE=v_feetype
                        and to_char(eff_date,'yyyymmdd')=substr(v_applytime,1,8)
                        and (decode(exp_date,null,'20370101',to_char(exp_date,'yyyymmdd'))=
                        decode(v_expiretime,null,'20370101',substr(v_expiretime,1,8)));
                    if(v_count > 0)then
                        v_note := '2';
						dbms_output.put_line('140');
                    else       
                       v_note := '3';  
dbms_output.put_line('141');					   
                    end if;
                else
                    v_note := '1';
					dbms_output.put_line('142');
                end if;
            else
                v_note := '0';
				dbms_output.put_line('143');
            end if;
        end if;
        
        --更新参数标志
        if(substr(v_note,1,1)='2')  then
            update work_datafile set status=2,notes=v_note where rowid=c1.rowid;
            v_oldnum := v_oldnum+1;
			dbms_output.put_line('144');
        else
            if(substr(v_note,1,1)='3')  then
                update work_datafile set status=3,notes=v_note where rowid=c1.rowid;
                v_diffnum := v_diffnum+1;
				dbms_output.put_line('145');
            else              
                if(substr(v_note,1)<>'0')then
                    update work_datafile set status=1,notes=v_note where rowid=c1.rowid;
                    v_newnum := v_newnum+1;
					dbms_output.put_line('146');
                end if;
            end if;
        end if;
    end loop;
    v_result := v_ret_imp||(v_newnum+v_oldnum+v_diffnum+v_delnum)
               ||v_ret_new||v_newnum||v_ret_old||v_oldnum
               ||v_ret_diff||v_diffnum||v_ret_del||v_delnum;

end;
/*******************************************************
********************************************************/
procedure DB_MAPPING_LIST_SNSP(v_map_id number, v_result  in out  varchar2) as
v_count         number(8);
v_oldnum        number(8);
v_notexistnum   number(8);
v_insert        number(8);
v_update        number(8);
v_delete        number(8);
v_note          varchar2(200);
v_sptype        varchar2(20);
v_sour          varchar2(200);
v_dest          varchar2(200);
v_num           number(8);
begin
    
    case     
        when v_map_id=83 then v_note:='省内梦网短信企业局数据'; v_sptype:='SNSMS';
		dbms_output.put_line('147');
        when v_map_id=84 then v_note:='省内梦网彩信企业局数据'; v_sptype:='SNMMS'; 
        dbms_output.put_line('148');
        when v_map_id=85 then v_note:='省内WAP企业局数据'; v_sptype:='SNWAP'; 
        dbms_output.put_line('149');
        when v_map_id=-83 then v_note:='省内梦网短信业务局数据'; v_sptype:='SNSMS';
        dbms_output.put_line('150');
        when v_map_id=-84 then v_note:='省内梦网彩信业务局数据'; v_sptype:='SNMMS'; 
        dbms_output.put_line('151');
        when v_map_id=-85 then v_note:='省内WAP业务局数据'; v_sptype:='SNWAP'; 
        dbms_output.put_line('152');
        else 
             v_result := '未找到该映射id:' || v_map_id ||'，请确认已定义';
        dbms_output.put_line('153');
             return;
    end case;
    
    v_count := 0;
    v_oldnum :=0;
    v_notexistnum :=0;
    v_insert :=0;
    v_update :=0;
    v_delete :=0;
                  
    if (v_map_id=83  or v_map_id=84 or v_map_id=85)  then--省内梦网短信,省内mms,省内wap企业局数据
        for c2 in(select F1,F2,F3,F4,F5,F6,F7,F8,F9,F10,F11,STATUS,rowid from WORK_DATAFILE  order by ID)
        loop   
            v_count := v_count + 1;
            if(c2.F1='新增') then
                if(c2.STATUS<>1) then
                    v_oldnum := v_oldnum + 1;
					dbms_output.put_line('154');
                else 
                    v_insert := v_insert + 1;
                    insert into mapping_list (MAPPING_SOUR,MAPPING_DEST,APPLYTIME,EXPIRETIME,MAPPING_ID,NOTE)
                    values( c2.F2||','||c2.F4,  '1,'||c2.F3||','||c2.F10, 
                    to_date(substr(c2.F11,1,8),'yyyymmdd'), null,
                    v_map_id, v_note||to_char(to_date('20201021','yyyymmdd'),'yyyymmddhh24miss')) ;
                --修改同步表
                /*
                    insert into STL_SP_CODE values (
                    c2.F2, v_sptype, c2.F3, c2.F10, c2.F4, 
                    c2.F6, c2.F7, c2.F5, null, c2.F8, c2.F9, 
                    to_date(substr(c2.F11,1,8),'yyyymmdd'), null, null, null,
                    v_note||to_char(to_date('20201021','yyyymmdd'),'yyyymmddhh24miss')) ;
                    */
                    insert into STL_SP_CODE (SP_CODE,SP_TYPE,SP_NAME,VPROV,SERV_CODE,
                    STL_MODE,VALID_CYCLES,STL_LEVEL,VREGION,SERV_PHONE,
                    FEE_TYPE,FEE_FUCTION,EFF_DATE,EXP_DATE,CYCLE1,CYCLE2,REMARK)
                    values (c2.F2, v_sptype, c2.F3, c2.F10, c2.F4, 
                    c2.F6, c2.F7, c2.F5, c2.F10, null, c2.F8, c2.F9, 
                    to_date(substr(c2.F11,1,8),'yyyymmdd'), null, null, null,
                    v_note||to_char(to_date('20201021','yyyymmdd'),'yyyymmddhh24miss'));
                    dbms_output.put_line('155');
                end if;               
            elsif (c2.F1='修改') then
                if(c2.STATUS<>2) then
                    v_notexistnum := v_notexistnum + 1;
					dbms_output.put_line('156');
                else                 
                    v_update := v_update + 1;
                    
                    v_sour := c2.F2||','||c2.F4;
                    v_dest := '1,'||c2.F3||','||c2.F10;
                    /*如果mapping_list里的数据没改变，就不需要修改mapping_list*/
                    select count(*) into v_num from mapping_list where mapping_id=v_map_id and 
                        mapping_sour=v_sour and mapping_dest=v_dest;
                    if( v_num =0) then 
                        update mapping_list set expiretime=to_date(substr(c2.F11,1,8),'yyyymmdd')
                        where mapping_sour=c2.F2||','||c2.F4 and (expiretime >to_date('20201021','yyyymmdd') or expiretime is null);
                    
                        insert into mapping_list (MAPPING_SOUR,MAPPING_DEST,APPLYTIME,EXPIRETIME,MAPPING_ID,NOTE)
                        values( c2.F2||','||c2.F4,  '1,'||c2.F3||','||c2.F10, 
                        to_date(substr(c2.F11,1,8),'yyyymmdd'), null,
                        v_map_id, v_note||to_char(to_date('20201021','yyyymmdd'),'yyyymmddhh24miss')) ;    
dbms_output.put_line('157');						
                    end if;
                    
                    --修改同步表
                    update STL_SP_CODE set exp_date=to_date(substr(c2.F11,1,8),'yyyymmdd')
                    where sp_code=c2.F2 and sp_type=v_sptype and 
                    (exp_date >to_date('20201021','yyyymmdd') or exp_date is null);
                    /*
                    insert into STL_SP_CODE values (
                    c2.F2, v_sptype, c2.F3, c2.F10, c2.F4, 
                    c2.F6, c2.F7, c2.F5, null, c2.F8, c2.F9, 
                    to_date(substr(c2.F11,1,8),'yyyymmdd'), null, null, null,
                    v_note||to_char(to_date('20201021','yyyymmdd'),'yyyymmddhh24miss')) ;
                    */
                    insert into STL_SP_CODE (SP_CODE,SP_TYPE,SP_NAME,VPROV,SERV_CODE,
                    STL_MODE,VALID_CYCLES,STL_LEVEL,VREGION,SERV_PHONE,
                    FEE_TYPE,FEE_FUCTION,EFF_DATE,EXP_DATE,CYCLE1,CYCLE2,REMARK)
                    values (c2.F2, v_sptype, c2.F3, c2.F10, c2.F4, 
                    c2.F6, c2.F7, c2.F5, c2.F10,null, c2.F8, c2.F9, 
                    to_date(substr(c2.F11,1,8),'yyyymmdd'), null, null, null,
                    v_note||to_char(to_date('20201021','yyyymmdd'),'yyyymmddhh24miss')) ;
                    dbms_output.put_line('158');
                end if;
            elsif (c2.F1='删除') then
                if(c2.STATUS<2) then
                    v_oldnum := v_oldnum + 1;
					dbms_output.put_line('159');
                else 
                    v_delete := v_delete + 1;
                    update mapping_list set expiretime=to_date(substr(c2.F11,1,8),'yyyymmdd')
                    where mapping_sour=c2.F2||','||c2.F4 and (expiretime >to_date('20201021','yyyymmdd') or expiretime is null);
                    --修改同步表
                    update STL_SP_CODE set exp_date=to_date(substr(c2.F11,1,8),'yyyymmdd')
                    where sp_code=c2.F2 and sp_type=v_sptype and 
                    (exp_date >to_date('20201021','yyyymmdd') or exp_date is null);
					dbms_output.put_line('160');
                end if;
            else
                v_result := '非法的操作类型:' || c2.F1 ||'，目前只支持''新增'',''修改'',''删除''';
				dbms_output.put_line('161');
                return;    
            end if;
            
        end loop;
/*
对于省内业务局数据中，如果是：
1。业务代码为空，且计费类型为03（包月），且是否核减沉默用户为0（不参与），则插入该spcode到集合81（不参与梦网沉默用户稽核的SP）
2。业务代码不为空，且计费类型为03（包月），且是否核减沉默用户为0（不参与），则插入该opercode到集合82（不参与稽核梦网业务代码）
*/  
    elsif (v_map_id=-83  or v_map_id=-84 or v_map_id=-85)  then--省内梦网短信,省内mms,省内wap业务局数据
        for c2 in(select F1,F2,F3,F4,F5,F6,F7,F8,F9,F10,F11,STATUS,rowid from WORK_DATAFILE  order by ID)
        loop   

            v_count := v_count + 1;
            if(c2.F1='新增') then 
                if(c2.STATUS<>1) then
                    v_oldnum := v_oldnum + 1;
					dbms_output.put_line('162');
                else 
                    v_insert := v_insert + 1;
                    insert into STL_SP_OPER (SP_CODE,SP_TYPE,OPER_CODE,OPER_NAME,CHRG_TYPE,FEE_RATE,		
                    SP_RATIO,FILTER_FLAG,EFF_DATE,EXP_DATE,CYCLE1,CYCLE2,REMARK)
                    values( c2.F2,v_sptype,c2.F3,c2.F4, c2.F5,c2.F6, 
                    c2.F7, c2.F8||c2.F9||'00000000000000', 
                    to_date(substr(c2.F10,1,8),'yyyymmdd'), null, null,null,
                    v_note||to_char(to_date('20201021','yyyymmdd'),'yyyymmddhh24miss')) ;
                    if(c2.F3 is null and c2.F5='03' and c2.F8='0') then
                        insert into set_value (set_id,applytime,expiretime,value,note)
                        values (81,to_date(substr(c2.F10,1,8),'yyyymmdd'),null,c2.F2,c2.F2||',不参与梦网沉默用户稽核');
						dbms_output.put_line('163');
                    end if;
                    if(c2.F3 is not null and c2.F5='03' and c2.F8='0') then
                        insert into set_value (set_id,applytime,expiretime,value,note)
                        values (82,to_date(substr(c2.F10,1,8),'yyyymmdd'),null,c2.F2||','||c2.F3,
                        c2.F2||','||c2.F3||',不参与梦网沉默用户稽核');
						dbms_output.put_line('164');
                    end if;
                    
                --修改同步表
                end if;               
            elsif (c2.F1='修改') then
                if(c2.STATUS<>2) then
                    v_notexistnum := v_notexistnum + 1;
					dbms_output.put_line('165');
                else                 
                    v_update := v_update + 1;
                  
                    --修改同步表
                    update STL_SP_OPER set exp_date=to_date(substr(c2.F10,1,8),'yyyymmdd')
                    where sp_code=c2.F2 and sp_type=v_sptype and CHRG_TYPE=c2.F5 and
                    (exp_date >to_date('20201021','yyyymmdd') or exp_date is null);
                    
                    insert into STL_SP_OPER (SP_CODE,SP_TYPE,OPER_CODE,OPER_NAME,CHRG_TYPE,FEE_RATE,		
                    SP_RATIO,FILTER_FLAG,EFF_DATE,EXP_DATE,CYCLE1,CYCLE2,REMARK)
                    values( c2.F2,v_sptype,c2.F3,c2.F4, c2.F5,c2.F6, 
                    c2.F7, c2.F8||c2.F9||'00000000000000', 
                    to_date(substr(c2.F10,1,8),'yyyymmdd'), null, null,null,
                    v_note||to_char(to_date('20201021','yyyymmdd'),'yyyymmddhh24miss')) ;
					dbms_output.put_line('166');
                end if;
            elsif (c2.F1='删除') then
                if(c2.STATUS<>2) then
                    v_oldnum := v_oldnum + 1;
					dbms_output.put_line('167');
                else 
                    v_delete := v_delete + 1;
                    --修改同步表
                    update STL_SP_OPER set exp_date=to_date(substr(c2.F10,1,8),'yyyymmdd')
                    where sp_code=c2.F2 and sp_type=v_sptype and CHRG_TYPE=c2.F5  and
                    (exp_date >to_date('20201021','yyyymmdd') or exp_date is null);
                    
                    if(c2.F3 is null and c2.F5='03' and c2.F8='0') then
                        update set_value set expiretime=to_date(substr(c2.F10,1,8),'yyyymmdd')
                        where  set_id=81 and value=c2.F2;
						dbms_output.put_line('168');
                    end if;
                    if(c2.F3 is not null and c2.F5='03' and c2.F8='0') then
                        update set_value set expiretime=to_date(substr(c2.F10,1,8),'yyyymmdd')
                        where  set_id=82 and value=c2.F2||','||c2.F3;
						dbms_output.put_line('169');
                    end if;
                    
                end if;
            else
                v_result := '非法的操作类型:' || c2.F1 ||'，目前只支持''新增'',''修改'',''删除''';
				dbms_output.put_line('170');
                return;    
            end if;
        end loop;

    end if;

    v_result := v_ret_ask||v_count ||v_ret_done||(v_insert+v_update+v_delete)
    ||v_ret_new||v_insert ||v_ret_update||v_update ||v_ret_delete||v_delete
    || v_ret_error ||(v_count-v_insert-v_update-v_delete)
   ;
              
end;


procedure PROC_HIGHCOSTEXCHANGERATE(v_result  in out  varchar2) as
v_imsi      varchar2(100);
v_money     varchar2(100);
v_note      varchar2(200);
v_unit      varchar2(100);
v_managecode    varchar2(100);
v_applytime     varchar2(100);
v_expiretime    varchar2(100);

v_count         number(6) := 0;
v_delnum        number(6) := 0;
v_newnum        number(6) := 0;
v_oldnum        number(6) := 0;
v_diffnum       number(6) := 0;

begin
    for c1 in(select F1,F2,F3,F4,F5,F6,F7,rowid from WORK_DATAFILE where STATUS=0 order by ID)
    loop
        v_managecode     := c1.F1;
        v_imsi     := c1.F2;
        v_note   := c1.F3;
        v_money    := c1.F4;
        v_unit   := c1.F5;
        v_applytime  := c1.F6;
        v_expiretime := c1.F7;
        if(v_applytime is null) then
            v_applytime := to_char(to_date('20201021','yyyymmdd'),'yyyy-mm-dd hh24:mi:ss');
            update work_datafile set F6=v_applytime where rowid=c1.rowid;
			dbms_output.put_line('171');
        end if;
        if((v_expiretime is null) or (to_date(v_expiretime,'yyyy-mm-dd hh24:mi:ss')>to_date('2037-01-01','yyyy-mm-dd'))) then
            v_expiretime := '2037-01-01 00:00:00';
            update work_datafile set F7=v_expiretime where rowid=c1.rowid;
			dbms_output.put_line('172');
        end if;
        
          
        --通过v_note传递参数存在标志，不作要求就是0，新增1，存在2，不一致3
        if (v_imsi is not null) then            
            select count(*) into v_count from highcostexchangerate where imsinum=v_imsi 
                and to_date('20201021','yyyymmdd') >=starttime and (to_date('20201021','yyyymmdd') <=endtime or endtime is null);
            if(v_count > 0)then          
                select count(*) into v_count from highcostexchangerate where imsinum=v_imsi
                   and to_char(starttime,'yyyy-mm-dd')=substr(v_applytime,1,10)
                   and (decode(endtime,null,'2037-01-01',to_char(endtime,'yyyy-mm-dd'))=
                   decode(v_expiretime,null,'2037-01-01',substr(v_expiretime,1,10)));
                   --and to_date('20201021','yyyymmdd') >=applytime and (to_date('20201021','yyyymmdd') <=expiretime or expiretime is null);
                 if(v_count > 0)then
                    v_note := '2';
					dbms_output.put_line('173');
                else       
                   v_note := '3';    
				   dbms_output.put_line('174');
                end if;
                
            else
                v_note := '1';
				dbms_output.put_line('175');
            end if;
        else
            v_note := '0';
			dbms_output.put_line('176');
        end if;
       
        --更新参数标志
        if(substr(v_note,1,1)='2')  then
            update work_datafile set status=2,notes=v_note where rowid=c1.rowid;
            v_oldnum := v_oldnum+1;
			dbms_output.put_line('177');
        else
            if(substr(v_note,1,1)='3')  then
                update work_datafile set status=3,notes=v_note where rowid=c1.rowid;
                v_diffnum := v_diffnum+1;
				dbms_output.put_line('178');
            else              
                if(substr(v_note,1)<>'0')then
                    update work_datafile set status=1,notes=v_note where rowid=c1.rowid;
                    v_newnum := v_newnum+1;
					dbms_output.put_line('179');
                end if;
            end if;
        end if;
    end loop;
    v_result := v_ret_imp||(v_newnum+v_oldnum+v_diffnum+v_delnum)
               ||v_ret_new||v_newnum||v_ret_old||v_oldnum
               ||v_ret_diff||v_diffnum||v_ret_del||v_delnum;

end;


procedure DB_HIGHCOSTEXCHANGERATE(v_result  in out  varchar2) as
v_count         number(8);
v_dupnum        number(8);

begin

    delete from HIGHCOSTEXCHANGERATE;
    insert into HIGHCOSTEXCHANGERATE (IMSINUM,MONEY,RATE,NOTE,UNIT,MANAGECODE,STARTTIME,ENDTIME)
    select a.F2,  a.F4, 1, a.F3, a.F5, a.F1,
         to_date(a.F6,'yyyy-mm-dd hh24:mi:ss'), 
         to_date(a.F7,'yyyy-mm-dd hh24:mi:ss')
         from WORK_DATAFILE a;
       
    select count(*) into v_count from WORK_DATAFILE;
    --删除重复纪录      
    delete from HIGHCOSTEXCHANGERATE a where 
        a.rowid!= (select max(b.rowid) from HIGHCOSTEXCHANGERATE b  
        where a.imsinum=b.imsinum 
        and a.starttime=b.starttime and a.endtime=b.endtime);
    
    v_dupnum :=sql%rowcount;
                     
    v_result := v_ret_imp||v_count||v_ret_update||(v_count-v_dupnum)
               ||v_ret_dup||v_dupnum ;
			   dbms_output.put_line('180');
              
end;


end;

$





  CREATE OR REPLACE PACKAGE BODY "PKG_GET_SPLIT_REGION_LIST" is

  -- Function and procedure implementations
  FUNCTION get(v_param_name in VARCHAR2, v_region in VARCHAR2)
    return REGION_TYPE is
    V_REGION_LIST REGION_TYPE;

    V_REGION_STR VARCHAR2(1024);
    V_GET_FLAG   NUMBER(4) := 0;
    V_TMP_REGION VARCHAR2(12);

    V_POS_BEGIN NUMBER(4) := 0;
    V_POS_END   NUMBER(4) := 0;
    V_INDEX     NUMBER(4) := 0;
  begin
    --split region and part
    for cl4 in (SELECT *
                  FROM GLOBAL_CONFIG
                 WHERE LOWER(PARAM_NAME) like
                       v_param_name || '_' || v_region || '_part%') loop
      V_REGION_STR := V_REGION_STR || ',' || cl4.param_value;
      V_GET_FLAG   := 1;
	  dbms_output.put_line('1');
    end loop;
    --split region
    IF V_GET_FLAG = 0 THEN
      for cl1 in (SELECT *
                    FROM GLOBAL_CONFIG
                   WHERE LOWER(PARAM_NAME) = v_param_name || '_' || v_region) loop
        V_REGION_STR := V_REGION_STR || ',' || cl1.param_value;
        V_GET_FLAG   := 1;
		 dbms_output.put_line('2');
      end loop;
    end if;
    --split part
    IF V_GET_FLAG = 0 THEN
      for cl1 in (SELECT *
                    FROM GLOBAL_CONFIG
                   WHERE LOWER(PARAM_NAME) like v_param_name || '_part%') loop
        V_REGION_STR := V_REGION_STR || ',' || cl1.param_value;
        V_GET_FLAG   := 1;
		 dbms_output.put_line('3');
      end loop;
    end if;
    --no split
    IF V_GET_FLAG = 0 THEN
      for cl1 in (SELECT *
                    FROM GLOBAL_CONFIG
                   WHERE LOWER(PARAM_NAME) = v_param_name) loop
        V_REGION_STR := V_REGION_STR || ',' || cl1.param_value;
        V_GET_FLAG   := 1;
		 dbms_output.put_line('4');
      end loop;
    end if;

    if V_REGION_STR is null then
	 dbms_output.put_line('5');
      return V_REGION_LIST;
    end if;
    <<Cl_SPLIT>>
    V_POS_END := INSTR(V_REGION_STR, ',', V_POS_BEGIN + 1);
    IF (V_POS_END > 0) THEN
      V_TMP_REGION := SUBSTR(V_REGION_STR,
                             V_POS_BEGIN + 1,
                             V_POS_END - V_POS_BEGIN - 1);
      IF (V_TMP_REGION IS NOT NULL) THEN
        V_REGION_LIST(V_INDEX) := V_TMP_REGION;
        V_INDEX := V_INDEX + 1;
 dbms_output.put_line('6');
      END IF;
      V_POS_BEGIN := V_POS_END;
      GOTO Cl_SPLIT;
    ELSE
      V_REGION_LIST(V_INDEX) := SUBSTR(V_REGION_STR, V_POS_BEGIN + 1);
	   dbms_output.put_line('7');
    END IF;
    return V_REGION_LIST;
  end;
begin
  -- Initialization
  null;
   dbms_output.put_line('8');
end pkg_get_split_region_list;


$



  CREATE OR REPLACE PACKAGE BODY "PKG_MASKCHECK" is

  --------------十进制转化为二进制 begin----------------------
 function d2b (n in number) return varchar2 is
    binval varchar2(64);
    n2     number := n;
    begin
      if n = 0 then
	   dbms_output.put_line('1');
         return '0';
      end if;
      while ( n2 > 0 ) loop
        binval := mod(n2, 2) || binval;
        n2 := trunc( n2 / 2 );
		 dbms_output.put_line('2');
      end loop;
      return binval;
  end d2b;
 --------------十进制转化为二进制 end---------------------------


  -----------某个字符串是否为数字字符串 begin-------------------
  function isNumber(p in varchar2)
  return number
  is
  result number;
  begin
    result := to_number(p);
	 dbms_output.put_line('3');
    return 1;
    exception
    when VALUE_ERROR then
	 dbms_output.put_line('4');
	return 0;
  end isNumber;
  -----------某个字符串是否为数字字符串 end-----------------------

 ------------将分隔符分隔的字符串拆分 begin-----------------------
 function fn_split (p_str IN VARCHAR2, p_delimiter IN VARCHAR2)
 RETURN ty_str_split PIPELINED
 IS
  j number:= 0;    ---分隔符的位置
  i number:= 1;    ---从哪个位置开始截取字符串
  len number:= 0;
  len1 number:= 0;
  str VARCHAR2 (64);
  BEGIN
    len := LENGTH (p_str);   ----被分割字符串的长度
    len1 := LENGTH (p_delimiter); ---分隔符的长度
    WHILE j < len
    LOOP
      j:= INSTR(p_str, p_delimiter, i);  ---取得分隔符的位置
      IF j = 0 THEN
        j := len;
        str := SUBSTR (p_str, i);
        PIPE ROW (str);
        IF i >= len THEN
		 dbms_output.put_line('5');
          EXIT;
        END IF;
      ELSE
        str := SUBSTR (p_str, i, j - i);
        i := j + len1;
        PIPE ROW (str);
		 dbms_output.put_line('6');
      END IF;
    END LOOP;
      RETURN;
    END fn_split;


 function maskcheck(a_ipmask in varchar2, a_checkip in varchar2 := null)
 return varchar2 is
  ip   varchar2(64);  ----登录IP的二进制流
  cmp  varchar2(64);  ----IP_rang_list中的IP二进制流
  mask number;        ----mask的位数
  n    number;        ----/的位置,如1.0.0.255/8，则n=10
  numFlag number;     ----是否为数字字符串
  type ty_str_split IS TABLE OF VARCHAR2 (64);
  arr2 ty_str_split := ty_str_split(); ----arr2存放IP地址拆分后的数据

  begin
  if a_ipmask = a_checkip then   --输入IP和IP_RANGE_LIST中IP一致返回1
     dbms_output.put_line('7');
	return '1';
  end if;
  -----------取得网络IP，网络位数 begin--------------------------
  n := instr(a_ipmask, '/');
  if n > 0 then                    ---取得/前面的IP和子网网络位的位数
     mask := to_number(substr(a_ipmask, n + 1));
     if mask<0 or mask >32 then    ---子网网络位数不在0-32之间
        dbms_output.put_line('8');
	   return '0';
     end if;
     if n = 1 then  ---如果只有/,则返回
	  dbms_output.put_line('9');
        return '0';
     end if;
     ip := substr(a_ipmask, 1, n-1);
  else
     mask := 0;
     ip := a_ipmask;
	  dbms_output.put_line('10');
  end if;
  -----------取得网络IP，网络位数 end--------------------------

  -----------将IP根据分隔符.进行拆分，并判断是否符合要求 begin-------
  select column_value bulk collect into arr2 from table(fn_split(ip, '.'));
  if arr2.count <> 4 then ---如果IP不是10.164.130.108这样被.分割成四部分，则返回
   dbms_output.put_line('11');
     return 0;
  end if;
  ip := '';
  for z in 1..arr2.count loop  --ip转化为二进制
     numFlag := isNumber(arr2(z));
     if numFlag = 0 or arr2(z)<0 or arr2(z)>255 then  ----如果分割后的数据，每部分不在0-255之间，或不是数字字符串，则返回
	  dbms_output.put_line('12');
        return 0;
     end if;
     ip := ip || substr(lpad(nvl(d2b(to_number(arr2(z))),'0'),8,'0'),0,8) ;
  end loop;
  if a_checkip is null then
    if mask > 0 then
      ip :=  substr(ip,1,mask);
	   dbms_output.put_line('13');
    end if ;
    return ip;
  else
    cmp := maskcheck(a_checkip);
    if mask > 0 and substr(cmp,1,mask) = substr(ip,1,mask) then
	 dbms_output.put_line('14');
      return '1';
    else
	 dbms_output.put_line('15');
      return '0';
    end if;
  end if;
  -----------将IP根据分隔符.进行拆分，并判断是否符合要求 end-------
end maskcheck;

end pkg_MaskCheck;


$


  CREATE OR REPLACE PACKAGE BODY "PKG_RERATING" AS
  /*******************************************************************************
  作    者:裘学欣39824
  创建时间:2008-06-16  CCBS CHG V300R001C01B07
  修改时间:
  --------------------------------------------------------------------------------
  主要功能:SRS.FUNC.006.001 话单回收控制改造
  *******************************************************************************/

  /*******************************************************************************
  Function:       p_Distask(a_Taskid NUMBER，a_RowCount out NUMBER)
  Description:    回收子任务分配
  Input:          a_Taskid 回收任务编号
  Output:         a_RowCount 分配的用户个数
  Return:         N/A
  *******************************************************************************/
  PROCEDURE p_Distask(a_Taskid   NUMBER,
                      a_Rowcount OUT NUMBER) AS
    v_All_Rows  NUMBER(8); --回收用户表行数
    v_Billcycle NUMBER(8); --任务表帐务周期
    v_Taskcount NUMBER(3); --子任务个数
    v_Status    NUMBER(3); --任务状态
    v_Childid   NUMBER(3); --子任务编号
    v_Set_Rows  NUMBER(8); --已分配行数
    v_Avg_Rows  NUMBER(8); --平均分配行数
  BEGIN

    --按传入的回收任务编号检查回收用户表数据
    SELECT COUNT(1)
      INTO v_All_Rows
      FROM Rerating_User
     WHERE Taskid = a_Taskid
       AND Status = 0;

    IF v_All_Rows = 0 THEN
      a_Rowcount := 0;
	   dbms_output.put_line('1');
      RETURN;
    END IF;

    --取帐务周期和子任务编号
    SELECT Billcycle,
           Taskcount,
           Status
      INTO v_Billcycle,
           v_Taskcount,
           v_Status
      FROM Rerating_Task
     WHERE Taskid = a_Taskid;

    IF v_Status > 0 THEN
      a_Rowcount := 0;
	   dbms_output.put_line('2');
      RETURN;
    END IF;

    --按地区查询用户资料表中的主机节点，填写资料内存节点
    UPDATE Rerating_User
       SET Nodeid    = f_Get_Nodeid(Region),
           Billcycle = v_Billcycle,
           Childid   = NULL
     WHERE Taskid = a_Taskid
       AND Status = 0;

    v_Avg_Rows := Ceil(v_All_Rows / v_Taskcount);

    --按分组查询各地区的回收用户条数，均匀分配子任务
    v_Childid  := 1;
    v_Set_Rows := 0;
    a_Rowcount := 0;
    FOR C2 IN (SELECT Nodeid,
                      Region,
                      COUNT(1) Sub_Rows
                 FROM Rerating_User
                WHERE Taskid = a_Taskid
                  AND Status = 0
                  AND Childid IS NULL
                GROUP BY Nodeid,
                         Region) LOOP
      BEGIN
        WHILE C2.Sub_Rows > 0 LOOP
          BEGIN
            IF C2.Sub_Rows <= (v_Avg_Rows - v_Set_Rows) THEN
              UPDATE Rerating_User
                 SET Childid = v_Childid
               WHERE Taskid = a_Taskid
                 AND Status = 0
                 AND Childid IS NULL
                 AND Nodeid = C2.Nodeid
                 AND Region = C2.Region;
              v_Set_Rows  := v_Set_Rows + C2.Sub_Rows;
              a_Rowcount  := a_Rowcount + C2.Sub_Rows;
              C2.Sub_Rows := 0;
			   dbms_output.put_line('3');
            ELSE
              v_Set_Rows := v_Avg_Rows - v_Set_Rows;
              UPDATE Rerating_User
                 SET Childid = v_Childid
               WHERE Taskid = a_Taskid
                 AND Status = 0
                 AND Childid IS NULL
                 AND Nodeid = C2.Nodeid
                 AND Region = C2.Region
                 AND Rownum <= v_Set_Rows;
              IF v_Childid < v_Taskcount THEN
                v_Childid := v_Childid + 1;
				 dbms_output.put_line('4');
              END IF;
              C2.Sub_Rows := C2.Sub_Rows - v_Set_Rows;
              a_Rowcount  := a_Rowcount + v_Set_Rows;
              v_Set_Rows  := 0;
            END IF;
          END;
        END LOOP;
      END;
    END LOOP;
    RETURN;
  END;

  /*******************************************************************************
  Function:       p_Genparam(a_Taskid NUMBER， a_Param out Varchar2)
  Description:    根据回收任务和回收类型生成回收参数
  Input:          a_Taskid回收任务编号
  Output:         a_Param 回收参数
  Return:         N/A
  *******************************************************************************/
  PROCEDURE p_Genparam(a_Taskid NUMBER,
                       a_Param  OUT VARCHAR2) AS
    v_Rerating_Type Rerating_Type%ROWTYPE;
    v_Rerating_Task Rerating_Task%ROWTYPE;
    v_Isdelaycdr    VARCHAR2(2);
  BEGIN

    --取参数数据
    SELECT *
      INTO v_Rerating_Task
      FROM Rerating_Task
     WHERE Taskid = a_Taskid;

    SELECT *
      INTO v_Rerating_Type
      FROM Rerating_Type
     WHERE Typeid = v_Rerating_Task.Typeid
       AND Inuse = 1;

    --取常量8888
    SELECT VALUE
      INTO v_Isdelaycdr
      FROM Const_Def
     WHERE Const_Id = 8888;

    IF To_Number(v_Isdelaycdr) = 1
       AND v_Rerating_Type.Charging_Module IS NULL THEN
      a_Param := '';
	   dbms_output.put_line('5');
      RETURN;
    END IF;

    --组合回收参数
    a_Param := a_Taskid || '%c0%cbill_qm%c' || v_Rerating_Type.Eventid || '%c' ||
               v_Rerating_Type.Undup_Module || '%c' ||
               v_Rerating_Type.Check_Dup || '%c' ||
               v_Rerating_Type.Rollback_From || '%c' ||
               f_Get_Tablename(a_Taskid) || '%c%s%c' ||
               v_Rerating_Type.Clear_Cycle || '%c' ||
               v_Rerating_Type.Flow_Module || '%c' ||
               v_Rerating_Type.Stat_Module;

    IF To_Number(v_Isdelaycdr) = 1 THEN
      a_Param := a_Param || '%c' || v_Rerating_Type.Charging_Module;
	   dbms_output.put_line('6');
    END IF;

    IF v_Rerating_Type.Field_Prefix IS NOT NULL THEN
      a_Param := a_Param || '%cfield_prefix=' || v_Rerating_Type.Field_Prefix;
	   dbms_output.put_line('7');
    END IF;

    IF v_Rerating_Type.Format_Proc > 0 THEN
      a_Param := a_Param || '%cformat_proc=' || v_Rerating_Type.Format_Proc;
	   dbms_output.put_line('8');
    END IF;

    IF v_Rerating_Type.Copyattrflag = 1 THEN
      a_Param := a_Param || '%crollback_copy_usages=' ||
                 v_Rerating_Type.Copyattrflag;
				  dbms_output.put_line('9');
    END IF;

    RETURN;

  EXCEPTION
    WHEN OTHERS THEN
      a_Param := '';
      RETURN;
  END;

  /*******************************************************************************
  Function:       p_Genwhere(a_Taskid NUMBER，a_Childid NUMBER，
                  a_Nodeid NUMBER, a_Where out Varchar2)
  Description:    根据回收任务、回收子任务、回收用户生成回收条件
  Input:          a_Taskid 回收任务编号，a_Childid 回收子任务编号，a_Nodeid 回收节点
  Output:         a_Where 回收条件
  Return:         N/A
  *******************************************************************************/
  PROCEDURE p_Genwhere(a_Taskid  NUMBER,
                       a_Childid NUMBER,
                       a_Nodeid  NUMBER,
                       a_Where   IN OUT VARCHAR2) AS
    v_Rerating_Type Rerating_Type%ROWTYPE;
    v_Rerating_Task Rerating_Task%ROWTYPE;
    v_User_List     VARCHAR2(800) := '';
    v_Command       VARCHAR2(2000) := '';
    v_Billcycle     VARCHAR2(32) := '';
    v_Region        VARCHAR2(32) := '';
    v_Userid        VARCHAR2(32) := '';
    v_Starttime     VARCHAR2(32) := '';
    v_Userattr      VARCHAR2(128) := '';
    v_Batchid       NUMBER(10) := 0;
    v_Rowid         VARCHAR2(32) := '';
    v_Count         NUMBER(4) := 0;
    v_Isreal        BOOLEAN := FALSE;
    v_Batchregion   VARCHAR2(32) := 'Init';
  BEGIN
    SELECT *
      INTO v_Rerating_Task
      FROM Rerating_Task
     WHERE Taskid = a_Taskid;
    SELECT *
      INTO v_Rerating_Type
      FROM Rerating_Type
     WHERE Typeid = v_Rerating_Task.Typeid;

    IF a_Where = 'REAL' THEN
      v_Isreal := TRUE;
      SELECT MAX(Batchid),
             MAX(Command),
             MAX(ROWID)
        INTO v_Batchid,
             v_Command,
             v_Rowid
        FROM Rerating_Task_Log
       WHERE Taskid = a_Taskid
         AND Childid = a_Childid
         AND Status = 0
         AND Rownum = 1;
      IF v_Batchid IS NULL THEN
        SELECT Seq_Rerating_Batchid.NEXTVAL
          INTO v_Batchid
          FROM Dual;
		   dbms_output.put_line('10');
      ELSE
        UPDATE Rerating_Task_Log
           SET Status  = 2,
               Endtime = to_date('20201022','yyyymmdd')
         WHERE ROWID = Chartorowid(v_Rowid);
        COMMIT;
        a_Where := Lpad(v_Batchid, 10, 0) || ' ' || v_Command;
		 dbms_output.put_line('11');
        RETURN;
      END IF;
    END IF;
    IF v_Rerating_Task.Status = 2 THEN
      v_Batchid := 0;
      a_Where   := 'is over.';
	   dbms_output.put_line('12');
      GOTO Addpatchid;
    ELSE
      a_Where := '';
	   dbms_output.put_line('13');
    END IF;

    v_Billcycle := Af_Get_Token(v_Rerating_Type.Fieldname, ',');
    v_Region    := Af_Get_Token(v_Rerating_Type.Fieldname, ',');
    v_Userid    := Af_Get_Token(v_Rerating_Type.Fieldname, ',');
    v_Starttime := Af_Get_Token(v_Rerating_Type.Fieldname, ',');
    v_Userattr  := Af_Get_Token(v_Rerating_Type.Fieldname, ',');

    --取回收类型条件
    IF v_Rerating_Type.Rerating_Where IS NOT NULL THEN
      a_Where := v_Rerating_Type.Rerating_Where;
 dbms_output.put_line('14');
      --取回收任务条件
      IF v_Rerating_Task.Task_Where IS NOT NULL THEN
        a_Where := a_Where || ' and ' || v_Rerating_Task.Task_Where;
		 dbms_output.put_line('15');
      END IF;
    ELSE

      --取回收任务条件
      a_Where := v_Rerating_Task.Task_Where;
	   dbms_output.put_line('16');
    END IF;

    --处理测试设置
    IF v_Rerating_Task.Istest = 1 THEN
      IF a_Where IS NOT NULL THEN
        a_Where := a_Where || ' and rownum <=' || v_Rerating_Task.Testrows;
		 dbms_output.put_line('17');
      ELSE
        a_Where := 'rownum <=' || v_Rerating_Task.Testrows;
		 dbms_output.put_line('18');
      END IF;
    END IF;

    IF v_Userid IS NULL THEN
      v_Batchid := 1;
      a_Where   := 'is invalid, userid is not found.';
	   dbms_output.put_line('19');
      GOTO Addpatchid;
    END IF;

    IF v_Rerating_Task.Wheretype = 0 THEN

      --取用户列表
      FOR C1 IN (SELECT ROWID,
                        Region,
                        Userid,
                        Status,
                        Batchid,
                        Rownum
                   FROM Rerating_User
                  WHERE Taskid = a_Taskid
                    AND Childid = a_Childid
                    AND Nodeid = a_Nodeid
                    AND Status = 0
                    AND Batchid IS NULL
                    AND Rownum <= v_Rerating_Task.Batchuser
                  ORDER BY Nodeid,
                           Region,
                           Userid) LOOP

        IF v_Batchregion = 'Init' THEN
          v_Batchregion := C1.Region;
		   dbms_output.put_line('20');
        END IF;
        IF v_Batchregion <> C1.Region
           OR (v_Batchregion IS NULL AND C1.Region IS NOT NULL)
           OR (v_Batchregion IS NOT NULL AND C1.Region IS NULL) THEN
		   dbms_output.put_line('21');
          GOTO Endbatch;
        END IF;
        IF C1."Rownum" = 1 THEN
          v_User_List := '''' || C1.Userid || '''';
		  dbms_output.put_line('22');
        ELSE
          v_User_List := v_User_List || ',''' || C1.Userid || '''';
		  dbms_output.put_line('23');
        END IF;
        IF v_Isreal THEN
          UPDATE Rerating_User
             SET Batchid = v_Batchid
           WHERE ROWID = C1.ROWID;
		   dbms_output.put_line('24');
        END IF;
      END LOOP;

      <<endbatch>>
      IF v_Isreal THEN
	  dbms_output.put_line('25');
        COMMIT;
      END IF;

      IF v_User_List IS NOT NULL THEN
        v_User_List := v_Userid || ' in (' || v_User_List || ')';
		dbms_output.put_line('26');
      ELSE
        SELECT COUNT(1)
          INTO v_Count
          FROM Rerating_User
         WHERE Taskid = a_Taskid
           AND Billcycle = v_Rerating_Task.Billcycle
           AND Rownum = 1;

        IF v_Count = 0 THEN
          IF a_Childid = 0 THEN
            v_User_List := 'MOD(' || v_Userid || ', ' ||
                           v_Rerating_Task.Taskcount || ') = 0';
						   dbms_output.put_line('27');
          ELSE
            v_User_List := 'MOD(' || v_Userid || ', ' ||
                           v_Rerating_Task.Taskcount || ') = ' ||
                           (a_Childid - 1);
						   dbms_output.put_line('28');
          END IF;
        ELSE
          v_Batchid := 0;
          a_Where   := 'is not found.';
		  dbms_output.put_line('29');
          GOTO Addpatchid;
        END IF;
      END IF;

      IF v_User_List IS NOT NULL THEN
        IF v_Billcycle IS NOT NULL THEN
          v_User_List := v_Billcycle || '=''' || v_Rerating_Task.Billcycle ||
                         ''' and ' || v_User_List;
						 dbms_output.put_line('30');
        END IF;
        IF v_Region IS NOT NULL
           AND v_Batchregion IS NOT NULL
           AND v_Batchregion <> 'Init' THEN
          v_User_List := v_Region || '=''' || v_Batchregion || ''' and ' ||
                         v_User_List;
						 dbms_output.put_line('31');
        END IF;
      ELSE
        IF v_Billcycle IS NOT NULL THEN
          IF a_Where IS NOT NULL THEN
            a_Where := v_Billcycle || '=''' || v_Rerating_Task.Billcycle ||
                       ''' and ' || a_Where;
					   dbms_output.put_line('32');
          ELSE
            a_Where := v_Billcycle || '=''' || v_Rerating_Task.Billcycle || '''';
			dbms_output.put_line('33');
          END IF;
        END IF;
      END IF;
    ELSE
      IF v_Starttime IS NULL THEN
        v_Batchid := 1;
        a_Where   := 'is invalid, starttime is not found.';
		dbms_output.put_line('34');
        GOTO Addpatchid;
      END IF;

      SELECT COUNT(1)
        INTO v_Count
        FROM Rerating_User
       WHERE Taskid = a_Taskid
         AND Childid = a_Childid
         AND Nodeid = a_Nodeid
         AND Status = 0
         AND Rownum = 1;

      IF v_Count > 0 THEN
        IF v_Isreal THEN
          UPDATE Rerating_User
             SET Batchid = v_Batchid
           WHERE Taskid = a_Taskid
             AND Childid = a_Childid
             AND Nodeid = a_Nodeid
             AND Status = 0
             AND Rownum <= v_Rerating_Task.Batchuser;
          COMMIT;
          v_User_List := '(' || v_Region || ',' || v_Billcycle || ',' ||
                         v_Userid || ',' || v_Starttime || ',' || v_Userattr ||
                         ') in (select region,billcycle,userid,cdrtime,userattr from ' ||
                         'rerating_user where Batchid = ' || v_Batchid || ') ';
						 dbms_output.put_line('35');
        ELSE
          v_User_List := '(' || v_Region || ',' || v_Billcycle || ',' ||
                         v_Userid || ',' || v_Starttime || ',' || v_Userattr ||
                         ') in (select region,billcycle,userid,cdrtime,userattr from ' ||
                         'rerating_user where taskid = ' || a_Taskid ||
                         ' and childid = ' || a_Childid || ' and nodeid = ' ||
                         a_Nodeid || ' and rownum <= ' ||
                         v_Rerating_Task.Batchuser || ')';
						 dbms_output.put_line('36');
        END IF;
      ELSE
        v_Batchid := 0;
        a_Where   := 'is not found.';
		dbms_output.put_line('37');
        GOTO Addpatchid;
      END IF;
    END IF;

    IF a_Where IS NOT NULL THEN
      IF v_User_List IS NOT NULL THEN
        a_Where := v_User_List || ' and ' || a_Where;
		dbms_output.put_line('38');
      END IF;
    ELSE
      a_Where := v_User_List;
    END IF;

    <<addpatchid>>
    IF v_Isreal THEN
      a_Where := Lpad(v_Batchid, 10, 0) || ' " where ' || a_Where || '"';
	  dbms_output.put_line('39');
    ELSE
      a_Where := ' where ' || a_Where;
	  dbms_output.put_line('40');
    END IF;
    RETURN;
  END;

  /*******************************************************************************
  Function:       f_Get_Nodeid(a_Region NUMBER)
  Description:    取计费节点号函数
  Input:          a_Region 地区
  Output:         N/A
  Return:        节点号
  *******************************************************************************/
  FUNCTION f_Get_Nodeid(a_Region NUMBER) RETURN NUMBER AS
    v_Area   VARCHAR2(64);
    v_Region VARCHAR2(64);
  BEGIN
    FOR C1 IN (SELECT Node_Id,
                      Area_Id
                 FROM Entity_Mem_Region) LOOP
      BEGIN
        v_Area := C1.Area_Id;
        WHILE Length(v_Area) > 0 LOOP
          BEGIN
            v_Region := Af_Get_Token(v_Area, ',');
            IF a_Region = To_Number(v_Region) THEN
			dbms_output.put_line('41');
              RETURN C1.Node_Id;
            END IF;
          END;
        END LOOP;
      END;
    END LOOP;
    RETURN 0;
  EXCEPTION
    WHEN OTHERS THEN
      RETURN 0;
  END;

  /*******************************************************************************
  Function:       f_Get_Tablename(a_Taskid NUMBER)
  Description:    取回收话单表函数
  Input:          a_Taskid 回收任务编号
  Output:         N/A
  Return:        回收话单表名
  *******************************************************************************/
  FUNCTION f_Get_Tablename(a_Taskid NUMBER) RETURN VARCHAR2 AS
    v_Rerating_Type Rerating_Type%ROWTYPE;
    v_Rerating_Task Rerating_Task%ROWTYPE;
    v_Eventtype     Event_Store_Def%ROWTYPE;
    v_Source_Table  VARCHAR2(32);
  BEGIN
    --取参数数据
    SELECT *
      INTO v_Rerating_Task
      FROM Rerating_Task
     WHERE Taskid = a_Taskid;

    SELECT *
      INTO v_Rerating_Type
      FROM Rerating_Type
     WHERE Typeid = v_Rerating_Task.Typeid
       AND Inuse = 1;

    BEGIN
      SELECT *
        INTO v_Eventtype
        FROM Event_Store_Def
       WHERE Upper(Table_Name) = Upper(v_Rerating_Type.Cdrtable);
      IF v_Eventtype.Event_Id <> v_Rerating_Type.Eventid THEN
	  dbms_output.put_line('42');
        Raise_Application_Error(-20201,
                                'The Rerating_Type.eventid is not valid.');
      END IF;
      IF v_Eventtype.Cycle_Flag = 1 THEN
        v_Source_Table := v_Rerating_Type.Cdrtable || v_Rerating_Task.Billcycle;
		dbms_output.put_line('43');
      ELSIF v_Eventtype.Cycle_Flag = 2 THEN
        v_Source_Table := v_Rerating_Type.Cdrtable ||
                          Substr(v_Rerating_Task.Billcycle, 1, 6);
						  dbms_output.put_line('44');
      ELSE
        v_Source_Table := v_Rerating_Type.Cdrtable;
		dbms_output.put_line('45');
      END IF;
    EXCEPTION
      WHEN No_Data_Found THEN
	  dbms_output.put_line('46');
        v_Source_Table := v_Rerating_Type.Cdrtable;
    END;
    RETURN v_Source_Table;
  END;

  /*******************************************************************************
  Function:       f_Get_Operator()
  Description:    取操作员函数
  Input:          N/A
  Output:         N/A
  Return:        操作员
  *******************************************************************************/
  FUNCTION f_Get_Operator RETURN VARCHAR2 AS
  BEGIN
dbms_output.put_line('48');
    RETURN g_Operator;
  END;

  /*******************************************************************************
  Function:       p_Set_Operator(Operator VARCHAR2)
  Description:    设置操作员函数
  Input:          操作员
  Output:         N/A
  Return:         N/A
  *******************************************************************************/
  PROCEDURE p_Set_Operator(a_Operator VARCHAR2) AS
  BEGIN
    g_Operator := a_Operator;
	dbms_output.put_line('47');
  END;
END;


$


  CREATE OR REPLACE PACKAGE BODY "PKG_ROAM" IS

  PROCEDURE af_judge(v_date IN DATE) AS
    v_destfilename_out VARCHAR2(40);
    v_destfilename_now VARCHAR2(40);
    v_seq_out          NUMBER;
    v_seq_now          NUMBER;
    v_flag             NUMBER;
  
  BEGIN
  
    SELECT destfilename, flag
      INTO v_destfilename_out, v_flag
      FROM outer_log
     WHERE yearmonth = to_char(v_date, 'yyyymmdd');
    IF v_flag = 1
    THEN
	dbms_output.put_line('1');
      RETURN;
    ELSE
      v_seq_out := to_number(substr(v_destfilename_out, 7, 2));
      SELECT filename
        INTO v_destfilename_now
        FROM outertranslog
       WHERE yearmonth = to_char(to_date('20200924','yyyymmdd'), 'yyyymmdd')
         AND fileid = 8
         AND currentbit = 1;
      v_seq_now := to_number(substr(v_destfilename_now, 7, 2));
      IF v_seq_out = v_seq_now + 1
      THEN
        UPDATE outer_log
           SET flag = 1
         WHERE yearmonth = to_char(v_date, 'yyyymmdd');
        INSERT INTO gsm_roam_recu
          SELECT * FROM gsm_roam;
        COMMIT;
		dbms_output.put_line('2');
        RETURN;
      ELSIF v_seq_out > v_seq_now + 1
      THEN
	  dbms_output.put_line('3');
        RETURN;
      ELSE
        --表示有补传的现象产生
        --调用函数产生话单
        ap_gencdr(v_seq_now + 1);
        UPDATE outer_log
           SET flag = 1
         WHERE yearmonth = to_char(v_date, 'yyyymmdd');
        INSERT INTO gsm_roam_recu
          SELECT * FROM gsm_roam;
		  dbms_output.put_line('4');
        COMMIT;
        RETURN;
      
      END IF;
    END IF;
  END;
  PROCEDURE ap_gencdr(s_seq VARCHAR2 DEFAULT NULL) AS
    v_starttime DATE;
    v_day       VARCHAR2(8);
    v_seq       NUMBER;
    v_filename  VARCHAR2(40);
    l_sql       VARCHAR2(204);
    v_dur       NUMBER;
  BEGIN
    SELECT to_char(to_date('20200924','yyyymmdd'), 'yyyymmdd') INTO v_day FROM dual;
    l_sql := 'truncate table gsm_roam';
    DELETE FROM outer_log
     WHERE to_char(starttime, 'yyyymmdd') = to_char(to_date('20200924','yyyymmdd'), 'yyyymmdd');
    EXECUTE IMMEDIATE l_sql;
    IF s_seq IS NULL
	
    THEN
    
      SELECT ceil(dbms_random.VALUE(1, 48)) INTO v_seq FROM dual;
      --SELECT MOD(v_day, 48) + 1 INTO v_seq FROM dual;
	  dbms_output.put_line('5');
    ELSE
      v_seq := s_seq;
	  dbms_output.put_line('6');
    END IF;
  
    v_dur := ceil(dbms_random.VALUE(1, 300));
  
    SELECT to_date(v_day, 'yyyymmdd') + v_seq * 15 / 60 / 24 -
           v_dur * 1 / 60 / 60 / 24
      INTO v_starttime
      FROM dual;
  
    v_filename := REPLACE('D' || to_char(to_date('20200924','yyyymmdd'), 'mmdd') ||
                          to_char(v_seq, '000') || '.531',
                          ' ');
    INSERT INTO gsm_roam
      (cdrtype,
       roamtype,
       calltype,
       telnum,
       vregion,
       hregion,
       hmanage,
       othertelnum,
       othervregion,
       otherhregion,
       othermanage,
       othernettype,
       servermanage,
       imsi,
       msrn,
       intrunkgroup,
       outtrunkgroup,
       mscid,
       lac,
       cellid,
       starttime,
       duration,
       partialflag,
       servicetype,
       servicecode,
       rfpowercapability,
       otherlac,
       othercellid,
       localfee,
       roamfee,
       tollfee,
       ruraladdfee,
       tolladdfee,
       tolldiscount,
       billingcycle,
       sourfilename,
       subscriberid,
       accountid,
       destfilename,
       thirdtelnum,
       tariffflag,
       errorcode,
       specialtype,
       servicebasic,
       devicetype,
       trunkmanage,
       tollfee2,
       localfee_s,
       roamfee_s,
       tollfee_s,
       ruraladdfee_s,
       tolladdfee_s,
       tollfee2_s,
       processtime,
       total_free,
       imei,
       region,
       usertype,
       discount_info,
       ofee1,
       rollbackflag,
       oothertelnum,
       rothertelnum,
       res1)
    VALUES
      ('T',
       '21',
       '11',
       '13512019600',
       '531',
       '22',
       '1.22',
       '13953101968',
       '531',
       '531',
       '1.531',
       'G',
       'OTHER',
       '460002010109600',
       '8613741540518',
       'BSC7I',
       'BSC7O',
       '8613741540',
       '5311',
       '1E01',
       v_starttime,
       v_dur + 34,
       '0',
       '00',
       '00',
       '',
       'BADB4B',
       '',
       0.000,
       1.800,
       0.000,
       0.000,
       0.000,
       531.000,
       substr(to_char(v_starttime, 'yyyymm'), 1, 6),
       'JN09.20080602.TTFILE1090',
       '',
       '',
       NULL,
       '',
       '000000B00000000',
       '',
       '00',
       '00',
       2,
       '',
       0.000,
       0.000,
       1.800,
       0.000,
       0.000,
       0.000,
       0.000,
       to_date('02-06-2008 13:30:17', 'dd-mm-yyyy hh24:mi:ss'),
       'gotone',
       '355392009397310',
       '',
       '',
       '',
       0.00,
       '',
       '13953101968',
       '13953101968',
       '5');
  
    INSERT INTO outer_log
      (yearmonth, telnum, starttime, duration, destfilename, flag)
    VALUES
      (to_char(to_date('20200924','yyyymmdd'), 'yyyymmdd'),
       '13808780074',
       v_starttime,
       23,
       v_filename,
       0);
    COMMIT;
  END;
END pkg_roam;

$



  CREATE OR REPLACE PACKAGE BODY "PKG_TEMPALTE_DEF" is

PROCEDURE AP_TempalteDataImport(p_Template_ID IN NUMBER, --模板编号
                                p_Tempalte_Type IN varchar2,
                                p_Rule_ID   IN VARCHAR2,
                                n_Success out number,
                                s_OutInfo out varchar2)is
  v_TariffType number := 0; --该参数为识别资费相关模板用
BEGIN
   if p_Tempalte_Type = 'TARIFF_PLAN'  --资费政策模板
   THEN
       v_TariffType := 1;
	   dbms_output.put_line('1');
   ELSIF p_Tempalte_Type = 'TARIFF_PLAN_ITEM'  --资费政策项模板
   THEN
       v_TariffType := 2;
	   dbms_output.put_line('2');
   ELSIF p_Tempalte_Type = 'TARIFF_SCHEMA'   --资费模式模板
   THEN
       v_TariffType := 3;
	   dbms_output.put_line('3');
   ELSIF p_Tempalte_Type = 'TARIFF_ITEM'  -- 资费政策项模板，实际是不存在的
   THEN
       v_TariffType := 4;
	   dbms_output.put_line('4');
   END IF;
   IF v_TariffType <> 0 THEN
       PKG_TEMPALTE_DEF.AP_TariffExport(p_Template_ID,v_TariffType, p_Rule_ID);
       n_Success := 1;
       s_OutInfo := 'Seccess';
	   dbms_output.put_line('5');
   ELSE
       dbms_output.put_line('The params input have wrong');
       s_OutInfo := 'Input param wrong ';
       n_Success := 0;
	   dbms_output.put_line('6');
   END IF;
   exception
        when others then
            dbms_output.put_line( 'Import data fail' );
            s_OutInfo := 'Unknow wrong';
            n_Success := 0;
			dbms_output.put_line('7');
END AP_TempalteDataImport;

PROCEDURE Ap_CreateTemplateCatalogId(n_Tree_Node_Id IN OUT NUMBER) IS
    v_Begid  NUMBER(8);
    v_Endid  NUMBER(8);
    v_Maxid  NUMBER(8);
    v_Count  NUMBER(8);
    v_Amount NUMBER(8);
BEGIN
    SELECT MAX(Tree_Node_Id) INTO v_Maxid FROM Template_Catalog;
    IF (v_Maxid = 99999999) THEN
        n_Tree_Node_Id := 0;
		dbms_output.put_line('8');
        RETURN;
    ELSIF (v_Maxid IS NULL) THEN
        n_Tree_Node_Id := 0;
		dbms_output.put_line('9');
        RETURN;
    END IF;
    SELECT COUNT(Tree_Node_Id)
      INTO v_Count
      FROM Template_Catalog
     WHERE Tree_Node_Id > 0;
    IF (v_Maxid = v_Count) THEN
        n_Tree_Node_Id := v_Maxid + 1;
		dbms_output.put_line('10');
        RETURN;
    END IF;
    v_Begid  := 1;
    v_Endid  := v_Maxid;
    v_Count  := 0;
    v_Amount := 0;
    LOOP
        SELECT COUNT(Tree_Node_Id)
          INTO v_Count
          FROM Template_Catalog
         WHERE Tree_Node_Id >= v_Begid
           AND Tree_Node_Id <= v_Endid;
        IF (v_Count = 1) THEN
            SELECT COUNT(Tree_Node_Id)
              INTO v_Count
              FROM Template_Catalog
             WHERE Tree_Node_Id = v_Begid;
            IF (v_Count = 0) THEN
                n_Tree_Node_Id := v_Begid;
				dbms_output.put_line('11');
            ELSE
                n_Tree_Node_Id := v_Endid;
				dbms_output.put_line('12');
            END IF;
            EXIT;
        END IF;
        v_Amount := v_Endid - v_Begid + 1;
        IF (v_Amount > v_Count) THEN
            v_Maxid := v_Endid;
            v_Endid := v_Begid + Trunc(v_Amount / 2);
			dbms_output.put_line('13');
        ELSIF (v_Amount = v_Count) THEN
            v_Begid := v_Endid;
            v_Endid := v_Maxid;
			dbms_output.put_line('14');
        ELSE
            n_Tree_Node_Id := -1;
			dbms_output.put_line('15');
            EXIT;
        END IF;
    END LOOP;
END Ap_CreateTemplateCatalogId;


PROCEDURE AP_CreateTemplateInfoId(n_template_id in out number) IS
    v_begid  NUMBER(8);
    v_endid  NUMBER(8);
    v_maxid  NUMBER(8);
    v_count  NUMBER(8);
    v_amount NUMBER(8);
BEGIN
    SELECT MAX(template_id) INTO v_maxid FROM template_info;
    IF (v_maxid = 99999999) THEN
        n_template_id := 0;
		dbms_output.put_line('16');
        RETURN;
    ELSIF (v_maxid IS null) THEN
        n_template_id := 1;
		dbms_output.put_line('17');
        RETURN;
    END IF;
    SELECT COUNT(template_id) INTO v_count FROM template_info;
    IF (v_maxid = v_count) THEN
        n_template_id := v_maxid + 1;
		dbms_output.put_line('18');
        RETURN;
    END IF;
    v_begid  := 1;
    v_endid  := v_maxid;
    v_count  := 0;
    v_amount := 0;
    LOOP
        SELECT COUNT(template_id)
          INTO v_count
          FROM template_info
         WHERE template_id >= v_begid
           AND template_id <= v_endid;
        IF (v_count = 1) THEN
            SELECT COUNT(template_id)
              INTO v_count
              FROM template_info
             WHERE template_id = v_begid;
            IF (v_count = 0) THEN
                n_template_id := v_begid;
				dbms_output.put_line('19');
            ELSE
                n_template_id := v_endid;
				dbms_output.put_line('20');
            END IF;
            EXIT;
        END IF;
        v_amount := v_endid - v_begid + 1;
        IF (v_amount > v_count) THEN
            v_maxid := v_endid;
            v_endid := v_begid + trunc(v_amount / 2);
			dbms_output.put_line('21');
        ELSIF (v_amount = v_count) THEN
            v_begid := v_endid;
            v_endid := v_maxid;
			dbms_output.put_line('22');
        ELSE
            n_template_id := -1;
			dbms_output.put_line('23');
            EXIT;
        END IF;
    END LOOP;
END AP_CreateTemplateInfoId;



PROCEDURE AP_TariffPlan(p_TariffPlanId IN NUMBER, --资费政策编号
                                          p_TemplateId   IN NUMBER) IS
  v_Flag NUMBER(1) := 1; --表中是否存在数据（0：不存在；1：已存在；）
begin
  SELECT COUNT(tariff_plan_id)
    INTO v_flag
    FROM trd_tariff_plan
   WHERE tariff_plan_id = p_TariffPlanId
     AND template_id = p_TemplateId;
  IF (v_Flag = 0) THEN
    INSERT INTO trd_tariff_plan
  (tariff_plan_id, tariff_plan_name, is_recursive, precedence, process_before, process_after, plantype, note, precedence_accdisc, priceplantype, region, template_id)
      SELECT t.tariff_plan_id, t.tariff_plan_name, t.is_recursive, t.precedence, t.process_before, t.process_after, t.plantype, t.note, t.precedence_accdisc, t.priceplantype, t.region, p_TemplateId
        FROM tariff_plan t
       WHERE tariff_plan_id = p_TariffPlanId;
	   dbms_output.put_line('24');
  END IF;
  FOR cur IN (SELECT *
                FROM tariff_plan_item
               WHERE tariff_plan_id = p_TariffPlanId) LOOP
    --DBMS_OUTPUT.PUT_LINE('AP_TariffPlan::Beg************************');
    --DBMS_OUTPUT.PUT_LINE('AP_TariffPlan::trd_tariff_plan_item:tariff_plan_item_sn = '||cur.tariff_plan_item_sn);
    SELECT COUNT(tariff_plan_id)
      INTO v_flag
      FROM trd_tariff_plan_item
     WHERE tariff_plan_item_sn = cur.tariff_plan_item_sn
     AND template_id = p_TemplateId;
    --DBMS_OUTPUT.PUT_LINE('AP_TariffPlan::trd_tariff_plan_item:v_flag='||v_flag);
    IF (v_flag = 0) THEN
      INSERT INTO trd_tariff_plan_item
        (tariff_plan_id,
         calctype,
         fee_id,
         tariff_schema_id,
         charging_event_id,
         precedence,
         applytime,
         expiretime,
         apply_method,
         discount_type,
         base_fee_id,
         discount_fee_id,
         charging_cond,
         switch_method,
         switch_unit,
         g_charging_cond,
         billcode,
         select_method,
         tariff_plan_item_sn,
         tariff_plan_item_name,
         serv_id,
         template_id)
      VALUES
        (cur.tariff_plan_id,
         cur.calctype,
         cur.fee_id,
         cur.tariff_schema_id,
         cur.charging_event_id,
         cur.precedence,
         cur.applytime,
         cur.expiretime,
         cur.apply_method,
         cur.discount_type,
         cur.base_fee_id,
         cur.discount_fee_id,
         cur.charging_cond,
         cur.switch_method,
         cur.switch_unit,
         cur.g_charging_cond,
         cur.billcode,
         cur.select_method,
         cur.tariff_plan_item_sn,
         cur.tariff_plan_item_name,
         cur.serv_id,
         p_TemplateId);
		 dbms_output.put_line('25');
    END IF;
    --DBMS_OUTPUT.PUT_LINE('AP_TariffPlan::AP_TariffSchema('|| cur.tariff_schema_id ||')');
    AP_TariffSchema(p_TariffSchemaId => cur.tariff_schema_id,
                    p_TemplateId     => p_TemplateId);
    --DBMS_OUTPUT.PUT_LINE('AP_TariffPlan::End************************');
  END LOOP;
END AP_TariffPlan;

  /*
  call AP_TariffPlan(1);
  commit;
  */

PROCEDURE AP_TariffSchema(p_TariffSchemaId IN NUMBER, --资费模式编号
                                            p_TemplateId     IN NUMBER) IS
  v_shemaids varchar2(2048) := ''; --工作变量，保存资费模式号 以"，"号分割
  v_sql      varchar2(3000);

  PROCEDURE Get_Schema_IDS(schema_id number) AS
  BEGIN
    FOR shemaid_row IN (SELECT tariff_id
                          FROM tariff_item
                         WHERE subtariff_type = 2
                           AND tariff_schema_id = schema_id
                         ORDER BY tariff_id) LOOP
      --首先判断是否已经存在，防止重复增加的情况
      IF (INSTR(v_shemaids, shemaid_row.tariff_id, 1) = 0) THEN
        v_shemaids := v_shemaids || ',''' || shemaid_row.TARIFF_ID || '''';
        Get_Schema_IDS(shemaid_row.tariff_id);
		dbms_output.put_line('26');
      END IF;
    END LOOP;
  END; --内部函数 Get_Schema_IDS 结束
BEGIN
  v_shemaids := p_TariffSchemaId;

  Get_Schema_IDS(p_TariffSchemaId);

  --DBMS_OUTPUT.PUT_LINE(v_shemaids);
  v_sql := 'DELETE FROM trd_tariff_schema WHERE tariff_schema_id in (' ||
           REPLACE(v_shemaids, '''') || ')';
  --DBMS_OUTPUT.PUT_LINE(v_sql);
  EXECUTE IMMEDIATE v_sql;
  v_sql := 'INSERT INTO trd_tariff_schema(tariff_schema_id, tariff_name, tariff_type, fieldcount, field_def, match_order, match_type, apply_method, refid, discount_fee_id, g_field_def, round_method, round_scale, ref_offset, event_id, billcode_order, createtime, region, template_id) SELECT tariff_schema_id, tariff_name, tariff_type, fieldcount, field_def, match_order, match_type, apply_method, refid, discount_fee_id, g_field_def, round_method, round_scale, ref_offset, event_id, billcode_order, createtime, region, ' || p_TemplateId ||
           ' FROM tariff_schema t WHERE tariff_schema_id in (' ||
           v_shemaids || ')';
  EXECUTE IMMEDIATE v_sql;
  v_sql := 'DELETE FROM trd_tariff_item WHERE tariff_schema_id in (' ||
           v_shemaids || ')';
  EXECUTE IMMEDIATE v_sql;
  v_sql := 'INSERT INTO trd_tariff_item (tariff_schema_id, applytime, expiretime, tariff_criteria, subtariff_type, tariff_id, ratio, ratetype, param_string, precedence, expr_id, g_param, is_dynamic, g_criteria, billcode, tariff_item_sn, tariff_item_name, template_id) SELECT tariff_schema_id, applytime, expiretime, tariff_criteria, subtariff_type, tariff_id, ratio, ratetype, param_string, precedence, expr_id, g_param, is_dynamic, g_criteria, billcode, tariff_item_sn, tariff_item_name, ' || p_TemplateId ||
           ' FROM tariff_item t WHERE tariff_schema_id in (' || v_shemaids || ')';
  EXECUTE IMMEDIATE v_sql;
  dbms_output.put_line('27');
END AP_TariffSchema;
/*
  CALL AP_TariffSchema(4,10000);
  COMMIT;
  */

PROCEDURE AP_TariffExport(p_TemplateId IN NUMBER, --模板编号
                                            p_TariffType IN NUMBER DEFAULT 1, --资费类型（1：资费政策；2：资费政策项；3：资费模式；4：资费模式项）
                                            p_TariffId   IN VARCHAR) AS
  v_Flag       NUMBER(1) := 1; --表中是否存在数据（0：不存在；1：已存在；）
  v_TariffId   tariff_schema.tariff_schema_id%TYPE; --资费模式编号
  v_TariffType tariff_item.subtariff_type%TYPE; --下级资费类型（2：资费模式；）
BEGIN
  CASE p_TariffType
    WHEN 1 THEN
      --资费政策
      PKG_TEMPALTE_DEF.AP_TariffPlan(TO_NUMBER(p_TariffId), p_TemplateId);
	  dbms_output.put_line('28');
    WHEN 2 THEN
      --资费政策项
      SELECT COUNT(tariff_plan_item_sn)
        INTO v_flag
        FROM trd_tariff_plan_item
       WHERE tariff_plan_item_sn = p_TariffId
         AND template_id = p_TemplateId;
      IF (v_Flag = 0) THEN
        /*INSERT INTO trd_tariff_plan_item
          SELECT t.*, p_TemplateId
            FROM tariff_plan_item t
           WHERE tariff_plan_item_sn = p_TariffId;*/
		  INSERT INTO trd_tariff_plan_item
    (TARIFF_PLAN_ID,
     CALCTYPE,
     FEE_ID,
     TARIFF_SCHEMA_ID,
     CHARGING_EVENT_ID,
     PRECEDENCE,
     APPLYTIME,
     EXPIRETIME,
     APPLY_METHOD,
     DISCOUNT_TYPE,
     BASE_FEE_ID,
     DISCOUNT_FEE_ID,
     CHARGING_COND,
     SWITCH_METHOD,
     SWITCH_UNIT,
     G_CHARGING_COND,
     BILLCODE,
     SELECT_METHOD,
     TARIFF_PLAN_ITEM_NAME,
     SERV_ID,
     TARIFF_PLAN_ITEM_SN,
     TEMPLATE_ID)
    SELECT t.TARIFF_PLAN_ID,
           t.CALCTYPE,
           t.FEE_ID,
           t.TARIFF_SCHEMA_ID,
           t.CHARGING_EVENT_ID,
           t.PRECEDENCE,
           t.APPLYTIME,
           t.EXPIRETIME,
           t.APPLY_METHOD,
           t.DISCOUNT_TYPE,
           t.BASE_FEE_ID,
           t.DISCOUNT_FEE_ID,
           t.CHARGING_COND,
           t.SWITCH_METHOD,
           t.SWITCH_UNIT,
           t.G_CHARGING_COND,
           t.BILLCODE,
           t.SELECT_METHOD,
           t.TARIFF_PLAN_ITEM_NAME,
           t.SERV_ID,
           t.TARIFF_PLAN_ITEM_SN,
           p_TemplateId
      FROM tariff_plan_item t
     WHERE t.tariff_plan_item_sn = p_TariffId;
	 dbms_output.put_line('29');
      END IF;
      SELECT tariff_schema_id
        INTO v_TariffId
        FROM tariff_plan_item
       WHERE tariff_plan_item_sn = p_TariffId;
      PKG_TEMPALTE_DEF.AP_TariffSchema(v_TariffId, p_TemplateId);
    WHEN 3 THEN
      --资费模式
      PKG_TEMPALTE_DEF.AP_TariffSchema(TO_NUMBER(p_TariffId), p_TemplateId);
	  dbms_output.put_line('30');
    WHEN 4 THEN
      --资费模式项
      SELECT COUNT(tariff_item_sn)
        INTO v_flag
        FROM trd_tariff_item
       WHERE tariff_item_sn = p_TariffId
         AND template_id = p_TemplateId;
      IF (v_Flag = 0) THEN
        /*INSERT INTO trd_tariff_item
          SELECT t.*, p_TemplateId
            FROM tariff_item t
           WHERE tariff_item_sn = p_TariffId;*/
		INSERT INTO trd_tariff_item (tariff_schema_id, applytime, expiretime, tariff_criteria, subtariff_type, tariff_id, ratio, ratetype, param_string, precedence, expr_id, g_param, is_dynamic, g_criteria, billcode, tariff_item_sn, tariff_item_name, template_id) SELECT tariff_schema_id, applytime, expiretime, tariff_criteria, subtariff_type, tariff_id, ratio, ratetype, param_string, precedence, expr_id, g_param, is_dynamic, g_criteria, billcode, tariff_item_sn, tariff_item_name, p_TemplateId
        FROM tariff_item t WHERE tariff_item_sn = p_TariffId;
		dbms_output.put_line('31');
      END IF;
      SELECT subtariff_type, tariff_schema_id
        INTO v_TariffType, v_TariffId
        FROM tariff_item
       WHERE tariff_item_sn = p_TariffId;
      IF (v_TariffType = 2) THEN
        --是否为资费类型
        --DBMS_OUTPUT.PUT_LINE('AP_TariffExport::tariff_plan_item:subtariff_type=2');
        PKG_TEMPALTE_DEF.AP_TariffSchema(v_TariffId, p_TemplateId);
		dbms_output.put_line('32');
      END IF;
  END CASE;
END AP_TariffExport;



end PKG_TEMPALTE_DEF;


$

delimiter ;$


 
 
 delimiter $;
 
 CREATE OR REPLACE PACKAGE BODY "PKG_VALIDUSER" AS
 PROCEDURE Setuser(v_User IN VARCHAR2,
  v_Istriggerneed NUMBER := 1,
  v_clientinfo in varchar2 := '') AS
 v_Usertype NUMBER:=0; --V3R1C01B01 39824 2007-3-19
 v_Logonid NUMBER:=0; --V3R1C12L07n11 sKF42558 2012-07-10

 BEGIN
 Pkg_Validuser.Userid := v_User;
 Pkg_Validuser.Istriggerneed := v_Istriggerneed;
 ClientInfo := v_clientinfo;

 select to_char(to_date('20201103010101','yyyymmddhh24miss'), 'yyyymmddhh24miss') ||
 lpad(to_char(seq_logon.nextval), 8, '0')
 into Pkg_Validuser.LogonSeq
 from dual;
 begin
  SELECT Usertype
  INTO v_Usertype
  FROM App_User
 WHERE User_Code = v_User
  AND Rownum = 1;  --V3R1C01B01 39824 2007-3-19
 Setusertype(v_Usertype); --V3R1C01B01 39824 2007-3-19

 --V3R1C12L07n11 sKF42558 2012-07-10
 SELECT Seq_Billingclientlog.NEXTVAL INTO v_Logonid FROM Dual;
 SetLogonid(v_Logonid);
 --V3R1C12L07n11 sKF42558 2012-07-10
				dbms_output.put_line('1');
 exception
 when others then
  null;
 end;
		dbms_output.put_line('2');
 END;

 PROCEDURE Defaultuser(v_Istriggerneed NUMBER := 1) AS
 BEGIN
 Pkg_Validuser.Userid := '_SYS_';
 Pkg_Validuser.Istriggerneed := v_Istriggerneed;
 if Pkg_Validuser.LogonSeq = '0' then
 select to_char(to_date('20201103010101','yyyymmddhh24miss'), 'yyyymmddhh24miss') ||
 lpad(to_char(seq_logon.nextval), 8, '0')
 into Pkg_Validuser.LogonSeq
 from dual;
		dbms_output.put_line('3');
 end if;
 END;

 --V3R1C01B01 39824 2007-3-19 set user type
 --modify by zkf28147 2010-8-9 增加开发模式，如果为开发模式设置usertype为1(即用户类型为管理员)
 PROCEDURE Setusertype(v_Usertype NUMBER := 0) AS
 v_SystemMode VARCHAR2(64);
 BEGIN
 select value
 into v_SystemMode
 from feature_def
  where name = 'system_mode';
 if v_SystemMode = 'development' then
 Pkg_Validuser.Usertype := 1;
	 dbms_output.put_line('4');
 else
 Pkg_Validuser.Usertype := v_Usertype;
	 dbms_output.put_line('5');
 end if;
 exception
 when no_data_found then
 Pkg_Validuser.Usertype := v_Usertype;
	 dbms_output.put_line('6');
 END;

 --V3R1C01B01 39885 2007-03-15
 --V3R1C12L07n11 sKF42558 2012-07-10 set logonid
 PROCEDURE SetLogonid(v_Logonid NUMBER := 0) AS
 BEGIN
 Pkg_Validuser.Logonid := v_Logonid;
		 dbms_output.put_line('7');
 END;
 --V3R1C12L07n11 sKF42558 2012-07-10

 function BeginAuditLog(a_AuditLog in out AuditLogType) return number is
  v_SystemMode VARCHAR2(64);
 begin
 begin
		--add by zkf28147 2010-8-9 增加开发模式，日志记录登陆操作系统的用户名 begin
  select value into v_SystemMode from feature_def
  where name = 'system_mode';
  if v_SystemMode = 'development' then
 select sys_context('userenv', 'OS_USER')
 into Pkg_Validuser.Userid
 from dual;
		 dbms_output.put_line('8');
  end if;
 exception
  when no_data_found then
  a_AuditLog.Operator := pkg_validuser.userid;
	 dbms_output.put_line('9');
 end;
 --add by zkf28147 2010-8-9 增加开发模式，日志记录登陆操作系统的用户名 end

 if istriggerneed=0 then
		dbms_output.put_line('10');
  return 0;
 end if;

 if pkg_validuser.userid is null then
  --raise_application_error(-20101, 'invalid user, can''t change data.');
  --return;
  pkg_validuser.userid := '_SYS_';
			dbms_output.put_line('11');
 end if;

 --Get Client Infomation
 --select sys_context('userenv','IP_ADDRESS')||' : '||sys_context('userenv','HOST') into vAuditLog.ClientInfo from dual;
 if ClientInfo is null then
  a_AuditLog.ClientInfo := sys_context('userenv','IP_ADDRESS')||' : '||sys_context('userenv','HOST');
  ClientInfo := a_AuditLog.ClientInfo;
			dbms_output.put_line('12');
 else
  a_AuditLog.ClientInfo := ClientInfo;
			dbms_output.put_line('13');
 end if;

  a_AuditLog.Operator := pkg_validuser.userid;

/*
 else
  select max(user_code) into a_AuditLog.Operator from app_user where user_code=userid;
  if a_AuditLog.Operator is null then
 raise_application_error(-20102, 'invalid user, can''t change data.');
 return 1;
  end if;
 end if;
*/
 --get max formnum
 select to_char(seq_audit.nextval) into a_AuditLog.LogNum from dual;
 a_AuditLog.LogNum:=a_AuditLog.Operator||lpad(a_AuditLog.LogNum,8,'0');
 a_AuditLog.LogTime:=to_date('20201103010101','yyyymmddhh24miss');
 --a_AuditLog.TableName:=a_TableName;
 --a_AuditLog.MaintainType := a_ChangeType;
 if a_AuditLog.MaintainType = 'I' then
 a_AuditLog.KeyValue := '';
		 dbms_output.put_line('14');
 end if;
 return 1;
 end;

 procedure WriteAuditLog(a_AuditLog in AuditLogType,
  a_fieldname varchar2,
  a_oldvalue date,
  a_newvalue date)is
 begin
 WriteAuditLog(a_AuditLog, a_fieldname,
  to_char(a_oldvalue, 'yyyy-mm-dd hh24:mi:ss'),
  to_char(a_newvalue, 'yyyy-mm-dd hh24:mi:ss'));
					 dbms_output.put_line('15');
 end;

 procedure WriteAuditLog(a_AuditLog in AuditLogType,
  a_fieldname varchar2,
  a_oldvalue varchar2,
  a_newvalue varchar2)is
 begin
 if (a_newvalue is not null and a_oldvalue is null or
  a_newvalue is null and a_oldvalue is not null) or
  (a_newvalue != a_oldvalue) then
 if Pkg_Validuser.LogonSeq = '0' then
 select to_char(to_date('20201103010101','yyyymmddhh24miss'), 'yyyymmddhh24miss') ||
  lpad(to_char(seq_logon.nextval), 8, '0')
 into Pkg_Validuser.LogonSeq
 from dual;
		 dbms_output.put_line('16');
 end if;
 -- add 21525
 if pkg_validuser.Logonid = 0 then
  SELECT Seq_Billingclientlog.NEXTVAL INTO pkg_validuser.Logonid FROM Dual;
	 dbms_output.put_line('17');
 end if;
 -- end
 insert into audit_log
 (lognum,
 operator,
 logtime,
 tablename,
 maintaintype,
 keyvalue,
 colname,
 oldvalue,
 newvalue,
 clientinfo,
 logonid,
 LogonSeq,
 OPERNUM)
 values
 (a_AuditLog.LogNum,
 a_AuditLog.Operator,
 a_AuditLog.LogTime,
 a_AuditLog.TableName,
 a_AuditLog.MaintainType,
 a_AuditLog.KeyValue,
 a_fieldname,
 a_oldvalue,
 a_newvalue,
 pkg_validuser.clientinfo,
 pkg_validuser.Logonid,
 pkg_validuser.LogonSeq,
 a_AuditLog.OPERNUM);
		 dbms_output.put_line('18');
 end if;
 end;

 procedure EndAuditLog(a_AuditLog in out AuditLogType) is
 begin
 --notify to bpm background process
 update param_table_def
  set lastchangetime = to_date('20201022','yyyymmdd')
  where table_name = a_AuditLog.TableName;
	 dbms_output.put_line('19');
 end;
 --V3R1C01B01 39885 2007-03-15

 procedure GetLogonSeq(a_LogonSeq OUT VARCHAR2) is
 begin
 a_LogonSeq := Pkg_Validuser.LogonSeq;
	dbms_output.put_line('20');
 end;

 function GetUserid(a_userid in varchar2) return varchar2 is
 v_userid varchar2(20);
 begin
 v_userid := Pkg_Validuser.Userid;
	 dbms_output.put_line('21');
 return v_userid;
 end;
END Pkg_Validuser;


$


delimiter ;$






delimiter $;

  CREATE OR REPLACE PACKAGE BODY "PKG_WORKORDER" is

    procedure ap_workorder_to_tariff(
        a_workorder_id          in varchar2,    --work order id
        a_region                in number,      --region
        a_result                out number,     --1-success, -1-failure
        a_msg                   out varchar2    --result message
    )
    is
        v_count                 number;
        v_tariff_plan_id        number;
    begin
        --check whether exist tariff plan
        v_count := 0;
        select count(*) into v_count from tariff_plan_ucd
                            where workorder_id = a_workorder_id;

        if v_count = 0 then
            a_result := -1;
            a_msg := '工单【' || to_char(a_workorder_id) || '】无资费政策，配置不完整!';
			dbms_output.put_line('1');
            return;
        end if;

        --generate tariff plan
        for c1 in (select rowid,t.* from tariff_plan_ucd t
                            where workorder_id = a_workorder_id ) loop

            --获取资费政策编号
            ap_get_plan_id(v_tariff_plan_id, a_result, a_msg);

            if a_result = -1 then
			dbms_output.put_line('2');
                return;
            end if;

            --生成新的资费政策
            insert into tariff_plan
                   (tariff_plan_id,
                    tariff_plan_name,
                    precedence,
                    process_before,
                    process_after,
                    plantype,
                    note,
                    precedence_accdisc,
                    priceplantype,
                    region)
            values
                   (v_tariff_plan_id,               --资费政策标识
                    c1.tariff_plan_name,             --资费政策名称
                    c1.precedence,                   --优先级
                    c1.process_before,               --批价前处理方案
                    c1.process_after,                --批价后处理方案
                    c1.plantype,                     --计划类型
                    c1.note,                         --资费政策说明
                    c1.precedence_accdisc,           --帐务优惠优先级
                    c1.priceplantype,                --定价计划类型
                    a_region);                       --地区号，0表示全省


            --生成新的资费政策项
            ap_gen_tariff_plan_item(a_workorder_id, a_region, c1.tariff_plan_id, v_tariff_plan_id, a_result, a_msg);

            if a_result = -1 then
			dbms_output.put_line('3');
                return;
            end if;

            --tariff_plan_item_ucd
            update tariff_plan_item_ucd set tariff_plan_id = v_tariff_plan_id
                where workorder_id = a_workorder_id and  tariff_plan_id = c1.tariff_plan_id;

            --tariff_schema_ucd
            update tariff_schema_ucd set tariff_plan_id = v_tariff_plan_id
                where workorder_id = a_workorder_id and  tariff_plan_id = c1.tariff_plan_id;

            --tariff_item_ucd
            update tariff_item_ucd set tariff_plan_id = v_tariff_plan_id
                where workorder_id = a_workorder_id and  tariff_plan_id = c1.tariff_plan_id;


            --修改ucd表中资费政策编号
            update tariff_plan_ucd set tariff_plan_id = v_tariff_plan_id
                where rowid = c1.rowid;

             /*
             --优惠处理
             ap_audit_disc(a_workorder_id, c1.tariff_plan_id, a_result, a_msg);
             if a_result = -1 then
                 return;
             end if;
             */
        end loop;

        a_result := 1;
        a_msg := '生成资费政策成功!';
        return;
        exception when others then
            a_msg := sqlerrm;
            a_result := -1;
			dbms_output.put_line('4');
            return;
    end ap_workorder_to_tariff;

    --generate tariff plan item
    procedure ap_gen_tariff_plan_item(
        a_workorder_id          in  varchar2,   --work order id
        a_region                in  number,     --region
        a_tariff_plan_id        in  number,     --tariff plan id
        a_new_tariff_plan_id    in  number,     --new tariff plan id
        a_result                out number,     --1 - success, -1 - failure
        a_msg out varchar2                      --result message
    )
    is
        v_schema_id         tariff_schema.tariff_schema_id%type;
        v_plan_item_sn      tariff_plan_item.tariff_plan_item_sn%type;
        v_chargingcond      tariff_plan_item.charging_cond%type;
        v_gchargingcond     tariff_plan_item.g_charging_cond%type;
    begin

        --generate tariff plan item
        for c2 in (select a.*, a.rowid from tariff_plan_item_ucd a
                       where workorder_id = a_workorder_id
                             and tariff_plan_id = a_tariff_plan_id) loop
           --get max tariff schema
            v_schema_id := 0;
            select nvl(max(tariff_schema_id), 0) + 1 into v_schema_id  from tariff_schema;

            v_plan_item_sn := af_get_tariff_sn();

            --把业务表达式编号替换为表达式
            if c2.serv_id is null then
                v_chargingcond := '1';
                v_gchargingcond := '1';
				dbms_output.put_line('5');
            else
                v_chargingcond := chr(3) || '0015-.' || lpad(c2.serv_id, 4, 0) || chr(4);
                select '业务表达式.' || trim(t.serv_desc) into v_gchargingcond
                    from serv_expr t where t.serv_id = c2.serv_id and rownum = 1;
					dbms_output.put_line('6');
            end if;

            insert into tariff_plan_item
                (tariff_plan_id,
                 tariff_plan_item_sn,
                 calctype,
                 fee_id,
                 tariff_schema_id,
                 charging_event_id,
                 precedence,
                 applytime,
                 expiretime,
                 apply_method,
                 discount_type,
                 base_fee_id,
                 discount_fee_id,
                 charging_cond,
                 switch_method,
                 switch_unit,
                 g_charging_cond,
                 billcode,
                 select_method,
                 tariff_plan_item_name,
                 serv_id)
            values
                (a_new_tariff_plan_id,          --资费政策ID
                 v_plan_item_sn,                --资费政策项序号
                 1,                             --计算类型，1-按实际类型，2-周期性，3-一次性
                 c2.fee_id,                     --要计算的费用ID
                 v_schema_id,                   --资费模式ID
                 c2.charging_event_id,          --要计算的计费事件
                 c2.precedence,                 --优先级
                 c2.applytime,                  --生效时间
                 c2.expiretime,                 --失效时间
                 c2.apply_method,               --生效失效判断方法
                 c2.discount_type,              --计算模式
                 c2.base_fee_id,                --基准费用
                 c2.discount_fee_id,            --优惠费用
                 v_chargingcond,                --计费条件
                 c2.switch_method,              --切换方式
                 c2.switch_unit,                --切换单位
                 v_gchargingcond,               --计费条件描述
                 c2.billcode,                   --账目编码
                 c2.select_method,              --批价原则
                 c2.tariff_name,                --资费政策项名称
                 c2.serv_id);                   --业务表达式ID

            --generate tariff schema
            ap_gen_tariff_schema(a_workorder_id, a_region, a_tariff_plan_id, c2.tariff_schema_sn, c2.charging_event_id,
                                 v_schema_id,  a_result, a_msg);

            if  a_result = -1 then
			dbms_output.put_line('7');
                return;
            end if;

            --update tariff_plan_item_ucd
            update tariff_plan_item_ucd
                set tariff_schema_id = v_schema_id,
                    tariff_plan_item_sn = v_plan_item_sn
                        where rowid = c2.rowid;
        end loop;

        a_result := 1;
        a_msg := '生成资费政策项成功!';
        return;
        exception when others then
            a_msg := sqlerrm;
            a_result := -1;
			dbms_output.put_line('8');
            return;
    end ap_gen_tariff_plan_item;

    --generate tariff schema
    procedure ap_gen_tariff_schema(
        a_workorder_id          in  varchar2,   --work order id
        a_region                in  number,     --region
        a_tariff_plan_id        in  number,     --tariff plan id
        a_tariff_schema_sn      in  number,     --tariff schema serial number
        a_charging_event_id     in  number,     --charging event id
        a_tariff_schema_id      in  number,     --tariff schema id
        a_result                out number,     --1-success, -1-failure
        a_msg                   out varchar2    --result message
    )
    is
       v_count         number(4);
    begin
        --check tariff schema
        select count(*) into v_count from tariff_schema_ucd
            where workorder_id = a_workorder_id
                and tariff_plan_id = a_tariff_plan_id
                and tariff_schema_sn = a_tariff_schema_sn;

        if v_count = 0 then
            a_result := - 1;
            a_msg := '没有找到资费模式序号为：' || to_char(a_tariff_schema_sn) || '的资费模式!';
            --a_msg := 'not found tariff_schema which tariff_schema_sn = ' || to_char(a_tariff_schema_sn) || '!';
			dbms_output.put_line('9');
            return;
        end if;

        --check tariff schema item
        select count(*) into v_count from tariff_item_ucd
            where workorder_id = a_workorder_id
                and tariff_plan_id = a_tariff_plan_id
                and tariff_schema_sn = a_tariff_schema_sn;

        if v_count = 0 then
            a_result := - 1;
            a_msg := '资费模式项不能为空。';
			dbms_output.put_line('10');
            return;
        end if;

        --generate tariff schema
        for c3 in ( select t.*, t.rowid  from tariff_schema_ucd t
                    where workorder_id = a_workorder_id
                      and tariff_plan_id = a_tariff_plan_id
                      and tariff_schema_sn = a_tariff_schema_sn) loop

            --insert into tariff_schema
            insert into tariff_schema
                (tariff_schema_id,
                 tariff_name,
                 tariff_type,
                 fieldcount,
                 field_def,
                 match_order,
                 match_type,
                 apply_method,
                 refid,
                 discount_fee_id,
                 g_field_def,
                 round_method,
                 round_scale,
                 ref_offset,
                 event_id,
                 billcode_order,
                 region)
            values
                (a_tariff_schema_id,            --资费模式号
                 c3.tariff_name,                --资费模式名称
                 c3.tariff_type,                --资费模式类型
                 nvl(c3.fieldcount,0),          --参数数量
                 c3.field_def,                  --参数定义
                 c3.match_order,                --匹配顺序
                 c3.match_type,                 --匹配方法
                 c3.apply_method,               --应用时间判断方法
                 c3.refid,                      --标记位置
                 c3.discount_fee_id,            --优惠费用标识
                 c3.g_field_def,                --中文参数定义
                 c3.round_method,               --取整方法
                 c3.round_scale,                --取整精度
                 c3.ref_offset,                 --标记偏移
                 a_charging_event_id,           --事件Id
                 c3.billcode_order,             --帐目编码取值顺序
                 a_region);                     --地区号，0表示全省

            --update tariff_schema_ucd
            update tariff_schema_ucd
                set tariff_schema_id = a_tariff_schema_id where rowid = c3.rowid;

            --generate tariff schema item
            ap_gen_tariff_schema_item(a_workorder_id, a_region, a_tariff_plan_id, a_tariff_schema_sn,
                                      a_charging_event_id, a_tariff_schema_id, a_result, a_msg);

            if a_result = -1 then
			dbms_output.put_line('11');
                return;
            end if;

        end loop;

        a_result := 1;
        a_msg := 'generate tariff schema success!';
        return;
        exception when others then
            a_msg := sqlerrm;
            a_result := -1;
			dbms_output.put_line('12');
            return;
    end ap_gen_tariff_schema;

    --generate tariff schema item
    procedure ap_gen_tariff_schema_item(
        a_workorder_id          in  varchar2,   --work order id
        a_region                in  number,     --region
        a_tariff_plan_id        in  number,     --tariff plan id
        a_tariff_schema_sn      in  number,     --tariff schema serial number
        a_charging_event_id     in  number,     --charging event id
        a_tariff_schema_id      in  number,     --tariff schema id
        a_result                out number,     --1-success, -1-failure
        a_msg                   out varchar2    --result message
    )
    is
        v_subschema_id          number(9);
        v_tariff_item_sn        varchar2(20);
    begin
        --generate tariff schema item
        for c4 in (select t.*, t.rowid from  tariff_item_ucd t
                       where workorder_id = a_workorder_id
                             and tariff_plan_id = a_tariff_plan_id
                             and tariff_schema_sn = a_tariff_schema_sn) loop

            v_tariff_item_sn := af_get_tariff_sn();

            case c4.subtariff_type
                when  1  then  --Charge Rate
                    if   c4.subtariff_schema_sn is null  then
                        a_result := -1;
                        a_msg := '费率代码不能为空!';
						dbms_output.put_line('13');
                        return;
                    end if;

                    update tariff_item_ucd set tariff_id = c4.subtariff_schema_sn
                        where rowid = c4.rowid ;
                    v_subschema_id := c4.subtariff_schema_sn;

                when  2  then  --Tariff
                    --generate tariff schema
                    v_subschema_id := 0;
                    select nvl(max(tariff_schema_id), 0) + 1 into v_subschema_id  from tariff_schema;

                    ap_gen_tariff_schema(a_workorder_id, a_region, a_tariff_plan_id, c4.subtariff_schema_sn,
                                        a_charging_event_id, v_subschema_id, a_result, a_msg);

                    if  a_result = -1 then
					dbms_output.put_line('14');
                        return;
                    end if;

                    --update tariff_item_ucd
                    update tariff_item_ucd set tariff_id = v_subschema_id where rowid = c4.rowid;

                else --3 Fee Item, 4-Average Charge Rate, 5-Max Usage Fee
                    update tariff_item_ucd set tariff_id = c4.subtariff_schema_sn
                        where rowid = c4.rowid ;
                    v_subschema_id := c4.subtariff_schema_sn;
					dbms_output.put_line('15');
            end case;

            insert into tariff_item
                  (tariff_schema_id,
                   tariff_item_sn,
                   tariff_item_name,
                   applytime,
                   expiretime,
                   tariff_criteria,
                   subtariff_type,
                   tariff_id,
                   ratio,
                   ratetype,
                   param_string,
                   precedence,
                   g_param,
                   g_criteria,
                   billcode)
            values
                   (a_tariff_schema_id,                 --资费模式号
                   v_tariff_item_sn,                    --资费模式项序号
                   c4.node_name,                        --资费模式项名称
                   c4.applytime,                        --生效时间
                   c4.expiretime,                       --失效时间
                   c4.tariff_criteria,                  --资费条件
                   c4.subtariff_type,                   --下级资费类型
                   v_subschema_id,                      --下级资费号
                   c4.ratio,                            --比率
                   nvl(c4.ratetype, 'c'),               --资费类型
                   c4.param_string,                     --资费参数串
                   c4.precedence,                       --优先级
                   c4.g_param,                          --资费参数串中文注释
                   c4.g_criteria,                       --资费条件中文注释
                   c4.billcode);                        --账目编码
        end loop;

        a_result := 1;
        a_msg := 'generate tariff schema item successed!';
        return;
        exception when others then
            a_msg := sqlerrm;
            a_result := -1;
			dbms_output.put_line('16');
            return;
    end ap_gen_tariff_schema_item;

    --generate ucd tariff plan
    procedure ap_gen_tariff_plan_ucd(
        a_workorder_id          in  varchar2, --work order id
        a_tariff_plan_id        in  number,   --tariff plan id
        a_new_tariff_plan_id    in  number,   --new tariff plan id
        a_source_type           in  number,   --0 from table of tariff_plan
                                              --1 from table of tariff_plan_ucd
        a_result                out number,   --1-success, -1-failurre
        a_msg                   out varchar2  --result message
    )
    is

    begin

        --check work order and tariff plan
        ap_check_workorder(a_workorder_id, a_new_tariff_plan_id, a_result, a_msg);

        if a_result = -1 then
		dbms_output.put_line('17');
            return;
        end if;

        --generate tariff plan ucd
        if a_source_type = 0 then
            insert into tariff_plan_ucd
              (workorder_id, tariff_plan_id, tariff_plan_name, optional, precedence, precedence_accdisc, process_before, process_after, plantype, priceplantype, note)
            select  a_workorder_id, a_new_tariff_plan_id, tariff_plan_name, 1, precedence, precedence_accdisc, process_before, process_after, plantype, priceplantype, note
                from tariff_plan where tariff_plan_id = a_tariff_plan_id;
				dbms_output.put_line('18');
        else
            insert into tariff_plan_ucd
              (workorder_id, tariff_plan_id, tariff_plan_name, optional, precedence, precedence_accdisc, process_before, process_after, plantype, priceplantype, note)
            select  a_workorder_id, a_new_tariff_plan_id, tariff_plan_name, optional, precedence, precedence_accdisc, process_before, process_after, plantype, priceplantype, note
                from tariff_plan_ucd where tariff_plan_id = a_tariff_plan_id;
				dbms_output.put_line('19');
        end if;

        a_result := 1;
        a_msg := 'generate tariff success!';
        return;

        exception when others then
            a_msg := sqlerrm;
            a_result := -1;
			dbms_output.put_line('20');
            return;
    end ap_gen_tariff_plan_ucd;

    --generate ucd tariff plan item
    procedure ap_gen_tariff_plan_item_ucd(
        a_workorder_id          in  varchar2, --work order id
        a_tariff_plan_id        in  number,   --tariff plan id
        a_new_tariff_plan_id    in  number,   --new tariff plan id
        a_tariff_plan_item_id   in  number,   --tariff paln item;
        a_result                out number,   --1-success,-1-failurre
        a_msg                   out varchar2  --result message
    )
    is
        v_schema_sn             number(4);
        v_tariff_schema_id      number(9);
        v_attr_class            number(4);
    begin

        --get schema_sn
        v_schema_sn := 0;
        select nvl(max(tariff_schema_sn), 0) + 1 into v_schema_sn from tariff_schema_ucd
            where workorder_id = a_workorder_id and tariff_plan_id = a_new_tariff_plan_id;

        --获取资费模式编号
        select tariff_schema_id into v_tariff_schema_id from tariff_plan_item
            where tariff_plan_id = a_tariff_plan_id
              and tariff_plan_item_sn = a_tariff_plan_item_id;

        --generate ucd tariff schema
        ap_gen_tariff_schema_ucd(a_workorder_id, a_tariff_plan_id, a_new_tariff_plan_id,
                                 v_schema_sn, v_tariff_schema_id, v_attr_class, a_result, a_msg);

         --generate ucd tariff plan item
        insert into tariff_plan_item_ucd
            (workorder_id,
             tariff_plan_id,
             tariff_schema_sn,
             tariff_name,
             fee_id,
             select_method,
             charging_event_id,
             precedence,
             applytime,
             expiretime,
             apply_method,
             discount_type,
             base_fee_id,
             discount_fee_id,
             charging_cond,
             g_charging_cond,
             switch_method,
             switch_unit,
             billcode,
             attr_class,
             serv_id)
        select a_workorder_id,                       --工单ID
               a_new_tariff_plan_id,                 --资费政策ID
               v_schema_sn,                          --资费模式序号
               tariff_plan_item_name,                --资费政策项名称
               fee_id,                               --要计算的费用ID
               select_method,                        --批价原则(1-取较低的费用;2-无条件取本费用;3-有条件取本费用)
               charging_event_id,                    --要计算的计费事件
               precedence,                           --优先级
               applytime,                            --生效时间
               expiretime,                           --失效时间
               apply_method,                         --生效失效判断方法(1-按当前时间匹配，2-按计费事件发生时间匹配，3-忽略时间匹配)
               discount_type,                        --计算模式
               base_fee_id,                          --基准费用
               discount_fee_id,                      --优惠费用
               charging_cond,                        --计费条件
               g_charging_cond,                      --计费条件描述
               switch_method,                        --切换方式(1-按开始时间，2-跨边界的一个时间单位向前靠，3-跨边界的一个时间单位向后靠，4-跨边界的一个时间单位按比例分配)
               switch_unit,                          --切换单位
               billcode,                             --账目编码
               v_attr_class,                         --计价属性（0－正常属性，11－时长,19－字节数,37－计费字节数)
               serv_id                               --业务表达式ID
        from tariff_plan_item
             where  tariff_plan_id = a_tariff_plan_id
               and  tariff_plan_item_sn = a_tariff_plan_item_id;

        if a_result = -1 then
		dbms_output.put_line('21');
            return;
        end if;

        a_result := 1;
        a_msg := 'generate tariff plan item success!';
        return;
      exception when others then
            a_msg := sqlerrm;
            a_result := -1;
			dbms_output.put_line('22');
            return;
    end ap_gen_tariff_plan_item_ucd;

    --generate ucd tariff schema
    procedure ap_gen_tariff_schema_ucd(
        a_workorder_id          in  varchar2,   --work order id
        a_tariff_plan_id        in  number,     --tariff plan id
        a_new_tariff_plan_id    in  number,     --new tariff plan id
        a_tariff_schema_sn      in  number,     --tariff schema serial number
        a_tariff_schema_id      in  number,     --tariff schema id
        a_attr_class            out number,     --attr class
        a_result                out number,     --1-success,-1-failurre
        a_msg out varchar2                      --result message
    )
    is
       v_subschema_num          number(3);
       v_is_combination         number(1);
       v_attr_class             number(2);
       v_unittype               number(2);
       v_units                  number(10,3);
       v_currency               number(9,3);
       v_subtariff_schema_sn    number(4);
       v_tariff_type            number(2);
       v_rate_code              number(8);
    begin

        a_attr_class := 0;
        select tariff_type into v_tariff_type from tariff_schema
             where tariff_schema_id = a_tariff_schema_id;

        if v_tariff_type = 3 then
            a_result := -1;
            a_msg := '系统暂不支持参数映射资费模式!';
			dbms_output.put_line('23');
            return;
        end if;

        v_subschema_num := 0;
        select count(*) into  v_subschema_num from tariff_item
            where tariff_schema_id = a_tariff_schema_id and subtariff_type = 2;

        --下级资费类型包含资费模式时，按组合资费政策导入
        if v_subschema_num > 0 then
            v_is_combination := 1;
			dbms_output.put_line('24');
        else
            v_is_combination := 0;
			dbms_output.put_line('25');
        end if;

        --generate tariff schema
        insert into tariff_schema_ucd
            (workorder_id,
             tariff_plan_id,
             tariff_schema_sn,
             tariff_name,
             tariff_type,
             is_combination,
             field_def,
             fieldcount,
             match_order,
             match_type,
             apply_method,
             refid,
             ref_offset,
             discount_fee_id,
             g_field_def,
             round_method,
             round_scale,
             billcode_order)
        select a_workorder_id,
             a_new_tariff_plan_id,
             a_tariff_schema_sn,
             tariff_name,
             tariff_type,
             v_is_combination,
             field_def,
             fieldcount,
             match_order,
             match_type,
             apply_method,
             refid,
             ref_offset,
             discount_fee_id,
             g_field_def,
             round_method,
             round_scale,
             billcode_order
        from tariff_schema where tariff_schema_id = a_tariff_schema_id;

        --generate tariff schema item
        for c4 in (select t.*, t.rowid from  tariff_item t
                       where tariff_schema_id = a_tariff_schema_id) loop
            v_rate_code := null;
            v_attr_class := null;
            v_unittype := null;
            v_units := null;
            v_currency := null;

            case c4.subtariff_type
                when  1  then  --Charge Rate
                    select attr_class, unittype, units, currency
                        into v_attr_class, v_unittype, v_units, v_currency
                        from charge_rate where ratecode = c4.tariff_id;
                     v_subtariff_schema_sn := c4.tariff_id;
                     v_currency := v_currency * c4.ratio;
                     a_attr_class := v_attr_class;
                     v_rate_code := c4.tariff_id;
					 dbms_output.put_line('26');
                when  2  then  --Tariff schema
                    select nvl(max(tariff_schema_sn), 0) + 1 into v_subtariff_schema_sn from tariff_schema_ucd
                        where workorder_id = a_workorder_id and tariff_plan_id = a_new_tariff_plan_id;

                    --generate sub ucd tariff schema
                    ap_gen_tariff_schema_ucd(a_workorder_id, a_tariff_plan_id, a_new_tariff_plan_id,
                                 v_subtariff_schema_sn, c4.tariff_id, a_attr_class, a_result, a_msg);
dbms_output.put_line('27');
                    if a_result = -1 then
					dbms_output.put_line('28');
                        return;
                    end if;

                else --3 Fee Item, 4-Average Charge Rate,5-Max Usage Fee
                    v_subtariff_schema_sn := c4.tariff_id;
					dbms_output.put_line('29');
            end case;

            insert into tariff_item_ucd
                (workorder_id,
                 tariff_plan_id,
                 tariff_schema_sn,
                 tariff_schema_id,
                 applytime,
                 expiretime,
                 tariff_criteria,
                 subtariff_type,
                 subtariff_schema_sn,
                 tariff_id,
                 param_string,
                 g_param,
                 ratetype,
                 precedence,
                 billcode,
                 g_criteria,
                 node_name,
                 ratio,
                 attr_class,
                 currency,
                 unittype,
                 units)
            values
                 (a_workorder_id,                     --工单ID
                  a_new_tariff_plan_id,               --资费政策ID
                  a_tariff_schema_sn,                 --资费模式序号
                  null,                               --资费模式号
                  c4.applytime,                       --生效时间
                  c4.expiretime,                      --失效时间
                  c4.tariff_criteria,                 --资费定义条件
                  c4.subtariff_type,                  --下级资费类型(1-费率，2-资费模式，3-费用项，4-当前费用项,5-封顶费用项)
                  v_subtariff_schema_sn,              --下级资费模式序号
                  c4.tariff_id,                       --下级资费号(对应正式库下级资费号)
                  c4.param_string,                    --参数串
                  c4.g_param,                         --参数串描述
                  c4.ratetype,                        --资费标记符
                  c4.precedence,                      --优先级
                  c4.billcode,                        --账目编码
                  c4.g_criteria,                      --资费定义条件描述
                  c4.tariff_item_name,                --节点名称
                  c4.ratio,                           --比率
                  v_attr_class,                       --计价属性（0－正常属性，11－时长,19－字节数,37－计费字节数)
                  v_currency,                         --金额
                  v_unittype,                         --单位种类
                  v_units);                           --单位
        end loop;

        a_result := 1;
        a_msg := 'generate ucd tariff schema success!';
        return;

        exception when others then
            a_msg := sqlerrm;
            a_result := -1;
			dbms_output.put_line('30');
            return;
    end ap_gen_tariff_schema_ucd;

    --copy ucd tariff plan item
    procedure ap_cp_tariff_plan_item_ucd(
        a_workorder_id          in  varchar2, --work order id
        a_new_workorder_id      in  varchar2, --new work order id
        a_tariff_plan_id        in  number,   --tariff plan id
        a_new_tariff_plan_id    in  number,   --new tariff plan id
        a_tariff_schema_sn      in  number,   --tariff schema serial number;
        a_result                out number,   --1-success, -1-failurre
        a_msg                   out varchar2  --result message
    )
    is
        v_new_schema_sn             number(9);
    begin
        --get new schema serial number
        v_new_schema_sn := 0;
        select nvl(max(tariff_schema_sn), 0) + 1 into v_new_schema_sn from tariff_schema_ucd
            where workorder_id = a_new_workorder_id and tariff_plan_id = a_new_tariff_plan_id;

        --copy ucd tariff plan item
        insert into tariff_plan_item_ucd
            (workorder_id,
             tariff_plan_id,
             tariff_schema_sn,
             fee_id,
             select_method,
             charging_event_id,
             precedence,
             applytime,
             expiretime,
             apply_method,
             discount_type,
             base_fee_id,
             discount_fee_id,
             charging_cond,
             g_charging_cond,
             switch_method,
             switch_unit,
             billcode,
             tariff_name,
             attr_class,
             serv_id)
        select a_new_workorder_id,
               a_new_tariff_plan_id,
               v_new_schema_sn,
               fee_id,
               select_method,
               charging_event_id,
               precedence,
               applytime,
               expiretime,
               apply_method,
               discount_type,
               base_fee_id,
               discount_fee_id,
               charging_cond,
               g_charging_cond,
               switch_method,
               switch_unit,
               billcode,
               tariff_name,
               attr_class,
               serv_id
        from tariff_plan_item_ucd  where workorder_id = a_workorder_id
              and tariff_plan_id = a_tariff_plan_id and tariff_schema_sn = a_tariff_schema_sn;

        --copy ucd tariff schema
        ap_cp_tariff_schema_ucd(a_workorder_id, a_new_workorder_id, a_tariff_plan_id, a_new_tariff_plan_id,
                                a_tariff_schema_sn, v_new_schema_sn, a_result, a_msg);

        if a_result = -1 then
		dbms_output.put_line('31');
            return;
        end if;

        a_result := 1;
        a_msg := 'copy tariff success!';
        return;
      exception when others then
            a_msg := sqlerrm;
            a_result := -1;
			dbms_output.put_line('32');
            return;
    end ap_cp_tariff_plan_item_ucd;

    --copy ucd tariff schema
    procedure ap_cp_tariff_schema_ucd(
        a_workorder_id          in  varchar2,   --old work order id
        a_new_workorder_id      in  varchar2,   --new work order id
        a_tariff_plan_id        in  number,     --old tariff plan id
        a_new_tariff_plan_id    in  number,     --new tariff plan id
        a_tariff_schema_sn      in  number,     --old tariff schema serial number
        a_new_tariff_schema_sn  in  number,     --new tariff schema serial number
        a_result                out number,     --1-success, -1-failurre
        a_msg                   out varchar2    --result message
    )
    is
       v_sub_schema_sn   number(4);
    begin

        --copy tariff schema
        insert into tariff_schema_ucd
            (workorder_id,
             tariff_plan_id,
             tariff_schema_sn,
             tariff_name,
             tariff_type,
             is_combination,
             field_def,
             fieldcount,
             match_order,
             match_type,
             apply_method,
             refid,
             ref_offset,
             discount_fee_id,
             g_field_def,
             round_method,
             round_scale,
             billcode_order)
        select a_new_workorder_id,
               a_new_tariff_plan_id,
               a_new_tariff_schema_sn,
               tariff_name,
               tariff_type,
               is_combination,
               field_def,
               fieldcount,
               match_order,
               match_type,
               apply_method,
               refid,
               ref_offset,
               discount_fee_id,
               g_field_def,
               round_method,
               round_scale,
               billcode_order
        from tariff_schema_ucd  where workorder_id = a_workorder_id
                and tariff_plan_id = a_tariff_plan_id and tariff_schema_sn = a_tariff_schema_sn;

        --copy tariff schema item
        for c4 in (select t.*, t.rowid from  tariff_item_ucd t  where workorder_id = a_workorder_id
                        and tariff_plan_id = a_tariff_plan_id and tariff_schema_sn = a_tariff_schema_sn) loop

            if c4.subtariff_type = 2 then --Tariff schema

                select nvl(max(tariff_schema_sn), 0) + 1 into v_sub_schema_sn from tariff_schema_ucd
                    where workorder_id = a_new_workorder_id and tariff_plan_id = a_new_tariff_plan_id;

                --generate sub ucd tariff schema
                ap_cp_tariff_schema_ucd(a_workorder_id, a_new_workorder_id, a_tariff_plan_id, a_new_tariff_plan_id,
                             c4.subtariff_schema_sn, v_sub_schema_sn, a_result, a_msg);
dbms_output.put_line('33');
                if a_result = -1 then
				dbms_output.put_line('34');
                    return;
                end if;
            else
                 v_sub_schema_sn := c4.subtariff_schema_sn;
				 dbms_output.put_line('35');
            end if;

            insert into tariff_item_ucd
                (workorder_id,
                 tariff_plan_id,
                 tariff_schema_sn,
                 tariff_schema_id,
                 applytime,
                 expiretime,
                 tariff_criteria,
                 subtariff_type,
                 subtariff_schema_sn,
                 tariff_id,
                 param_string,
                 g_param,
                 ratetype,
                 precedence,
                 billcode,
                 g_criteria,
                 attr_class,
                 currency,
                 unittype,
                 units,
                 node_name,
                 ratio)
            values
                (a_new_workorder_id,
                 a_new_tariff_plan_id,
                 a_new_tariff_schema_sn,
                 null,
                 c4.applytime,
                 c4.expiretime,
                 c4.tariff_criteria,
                 c4.subtariff_type,
                 v_sub_schema_sn,
                 c4.tariff_id,
                 c4.param_string,
                 c4.g_param,
                 c4.ratetype,
                 c4.precedence,
                 c4.billcode,
                 c4.g_criteria,
                 c4.attr_class,
                 c4.currency,
                 c4.unittype,
                 c4.units,
                 c4.node_name,
                 c4.ratio);
        end loop; --c4

        a_result := 1;
        a_msg := 'generate ucd tariff schema success!';
        return;

        exception when others then
            a_msg := sqlerrm;
            a_result := -1;
			dbms_output.put_line('36');
            return;
    end ap_cp_tariff_schema_ucd;


    --save ucd tariff schema
    procedure ap_save_tariff_schema_ucd(
        a_workorder_id	        in  varchar2,       --工单ID
        a_tariff_plan_id   	    in  number,         --资费政策ID
        a_tariff_schema_sn	    in  number,         --资费模式序号
        a_tariff_name 	        in  varchar2,       --资费模式名称
        a_tariff_type     	    in  number,         --资费模式类型
        a_is_combination	    in  number,         --是否组合资费模式
        a_field_def       	    in  varchar2,       --参数字段定义
        a_fieldcount      	    in  number,         --参数字段定义数
        a_match_order     	    in  number,         --参数匹配次序
        a_match_type      	    in  number,         --匹配类型
        a_apply_method    	    in  number,         --应用时间判断方法
        a_refid           	    in  number,         --资费标记位置
        a_ref_offset      	    in  number,         --资费标记偏移
        a_discount_fee_id 	    in  number,         --优惠费用
        a_g_field_def     	    in  varchar2,       --中文参数定义
        a_round_method    	    in  number,         --取整方法
        a_round_scale     	    in  number,         --取整精度
        a_billcode_order	    in  number,         --账目编码取值顺序
        a_rowid                 out varchar2,       --返回rowid
        a_result                out number,         --1-成功, -1-失败
        a_msg                   out varchar2        --结果信息
    )
    is
    begin
        --insert into tariff schema
        insert into tariff_schema_ucd
            (workorder_id,
             tariff_plan_id,
             tariff_schema_sn,
             tariff_name,
             tariff_type,
             is_combination,
             field_def,
             fieldcount,
             match_order,
             match_type,
             apply_method,
             refid,
             ref_offset,
             discount_fee_id,
             g_field_def,
             round_method,
             round_scale,
             billcode_order)
         values
           (a_workorder_id,
            a_tariff_plan_id,
            a_tariff_schema_sn,
            a_tariff_name,
            a_tariff_type,
            a_is_combination,
            a_field_def,
            a_fieldcount,
            a_match_order,
            a_match_type,
            a_apply_method,
            a_refid,
            a_ref_offset,
            a_discount_fee_id,
            a_g_field_def,
            a_round_method,
            a_round_scale,
            a_billcode_order)
            returning rowid into a_rowid;

        a_result := 1;
        a_msg := 'save ucd tariff schema success!';
		dbms_output.put_line('37');
        return;

        exception when others then
            a_msg := sqlerrm;
            a_result := -1;
			dbms_output.put_line('38');
            return;
    end ap_save_tariff_schema_ucd;

    --save ucd tariff item
    procedure ap_save_tariff_item(
        a_workorder_id	        in    varchar2,		    --工单ID
        a_tariff_plan_id   	    in    number,     	    --资费政策ID
        a_tariff_schema_sn	    in    number,	    	--资费模式序号
        a_applytime	            in    date,         	--生效时间
        a_expiretime	        in    date,         	--失效时间
        a_tariff_criteria 	    in    varchar2,		    --资费定义条件
        a_subtariff_type  	    in    number,    		--下级资费类型
        a_subtariff_schema_sn	in    number,		    --下级资费模式序号
        a_precedence      	    in    number,    		--优先级
        a_g_criteria      	    in    varchar2,		    --资费定义条件描述
        a_node_name	            in    varchar2,		    --节点名称
        a_ratio	                in    number,		    --比率
        a_attr_class	        in    number,     	    --计价属性
        a_rowid                 out   varchar2,         --返回rowid
        a_result                out   number,           --1-成功, -1-失败
        a_msg                   out   varchar2          --结果信息
    )
    is
    begin
        insert into tariff_item_ucd
                (workorder_id,
                 tariff_plan_id,
                 tariff_schema_sn,
                 applytime,
                 expiretime,
                 tariff_criteria,
                 subtariff_type,
                 subtariff_schema_sn,
                 precedence,
                 g_criteria,
                 attr_class,
                 node_name,
                 ratio)
            values
               (a_workorder_id,
                a_tariff_plan_id ,
                a_tariff_schema_sn,
                a_applytime,
                a_expiretime,
                a_tariff_criteria,
                a_subtariff_type,
                a_subtariff_schema_sn,
                a_precedence,
                a_g_criteria,
                a_attr_class,
                a_node_name,
                a_ratio)
                returning rowid into a_rowid;

        a_result := 1;
        a_msg := 'save tariff item successed!';
		dbms_output.put_line('39');
        return;
        exception when others then
            a_msg := sqlerrm;
            a_result := -1;
			dbms_output.put_line('40');
            return;
    end ap_save_tariff_item;

    --check work order and tariff plan
    procedure ap_check_workorder(
        a_workorder_id          in  varchar2, --work order id
        a_tariff_plan_id        in  number,   --tariff plan id
        a_result                out number,   --1-success, -1-failurre
        a_msg                   out varchar2  --result message
    )
    is
        v_count                 number;
    begin

        --check work order
        v_count := 0;
        select count(*) into v_count from work_order_def
                            where workorder_id = a_workorder_id;

        if v_count = 0 then
            a_result := -1;
            a_msg := '工单【' || to_char(a_workorder_id) || '】不存在。';
			dbms_output.put_line('41');
            return;
        end if;

        --check tariff plan
        v_count := 0;
        select count(*) into v_count from tariff_plan
                            where tariff_plan_id = a_tariff_plan_id;

        if v_count > 0 then
            a_result := -1;
            a_msg := '资费政策【' || to_char(a_tariff_plan_id) || '】已经存在。';
			dbms_output.put_line('42');
            return;
        end if;

        a_result := 1;
        return;

        exception when others then
            a_msg := sqlerrm;
            a_result := -1;
			dbms_output.put_line('43');
            return;
    end ap_check_workorder;

    procedure ap_get_workorder_id(
        a_region_id             in  number,    --region id
        a_workorder_id          out varchar2   -- work order id
    )
    is
        v_serial                number(4);
        v_tmpStr                varchar2(10);
        v_region                varchar2(10);

    begin
        select max(to_number(substr(nvl(workorder_id,'00000000000000'),12,3))) + 1 into v_serial from work_order_def
            where region = a_region_id and trunc(create_date) = trunc(to_date('20201023','yyyymmdd'));

        v_region := lpad(a_region_id, 3, '0');

        if v_serial is null  then
            a_workorder_id :=  v_region || to_char(to_date('20201023','yyyymmdd'), 'yyyymmdd') || '001';
			dbms_output.put_line('44');
        else
            v_tmpStr := lpad(v_serial, 3, '0');
            a_workorder_id :=  v_region || to_char(to_date('20201023','yyyymmdd'), 'yyyymmdd') || v_tmpStr;
			dbms_output.put_line('45');
        end if;
    end ap_get_workorder_id;

    --delete work order
    procedure ap_del_workorder(
        a_workorder_id          in varchar2,   -- work order id
        a_result                out number     --1-success, -1-failurre
    )
    is

    begin
        --work order
        delete from work_order_def where workorder_id = a_workorder_id;

        --tariff plan
        delete from tariff_plan_ucd where workorder_id = a_workorder_id;

        --tariff plan item
        delete from tariff_plan_item_ucd where workorder_id = a_workorder_id;

        --tariff schema
        delete from tariff_schema_ucd where workorder_id = a_workorder_id;

        --tariff item
        delete from tariff_item_ucd where workorder_id = a_workorder_id;

        --Billdisc_Def_Ucd
        delete from  Billdisc_Def_Ucd where workorder_id = a_workorder_id;

        --Discitem_Def_Ucd
        delete from Discitem_Def_Ucd where workorder_id = a_workorder_id;

        --Discrefitem_Def_Ucd
        delete from Discrefitem_Def_Ucd where workorder_id = a_workorder_id;

        --Discvaluepercent_Def_Ucd
        delete from Discvaluepercent_Def_Ucd where workorder_id = a_workorder_id;

        --Disc_Batch_List_Ucd
        delete from Disc_Batch_List_Ucd  where workorder_id = a_workorder_id;

        --work_order_process_log
        delete from work_order_process_log  where workorder_id = a_workorder_id;
        commit;
        a_result := 1;
		dbms_output.put_line('46');
        return;
        exception when others then
            rollback;
            a_result := -1;
			dbms_output.put_line('47');
    end ap_del_workorder;

    --delete tariff plan
    procedure ap_del_tariff_plan(
        a_workorder_id          in varchar2,   -- work order id
        a_plan_id               in number,     -- tariff plan id
        a_result                out number     --1-success, -1-failurre
    )
    is

    begin
        --tariff plan
        delete from tariff_plan_ucd
            where workorder_id = a_workorder_id and tariff_plan_id = a_plan_id;

        --tariff plan item
        delete from tariff_plan_item_ucd
            where workorder_id = a_workorder_id and tariff_plan_id = a_plan_id;

        --tariff schema
        delete from tariff_schema_ucd
            where workorder_id = a_workorder_id and tariff_plan_id = a_plan_id;

        --tariff item
        delete from tariff_item_ucd
            where workorder_id = a_workorder_id and tariff_plan_id = a_plan_id;

        --process log
        delete from work_order_process_log where workorder_id = a_workorder_id;

        --Billdisc_Def_Ucd
        delete from  Billdisc_Def_Ucd
            where workorder_id = a_workorder_id and  tariffplan_id = a_plan_id;

        --Discitem_Def_Ucd
        delete from Discitem_Def_Ucd
            where workorder_id = a_workorder_id and  tariffplan_id = a_plan_id;

        --Discrefitem_Def_Ucd
        delete from Discrefitem_Def_Ucd
            where workorder_id = a_workorder_id and  tariffplan_id = a_plan_id;

        --Discvaluepercent_Def_Ucd
        delete from Discvaluepercent_Def_Ucd
            where workorder_id = a_workorder_id and  tariffplan_id = a_plan_id;

        --Disc_Batch_List_Ucd
        delete from Disc_Batch_List_Ucd
            where workorder_id = a_workorder_id and  tariffplan_id = a_plan_id;
dbms_output.put_line('48');
        commit;
        a_result := 1;
        return;
        exception when others then
            rollback;
            a_result := -1;
			dbms_output.put_line('49');
    end ap_del_tariff_plan;

   --delete ucd tariff plan item
    procedure ap_del_plan_item_ucd(
        a_workorder_id          in varchar2,   -- work order id
        a_tariff_plan_id        in number,     -- tariff plan id
        a_tariff_schema_sn      in number,     -- tariff schema serial number
        a_result                out number     --1-success, -1-failurre
    )
    is

    begin
        --delete tariff plan item
        delete from tariff_plan_item_ucd a where workorder_id = a_workorder_id
            and tariff_plan_id = a_tariff_plan_id and tariff_schema_sn = a_tariff_schema_sn;

        --delete tariff schema
        ap_del_tariff_schema_ucd(a_workorder_id, a_tariff_plan_id, a_tariff_schema_sn, a_result);
		dbms_output.put_line('50');
        return;
        exception when others then
            a_result := -1;
    end ap_del_plan_item_ucd;

    --del ucd tariff schema
    procedure ap_del_tariff_schema_ucd(
        a_workorder_id          in  varchar2,   --work order id
        a_tariff_plan_id        in  number,     --tariff plan id
        a_tariff_schema_sn      in  number,     --tariff schema serial number
        a_result                out number      --1-success, -1-failurre
    )
    is

    begin
        --delete tariff schema item
        for c1 in (select t.*, t.rowid from  tariff_item_ucd t  where workorder_id = a_workorder_id
                        and tariff_plan_id = a_tariff_plan_id and tariff_schema_sn = a_tariff_schema_sn) loop

            if c1.subtariff_type = 2 then --Tariff schema

                --delete sub ucd tariff schema
                ap_del_tariff_schema_ucd(a_workorder_id, a_tariff_plan_id, c1.subtariff_schema_sn, a_result);

                if a_result = -1 then
				dbms_output.put_line('51');
                    return;
                end if;
             else
                 delete from tariff_item_ucd where rowid = c1.rowid ;
				 dbms_output.put_line('52');
             end if;
          end loop; --c1

        --delete tariff schema
        delete from tariff_schema_ucd where workorder_id = a_workorder_id
                and tariff_plan_id = a_tariff_plan_id and tariff_schema_sn = a_tariff_schema_sn;

        a_result := 1;
        return;
        exception when others then
            a_result := -1;
            return;
    end ap_del_tariff_schema_ucd;

    --add process log
    procedure ap_add_process_log(
        a_workorder_id          in varchar2,   -- work order id
        a_operator              in varchar2,   -- operator
        a_status                in number,     -- work order status
        a_reason                in varchar2,   -- reject reason
        a_result                out number     --1-success, -1-failurre
    )
    is
        v_order     number(4);
    begin
        --get process order
        select nvl(max(process_order),0) + 1  into v_order from work_order_process_log
            where workorder_id = a_workorder_id;

        --insert into  work_order_process_log
        insert into work_order_process_log
        (workorder_id, process_order, operator, processtime, status, description)
        values
        (a_workorder_id, v_order, a_operator, to_date('20201023','yyyymmdd'), a_status, a_reason);

        --update work order status
        update work_order_def set status = a_status where workorder_id = a_workorder_id;
        a_result := 1;
		dbms_output.put_line('53');
        return;

        exception when others then
            a_result := -1;
    end ap_add_process_log;

    --ap_judge_plan_id
    procedure ap_judge_plan_id(
        a_rowid                 in varchar2,   -- rowid
        a_plan_id               in number,     -- tariff plan id
        a_result                out number     --1-success, -1-failurre
    )
    is
        v_count     number(4);
    begin

        select count(*) into v_count from tariff_plan
            where tariff_plan_id = a_plan_id;

        if v_count > 0 then
            a_result := -1;
			dbms_output.put_line('54');
            return;
        end if;

        if a_rowid is null then
            select count(*) into v_count from tariff_plan_ucd
            where tariff_plan_id = a_plan_id;
			dbms_output.put_line('55');
        else
            select count(*) into v_count from tariff_plan_ucd
            where rowid <> a_rowid and tariff_plan_id = a_plan_id;
			dbms_output.put_line('56');
        end if;

        if v_count > 0 then
            a_result := -1;
			dbms_output.put_line('57');
            return;
        end if;
        a_result := 1;
        return;
    end ap_judge_plan_id;

    --update tarriff plan id
    procedure ap_update_plan_id(
        a_workorder_id          in varchar2,   -- work order id
        a_old_plan_id           in number,     -- old tariff plan id
        a_new_plan_id           in number,     -- new tariff plan id
        a_result                out number     --1-success, -1-failurre
    )
    is
    begin
        --tariff_plan_item_ucd
        update tariff_plan_item_ucd set tariff_plan_id = a_new_plan_id
            where workorder_id = a_workorder_id and  tariff_plan_id = a_old_plan_id;

        --tariff_plan_item_ucd
        update tariff_schema_ucd set tariff_plan_id = a_new_plan_id
            where workorder_id = a_workorder_id and  tariff_plan_id = a_old_plan_id;

        --tariff_item_ucd
        update tariff_item_ucd set tariff_plan_id = a_new_plan_id
            where workorder_id = a_workorder_id and  tariff_plan_id = a_old_plan_id;

        --Billdisc_Def_Ucd
        update Billdisc_Def_Ucd set tariffplan_id = a_new_plan_id
            where workorder_id = a_workorder_id and  tariffplan_id = a_old_plan_id;

        --Discitem_Def_Ucd
        update Discitem_Def_Ucd set tariffplan_id = a_new_plan_id
            where workorder_id = a_workorder_id and  tariffplan_id = a_old_plan_id;

        --Discrefitem_Def_Ucd
        update Discrefitem_Def_Ucd set tariffplan_id = a_new_plan_id
            where workorder_id = a_workorder_id and  tariffplan_id = a_old_plan_id;

        --Discvaluepercent_Def_Ucd
        update Discvaluepercent_Def_Ucd set tariffplan_id = a_new_plan_id
            where workorder_id = a_workorder_id and  tariffplan_id = a_old_plan_id;

        --Disc_Batch_List_Ucd
        update Disc_Batch_List_Ucd set tariffplan_id = a_new_plan_id
            where workorder_id = a_workorder_id and  tariffplan_id = a_old_plan_id;

        a_result := 1;
		dbms_output.put_line('58');
        return;
        exception when others then
            a_result := -1;
    end ap_update_plan_id;

    --获取资费政策编号
    procedure ap_get_plan_id(
        a_plan_id               out number,    -- tariff plan id
        a_result                out number,    --1-success, -1-failurre
        a_msg                   out varchar2   --result message
    )
    is
        v_plan_id   number;
    begin

        v_plan_id := TARIFF_PLAN_ID_START + 1;

        --从资费政策表(tariff_plan)找一个未用的资费政策编号作为新的资费政策编号
        for c1 in (select tariff_plan_id from tariff_plan
                       where tariff_plan_id > TARIFF_PLAN_ID_START
                       order by tariff_plan_id) loop

           if  v_plan_id <> c1.tariff_plan_id  then
               a_result := 1;
               a_plan_id := v_plan_id;
			   dbms_output.put_line('59');
               return;
           else
               v_plan_id := v_plan_id + 1;
			   dbms_output.put_line('60');
           end if;
       end loop;

       if v_plan_id >= TARIFF_PLAN_ID_MAX then
            a_result := -1;
            a_msg := '资费政策编号已超过最大值【' ||TARIFF_PLAN_ID_MAX || '】,无法生成资费政策!' ;
			dbms_output.put_line('61');
            return;
       end if;
       a_plan_id := v_plan_id;
       a_result := 1;
       return;
    end ap_get_plan_id;

    procedure ap_get_schema_id(
        a_schema_id             out number,    -- tariff schema id
        a_result                out number,    --1-success, -1-failurre
        a_msg                   out varchar2  --result message
    )
    is
        v_schema_id   number;
    begin

        v_schema_id := TARIFF_SCHEMA_ID_START + 1;

        --从资费模式表(tariff_schema )找一个未用的资费模式编号作为新的资费模式编号
        for c1 in (select tariff_schema_id from tariff_schema
                       where tariff_schema_id > TARIFF_SCHEMA_ID_START
                       order by tariff_schema_id ) loop

           if  v_schema_id <> c1.tariff_schema_id  then
               a_result := 1;
               a_schema_id := v_schema_id;
			   dbms_output.put_line('62');
               return;
           else
               v_schema_id := v_schema_id + 1;
			   dbms_output.put_line('63');
           end if;
       end loop;

       if v_schema_id >= TARIFF_SCHEMA_ID_START then
            a_result := -1;
            a_msg := '资费模式编号已超过最大值【' ||TARIFF_SCHEMA_ID_START || '】,无法生成资费政策!' ;
			dbms_output.put_line('64');
            return;
       end if;
       a_schema_id := v_schema_id;
       a_result := 1;
       return;
    end ap_get_schema_id;
begin
  -- Initialization
   null;
end PKG_WORKORDER;


$


  CREATE OR REPLACE PACKAGE BODY "ROAMPROC" 
is

function	getHighRate(imsi varchar2)
  return number as
  r number;
begin

  r := 1;

  --根据高额门限定义与标准高额门限值 50 SDR计算比例
  --标准值50如修改, 需同时修改此处与ap_set_HIGHALERTLEVEL存储过程

	for cc in (select money/50 rate from highcostexchangerate
              where imsi like imsinum||'%' and nvl(starttime,to_date('20201023','yyyymmdd')-1)<=to_date('20201023','yyyymmdd') and nvl(endtime,to_date('20201023','yyyymmdd')+1)>to_date('20201023','yyyymmdd')
              order by imsinum desc)
  loop
		r := cc.rate;
		dbms_output.put_line('1');
		exit;
	end loop;

	return r;

end ;

function	getThreshold(imsi varchar2)
  return number as
  r number(10,2);
begin
	r := 1;
	for cc in (select money from highcostexchangerate
      where imsi like imsinum||'%' and nvl(starttime,to_date('20201023','yyyymmdd')-1)<=to_date('20201023','yyyymmdd') and nvl(endtime,to_date('20201023','yyyymmdd')+1)>to_date('20201023','yyyymmdd')
       order by imsinum desc)
  loop
		r := cc.money;
		dbms_output.put_line('2');
		exit;
	end loop;

	return r;

end ;

function GetSdrRate( sRoamType varchar2 ) return number
is
	sUserType		varchar2(3);
	sNetType		varchar2(1);
	nRate			number(10,2);
begin
	if sRoamType='I' then
		sNetType :='G';
		sUserType:='231';
		dbms_output.put_line('3');
	elsif sRoamType='K' then
		sNetType :='G';
		sUserType:='231';
		dbms_output.put_line('4');
	else
	dbms_output.put_line('5');
		return 1;
	end if;

  nRate := 1;

	select nvl(sdrrate,1) into nRate from highalertlevel
	  where nettype=sNetType and usertype=sUserType
      and nvl(apply_time,to_date('20201023','yyyymmdd')-1)<=to_date('20201023','yyyymmdd') and nvl(expire_time,to_date('20201023','yyyymmdd')+1)>to_date('20201023','yyyymmdd')
      and rownum=1;

	return nRate;
exception
	when others then
		return 1;
end;


function toAlarmLevel( nFee number,sRoamType varchar2 ,sImsi varchar2) return varchar2
is
	sAlarmLevel		varchar2(1);
	sUserType		varchar2(3);
	sNetType		varchar2(1);
	nRate			number;
begin
	if sRoamType='A' then
		sNetType :='T';
		sUserType:='221';
		dbms_output.put_line('6');
	elsif sRoamType='D' then
		sNetType :='G';
		sUserType:='221';
		dbms_output.put_line('7');
	elsif sRoamType='I' then
		sNetType :='G';
		sUserType:='231';
		dbms_output.put_line('8');
	elsif sRoamType='K' then
		sNetType :='G';
		sUserType:='231';
		dbms_output.put_line('9');
	else
	dbms_output.put_line('10');
		return null;
	end if;
	nRate := getHighRate(sImsi);
	select alarmlevelno into sAlarmLevel from highalertlevel
   	where nettype=sNetType and usertype=sUserType and nFee >= lowerlimit*nRate and nFee < upperlimit*nRate
      and nvl(apply_time,to_date('20201023','yyyymmdd')-1)<=to_date('20201023','yyyymmdd') and nvl(expire_time,to_date('20201023','yyyymmdd')+1)>to_date('20201023','yyyymmdd');

	return sAlarmLevel;
exception
	when others then
		return null;
end;


procedure stat_highcost( sRoamType varchar2, sReportDay varchar2, sReportNum varchar2 )
is
	dReportDay	date;
	sYesterday	varchar2(20);
  nSdrRate    number;
begin

	dReportDay := to_date(sReportDay,'yyyymmdd');
	sYesterday := to_char(dReportDay -1, 'yyyymmdd');

  if sRoamType <> 'I' then
  	insert into guesthighcostreport
  		(roamtype,reportday,reportnum,nettype,imsimin,firstcalltime,totalduration,totalfee,totalcount,hprovince,alarmlevel,
      SERVICETYPE,LASTCALLTIME,VOLUME,SDR,THRESHOLD,recordtype)
  		select sRoamType,sReportDay,sReportNum,nettype,imsimin,firstcalltime,totalduration,totalfee,totalcount,
            hprovince,toAlarmLevel(totalfee,sRoamType,imsimin),
            servicetype,lastcalltime,totalvolume,0,0,'01'
  		from guesthighcoststat
  		where roamtype=sRoamType and statday in (sYesterday,sReportDay) and lastchangetime>=dReportDay and toAlarmLevel(totalfee,sRoamType,imsimin) is not null;
		dbms_output.put_line('11');
  else
    nSdrRate := GetSdrRate(sRoamType);
  	insert into guesthighcostreport
  		(roamtype,reportday,reportnum,nettype,imsimin,firstcalltime,
        totalduration,totalfee,totalcount,hprovince,alarmlevel,
        SERVICETYPE,LASTCALLTIME,VOLUME,SDR,THRESHOLD,recordtype)
  		select sRoamType,sReportDay,sReportNum,nettype,imsimin,firstcalltime,
          trunc(totalduration/3600)*10000+trunc(mod(totalduration,3600)/60)*100+mod(totalduration,60),totalfee,totalcount,hprovince,'1',
          servicetype,lastcalltime,totalvolume,totalfee/nSdrRate*1000,getThreshold(imsimin),'01'
  		from guesthighcoststat
  		where roamtype=sRoamType and statday in (sYesterday,sReportDay)
         and lastchangetime>=dReportDay
         and (RoamType,statday,nettype,imsimin) in (
        		select RoamType,statday,nettype,imsimin
        		from guesthighcoststat
        		where roamtype=sRoamType and statday in (sYesterday,sReportDay)
               and lastchangetime>=dReportDay
            group by RoamType,statday,nettype,imsimin
               having toAlarmLevel(sum(totalfee),sRoamType,imsimin) is not null
         );
		 dbms_output.put_line('12');
  end if;

	/* 非国际漫游来访高额报告, delete dup (imsi,firstcallday,alarmlevel) records in this time */
  if sRoamType <> 'I' then
  	delete from guesthighcostreport
  	where roamtype=sRoamType and reportday=sReportDay and reportnum=sReportNum
  		and (imsimin,trunc(firstcalltime,'dd'),alarmlevel) in
  		(select imsimin,trunc(firstcalltime,'dd'),alarmlevel
            from guesthighcostreport
        where roamtype=sRoamType and reportday in (sYesterday,sReportDay)
  		 group by imsimin,trunc(firstcalltime,'dd'),alarmlevel
  		 having count(*) > 1);
		 dbms_output.put_line('13');
  else
    --删除本次报告中的重复记录
  	delete from guesthighcostreport
  	where roamtype=sRoamType and reportday=sReportDay and reportnum=sReportNum
  		and imsimin in
  		(select imsimin
            from guesthighcostreport
        where roamtype=sRoamType and reportday=sReportDay and reportnum=sReportNum
  		 group by imsimin,servicetype
  		 having count(*) > 1);

    --删除本天已生成高额报告且金额未发生变化的高额报告
   	delete from guesthighcostreport
      	where roamtype=sRoamType and reportday=sReportDay and reportnum=sReportNum
         and imsimin in (select imsimin from
          	(
            select imsimin,sum(totalfee)
                  from guesthighcostreport
              where roamtype=sRoamType and reportday=sReportDay
                  and reportnum=sReportNum
        		 group by imsimin,reportnum
             intersect
              select imsimin,sum(totalfee)
                  from guesthighcostreport
              where roamtype=sRoamType and reportday=sReportDay
                  and reportnum<>sReportNum
        		 group by imsimin,reportnum
            )
           );
		   dbms_output.put_line('14');
  end if;
	return;
end;

procedure newdaytask( sNetType varchar2 )
is
begin
	/* clean guesthighcoststat */
	delete from guesthighcoststat where statday < to_char(to_date('20201023','yyyymmdd') -3,'YYYYMMDD') and nettype=sNetType;
	/* other new day tasks may been added below */
	/* ... */
	dbms_output.put_line('15');
end;

end;



$


  CREATE OR REPLACE PACKAGE BODY "TU" IS
  -- Private type declarations
  --type <TypeName> is <Datatype>;
  -- Private constant declarations
  --<ConstantName> constant <Datatype> := <Value>;
  -- Private variable declarations
  --<VariableName> <Datatype>;
  -- Function and procedure implementations
  /*function <FunctionName>(<Parameter> <Datatype>) return <Datatype> is
  <LocalVariable> <Datatype>;
  begin
  <Statement>;
  return(<Result>);
  end;
  */
  PROCEDURE add_default_plan_item(a_plan_id NUMBER) IS
  BEGIN
    INSERT INTO tariff_plan_item
      (tariff_plan_id,
       calctype,
       fee_id,
       tariff_schema_id,
       charging_event_id,
       precedence,
       applytime,
       expiretime,
       apply_method,
       discount_type,
       base_fee_id,
       discount_fee_id,
       charging_cond,
       switch_method,
       switch_unit,
       g_charging_cond)
    VALUES
      (a_plan_id,
       1,
       1,
       388,
       7,
       5,
       to_date('01-01-2000', 'dd-mm-yyyy'),
       NULL,
       2,
       6,
       0,
       0,
       ' ',
       2,
       60,
       ' ');
    COMMIT;
	dbms_output.put_line('1');
  END;

  PROCEDURE copy_remind_limit_para(from_plan_id NUMBER, to_plan_id NUMBER) IS
    v_cnt NUMBER;
    v_paraid remind_limit_para.paraid%type;
  BEGIN
    v_cnt := 0;

    SELECT COUNT(*)
      INTO v_cnt
      FROM remind_limit_para a
     WHERE a.tariffid = to_plan_id;

    IF v_cnt > 0
    THEN
	dbms_output.put_line('2');
      raise_application_error(-20101,
                              '复制的计费事件在目标库的REMIND_LIMIT_PARA有定义');
      RETURN;
    END IF;
    select max(paraid) into v_paraid from remind_limit_para;
    INSERT INTO remind_limit_para
      ( paraid,
        region,
        tariffid,
        attrid,
        attrtype,
        limitvalue,
        tariffname,
        stdfee,
        templateno,
        availtime,
        expiretime,
        rate,
        logicexpr,
        reminddefault,
        stdfree,
        halffree,
        halflimit,
        remindattrvalue,
        rtcctemplateno,
        batch_id,
        remind_type,
        expr_cond,
        g_expr_cond,
        g_limitvalue,
        g_stdfree)
      SELECT  v_paraid+rownum,
              region,
              to_plan_id,
              attrid,
              attrtype,
              limitvalue,
              tariffname,
              stdfee,
              templateno,
              availtime,
              expiretime,
              rate,
              logicexpr,
              reminddefault,
              stdfree,
              halffree,
              halflimit,
              remindattrvalue,
              rtcctemplateno,
              batch_id,
              remind_type,
              expr_cond,
              g_expr_cond,
              g_limitvalue,
              g_stdfree
        FROM remind_limit_para a
       WHERE a.tariffid = from_plan_id;
  END;

  PROCEDURE copy_plan_items(a_from_plan_id NUMBER, a_to_plan_id NUMBER) IS
  BEGIN
    INSERT INTO tariff_plan_item
      (tariff_plan_id,
       calctype,
       fee_id,
       tariff_schema_id,
       charging_event_id,
       precedence,
       applytime,
       expiretime,
       apply_method,
       discount_type,
       base_fee_id,
       discount_fee_id,
       charging_cond,
       switch_method,
       switch_unit,
       g_charging_cond,
       billcode,
       select_method)
      SELECT a_to_plan_id,
             calctype,
             fee_id,
             tariff_schema_id,
             charging_event_id,
             precedence,
             applytime,
             expiretime,
             apply_method,
             discount_type,
             base_fee_id,
             discount_fee_id,
             charging_cond,
             switch_method,
             switch_unit,
             g_charging_cond,
             billcode,
             select_method
        FROM tariff_plan_item
       WHERE tariff_plan_id = a_from_plan_id;
    COMMIT;
	dbms_output.put_line('3');
  END;
  PROCEDURE copy_schema(a_from_schema_id NUMBER, a_to_schema_id NUMBER) IS
  BEGIN
    DELETE FROM tariff_schema
     WHERE tariff_schema_id = a_to_schema_id
       AND TRIM(tariff_name) IS NULL;
    INSERT INTO tariff_schema
      (tariff_schema_id,
        tariff_name,
        tariff_type,
        fieldcount,
        field_def,
        match_order,
        match_type,
        apply_method,
        refid,
        discount_fee_id,
        g_field_def,
        round_method,
        round_scale,
        ref_offset,
        event_id,
        billcode_order,
        createtime,
        region)
      SELECT a_to_schema_id,
        tariff_name,
        tariff_type,
        fieldcount,
        field_def,
        match_order,
        match_type,
        apply_method,
        refid,
        discount_fee_id,
        g_field_def,
        round_method,
        round_scale,
        ref_offset,
        event_id,
        billcode_order,
        createtime,
        region
        FROM tariff_schema
       WHERE tariff_schema_id = a_from_schema_id;
    --values (200, '费用 (0.20元/分钟)', 1, 0, null, 2, 1, 2, 0, 0, null, 0, 0, 0, null);
    DELETE FROM tariff_item WHERE tariff_schema_id = a_to_schema_id;
    INSERT INTO tariff_item
      (tariff_schema_id,
       applytime,
       expiretime,
       tariff_criteria,
       subtariff_type,
       tariff_id,
       ratio,
       ratetype,
       param_string,
       precedence,
       expr_id,
       g_param,
       is_dynamic,
       g_criteria,
       TARIFF_ITEM_NAME)
      SELECT a_to_schema_id,
             applytime,
             expiretime,
             tariff_criteria,
             subtariff_type,
             tariff_id,
             ratio,
             ratetype,
             param_string,
             precedence,
             expr_id,
             g_param,
             is_dynamic,
             g_criteria,
             TARIFF_ITEM_NAME
        FROM tariff_item
       WHERE tariff_schema_id = a_from_schema_id;
	   dbms_output.put_line('4');
    --values (200, to_date('01-01-2001', 'dd-mm-yyyy'), null, ' ', 1, 86, 1, ' ', null, 5, null, null, 0, ' ');
    COMMIT;
  END;
  /* Formatted on 2005/10/24 15:52 (Formatter Plus v4.8.6) */
  PROCEDURE copy_month_priprivilege(a_from_plan_id NUMBER,
                                    a_to_plan_id   NUMBER) IS
  BEGIN
    INSERT INTO privilege_new_month_callfee
      ( hregion,
        itemname,
        feetype,
        value,
        freevolume,
        attr_id,
        billingcode,
        freetype,
        halftype,
        billifstop,
        calctype,
        specialtype,
        startcycle,
        endcycle,
        group_flag,
        group_share_flag,
        cycle_count,
        entity_id,
        process_type,
        ratioattr_id,
        present_type)
      SELECT  hregion,
              itemname,
              feetype,
              value,
              freevolume,
              attr_id,
              a_to_plan_id,
              freetype,
              halftype,
              billifstop,
              calctype,
              specialtype,
              startcycle,
              endcycle,
              group_flag,
              group_share_flag,
              cycle_count,
              entity_id,
              process_type,
              ratioattr_id,
              present_type
        FROM privilege_new_month_callfee
       WHERE billingcode = a_from_plan_id;
      insert into privilege_usage_def
           (hregion,
            billingcode,
            itemname,
            freevolume,
            attr_id,
            freetype,
            startcycle,
            endcycle,
            entity_id,
            share_entity_id,
            share_attrid,
            limit_attrid,
            limit_entity,
            limit_type,
            halftype)
     select hregion,
            a_to_plan_id,
            itemname,
            freevolume,
            attr_id,
            freetype,
            startcycle,
            endcycle,
            entity_id,
            share_entity_id,
            share_attrid,
            limit_attrid,
            limit_entity,
            limit_type,
            halftype from privilege_usage_def a
      where billingcode=a_from_plan_id;
	  dbms_output.put_line('5');
    COMMIT;
  END;
  PROCEDURE copy_schema2(a_from_schema_id NUMBER, a_to_schema_id NUMBER) IS
  BEGIN
    DELETE FROM tariff_schema WHERE tariff_schema_id = a_to_schema_id;
    INSERT INTO tariff_schema
      (tariff_schema_id,
       tariff_name,
       tariff_type,
       fieldcount,
       field_def,
       match_order,
       match_type,
       apply_method,
       refid,
       discount_fee_id,
       g_field_def,
       round_method,
       round_scale,
       ref_offset,
       event_id)
      SELECT a_to_schema_id,
             tariff_name,
             tariff_type,
             fieldcount,
             field_def,
             match_order,
             match_type,
             apply_method,
             refid,
             discount_fee_id,
             g_field_def,
             round_method,
             round_scale,
             ref_offset,
             event_id
        FROM tariff_schema
       WHERE tariff_schema_id = a_from_schema_id;
    --values (200, '费用 (0.20元/分钟)', 1, 0, null, 2, 1, 2, 0, 0, null, 0, 0, 0, null);
    DELETE FROM tariff_item WHERE tariff_schema_id = a_to_schema_id;
    INSERT INTO tariff_item
      (tariff_schema_id,
       applytime,
       expiretime,
       tariff_criteria,
       subtariff_type,
       tariff_id,
       ratio,
       ratetype,
       param_string,
       precedence,
       expr_id,
       g_param,
       is_dynamic,
       g_criteria)
      SELECT a_to_schema_id,
             applytime,
             expiretime,
             tariff_criteria,
             subtariff_type,
             tariff_id,
             ratio,
             ratetype,
             param_string,
             precedence,
             expr_id,
             g_param,
             is_dynamic,
             g_criteria
        FROM tariff_item
       WHERE tariff_schema_id = a_from_schema_id;
    --values (200, to_date('01-01-2001', 'dd-mm-yyyy'), null, ' ', 1, 86, 1, ' ', null, 5, null, null, 0, ' ');
    COMMIT;
	dbms_output.put_line('6');
  END;
  FUNCTION new_cdrtype(cdrtype VARCHAR2) RETURN VARCHAR2 IS
  BEGIN
    IF cdrtype = '01'
    THEN
	dbms_output.put_line('7');
      RETURN 'O';
    ELSIF cdrtype = '02'
    THEN
	dbms_output.put_line('8');
      RETURN 'T';
    ELSIF cdrtype = '03'
    THEN
	dbms_output.put_line('9');
      RETURN 'F';
    ELSE
	dbms_output.put_line('10');
      RETURN NULL;
    END IF;
  END;
  FUNCTION new_calltype(callflag VARCHAR2) RETURN VARCHAR2 IS
  BEGIN
    IF callflag = '000'
    THEN
	dbms_output.put_line('11');
      RETURN NULL;
    ELSIF callflag = 'L00'
    THEN
	dbms_output.put_line('12');
      RETURN '0|1';
    ELSIF callflag = 'L01'
    THEN
	dbms_output.put_line('13');
      RETURN '21|22';
    ELSIF callflag = 'L02'
    THEN
	dbms_output.put_line('14');
      RETURN '23';
    ELSE
	dbms_output.put_line('15');
      RETURN NULL;
    END IF;
  END;
  FUNCTION get_ratecode(a_rate NUMBER, a_unit NUMBER) RETURN NUMBER IS
    code NUMBER := 0;
  BEGIN
    --  select rate_code into code from tcl_rate_dict where rate=a_rate and unit=a_unit;
    --  return code;
    --exception
    --    when others then
	dbms_output.put_line('16');
    RETURN NULL;
  END;
  FUNCTION correct_date(a_date DATE) RETURN DATE IS
  BEGIN
    IF a_date > to_date('20370101', 'yyyymmdd')
    THEN
	dbms_output.put_line('17');
      RETURN to_date('20370101', 'yyyymmdd');
    ELSE
	dbms_output.put_line('18');
      RETURN a_date;
    END IF;
  END;
  PROCEDURE copy_usage_attr(a_from_entityid NUMBER, a_from_attr NUMBER, a_to_attr NUMBER) IS
  BEGIN
    INSERT INTO entity_attr_def
      (entity_id,
       attr_id,
       attr_name,
       attr_type,
       multivalue,
       isunique,
       isresident,
       attr_class,
       datatype,
       length,
       scale,
       expire_days)
      SELECT entity_id,
             a_to_attr,
             attr_name,
             attr_type,
             multivalue,
             isunique,
             isresident,
             attr_class,
             datatype,
             length,
             scale,
             expire_days
        FROM entity_attr_def
       WHERE entity_id = a_from_entityid
         AND attr_id = a_from_attr;
    INSERT INTO total_usage_attr_def
      (entity_id, attr_id, usage_type)
      SELECT entity_id, a_to_attr, usage_type
        FROM total_usage_attr_def
       WHERE entity_id = a_from_entityid
         AND attr_id = a_from_attr;
    INSERT INTO free_usage_attr_def
      (entity_id,
       attr_id,
       fixed_free_usage,
       free_limit,
       free_type)
      SELECT entity_id,
             a_to_attr,
             fixed_free_usage,
             free_limit,
             free_type
        FROM free_usage_attr_def
       WHERE entity_id = a_from_entityid
         AND attr_id = a_from_attr;
    INSERT INTO base_attr_def
      ( entity_id,
        attr_id,
        base_attr_type,
        base_attr_id,
        charging_event_id,
        call_usage_cond,
        round_unit,
        round_type,
        g_call_usage_cond,
        seqno,
        convert_unit)
      SELECT  entity_id,
              attr_id,
              base_attr_type,
              base_attr_id,
              charging_event_id,
              call_usage_cond,
              round_unit,
              round_type,
              g_call_usage_cond,
              (select max(seqno)+1 from base_attr_def b where b.entity_id=a_from_entityid),
              convert_unit
        FROM base_attr_def
       WHERE entity_id = a_from_entityid
         AND attr_id = a_from_attr;
    COMMIT;
	dbms_output.put_line('19');
  END;
  PROCEDURE ap_copy_custinfo_def(a_from_entityid NUMBER,a_from_attr NUMBER, a_count NUMBER) IS
    v_cnt NUMBER := a_count;
    a_to_attr entity_attr_def.attr_id%type;
  BEGIN
    select max(attr_id)+1 into a_to_attr from entity_attr_def b
     where b.entity_id=a_from_entityid;
    if mod(v_cnt,1)=0 and v_cnt>0 then
       while (v_cnt>0) loop
	        copy_usage_attr(a_from_entityid,a_from_attr,a_to_attr);
          a_to_attr := a_to_attr + 1;
          v_cnt := v_cnt - 1;
		  dbms_output.put_line('20');
       end loop;
    else
	dbms_output.put_line('21');
      raise_application_error(-20101,'执行次数必须为>0的整数！');
      RETURN;
    end if;
    COMMIT;
  END;
  FUNCTION deep_copy_tariff_schema(a_from_schema NUMBER,
                                   a_tariff_name VARCHAR2 := NULL)
    RETURN NUMBER AS
    v_new_schema NUMBER(9);
    v_schema_id  NUMBER(9);
  BEGIN
    SELECT MIN(tariff_schema_id)
      INTO v_new_schema
      FROM tariff_schema
     WHERE tariff_name LIKE '#UNUSED#'
       AND tariff_schema_id > 460;
    dbms_output.put_line('new tariff_schema from:' || a_from_schema ||
                         ' to : ' || v_new_schema);
    FOR r IN (SELECT *
                FROM tariff_schema
               WHERE tariff_schema_id = a_from_schema)
    LOOP
      r.tariff_schema_id := v_new_schema;
      IF a_tariff_name IS NOT NULL
      THEN
        r.tariff_name := a_tariff_name;
		dbms_output.put_line('22');
      END IF;
      UPDATE tariff_schema
         SET ROW = r
       WHERE tariff_schema_id = v_new_schema;
      DELETE FROM tariff_item WHERE tariff_schema_id = v_new_schema;
    END LOOP;
    FOR r IN (SELECT *
                FROM tariff_item
               WHERE tariff_schema_id = a_from_schema)
    LOOP
      r.tariff_schema_id := v_new_schema;
      INSERT INTO tariff_item VALUES r;
	  dbms_output.put_line('23');
    END LOOP;
    FOR r IN (SELECT DISTINCT to_number(tariff_id) schema_id
                FROM tariff_item
               WHERE tariff_schema_id = v_new_schema
                 AND subtariff_type = 2)
    LOOP
      v_schema_id := deep_copy_tariff_schema(r.schema_id);
      UPDATE tariff_item
         SET tariff_id = v_schema_id
       WHERE tariff_schema_id = v_new_schema
         AND subtariff_type = 2
         AND tariff_id = r.schema_id;
      dbms_output.put_line('updated ' || SQL%ROWCOUNT || ' rows.');
	  dbms_output.put_line('24');
    END LOOP;
    RETURN v_new_schema;
  END;
  FUNCTION af_getstr(str VARCHAR2, v_split VARCHAR2, pos NUMBER)
    RETURN VARCHAR2 IS
    v_arstr    VARCHAR2(400);
    v_str      VARCHAR2(300);
    v_last_pos NUMBER;
    v_pos      NUMBER;
  BEGIN
    v_str := '';
    IF substr(str, length(str), 1) <> v_split
    THEN
      v_arstr := str || v_split;
	  dbms_output.put_line('25');
    ELSE
      v_arstr := str;
	  dbms_output.put_line('26');
    END IF;
    IF pos != 1
    THEN
      v_last_pos := instr(v_arstr, v_split, 1, pos - 1);
      v_pos      := instr(v_arstr, v_split, 1, pos);
      v_str      := substr(v_arstr, v_last_pos + 1, v_pos - v_last_pos - 1);
	  dbms_output.put_line('27');
    ELSE
      v_pos := instr(v_arstr, v_split, 1, pos);
      v_str := substr(v_arstr, 1, v_pos - 1);
	  dbms_output.put_line('28');
    END IF;

    RETURN v_str;
  END;
  --/*拷贝资费政策*/
  PROCEDURE ap_copy_tariff_plan(from_plan_id NUMBER, to_plan_id NUMBER) IS
    v_cnt NUMBER;
  BEGIN
    v_cnt := 0;

    SELECT COUNT(*)
      INTO v_cnt
      FROM tariff_plan a
     WHERE a.tariff_plan_id = to_plan_id;

    SELECT COUNT(*)
      INTO v_cnt
      FROM tariff_plan_item a
     WHERE a.tariff_plan_id = to_plan_id;

    IF v_cnt > 0
    THEN
	dbms_output.put_line('29');
      raise_application_error(-20101,
                              '复制的计费事件在目标库的TARIFF_PLAN或TARIFF_PLAN_ITEM有定义');
      RETURN;
    END IF;

    INSERT INTO tariff_plan
      ( tariff_plan_id,
        tariff_plan_name,
        is_recursive,
        precedence,
        process_before,
        process_after,
        plantype,
        note,
        precedence_accdisc,
        priceplantype,
        region,
        history)
      SELECT to_plan_id,
        tariff_plan_name,
        is_recursive,
        precedence,
        process_before,
        process_after,
        plantype,
        note,
        precedence_accdisc,
        priceplantype,
        region,
        history
        FROM tariff_plan a
       WHERE a.tariff_plan_id = from_plan_id;

    INSERT INTO tariff_plan_item
      ( tariff_plan_id,
        calctype,
        fee_id,
        tariff_schema_id,
        charging_event_id,
        precedence,
        applytime,
        expiretime,
        apply_method,
        discount_type,
        base_fee_id,
        discount_fee_id,
        charging_cond,
        switch_method,
        switch_unit,
        g_charging_cond,
        billcode,
        select_method,
        tariff_plan_item_name,
        serv_id)
      SELECT to_plan_id,
        calctype,
        fee_id,
        tariff_schema_id,
        charging_event_id,
        precedence,
        applytime,
        expiretime,
        apply_method,
        discount_type,
        base_fee_id,
        discount_fee_id,
        charging_cond,
        switch_method,
        switch_unit,
        g_charging_cond,
        billcode,
        select_method,
        tariff_plan_item_name,
        serv_id
        FROM tariff_plan_item a
       WHERE a.tariff_plan_id = from_plan_id;

    INSERT INTO product_element(product_element_type, product_element_code, product_element_name, note, history)
    select product_element_type, to_plan_id, product_element_name, note, history
    from product_element a where a.product_element_code=to_plan_id;

    INSERT INTO product_element_tariff(PRODUCT_ELEMENT_TYPE,PRODUCT_ELEMENT_CODE,TARIFF_PLAN_ID)
    select product_element_type, to_plan_id, decode(tariff_plan_id,a.product_element_code,to_plan_id,tariff_plan_id)
    from product_element_tariff a where a.product_element_code=to_plan_id;

  END;
  --/*拷贝账单分拆定义*/
  PROCEDURE ap_copy_bill_split(from_plan_id NUMBER, to_plan_id NUMBER) IS
    v_cnt NUMBER;
  BEGIN
    v_cnt := 0;

    SELECT COUNT(*)
      INTO v_cnt
      FROM ItemSplit_Cfg a
     WHERE a.ITEMADDCODE = to_plan_id;

    SELECT COUNT(*)
      INTO v_cnt
      FROM ItemSplit_Def a
     WHERE a.ITEMADDCODE = to_plan_id;

    IF v_cnt > 0
    THEN
	dbms_output.put_line('30');
      raise_application_error(-20101,
                              '复制的计费事件在目标库的ItemSplit_Cfg或ItemSplit_Def有定义');
      RETURN;
    END IF;

    INSERT INTO ItemSplit_Cfg
      ( itemcode,
        itemaddcode,
        splitcode,
        discval,
        maxrefval,
        inuse)
      SELECT  itemcode,
              to_plan_id,
              splitcode,
              discval,
              maxrefval,
              inuse
        FROM ItemSplit_Cfg a
       WHERE a.itemaddcode = from_plan_id;

    INSERT INTO ItemSplit_Def
      ( itemcode,
        itemaddcode,
        itemcode_sub,
        percentum,
        inuse,
        businessclass,
        business,
        isusecount,
        beyonditemcode)
      SELECT  itemcode,
              to_plan_id,
              itemcode_sub,
              percentum,
              inuse,
              businessclass,
              business,
              isusecount,
              beyonditemcode
        FROM ItemSplit_Def a
       WHERE a.itemaddcode = from_plan_id;
  END;
  PROCEDURE ap_do_base(v_tariff_plan_id IN NUMBER,
                       v_attr_id        IN NUMBER,
                       v_freevolum      IN NUMBER) AS
  BEGIN
    INSERT INTO billdisc_def
      (tariffplan_id,
       planitem_id,
       disc_entity,
       disc_cond,
       g_disc_cond,
       disc_type,
       refitem_type,
       freerefitem_type,
       refitem_code,
       refitem_cycles,
       refitem_cycle_offset,
       refvalue_calc_type,
       refvalue_unit,
       disc_object_type,
       disc_item_code,
       start_cycle_offset,
       valid_cycle_type,
       valid_cycles,
       inuse,
       tariffplan_type,
       disc_item_type,
       freerefitem_min_usage,
       note,
       isdisctspec,
       specdisct_expr,
       g_specdisct_expr)
    VALUES
      (v_tariff_plan_id,
       50,
       1,
       '0010-.0011==50 and((not(0003-.0084 match [''US2'']and 0003-.0084<>''US28'')))',
       'Class引用源事件.时长==50 and((not(用户.用户状态 match [''US2'']and 用户.用户状态<>''US28'')))',
       4,
       1,
       1,
       '',
       1,
       0,
       1,
       2,
       2,
       v_attr_id,
       0,
       4,
       1,
       1,
       3,
       1,
       0,
       '',
       '0',
       '0',
       '');
    INSERT INTO discitem_def
      (tariffplan_id,
       planitem_id,
       discitem_id,
       min_refvalue,
       max_refvalue,
       calc_type,
       disc_value,
       max_disc_value,
       inuse,
       refvalue_add,
       disct_expr,
       g_disct_expr,
       discitem_cond,
       g_discitem_cond)
    VALUES
      (v_tariff_plan_id,
       50,
       1,
       0,
       99999999,
       1,
       v_freevolum,
       v_freevolum,
       1,
       NULL,
       '0',
       '',
       '',
       '');
    INSERT INTO disc_batch_list
      (batch_id, tariffplan_id, start_cycle, end_cycle)
    VALUES
      (50, v_tariff_plan_id, to_char(to_date('20201030','yyyymmdd'), 'yyyymm') || '01', 99999999);
	  dbms_output.put_line('31');

  END;
  PROCEDURE ap_do_inter(v_imsi      IN VARCHAR2,
                        v_roamfee   IN NUMBER, --0 不需要维护 国际漫游费用，单位元  如果是有主被叫，是主叫的漫游费用
                        v_troamfee  IN NUMBER, --0 不需要维护 国际漫游费用，单位元  如果是有主被叫，是被叫的漫游费用
                        v_lfee      IN NUMBER, -- 0 不需要维护 拨打非本国长途 单位元计费单元是分钟的
                        v_lfee2     IN NUMBER, -- 0 不需要维护，如果是0 单位元，计费单元是分钟的
                        v_smsfee    IN NUMBER, -- 不需要维护,如果是0，单位是元，短信费用
                        v_gprsfee   IN NUMBER, -- 不需要维护，如果是0，单位是分，GPRS费用.
                        v_applytime IN DATE) IS
    --不需要维护
    --不需要维护
    v_hmanage VARCHAR2(32);
    v_hregion VARCHAR2(32);
    v_cnt     NUMBER(10);
  BEGIN
    v_cnt := 0;
    SELECT COUNT(*)
      INTO v_cnt
      FROM mapping_list
     WHERE mapping_id = 10
       AND mapping_sour = v_imsi
       AND nvl(expiretime, to_date('20370101000000', 'yyyymmddhh24miss')) >
           v_applytime;
    IF v_cnt <> 1
    THEN
	dbms_output.put_line('32');
      raise_application_error(-20100,
                              '维护国家的imsi 前5位在值映射10里面没有维护，请维护');
    END IF;
    SELECT TRIM(substr(mapping_dest, 1, instr(mapping_dest, ',') - 1)),
           TRIM(substr(mapping_dest, instr(mapping_dest, ',') + 1))
      INTO v_hregion, v_hmanage
      FROM mapping_list
     WHERE mapping_id = 10
       AND mapping_sour = v_imsi
       AND nvl(expiretime, to_date('20370101000000', 'yyyymmddhh24miss')) >
           v_applytime;

    /*GPRS国际漫游费用,资费模式282*/
    IF v_gprsfee > 0
    THEN
      SELECT COUNT(1)
        INTO v_cnt
        FROM tariff_item a
       WHERE a.tariff_schema_id = 282
         AND a.tariff_criteria = v_imsi || ',';
		 dbms_output.put_line('33');
      IF v_cnt > 0
      THEN
        UPDATE tariff_item a
           SET expiretime = v_applytime
         WHERE a.tariff_schema_id = 282
           AND a.tariff_criteria = v_imsi || ',';
		   dbms_output.put_line('34');
      END IF;
      INSERT INTO tariff_item
        (tariff_schema_id,
         applytime,
         expiretime,
         tariff_criteria,
         subtariff_type,
         tariff_id,
         ratio,
         ratetype,
         param_string,
         precedence,
         expr_id,
         g_param,
         is_dynamic,
         g_criteria,
         billcode)
      VALUES
        (282,
         v_applytime,
         NULL,
         v_imsi || ',',
         1,
         178,
         v_gprsfee / 0.1,
         ' ',
         NULL,
         5,
         NULL,
         NULL,
         0,
         NULL,
         NULL);
    END IF;
    /*SMS国际漫游费用,资费模式57*/
    IF v_smsfee > 0
    THEN
      SELECT COUNT(1)
        INTO v_cnt
        FROM tariff_item a
       WHERE a.tariff_schema_id = 57
         AND a.tariff_criteria = v_hregion || ',' || v_hmanage || ',';
		 dbms_output.put_line('35');
      IF v_cnt > 0
      THEN
        UPDATE tariff_item a
           SET expiretime = v_applytime
         WHERE a.tariff_schema_id = 57
           AND a.tariff_criteria = v_hregion || ',' || v_hmanage || ',';
		   dbms_output.put_line('36');
      END IF;
      INSERT INTO tariff_item
        (tariff_schema_id,
         applytime,
         expiretime,
         tariff_criteria,
         subtariff_type,
         tariff_id,
         ratio,
         ratetype,
         param_string,
         precedence,
         expr_id,
         g_param,
         is_dynamic,
         g_criteria,
         billcode)
      VALUES
        (57,
         v_applytime,
         NULL,
         v_hregion || ',' || v_hmanage || ',',
         1,
         275,
         v_smsfee / 1,
         ' ',
         NULL,
         5,
         NULL,
         NULL,
         0,
         NULL,
         NULL);
    END IF;
    IF v_roamfee > 0
    THEN
      /*GSM国际漫游费用,资费模式49*/
      IF v_troamfee = 0 OR v_troamfee = v_roamfee
      THEN
        SELECT COUNT(1)
          INTO v_cnt
          FROM tariff_item a
         WHERE a.tariff_schema_id = 49
           AND a.tariff_criteria = v_hregion || ',' || v_hmanage;
		   dbms_output.put_line('37');
        IF v_cnt > 0
        THEN
          UPDATE tariff_item a
             SET expiretime = v_applytime
           WHERE a.tariff_schema_id = 49
             AND a.tariff_criteria = v_hregion || ',' || v_hmanage;
			 dbms_output.put_line('38');
        END IF;
        INSERT INTO tariff_item
          (tariff_schema_id,
           applytime,
           expiretime,
           tariff_criteria,
           subtariff_type,
           tariff_id,
           ratio,
           ratetype,
           param_string,
           precedence,
           expr_id,
           g_param,
           is_dynamic,
           g_criteria,
           billcode)
        VALUES
          (49,
           v_applytime,
           NULL,
           v_hregion || ',' || v_hmanage,
           1,
           206,
           v_roamfee / 1,
           ' ',
           NULL,
           5,
           NULL,
           NULL,
           0,
           NULL,
           NULL);

      ELSE
        /*有被叫漫游费用,维护资费模式3005*/
        INSERT INTO tariff_item
          (tariff_schema_id,
           applytime,
           expiretime,
           tariff_criteria,
           subtariff_type,
           tariff_id,
           ratio,
           ratetype,
           param_string,
           precedence,
           expr_id,
           g_param,
           is_dynamic,
           g_criteria,
           billcode)
        VALUES
          (3005,
           v_applytime,
           NULL,
           v_hregion || ',' || v_hmanage || ',O',
           1,
           3,
           v_roamfee / 0.1,
           ' ',
           '',
           1,
           NULL,
           '',
           0,
           '',
           '');

        INSERT INTO tariff_item
          (tariff_schema_id,
           applytime,
           expiretime,
           tariff_criteria,
           subtariff_type,
           tariff_id,
           ratio,
           ratetype,
           param_string,
           precedence,
           expr_id,
           g_param,
           is_dynamic,
           g_criteria,
           billcode)
        VALUES
          (3005,
           v_applytime,
           NULL,
           v_hregion || ',' || v_hmanage || ',T',
           1,
           3,
           v_troamfee / 0.1,
           ' ',
           '',
           1,
           NULL,
           '',
           0,
           '',
           '');
        INSERT INTO tariff_item
          (tariff_schema_id,
           applytime,
           expiretime,
           tariff_criteria,
           subtariff_type,
           tariff_id,
           ratio,
           ratetype,
           param_string,
           precedence,
           expr_id,
           g_param,
           is_dynamic,
           g_criteria,
           billcode)
        VALUES
          (49,
           v_applytime,
           NULL,
           v_hregion || ',' || v_hmanage,
           2,
           3005,
           1.00,
           ' ',
           '0005-.0706,0005-.0707,0005-.0701',
           5,
           NULL,
           '源计费事件.本方归属区号,源计费事件.本方运营商,源计费事件.话单类型',
           0,
           '',
           '');
		   dbms_output.put_line('39');
      END IF;
    END IF;
    /*GSM国际漫游长途费用,资费模式55*/
    /*拨打非本地长途*/
    IF v_lfee > 0
    THEN
      SELECT COUNT(1)
        INTO v_cnt
        FROM tariff_item a
       WHERE a.tariff_schema_id = 55
         AND a.tariff_criteria = v_hregion || ',' || v_hmanage || ',';
      IF v_cnt > 0
      THEN
        UPDATE tariff_item a
           SET expiretime = v_applytime
         WHERE a.tariff_schema_id = 55
           AND a.tariff_criteria = v_hregion || ',' || v_hmanage || ',';
		   dbms_output.put_line('40');
      END IF;
      INSERT INTO tariff_item
        (tariff_schema_id,
         applytime,
         expiretime,
         tariff_criteria,
         subtariff_type,
         tariff_id,
         ratio,
         ratetype,
         param_string,
         precedence,
         expr_id,
         g_param,
         is_dynamic,
         g_criteria,
         billcode)
      VALUES
        (55,
         v_applytime,
         NULL,
         v_hregion || ',' || v_hmanage || ',',
         1,
         284,
         v_lfee / 10 / 0.01,
         ' ',
         NULL,
         5,
         NULL,
         NULL,
         0,
         NULL,
         NULL);
    END IF;
    IF v_lfee2 > 0 AND v_lfee2 <> v_lfee
    THEN
      SELECT COUNT(1)
        INTO v_cnt
        FROM tariff_item a
       WHERE a.tariff_schema_id = 55
         AND a.tariff_criteria =
             v_hregion || ',' || v_hmanage || ',' || v_hregion;
      IF v_cnt > 0
      THEN
        UPDATE tariff_item a
           SET expiretime = v_applytime
         WHERE a.tariff_schema_id = 55
           AND a.tariff_criteria =
               v_hregion || ',' || v_hmanage || ',' || v_hregion;
			   dbms_output.put_line('41');
      END IF;
      INSERT INTO tariff_item
        (tariff_schema_id,
         applytime,
         expiretime,
         tariff_criteria,
         subtariff_type,
         tariff_id,
         ratio,
         ratetype,
         param_string,
         precedence,
         expr_id,
         g_param,
         is_dynamic,
         g_criteria,
         billcode)
      VALUES
        (55,
         v_applytime,
         NULL,
         v_hregion || ',' || v_hmanage || ',' || v_hregion,
         1,
         284,
         v_lfee2 / 10 / 0.01,
         ' ',
         NULL,
         5,
         NULL,
         NULL,
         0,
         NULL,
         NULL);
    END IF;
  END;
  PROCEDURE ap_gen_billcycle(v_event_id IN NUMBER) IS
    v_cnt NUMBER;
  BEGIN
    SELECT COUNT(*)
      INTO v_cnt
      FROM charging_cycle_schema a
     WHERE a.cycle_schema_id = 2
       AND a.charging_event_id = v_event_id;
    IF v_cnt <> 1
    THEN
	dbms_output.put_line('42');
      raise_application_error(-20100,
                              '请在帐期方案2里面维护数据，然后执行');
    END IF;
    FOR cc IN (SELECT * FROM custom_cycle_schema)
    LOOP
      INSERT INTO charging_cycle_schema
        (cycle_schema_id,
         charging_event_id,
         billing_cond,
         expr_id,
         g_billing_cond)
        SELECT cc.cycle_schema_id,
               v_event_id,
               billing_cond,
               expr_id,
               g_billing_cond
          FROM charging_cycle_schema a
         WHERE a.cycle_schema_id = 2
           AND a.charging_event_id = v_event_id;
      dbms_output.put_line(NVL(LENGTH('43'), 0));
		   --dbms_output.put_line('43');
    END LOOP;

  END;

  /*拷贝帐务优惠*/
  PROCEDURE ap_copy_account_priv(from_plan_id NUMBER, to_plan_id NUMBER) IS
    v_num NUMBER;
  BEGIN
    INSERT INTO billdisc_def
      (tariffplan_id,
       planitem_id,
       disc_entity,
       disc_cond,
       g_disc_cond,
       disc_type,
       refitem_type,
       freerefitem_type,
       refitem_code,
       refitem_cycles,
       refitem_cycle_offset,
       refvalue_calc_type,
       refvalue_unit,
       disc_object_type,
       disc_item_code,
       start_cycle_offset,
       valid_cycle_type,
       valid_cycles,
       inuse,
       tariffplan_type,
       disc_item_type,
       freerefitem_min_usage,
       note)
      SELECT to_plan_id,
             planitem_id,
             disc_entity,
             disc_cond,
             g_disc_cond,
             disc_type,
             refitem_type,
             freerefitem_type,
             refitem_code,
             refitem_cycles,
             refitem_cycle_offset,
             refvalue_calc_type,
             refvalue_unit,
             disc_object_type,
             disc_item_code,
             start_cycle_offset,
             valid_cycle_type,
             valid_cycles,
             inuse,
             tariffplan_type,
             disc_item_type,
             freerefitem_min_usage,
             note
        FROM billdisc_def a
       WHERE a.tariffplan_id = from_plan_id;

    INSERT INTO discitem_def
      (tariffplan_id,
       planitem_id,
       discitem_id,
       min_refvalue,
       max_refvalue,
       calc_type,
       disc_value,
       max_disc_value,
       inuse,
       refvalue_add)
      SELECT to_plan_id,
             planitem_id,
             discitem_id,
             min_refvalue,
             max_refvalue,
             calc_type,
             disc_value,
             max_disc_value,
             inuse,
             refvalue_add
        FROM discitem_def a
       WHERE a.tariffplan_id = from_plan_id;

    INSERT INTO discvaluepercent_def
      (tariffplan_id, planitem_id, cond_expr, percent, g_cond_expr, seqno)
      SELECT to_plan_id,
             planitem_id,
             cond_expr,
             percent,
             g_cond_expr,
             seqno
        FROM discvaluepercent_def
       WHERE tariffplan_id = from_plan_id;

    INSERT INTO discrefitem_def
      (tariffplan_id, planitem_id, item_code, item_desc, inuse)
      SELECT to_plan_id, planitem_id, item_code, item_desc, inuse
        FROM discrefitem_def
       WHERE tariffplan_id = from_plan_id;

    /*配置批次定义*/
    /*
      SELECT COUNT(*)
        INTO v_num
        FROM disc_batch_list
       WHERE tariffplan_id = from_plan_id;

      IF v_num = 0
      THEN
        raise_application_error(-60427, '复制的帐务优惠在批次里面没有定义');
        ROLLBACK;
        RETURN;
      END IF;
    */
    /*加入批次定义*/
    /*
      INSERT INTO disc_batch_list
        (batch_id, tariffplan_id, start_cycle, end_cycle)
        SELECT batch_id,
               to_plan_id,
               to_number(to_char(SYSDATE, 'yyyymm')),
               end_cycle
          FROM disc_batch_list
         WHERE tariffplan_id = from_plan_id;
		 
    */
	dbms_output.put_line('44');
  END;


  PROCEDURE deep_copy_tariff_plan_vpn(a_from_plan NUMBER, a_to_plan NUMBER) AS
    v_tariff_name tariff_plan.tariff_plan_name%TYPE;
    v_schema_id   tariff_plan_item.tariff_schema_id%TYPE;
  BEGIN
    dbms_output.put_line('deep copy tariff_plan from: ' || a_from_plan ||
                         ' to: ' || a_to_plan);
    SELECT tariff_plan_name
      INTO v_tariff_name
      FROM tariff_plan_vpn
     WHERE tariff_plan_id = a_to_plan;
    FOR r IN (SELECT *
                FROM tariff_plan_item_vpn
               WHERE tariff_plan_id = a_from_plan)
    LOOP
      r.tariff_plan_id := a_to_plan;
      INSERT INTO tariff_plan_item_vpn VALUES r;
	  dbms_output.put_line('45');
    END LOOP;
    FOR r IN (SELECT DISTINCT tariff_schema_id
                FROM tariff_plan_item_vpn
               WHERE tariff_plan_id = a_to_plan)
    LOOP
      v_schema_id := deep_copy_tariff_schema(r.tariff_schema_id,
                                             v_tariff_name);
      UPDATE tariff_plan_item_vpn
         SET tariff_schema_id = v_schema_id
       WHERE tariff_plan_id = a_to_plan
         AND tariff_schema_id = r.tariff_schema_id;
		 dbms_output.put_line('46');
    END LOOP;
  END;
BEGIN
  -- Initialization
  --<Statement>;
  NULL;
  dbms_output.put_line('47');

END tu;


$

  CREATE OR REPLACE PACKAGE BODY "TU_BAK" IS
  -- Private type declarations
  --type <TypeName> is <Datatype>;
  -- Private constant declarations
  --<ConstantName> constant <Datatype> := <Value>;
  -- Private variable declarations
  --<VariableName> <Datatype>;
  -- Function and procedure implementations
  /*function <FunctionName>(<Parameter> <Datatype>) return <Datatype> is
  <LocalVariable> <Datatype>;
  begin
  <Statement>;
  return(<Result>);
  end;
  */
  PROCEDURE add_default_plan_item(a_plan_id NUMBER) IS
  BEGIN
    INSERT INTO tariff_plan_item
      (tariff_plan_id,
       calctype,
       fee_id,
       tariff_schema_id,
       charging_event_id,
       precedence,
       applytime,
       expiretime,
       apply_method,
       discount_type,
       base_fee_id,
       discount_fee_id,
       charging_cond,
       switch_method,
       switch_unit,
       g_charging_cond)
    VALUES
      (a_plan_id,
       1,
       1,
       388,
       7,
       5,
       to_date('01-01-2000', 'dd-mm-yyyy'),
       NULL,
       2,
       6,
       0,
       0,
       ' ',
       2,
       60,
       ' ');
    COMMIT;
	dbms_output.put_line('1');
  END;
  PROCEDURE copy_plan_items(a_from_plan_id NUMBER, a_to_plan_id NUMBER) IS
  BEGIN
    INSERT INTO tariff_plan_item
      (tariff_plan_id,
       calctype,
       fee_id,
       tariff_schema_id,
       charging_event_id,
       precedence,
       applytime,
       expiretime,
       apply_method,
       discount_type,
       base_fee_id,
       discount_fee_id,
       charging_cond,
       switch_method,
       switch_unit,
       g_charging_cond,
       billcode,
       select_method,
       tariff_plan_item_sn)
      SELECT a_to_plan_id,
             calctype,
             fee_id,
             tariff_schema_id,
             charging_event_id,
             precedence,
             applytime,
             expiretime,
             apply_method,
             discount_type,
             base_fee_id,
             discount_fee_id,
             charging_cond,
             switch_method,
             switch_unit,
             g_charging_cond,
             billcode,
             select_method,
             tariff_plan_item_sn
        FROM tariff_plan_item
       WHERE tariff_plan_id = a_from_plan_id;
    COMMIT;
	dbms_output.put_line('2');
  END;
  PROCEDURE copy_schema(a_from_schema_id NUMBER, a_to_schema_id NUMBER) IS
  BEGIN
    DELETE FROM tariff_schema
     WHERE tariff_schema_id = a_to_schema_id
       AND TRIM(tariff_name) IS NULL;
    INSERT INTO tariff_schema
      (tariff_schema_id,
       tariff_name,
       tariff_type,
       fieldcount,
       field_def,
       match_order,
       match_type,
       apply_method,
       refid,
       discount_fee_id,
       g_field_def,
       round_method,
       round_scale,
       ref_offset,
       event_id)
      SELECT a_to_schema_id,
             tariff_name,
             tariff_type,
             fieldcount,
             field_def,
             match_order,
             match_type,
             apply_method,
             refid,
             discount_fee_id,
             g_field_def,
             round_method,
             round_scale,
             ref_offset,
             event_id
        FROM tariff_schema
       WHERE tariff_schema_id = a_from_schema_id;
    --values (200, '费用 (0.20元/分钟)', 1, 0, null, 2, 1, 2, 0, 0, null, 0, 0, 0, null);
    DELETE FROM tariff_item WHERE tariff_schema_id = a_to_schema_id;
    INSERT INTO tariff_item
      (tariff_schema_id,
       applytime,
       expiretime,
       tariff_criteria,
       subtariff_type,
       tariff_id,
       ratio,
       ratetype,
       param_string,
       precedence,
       expr_id,
       g_param,
       is_dynamic,
       g_criteria)
      SELECT a_to_schema_id,
             applytime,
             expiretime,
             tariff_criteria,
             subtariff_type,
             tariff_id,
             ratio,
             ratetype,
             param_string,
             precedence,
             expr_id,
             g_param,
             is_dynamic,
             g_criteria
        FROM tariff_item
       WHERE tariff_schema_id = a_from_schema_id;
    --values (200, to_date('01-01-2001', 'dd-mm-yyyy'), null, ' ', 1, 86, 1, ' ', null, 5, null, null, 0, ' ');
    COMMIT;
	dbms_output.put_line('3');
  END;
  /* Formatted on 2005/10/24 15:52 (Formatter Plus v4.8.6) */
  PROCEDURE copy_month_priprivilege(a_from_plan_id NUMBER,
                                    a_to_plan_id   NUMBER) IS
  BEGIN
    INSERT INTO privilege_new_month_callfee
      (hregion,
       itemname,
       feetype,
       VALUE,
       freevolume,
       attr_id,
       billingcode,
       freetype,
       halftype,
       billifstop,
       calctype,
       specialtype)
      SELECT hregion,
             itemname,
             feetype,
             VALUE,
             freevolume,
             attr_id,
             a_to_plan_id,
             freetype,
             halftype,
             billifstop,
             calctype,
             specialtype
        FROM privilege_new_month_callfee
       WHERE billingcode = a_from_plan_id;
    COMMIT;
	dbms_output.put_line('4');
  END;
  PROCEDURE copy_schema2(a_from_schema_id NUMBER, a_to_schema_id NUMBER) IS
  BEGIN
    DELETE FROM tariff_schema WHERE tariff_schema_id = a_to_schema_id;
    INSERT INTO tariff_schema
      (tariff_schema_id,
       tariff_name,
       tariff_type,
       fieldcount,
       field_def,
       match_order,
       match_type,
       apply_method,
       refid,
       discount_fee_id,
       g_field_def,
       round_method,
       round_scale,
       ref_offset,
       event_id)
      SELECT a_to_schema_id,
             tariff_name,
             tariff_type,
             fieldcount,
             field_def,
             match_order,
             match_type,
             apply_method,
             refid,
             discount_fee_id,
             g_field_def,
             round_method,
             round_scale,
             ref_offset,
             event_id
        FROM tariff_schema
       WHERE tariff_schema_id = a_from_schema_id;
    --values (200, '费用 (0.20元/分钟)', 1, 0, null, 2, 1, 2, 0, 0, null, 0, 0, 0, null);
    DELETE FROM tariff_item WHERE tariff_schema_id = a_to_schema_id;
    INSERT INTO tariff_item
      (tariff_schema_id,
       applytime,
       expiretime,
       tariff_criteria,
       subtariff_type,
       tariff_id,
       ratio,
       ratetype,
       param_string,
       precedence,
       expr_id,
       g_param,
       is_dynamic,
       g_criteria)
      SELECT a_to_schema_id,
             applytime,
             expiretime,
             tariff_criteria,
             subtariff_type,
             tariff_id,
             ratio,
             ratetype,
             param_string,
             precedence,
             expr_id,
             g_param,
             is_dynamic,
             g_criteria
        FROM tariff_item
       WHERE tariff_schema_id = a_from_schema_id;
    --values (200, to_date('01-01-2001', 'dd-mm-yyyy'), null, ' ', 1, 86, 1, ' ', null, 5, null, null, 0, ' ');
    COMMIT;
	dbms_output.put_line('5');
  END;
  FUNCTION new_cdrtype(cdrtype VARCHAR2) RETURN VARCHAR2 IS
  BEGIN
    IF cdrtype = '01'
    THEN
	dbms_output.put_line('6');
      RETURN 'O';
    ELSIF cdrtype = '02'
    THEN
	dbms_output.put_line('7');
      RETURN 'T';
    ELSIF cdrtype = '03'
    THEN
	dbms_output.put_line('8');
      RETURN 'F';
    ELSE
	dbms_output.put_line('9');
      RETURN NULL;
    END IF;
  END;
  FUNCTION new_calltype(callflag VARCHAR2) RETURN VARCHAR2 IS
  BEGIN
    IF callflag = '000'
    THEN
	dbms_output.put_line('10');
      RETURN NULL;
    ELSIF callflag = 'L00'
    THEN
	dbms_output.put_line('11');
      RETURN '0|1';
    ELSIF callflag = 'L01'
    THEN
	dbms_output.put_line('12');
      RETURN '21|22';
    ELSIF callflag = 'L02'
    THEN
	dbms_output.put_line('13');
      RETURN '23';
    ELSE
	dbms_output.put_line('14');
      RETURN NULL;
    END IF;
  END;
  FUNCTION get_ratecode(a_rate NUMBER, a_unit NUMBER) RETURN NUMBER IS
    code NUMBER := 0;
  BEGIN
    --  select rate_code into code from tcl_rate_dict where rate=a_rate and unit=a_unit;
    --  return code;
    --exception
    --    when others then
	dbms_output.put_line('15');
    RETURN NULL;
  END;
  FUNCTION correct_date(a_date DATE) RETURN DATE IS
  BEGIN
    IF a_date > to_date('20370101', 'yyyymmdd')
    THEN
	dbms_output.put_line('16');
      RETURN to_date('20370101', 'yyyymmdd');
    ELSE
	dbms_output.put_line('17');
      RETURN a_date;
    END IF;
  END;
  PROCEDURE copy_usage_attr(a_from_attr NUMBER, a_to_attr NUMBER) IS
  BEGIN
    INSERT INTO entity_attr_def
      (entity_id,
       attr_id,
       attr_name,
       attr_type,
       multivalue,
       isunique,
       isresident,
       attr_class,
       datatype,
       length,
       scale,
       expire_days)
      SELECT entity_id,
             a_to_attr,
             attr_name,
             attr_type,
             multivalue,
             isunique,
             isresident,
             attr_class,
             datatype,
             length,
             scale,
             expire_days
        FROM entity_attr_def
       WHERE entity_id = 1
         AND attr_id = a_from_attr;
    INSERT INTO total_usage_attr_def
      (entity_id, attr_id, usage_type)
      SELECT entity_id, a_to_attr, usage_type
        FROM total_usage_attr_def
       WHERE entity_id = 1
         AND attr_id = a_from_attr;
    INSERT INTO free_usage_attr_def
      (entity_id,
       attr_id,
       fixed_free_usage,
       free_limit,
       free_type)
      SELECT entity_id,
             a_to_attr,
             fixed_free_usage,
             free_limit,
             free_type
        FROM free_usage_attr_def
       WHERE entity_id = 1
         AND attr_id = a_from_attr;
    INSERT INTO base_attr_def
      (entity_id,
       attr_id,
       base_attr_type,
       base_attr_id,
       charging_event_id,
       call_usage_cond,
       round_unit,
       round_type,
       g_call_usage_cond)
      SELECT entity_id,
             a_to_attr,
             base_attr_type,
             base_attr_id,
             charging_event_id,
             call_usage_cond,
             round_unit,
             round_type,
             g_call_usage_cond
        FROM base_attr_def
       WHERE entity_id = 1
         AND attr_id = a_from_attr;
    COMMIT;
	dbms_output.put_line('18');
  END;
  FUNCTION deep_copy_tariff_schema(a_from_schema NUMBER,
                                   a_tariff_name VARCHAR2 := NULL)
    RETURN NUMBER AS
    v_new_schema NUMBER(9);
    v_schema_id  NUMBER(9);
  BEGIN
    SELECT MIN(tariff_schema_id)
      INTO v_new_schema
      FROM tariff_schema
     WHERE tariff_name LIKE '#UNUSED#'
       AND tariff_schema_id > 460;
    dbms_output.put_line('new tariff_schema from:' || a_from_schema ||
                         ' to : ' || v_new_schema);
    FOR r IN (SELECT *
                FROM tariff_schema
               WHERE tariff_schema_id = a_from_schema)
    LOOP
      r.tariff_schema_id := v_new_schema;
      IF a_tariff_name IS NOT NULL
      THEN
        r.tariff_name := a_tariff_name;
		dbms_output.put_line('19');
      END IF;
      UPDATE tariff_schema
         SET ROW = r
       WHERE tariff_schema_id = v_new_schema;
      DELETE FROM tariff_item WHERE tariff_schema_id = v_new_schema;
    END LOOP;
    FOR r IN (SELECT *
                FROM tariff_item
               WHERE tariff_schema_id = a_from_schema)
    LOOP
      r.tariff_schema_id := v_new_schema;
      INSERT INTO tariff_item VALUES r;
	  dbms_output.put_line('20');
    END LOOP;
    FOR r IN (SELECT DISTINCT to_number(tariff_id) schema_id
                FROM tariff_item
               WHERE tariff_schema_id = v_new_schema
                 AND subtariff_type = 2)
    LOOP
      v_schema_id := deep_copy_tariff_schema(r.schema_id);
      UPDATE tariff_item
         SET tariff_id = v_schema_id
       WHERE tariff_schema_id = v_new_schema
         AND subtariff_type = 2
         AND tariff_id = r.schema_id;
      dbms_output.put_line('updated ' || SQL%ROWCOUNT || ' rows.');
	  dbms_output.put_line('21');
    END LOOP;
    RETURN v_new_schema;
  END;
  FUNCTION af_getstr(str VARCHAR2, v_split VARCHAR2, pos NUMBER)
    RETURN VARCHAR2 IS
    v_arstr    VARCHAR2(400);
    v_str      VARCHAR2(300);
    v_last_pos NUMBER;
    v_pos      NUMBER;
  BEGIN
    v_str := '';
    IF substr(str, length(str), 1) <> v_split
    THEN
      v_arstr := str || v_split;
	  dbms_output.put_line('22');
    ELSE
      v_arstr := str;
	  dbms_output.put_line('23');
    END IF;
    IF pos != 1
    THEN
      v_last_pos := instr(v_arstr, v_split, 1, pos - 1);
      v_pos      := instr(v_arstr, v_split, 1, pos);
      v_str      := substr(v_arstr, v_last_pos + 1, v_pos - v_last_pos - 1);
	  dbms_output.put_line('24');
    ELSE
      v_pos := instr(v_arstr, v_split, 1, pos);
      v_str := substr(v_arstr, 1, v_pos - 1);
	  dbms_output.put_line('25');
    END IF;

    RETURN v_str;
  END;
  --/*拷贝资费政策*/
  PROCEDURE ap_copy_tariff_plan(from_plan_id NUMBER, to_plan_id NUMBER) IS
    v_cnt NUMBER;
  BEGIN
    v_cnt := 0;

    SELECT COUNT(*)
      INTO v_cnt
      FROM tariff_plan a
     WHERE a.tariff_plan_id = to_plan_id;

    SELECT COUNT(*)
      INTO v_cnt
      FROM tariff_plan_item a
     WHERE a.tariff_plan_id = to_plan_id;

    IF v_cnt > 0
    THEN
	dbms_output.put_line('26');
      raise_application_error(-20101,
                              '复制的计费事件在目标库的TARIFF_PLAN或TARIFF_PLAN_ITEM有定义');
      RETURN;
    END IF;

    INSERT INTO tariff_plan
      (tariff_plan_id,
       tariff_plan_name,
       is_recursive,
       precedence,
       process_before,
       process_after,
       plantype,
       note,
       precedence_accdisc)
      SELECT to_plan_id,
             tariff_plan_name,
             is_recursive,
             precedence,
             process_before,
             process_after,
             plantype,
             note,
             precedence_accdisc
        FROM tariff_plan a
       WHERE a.tariff_plan_id = from_plan_id;

    INSERT INTO tariff_plan_item
      (tariff_plan_id,
       calctype,
       fee_id,
       tariff_schema_id,
       charging_event_id,
       precedence,
       applytime,
       expiretime,
       apply_method,
       discount_type,
       base_fee_id,
       discount_fee_id,
       charging_cond,
       switch_method,
       switch_unit,
       g_charging_cond,
       billcode,
       select_method,
       tariff_plan_item_sn)
      SELECT to_plan_id,
             calctype,
             fee_id,
             tariff_schema_id,
             charging_event_id,
             precedence,
             applytime,
             expiretime,
             apply_method,
             discount_type,
             base_fee_id,
             discount_fee_id,
             charging_cond,
             switch_method,
             switch_unit,
             g_charging_cond,
             billcode,
             select_method,
             tariff_plan_item_sn
        FROM tariff_plan_item a
       WHERE a.tariff_plan_id = from_plan_id;
	   dbms_output.put_line('27');
  END;
  PROCEDURE ap_do_base(v_tariff_plan_id IN NUMBER,
                       v_attr_id        IN NUMBER,
                       v_freevolum      IN NUMBER) AS
  BEGIN
    INSERT INTO billdisc_def
      (tariffplan_id,
       planitem_id,
       disc_entity,
       disc_cond,
       g_disc_cond,
       disc_type,
       refitem_type,
       freerefitem_type,
       refitem_code,
       refitem_cycles,
       refitem_cycle_offset,
       refvalue_calc_type,
       refvalue_unit,
       disc_object_type,
       disc_item_code,
       start_cycle_offset,
       valid_cycle_type,
       valid_cycles,
       inuse,
       tariffplan_type,
       disc_item_type,
       freerefitem_min_usage,
       note,
       isdisctspec,
       specdisct_expr,
       g_specdisct_expr)
    VALUES
      (v_tariff_plan_id,
       50,
       1,
       '0010-.0011==50 and((not(0003-.0084 match [''US2'']and 0003-.0084<>''US28'')))',
       'Class引用源事件.时长==50 and((not(用户.用户状态 match [''US2'']and 用户.用户状态<>''US28'')))',
       4,
       1,
       1,
       '',
       1,
       0,
       1,
       2,
       2,
       v_attr_id,
       0,
       4,
       1,
       1,
       3,
       1,
       0,
       '',
       '0',
       '0',
       '');
    INSERT INTO discitem_def
      (tariffplan_id,
       planitem_id,
       discitem_id,
       min_refvalue,
       max_refvalue,
       calc_type,
       disc_value,
       max_disc_value,
       inuse,
       refvalue_add,
       disct_expr,
       g_disct_expr,
       discitem_cond,
       g_discitem_cond)
    VALUES
      (v_tariff_plan_id,
       50,
       1,
       0,
       99999999,
       1,
       v_freevolum,
       v_freevolum,
       1,
       NULL,
       '0',
       '',
       '',
       '');
    INSERT INTO disc_batch_list
      (batch_id, tariffplan_id, start_cycle, end_cycle)
    VALUES
      (50, v_tariff_plan_id, to_char(to_date('20201030','yyyymmdd'), 'yyyymm') || '01', 99999999);
dbms_output.put_line('28');
  END;
  PROCEDURE ap_do_inter(v_imsi      IN VARCHAR2,
                        v_roamfee   IN NUMBER, --0 不需要维护 国际漫游费用，单位元  如果是有主被叫，是主叫的漫游费用
                        v_troamfee  IN NUMBER, --0 不需要维护 国际漫游费用，单位元  如果是有主被叫，是被叫的漫游费用
                        v_lfee      IN NUMBER, -- 0 不需要维护 拨打非本国长途 单位元计费单元是分钟的
                        v_lfee2     IN NUMBER, -- 0 不需要维护，如果是0 单位元，计费单元是分钟的
                        v_smsfee    IN NUMBER, -- 不需要维护,如果是0，单位是元，短信费用
                        v_gprsfee   IN NUMBER, -- 不需要维护，如果是0，单位是分，GPRS费用.
                        v_applytime IN DATE) IS
    --不需要维护
    --不需要维护
    v_hmanage VARCHAR2(32);
    v_hregion VARCHAR2(32);
    v_cnt     NUMBER(10);
  BEGIN
    v_cnt := 0;
    SELECT COUNT(*)
      INTO v_cnt
      FROM mapping_list
     WHERE mapping_id = 10
       AND mapping_sour = v_imsi
       AND nvl(expiretime, to_date('20370101000000', 'yyyymmddhh24miss')) >
           v_applytime;
    IF v_cnt <> 1
    THEN
	dbms_output.put_line('29');
      raise_application_error(-20100,
                              '维护国家的imsi 前5位在值映射10里面没有维护，请维护');
    END IF;
    SELECT TRIM(substr(mapping_dest, 1, instr(mapping_dest, ',') - 1)),
           TRIM(substr(mapping_dest, instr(mapping_dest, ',') + 1))
      INTO v_hregion, v_hmanage
      FROM mapping_list
     WHERE mapping_id = 10
       AND mapping_sour = v_imsi
       AND nvl(expiretime, to_date('20370101000000', 'yyyymmddhh24miss')) >
           v_applytime;

    /*GPRS国际漫游费用,资费模式282*/
    IF v_gprsfee > 0
    THEN
      SELECT COUNT(1)
        INTO v_cnt
        FROM tariff_item a
       WHERE a.tariff_schema_id = 282
         AND a.tariff_criteria = v_imsi || ',';
		 dbms_output.put_line('30');
      IF v_cnt > 0
      THEN
        UPDATE tariff_item a
           SET expiretime = v_applytime
         WHERE a.tariff_schema_id = 282
           AND a.tariff_criteria = v_imsi || ',';
		   dbms_output.put_line('31');
      END IF;
      INSERT INTO tariff_item
        (tariff_schema_id,
         applytime,
         expiretime,
         tariff_criteria,
         subtariff_type,
         tariff_id,
         ratio,
         ratetype,
         param_string,
         precedence,
         expr_id,
         g_param,
         is_dynamic,
         g_criteria,
         billcode)
      VALUES
        (282,
         v_applytime,
         NULL,
         v_imsi || ',',
         1,
         178,
         v_gprsfee / 0.1,
         ' ',
         NULL,
         5,
         NULL,
         NULL,
         0,
         NULL,
         NULL);
    END IF;
    /*SMS国际漫游费用,资费模式57*/
    IF v_smsfee > 0
    THEN
      SELECT COUNT(1)
        INTO v_cnt
        FROM tariff_item a
       WHERE a.tariff_schema_id = 57
         AND a.tariff_criteria = v_hregion || ',' || v_hmanage || ',';
		 dbms_output.put_line('32');
      IF v_cnt > 0
      THEN
        UPDATE tariff_item a
           SET expiretime = v_applytime
         WHERE a.tariff_schema_id = 57
           AND a.tariff_criteria = v_hregion || ',' || v_hmanage || ',';
		   dbms_output.put_line('33');
      END IF;
      INSERT INTO tariff_item
        (tariff_schema_id,
         applytime,
         expiretime,
         tariff_criteria,
         subtariff_type,
         tariff_id,
         ratio,
         ratetype,
         param_string,
         precedence,
         expr_id,
         g_param,
         is_dynamic,
         g_criteria,
         billcode)
      VALUES
        (57,
         v_applytime,
         NULL,
         v_hregion || ',' || v_hmanage || ',',
         1,
         275,
         v_smsfee / 1,
         ' ',
         NULL,
         5,
         NULL,
         NULL,
         0,
         NULL,
         NULL);
    END IF;
    IF v_roamfee > 0
    THEN
      /*GSM国际漫游费用,资费模式49*/
      IF v_troamfee = 0 OR v_troamfee = v_roamfee
      THEN
        SELECT COUNT(1)
          INTO v_cnt
          FROM tariff_item a
         WHERE a.tariff_schema_id = 49
           AND a.tariff_criteria = v_hregion || ',' || v_hmanage;
		   dbms_output.put_line('34');
        IF v_cnt > 0
        THEN
          UPDATE tariff_item a
             SET expiretime = v_applytime
           WHERE a.tariff_schema_id = 49
             AND a.tariff_criteria = v_hregion || ',' || v_hmanage;
			 dbms_output.put_line('35');
        END IF;
        INSERT INTO tariff_item
          (tariff_schema_id,
           applytime,
           expiretime,
           tariff_criteria,
           subtariff_type,
           tariff_id,
           ratio,
           ratetype,
           param_string,
           precedence,
           expr_id,
           g_param,
           is_dynamic,
           g_criteria,
           billcode)
        VALUES
          (49,
           v_applytime,
           NULL,
           v_hregion || ',' || v_hmanage,
           1,
           206,
           v_roamfee / 1,
           ' ',
           NULL,
           5,
           NULL,
           NULL,
           0,
           NULL,
           NULL);

      ELSE
        /*有被叫漫游费用,维护资费模式3005*/
        INSERT INTO tariff_item
          (tariff_schema_id,
           applytime,
           expiretime,
           tariff_criteria,
           subtariff_type,
           tariff_id,
           ratio,
           ratetype,
           param_string,
           precedence,
           expr_id,
           g_param,
           is_dynamic,
           g_criteria,
           billcode)
        VALUES
          (3005,
           v_applytime,
           NULL,
           v_hregion || ',' || v_hmanage || ',O',
           1,
           3,
           v_roamfee / 0.1,
           ' ',
           '',
           1,
           NULL,
           '',
           0,
           '',
           '');

        INSERT INTO tariff_item
          (tariff_schema_id,
           applytime,
           expiretime,
           tariff_criteria,
           subtariff_type,
           tariff_id,
           ratio,
           ratetype,
           param_string,
           precedence,
           expr_id,
           g_param,
           is_dynamic,
           g_criteria,
           billcode)
        VALUES
          (3005,
           v_applytime,
           NULL,
           v_hregion || ',' || v_hmanage || ',T',
           1,
           3,
           v_troamfee / 0.1,
           ' ',
           '',
           1,
           NULL,
           '',
           0,
           '',
           '');
        INSERT INTO tariff_item
          (tariff_schema_id,
           applytime,
           expiretime,
           tariff_criteria,
           subtariff_type,
           tariff_id,
           ratio,
           ratetype,
           param_string,
           precedence,
           expr_id,
           g_param,
           is_dynamic,
           g_criteria,
           billcode)
        VALUES
          (49,
           v_applytime,
           NULL,
           v_hregion || ',' || v_hmanage,
           2,
           3005,
           1.00,
           ' ',
           '0005-.0706,0005-.0707,0005-.0701',
           5,
           NULL,
           '源计费事件.本方归属区号,源计费事件.本方运营商,源计费事件.话单类型',
           0,
           '',
           '');
		   dbms_output.put_line('36');
      END IF;
    END IF;
    /*GSM国际漫游长途费用,资费模式55*/
    /*拨打非本地长途*/
    IF v_lfee > 0
    THEN
      SELECT COUNT(1)
        INTO v_cnt
        FROM tariff_item a
       WHERE a.tariff_schema_id = 55
         AND a.tariff_criteria = v_hregion || ',' || v_hmanage || ',';
		 dbms_output.put_line('37');
      IF v_cnt > 0
      THEN
        UPDATE tariff_item a
           SET expiretime = v_applytime
         WHERE a.tariff_schema_id = 55
           AND a.tariff_criteria = v_hregion || ',' || v_hmanage || ',';
		   dbms_output.put_line('38');
      END IF;
      INSERT INTO tariff_item
        (tariff_schema_id,
         applytime,
         expiretime,
         tariff_criteria,
         subtariff_type,
         tariff_id,
         ratio,
         ratetype,
         param_string,
         precedence,
         expr_id,
         g_param,
         is_dynamic,
         g_criteria,
         billcode)
      VALUES
        (55,
         v_applytime,
         NULL,
         v_hregion || ',' || v_hmanage || ',',
         1,
         284,
         v_lfee / 10 / 0.01,
         ' ',
         NULL,
         5,
         NULL,
         NULL,
         0,
         NULL,
         NULL);
    END IF;
    IF v_lfee2 > 0 AND v_lfee2 <> v_lfee
    THEN
      SELECT COUNT(1)
        INTO v_cnt
        FROM tariff_item a
       WHERE a.tariff_schema_id = 55
         AND a.tariff_criteria =
             v_hregion || ',' || v_hmanage || ',' || v_hregion;
			 dbms_output.put_line('39');
      IF v_cnt > 0
      THEN
        UPDATE tariff_item a
           SET expiretime = v_applytime
         WHERE a.tariff_schema_id = 55
           AND a.tariff_criteria =
               v_hregion || ',' || v_hmanage || ',' || v_hregion;
			   dbms_output.put_line('40');
      END IF;
      INSERT INTO tariff_item
        (tariff_schema_id,
         applytime,
         expiretime,
         tariff_criteria,
         subtariff_type,
         tariff_id,
         ratio,
         ratetype,
         param_string,
         precedence,
         expr_id,
         g_param,
         is_dynamic,
         g_criteria,
         billcode)
      VALUES
        (55,
         v_applytime,
         NULL,
         v_hregion || ',' || v_hmanage || ',' || v_hregion,
         1,
         284,
         v_lfee2 / 10 / 0.01,
         ' ',
         NULL,
         5,
         NULL,
         NULL,
         0,
         NULL,
         NULL);
    END IF;
  END;
  PROCEDURE ap_gen_billcycle(v_event_id IN NUMBER) IS
    v_cnt NUMBER;
  BEGIN
    SELECT COUNT(*)
      INTO v_cnt
      FROM charging_cycle_schema a
     WHERE a.cycle_schema_id = 2
       AND a.charging_event_id = v_event_id;
    IF v_cnt <> 1
    THEN
	dbms_output.put_line('41');
      raise_application_error(-20100,
                              '请在帐期方案2里面维护数据，然后执行');
    END IF;
    FOR cc IN (SELECT * FROM custom_cycle_schema)
    LOOP
      INSERT INTO charging_cycle_schema
        (cycle_schema_id,
         charging_event_id,
         billing_cond,
         expr_id,
         g_billing_cond)
        SELECT cc.cycle_schema_id,
               v_event_id,
               billing_cond,
               expr_id,
               g_billing_cond
          FROM charging_cycle_schema a
         WHERE a.cycle_schema_id = 2
           AND a.charging_event_id = v_event_id;
		   dbms_output.put_line('42');
    END LOOP;

  END;

  /*拷贝帐务优惠*/
  PROCEDURE ap_copy_account_priv(from_plan_id NUMBER, to_plan_id NUMBER) IS
    v_num NUMBER;
  BEGIN
    INSERT INTO billdisc_def
      (tariffplan_id,
       planitem_id,
       disc_entity,
       disc_cond,
       g_disc_cond,
       disc_type,
       refitem_type,
       freerefitem_type,
       refitem_code,
       refitem_cycles,
       refitem_cycle_offset,
       refvalue_calc_type,
       refvalue_unit,
       disc_object_type,
       disc_item_code,
       start_cycle_offset,
       valid_cycle_type,
       valid_cycles,
       inuse,
       tariffplan_type,
       disc_item_type,
       freerefitem_min_usage,
       note)
      SELECT to_plan_id,
             planitem_id,
             disc_entity,
             disc_cond,
             g_disc_cond,
             disc_type,
             refitem_type,
             freerefitem_type,
             refitem_code,
             refitem_cycles,
             refitem_cycle_offset,
             refvalue_calc_type,
             refvalue_unit,
             disc_object_type,
             disc_item_code,
             start_cycle_offset,
             valid_cycle_type,
             valid_cycles,
             inuse,
             tariffplan_type,
             disc_item_type,
             freerefitem_min_usage,
             note
        FROM billdisc_def a
       WHERE a.tariffplan_id = from_plan_id;

    INSERT INTO discitem_def
      (tariffplan_id,
       planitem_id,
       discitem_id,
       min_refvalue,
       max_refvalue,
       calc_type,
       disc_value,
       max_disc_value,
       inuse,
       refvalue_add)
      SELECT to_plan_id,
             planitem_id,
             discitem_id,
             min_refvalue,
             max_refvalue,
             calc_type,
             disc_value,
             max_disc_value,
             inuse,
             refvalue_add
        FROM discitem_def a
       WHERE a.tariffplan_id = from_plan_id;

    INSERT INTO discvaluepercent_def
      (tariffplan_id, planitem_id, cond_expr, percent, g_cond_expr, seqno)
      SELECT to_plan_id,
             planitem_id,
             cond_expr,
             percent,
             g_cond_expr,
             seqno
        FROM discvaluepercent_def
       WHERE tariffplan_id = from_plan_id;

    INSERT INTO discrefitem_def
      (tariffplan_id, planitem_id, item_code, item_desc, inuse)
      SELECT to_plan_id, planitem_id, item_code, item_desc, inuse
        FROM discrefitem_def
       WHERE tariffplan_id = from_plan_id;

    /*配置批次定义*/
    /*
      SELECT COUNT(*)
        INTO v_num
        FROM disc_batch_list
       WHERE tariffplan_id = from_plan_id;

      IF v_num = 0
      THEN
        raise_application_error(-60427, '复制的帐务优惠在批次里面没有定义');
        ROLLBACK;
        RETURN;
      END IF;
    */
    /*加入批次定义*/
    /*
      INSERT INTO disc_batch_list
        (batch_id, tariffplan_id, start_cycle, end_cycle)
        SELECT batch_id,
               to_plan_id,
               to_number(to_char(SYSDATE, 'yyyymm')),
               end_cycle
          FROM disc_batch_list
         WHERE tariffplan_id = from_plan_id;
    */
	dbms_output.put_line('43');
  END;
  PROCEDURE deep_copy_tariff_plan_vpn(a_from_plan NUMBER, a_to_plan NUMBER) AS
    v_tariff_name tariff_plan.tariff_plan_name%TYPE;
    v_schema_id   tariff_plan_item.tariff_schema_id%TYPE;
  BEGIN
    dbms_output.put_line('deep copy tariff_plan from: ' || a_from_plan ||
                         ' to: ' || a_to_plan);
    SELECT tariff_plan_name
      INTO v_tariff_name
      FROM tariff_plan_vpn
     WHERE tariff_plan_id = a_to_plan;
    FOR r IN (SELECT *
                FROM tariff_plan_item_vpn
               WHERE tariff_plan_id = a_from_plan)
    LOOP
      r.tariff_plan_id := a_to_plan;
      INSERT INTO tariff_plan_item_vpn VALUES r;
	  dbms_output.put_line('44');
    END LOOP;
    FOR r IN (SELECT DISTINCT tariff_schema_id
                FROM tariff_plan_item_vpn
               WHERE tariff_plan_id = a_to_plan)
    LOOP
      v_schema_id := deep_copy_tariff_schema(r.tariff_schema_id,
                                             v_tariff_name);
      UPDATE tariff_plan_item_vpn
         SET tariff_schema_id = v_schema_id
       WHERE tariff_plan_id = a_to_plan
         AND tariff_schema_id = r.tariff_schema_id;
		 dbms_output.put_line('45');
    END LOOP;
  END;
BEGIN
  -- Initialization
  --<Statement>;
  NULL;
dbms_output.put_line('46');
END tu_bak;


$

  CREATE OR REPLACE PACKAGE BODY "UTIL" is

  procedure print_clob(a_script clob)
  is
        vScript clob := a_script;
    vLen number(10);
    vIndex number(10);
  begin
    vLen := length(vScript);
    vIndex := instr(vScript, chr(10));
    while vIndex < vLen and vIndex > 0
    loop
      dbms_output.put_line(substr(vScript,1,vIndex-1));
      vScript := substr(vScript, vIndex + 1);
      vIndex := instr(vScript, chr(10));
	  dbms_output.put_line('1');
    end loop;
    dbms_output.put_line(vScript);
  end;

    procedure parse_event_str(a_str varchar2, a_trim boolean) is
        vEventId number;
        vVersionId number;
        vValue   varchar2(1024);
        vPosition number;
    begin

        if substr(a_str,1,1) = 'V' then
            vEventId := substr(a_str, 2, 3);
            vPosition := 8;
			dbms_output.put_line('2');
        else
            vEventId := substr(a_str, 1, 3);
            vPosition := 3;
			dbms_output.put_line('3');
        end if;

        select nvl(max(version),1) into vVersionId
              from charging_event_def where event_id = vEventId;
        if substr(a_str,1,1) = 'V' then
            vVersionId := substr(a_str, 5, 4);
			dbms_output.put_line('4');
        end if;

        dbms_output.put_line('');
        dbms_output.put_line('## event_id = ' || vEventId || '## version_id = ' || vVersionId);
        for r in (select a.attr_id, a.attr_name, a.field_name, a.length,
                         decode(a.version,
                                vVersionId, decode(a.datatype,
                                                   1, 'str',
                                                   2, 'dt',
                                                   3, 'int',
                                                   4, 'dec',
                                                   'xxx'),
                                nvl(decode(b.datatype,
                                           1, 'str',
                                           2, 'dt',
                                           3, 'int',
                                           4, 'dec',
                                           'xxx'), decode(a.datatype,
                                                          1, 'str',
                                                          2, 'dt',
                                                          3, 'int',
                                                          4, 'dec',
                                                          'xxx'))) datatype_
                   from
                   (select attr_id,attr_name,field_name,length,datatype,
                           (select version from charging_event_def where event_id=vEventId) version
                           from event_attr_def where event_id=vEventId) a,
                   (select * from event_attr_datatype_def where event_id=vEventId
                           and nvl(minversion,1)<=vVersionId and
                           vVersionId <= decode(nvl(maxversion,0),0,9999,maxversion)) b
                   where a.attr_id=b.attr_id(+)
                   )
        loop

            vValue := substrb(a_str, vPosition + 1, r.length);
            if a_trim then
                vValue := trim(vValue);
				dbms_output.put_line('5');
            end if;
            dbms_output.put_line(lpad(r.attr_id, 4) || '#' ||
                                 lpad(vPosition, 4) || ':' ||
                                 lpad(r.length, 4) || ':' ||
                                 lpad(r.datatype_, 3) || ':' ||
                                 lpad(r.attr_name, 16) || ':' ||
                                 lpad(r.field_name, 15) || '=[' || vValue || ']');
            vPosition := vPosition + r.length;
        end loop;
    end;

    --select table_name, count(*), min(trigger_name), max(trigger_name) from user_triggers group by table_name having count(*)>1;

    function get_audit_trigger_script(a_table_name varchar2) return clob is
        vScript    clob := '';
        vTableName varchar2(100);
        vTrigName  varchar2(100);
        vStr       clob;
        vTmpStr    varchar2(2048);
        vIndex     number(10);
    begin
        vTableName := upper(a_table_name);
        vTrigName  := 'TRIG_' || vTableName || '_AUDIT';
        if length(vTrigName) > 30 then
            vTrigName := 'TRIG_' || vTableName;
			dbms_output.put_line('6');
        end if;

        vScript := 'create or replace trigger ' || vTrigName || chr(10);
        vScript := vScript || '    after delete or insert or update on ' ||
                   vTableName || ' for each row' || chr(10);
        vScript := vScript ||
                   '-- trigger created using util.create_audit_trigger() [by: Junqiang,Luo]' ||
                   chr(10);

        vScript := vScript || 'declare' || chr(10) ||
                   '    vAuditLog       pkg_validuser.AuditLogType;' || chr(10) ||
                   'begin' || chr(10) ||
                   '    vAuditLog.TableName:=''' || vTableName || ''';' || chr(10) ||
                   '    if INSERTING then' || chr(10) ||
                   '        vAuditLog.MaintainType := ''I'';' || chr(10) ||
                   '    elsif UPDATING then' || chr(10) ||
                   '        vAuditLog.MaintainType := ''U'';' || chr(10) ||
                   '    elsif DELETING then' || chr(10) ||
                   '        vAuditLog.MaintainType := ''D'';' || chr(10) ||
                   '    else' || chr(10) ||
                   '        vAuditLog.MaintainType := ''?'';' || chr(10) ||
                   '    end if;' || chr(10) ||
                   '    --get key of ' || vTableName || chr(10);
        vStr   := '';
        vIndex := 0;
        for r in (select b.column_name, a.data_type
                    from user_tab_columns a, user_ind_columns b
                   where b.index_name = (select index_name
                                           from user_indexes
                                          where table_name = vTableName
                                            and uniqueness = 'UNIQUE'
                                            and rownum = 1)
                     and a.table_name = vTableName
                     and a.column_name = b.column_name)
        loop
            vIndex := vIndex + 1;
            if r.data_type = 'DATE' then
                vTmpStr := 'to_char(:old.' || r.column_name || ',''yyyy-mm-dd hh24:mi:ss'')';
				dbms_output.put_line('7');
            else
                vTmpStr := ':old.' || r.column_name;
				dbms_output.put_line('8');
            end if;
            if vIndex <> 1 then
                vStr := vStr || ' and ''' || chr(10);
				dbms_output.put_line('9');
            end if;
            vStr := vStr || '        || ''' || r.column_name || '=''''''||' || vTmpStr || '||''''''';
        end loop;
        if vIndex = 0 then
		dbms_output.put_line('10');
            raise_application_error(-20001, 'table has no unique index.');
        end if;
        vScript := vScript ||
                    '    vAuditLog.KeyValue := ''''' || chr(10) || vStr || ''';' || chr(10) || chr(10) ||
                    '    if pkg_validuser.BeginAuditLog(vAuditLog) = 0 then' || chr(10) ||
                    '        return;' || chr(10) ||
                    '    end if;' || chr(10) || chr(10);
        for r in (select column_name, data_type
                    from user_tab_columns
                   where table_name = vTableName
                   order by column_id)
        loop
            vScript := vScript ||
                       '    pkg_validuser.WriteAuditLog(vAuditLog, '''|| r.column_name ||
                       ''', :old.' || r.column_name || ', :new.' || r.column_name || ');' || chr(10);
					   dbms_output.put_line('11');
        end loop;
        vScript := vScript || chr(10) ||
                   '    pkg_validuser.EndAuditLog(vAuditLog);' || chr(10) ||
                   'end; --end of this trigger' || chr(10);
        return vScript;
    end;

    procedure create_audit_trigger(a_table_name varchar2,
                                   a_execute    boolean) is
        vScript long;
    begin
        vScript := get_audit_trigger_script(a_table_name);
        if a_execute then
            execute immediate vScript;
			dbms_output.put_line('12');
        else
		dbms_output.put_line('13');
            print_clob(vScript);
        end if;
    end;
/*
  function get_plan_used_tariffs(a_tarifflist sys_refcursor) return PlanUsedTariffSet pipelined
  is
  begin
    return;
  end;

  function get_tariff_plan_used_tariffs(a_planlist sys_refcursor) return PlanUsedTariffSet pipelined
  is
    v PlanUsedTariffSet;
  begin
    v := get_plan_used_tariffs(a_planlist);
    return;
  end;

  function get_tariff_plan_usage_attr(a_planlist sys_refcursor) return PlanUsageAttrSet pipelined
  is
    v_planid number(8);
    v_row PlanUsageAttrRec;
  begin
    loop
        fetch a_planlist into v_planid;
        exit when a_planlist%notfound;
      for r in (select * from tariff_plan where tariff_plan_id=v_planid)
      loop

      v_row.plan_id := v_planid;
      v_row.usage_type := 0;
      v_row.attr_id := 999;
      pipe row(v_row);
    end loop;
    return;
  end;
*/
    function get_event_script(a_event_id number, a_table_name varchar2 := 'event') return clob is
        vScript clob := '';
        vIndex  number(10) := 0;
        vStr    varchar2(100);

        --V3R1C1B093 AR.FUNC.011 39824 add begin
        v_Length  number(10) := 0;
        v_Version  number(10) := 0;
        i  number(10) := 1;
        vScriptvar    varchar2(32767);
        INVALID__DML exception;
        pragma EXCEPTION_INIT(INVALID__DML, -14552);
        --V3R1C1B093 AR.FUNC.011 39824 add end

    begin
        vScript := 'create table '||upper(a_table_name)||' (' || chr(10);
        for r in (select *
                    from event_attr_def
                   where event_id = a_event_id
                     and trim(field_name) is not null
                   order by attr_id)
        loop
            if vIndex > 0 then
                vScript := vScript || ',' || chr(10);
				dbms_output.put_line('14');
            end if;

            --V3R1C1B093 AR.FUNC.011 39824 add get version length begin
            while i <= length(r.field_name) loop
                if ascii(substr(r.field_name, i, 1)) > 127 then
                    raise_application_error(-20001,
                    'include nonsupport character in event_attr_def.field_name: '
                    ||r.field_name);
					dbms_output.put_line('15');
                end if;
                i := i + 1;
            end loop;

            select max(version) into v_Version
              from charging_event_def
             where event_id = a_event_id;

       begin
            select r.length into v_Length
              from event_attr_datatype_def
             where event_id = a_event_id and attr_id = r.attr_id
               and v_Version >= nvl(minversion, 1)
               and v_Version <= decode(nvl(maxversion,0),0,9999,maxversion);
			   dbms_output.put_line('16');
            exception
               when others then
                    v_Length := r.length;
       end;

            if r.datatype = 1 then
                --string
                vStr := 'varchar2(' || v_Length || ')';
				dbms_output.put_line('17');
            elsif r.datatype = 2 then
                --datetime
                vStr := 'date';
				dbms_output.put_line('18');
            elsif r.datatype = 3 then
                --integer
                vStr := 'number(' || v_Length || ')';
				dbms_output.put_line('19');
            elsif r.datatype = 4 then
                --float
                if r.scale > 0 then
                    v_Length := v_Length - 1;
					dbms_output.put_line('20');
                end if;
                vStr := 'number(' || v_Length || ',' || r.scale || ')';
            elsif r.datatype = 6 then
                --bigint
                vStr := 'number(' || v_Length || ')';
				dbms_output.put_line('21');
            else
			dbms_output.put_line('22');
                raise_application_error(-20001, 'unknown datatype.');
            --V3R1C1B093 AR.FUNC.011 39824 add get version length end

            end if;
            vScript := vScript || upper( r.field_name ||' '|| vStr);
      vIndex := vIndex + 1;
        end loop;
        vScript := vScript || chr(10) || ')' || chr(10);

        --V3R1C1B093 AR.FUNC.011 39824 add get version length begin
        begin
            vScriptvar := 'explain plan for '||vScript;
            execute immediate vScriptvar;
			dbms_output.put_line('23');
        exception
            when INVALID__DML then
			dbms_output.put_line('24');
                return vScript;
            when others then
                return chr(10)||chr(10)||sqlerrm||chr(10)||chr(10)||chr(10)||vScript;
        end;
        --V3R1C1B093 AR.FUNC.011 39824 add get version length end
    end;

    --V3R1C1B093 AR.FUNC.011 39824 2009-2-27 add begin
    procedure check_sql_syntax(a_sql clob)
    is
        l_cursor integer default 0;
    begin
        l_cursor := dbms_sql.open_cursor;
        dbms_sql.parse(l_cursor, 'explain plan for '||a_sql, dbms_sql.native);
        dbms_sql.close_cursor(l_cursor);
		dbms_output.put_line('25');
    end;
    --V3R1C1B093 AR.FUNC.011 39824 2009-2-27 add end

  function get_billingchecks(a_billingdef sys_refcursor) return BillingCheckSet pipelined
  is
    v_item_expr varchar2(200);
    v_pos number(4) := 1;
    v_record BillingCheckRec;
  begin
    loop
      fetch a_billingdef into v_record.event_id, v_item_expr;
      exit when a_billingdef%notfound;
      v_pos := 1;
      loop
          v_pos := instr(v_item_expr,'0005',v_pos);
          exit when v_pos = 0;
        v_pos := v_pos + 6;
        v_record.attr_id := substr(v_item_expr,v_pos,4);
        pipe row(v_record);
        v_pos := v_pos + 4;
		dbms_output.put_line('26');
      end loop;
    end loop;
    return;
  end;
end util;


$


delimiter ;$



