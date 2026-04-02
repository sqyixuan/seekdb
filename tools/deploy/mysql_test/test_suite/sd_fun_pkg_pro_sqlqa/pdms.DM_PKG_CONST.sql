delimiter //;
create or replace package pdms.DM_PKG_CONST is

  CARD_TYPE_ALL constant number(1) := 0;

  
  CARD_TYPE_PORTION constant number(1) := 1;

 
  CARD_TYPE_STATE_UNACTIVE constant number(1) := 0;

 
  CARD_TYPE_STATE_ACTIVE constant number(1) := 1;

  
  CARD_TYPE_ATTR_XCXK constant number(1) := 0;

 
  CARD_TYPE_ATTR_LBYK constant number(1) := 1;

  
  ATTR_LEVEL_0 constant number(2) := 0;

 
  ATTR_LEVEL_1 constant number(2) := 1;

  
  ATTR_LEVEL_2 constant number(2) := 2;

 
  ATTR_END_TAG_0 constant varchar2(10)  := '0';

 
  ATTR_END_TAG_1 constant varchar2(10)  := '1';

  
  ATTR_MAX_NOTE constant number(2)  := 30;

 
  DATA_SEG_STATE_UNUSED constant number(1) := 0;

 
  DATA_SEG_STATE_USED constant number(1) := 1;

 
  DATA_SEG_STATE_ALL_USED constant number(1) := 2;

 
  DATA_SEG_STATE_NG constant number(1) := 3;

  
  MSISDN_UNUSED_COUNT constant number(6) := 100000;

  
  MSISDN_195_UNUSED_COUNT constant number(6) := 20000;


  IMSI_UNUSED_COUNT constant number(5) := 10000;

 
  IMSI_TYPE_NEW constant number(1) := 0;


  IMSI_TYPE_REPLACE constant number(1) := 1;

 
  IMSI_TYPE_NEW_STR constant varchar2(20) := '新卡';

  
  IMSI_TYPE_REPLACE_STR constant varchar2(20) := '补卡';

 
  IMSI_STATE_UNUSED constant number(1) := 0;

 
  IMSI_STATE_USED constant number(1) := 1;

 
  IMSI_STATE_ALL_USED constant number(1) := 2;

  
  IMSI_STATE_NG constant number(1) := 3;

  
  BACTH_STATE_INIT constant number(1) := 0;

 
  BACTH_STATE_GENERATE constant number(1) := 1;

  
  BACTH_STATE_SUBMIT constant number(1) := 2;


  BACTH_STATE_DATA_GEN constant number(1) := 3;

  
  DATA_STATE_INIT constant number(1) := 0;

 
  DATA_STATE_GENERATE constant number(1) := 1;

  
  BC_BACTH_STATE_INIT constant VARCHAR2(1) := 0;

  
  BC_BACTH_STATE_GENERATING constant VARCHAR2(1) := 1;

  
  BC_BACTH_STATE_GENERATE constant VARCHAR2(1) := 2;

  
  BC_BACTH_STATE_EXPORT constant VARCHAR2(1) := 3;

  
  BC_BACTH_STATE_SUBMIT constant VARCHAR2(1) := 4;

  
  CARN_SN_PRESET_FLAG_YES constant varchar2(1) := '0';

  
  CARN_SN_PRESET_FLAG_NO constant varchar2(1) := '1';

 
  CARN_SN_SINGLE_FLAG_YES constant varchar2(1) := '0';

  
  CARN_SN_SINGLE_FLAG_NO constant varchar2(1) := '1';

 
  CARN_SN_SIM_FLAG_YES constant varchar2(2) := '00';

  
  CARN_SN_SIM_FLAG_NO constant varchar2(2) := '01';

  
  CARN_SN_SWP_FLAG_YES constant varchar2(1) := '1';

 
  CARN_SN_SWP_FLAG_NO constant varchar2(1) := '0';

  
  CARN_SN_M2M_FLAG_YES constant varchar2(1) := '1';

  
  CARN_SN_M2M_FLAG_NO constant varchar2(1) := '0';

  
  FACTORY_NAME_RPS constant varchar2(20) := '远程写卡';

  PIN1 constant varchar2(4) := '1234';

 
  KIND_ID constant NUMBER(3)   := 105;

  ORDER_TYPE_LBYK constant varchar2(100)   := '2';

  ORDER_TYPE_CYC constant varchar2(100)  := '3';

  ORDER_TYPE_CYC_LBYK constant varchar2(100)   := '4';

  BK_CARD_TYPE_LBYK constant varchar2(10)  := 'LBYK@';

  BATCH_TYPE_CYC constant varchar2(10)   := 'CYC@';

  IMSI_CYC_STATUS_UNUSED constant NUMBER(1)  := 0;

  IMSI_CYC_STATUS_USED constant NUMBER(1)  := 1;

  IMSI_CYC_STATUS_GENERATE constant NUMBER(1)  := 2;

  IMSI_CYC_TEMP_STATUS_UNUSED constant NUMBER(1)   := 0;

  IMSI_CYC_TEMP_STATUS_USED constant NUMBER(1)   := 1;

  IMSI_CYC_BACKUP_DAYS constant NUMBER(1)  := 7;

  BATCH_TYPE_IMSI_CYC_NO constant NUMBER(1) := 1;

  BATCH_TYPE_IMSI_CYC_YES constant NUMBER(1)  := 2;

  IMSI_AUTO_CYC_LIMIT_MAX constant NUMBER(10) := 100;

  IMSI_AUTO_CYC_STATUS_0 constant NUMBER(10)  := 0;
  
  IMSI_AUTO_CYC_STATUS_1 constant NUMBER(10)  := 1;
  
  IMSI_AUTO_CYC_STATUS_2 constant NUMBER(10)  := 2;
 
  IMSI_AUTO_CYC_STATUS_3 constant NUMBER(10)  := 3;

  IMSI_CYC_IMPORT_TYPE_0  constant NUMBER(1)  := 0;
  IMSI_CYC_IMPORT_TYPE_1  constant NUMBER(1)  := 1;

  BATCH_CYC_IMSI_STATE_0 constant number(1) := 0;
  BATCH_CYC_IMSI_STATE_1 constant number(1) := 1;
  BATCH_CYC_IMSI_STATE_2 constant number(1) := 2;
  BATCH_CYC_IMSI_STATE_3 constant number(1) := 3;
  BATCH_CYC_IMSI_STATE_4 constant number(1) := 4;

  OPERATORS_CHINA_UNICOM constant number(1) := 1;

  OPERATORS_CHINA_TELECOM constant number(1) := 2;

  OPERATORS_CHINA_MOBILE constant number(1) := 3;

  XH_NET_SEG_UNUSED_COUNT constant number(6) := 300000;

  XH_NET_SEG_UNUSED_N0_COUNT constant number(6) := 100000;

  XH_NET_SEG_UNUSED_TY_COUNT constant number(8) := 1700000;

  XH_NET_SEG_STATE_UNUSED constant number(1) := 0;

  XH_NET_SEG_STATE_USED constant number(1) := 1;

  XH_NET_SEG_STATE_ALL_USED constant number(1) := 2;

  XH_IMSI_UNUSED_COUNT constant number(6) := 100000;

  XH_IMSI_TYPE_NEW constant number(1) := 0;

  XH_IMSI_TYPE_REPLACE constant number(1) := 1;


  XH_IMSI_STATE_UNUSED constant number(1) := 0;


  XH_IMSI_STATE_USED constant number(1) := 1;

  XH_IMSI_STATE_ALL_USED constant number(1) := 2;

  XH_DATA_STATE_INIT constant number(1) := 0;

  XH_DATA_STATE_GENERATE constant number(1) := 1;

  YHDZD_FACTORY constant varchar2(1) := 'E';

  AUTO_MAKE_DATATYPE0 constant varchar2(100)   := '0';
  AUTO_MAKE_DATATYPE1 constant varchar2(100)   := '1';
  AUTO_RESOURCE_STATE0 constant varchar2(2)  := '0';
  AUTO_RESOURCE_STATE1 constant varchar2(2)  := '1';
  AUTO_RESOURCE_STATE2 constant varchar2(2)  := '2';
  AUTO_RESOURCE_STATE3 constant varchar2(2)  := '3';

  AUTO_ORDER_TYPE6     constant varchar2(2)  := '6';
  
  AUTO_RESOURCE_MAX constant number(3) := 900;
  
  AUTO_ASSIGN_COUNT constant varchar2(30)  := 'PDMS.AUTO.MAKECOUNT';
 
  AUTO_ASSIGN_FACTORY constant varchar2(30)  := 'PDMS.AUTOBATCH.FACTORY';

  AUTO_ASSIGN_CARD_TYPE constant varchar2(30)  := 'PDMS.AUTOBATCH.CARDTYPE';

  AUTO_IMSI_TYPE constant varchar2(30)   := 'PDMS.AUTOBATCH.IMSITYPE';

  AUTO_BATCH_TYPE constant varchar2(30)  := 'PDMS.AUTOBATCH.BATCHTYPE';

  AUTO_LOG_TYPE constant number(2)   := 7;

end DM_PKG_CONST;
//

delimiter ;//