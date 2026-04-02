delimiter //;
CREATE OR REPLACE FUNCTION "IM"."AF_IM_GETMISORDERID" (V_SWORKORDERID IN VARCHAR2,V_REGION IN VARCHAR2 )
RETURN VARCHAR2 is

v_misOrderIds VARCHAR2(1024); 


begin

for misOrderId in (SELECT T.MIS_ORDER_ID
FROM IM_FLOW_PACT_CHANGE_SIM T
WHERE T.WORKORDERID = v_sWorkOrderId
    and t.region = v_region) loop
 if length(v_misOrderIds) > 0 then
   v_misOrderIds := v_misOrderIds || ',';
 end if;
v_misOrderIds := v_misOrderIds || misOrderId.MIS_ORDER_ID;
end loop;

return v_misOrderIds;

end AF_IM_GETMISORDERID;
//
CREATE OR REPLACE FUNCTION "IM"."AF_IM_GETMISPACTID" (V_SWORKORDERID IN VARCHAR2,V_REGION IN VARCHAR2 )
RETURN VARCHAR2 is
v_misPactIds VARCHAR2(1024); 

begin
for misPactId in ( SELECT T.MIS_PACT_ID
FROM IM_FLOW_PACT_CHANGE_SIM T
WHERE T.WORKORDERID = v_sWorkOrderId
    and t.region = v_region) loop
 if length(v_misPactIds) > 0 then
   v_misPactIds := v_misPactIds || ',';
 end if;
v_misPactIds := v_misPactIds || misPactId.MIS_PACT_ID;
end loop;

return v_misPactIds;

end AF_IM_GETMISPACTID;
//
 CREATE OR REPLACE FUNCTION "IM"."AF_PSI_MAINTAIN_APPLY" (P_REGION       IN VARCHAR2,
                                                 p_orgId        IN VARCHAR2,
                                                 p_operResult   IN VARCHAR2,
                                                 p_maintainAddr IN VARCHAR2)
  RETURN VARCHAR2 is
  v_partnerLevel VARCHAR2(256); 
begin
  begin
    select nvl(t.partner_level,'C')
      into v_partnerLevel
      from psi_info_partner t
     where t.partner_type = 'PMPSalesServ'
       and t.status = '1'
       and t.region = p_region
       and t.partner_id = p_orgId;
  EXCEPTION
    when no_data_found then
      v_partnerLevel := 'C';
  end;
  if p_operResult = 'CHANGE_MACHINE' then
     if v_partnerLevel = 'A+' then
        return 'maintainStatus:PENDING;deliverFlag:0;status:STAY_EXCHANGE;pendingOrgId:' || p_orgId || ';maintainOrgid:' || p_orgId;
     elsif v_partnerLevel ='A' OR v_partnerLevel ='B' then
        return 'maintainStatus:PENDING;deliverFlag:1;status:STAY_EXCHANGE;pendingOrgId:' || p_orgId || ';maintainOrgid:' || p_orgId;
     elsif v_partnerLevel = 'C' then
       return 'maintainStatus:PENDING;deliverFlag:1;status:STAY_MAINTAIN_SEND;pendingOrgId:' || p_orgId || ';maintainOrgid:' || p_orgId;
      end if;
  elsif p_operResult ='MAINTAIN' OR p_operResult = 'RENEW_CHECK' THEN
      if p_maintainAddr = 'MANUFACTURER' then
         return 'maintainStatus:PENDING;deliverFlag:1;status:MANUFACTURER_MAINTAIN;pendingOrgId:' || p_orgId || ';maintainOrgid:' || p_orgId;
       elsif p_maintainAddr = 'MAINTAIN_ADDR' or (p_maintainAddr is null) then
          if v_partnerLevel = 'A+' then
             return 'maintainStatus:PENDING;deliverFlag:0;status:STAY_MAINTAIN;pendingOrgId:' || p_orgId || ';maintainOrgid:' || p_orgId;
          elsif v_partnerLevel = 'A' or v_partnerLevel = 'B'then
             return 'maintainStatus:PENDING;deliverFlag:1;status:STAY_MAINTAIN;pendingOrgId:' || p_orgId || ';maintainOrgid:' || p_orgId;
          elsif v_partnerLevel = 'C' then
             return 'maintainStatus:PENDING;deliverFlag:1;status:STAY_MAINTAIN_SEND;pendingOrgId:' || p_orgId || ';maintainOrgid:' || p_orgId;
          end if;
       end if;
   end if;
end af_psi_maintain_apply;
//
CREATE OR REPLACE FUNCTION "IM"."AF_PSI_MAINTAIN_BACKVENDOR" (P_REGION     IN VARCHAR2,
                                                      p_orgId      IN VARCHAR2,
                                                      p_maintainId IN VARCHAR2,
                                                      p_operResult IN VARCHAR2)
  RETURN VARCHAR2 is
  v_returnOrgId varchar2(32);
begin
  begin
    select nvl(t.return_orgid,p_orgId)
      into v_returnOrgId
      from psi_ass_termmaintain t
     where t.region = p_region
       and t.maintain_id = p_maintainId;
  EXCEPTION
    when no_data_found then
      v_returnOrgId := p_orgId;
  end;
  if p_operResult = 'stopMaintain' then
    if v_returnOrgId = p_orgId then
      return 'maintainStatus:STOP;deliverFlag:0;status:STAY_RETURN;pendingOrgId.:' || p_orgId || ';maintainOrgid:';
    else
      return 'maintainStatus:STOP;deliverFlag:1;status:STAY_RETURN_SEND;pendingOrgId:' || p_orgId || ';maintainOrgid:';
    end if;
  elsif p_operResult = 'returnFactory' then
    if v_returnOrgId = p_orgId then
      return 'maintainStatus:FINISH;deliverFlag:0;status:STAY_RETURN;pendingOrgId:' || p_orgId || ';maintainOrgid:' || p_orgId;
    else
      return 'maintainStatus:FINISH;deliverFlag:1;status:STAY_RETURN_SEND;pendingOrgId:' || p_orgId || ';maintainOrgid:' || p_orgId;
    end if;
  end if;
end af_psi_maintain_backvendor;
//

CREATE OR REPLACE FUNCTION "IM"."AF_PSI_MAINTAIN_EXCHG" (P_REGION     IN VARCHAR2,
                                                 p_orgId      IN VARCHAR2,
                                                 p_maintainId IN VARCHAR2,
                                                 p_operResult IN VARCHAR2)
  RETURN VARCHAR2 is
  v_returnOrgId varchar2(32); 
begin
  begin
    select nvl(t.return_orgid,p_orgId)
      into v_returnOrgId
      from psi_ass_termmaintain t
     where t.region = p_region
       and t.maintain_id = p_maintainId;
  EXCEPTION
    when no_data_found then
      v_returnOrgId := p_orgId;
  end;
  if p_operResult = 'stopMaintain' then
    if v_returnOrgId = p_orgId then
      return 'maintainStatus:STOP;deliverFlag:0;status:STAY_RETURN;pendingOrgId:' || p_orgId || ';maintainOrgid:';
    else
      return 'maintainStatus:STOP;deliverFlag:1;status:STAY_RETURN_SEND;pendingOrgId:' || p_orgId || ';maintainOrgid:';
    end if;
  elsif p_operResult = 'finishMaintain' then
    if v_returnOrgId = p_orgId then
      return 'maintainStatus:FINISH;deliverFlag:0;status:STAY_RETURN;pendingOrgId:' || p_orgId || ';maintainOrgid:' || p_orgId;
    else
      return 'maintainStatus:FINISH;deliverFlag:1;status:STAY_RETURN_SEND;pendingOrgId:' || p_orgId || ';maintainOrgid:' || p_orgId;
    end if;
  end if;
end af_psi_maintain_exchg;
//

CREATE OR REPLACE FUNCTION "IM"."AF_PSI_MAINTAIN_SENDRECV" (P_REGION     IN VARCHAR2,
                                                    p_busiType   IN VARCHAR2,
                                                    p_maintainId IN VARCHAR2,
                                                    p_sendOrgid  IN VARCHAR2)
  RETURN VARCHAR2 is
  v_status         varchar2(32); 
  v_maintainStatus varchar2(32);
  v_selledType     varchar2(32);
  v_maintainAddr   varchar2(32);
begin
  begin
    select t.status, t.maintain_status, t.selled_type, t.MAINTAIN_ADDR
      into v_status, v_maintainStatus, v_selledType, v_maintainAddr
      from psi_ass_termmaintain t
     where t.region = p_region
       and t.maintain_id = p_maintainId;
  EXCEPTION
    when no_data_found then
      return '';
  end;
  if p_busiType = 'MAINTAIN_SEND' then
    if v_maintainStatus = 'PENDING' then
      return 'maintainStatus:;deliverFlag:1;status:STAY_MAINTAIN_RECEIVE;pendingOrgId:' || p_sendOrgid || ';maintainOrgid:' || p_sendOrgid;
    elsif (v_maintainStatus = 'FINISH' OR v_maintainStatus = 'STOP') then
      return 'maintainStatus:;deliverFlag:1;status:STAY_RETURN_RECEIVE;pendingOrgId:' || p_sendOrgid || ';maintainOrgid:';
    end if;
  elsif p_busiType = 'MAINTAIN_RECV' then
    if v_status = 'STAY_RETURN_RECEIVE' then
      return 'maintainStatus:;deliverFlag:0;status:STAY_RETURN';
    elsif v_status = 'STAY_MAINTAIN_RECEIVE' then
      if v_selledType = 'CHANGE_MACHINE' then
        return 'maintainStatus:;deliverFlag:1;status:STAY_EXCHANGE';
      else
        if (v_maintainAddr = 'MAINTAIN_ADDR' OR v_maintainAddr IS NULL) then
          return 'maintainStatus:;deliverFlag:1;status:STAY_MAINTAIN';
        elsif v_maintainAddr = 'MANUFACTURER' then
          return 'maintainStatus:;deliverFlag:1;status:MANUFACTURER_MAINTAIN';
        end if;
      end if;
    end if;
  end if;
end af_psi_maintain_sendRecv;
//

CREATE OR REPLACE FUNCTION "IM"."AF_PSI_MAINTAIN_SERV" (P_REGION     IN VARCHAR2,
                                                p_orgId      IN VARCHAR2,
                                                p_maintainId IN VARCHAR2,
                                                p_operResult IN VARCHAR2)
  RETURN VARCHAR2 is
  v_returnOrgId varchar2(32);
begin
  begin
    select nvl(t.return_orgid,p_orgId)
      into v_returnOrgId
      from psi_ass_termmaintain t
     where t.region = p_region
       and t.maintain_id = p_maintainId;
  EXCEPTION
    when no_data_found then
      v_returnOrgId := p_orgId;
  end;
  if p_operResult = 'returnFactory' then
    return 'maintainStatus:PENDING;deliverFlag:0;status:MANUFACTURER_MAINTAIN;pendingOrgId:' || p_orgId || ';maintainOrgid:' || p_orgId;
  elsif p_operResult = 'stopMaintain' then
    if v_returnOrgId = p_orgId then
      return 'maintainStatus:STOP;deliverFlag:0;status:STAY_RETURN;pendingOrgId:' || p_orgId || ';maintainOrgid:' || p_orgId;
    else
      return 'maintainStatus:STOP;deliverFlag:1;status:STAY_RETURN_SEND;pendingOrgId:' || p_orgId || ';maintainOrgid:' || p_orgId;
    end if;
  elsif p_operResult = 'finishMaintain' then
    if v_returnOrgId = p_orgId then
      return 'maintainStatus:FINISH;deliverFlag:0;status:STAY_RETURN;pendingOrgId:' || p_orgId || ';maintainOrgid:' || p_orgId;

    else
      return 'maintainStatus:FINISH;deliverFlag:1;status:STAY_RETURN_SEND;pendingOrgId:' || p_orgId || ';maintainOrgid:' || p_orgId;
    end if;
  end if;
end af_psi_maintain_serv;
//

CREATE OR REPLACE FUNCTION "IM"."AF_PSI_MAINTAIN_STEP" (P_STATUS IN VARCHAR2)
  RETURN VARCHAR2 is
begin
  if p_status = 'STAY_MAINTAIN_SEND' then
    return '1';
  elsif p_status = 'STAY_MAINTAIN_RECEIVE' then
    return '2';
  elsif p_status = 'STAY_MAINTAIN' or p_status = 'MANUFACTURER_MAINTAIN' or
        p_status = 'STAY_EXCHANGE' then
    return '3';
  elsif p_status = 'STAY_RETURN_SEND' or p_status = 'STAY_RETURN_RECEIVE' then
    return '4';
  elsif p_status = 'STAY_RETURN' or p_status = 'END' or p_status = 'VISIT_END' then
    return '5';
  elsif p_status = 'INVALID' then
    return '0';
  end if;
end af_psi_maintain_step;
//

CREATE OR REPLACE FUNCTION "IM"."AF_PSI_TERMMAIN_FEE_PROCESS" (P_REGION        IN VARCHAR2,
                                                       p_orgId         IN VARCHAR2,
                                                       p_serviceId     IN VARCHAR2,
                                                       p_svcKind       IN VARCHAR2,
                                                       p_saleChannel   IN VARCHAR2,
                                                       p_isQualitySpan IN VARCHAR2,
                                                       p_userLevel     IN VARCHAR2)
  RETURN VARCHAR2 is
  v_receivedFee  varchar(32); --实收
  v_recFee       varchar(32); --应收
  v_disCountFee  varchar(32); --优惠
  v_feeType      varchar2(32); --费用类型
  v_chargeItemId varchar2(32); --费用明细项
begin
  if p_isQualitySpan = 'IN_QUALITY_SPAN' then
    select T.QUALITY_SPAN_FEE,
           T.BASE_FEE,
           (SELECT T3.DESCRIPTION
              FROM PSI_DICT_ITEM T3
             WHERE T.CHARGE_ITEM_ID = T3.DICT_ID
               AND T3.GROUP_ID = 'CHARGE_ITEM'
               AND T3.STATUS = '1'),
           T.CHARGE_ITEM_ID
      into v_receivedFee, v_recFee, v_feeType, v_chargeItemId
      from (select T1.QUALITY_SPAN_FEE,
                   T1.BASE_FEE,
                   T1.CHARGE_ITEM_ID,
                   T2.HICHYOFFSET as SN
              from PSI_CFG_TERMMAIN_FEEDEF T1, ORGANIZATION_CHILD T2
             WHERE T1.REGION     = p_region
               AND T1.SVC_KIND   = p_svcKind
               AND T1.SERVICE_ID = p_serviceId
               AND T2.SUBWAYID   = p_orgId
               AND T1.STATUS     = 'VALID'
               AND T1.ORG_ID     = T2.PARWAYID
             order by T2.HICHYOFFSET ASC) t
        WHERE T.SN = (SELECT MIN(T2.HICHYOFFSET) AS SN
                 from PSI_CFG_TERMMAIN_FEEDEF T1, ORGANIZATION_CHILD T2
               WHERE T1.REGION   = p_region
               AND T1.SVC_KIND   = p_svcKind
               AND T1.SERVICE_ID = p_serviceId
               AND T2.SUBWAYID   = p_orgId
               AND T1.STATUS     = 'VALID'
               AND T1.ORG_ID     = T2.PARWAYID);
  elsif (p_saleChannel = 'SOCIETY' or p_saleChannel = 'OTHER') then
    select (case
             when p_saleChannel = 'SOCIETY' THEN
              T.OTHER_FEE1
             else
              T.OTHER_FEE2
           END),
           T.BASE_FEE,
           (SELECT T3.DESCRIPTION
              FROM PSI_DICT_ITEM T3
             WHERE T.CHARGE_ITEM_ID = T3.DICT_ID
               AND T3.GROUP_ID = 'CHARGE_ITEM'
               AND T3.STATUS = '1'),
           T.CHARGE_ITEM_ID
      into v_receivedFee, v_recFee, v_feeType, v_chargeItemId
       from (select T1.OTHER_FEE1,
                    T1.OTHER_FEE2,
                    T1.BASE_FEE,
                    T1.CHARGE_ITEM_ID,
                    T2.HICHYOFFSET as SN
              from PSI_CFG_TERMMAIN_FEEDEF T1, ORGANIZATION_CHILD T2
             WHERE T1.REGION     = p_region
               AND T1.SVC_KIND   = p_svcKind
               AND T1.SERVICE_ID = p_serviceId
               AND T2.SUBWAYID   = p_orgId
               AND T1.STATUS     = 'VALID' AND T1.ORG_ID     = T2.PARWAYID
             order by T2.HICHYOFFSET ASC) t
        WHERE T.SN = (SELECT MIN(T2.HICHYOFFSET) AS SN
                 from PSI_CFG_TERMMAIN_FEEDEF T1, ORGANIZATION_CHILD T2
               WHERE T1.REGION   = p_region
               AND T1.SVC_KIND   = p_svcKind
               AND T1.SERVICE_ID = p_serviceId
               AND T2.SUBWAYID   = p_orgId
               AND T1.STATUS     = 'VALID'
               AND T1.ORG_ID     = T2.PARWAYID);
  else
    select (case
             when p_userLevel = '0' THEN
              T.VIP_COMM_FEE
             when p_userLevel = '1' THEN
              T.VIP_DIAMOND_FEE
             when p_userLevel = '3' THEN
              T.VIP_GOLD_FEE
             when p_userLevel = '5' THEN
              T.VIP_SILVER_FEE
             when p_userLevel is null THEN
              T.BASE_FEE
           END),
           T.BASE_FEE,
           (SELECT T3.DESCRIPTION
              FROM PSI_DICT_ITEM T3
             WHERE T.CHARGE_ITEM_ID = T3.DICT_ID
               AND T3.GROUP_ID = 'CHARGE_ITEM'
               AND T3.STATUS = '1'),
           T.CHARGE_ITEM_ID
      into v_receivedFee, v_recFee, v_feeType, v_chargeItemId
         from (select T1.VIP_COMM_FEE,
                      T1.VIP_DIAMOND_FEE,
                      T1.VIP_GOLD_FEE,
                      T1.VIP_SILVER_FEE,
                      T1.BASE_FEE,
                      T1.CHARGE_ITEM_ID,
                      T2.HICHYOFFSET as SN
              from PSI_CFG_TERMMAIN_FEEDEF T1, ORGANIZATION_CHILD T2
             WHERE T1.REGION     = p_region
               AND T1.SVC_KIND   = p_svcKind
               AND T1.SERVICE_ID = p_serviceId
               AND T2.SUBWAYID   = p_orgId
               AND T1.STATUS     = 'VALID' AND T1.ORG_ID     = T2.PARWAYID
             order by T2.HICHYOFFSET ASC) t
        WHERE T.SN = (SELECT MIN(T2.HICHYOFFSET) AS SN
                 from PSI_CFG_TERMMAIN_FEEDEF T1, ORGANIZATION_CHILD T2
               WHERE T1.REGION   = p_region
               AND T1.SVC_KIND   = p_svcKind
               AND T1.SERVICE_ID = p_serviceId
               AND T2.SUBWAYID   = p_orgId
               AND T1.STATUS     = 'VALID' AND T1.ORG_ID     = T2.PARWAYID);
  end if;
  if (to_number(v_recFee) < to_number(v_receivedFee)) then
    v_recFee      := v_receivedFee; 
    v_disCountFee := 0; 
  else
    v_disCountFee := to_number(v_recFee) - to_number(v_receivedFee);
  end if;
  v_receivedFee := trim(TO_CHAR(v_receivedFee / 100, '9999999999999999990.99'));
  v_recFee      := trim(TO_CHAR(v_recFee / 100, '9999999999999999990.99'));
  v_disCountFee := trim(TO_CHAR(v_disCountFee / 100, '9999999999999999990.99'));
  return 'receivedFee:' || v_receivedFee || ';recFee:' || v_recFee || ';disCountFee:' || v_disCountFee || ';feeType:' || v_feeType || ';chargeItemId:' || v_chargeItemId;
EXCEPTION
  when no_data_found then
    return '';
end;
//
CREATE OR REPLACE FUNCTION "IM"."AF_TELNUM_RULE" (V_TELNUM VARCHAR2, V_QRYRULE VARCHAR2)
return number
is
v_tmp varchar2(32);
v_count number;
begin
       if v_qryrule = '%AAAAA%' then
          for i in 1..7 loop
              v_tmp := substr(v_telnum, i, 1);
              for j in 1..4 loop
                if v_tmp != substr(v_telnum, i+j, 1) then
                  exit;
                end if;
                v_count := j;
              end loop;

              if v_count = 4 then
                  if i = 7 then
                   return 1;
                  else
                     if v_tmp = substr(v_telnum, i+5, 1) then
                      return 0;
                     else
                      return 1;
                     end if;
                  end if;
              end if;

          end loop;
           return 0;
       end if;

       if v_qryrule = '%AAAA%' then
          for i in 1..8 loop
              v_tmp := substr(v_telnum, i, 1);
              for j in 1..3 loop
                if v_tmp != substr(v_telnum, i+j, 1) then
                  exit;
                end if;
                v_count := j;
              end loop;

              if v_count = 3 then
                  if i = 8 then
                   return 1;
                  else
                     if v_tmp = substr(v_telnum, i+4, 1) then
                      return 0;
                     else
                      return 1;
                     end if;
                  end if;
              end if;

          end loop;
           return 0;
       end if;

       if v_qryrule = '%AAA%' then
          for i in 1..9 loop
              v_tmp := substr(v_telnum, i, 1);
              for j in 1..2 loop
                if v_tmp != substr(v_telnum, i+j, 1) then
                  exit;
                end if;
                v_count := j;
              end loop;

              if v_count = 2 then
                  if i = 9 then
                   return 1;
                  else
                     if v_tmp = substr(v_telnum, i+3, 1) then
                      return 0;
                     else
                      return 1;
                     end if;
                  end if;
              end if;

          end loop;
           return 0;
       end if;

       if v_qryrule = '%AA%' then
          for i in 1..10 loop
              v_tmp := substr(v_telnum, i, 1);
              if v_tmp = substr(v_telnum, i+1, 1) then
                  if i = 10 then
                   return 1;
                  else
                     if v_tmp = substr(v_telnum, i+2, 1) then
                      return 0;
                     else
                      return 1;
                     end if;
                  end if;
              end if;
          end loop;
           return 0;
       end if;

       if v_qryrule = '%ABCDE%' then
          for i in 1..7 loop
              v_tmp := substr(v_telnum, i, 1);
              for j in 1..4 loop
                if v_tmp + j != substr(v_telnum, i+j, 1) then
                  exit;
                end if;
                v_count := j;
              end loop;

              if v_count = 4 then
                  if i = 7 then
                   return 1;
                  else
                     if v_tmp + 5 = substr(v_telnum, i+5, 1) then
                      return 0;
                     else
                      return 1;
                     end if;
                  end if;
              end if;

          end loop;
           return 0;
       end if;

       if v_qryrule = '%ABCDE%' then
          for i in 1..7 loop
              v_tmp := substr(v_telnum, i, 1);
              for j in 1..4 loop
                if v_tmp + j != substr(v_telnum, i+j, 1) then
                  exit;
                end if;
                v_count := j;
              end loop;

              if v_count = 4 then
                  if i = 7 then
                   return 1;
                  else
                     if v_tmp + 5 = substr(v_telnum, i+5, 1) then
                      return 0;
                     else
                      return 1;
                     end if;
                  end if;
              end if;

          end loop;
           return 0;
       end if;

       if v_qryrule = '%ABCD%' then
          for i in 1..8 loop
              v_tmp := substr(v_telnum, i, 1);
              for j in 1..3 loop
                if v_tmp + j != substr(v_telnum, i+j, 1) then
                  exit;
                end if;
                v_count := j;
              end loop;

              if v_count = 3 then
                  if i = 8 then
                   return 1;
                  else
                     if v_tmp + 4 = substr(v_telnum, i+4, 1) then
                      return 0;
                     else
                      return 1;
                     end if;
                  end if;
              end if;

          end loop;
           return 0;
       end if;

       if v_qryrule = '%ABC%' then
          for i in 1..9 loop
              v_tmp := substr(v_telnum, i, 1);
              for j in 1..2 loop
                if v_tmp + j != substr(v_telnum, i+j, 1) then
                  exit;
                end if;
                v_count := j;
              end loop;

              if v_count = 2 then
                  if i = 9 then
                   return 1;
                  else
                     if v_tmp + 3 = substr(v_telnum, i+3, 1) then
                      return 0;
                     else
                      return 1;
                     end if;
                  end if;
              end if;

          end loop;
           return 0;
       end if;

       if v_qryrule = '%AABB%' then
          for i in 1..8 loop
              if      substr(v_telnum, i,   1)  = substr(v_telnum, i+1, 1)
                 and  substr(v_telnum, i+2, 1)  = substr(v_telnum, i+3, 1)
                 and  substr(v_telnum, i,   1) != substr(v_telnum, i+2, 1) then

                  if i = 8 then
                     return 1;
                  else
                     if substr(v_telnum, i+3, 1) = substr(v_telnum, i+4, 1) then
                      return 0;
                     else
                      return 1;
                     end if;
                  end if;

              end if;

          end loop;
           return 0;
       end if;

       if v_qryrule = '%88%' then

          if instr(v_telnum, '88', 1,1) > 0 and instr(v_telnum, '88', 1,2) = 0 then
             return 1;
          end if;

          return 0;

       end if;

       if v_qryrule = '%66%' then

          if instr(v_telnum, '66', 1,1) > 0 and instr(v_telnum, '66', 1,2) = 0 then
             return 1;
          end if;

          return 0;

       end if;

       if v_qryrule = '%4%' then

          if instr(substr(v_telnum, -8, 8), '4', 1,1) > 0  then
             return 0;
          end if;

          return 1;

       end if;

       return 0;
end AF_TELNUM_RULE;
//

  CREATE OR REPLACE FUNCTION "IM"."CHECKCHANNELTYPE" (i_srcOrgId in VARCHAR2,
                                            i_desOrgId in VARCHAR2)
  return varchar2

 IS

  v_srcOrgType VARCHAR2(20);

  v_desOrgType VARCHAR2(20);

  v_srcResult VARCHAR2(20);

  v_desResult VARCHAR2(20);

  v_result VARCHAR2(8);

begin

  SELECT CASE

           WHEN A.ORGTYPE IN ('ogkdAgent', 'ogkdDept') THEN

            (CASE

              WHEN C.AGENTTYPE IN

                   ('110202',
                    '210101',
                    '210102',
                    '130300',
                    '190000',
                    '300020',
                    '300040',
                    '300041',
                    '300042',
                    '300060',
                    '300061',
                    '300062',
                    '300021',
                    '300140',
                    '300141',
                    '300142',
                    '300143') THEN

               'ogkdAgent'

              ELSE

               'ogkdDept'

            END)

           ELSE

            NVL(B.PARTNER_TYPE, A.ORGTYPE)

         END ORG_TYPE
    INTO v_srcOrgType

    FROM ORGANIZATION A, PSI_INFO_PARTNER B, AGENT C

   WHERE A.ORGID = B.ORG_ID(+)

     AND A.STATUS = B.STATUS(+)

     AND A.ORGID = C.AGENTID(+)

     AND A.STATUS = 1

     AND orgid = i_srcOrgId
     AND ROWNUM = 1;

  SELECT CASE

           WHEN A.ORGTYPE IN ('ogkdAgent', 'ogkdDept') THEN

            (CASE

              WHEN C.AGENTTYPE IN

                   ('110202',
                    '210101',
                    '210102',
                    '130300',
                    '190000',
                    '300020',
                    '300040',
                    '300041',
                    '300042',
                    '300060',
                    '300061',
                    '300062',
                    '300021',
                    '300140',
                    '300141',
                    '300142',
                    '300143') THEN

               'ogkdAgent'

              ELSE

               'ogkdDept'

            END)

           ELSE

            NVL(B.PARTNER_TYPE, A.ORGTYPE)

         END ORG_TYPE
    INTO v_desOrgType

    FROM ORGANIZATION A, PSI_INFO_PARTNER B, AGENT C

   WHERE A.ORGID = B.ORG_ID(+)

     AND A.STATUS = B.STATUS(+)

     AND A.ORGID = C.AGENTID(+)

     AND A.STATUS = 1

     AND orgid = i_desOrgId
     AND ROWNUM = 1;

  IF v_srcOrgType != 'ogkdAgent' THEN

    v_srcResult := 'SELF';

  ELSE

    v_srcResult := 'AGENT';

  END IF;

  IF v_desOrgType != 'ogkdAgent' THEN

    v_desResult := 'SELF';

  ELSE

    v_desResult := 'AGENT';

  END IF;

  IF v_srcResult != v_desResult THEN

    v_result := '0';

  ELSE

    v_result := '1';

  END IF;

  return(v_result);

end CHECKCHANNELTYPE;
//
CREATE OR REPLACE FUNCTION "IM"."F_CREATE_PERSONALSTORE" (i_operid IN VARCHAR2 --  操作员工号
                                                  ) return number is

  v_region number(6);

  v_storeName VARCHAR2(200);

  v_relaInfo VARCHAR2(500);

  v_orgId VARCHAR2(32);

  v_numStoreExist number(1);

  v_sotreExpand VARCHAR2(3);

begin
  --查询操作员信息
  select region, opername||'('||operid||')', contactphone, orgid
    into v_region, v_storeName, v_relaInfo, v_orgId
    from operator
   where operid = i_operid;
  --是否已存在个人仓库
  Select Count(*)
    into v_numStoreExist
    From im.im_store t
   where t.region = v_region
     and OPER_ID = i_operid;

  if (v_numStoreExist = 1) then
    --已经存在，这里需要确定是否更新数据
    --update  im.im_store@lnk_zy1a set STORE_NAME= v_storeName,RELA_INFO= v_relaInfo,ORG_ID=v_orgId,STATUS ='VALID' where t.region = v_region and STORE_ID = i_operid;
    return 0;
  else
    --不存在 添加
    select dbms_random.string('X',3)  into v_sotreExpand from dual;
    insert into im.im_store
      (STORE_ID,
       STORE_NAME,
       STORE_GRADE,
       STORE_TYPE,
       STORE_ADDRESS,
       RELA_INFO,
       ASIG_METHOD,
       ORG_ID,
       STATUS,
       EFF_DATE,
       EXP_DATE,
       REGION,
       OPER_ID,
       MANAGE_OPER,
       IF_DEFAULT)
    VALUES
      (v_orgId || '.' || v_sotreExpand,
       v_storeName,
       'PERSONAL',
       'ENTITY',
       '',
       v_relaInfo,
       'DETAIL',
       v_orgId,
       'VALID',
       sysdate,
       to_date('2050/12/31', 'yyyy/mm/dd'),
       v_region,
       i_operid,
       '',
       'NO');
    return 1;
  end if;
  commit;
EXCEPTION
  WHEN others THEN
    return 2;
end F_Create_personalStore;
//

CREATE OR REPLACE FUNCTION "IM"."FUNC_IM_VCLOAD_NAME" (V_REGION VARCHAR2,
                                               V_BATH   VARCHAR2)
  RETURN VARCHAR2 AS

  V_PREFILENAME VARCHAR2(32) := 'vcload.' || TO_CHAR(SYSDATE, 'YYYYMMDD') || '.0';
  V_TMEPBATH    VARCHAR2(32) := '';
  V_COUNT       NUMBER;

BEGIN
  IF 2 = LENGTH(V_BATH) AND V_BATH > '00' AND V_BATH <= '99'
  THEN

    SELECT COUNT(1)
      INTO V_COUNT
      FROM IM_IF_LDSTORE_SYNTASK T
     WHERE T.CREATE_DATE > TRUNC(SYSDATE) AND
           V_BATH = SUBSTR(T.VCLOAD_FILE, LENGTH(T.VCLOAD_FILE) - 1);

    IF V_COUNT = 0
    THEN

      V_PREFILENAME := V_PREFILENAME || V_BATH;
      RETURN V_PREFILENAME;

    END IF;
  END IF;

  SELECT LPAD(NUM, 2, '0') NUM
    INTO V_TMEPBATH
    FROM (SELECT TO_CHAR(ROWNUM) NUM FROM DICT_ITEM WHERE ROWNUM < 100) A
   WHERE A.NUM NOT IN
         (SELECT SUBSTR(VCLOAD_FILE, LENGTH(VCLOAD_FILE) - 1) XX
            FROM IM_IF_LDSTORE_SYNTASK
           WHERE CREATE_DATE > TRUNC(SYSDATE) AND VCLOAD_FILE IS NOT NULL) AND
         ROWNUM = 1;

  V_PREFILENAME := V_PREFILENAME || V_TMEPBATH;
  RETURN V_PREFILENAME;

EXCEPTION
  WHEN NO_DATA_FOUND THEN
    RETURN '';
  WHEN OTHERS THEN
    RETURN '';

END;
//
CREATE OR REPLACE FUNCTION "IM"."HWWH_ZH_BRAND" (v_restype IN varchar2)
  RETURN VARCHAR2 IS
  v_return VARCHAR2(200);
BEGIN
  select t.brand_name
    into v_return
    from psi_cfg_resbrand t
   where t.brand_id = (select a.brand_id
                         from im_res_type a
                        where a.res_type_id = v_restype);

  RETURN(v_return);
EXCEPTION
  WHEN OTHERS THEN
    RETURN NULL;
END hwwh_zh_brand;
//

CREATE OR REPLACE FUNCTION "IM"."HWWH_ZH_ORGNAME" (v_restype IN varchar2)
  RETURN VARCHAR2 IS
  v_return VARCHAR2(200);
BEGIN
  select a.ORGNAME
    into v_return
    from organization a
   where a.ORGID = v_restype;

  RETURN(v_return);
EXCEPTION
  WHEN OTHERS THEN
    RETURN NULL;
END hwwh_zh_orgname;
//
CREATE OR REPLACE FUNCTION "IM"."HWWH_ZH_REGION" (v_restype IN varchar2)
  RETURN VARCHAR2 IS
  v_return VARCHAR2(200);
BEGIN
  select a.regionname
    into v_return
    from region_list a
   where a.region = v_restype;

  RETURN(v_return);
EXCEPTION
  WHEN OTHERS THEN
    RETURN NULL;
END hwwh_zh_region;
//
CREATE OR REPLACE FUNCTION "IM"."HWWH_ZH_RESTYPE" (v_restype IN varchar2)
  RETURN VARCHAR2 IS
  v_return VARCHAR2(200);
BEGIN
  select a.res_type_name
    into v_return
    from im_res_type a
   where a.res_type_id = v_restype;

  RETURN(v_return);
EXCEPTION
  WHEN OTHERS THEN
    RETURN NULL;
END hwwh_zh_restype;
//

CREATE OR REPLACE FUNCTION "IM"."HWWH_ZH_TAC" (v_restype IN varchar2)
  RETURN VARCHAR2 IS
  v_return VARCHAR2(200);
BEGIN
  for i in (select a.tac_id
              from im_imei_tac a
             where a.res_type_id = v_restype
               and a.status = 'VALID') loop
    v_return := v_return || i.tac_id||',';
  end loop;
  RETURN(v_return);
EXCEPTION
  WHEN OTHERS THEN
    RETURN NULL;
END hwwh_zh_tac;
//
CREATE OR REPLACE FUNCTION "IM"."PSI_GETSTATDAYPSITYPE" (inoutFlag    IN VARCHAR2,
                                                   busiType     IN VARCHAR2,
                                                   subsBusiType IN VARCHAR2)
  RETURN VARCHAR2 IS
  v_return VARCHAR2(32);
BEGIN
  v_return := null;

  if inoutFlag = 'IN' and busiType = 'LDSTORE' then
    v_return := 'LDSTORE_NUM'; --入库数量
  elsif inoutFlag = 'OUT' and busiType = 'WITHDRAW' then
    v_return := 'REFUND_NUM'; --退库数量
  elsif inoutFlag = 'IN' and busiType = 'ASIGIN' then
    v_return := 'ASIGIN_NUM'; --调入数量
  elsif inoutFlag = 'OUT' and busiType = 'ASIGOUT' then
    v_return := 'ASIGOUT_NUM'; --调出数量
  elsif inoutFlag = 'OUT' and busiType = 'SALE' then
    v_return := 'SALE_NUM'; --销售数量
  elsif inoutFlag = 'IN' and busiType = 'UNSALE' then
    v_return := 'SALEBACK_NUM'; --销售回退数量
  else
    v_return := null;
  end if;

  RETURN(v_return);
EXCEPTION
  WHEN OTHERS THEN
    RETURN NULL;
END psi_getStatDaypsiType;
//
CREATE OR REPLACE FUNCTION "IM"."PSI_QRYRESTYPEAPPENDATTR" (i_resTypeId IN VARCHAR2,i_saleNumRankType IN VARCHAR2)
  RETURN VARCHAR2 IS
  v_return     VARCHAR2(4000);
  v_attr_id    VARCHAR2(32);
  v_attr_value VARCHAR2(500);
  v_sale_num   number(10);
  v_crwSale_num number(10);
  v_crwSale_num_pre number(10);
BEGIN
  --从机型附加属性表获取
  for attrList in (select t.attr_id, t.attr_value
                     from im_res_type_attr_set t
                    where t.res_type_id = i_resTypeId
                      and t.status = 'VALID') loop
    begin
      v_attr_id    := attrList.attr_id;
      v_attr_value := attrList.attr_value;

      v_return := v_return || v_attr_id || ':' || v_attr_value || ';';
    end;
  end loop;

 --追加实时订货量
 select nvl((select static_amt
              from PSI_INFO_MOB_RANKINGLIST
             where ranking_type = 'GHH_MobRankList'
               and res_type_id = i_resTypeId),
            0)
   into v_sale_num
   from dual;

   v_return := v_return || 'GHH_MobRankList' || ':' || v_sale_num || ';';

  --追加众筹的订货量、认购数量
  if i_saleNumRankType = 'CRW_MobRankList' then
    --众筹采购数量
  select nvl((select static_amt
              from PSI_INFO_MOB_RANKINGLIST
             where ranking_type = 'CRW_MobRankList'
               and res_type_id = i_resTypeId),
            0)
   into v_crwSale_num
   from dual;
   v_return := v_return || 'CRW_MobRankList' || ':' || v_crwSale_num || ';';

   --众筹认购数量
   select nvl((select static_amt1
              from PSI_INFO_MOB_RANKINGLIST
             where ranking_type = 'CRW_MobRankList'
               and res_type_id = i_resTypeId),
            0)
   into v_crwSale_num_pre
   from dual;
   v_return := v_return || 'CRW_MobRankList_Pre' || ':' || v_crwSale_num_pre || ';';
  end if;

  RETURN(v_return);
EXCEPTION
  WHEN OTHERS THEN
    RETURN NULL;
END psi_qryRestypeAppendattr;
//

CREATE OR REPLACE FUNCTION "IM"."QRYCHANNELTYPE" (i_orgid IN VARCHAR2,i_region IN VARCHAR2)

RETURN VARCHAR2

IS

       v_orgType VARCHAR2(20);

       v_result  VARCHAR2(8);

       BEGIN
       SELECT CASE
           WHEN A.ORGTYPE IN ('ogkdAgent', 'ogkdDept') THEN
            (CASE
                WHEN (SELECT M.AGENTTYPE FROM AGENT M WHERE M.AGENTID = A.ORGID) IN
                     ( SELECT DICT_ID FROM PSI_DICT_ITEM WHERE GROUP_ID='LOC_FAN_OGKDAGENT') THEN
                 'ogkdAgent'
                ELSE
                 'ogkdDept'
            END)
           ELSE
            NVL((SELECT M.PARTNER_TYPE
                FROM PSI_INFO_PARTNER M
                WHERE M.ORG_id = A.ORGID)
               , A.ORGTYPE)
      END ORG_TYPE INTO v_orgType
      FROM ORGANIZATION A
     WHERE orgid=i_orgid AND REGION=i_region AND ROWNUM = 1;

           IF v_orgType !='ogkdAgent' THEN

             v_result:='SELF';

           ELSE

             v_result:='AGENT';

           END IF;

        RETURN(v_result);

        EXCEPTION

            WHEN OTHERS THEN

            RETURN NULL;

END QRYCHANNELTYPE;

//

CREATE OR REPLACE FUNCTION "IM"."SNC_RMCONST_6YHN" 	
  ( p_query in varchar2 ) 
  return varchar2              	
  as                                                                   	    
  l_query long;                                                    	    
  l_char  varchar2(10);                                            	    
  l_in_quotes boolean default FALSE;                               	
  begin                                                                	    
  for i in 1 .. length( p_query )                                  	    
  loop                                                             		
  l_char := substr(p_query,i,1);                                   		
  if ( l_char = '''' and l_in_quotes )                             		
  then                                                             		    
  l_in_quotes := FALSE;                                        		
  elsif ( l_char = '''' and NOT l_in_quotes )                      		
  then                                                             		    
  l_in_quotes := TRUE;                                         		    
  l_query := l_query || '''#';                                 		
  end if;                                                          		
  if ( NOT l_in_quotes ) then                                      		    
  l_query := l_query || l_char;                                		
  end if;                                                          	    
  end loop;                                                        	    
  l_query := translate( l_query, '0123456789', '@@@@@@@@@@' );     	    
  for i in 0 .. 8 loop                                             		
  l_query := replace( l_query, lpad('@',10-i,'@'), '@' );          		
  l_query := replace( l_query, lpad(' ',10-i,' '), ' ' );          	    
  end loop;                                                        	    
  return upper(l_query);                                           	
  end;
//

CREATE OR REPLACE FUNCTION "IM_DICT"."AF_TELNUM_RULE" (V_TELNUM VARCHAR2, V_QRYRULE VARCHAR2)
return number
is
v_tmp varchar2(32);
v_count number;
begin
       if v_qryrule = '%AAAAA%' then
          for i in 1..7 loop
              v_tmp := substr(v_telnum, i, 1);
              for j in 1..4 loop
                if v_tmp != substr(v_telnum, i+j, 1) then
                  exit;
                end if;
                v_count := j;
              end loop;

              if v_count = 4 then
                  if i = 7 then
                   return 1;
                  else
                     if v_tmp = substr(v_telnum, i+5, 1) then
                      return 0;
                     else
                      return 1;
                     end if;
                  end if;
              end if;

          end loop;
           return 0;
       end if;

       if v_qryrule = '%AAAA%' then
          for i in 1..8 loop
              v_tmp := substr(v_telnum, i, 1);
              for j in 1..3 loop
                if v_tmp != substr(v_telnum, i+j, 1) then
                  exit;
                end if;
                v_count := j;
              end loop;

              if v_count = 3 then
                  if i = 8 then
                   return 1;
                  else
                     if v_tmp = substr(v_telnum, i+4, 1) then
                      return 0;
                     else
                      return 1;
                     end if;
                  end if;
              end if;

          end loop;
           return 0;
       end if;

       if v_qryrule = '%AAA%' then
          for i in 1..9 loop
              v_tmp := substr(v_telnum, i, 1);
              for j in 1..2 loop
                if v_tmp != substr(v_telnum, i+j, 1) then
                  exit;
                end if;
                v_count := j;
              end loop;

              if v_count = 2 then
                  if i = 9 then
                   return 1;
                  else
                     if v_tmp = substr(v_telnum, i+3, 1) then
                      return 0;
                     else
                      return 1;
                     end if;
                  end if;
              end if;

          end loop;
           return 0;
       end if;

       if v_qryrule = '%AA%' then
          for i in 1..10 loop
              v_tmp := substr(v_telnum, i, 1);
              if v_tmp = substr(v_telnum, i+1, 1) then
                  if i = 10 then
                   return 1;
                  else
                     if v_tmp = substr(v_telnum, i+2, 1) then
                      return 0;
                     else
                      return 1;
                     end if;
                  end if;
              end if;
          end loop;
           return 0;
       end if;

       if v_qryrule = '%ABCDE%' then
          for i in 1..7 loop
              v_tmp := substr(v_telnum, i, 1);
              for j in 1..4 loop
                if v_tmp + j != substr(v_telnum, i+j, 1) then
                  exit;
                end if;
                v_count := j;
              end loop;

              if v_count = 4 then
                  if i = 7 then
                   return 1;
                  else
                     if v_tmp + 5 = substr(v_telnum, i+5, 1) then
                      return 0;
                     else
                      return 1;
                     end if;
                  end if;
              end if;

          end loop;
           return 0;
       end if;

       if v_qryrule = '%ABCDE%' then
          for i in 1..7 loop
              v_tmp := substr(v_telnum, i, 1);
              for j in 1..4 loop
                if v_tmp + j != substr(v_telnum, i+j, 1) then
                  exit;
                end if;
                v_count := j;
              end loop;

              if v_count = 4 then
                  if i = 7 then
                   return 1;
                  else
                     if v_tmp + 5 = substr(v_telnum, i+5, 1) then
                      return 0;
                     else
                      return 1;
                     end if;
                  end if;
              end if;

          end loop;
           return 0;
       end if;

       if v_qryrule = '%ABCD%' then
          for i in 1..8 loop
              v_tmp := substr(v_telnum, i, 1);
              for j in 1..3 loop
                if v_tmp + j != substr(v_telnum, i+j, 1) then
                  exit;
                end if;
                v_count := j;
              end loop;

              if v_count = 3 then
                  if i = 8 then
                   return 1;
                  else
                     if v_tmp + 4 = substr(v_telnum, i+4, 1) then
                      return 0;
                     else
                      return 1;
                     end if;
                  end if;
              end if;

          end loop;
           return 0;
       end if;

       if v_qryrule = '%ABC%' then
          for i in 1..9 loop
              v_tmp := substr(v_telnum, i, 1);
              for j in 1..2 loop
                if v_tmp + j != substr(v_telnum, i+j, 1) then
                  exit;
                end if;
                v_count := j;
              end loop;

              if v_count = 2 then
                  if i = 9 then
                   return 1;
                  else
                     if v_tmp + 3 = substr(v_telnum, i+3, 1) then
                      return 0;
                     else
                      return 1;
                     end if;
                  end if;
              end if;

          end loop;
           return 0;
       end if;

       if v_qryrule = '%AABB%' then
          for i in 1..8 loop
              if      substr(v_telnum, i,   1)  = substr(v_telnum, i+1, 1)
                 and  substr(v_telnum, i+2, 1)  = substr(v_telnum, i+3, 1)
                 and  substr(v_telnum, i,   1) != substr(v_telnum, i+2, 1) then

                  if i = 8 then
                     return 1;
                  else
                     if substr(v_telnum, i+3, 1) = substr(v_telnum, i+4, 1) then
                      return 0;
                     else
                      return 1;
                     end if;
                  end if;

              end if;

          end loop;
           return 0;
       end if;

       if v_qryrule = '%88%' then

          if instr(v_telnum, '88', 1,1) > 0 and instr(v_telnum, '88', 1,2) = 0 then
             return 1;
          end if;

          return 0;

       end if;

       if v_qryrule = '%66%' then

          if instr(v_telnum, '66', 1,1) > 0 and instr(v_telnum, '66', 1,2) = 0 then
             return 1;
          end if;

          return 0;

       end if;

       if v_qryrule = '%4%' then

          if instr(substr(v_telnum, -8, 8), '4', 1,1) > 0  then
             return 0;
          end if;

          return 1;

       end if;

       return 0;
end AF_TELNUM_RULE;
//

delimiter ;//