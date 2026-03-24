delimiter //;
CREATE OR REPLACE FUNCTION PPS."FN_CUST_ISEXIST" (i_Source_Person_Id Varchar2
                                          ,i_Id               Varchar2)
   Return Number Is

   v_Count_a Number(10);
   v_Count_b Number(10);
   v_Count_c Number(10);

Begin

   Select Count(1)
     Into v_Count_a
     From Tbl_Ids_Person
    Where Source_Person_Id = i_Source_Person_Id;

   If v_Count_a = 1 Then
      Return 1;
   Else
      Select Count(1)
        Into v_Count_b
        From (Select Source_Person_Id
                    ,Name
                    ,Sex
                    ,Bthdate
                    ,Hdesp
                    ,Idtype
                    ,Branch_Id
                    ,Diedate
                From Tbl_Ids_Person
               Where (Id = i_Id Or Id15 = i_Id)
               Order By Sno Desc)
       Where Rownum = 1;

      If v_Count_b = 1 Then
         Return 2;
      Else
         Select Count(1)
           Into v_Count_c
           From (Select Source_Person_Id
                       ,Name
                       ,Sex
                       ,Bthdate
                       ,Hdesp
                       ,Idtype
                       ,Branch_Id
                       ,Diedate
                   From Tbl_Ids_Person
                  Where (Id = Replace(i_Id, 'X', 'x') Or
                        Id15 = Replace(i_Id, 'x', 'X'))
                  Order By Sno Desc)
          Where Rownum = 1;
         If v_Count_c = 1 Then
            Return 3;
         Elsif v_Count_c = 0 Then
            Return 4;
         End If;
      End If;
   End If;
Exception
   When Others Then
      Return 5;
End;
//

CREATE OR REPLACE FUNCTION PPS."FN_PID15TO18" (i_Pid Varchar2) Return Varchar Is
   v_1      Number(1);
   v_2      Number(1);
   v_3      Number(1);
   v_4      Number(1);
   v_5      Number(1);
   v_6      Number(1);
   v_7      Number(1);
   v_8      Number(1);
   v_9      Number(1);
   v_10     Number(1);
   v_11     Number(1);
   v_12     Number(1);
   v_13     Number(1);
   v_14     Number(1);
   v_15     Number(1);
   v_16     Number(1);
   v_17     Number(1);
   v_Sum    Number(30);
   v_Mod    Varchar2(10);
   v_18     Varchar2(10);
   v_Result Varchar2(20);
Begin
   If Length(i_Pid) = 15 Then
      v_1  := To_Number(Substr(i_Pid, 1, 1));
      v_2  := To_Number(Substr(i_Pid, 2, 1));
      v_3  := To_Number(Substr(i_Pid, 3, 1));
      v_4  := To_Number(Substr(i_Pid, 4, 1));
      v_5  := To_Number(Substr(i_Pid, 5, 1));
      v_6  := To_Number(Substr(i_Pid, 6, 1));
      v_7  := 1;
      v_8  := 9;
      v_9  := To_Number(Substr(i_Pid, 7, 1));
      v_10 := To_Number(Substr(i_Pid, 8, 1));
      v_11 := To_Number(Substr(i_Pid, 9, 1));
      v_12 := To_Number(Substr(i_Pid, 10, 1));
      v_13 := To_Number(Substr(i_Pid, 11, 1));
      v_14 := To_Number(Substr(i_Pid, 12, 1));
      v_15 := To_Number(Substr(i_Pid, 13, 1));
      v_16 := To_Number(Substr(i_Pid, 14, 1));
      v_17 := To_Number(Substr(i_Pid, 15, 1));

      v_Sum := v_1 * 7 + v_2 * 9 + v_3 * 10 + v_4 * 5 + v_5 * 8 + v_6 * 4 +
               v_7 * 2 + v_8 * 1 + v_9 * 6 + v_10 * 3 + v_11 * 7 + v_12 * 9 +
               v_13 * 10 + v_14 * 5 + v_15 * 8 + v_16 * 4 + v_17 * 2;

      v_Mod := To_Char(Mod(v_Sum, 11));
      Select Decode(v_Mod,
                    '0',
                    '1',
                    '1',
                    '0',
                    '2',
                    'X',
                    '3',
                    '9',
                    '4',
                    '8',
                    '5',
                    '7',
                    '6',
                    '6',
                    '7',
                    '5',
                    '8',
                    '4',
                    '9',
                    '3',
                    '10',
                    '2')
        Into v_18
        From Dual;

      v_Result := Substr(i_Pid, 1, 6) || '19' || Substr(i_Pid, 7, 9) || v_18;

      Return v_Result;
   Else
      Return '';
   End If;
End;
//

CREATE OR REPLACE PACKAGE PPS."BMDM_CONST" is


  const_fgs constant varchar2(1000) := '21,25';
  v_char    constant char(1) :='''';
end bmdm_const;
//






CREATE OR REPLACE PROCEDURE "P_PUB_INSERT_LOG" (v_work_date in varchar2, v_proc_type in varchar2, v_proc_name in varchar2, v_step_no in varchar2, v_step_desc in varchar2, v_row_num number, v_sql_code varchar2, v_sql_errm varchar2, v_execute_flag varchar2, v_begin_time date, v_end_time date)
    is

  BEGIN
    INSERT INTO STAT_ERROR_LOG
      (WORK_DATE
      ,PROC_TYPE
      ,PROC_NAME
      ,step_no
      ,step_desc
      ,ROW_NUM
      ,SQL_CODE
      ,SQL_ERRM
      ,EXECUTE_FLAG
      ,begin_time
      ,end_time
      )
    VALUES
      (v_work_date
      ,v_proc_type
      ,v_proc_name
      ,v_step_no
      ,v_step_desc
      ,v_row_num
      ,v_sql_code
      ,v_sql_errm
      ,v_execute_flag
      ,v_begin_time
      ,v_end_time);

 commit;

  END;
//
CREATE OR REPLACE PACKAGE "PKG_HENAN_ORG_ALL" Is
-- yanyan
Procedure proc2020_TBL_BENEFIT_CHK_INFO;
Procedure proc2020_JOB_PAYDIFFERENCE;
Procedure proc2020BENEFIT_INFO_COUNT;
Procedure proc2020FAIL_BENEFIT_INFO;
Procedure proc2020_D01_RISKCON(i_Branchid In D01_RISKCON.BRANCH%Type);
Procedure proc2020_JOB_PAYDETAILED;
Procedure proc2020_JOB_PAYMENTCHECK;
Procedure proc2020_JOB_POLICYNOPAY;
Procedure proc2020_TBL_BEN_NOTI_INFO;
Procedure proc2020_TBL_CUST_BEN_INFO;
-- zhangguoting
Procedure proc2020_tbl_benefit_info;
-- haoxu
Procedure proc2020_BENEFIT_DELETE;
Procedure proc2020_ACTUARY_STATISTICS;
Procedure proc2020_TBL_SURV_DATA_INFO;
Procedure proc2020_SURV_DATA_INFO_ONE;
Procedure proc2020_SURV_DATA_INFO_TMP;
Procedure proc2020_SURV_DATA_INFO_TMP2;
Procedure proc2020_TBL_SURV_TASK_INFO;
Procedure proc2020_TBL_YUGU_PAY;

-- mengnan
Procedure proc2020_tbl_ids_account;
Procedure proc2020_tbl_ids_address;
Procedure proc2020_tbl_ids_cdelrec;
Procedure proc2020_tbl_ids_chgcon;
Procedure proc2020_tbl_ids_debitrec(i_Branchid In tbl_ids_debitrec.branch%Type);
Procedure proc2020_tbl_ids_email;
Procedure proc2020_tbl_ids_empno;
Procedure proc2020_tbl_ids_moneysch(i_Branchid In Tbl_Ids_Moneysch.Branch_Id%Type);
Procedure proc2020_tbl_ids_padrec;
Procedure proc2020_tbl_ids_phone;
Procedure proc2020_tbl_ids_plc_acct;
Procedure proc2020_tbl_ids_realpayrc;
End Pkg_henan_org_all;
//
CREATE OR REPLACE PACKAGE BODY "PKG_HENAN_ORG_ALL" Is

-- yanyan
-- 1
procedure proc2020_TBL_BENEFIT_CHK_INFO is
 cursor c_cur is
    select a.policyno_id,
           a.class_code,
           a.del_code,
           a.type_no,
           a.del_num,
           b.org3_id_new,
           b.org2_id_new
      from TBL_BENEFIT_CHK_INFO a, tbl_org_revolution b
     where a.comp_id = b.org3_id
       and a.branch_id = b.org2_id
       and a.branch_id in ('00000000004511', '00000000004515','00000000004519', '00000000004523','00000000004474');

  v_new_orgcode3 varchar2(14);
  v_new_orgcode2 varchar2(14);
  v_count        number(19);
  v_Work_Date    Varchar2(8) := To_Char(Sysdate, 'yyyymmdd');
  v_Proc_Type    Varchar(10) := '';
  v_Proc_Name    Varchar2(50) := 'proc2020_TBL_BENEFIT_CHK_INFO';
  v_Step_No      Varchar2(10);
  v_Step_Desc    Varchar2(200);
  v_Begin_Time   Date;
  v_End_Time     Date;
  v_Sql_Code     Varchar2(10);
  v_Sql_Errm     Varchar2(200);
  v_Execute_Flag Varchar2(10);
  v_cur          c_cur%rowtype;
begin
  v_count      := 0;
  v_Begin_Time := sysdate;
  open c_cur;
  loop
    fetch c_cur
      into v_cur;
    exit when c_cur%notfound;

    v_Step_No   := 'step_1';
    v_Step_Desc := '保单号:' || v_cur.policyno_id || '险种:' || v_cur.class_code ||
                   '责任:' || v_cur.del_code || '子码:' || v_cur.type_no ||
                   '期次:' || v_cur.del_num;

    update TBL_BENEFIT_CHK_INFO
       set comp_id = v_cur.org3_id_new, branch_id = v_cur.org2_id_new
     where policyno_id = v_cur.policyno_id
       and class_code = v_cur.class_code
       and del_code = v_cur.del_code
       and type_no = v_cur.type_no
       and del_num = v_cur.del_num;

    -- dbms_output.put_line(sqlcode);
    v_count := v_count + 1;
    if mod(v_count, 10000) = 0 then
      commit;
    end if;
  end loop;
  commit;
close c_cur;
  v_End_Time := sysdate;
  v_Step_Desc :='结束';
  v_Execute_Flag := 'y';
  p_Pub_Insert_Log(v_Work_Date,
                   v_Proc_Type,
                   v_Proc_Name,
                   v_Step_No,
                   v_Step_Desc,
                   v_count,
                   v_Sql_Code,
                   v_Sql_Errm,
                   v_Execute_Flag,
                   v_Begin_Time,
                   v_End_Time);

Exception
  When Others Then
    If c_cur%Isopen Then
      Close c_cur;
    End If;
    Rollback;
    v_Sql_Code     := Sqlcode;
    v_Sql_Errm     := Trim(Sqlerrm);
    v_End_Time     := Sysdate;
    v_Execute_Flag := 'n';
    p_Pub_Insert_Log(v_Work_Date,
                     v_Proc_Type,
                     v_Proc_Name,
                     v_Step_No,
                     v_Step_Desc,
                     v_count,
                     v_Sql_Code,
                     v_Sql_Errm,
                     v_Execute_Flag,
                     v_Begin_Time,
                     v_End_Time);
    close c_cur;
end;


-- 2
procedure proc2020_JOB_PAYDIFFERENCE is
 cursor c_cur is
    select a.rowid
      from JOB_PAYDIFFERENCE a
     where  a.branchid in ('00000000004511', '00000000004515','00000000004519', '00000000004523','00000000004474');

  v_new_orgcode3 varchar2(14);
  v_new_orgcode2 varchar2(14);
  v_count        number(19);
  v_Work_Date    Varchar2(8) := To_Char(Sysdate, 'yyyymmdd');
  v_Proc_Type    Varchar(10) := '';
  v_Proc_Name    Varchar2(50) := 'proc2020_JOB_PAYDIFFERENCE';
  v_Step_No      Varchar2(10);
  v_Step_Desc    Varchar2(200);
  v_Begin_Time   Date;
  v_End_Time     Date;
  v_Sql_Code     Varchar2(10);
  v_Sql_Errm     Varchar2(200);
  v_Execute_Flag Varchar2(10);
  v_cur          c_cur%rowtype;
begin
  v_count      := 0;
  v_Begin_Time := sysdate;
  open c_cur;
  loop
    fetch c_cur
      into v_cur;
    exit when c_cur%notfound;

    v_Step_No   := 'step_1';
    v_Step_Desc := 'rowid:' || v_cur.rowid;

    update JOB_PAYDIFFERENCE
       set branchid = '00000000000019'
     where rowid = v_cur.rowid;

    -- dbms_output.put_line(sqlcode);
    v_count := v_count + 1;
    if mod(v_count, 10000) = 0 then
      commit;
    end if;
  end loop;
  commit;
close c_cur;
  v_End_Time := sysdate;
  v_Step_Desc :='结束';
  v_Execute_Flag := 'y';
  p_Pub_Insert_Log(v_Work_Date,
                   v_Proc_Type,
                   v_Proc_Name,
                   v_Step_No,
                   v_Step_Desc,
                   v_count,
                   v_Sql_Code,
                   v_Sql_Errm,
                   v_Execute_Flag,
                   v_Begin_Time,
                   v_End_Time);

Exception
  When Others Then
    If c_cur%Isopen Then
      Close c_cur;
    End If;
    Rollback;
    v_Sql_Code     := Sqlcode;
    v_Sql_Errm     := Trim(Sqlerrm);
    v_End_Time     := Sysdate;
    v_Execute_Flag := 'n';
    p_Pub_Insert_Log(v_Work_Date,
                     v_Proc_Type,
                     v_Proc_Name,
                     v_Step_No,
                     v_Step_Desc,
                     v_count,
                     v_Sql_Code,
                     v_Sql_Errm,
                     v_Execute_Flag,
                     v_Begin_Time,
                     v_End_Time);
    close c_cur;
end;

-- 3
procedure proc2020BENEFIT_INFO_COUNT is
 cursor c_cur is
    select a.policyno_id,
           a.class_code,
           a.del_code,
           a.type_no,
           a.del_num,
           b.org3_id_new,
           b.org2_id_new
      from TBL_BENEFIT_INFO_COUNT a, tbl_org_revolution b
     where a.comp_id = b.org3_id
       and a.branch_id = b.org2_id
       and a.branch_id in ('00000000004511', '00000000004515','00000000004519', '00000000004523','00000000004474');

  v_new_orgcode3 varchar2(14);
  v_new_orgcode2 varchar2(14);
  v_count        number(19);
  v_Work_Date    Varchar2(8) := To_Char(Sysdate, 'yyyymmdd');
  v_Proc_Type    Varchar(10) := '';
  v_Proc_Name    Varchar2(50) := 'proc_TBL_BENEFIT_INFO_COUNT';
  v_Step_No      Varchar2(10);
  v_Step_Desc    Varchar2(200);
  v_Begin_Time   Date;
  v_End_Time     Date;
  v_Sql_Code     Varchar2(10);
  v_Sql_Errm     Varchar2(200);
  v_Execute_Flag Varchar2(10);
  v_cur          c_cur%rowtype;
begin
  v_count      := 0;
  v_Begin_Time := sysdate;
  open c_cur;
  loop
    fetch c_cur
      into v_cur;
    exit when c_cur%notfound;

    v_Step_No   := 'step_1';
    v_Step_Desc := '保单号:' || v_cur.policyno_id || '险种:' || v_cur.class_code ||
                   '责任:' || v_cur.del_code || '子码:' || v_cur.type_no ||
                   '期次:' || v_cur.del_num;

    update TBL_BENEFIT_INFO_COUNT
       set comp_id = v_cur.org3_id_new, branch_id = v_cur.org2_id_new
     where policyno_id = v_cur.policyno_id
       and class_code = v_cur.class_code
       and del_code = v_cur.del_code
       and type_no = v_cur.type_no
       and del_num = v_cur.del_num;

    -- dbms_output.put_line(sqlcode);
    v_count := v_count + 1;
    if mod(v_count, 10000) = 0 then
      commit;
    end if;
  end loop;
  commit;
close c_cur;
  v_End_Time := sysdate;
  v_Step_Desc :='结束';
  v_Execute_Flag := 'y';
  p_Pub_Insert_Log(v_Work_Date,
                   v_Proc_Type,
                   v_Proc_Name,
                   v_Step_No,
                   v_Step_Desc,
                   v_count,
                   v_Sql_Code,
                   v_Sql_Errm,
                   v_Execute_Flag,
                   v_Begin_Time,
                   v_End_Time);

Exception
  When Others Then
    If c_cur%Isopen Then
      Close c_cur;
    End If;
    Rollback;
    v_Sql_Code     := Sqlcode;
    v_Sql_Errm     := Trim(Sqlerrm);
    v_End_Time     := Sysdate;
    v_Execute_Flag := 'n';
    p_Pub_Insert_Log(v_Work_Date,
                     v_Proc_Type,
                     v_Proc_Name,
                     v_Step_No,
                     v_Step_Desc,
                     v_count,
                     v_Sql_Code,
                     v_Sql_Errm,
                     v_Execute_Flag,
                     v_Begin_Time,
                     v_End_Time);
    close c_cur;
end;

-- 4
procedure proc2020FAIL_BENEFIT_INFO is
 cursor c_cur is
    select a.policyno_id,
           a.class_code,
           a.del_code,
           a.type_no,
           a.del_num,
           b.org3_id_new,
           b.org2_id_new
      from TBL_FAIL_BENEFIT_INFO a, tbl_org_revolution b
     where a.comp_id = b.org3_id
       and a.branch_id = b.org2_id
       and a.branch_id in ('00000000004511', '00000000004515','00000000004519', '00000000004523','00000000004474');

  v_new_orgcode3 varchar2(14);
  v_new_orgcode2 varchar2(14);
  v_count        number(19);
  v_Work_Date    Varchar2(8) := To_Char(Sysdate, 'yyyymmdd');
  v_Proc_Type    Varchar(10) := '';
  v_Proc_Name    Varchar2(50) := 'proc2020FAIL_BENEFIT_INFO';
  v_Step_No      Varchar2(10);
  v_Step_Desc    Varchar2(200);
  v_Begin_Time   Date;
  v_End_Time     Date;
  v_Sql_Code     Varchar2(10);
  v_Sql_Errm     Varchar2(200);
  v_Execute_Flag Varchar2(10);
  v_cur          c_cur%rowtype;
begin
  v_count      := 0;
  v_Begin_Time := sysdate;
  open c_cur;
  loop
    fetch c_cur
      into v_cur;
    exit when c_cur%notfound;

    v_Step_No   := 'step_1';
    v_Step_Desc := '保单号:' || v_cur.policyno_id || '险种:' || v_cur.class_code ||
                   '责任:' || v_cur.del_code || '子码:' || v_cur.type_no ||
                   '期次:' || v_cur.del_num;

    update TBL_FAIL_BENEFIT_INFO
       set comp_id = v_cur.org3_id_new, branch_id = v_cur.org2_id_new
     where policyno_id = v_cur.policyno_id
       and class_code = v_cur.class_code
       and del_code = v_cur.del_code
       and type_no = v_cur.type_no
       and del_num = v_cur.del_num;

    -- dbms_output.put_line(sqlcode);
    v_count := v_count + 1;
    if mod(v_count, 10000) = 0 then
      commit;
    end if;
  end loop;
  commit;
close c_cur;
  v_End_Time := sysdate;
  v_Step_Desc :='结束';
  v_Execute_Flag := 'y';
  p_Pub_Insert_Log(v_Work_Date,
                   v_Proc_Type,
                   v_Proc_Name,
                   v_Step_No,
                   v_Step_Desc,
                   v_count,
                   v_Sql_Code,
                   v_Sql_Errm,
                   v_Execute_Flag,
                   v_Begin_Time,
                   v_End_Time);

Exception
  When Others Then
    If c_cur%Isopen Then
      Close c_cur;
    End If;
    Rollback;
    v_Sql_Code     := Sqlcode;
    v_Sql_Errm     := Trim(Sqlerrm);
    v_End_Time     := Sysdate;
    v_Execute_Flag := 'n';
    p_Pub_Insert_Log(v_Work_Date,
                     v_Proc_Type,
                     v_Proc_Name,
                     v_Step_No,
                     v_Step_Desc,
                     v_count,
                     v_Sql_Code,
                     v_Sql_Errm,
                     v_Execute_Flag,
                     v_Begin_Time,
                     v_End_Time);
    close c_cur;
end;

-- 5
procedure proc2020_D01_RISKCON (i_Branchid In D01_RISKCON.BRANCH%Type)is
 cursor c_cur is
    select a.rowid
      from D01_RISKCON a
     where  a.branch = i_Branchid;


  v_count        number(19);
  v_Work_Date    Varchar2(8) := To_Char(Sysdate, 'yyyymmdd');
  v_Proc_Type    Varchar(10) := '';
  v_Proc_Name    Varchar2(50) := 'proc2020_D01_RISKCON';
  v_Step_No      Varchar2(10);
  v_Step_Desc    Varchar2(200);
  v_Begin_Time   Date;
  v_End_Time     Date;
  v_Sql_Code     Varchar2(10);
  v_Sql_Errm     Varchar2(200);
  v_Execute_Flag Varchar2(10);
  v_cur          c_cur%rowtype;
begin
  v_count      := 0;
  v_Begin_Time := sysdate;
  open c_cur;
  loop
    fetch c_cur
      into v_cur;
    exit when c_cur%notfound;

    v_Step_No   := 'step_1';
    v_Step_Desc := 'rowid:' || v_cur.rowid;

    update D01_RISKCON
       set branch = '00000000000019'
     where rowid = v_cur.rowid;

    -- dbms_output.put_line(sqlcode);
    v_count := v_count + 1;
    if mod(v_count, 10000) = 0 then
      commit;
    end if;
  end loop;
  commit;
  close c_cur;
  v_End_Time := sysdate;
  v_Step_Desc :='结束';
  v_Execute_Flag := 'y';
  p_Pub_Insert_Log(v_Work_Date,
                   v_Proc_Type,
                   v_Proc_Name,
                   v_Step_No,
                   v_Step_Desc,
                   v_count,
                   v_Sql_Code,
                   v_Sql_Errm,
                   v_Execute_Flag,
                   v_Begin_Time,
                   v_End_Time);

Exception
  When Others Then
    If c_cur%Isopen Then
      Close c_cur;
    End If;
    Rollback;
    v_Sql_Code     := Sqlcode;
    v_Sql_Errm     := Trim(Sqlerrm);
    v_End_Time     := Sysdate;
    v_Execute_Flag := 'n';
    p_Pub_Insert_Log(v_Work_Date,
                     v_Proc_Type,
                     v_Proc_Name,
                     v_Step_No,
                     v_Step_Desc,
                     v_count,
                     v_Sql_Code,
                     v_Sql_Errm,
                     v_Execute_Flag,
                     v_Begin_Time,
                     v_End_Time);
    close c_cur;
end;

-- 6
procedure proc2020_JOB_PAYDETAILED is
 cursor c_cur is
    select a.rowid
      from JOB_PAYDETAILED a
     where  a.branchid in ('00000000004511', '00000000004515','00000000004519', '00000000004523','00000000004474');

  v_new_orgcode3 varchar2(14);
  v_new_orgcode2 varchar2(14);
  v_count        number(19);
  v_Work_Date    Varchar2(8) := To_Char(Sysdate, 'yyyymmdd');
  v_Proc_Type    Varchar(10) := '';
  v_Proc_Name    Varchar2(50) := 'proc2020_JOB_PAYDETAILED';
  v_Step_No      Varchar2(10);
  v_Step_Desc    Varchar2(200);
  v_Begin_Time   Date;
  v_End_Time     Date;
  v_Sql_Code     Varchar2(10);
  v_Sql_Errm     Varchar2(200);
  v_Execute_Flag Varchar2(10);
  v_cur          c_cur%rowtype;
begin
  v_count      := 0;
  v_Begin_Time := sysdate;
  open c_cur;
  loop
    fetch c_cur
      into v_cur;
    exit when c_cur%notfound;

    v_Step_No   := 'step_1';
    v_Step_Desc := 'rowid:' || v_cur.rowid;

    update JOB_PAYDETAILED
       set branchid = '00000000000019'
     where rowid = v_cur.rowid;

    -- dbms_output.put_line(sqlcode);
    v_count := v_count + 1;
    if mod(v_count, 10000) = 0 then
      commit;
    end if;
  end loop;
  commit;
 close c_cur;

  v_End_Time := sysdate;
  v_Step_Desc :='结束';
  v_Execute_Flag := 'y';
  p_Pub_Insert_Log(v_Work_Date,
                   v_Proc_Type,
                   v_Proc_Name,
                   v_Step_No,
                   v_Step_Desc,
                   v_count,
                   v_Sql_Code,
                   v_Sql_Errm,
                   v_Execute_Flag,
                   v_Begin_Time,
                   v_End_Time);

Exception
  When Others Then
    If c_cur%Isopen Then
      Close c_cur;
    End If;
    Rollback;
    v_Sql_Code     := Sqlcode;
    v_Sql_Errm     := Trim(Sqlerrm);
    v_End_Time     := Sysdate;
    v_Execute_Flag := 'n';
    p_Pub_Insert_Log(v_Work_Date,
                     v_Proc_Type,
                     v_Proc_Name,
                     v_Step_No,
                     v_Step_Desc,
                     v_count,
                     v_Sql_Code,
                     v_Sql_Errm,
                     v_Execute_Flag,
                     v_Begin_Time,
                     v_End_Time);
    close c_cur;
end;

-- 7
procedure proc2020_JOB_PAYMENTCHECK is

  TYPE BenefitTab IS TABLE OF JOB_PAYMENTCHECK%ROWTYPE;
  benefit_rec BenefitTab;

  CURSOR c_cur IS
    SELECT *
      FROM JOB_PAYMENTCHECK a
     WHERE a.branchid IN ('00000000004511','00000000004515','00000000004519','00000000004523','00000000004474');

  v_count        number(19);
  v_Work_Date    Varchar2(8) := To_Char(Sysdate, 'yyyymmdd');
  v_Proc_Type    Varchar(10) := '';
  v_Proc_Name    Varchar2(50) := 'proc2020_JOB_PAYMENTCHECK';
  v_Step_No      Varchar2(10);
  v_Step_Desc    Varchar2(200);
  v_Begin_Time   Date;
  v_End_Time     Date;
  v_Sql_Code     Varchar2(10);
  v_Sql_Errm     Varchar2(200);
  v_Execute_Flag Varchar2(10);

BEGIN
  v_count      := 0;
  v_Begin_Time := sysdate;

  OPEN c_cur;

  LOOP
    FETCH c_cur BULK COLLECT
      INTO benefit_rec LIMIT 10000;

    EXIT WHEN benefit_rec.count = 0;


    FOR i IN benefit_rec.first .. benefit_rec.last LOOP

       v_Step_No   := 'step_1';
       v_Step_Desc := '保单号:' ||  benefit_rec(i).policyno || '险种:' ||  benefit_rec(i).classcode ||
                   '责任:' ||  benefit_rec(i).delcode || '子码:' ||  benefit_rec(i).typeno ||
                   '期次:' ||  benefit_rec(i).delnum;

     UPDATE JOB_PAYMENTCHECK a
         SET ( compid,branchid) = (SELECT b.org3_id_new,  b.org2_id_new
                                       FROM tbl_org_revolution b
                                      WHERE  a.compid = b.org3_id
                                        AND a.branchid = b.org2_id)
       WHERE a.policyno = benefit_rec(i)
      .policyno
         AND a.classcode = benefit_rec(i)
      .classcode
         AND a.delcode = benefit_rec(i)
      .delcode
         AND a.typeno = benefit_rec(i)
      .typeno
         AND a.delnum = benefit_rec(i)
      .delnum
         AND EXISTS (SELECT 1
                FROM tbl_org_revolution b
               WHERE  a.compid = b.org3_id
                 AND a.branchid = b.org2_id);

       v_count := v_count + 1;
      COMMIT;

    END LOOP;

  END LOOP;
  CLOSE c_cur;

  v_End_Time := sysdate;
  v_Step_Desc :='结束';
  v_Execute_Flag := 'y';
  p_Pub_Insert_Log(v_Work_Date,
                   v_Proc_Type,
                   v_Proc_Name,
                   v_Step_No,
                   v_Step_Desc,
                   v_count,
                   v_Sql_Code,
                   v_Sql_Errm,
                   v_Execute_Flag,
                   v_Begin_Time,
                   v_End_Time);
  Exception
  When Others Then
    If c_cur%Isopen Then
      Close c_cur;
    End If;
    Rollback;
    v_Sql_Code     := Sqlcode;
    v_Sql_Errm     := Trim(Sqlerrm);
    v_End_Time     := Sysdate;
    v_Execute_Flag := 'n';
    p_Pub_Insert_Log(v_Work_Date,
                     v_Proc_Type,
                     v_Proc_Name,
                     v_Step_No,
                     v_Step_Desc,
                     v_count,
                     v_Sql_Code,
                     v_Sql_Errm,
                     v_Execute_Flag,
                     v_Begin_Time,
                     v_End_Time);
    close c_cur;
end;

-- 8
procedure proc2020_JOB_POLICYNOPAY is

  TYPE BenefitTab IS TABLE OF JOB_POLICYNOPAY%ROWTYPE;
  benefit_rec BenefitTab;

  CURSOR c_cur IS
    SELECT *
      FROM JOB_POLICYNOPAY a
     WHERE a.branchid IN ('00000000004511','00000000004515','00000000004519','00000000004523','00000000004474');

  v_count        number(19);
  v_Work_Date    Varchar2(8) := To_Char(Sysdate, 'yyyymmdd');
  v_Proc_Type    Varchar(10) := '';
  v_Proc_Name    Varchar2(50) := 'proc2020_JOB_POLICYNOPAY';
  v_Step_No      Varchar2(10);
  v_Step_Desc    Varchar2(200);
  v_Begin_Time   Date;
  v_End_Time     Date;
  v_Sql_Code     Varchar2(10);
  v_Sql_Errm     Varchar2(200);
  v_Execute_Flag Varchar2(10);

BEGIN
  v_count      := 0;
  v_Begin_Time := sysdate;

  OPEN c_cur;

  LOOP
    FETCH c_cur BULK COLLECT
      INTO benefit_rec LIMIT 10000;

    EXIT WHEN benefit_rec.count = 0;


    FOR i IN benefit_rec.first .. benefit_rec.last LOOP

       v_Step_No   := 'step_1';
       v_Step_Desc := '保单号:' ||  benefit_rec(i).policyno || '险种:' ||  benefit_rec(i).classcode ||
                   '责任:' ||  benefit_rec(i).delcode || '子码:' ||  benefit_rec(i).typeno;

     UPDATE JOB_POLICYNOPAY a
         SET (branchid) = (SELECT  b.org2_id_new
                                       FROM tbl_org_revolution b
                                      WHERE   a.branchid = b.org2_id and ROWNUM=1)
       WHERE a.policyno = benefit_rec(i)
      .policyno
         AND a.classcode = benefit_rec(i)
      .classcode
         AND a.delcode = benefit_rec(i)
      .delcode
         AND a.typeno = benefit_rec(i)
      .typeno
         AND EXISTS (SELECT 1
                FROM tbl_org_revolution b
               WHERE  a.branchid = b.org2_id);

       v_count := v_count + 1;
      COMMIT;

    END LOOP;

  END LOOP;
  CLOSE c_cur;

  v_End_Time := sysdate;
  v_Step_Desc :='结束';
  v_Execute_Flag := 'y';
  p_Pub_Insert_Log(v_Work_Date,
                   v_Proc_Type,
                   v_Proc_Name,
                   v_Step_No,
                   v_Step_Desc,
                   v_count,
                   v_Sql_Code,
                   v_Sql_Errm,
                   v_Execute_Flag,
                   v_Begin_Time,
                   v_End_Time);
  Exception
  When Others Then
    If c_cur%Isopen Then
      Close c_cur;
    End If;
    Rollback;
    v_Sql_Code     := Sqlcode;
    v_Sql_Errm     := Trim(Sqlerrm);
    v_End_Time     := Sysdate;
    v_Execute_Flag := 'n';
    p_Pub_Insert_Log(v_Work_Date,
                     v_Proc_Type,
                     v_Proc_Name,
                     v_Step_No,
                     v_Step_Desc,
                     v_count,
                     v_Sql_Code,
                     v_Sql_Errm,
                     v_Execute_Flag,
                     v_Begin_Time,
                     v_End_Time);
    close c_cur;
end;


-- 9
procedure proc2020_TBL_BEN_NOTI_INFO is

  TYPE BenefitTab IS TABLE OF TBL_BEN_NOTI_INFO%ROWTYPE;
  benefit_rec BenefitTab;

  CURSOR c_cur IS
    SELECT *
      FROM TBL_BEN_NOTI_INFO a
     WHERE a.branch_id IN ('00000000004511','00000000004515','00000000004519','00000000004523','00000000004474');

  v_count        number(19);
  v_Work_Date    Varchar2(8) := To_Char(Sysdate, 'yyyymmdd');
  v_Proc_Type    Varchar(10) := '';
  v_Proc_Name    Varchar2(50) := 'proc2020_TBL_BEN_NOTI_INFO';
  v_Step_No      Varchar2(10);
  v_Step_Desc    Varchar2(200);
  v_Begin_Time   Date;
  v_End_Time     Date;
  v_Sql_Code     Varchar2(10);
  v_Sql_Errm     Varchar2(200);
  v_Execute_Flag Varchar2(10);

BEGIN
  v_count      := 0;
  v_Begin_Time := sysdate;

  OPEN c_cur;

  LOOP
    FETCH c_cur BULK COLLECT
      INTO benefit_rec LIMIT 10000;

    EXIT WHEN benefit_rec.count = 0;


    FOR i IN benefit_rec.first .. benefit_rec.last LOOP

       v_Step_No   := 'step_1';
       v_Step_Desc := '保单号:' ||  benefit_rec(i).policyno_id || '险种:' ||  benefit_rec(i).class_code ||
                   '责任:' ||  benefit_rec(i).del_code || '子码:' ||  benefit_rec(i).type_no ||
                   '期次:' ||  benefit_rec(i).del_num || '通知方式:' ||  benefit_rec(i).NOTI_TYPE;

     UPDATE TBL_BEN_NOTI_INFO a
         SET (comp_id, branch_id) = (SELECT b.org3_id_new, b.org2_id_new
                                       FROM tbl_org_revolution b
                                      WHERE a.comp_id = b.org3_id
                                        AND a.branch_id = b.org2_id)
       WHERE a.policyno_id = benefit_rec(i)
      .policyno_id
         AND a.class_code = benefit_rec(i)
      .class_code
         AND a.del_code = benefit_rec(i)
      .del_code
         AND a.type_no = benefit_rec(i)
      .type_no
         AND a.del_num = benefit_rec(i).del_num
         AND a.NOTI_TYPE = benefit_rec(i).NOTI_TYPE
         AND EXISTS (SELECT 1
                FROM tbl_org_revolution b
               WHERE a.comp_id = b.org3_id
                 AND a.branch_id = b.org2_id);

       v_count := v_count + 1;
      COMMIT;

    END LOOP;

  END LOOP;
  CLOSE c_cur;

  v_End_Time := sysdate;
  v_Step_Desc :='结束';
  v_Execute_Flag := 'y';
  p_Pub_Insert_Log(v_Work_Date,
                   v_Proc_Type,
                   v_Proc_Name,
                   v_Step_No,
                   v_Step_Desc,
                   v_count,
                   v_Sql_Code,
                   v_Sql_Errm,
                   v_Execute_Flag,
                   v_Begin_Time,
                   v_End_Time);
  Exception
  When Others Then
    If c_cur%Isopen Then
      Close c_cur;
    End If;
    Rollback;
    v_Sql_Code     := Sqlcode;
    v_Sql_Errm     := Trim(Sqlerrm);
    v_End_Time     := Sysdate;
    v_Execute_Flag := 'n';
    p_Pub_Insert_Log(v_Work_Date,
                     v_Proc_Type,
                     v_Proc_Name,
                     v_Step_No,
                     v_Step_Desc,
                     v_count,
                     v_Sql_Code,
                     v_Sql_Errm,
                     v_Execute_Flag,
                     v_Begin_Time,
                     v_End_Time);
    close c_cur;
end;


-- 10
procedure proc2020_TBL_CUST_BEN_INFO is

  TYPE BenefitTab IS TABLE OF TBL_CUST_BEN_INFO%ROWTYPE;
  benefit_rec BenefitTab;

  CURSOR c_cur IS
    SELECT *
      FROM TBL_CUST_BEN_INFO a
     WHERE a.branch_id IN ('00000000004511','00000000004515','00000000004519','00000000004523','00000000004474');

  v_count        number(19);
  v_Work_Date    Varchar2(8) := To_Char(Sysdate, 'yyyymmdd');
  v_Proc_Type    Varchar(10) := '';
  v_Proc_Name    Varchar2(50) := 'proc2020_TBL_CUST_BEN_INFO';
  v_Step_No      Varchar2(10);
  v_Step_Desc    Varchar2(200);
  v_Begin_Time   Date;
  v_End_Time     Date;
  v_Sql_Code     Varchar2(10);
  v_Sql_Errm     Varchar2(200);
  v_Execute_Flag Varchar2(10);

BEGIN
  v_count      := 0;
  v_Begin_Time := sysdate;

  OPEN c_cur;

  LOOP
    FETCH c_cur BULK COLLECT
      INTO benefit_rec LIMIT 10000;

    EXIT WHEN benefit_rec.count = 0;


    FOR i IN benefit_rec.first .. benefit_rec.last LOOP

       v_Step_No   := 'step_1';
       v_Step_Desc := '保单号:' ||  benefit_rec(i).policyno_id || '险种:' ||  benefit_rec(i).class_code ||
                   '责任:' ||  benefit_rec(i).del_code || '子码:' ||  benefit_rec(i).type_no ;

     UPDATE TBL_CUST_BEN_INFO a
         SET ( branch_id) = (SELECT  b.org2_id_new
                                       FROM tbl_org_revolution b
                                      WHERE  a.branch_id = b.org2_id and ROWNUM=1)
       WHERE a.policyno_id = benefit_rec(i)
      .policyno_id
         AND a.class_code = benefit_rec(i)
      .class_code
         AND a.del_code = benefit_rec(i)
      .del_code
         AND a.type_no = benefit_rec(i)
      .type_no
         AND EXISTS (SELECT 1
                FROM tbl_org_revolution b
               WHERE  a.branch_id = b.org2_id);

       v_count := v_count + 1;
      COMMIT;

    END LOOP;

  END LOOP;
  CLOSE c_cur;

  v_End_Time := sysdate;
  v_Step_Desc :='结束';
  v_Execute_Flag := 'y';
  p_Pub_Insert_Log(v_Work_Date,
                   v_Proc_Type,
                   v_Proc_Name,
                   v_Step_No,
                   v_Step_Desc,
                   v_count,
                   v_Sql_Code,
                   v_Sql_Errm,
                   v_Execute_Flag,
                   v_Begin_Time,
                   v_End_Time);
  Exception
  When Others Then
    If c_cur%Isopen Then
      Close c_cur;
    End If;
    Rollback;
    v_Sql_Code     := Sqlcode;
    v_Sql_Errm     := Trim(Sqlerrm);
    v_End_Time     := Sysdate;
    v_Execute_Flag := 'n';
    p_Pub_Insert_Log(v_Work_Date,
                     v_Proc_Type,
                     v_Proc_Name,
                     v_Step_No,
                     v_Step_Desc,
                     v_count,
                     v_Sql_Code,
                     v_Sql_Errm,
                     v_Execute_Flag,
                     v_Begin_Time,
                     v_End_Time);
    close c_cur;
end;


-- zhangguoting
-- 11
procedure proc2020_tbl_benefit_info is

  TYPE BenefitTab IS TABLE OF tbl_benefit_info%ROWTYPE;
  benefit_rec BenefitTab;

  CURSOR c_cur IS
    SELECT *
      FROM tbl_benefit_info a
     WHERE a.branch_id IN ('00000000004511','00000000004515','00000000004519','00000000004523','00000000004474');

  v_count        number(19);
  v_Work_Date    Varchar2(8) := To_Char(Sysdate, 'yyyymmdd');
  v_Proc_Type    Varchar(10) := '';
  v_Proc_Name    Varchar2(50) := 'proc2020_tbl_benefit_info';
  v_Step_No      Varchar2(10);
  v_Step_Desc    Varchar2(200);
  v_Begin_Time   Date;
  v_End_Time     Date;
  v_Sql_Code     Varchar2(10);
  v_Sql_Errm     Varchar2(200);
  v_Execute_Flag Varchar2(10);

BEGIN
  v_count      := 0;
  v_Begin_Time := sysdate;

  OPEN c_cur;

  LOOP
    FETCH c_cur BULK COLLECT
      INTO benefit_rec LIMIT 10000;

    EXIT WHEN benefit_rec.count = 0;


    FOR i IN benefit_rec.first .. benefit_rec.last LOOP

       v_Step_No   := 'step_1';
       v_Step_Desc := '保单号:' ||  benefit_rec(i).policyno_id || '险种:' ||  benefit_rec(i).class_code ||
                   '责任:' ||  benefit_rec(i).del_code || '子码:' ||  benefit_rec(i).type_no ||
                   '期次:' ||  benefit_rec(i).del_num;

     UPDATE tbl_benefit_info a
         SET (comp_id, branch_id) = (SELECT b.org3_id_new, b.org2_id_new
                                       FROM tbl_org_revolution b
                                      WHERE a.comp_id = b.org3_id
                                        AND a.branch_id = b.org2_id)
       WHERE a.policyno_id = benefit_rec(i)
      .policyno_id
         AND a.class_code = benefit_rec(i)
      .class_code
         AND a.del_code = benefit_rec(i)
      .del_code
         AND a.type_no = benefit_rec(i)
      .type_no
         AND a.del_num = benefit_rec(i).del_num
         AND EXISTS (SELECT 1
                FROM tbl_org_revolution b
               WHERE a.comp_id = b.org3_id
                 AND a.branch_id = b.org2_id);

       v_count := v_count + 1;
      COMMIT;

    END LOOP;

  END LOOP;
  CLOSE c_cur;

  v_End_Time := sysdate;
  v_Step_Desc :='结束';
  v_Execute_Flag := 'y';
  p_Pub_Insert_Log(v_Work_Date,
                   v_Proc_Type,
                   v_Proc_Name,
                   v_Step_No,
                   v_Step_Desc,
                   v_count,
                   v_Sql_Code,
                   v_Sql_Errm,
                   v_Execute_Flag,
                   v_Begin_Time,
                   v_End_Time);
  Exception
  When Others Then
    If c_cur%Isopen Then
      Close c_cur;
    End If;
    Rollback;
    v_Sql_Code     := Sqlcode;
    v_Sql_Errm     := Trim(Sqlerrm);
    v_End_Time     := Sysdate;
    v_Execute_Flag := 'n';
    p_Pub_Insert_Log(v_Work_Date,
                     v_Proc_Type,
                     v_Proc_Name,
                     v_Step_No,
                     v_Step_Desc,
                     v_count,
                     v_Sql_Code,
                     v_Sql_Errm,
                     v_Execute_Flag,
                     v_Begin_Time,
                     v_End_Time);
    close c_cur;
end;

-- haoxu
-- 12
procedure proc2020_BENEFIT_DELETE is
 cursor c_cur is
    select a.policyno_id,
           a.class_code,
           a.del_code,
           a.type_no,
           a.del_num,
           b.org3_id_new,
           b.org2_id_new
      from TBL_PRE_BENEFIT_DELETE a, tbl_org_revolution b
     where a.comp_id = b.org3_id
       and a.branch_id = b.org2_id
       and a.branch_id in ('00000000004511', '00000000004515','00000000004519', '00000000004523','00000000004474');

  v_new_orgcode3 varchar2(14);
  v_new_orgcode2 varchar2(14);
  v_count        number(19);
  v_Work_Date    Varchar2(8) := To_Char(Sysdate, 'yyyymmdd');
  v_Proc_Type    Varchar(10) := '';
  v_Proc_Name    Varchar2(50) := 'proc2020_TBL_PRE_BENEFIT_DELETE';
  v_Step_No      Varchar2(10);
  v_Step_Desc    Varchar2(200);
  v_Begin_Time   Date;
  v_End_Time     Date;
  v_Sql_Code     Varchar2(10);
  v_Sql_Errm     Varchar2(200);
  v_Execute_Flag Varchar2(10);
  v_cur          c_cur%rowtype;
begin
  v_count      := 0;
  v_Begin_Time := sysdate;
  open c_cur;
  loop
    fetch c_cur
      into v_cur;
    exit when c_cur%notfound;

    v_Step_No   := 'step_1';
    v_Step_Desc := '保单号:' || v_cur.policyno_id || '险种:' || v_cur.class_code ||
                   '责任:' || v_cur.del_code || '子码:' || v_cur.type_no ||
                   '期次:' || v_cur.del_num;

    update TBL_PRE_BENEFIT_DELETE
       set comp_id = v_cur.org3_id_new, branch_id = v_cur.org2_id_new
     where policyno_id = v_cur.policyno_id
       and class_code = v_cur.class_code
       and del_code = v_cur.del_code
       and type_no = v_cur.type_no
       and del_num = v_cur.del_num;

    -- dbms_output.put_line(sqlcode);
    v_count := v_count + 1;
    if mod(v_count, 10000) = 0 then
      commit;
    end if;
  end loop;
  commit;
close c_cur;
  v_End_Time := sysdate;
  v_Step_Desc :='结束';
  v_Execute_Flag := 'y';
  p_Pub_Insert_Log(v_Work_Date,
                   v_Proc_Type,
                   v_Proc_Name,
                   v_Step_No,
                   v_Step_Desc,
                   v_count,
                   v_Sql_Code,
                   v_Sql_Errm,
                   v_Execute_Flag,
                   v_Begin_Time,
                   v_End_Time);

Exception
  When Others Then
    If c_cur%Isopen Then
      Close c_cur;
    End If;
    Rollback;
    v_Sql_Code     := Sqlcode;
    v_Sql_Errm     := Trim(Sqlerrm);
    v_End_Time     := Sysdate;
    v_Execute_Flag := 'n';
    p_Pub_Insert_Log(v_Work_Date,
                     v_Proc_Type,
                     v_Proc_Name,
                     v_Step_No,
                     v_Step_Desc,
                     v_count,
                     v_Sql_Code,
                     v_Sql_Errm,
                     v_Execute_Flag,
                     v_Begin_Time,
                     v_End_Time);
    close c_cur;
end;


-- 13
procedure proc2020_ACTUARY_STATISTICS is
 cursor c_cur is
    select a.policyno_id,
           a.class_code,
           a.del_code,
           a.type_no,
           a.del_num,
           b.org3_id_new,
           b.org2_id_new
      from TBL_RPT_ACTUARY_STATISTICS a, tbl_org_revolution b
     where a.comp_id = b.org3_id
       and a.branch_id = b.org2_id
       and a.branch_id in ('00000000004511', '00000000004515','00000000004519', '00000000004523','00000000004474');

  v_new_orgcode3 varchar2(14);
  v_new_orgcode2 varchar2(14);
  v_count        number(19);
  v_Work_Date    Varchar2(8) := To_Char(Sysdate, 'yyyymmdd');
  v_Proc_Type    Varchar(10) := '';
  v_Proc_Name    Varchar2(50) := 'proc2020_TBL_RPT_ACTUARY_STATISTICS';
  v_Step_No      Varchar2(10);
  v_Step_Desc    Varchar2(200);
  v_Begin_Time   Date;
  v_End_Time     Date;
  v_Sql_Code     Varchar2(10);
  v_Sql_Errm     Varchar2(200);
  v_Execute_Flag Varchar2(10);
  v_cur          c_cur%rowtype;
begin
  v_count      := 0;
  v_Begin_Time := sysdate;
  open c_cur;
  loop
    fetch c_cur
      into v_cur;
    exit when c_cur%notfound;

    v_Step_No   := 'step_1';
    v_Step_Desc := '保单号:' || v_cur.policyno_id || '险种:' || v_cur.class_code ||
                   '责任:' || v_cur.del_code || '子码:' || v_cur.type_no ||
                   '期次:' || v_cur.del_num;

    update TBL_RPT_ACTUARY_STATISTICS
       set comp_id = v_cur.org3_id_new, branch_id = v_cur.org2_id_new
     where policyno_id = v_cur.policyno_id
       and class_code = v_cur.class_code
       and del_code = v_cur.del_code
       and type_no = v_cur.type_no
       and del_num = v_cur.del_num;

    -- dbms_output.put_line(sqlcode);
    v_count := v_count + 1;
    if mod(v_count, 10000) = 0 then
      commit;
    end if;
  end loop;
  commit;
close c_cur;
  v_End_Time := sysdate;
  v_Step_Desc :='结束';
  v_Execute_Flag := 'y';
  p_Pub_Insert_Log(v_Work_Date,
                   v_Proc_Type,
                   v_Proc_Name,
                   v_Step_No,
                   v_Step_Desc,
                   v_count,
                   v_Sql_Code,
                   v_Sql_Errm,
                   v_Execute_Flag,
                   v_Begin_Time,
                   v_End_Time);

Exception
  When Others Then
    If c_cur%Isopen Then
      Close c_cur;
    End If;
    Rollback;
    v_Sql_Code     := Sqlcode;
    v_Sql_Errm     := Trim(Sqlerrm);
    v_End_Time     := Sysdate;
    v_Execute_Flag := 'n';
    p_Pub_Insert_Log(v_Work_Date,
                     v_Proc_Type,
                     v_Proc_Name,
                     v_Step_No,
                     v_Step_Desc,
                     v_count,
                     v_Sql_Code,
                     v_Sql_Errm,
                     v_Execute_Flag,
                     v_Begin_Time,
                     v_End_Time);
    close c_cur;
end;


-- 14
procedure proc2020_TBL_SURV_DATA_INFO is
 cursor c_cur is
    select a.APID_ID,
           a.TAKE_DATE,
           b.org3_id_new,
           b.org2_id_new
      from TBL_SURV_DATA_INFO a, tbl_org_revolution b
     where a.comp_id = b.org3_id
       and a.branch_id = b.org2_id
       and a.branch_id in ('00000000004511', '00000000004515','00000000004519', '00000000004523','00000000004474');

  v_new_orgcode3 varchar2(14);
  v_new_orgcode2 varchar2(14);
  v_count        number(19);
  v_Work_Date    Varchar2(8) := To_Char(Sysdate, 'yyyymmdd');
  v_Proc_Type    Varchar(10) := '';
  v_Proc_Name    Varchar2(50) := 'proc2020_TBL_SURV_DATA_INFO';
  v_Step_No      Varchar2(10);
  v_Step_Desc    Varchar2(200);
  v_Begin_Time   Date;
  v_End_Time     Date;
  v_Sql_Code     Varchar2(10);
  v_Sql_Errm     Varchar2(200);
  v_Execute_Flag Varchar2(10);
  v_cur          c_cur%rowtype;
begin
  v_count      := 0;
  v_Begin_Time := sysdate;
  open c_cur;
  loop
    fetch c_cur
      into v_cur;
    exit when c_cur%notfound;

    v_Step_No   := 'step_1';
    v_Step_Desc := '被保人身份证号:' || v_cur.APID_ID || '登记日期:' || v_cur.TAKE_DATE;

    update TBL_SURV_DATA_INFO
       set comp_id = v_cur.org3_id_new, branch_id = v_cur.org2_id_new
     where APID_ID = v_cur.APID_ID
       and TAKE_DATE = v_cur.TAKE_DATE;

    -- dbms_output.put_line(sqlcode);
    v_count := v_count + 1;
    if mod(v_count, 10000) = 0 then
      commit;
    end if;
  end loop;
  commit;
close c_cur;
  v_End_Time := sysdate;
  v_Step_Desc :='结束';
  v_Execute_Flag := 'y';
  p_Pub_Insert_Log(v_Work_Date,
                   v_Proc_Type,
                   v_Proc_Name,
                   v_Step_No,
                   v_Step_Desc,
                   v_count,
                   v_Sql_Code,
                   v_Sql_Errm,
                   v_Execute_Flag,
                   v_Begin_Time,
                   v_End_Time);

Exception
  When Others Then
    If c_cur%Isopen Then
      Close c_cur;
    End If;
    Rollback;
    v_Sql_Code     := Sqlcode;
    v_Sql_Errm     := Trim(Sqlerrm);
    v_End_Time     := Sysdate;
    v_Execute_Flag := 'n';
    p_Pub_Insert_Log(v_Work_Date,
                     v_Proc_Type,
                     v_Proc_Name,
                     v_Step_No,
                     v_Step_Desc,
                     v_count,
                     v_Sql_Code,
                     v_Sql_Errm,
                     v_Execute_Flag,
                     v_Begin_Time,
                     v_End_Time);
    close c_cur;
end;


-- 15
procedure proc2020_SURV_DATA_INFO_ONE is
 cursor c_cur is
    select a.APID_ID,
           a.TAKE_DATE,
           b.org3_id_new,
           b.org2_id_new
      from TBL_SURV_DATA_INFO_ONE a, tbl_org_revolution b
     where a.comp_id = b.org3_id
       and a.branch_id = b.org2_id
       and a.branch_id in ('00000000004511', '00000000004515','00000000004519', '00000000004523','00000000004474');

  v_new_orgcode3 varchar2(14);
  v_new_orgcode2 varchar2(14);
  v_count        number(19);
  v_Work_Date    Varchar2(8) := To_Char(Sysdate, 'yyyymmdd');
  v_Proc_Type    Varchar(10) := '';
  v_Proc_Name    Varchar2(50) := 'proc2020_TBL_SURV_DATA_INFO_ONE';
  v_Step_No      Varchar2(10);
  v_Step_Desc    Varchar2(200);
  v_Begin_Time   Date;
  v_End_Time     Date;
  v_Sql_Code     Varchar2(10);
  v_Sql_Errm     Varchar2(200);
  v_Execute_Flag Varchar2(10);
  v_cur          c_cur%rowtype;
begin
  v_count      := 0;
  v_Begin_Time := sysdate;
  open c_cur;
  loop
    fetch c_cur
      into v_cur;
    exit when c_cur%notfound;

    v_Step_No   := 'step_1';
    v_Step_Desc := '被保人身份证号:' || v_cur.APID_ID || '登记日期:' || v_cur.TAKE_DATE;

    update TBL_SURV_DATA_INFO_ONE
       set comp_id = v_cur.org3_id_new, branch_id = v_cur.org2_id_new
     where APID_ID = v_cur.APID_ID
       and TAKE_DATE = v_cur.TAKE_DATE;

    -- dbms_output.put_line(sqlcode);
    v_count := v_count + 1;
    if mod(v_count, 10000) = 0 then
      commit;
    end if;
  end loop;
  commit;
close c_cur;
  v_End_Time := sysdate;
  v_Step_Desc :='结束';
  v_Execute_Flag := 'y';
  p_Pub_Insert_Log(v_Work_Date,
                   v_Proc_Type,
                   v_Proc_Name,
                   v_Step_No,
                   v_Step_Desc,
                   v_count,
                   v_Sql_Code,
                   v_Sql_Errm,
                   v_Execute_Flag,
                   v_Begin_Time,
                   v_End_Time);

Exception
  When Others Then
    If c_cur%Isopen Then
      Close c_cur;
    End If;
    Rollback;
    v_Sql_Code     := Sqlcode;
    v_Sql_Errm     := Trim(Sqlerrm);
    v_End_Time     := Sysdate;
    v_Execute_Flag := 'n';
    p_Pub_Insert_Log(v_Work_Date,
                     v_Proc_Type,
                     v_Proc_Name,
                     v_Step_No,
                     v_Step_Desc,
                     v_count,
                     v_Sql_Code,
                     v_Sql_Errm,
                     v_Execute_Flag,
                     v_Begin_Time,
                     v_End_Time);
    close c_cur;
end;


-- 16
procedure proc2020_SURV_DATA_INFO_TMP is
 cursor c_cur is
    select a.APID_ID,
           a.TAKE_DATE,
           b.org3_id_new,
           b.org2_id_new
      from TBL_SURV_DATA_INFO_TMP a, tbl_org_revolution b
     where a.comp_id = b.org3_id
       and a.branch_id = b.org2_id
       and a.branch_id in ('00000000004511', '00000000004515','00000000004519', '00000000004523','00000000004474');

  v_new_orgcode3 varchar2(14);
  v_new_orgcode2 varchar2(14);
  v_count        number(19);
  v_Work_Date    Varchar2(8) := To_Char(Sysdate, 'yyyymmdd');
  v_Proc_Type    Varchar(10) := '';
  v_Proc_Name    Varchar2(50) := 'proc2020_TBL_SURV_DATA_INFO_TMP';
  v_Step_No      Varchar2(10);
  v_Step_Desc    Varchar2(200);
  v_Begin_Time   Date;
  v_End_Time     Date;
  v_Sql_Code     Varchar2(10);
  v_Sql_Errm     Varchar2(200);
  v_Execute_Flag Varchar2(10);
  v_cur          c_cur%rowtype;
begin
  v_count      := 0;
  v_Begin_Time := sysdate;
  open c_cur;
  loop
    fetch c_cur
      into v_cur;
    exit when c_cur%notfound;

    v_Step_No   := 'step_1';
    v_Step_Desc := '被保人身份证号:' || v_cur.APID_ID || '登记日期:' || v_cur.TAKE_DATE;

    update TBL_SURV_DATA_INFO_TMP
       set comp_id = v_cur.org3_id_new, branch_id = v_cur.org2_id_new
     where APID_ID = v_cur.APID_ID
       and TAKE_DATE = v_cur.TAKE_DATE;

    -- dbms_output.put_line(sqlcode);
    v_count := v_count + 1;
    if mod(v_count, 10000) = 0 then
      commit;
    end if;
  end loop;
  commit;
close c_cur;
  v_End_Time := sysdate;
  v_Step_Desc :='结束';
  v_Execute_Flag := 'y';
  p_Pub_Insert_Log(v_Work_Date,
                   v_Proc_Type,
                   v_Proc_Name,
                   v_Step_No,
                   v_Step_Desc,
                   v_count,
                   v_Sql_Code,
                   v_Sql_Errm,
                   v_Execute_Flag,
                   v_Begin_Time,
                   v_End_Time);

Exception
  When Others Then
    If c_cur%Isopen Then
      Close c_cur;
    End If;
    Rollback;
    v_Sql_Code     := Sqlcode;
    v_Sql_Errm     := Trim(Sqlerrm);
    v_End_Time     := Sysdate;
    v_Execute_Flag := 'n';
    p_Pub_Insert_Log(v_Work_Date,
                     v_Proc_Type,
                     v_Proc_Name,
                     v_Step_No,
                     v_Step_Desc,
                     v_count,
                     v_Sql_Code,
                     v_Sql_Errm,
                     v_Execute_Flag,
                     v_Begin_Time,
                     v_End_Time);
    close c_cur;
end;


-- 17
procedure proc2020_SURV_DATA_INFO_TMP2 is
 cursor c_cur is
    select a.APID_ID,
           a.TAKE_DATE,
           b.org3_id_new,
           b.org2_id_new
      from TBL_SURV_DATA_INFO_TMP2 a, tbl_org_revolution b
     where a.comp_id = b.org3_id
       and a.branch_id = b.org2_id
       and a.branch_id in ('00000000004511', '00000000004515','00000000004519', '00000000004523','00000000004474');

  v_new_orgcode3 varchar2(14);
  v_new_orgcode2 varchar2(14);
  v_count        number(19);
  v_Work_Date    Varchar2(8) := To_Char(Sysdate, 'yyyymmdd');
  v_Proc_Type    Varchar(10) := '';
  v_Proc_Name    Varchar2(50) := 'proc2020_TBL_SURV_DATA_INFO_TMP2';
  v_Step_No      Varchar2(10);
  v_Step_Desc    Varchar2(200);
  v_Begin_Time   Date;
  v_End_Time     Date;
  v_Sql_Code     Varchar2(10);
  v_Sql_Errm     Varchar2(200);
  v_Execute_Flag Varchar2(10);
  v_cur          c_cur%rowtype;
begin
  v_count      := 0;
  v_Begin_Time := sysdate;
  open c_cur;
  loop
    fetch c_cur
      into v_cur;
    exit when c_cur%notfound;

    v_Step_No   := 'step_1';
    v_Step_Desc := '被保人身份证号:' || v_cur.APID_ID || '登记日期:' || v_cur.TAKE_DATE;

    update TBL_SURV_DATA_INFO_TMP2
       set comp_id = v_cur.org3_id_new, branch_id = v_cur.org2_id_new
     where APID_ID = v_cur.APID_ID
       and TAKE_DATE = v_cur.TAKE_DATE;

    -- dbms_output.put_line(sqlcode);
    v_count := v_count + 1;
    if mod(v_count, 10000) = 0 then
      commit;
    end if;
  end loop;
  commit;
 close c_cur;
  v_End_Time := sysdate;
  v_Step_Desc :='结束';
  v_Execute_Flag := 'y';
  p_Pub_Insert_Log(v_Work_Date,
                   v_Proc_Type,
                   v_Proc_Name,
                   v_Step_No,
                   v_Step_Desc,
                   v_count,
                   v_Sql_Code,
                   v_Sql_Errm,
                   v_Execute_Flag,
                   v_Begin_Time,
                   v_End_Time);

Exception
  When Others Then
    If c_cur%Isopen Then
      Close c_cur;
    End If;
    Rollback;
    v_Sql_Code     := Sqlcode;
    v_Sql_Errm     := Trim(Sqlerrm);
    v_End_Time     := Sysdate;
    v_Execute_Flag := 'n';
    p_Pub_Insert_Log(v_Work_Date,
                     v_Proc_Type,
                     v_Proc_Name,
                     v_Step_No,
                     v_Step_Desc,
                     v_count,
                     v_Sql_Code,
                     v_Sql_Errm,
                     v_Execute_Flag,
                     v_Begin_Time,
                     v_End_Time);
    close c_cur;
end;


-- 18
procedure proc2020_TBL_SURV_TASK_INFO is
 cursor c_cur is
    select a.APP_DATE,
           a.PID_ID,
           b.org3_id_new,
           b.org2_id_new
      from TBL_SURV_TASK_INFO a, tbl_org_revolution b
     where a.comp_id = b.org3_id
       and a.branch_id = b.org2_id
       and a.branch_id in ('00000000004511', '00000000004515','00000000004519', '00000000004523','00000000004474');

  v_new_orgcode3 varchar2(14);
  v_new_orgcode2 varchar2(14);
  v_count        number(19);
  v_Work_Date    Varchar2(8) := To_Char(Sysdate, 'yyyymmdd');
  v_Proc_Type    Varchar(10) := '';
  v_Proc_Name    Varchar2(50) := 'proc2020_TBL_SURV_TASK_INFO';
  v_Step_No      Varchar2(10);
  v_Step_Desc    Varchar2(200);
  v_Begin_Time   Date;
  v_End_Time     Date;
  v_Sql_Code     Varchar2(10);
  v_Sql_Errm     Varchar2(200);
  v_Execute_Flag Varchar2(10);
  v_cur          c_cur%rowtype;
begin
  v_count      := 0;
  v_Begin_Time := sysdate;
  open c_cur;
  loop
    fetch c_cur
      into v_cur;
    exit when c_cur%notfound;

    v_Step_No   := 'step_1';
    v_Step_Desc := 'APP日期:' || v_cur.APP_DATE || 'PIDID:' || v_cur.PID_ID;

    update TBL_SURV_TASK_INFO
       set comp_id = v_cur.org3_id_new, branch_id = v_cur.org2_id_new
     where APP_DATE = v_cur.APP_DATE
       and PID_ID = v_cur.PID_ID;

    -- dbms_output.put_line(sqlcode);
    v_count := v_count + 1;
    if mod(v_count, 10000) = 0 then
      commit;
    end if;
  end loop;
  commit;
close c_cur;
  v_End_Time := sysdate;
  v_Step_Desc :='结束';
  v_Execute_Flag := 'y';
  p_Pub_Insert_Log(v_Work_Date,
                   v_Proc_Type,
                   v_Proc_Name,
                   v_Step_No,
                   v_Step_Desc,
                   v_count,
                   v_Sql_Code,
                   v_Sql_Errm,
                   v_Execute_Flag,
                   v_Begin_Time,
                   v_End_Time);

Exception
  When Others Then
    If c_cur%Isopen Then
      Close c_cur;
    End If;
    Rollback;
    v_Sql_Code     := Sqlcode;
    v_Sql_Errm     := Trim(Sqlerrm);
    v_End_Time     := Sysdate;
    v_Execute_Flag := 'n';
    p_Pub_Insert_Log(v_Work_Date,
                     v_Proc_Type,
                     v_Proc_Name,
                     v_Step_No,
                     v_Step_Desc,
                     v_count,
                     v_Sql_Code,
                     v_Sql_Errm,
                     v_Execute_Flag,
                     v_Begin_Time,
                     v_End_Time);
    close c_cur;
end;


-- 19
procedure proc2020_TBL_YUGU_PAY is
 cursor c_cur is
    select a.policyno_id,
           a.class_code,
           a.del_code,
           a.type_no,
           a.del_num,
           b.org3_id_new,
           b.org2_id_new
      from TBL_YUGU_PAY a, tbl_org_revolution b
     where a.comp_id = b.org3_id
       and a.branch_id = b.org2_id
       and a.branch_id in ('00000000004511', '00000000004515','00000000004519', '00000000004523','00000000004474');

  v_new_orgcode3 varchar2(14);
  v_new_orgcode2 varchar2(14);
  v_count        number(19);
  v_Work_Date    Varchar2(8) := To_Char(Sysdate, 'yyyymmdd');
  v_Proc_Type    Varchar(10) := '';
  v_Proc_Name    Varchar2(50) := 'proc2020_TBL_YUGU_PAY';
  v_Step_No      Varchar2(10);
  v_Step_Desc    Varchar2(200);
  v_Begin_Time   Date;
  v_End_Time     Date;
  v_Sql_Code     Varchar2(10);
  v_Sql_Errm     Varchar2(200);
  v_Execute_Flag Varchar2(10);
  v_cur          c_cur%rowtype;
begin
  v_count      := 0;
  v_Begin_Time := sysdate;
  open c_cur;
  loop
    fetch c_cur
      into v_cur;
    exit when c_cur%notfound;

    v_Step_No   := 'step_1';
    v_Step_Desc := '保单号:' || v_cur.policyno_id || '险种:' || v_cur.class_code ||
                   '责任:' || v_cur.del_code || '子码:' || v_cur.type_no ||
                   '期次:' || v_cur.del_num;

    update TBL_YUGU_PAY
       set comp_id = v_cur.org3_id_new, branch_id = v_cur.org2_id_new
     where policyno_id = v_cur.policyno_id
       and class_code = v_cur.class_code
       and del_code = v_cur.del_code
       and type_no = v_cur.type_no
       and del_num = v_cur.del_num;

    -- dbms_output.put_line(sqlcode);
    v_count := v_count + 1;
    if mod(v_count, 10000) = 0 then
      commit;
    end if;
  end loop;
  commit;
close c_cur;
  v_End_Time := sysdate;
  v_Step_Desc :='结束';
  v_Execute_Flag := 'y';
  p_Pub_Insert_Log(v_Work_Date,
                   v_Proc_Type,
                   v_Proc_Name,
                   v_Step_No,
                   v_Step_Desc,
                   v_count,
                   v_Sql_Code,
                   v_Sql_Errm,
                   v_Execute_Flag,
                   v_Begin_Time,
                   v_End_Time);

Exception
  When Others Then
    If c_cur%Isopen Then
      Close c_cur;
    End If;
    Rollback;
    v_Sql_Code     := Sqlcode;
    v_Sql_Errm     := Trim(Sqlerrm);
    v_End_Time     := Sysdate;
    v_Execute_Flag := 'n';
    p_Pub_Insert_Log(v_Work_Date,
                     v_Proc_Type,
                     v_Proc_Name,
                     v_Step_No,
                     v_Step_Desc,
                     v_count,
                     v_Sql_Code,
                     v_Sql_Errm,
                     v_Execute_Flag,
                     v_Begin_Time,
                     v_End_Time);
    close c_cur;
end;


-- megnnan
procedure proc2020_tbl_ids_account is

  TYPE IdsAccount IS TABLE OF tbl_ids_account%ROWTYPE;
  IdsAccount_rec IdsAccount;

  CURSOR c_cur IS
    SELECT *
      FROM tbl_ids_account a
     WHERE a.branch IN ('00000000004511','00000000004515','00000000004519','00000000004523','00000000004474');

  v_count        number(19);
  v_Work_Date    Varchar2(8) := To_Char(Sysdate, 'yyyymmdd');
  v_Proc_Type    Varchar(10) := '';
  v_Proc_Name    Varchar2(50) := 'proc2020_tbl_ids_account';
  v_Step_No      Varchar2(10);
  v_Step_Desc    Varchar2(200);
  v_Begin_Time   Date;
  v_End_Time     Date;
  v_Sql_Code     Varchar2(10);
  v_Sql_Errm     Varchar2(200);
  v_Execute_Flag Varchar2(10);

BEGIN
  v_count      := 0;
  v_Begin_Time := sysdate;

  OPEN c_cur;

  LOOP
    FETCH c_cur BULK COLLECT
      INTO IdsAccount_rec LIMIT 10000;

    EXIT WHEN IdsAccount_rec.count = 0;


    FOR i IN IdsAccount_rec.first .. IdsAccount_rec.last LOOP

       v_Step_No   := 'step_1';
       v_Step_Desc := '保单号:' ||  IdsAccount_rec(i).policyno || '物理类型:' ||  IdsAccount_rec(i).typeid || '账户所有人证件号:' ||  IdsAccount_rec(i).ownerid;

     UPDATE tbl_ids_account a
         SET branch = (select distinct org2_id_new  from tbl_org_revolution b
                                      WHERE a.branch = b.org2_id)
       WHERE a.policyno = IdsAccount_rec(i)
      .policyno
         AND a.typeid = IdsAccount_rec(i)
      .typeid
         AND a.ownerid = IdsAccount_rec(i)
      .ownerid
         AND EXISTS (SELECT 1
                FROM tbl_org_revolution b
               WHERE a.branch = b.org2_id);

       v_count := v_count + 1;
      COMMIT;

    END LOOP;

  END LOOP;
  CLOSE c_cur;

  v_End_Time := sysdate;
  v_Step_Desc :='结束';
  v_Execute_Flag := 'y';
  p_Pub_Insert_Log(v_Work_Date,
                   v_Proc_Type,
                   v_Proc_Name,
                   v_Step_No,
                   v_Step_Desc,
                   v_count,
                   v_Sql_Code,
                   v_Sql_Errm,
                   v_Execute_Flag,
                   v_Begin_Time,
                   v_End_Time);
  Exception
  When Others Then
    If c_cur%Isopen Then
      Close c_cur;
    End If;
    Rollback;
    v_Sql_Code     := Sqlcode;
    v_Sql_Errm     := Trim(Sqlerrm);
    v_End_Time     := Sysdate;
    v_Execute_Flag := 'n';
    p_Pub_Insert_Log(v_Work_Date,
                     v_Proc_Type,
                     v_Proc_Name,
                     v_Step_No,
                     v_Step_Desc,
                     v_count,
                     v_Sql_Code,
                     v_Sql_Errm,
                     v_Execute_Flag,
                     v_Begin_Time,
                     v_End_Time);
    close c_cur;
end;

procedure proc2020_tbl_ids_address is

  TYPE IdsAddress IS TABLE OF tbl_ids_address%ROWTYPE;
  IdsAddress_rec IdsAddress;

  CURSOR c_cur IS
    SELECT *
      FROM tbl_ids_address a
     WHERE a.branch_id IN ('00000000004511','00000000004515','00000000004519','00000000004523','00000000004474');

  v_count        number(19);
  v_Work_Date    Varchar2(8) := To_Char(Sysdate, 'yyyymmdd');
  v_Proc_Type    Varchar(10) := '';
  v_Proc_Name    Varchar2(50) := 'proc2020_tbl_ids_address';
  v_Step_No      Varchar2(10);
  v_Step_Desc    Varchar2(200);
  v_Begin_Time   Date;
  v_End_Time     Date;
  v_Sql_Code     Varchar2(10);
  v_Sql_Errm     Varchar2(200);
  v_Execute_Flag Varchar2(10);

BEGIN
  v_count      := 0;
  v_Begin_Time := sysdate;

  OPEN c_cur;

  LOOP
    FETCH c_cur BULK COLLECT
      INTO IdsAddress_rec LIMIT 10000;

    EXIT WHEN IdsAddress_rec.count = 0;


    FOR i IN IdsAddress_rec.first .. IdsAddress_rec.last LOOP

       v_Step_No   := 'step_1';
       v_Step_Desc := 'source_person_id:' ||  IdsAddress_rec(i).source_person_id || 'usage:' ||  IdsAddress_rec(i).usage || 'seq:' ||  IdsAddress_rec(i).seq;

     UPDATE tbl_ids_address a
         SET branch_id = (select distinct org2_id_new  from tbl_org_revolution b
                                      WHERE a.branch_id = b.org2_id)
       WHERE a.source_person_id = IdsAddress_rec(i)
      .source_person_id
         AND a.usage = IdsAddress_rec(i)
      .usage
         AND a.seq = IdsAddress_rec(i)
      .seq
         AND EXISTS (SELECT 1
                FROM tbl_org_revolution b
               WHERE a.branch_id = b.org2_id);

       v_count := v_count + 1;
      COMMIT;

    END LOOP;

  END LOOP;
  CLOSE c_cur;

  v_End_Time := sysdate;
  v_Step_Desc :='结束';
  v_Execute_Flag := 'y';
  p_Pub_Insert_Log(v_Work_Date,
                   v_Proc_Type,
                   v_Proc_Name,
                   v_Step_No,
                   v_Step_Desc,
                   v_count,
                   v_Sql_Code,
                   v_Sql_Errm,
                   v_Execute_Flag,
                   v_Begin_Time,
                   v_End_Time);
  Exception
  When Others Then
    If c_cur%Isopen Then
      Close c_cur;
    End If;
    Rollback;
    v_Sql_Code     := Sqlcode;
    v_Sql_Errm     := Trim(Sqlerrm);
    v_End_Time     := Sysdate;
    v_Execute_Flag := 'n';
    p_Pub_Insert_Log(v_Work_Date,
                     v_Proc_Type,
                     v_Proc_Name,
                     v_Step_No,
                     v_Step_Desc,
                     v_count,
                     v_Sql_Code,
                     v_Sql_Errm,
                     v_Execute_Flag,
                     v_Begin_Time,
                     v_End_Time);
    close c_cur;
end;

procedure proc2020_tbl_ids_cdelrec is

  TYPE IdsCdelrec IS TABLE OF tbl_ids_cdelrec%ROWTYPE;
  IdsCdelrec_rec IdsCdelrec;

  CURSOR c_cur IS
    SELECT *
      FROM tbl_ids_cdelrec a
     WHERE a.branch IN ('00000000004511','00000000004515','00000000004519','00000000004523','00000000004474');

  v_count        number(19);
  v_Work_Date    Varchar2(8) := To_Char(Sysdate, 'yyyymmdd');
  v_Proc_Type    Varchar(10) := '';
  v_Proc_Name    Varchar2(50) := 'proc2020_tbl_ids_cdelrec';
  v_Step_No      Varchar2(10);
  v_Step_Desc    Varchar2(200);
  v_Begin_Time   Date;
  v_End_Time     Date;
  v_Sql_Code     Varchar2(10);
  v_Sql_Errm     Varchar2(200);
  v_Execute_Flag Varchar2(10);

BEGIN
  v_count      := 0;
  v_Begin_Time := sysdate;


  OPEN c_cur;

  LOOP
    FETCH c_cur BULK COLLECT
      INTO IdsCdelrec_rec LIMIT 10000;

    EXIT WHEN IdsCdelrec_rec.count = 0;


    FOR i IN IdsCdelrec_rec.first .. IdsCdelrec_rec.last LOOP

       v_Step_No   := 'step_1';
       v_Step_Desc := '分公司:' || IdsCdelrec_rec(i).branch || '保单号:' || IdsCdelrec_rec(i).policyno || '险种:' || IdsCdelrec_rec(i).classcode || '责任码:' || IdsCdelrec_rec(i).delcode || '责任子码:' || IdsCdelrec_rec(i).typeno;

     UPDATE tbl_ids_cdelrec a
         SET branch = (select distinct org2_id_new  from tbl_org_revolution b
                                      WHERE a.branch = b.org2_id)
       WHERE a.branch = IdsCdelrec_rec(i)
      .branch
         AND a.policyno = IdsCdelrec_rec(i)
      .policyno
         AND a.classcode = IdsCdelrec_rec(i)
      .classcode
       AND a.delcode = IdsCdelrec_rec(i)
      .delcode
       AND a.typeno = IdsCdelrec_rec(i)
      .typeno
         AND EXISTS (SELECT 1
                FROM tbl_org_revolution b
               WHERE a.branch = b.org2_id);

       v_count := v_count + 1;
      COMMIT;

    END LOOP;

  END LOOP;
  CLOSE c_cur;

  v_End_Time := sysdate;
  v_Step_Desc :='结束';
  v_Execute_Flag := 'y';
  p_Pub_Insert_Log(v_Work_Date,
                   v_Proc_Type,
                   v_Proc_Name,
                   v_Step_No,
                   v_Step_Desc,
                   v_count,
                   v_Sql_Code,
                   v_Sql_Errm,
                   v_Execute_Flag,
                   v_Begin_Time,
                   v_End_Time);
  Exception
  When Others Then
    If c_cur%Isopen Then
      Close c_cur;
    End If;
    Rollback;
    v_Sql_Code     := Sqlcode;
    v_Sql_Errm     := Trim(Sqlerrm);
    v_End_Time     := Sysdate;
    v_Execute_Flag := 'n';
    p_Pub_Insert_Log(v_Work_Date,
                     v_Proc_Type,
                     v_Proc_Name,
                     v_Step_No,
                     v_Step_Desc,
                     v_count,
                     v_Sql_Code,
                     v_Sql_Errm,
                     v_Execute_Flag,
                     v_Begin_Time,
                     v_End_Time);
    close c_cur;

end;

procedure proc2020_tbl_ids_chgcon is

  TYPE IdsChgcon IS TABLE OF tbl_ids_chgcon%ROWTYPE;
  IdsChgcon_rec IdsChgcon;

  CURSOR c_cur IS
    SELECT *
      FROM tbl_ids_chgcon a
     WHERE a.branch_id IN ('00000000004511','00000000004515','00000000004519','00000000004523','00000000004474');

  v_count        number(19);
  v_Work_Date    Varchar2(8) := To_Char(Sysdate, 'yyyymmdd');
  v_Proc_Type    Varchar(10) := '';
  v_Proc_Name    Varchar2(50) := 'proc2020_tbl_ids_chgcon';
  v_Step_No      Varchar2(10);
  v_Step_Desc    Varchar2(200);
  v_Begin_Time   Date;
  v_End_Time     Date;
  v_Sql_Code     Varchar2(10);
  v_Sql_Errm     Varchar2(200);
  v_Execute_Flag Varchar2(10);

BEGIN
  v_count      := 0;
  v_Begin_Time := sysdate;

  OPEN c_cur;

  LOOP
    FETCH c_cur BULK COLLECT
      INTO IdsChgcon_rec LIMIT 10000;

    EXIT WHEN IdsChgcon_rec.count = 0;


    FOR i IN IdsChgcon_rec.first .. IdsChgcon_rec.last LOOP

       v_Step_No   := 'step_1';
       v_Step_Desc := '保单号:' ||  IdsChgcon_rec(i).policyno || '险种:' ||  IdsChgcon_rec(i).classcode || 'modyname:' ||  IdsChgcon_rec(i).modyname || 'modino:' ||  IdsChgcon_rec(i).modino;

     UPDATE tbl_ids_chgcon a
         SET branch_id = (select distinct org2_id_new  from tbl_org_revolution b
                                      WHERE a.branch_id = b.org2_id)
       WHERE a.policyno = IdsChgcon_rec(i)
      .policyno
         AND a.classcode = IdsChgcon_rec(i)
      .classcode
         AND a.modyname = IdsChgcon_rec(i)
      .modyname
       AND a.modino = IdsChgcon_rec(i)
      .modino
         AND EXISTS (SELECT 1
                FROM tbl_org_revolution b
               WHERE a.branch_id = b.org2_id);

       v_count := v_count + 1;
      COMMIT;

    END LOOP;

  END LOOP;
  CLOSE c_cur;

  v_End_Time := sysdate;
  v_Step_Desc :='结束';
  v_Execute_Flag := 'y';
  p_Pub_Insert_Log(v_Work_Date,
                   v_Proc_Type,
                   v_Proc_Name,
                   v_Step_No,
                   v_Step_Desc,
                   v_count,
                   v_Sql_Code,
                   v_Sql_Errm,
                   v_Execute_Flag,
                   v_Begin_Time,
                   v_End_Time);
  Exception
  When Others Then
    If c_cur%Isopen Then
      Close c_cur;
    End If;
    Rollback;
    v_Sql_Code     := Sqlcode;
    v_Sql_Errm     := Trim(Sqlerrm);
    v_End_Time     := Sysdate;
    v_Execute_Flag := 'n';
    p_Pub_Insert_Log(v_Work_Date,
                     v_Proc_Type,
                     v_Proc_Name,
                     v_Step_No,
                     v_Step_Desc,
                     v_count,
                     v_Sql_Code,
                     v_Sql_Errm,
                     v_Execute_Flag,
                     v_Begin_Time,
                     v_End_Time);
    close c_cur;
end;

-- haha
procedure proc2020_tbl_ids_debitrec (i_Branchid In tbl_ids_debitrec.branch%Type)is
  CURSOR c_cur IS
    SELECT a.rowid
      FROM tbl_ids_debitrec a
     WHERE a.branch = i_Branchid;

  v_count        number(19);
  v_Work_Date    Varchar2(8) := To_Char(Sysdate, 'yyyymmdd');
  v_Proc_Type    Varchar(10) := '';
  v_Proc_Name    Varchar2(50) := 'proc2020_tbl_ids_debitrec';
  v_Step_No      Varchar2(10);
  v_Step_Desc    Varchar2(200);
  v_Begin_Time   Date;
  v_End_Time     Date;
  v_Sql_Code     Varchar2(10);
  v_Sql_Errm     Varchar2(200);
  v_Execute_Flag Varchar2(10);
  v_cur          c_cur%rowtype;

BEGIN
  v_count      := 0;
  v_Begin_Time := sysdate;

  OPEN c_cur;

  LOOP
    FETCH c_cur
      INTO v_cur;
    EXIT WHEN c_cur%notfound;

    v_Step_No   := 'step_1';
    v_Step_Desc := 'rowid:' || v_cur.rowid;

    UPDATE tbl_ids_debitrec a
         SET branch = '00000000000019'
       where rowid = v_cur.rowid;

    v_count := v_count + 1;
    if mod(v_count, 10000) = 0 then
      commit;
    end if;
  END LOOP;
  commit;
  CLOSE c_cur;

  v_End_Time := sysdate;
  v_Step_Desc :='结束';
  v_Execute_Flag := 'y';
  p_Pub_Insert_Log(v_Work_Date,
                   v_Proc_Type,
                   v_Proc_Name,
                   v_Step_No,
                   v_Step_Desc,
                   v_count,
                   v_Sql_Code,
                   v_Sql_Errm,
                   v_Execute_Flag,
                   v_Begin_Time,
                   v_End_Time);
  Exception
  When Others Then
    If c_cur%Isopen Then
      Close c_cur;
    End If;
    Rollback;
    v_Sql_Code     := Sqlcode;
    v_Sql_Errm     := Trim(Sqlerrm);
    v_End_Time     := Sysdate;
    v_Execute_Flag := 'n';
    p_Pub_Insert_Log(v_Work_Date,
                     v_Proc_Type,
                     v_Proc_Name,
                     v_Step_No,
                     v_Step_Desc,
                     v_count,
                     v_Sql_Code,
                     v_Sql_Errm,
                     v_Execute_Flag,
                     v_Begin_Time,
                     v_End_Time);
    close c_cur;
end;

procedure proc2020_tbl_ids_email is

  TYPE IdsEmail IS TABLE OF tbl_ids_email%ROWTYPE;
  IdsEmail_rec IdsEmail;

  CURSOR c_cur IS
    SELECT *
      FROM tbl_ids_email a
     WHERE a.branch_id IN ('00000000004511','00000000004515','00000000004519','00000000004523','00000000004474');

  v_count        number(19);
  v_Work_Date    Varchar2(8) := To_Char(Sysdate, 'yyyymmdd');
  v_Proc_Type    Varchar(10) := '';
  v_Proc_Name    Varchar2(50) := 'proc2020_tbl_ids_email';
  v_Step_No      Varchar2(10);
  v_Step_Desc    Varchar2(200);
  v_Begin_Time   Date;
  v_End_Time     Date;
  v_Sql_Code     Varchar2(10);
  v_Sql_Errm     Varchar2(200);
  v_Execute_Flag Varchar2(10);

BEGIN
  v_count      := 0;
  v_Begin_Time := sysdate;

  OPEN c_cur;

  LOOP
    FETCH c_cur BULK COLLECT
      INTO IdsEmail_rec LIMIT 10000;

    EXIT WHEN IdsEmail_rec.count = 0;


    FOR i IN IdsEmail_rec.first .. IdsEmail_rec.last LOOP

       v_Step_No   := 'step_1';
       v_Step_Desc := 'source_person_id:' ||  IdsEmail_rec(i).source_person_id || 'usage:' ||  IdsEmail_rec(i).usage;

     UPDATE tbl_ids_email a
         SET branch_id = (select distinct org2_id_new  from tbl_org_revolution b
                                      WHERE a.branch_id = b.org2_id)
       WHERE a.source_person_id = IdsEmail_rec(i)
      .source_person_id
         AND a.usage = IdsEmail_rec(i)
      .usage
         AND EXISTS (SELECT 1
                FROM tbl_org_revolution b
               WHERE a.branch_id = b.org2_id);

       v_count := v_count + 1;
      COMMIT;

    END LOOP;

  END LOOP;
  CLOSE c_cur;

  v_End_Time := sysdate;
  v_Step_Desc :='结束';
  v_Execute_Flag := 'y';
  p_Pub_Insert_Log(v_Work_Date,
                   v_Proc_Type,
                   v_Proc_Name,
                   v_Step_No,
                   v_Step_Desc,
                   v_count,
                   v_Sql_Code,
                   v_Sql_Errm,
                   v_Execute_Flag,
                   v_Begin_Time,
                   v_End_Time);
  Exception
  When Others Then
    If c_cur%Isopen Then
      Close c_cur;
    End If;
    Rollback;
    v_Sql_Code     := Sqlcode;
    v_Sql_Errm     := Trim(Sqlerrm);
    v_End_Time     := Sysdate;
    v_Execute_Flag := 'n';
    p_Pub_Insert_Log(v_Work_Date,
                     v_Proc_Type,
                     v_Proc_Name,
                     v_Step_No,
                     v_Step_Desc,
                     v_count,
                     v_Sql_Code,
                     v_Sql_Errm,
                     v_Execute_Flag,
                     v_Begin_Time,
                     v_End_Time);
    close c_cur;
end;

procedure proc2020_tbl_ids_empno is

  TYPE IdsEmpno IS TABLE OF tbl_ids_empno%ROWTYPE;
  IdsEmpno_rec IdsEmpno;

  CURSOR c_cur IS
    SELECT *
      FROM tbl_ids_empno a
     WHERE a.branch IN ('00000000004511','00000000004515','00000000004519','00000000004523','00000000004474');

  v_count        number(19);
  v_Work_Date    Varchar2(8) := To_Char(Sysdate, 'yyyymmdd');
  v_Proc_Type    Varchar(10) := '';
  v_Proc_Name    Varchar2(50) := 'proc2020_tbl_ids_empno';
  v_Step_No      Varchar2(10);
  v_Step_Desc    Varchar2(200);
  v_Begin_Time   Date;
  v_End_Time     Date;
  v_Sql_Code     Varchar2(10);
  v_Sql_Errm     Varchar2(200);
  v_Execute_Flag Varchar2(10);

BEGIN
  v_count      := 0;
  v_Begin_Time := sysdate;

  OPEN c_cur;

  LOOP
    FETCH c_cur BULK COLLECT
      INTO IdsEmpno_rec LIMIT 10000;

    EXIT WHEN IdsEmpno_rec.count = 0;


    FOR i IN IdsEmpno_rec.first .. IdsEmpno_rec.last LOOP

       v_Step_No   := 'step_1';
       v_Step_Desc := 'empno:' ||  IdsEmpno_rec(i).empno;

     UPDATE tbl_ids_empno a
         SET branch = (select distinct org2_id_new  from tbl_org_revolution b
                                      WHERE a.branch = b.org2_id)
       WHERE a.empno = IdsEmpno_rec(i)
      .empno
         AND EXISTS (SELECT 1
                FROM tbl_org_revolution b
               WHERE a.branch = b.org2_id);

       v_count := v_count + 1;
      COMMIT;

    END LOOP;

  END LOOP;
  CLOSE c_cur;

  v_End_Time := sysdate;
  v_Step_Desc :='结束';
  v_Execute_Flag := 'y';
  p_Pub_Insert_Log(v_Work_Date,
                   v_Proc_Type,
                   v_Proc_Name,
                   v_Step_No,
                   v_Step_Desc,
                   v_count,
                   v_Sql_Code,
                   v_Sql_Errm,
                   v_Execute_Flag,
                   v_Begin_Time,
                   v_End_Time);
  Exception
  When Others Then
    If c_cur%Isopen Then
      Close c_cur;
    End If;
    Rollback;
    v_Sql_Code     := Sqlcode;
    v_Sql_Errm     := Trim(Sqlerrm);
    v_End_Time     := Sysdate;
    v_Execute_Flag := 'n';
    p_Pub_Insert_Log(v_Work_Date,
                     v_Proc_Type,
                     v_Proc_Name,
                     v_Step_No,
                     v_Step_Desc,
                     v_count,
                     v_Sql_Code,
                     v_Sql_Errm,
                     v_Execute_Flag,
                     v_Begin_Time,
                     v_End_Time);
    close c_cur;
end;

-- jiaofei
procedure proc2020_tbl_ids_moneysch (i_Branchid In tbl_ids_moneysch.Branch_Id%Type)is
 cursor c_cur is
    select a.rowid
      from tbl_ids_moneysch a
     where  a.branch_id = i_Branchid;


  v_count        number(19);
  v_Work_Date    Varchar2(8) := To_Char(Sysdate, 'yyyymmdd');
  v_Proc_Type    Varchar(10) := '';
  v_Proc_Name    Varchar2(50) := 'proc2020_tbl_ids_moneysch';
  v_Step_No      Varchar2(10);
  v_Step_Desc    Varchar2(200);
  v_Begin_Time   Date;
  v_End_Time     Date;
  v_Sql_Code     Varchar2(10);
  v_Sql_Errm     Varchar2(200);
  v_Execute_Flag Varchar2(10);
  v_cur          c_cur%rowtype;
begin
  v_count      := 0;
  v_Begin_Time := sysdate;
  open c_cur;
  loop
  fetch c_cur
      into v_cur;
    exit when c_cur%notfound;

    v_Step_No   := 'step_1';
    v_Step_Desc := 'rowid:' || v_cur.rowid;

    update tbl_ids_moneysch
       set branch_id = '00000000000019'
     where rowid = v_cur.rowid;

    -- dbms_output.put_line(sqlcode);
    v_count := v_count + 1;
    if mod(v_count, 10000) = 0 then
      commit;
    end if;
  end loop;
  commit;
  close c_cur;
  v_End_Time := sysdate;
  v_Step_Desc :='结束';
  v_Execute_Flag := 'y';
  p_Pub_Insert_Log(v_Work_Date,
 v_Proc_Type,
                   v_Proc_Name,
                   v_Step_No,
                   v_Step_Desc,
                   v_count,
           v_Sql_Code,
                   v_Sql_Errm,
                   v_Execute_Flag,
                   v_Begin_Time,
                   v_End_Time);

Exception
  When Others Then
    If c_cur%Isopen Then
      Close c_cur;
    End If;
    Rollback;
    v_Sql_Code     := Sqlcode;
    v_Sql_Errm     := Trim(Sqlerrm);
    v_End_Time     := Sysdate;
    v_Execute_Flag := 'n';
 p_Pub_Insert_Log(v_Work_Date,
                     v_Proc_Type,
                     v_Proc_Name,
                     v_Step_No,
                     v_Step_Desc,
                     v_count,
                     v_Sql_Code,
                     v_Sql_Errm,
                     v_Execute_Flag,
                     v_Begin_Time,
                     v_End_Time);
    close c_cur;
end;

procedure proc2020_tbl_ids_padrec is

  TYPE IdsPadrec IS TABLE OF tbl_ids_padrec%ROWTYPE;
  IdsPadrec_rec IdsPadrec;

  CURSOR c_cur IS
    SELECT *
      FROM tbl_ids_padrec a
     WHERE a.branch IN ('00000000004511','00000000004515','00000000004519','00000000004523','00000000004474');

  v_count        number(19);
  v_Work_Date    Varchar2(8) := To_Char(Sysdate, 'yyyymmdd');
  v_Proc_Type    Varchar(10) := '';
  v_Proc_Name    Varchar2(50) := 'proc2020_tbl_ids_padrec';
  v_Step_No      Varchar2(10);
  v_Step_Desc    Varchar2(200);
  v_Begin_Time   Date;
  v_End_Time     Date;
  v_Sql_Code     Varchar2(10);
  v_Sql_Errm     Varchar2(200);
  v_Execute_Flag Varchar2(10);

BEGIN
  v_count      := 0;
  v_Begin_Time := sysdate;

  OPEN c_cur;

  LOOP
    FETCH c_cur BULK COLLECT
      INTO IdsPadrec_rec LIMIT 10000;

    EXIT WHEN IdsPadrec_rec.count = 0;


    FOR i IN IdsPadrec_rec.first .. IdsPadrec_rec.last LOOP

       v_Step_No   := 'step_1';
       v_Step_Desc := '保单号:' ||  IdsPadrec_rec(i).policyno || 'padtime:' ||  IdsPadrec_rec(i).padtime;

     UPDATE tbl_ids_padrec a
         SET branch = (select distinct org2_id_new  from tbl_org_revolution b
                                      WHERE a.branch = b.org2_id)
       WHERE a.policyno = IdsPadrec_rec(i)
      .policyno
         AND a.padtime = IdsPadrec_rec(i)
      .padtime
         AND EXISTS (SELECT 1
                FROM tbl_org_revolution b
               WHERE a.branch = b.org2_id);

       v_count := v_count + 1;
      COMMIT;

    END LOOP;

  END LOOP;
  CLOSE c_cur;

  v_End_Time := sysdate;
  v_Step_Desc :='结束';
  v_Execute_Flag := 'y';
  p_Pub_Insert_Log(v_Work_Date,
                   v_Proc_Type,
                   v_Proc_Name,
                   v_Step_No,
                   v_Step_Desc,
                   v_count,
                   v_Sql_Code,
                   v_Sql_Errm,
                   v_Execute_Flag,
                   v_Begin_Time,
                   v_End_Time);
  Exception
  When Others Then
    If c_cur%Isopen Then
      Close c_cur;
    End If;
    Rollback;
    v_Sql_Code     := Sqlcode;
    v_Sql_Errm     := Trim(Sqlerrm);
    v_End_Time     := Sysdate;
    v_Execute_Flag := 'n';
    p_Pub_Insert_Log(v_Work_Date,
                     v_Proc_Type,
                     v_Proc_Name,
                     v_Step_No,
                     v_Step_Desc,
                     v_count,
                     v_Sql_Code,
                     v_Sql_Errm,
                     v_Execute_Flag,
                     v_Begin_Time,
                     v_End_Time);
    close c_cur;
end;

procedure proc2020_tbl_ids_phone is

  TYPE IdsPhone IS TABLE OF tbl_ids_phone%ROWTYPE;
  IdsPhone_rec IdsPhone;

  CURSOR c_cur IS
    SELECT *
      FROM tbl_ids_phone a
     WHERE a.branch_id IN ('00000000004511','00000000004515','00000000004519','00000000004523','00000000004474');

  v_count        number(19);
  v_Work_Date    Varchar2(8) := To_Char(Sysdate, 'yyyymmdd');
  v_Proc_Type    Varchar(10) := '';
  v_Proc_Name    Varchar2(50) := 'proc2020_tbl_ids_phone';
  v_Step_No      Varchar2(10);
  v_Step_Desc    Varchar2(200);
  v_Begin_Time   Date;
  v_End_Time     Date;
  v_Sql_Code     Varchar2(10);
  v_Sql_Errm     Varchar2(200);
  v_Execute_Flag Varchar2(10);

BEGIN
  v_count      := 0;
  v_Begin_Time := sysdate;

  OPEN c_cur;

  LOOP
    FETCH c_cur BULK COLLECT
      INTO IdsPhone_rec LIMIT 10000;

    EXIT WHEN IdsPhone_rec.count = 0;


    FOR i IN IdsPhone_rec.first .. IdsPhone_rec.last LOOP

       v_Step_No   := 'step_1';
       v_Step_Desc := 'source_person_id:' ||  IdsPhone_rec(i).source_person_id || 'usage:' ||  IdsPhone_rec(i).usage || 'seq:' ||  IdsPhone_rec(i).seq;

     UPDATE tbl_ids_phone a
         SET branch_id = (select distinct org2_id_new  from tbl_org_revolution b
                                      WHERE a.branch_id = b.org2_id)
       WHERE a.source_person_id = IdsPhone_rec(i)
      .source_person_id
         AND a.usage = IdsPhone_rec(i)
      .usage
         AND a.seq = IdsPhone_rec(i)
      .seq
         AND EXISTS (SELECT 1
                FROM tbl_org_revolution b
               WHERE a.branch_id = b.org2_id);

       v_count := v_count + 1;
      COMMIT;

    END LOOP;

  END LOOP;
  CLOSE c_cur;

  v_End_Time := sysdate;
  v_Step_Desc :='结束';
  v_Execute_Flag := 'y';
  p_Pub_Insert_Log(v_Work_Date,
                   v_Proc_Type,
                   v_Proc_Name,
                   v_Step_No,
                   v_Step_Desc,
                   v_count,
                   v_Sql_Code,
                   v_Sql_Errm,
                   v_Execute_Flag,
                   v_Begin_Time,
                   v_End_Time);
  Exception
  When Others Then
    If c_cur%Isopen Then
      Close c_cur;
    End If;
    Rollback;
    v_Sql_Code     := Sqlcode;
    v_Sql_Errm     := Trim(Sqlerrm);
    v_End_Time     := Sysdate;
    v_Execute_Flag := 'n';
    p_Pub_Insert_Log(v_Work_Date,
                     v_Proc_Type,
                     v_Proc_Name,
                     v_Step_No,
                     v_Step_Desc,
                     v_count,
                     v_Sql_Code,
                     v_Sql_Errm,
                     v_Execute_Flag,
                     v_Begin_Time,
                     v_End_Time);
    close c_cur;
end;

procedure proc2020_tbl_ids_plc_acct is

  TYPE IdsPlcAcct IS TABLE OF tbl_ids_plc_acct%ROWTYPE;
  IdsPlcAcct_rec IdsPlcAcct;

  CURSOR c_cur IS
    SELECT *
      FROM tbl_ids_plc_acct a
     WHERE a.branch IN ('00000000004511','00000000004515','00000000004519','00000000004523','00000000004474');

  v_count        number(19);
  v_Work_Date    Varchar2(8) := To_Char(Sysdate, 'yyyymmdd');
  v_Proc_Type    Varchar(10) := '';
  v_Proc_Name    Varchar2(50) := 'proc2020_tbl_ids_plc_acct';
  v_Step_No      Varchar2(10);
  v_Step_Desc    Varchar2(200);
  v_Begin_Time   Date;
  v_End_Time     Date;
  v_Sql_Code     Varchar2(10);
  v_Sql_Errm     Varchar2(200);
  v_Execute_Flag Varchar2(10);

BEGIN
  v_count      := 0;
  v_Begin_Time := sysdate;

  OPEN c_cur;

  LOOP
    FETCH c_cur BULK COLLECT
      INTO IdsPlcAcct_rec LIMIT 10000;

    EXIT WHEN IdsPlcAcct_rec.count = 0;


    FOR i IN IdsPlcAcct_rec.first .. IdsPlcAcct_rec.last LOOP

       v_Step_No   := 'step_1';
       v_Step_Desc := '保单号:' ||  IdsPlcAcct_rec(i).policy_no || '险种:' ||  IdsPlcAcct_rec(i).classcode || '责任码:' ||  IdsPlcAcct_rec(i).delcode || '责任子码:' ||  IdsPlcAcct_rec(i).typeno || 'acct_usage:' ||  IdsPlcAcct_rec(i).acct_usage || 'plc_bp_trans_acct_id:' ||  IdsPlcAcct_rec(i).plc_bp_trans_acct_id;

     UPDATE tbl_ids_plc_acct a
         SET branch = (select distinct org2_id_new  from tbl_org_revolution b
                                      WHERE a.branch = b.org2_id)
       WHERE a.policy_no = IdsPlcAcct_rec(i)
      .policy_no
         AND a.classcode = IdsPlcAcct_rec(i)
      .classcode
         AND a.delcode = IdsPlcAcct_rec(i)
      .delcode
         AND a.typeno = IdsPlcAcct_rec(i)
      .typeno
         AND a.acct_usage = IdsPlcAcct_rec(i)
      .acct_usage
         AND a.plc_bp_trans_acct_id = IdsPlcAcct_rec(i)
      .plc_bp_trans_acct_id
         AND EXISTS (SELECT 1
                FROM tbl_org_revolution b
               WHERE a.branch = b.org2_id);

       v_count := v_count + 1;
      COMMIT;

    END LOOP;

  END LOOP;
  CLOSE c_cur;

  v_End_Time := sysdate;
  v_Step_Desc :='结束';
  v_Execute_Flag := 'y';
  p_Pub_Insert_Log(v_Work_Date,
                   v_Proc_Type,
                   v_Proc_Name,
                   v_Step_No,
                   v_Step_Desc,
                   v_count,
                   v_Sql_Code,
                   v_Sql_Errm,
                   v_Execute_Flag,
                   v_Begin_Time,
                   v_End_Time);
  Exception
  When Others Then
    If c_cur%Isopen Then
      Close c_cur;
    End If;
    Rollback;
    v_Sql_Code     := Sqlcode;
    v_Sql_Errm     := Trim(Sqlerrm);
    v_End_Time     := Sysdate;
    v_Execute_Flag := 'n';
    p_Pub_Insert_Log(v_Work_Date,
                     v_Proc_Type,
                     v_Proc_Name,
                     v_Step_No,
                     v_Step_Desc,
                     v_count,
                     v_Sql_Code,
                     v_Sql_Errm,
                     v_Execute_Flag,
                     v_Begin_Time,
                     v_End_Time);
    close c_cur;
end;

procedure proc2020_tbl_ids_realpayrc is

  TYPE IdsRealpayrc IS TABLE OF tbl_ids_realpayrc%ROWTYPE;
  IdsRealpayrc_rec IdsRealpayrc;

  CURSOR c_cur IS
    SELECT *
      FROM tbl_ids_realpayrc a
     WHERE a.branch IN ('00000000004511','00000000004515','00000000004519','00000000004523','00000000004474');

  v_count        number(19);
  v_Work_Date    Varchar2(8) := To_Char(Sysdate, 'yyyymmdd');
  v_Proc_Type    Varchar(10) := '';
  v_Proc_Name    Varchar2(50) := 'proc2020_tbl_ids_realpayrc';
  v_Step_No      Varchar2(10);
  v_Step_Desc    Varchar2(200);
  v_Begin_Time   Date;
  v_End_Time     Date;
  v_Sql_Code     Varchar2(10);
  v_Sql_Errm     Varchar2(200);
  v_Execute_Flag Varchar2(10);

BEGIN
  v_count      := 0;
  v_Begin_Time := sysdate;

  OPEN c_cur;

  LOOP
    FETCH c_cur BULK COLLECT
      INTO IdsRealpayrc_rec LIMIT 10000;

    EXIT WHEN IdsRealpayrc_rec.count = 0;


    FOR i IN IdsRealpayrc_rec.first .. IdsRealpayrc_rec.last LOOP

       v_Step_No   := 'step_1';
       v_Step_Desc := '保单号:' ||  IdsRealpayrc_rec(i).policyno || '险种:' ||  IdsRealpayrc_rec(i).classcode || '责任码:' ||  IdsRealpayrc_rec(i).delcode || '责任子码:' ||  IdsRealpayrc_rec(i).typeno || 'paytime:' ||  IdsRealpayrc_rec(i).paytime;

     UPDATE tbl_ids_realpayrc a
         SET branch = (select distinct org2_id_new  from tbl_org_revolution b
                                      WHERE a.branch = b.org2_id)
       WHERE a.policyno = IdsRealpayrc_rec(i)
      .policyno
         AND a.classcode = IdsRealpayrc_rec(i)
      .classcode
         AND a.delcode = IdsRealpayrc_rec(i)
      .delcode
         AND a.typeno = IdsRealpayrc_rec(i)
      .typeno
         AND a.paytime = IdsRealpayrc_rec(i)
      .paytime
         AND EXISTS (SELECT 1
                FROM tbl_org_revolution b
               WHERE a.branch = b.org2_id);

       v_count := v_count + 1;
      COMMIT;

    END LOOP;

  END LOOP;
  CLOSE c_cur;

  v_End_Time := sysdate;
  v_Step_Desc :='结束';
  v_Execute_Flag := 'y';
  p_Pub_Insert_Log(v_Work_Date,
                   v_Proc_Type,
                   v_Proc_Name,
                   v_Step_No,
                   v_Step_Desc,
                   v_count,
                   v_Sql_Code,
                   v_Sql_Errm,
                   v_Execute_Flag,
                   v_Begin_Time,
                   v_End_Time);
  Exception
  When Others Then
    If c_cur%Isopen Then
      Close c_cur;
    End If;
    Rollback;
    v_Sql_Code     := Sqlcode;
    v_Sql_Errm     := Trim(Sqlerrm);
    v_End_Time     := Sysdate;
    v_Execute_Flag := 'n';
    p_Pub_Insert_Log(v_Work_Date,
                     v_Proc_Type,
                     v_Proc_Name,
                     v_Step_No,
                     v_Step_Desc,
                     v_count,
                     v_Sql_Code,
                     v_Sql_Errm,
                     v_Execute_Flag,
                     v_Begin_Time,
                     v_End_Time);
    close c_cur;
end;
End Pkg_henan_org_all;
//



CREATE OR REPLACE PROCEDURE PPS."MYEXCEPTIONPROCE" (custid in varchar2)
as
personid varchar2(100);
custname varchar2(100);
testperson exception;
begin
  select person_id into personid from TBL_IDS_PERSON where id=custid;
  select name into custname from TBL_IDS_PERSON where id=custid;
  if(custname='test111') then
    raise testperson;
  end if;
exception
  when testperson then update TBL_IDS_PERSON set person_id=personid||'+++' where id=custid;
end;
//
CREATE OR REPLACE PROCEDURE PPS."MYTESTPROCE" (custid in varchar2,birthdate out varchar2)
is
custname varchar2(100);
personid varchar2(100);

begin
  select name into custname from TBL_IDS_PERSON where id=custid;
  if(custname is not null) then
      update TBL_IDS_PERSON set name=custname||'111' where id=custid;
  else
      update TBL_IDS_PERSON set name='test3333' where id=custid;
  end if;
  begin
  select person_id into personid from TBL_IDS_PERSON where id=custid;
  select bthdate into birthdate from TBL_IDS_PERSON where id=custid;
  end;
end;
//


CREATE OR REPLACE PROCEDURE PPS."P_PUB_INSERT_LOG" (v_work_date in varchar2, v_proc_type in varchar2, v_proc_name in varchar2, v_step_no in varchar2, v_step_desc in varchar2, v_row_num number, v_sql_code varchar2, v_sql_errm varchar2, v_execute_flag varchar2, v_begin_time date, v_end_time date)
    is

  BEGIN
    INSERT INTO STAT_ERROR_LOG
      (WORK_DATE
      ,PROC_TYPE
      ,PROC_NAME
      ,step_no
      ,step_desc
      ,ROW_NUM
      ,SQL_CODE
      ,SQL_ERRM
      ,EXECUTE_FLAG
      ,begin_time
      ,end_time
      )
    VALUES
      (v_work_date
      ,v_proc_type
      ,v_proc_name
      ,v_step_no
      ,v_step_desc
      ,v_row_num
      ,v_sql_code
      ,v_sql_errm
      ,v_execute_flag
      ,v_begin_time
      ,v_end_time);

 commit;

  END;
//


CREATE OR REPLACE PROCEDURE PPS."PROC2020DBENEFIT_INFO_COUNT" is

  TYPE BenefitTab IS TABLE OF TBL_BENEFIT_INFO_COUNT%ROWTYPE;
  benefit_rec BenefitTab;

  CURSOR c_cur IS
    select * from TBL_BENEFIT_INFO_COUNT  where branch_id='00000000000019';

  v_count        number(19);
  v_Work_Date    Varchar2(8) := To_Char(Sysdate, 'yyyymmdd');
  v_Proc_Type    Varchar(10) := '';
  v_Proc_Name    Varchar2(50) := 'proc2020D_TBL_BENEFIT_CHK_INFO';
  v_Step_No      Varchar2(10);
  v_Step_Desc    Varchar2(200);
  v_Begin_Time   Date;
  v_End_Time     Date;
  v_Sql_Code     Varchar2(10);
  v_Sql_Errm     Varchar2(200);
  v_Execute_Flag Varchar2(10);

BEGIN
  v_count      := 0;
  v_Begin_Time := sysdate;

  OPEN c_cur;

  LOOP
    FETCH c_cur BULK COLLECT
      INTO benefit_rec LIMIT 10000;

    EXIT WHEN benefit_rec.count = 0;


    FOR i IN benefit_rec.first .. benefit_rec.last LOOP

       v_Step_No   := 'step_1';
       v_Step_Desc := '保单号:' ||  benefit_rec(i).policyno_id || '险种:' ||  benefit_rec(i).class_code ||
                   '责任:' ||  benefit_rec(i).del_code || '子码:' ||  benefit_rec(i).type_no ||
                   '期次:' ||  benefit_rec(i).del_num;

  delete from TBL_BENEFIT_INFO_COUNT  a
     WHERE a.policyno_id = benefit_rec(i)
      .policyno_id
         AND a.class_code = benefit_rec(i)
      .class_code
         AND a.del_code = benefit_rec(i)
      .del_code
         AND a.type_no = benefit_rec(i)
      .type_no
         AND a.del_num = benefit_rec(i).del_num;

       v_count := v_count + 1;
      COMMIT;

    END LOOP;

  END LOOP;
  CLOSE c_cur;

  v_End_Time := sysdate;
  v_Step_Desc :='结束';
  v_Execute_Flag := 'y';
  p_Pub_Insert_Log(v_Work_Date,
                   v_Proc_Type,
                   v_Proc_Name,
                   v_Step_No,
                   v_Step_Desc,
                   v_count,
                   v_Sql_Code,
                   v_Sql_Errm,
                   v_Execute_Flag,
                   v_Begin_Time,
                   v_End_Time);
  Exception
  When Others Then
    If c_cur%Isopen Then
      Close c_cur;
    End If;
    Rollback;
    v_Sql_Code     := Sqlcode;
    v_Sql_Errm     := Trim(Sqlerrm);
    v_End_Time     := Sysdate;
    v_Execute_Flag := 'n';
    p_Pub_Insert_Log(v_Work_Date,
                     v_Proc_Type,
                     v_Proc_Name,
                     v_Step_No,
                     v_Step_Desc,
                     v_count,
                     v_Sql_Code,
                     v_Sql_Errm,
                     v_Execute_Flag,
                     v_Begin_Time,
                     v_End_Time);
    close c_cur;
end;
//


CREATE OR REPLACE PROCEDURE PPS."PROC2020DFAIL_BENEFIT_INFO" is

  TYPE BenefitTab IS TABLE OF TBL_FAIL_BENEFIT_INFO%ROWTYPE;
  benefit_rec BenefitTab;

  CURSOR c_cur IS
    select * from TBL_FAIL_BENEFIT_INFO  where branch_id='00000000000019';

  v_count        number(19);
  v_Work_Date    Varchar2(8) := To_Char(Sysdate, 'yyyymmdd');
  v_Proc_Type    Varchar(10) := '';
  v_Proc_Name    Varchar2(50) := 'proc2020DFAIL_BENEFIT_INFO';
  v_Step_No      Varchar2(10);
  v_Step_Desc    Varchar2(200);
  v_Begin_Time   Date;
  v_End_Time     Date;
  v_Sql_Code     Varchar2(10);
  v_Sql_Errm     Varchar2(200);
  v_Execute_Flag Varchar2(10);

BEGIN
  v_count      := 0;
  v_Begin_Time := sysdate;

  OPEN c_cur;

  LOOP
    FETCH c_cur BULK COLLECT
      INTO benefit_rec LIMIT 10000;

    EXIT WHEN benefit_rec.count = 0;


    FOR i IN benefit_rec.first .. benefit_rec.last LOOP

       v_Step_No   := 'step_1';
       v_Step_Desc := '保单号:' ||  benefit_rec(i).policyno_id || '险种:' ||  benefit_rec(i).class_code ||
                   '责任:' ||  benefit_rec(i).del_code || '子码:' ||  benefit_rec(i).type_no ||
                   '期次:' ||  benefit_rec(i).del_num;

   delete from TBL_FAIL_BENEFIT_INFO  a
     WHERE a.policyno_id = benefit_rec(i)
      .policyno_id
         AND a.class_code = benefit_rec(i)
      .class_code
         AND a.del_code = benefit_rec(i)
      .del_code
         AND a.type_no = benefit_rec(i)
      .type_no
         AND a.del_num = benefit_rec(i).del_num;

       v_count := v_count + 1;
      COMMIT;

    END LOOP;

  END LOOP;
  CLOSE c_cur;

  v_End_Time := sysdate;
  v_Step_Desc :='结束';
  v_Execute_Flag := 'y';
  p_Pub_Insert_Log(v_Work_Date,
                   v_Proc_Type,
                   v_Proc_Name,
                   v_Step_No,
                   v_Step_Desc,
                   v_count,
                   v_Sql_Code,
                   v_Sql_Errm,
                   v_Execute_Flag,
                   v_Begin_Time,
                   v_End_Time);
  Exception
  When Others Then
    If c_cur%Isopen Then
      Close c_cur;
    End If;
    Rollback;
    v_Sql_Code     := Sqlcode;
    v_Sql_Errm     := Trim(Sqlerrm);
    v_End_Time     := Sysdate;
    v_Execute_Flag := 'n';
    p_Pub_Insert_Log(v_Work_Date,
                     v_Proc_Type,
                     v_Proc_Name,
                     v_Step_No,
                     v_Step_Desc,
                     v_count,
                     v_Sql_Code,
                     v_Sql_Errm,
                     v_Execute_Flag,
                     v_Begin_Time,
                     v_End_Time);
    close c_cur;
end;
//


CREATE OR REPLACE PROCEDURE PPS."PROC2020D_D01_RISKCON" is

  TYPE BenefitTab IS TABLE OF D01_RISKCON%ROWTYPE;
  benefit_rec BenefitTab;

  CURSOR c_cur IS
    select * from D01_RISKCON  where branch='00000000000019';

  v_count        number(19);
  v_Work_Date    Varchar2(8) := To_Char(Sysdate, 'yyyymmdd');
  v_Proc_Type    Varchar(10) := '';
  v_Proc_Name    Varchar2(50) := 'proc2020D_D01_RISKCON';
  v_Step_No      Varchar2(10);
  v_Step_Desc    Varchar2(200);
  v_Begin_Time   Date;
  v_End_Time     Date;
  v_Sql_Code     Varchar2(10);
  v_Sql_Errm     Varchar2(200);
  v_Execute_Flag Varchar2(10);

BEGIN
  v_count      := 0;
  v_Begin_Time := sysdate;

  OPEN c_cur;

  LOOP
    FETCH c_cur BULK COLLECT
      INTO benefit_rec LIMIT 10000;

    EXIT WHEN benefit_rec.count = 0;


    FOR i IN benefit_rec.first .. benefit_rec.last LOOP

       v_Step_No   := 'step_1';
       v_Step_Desc := '保单号:' ||  benefit_rec(i).policyno || '险种:' ||  benefit_rec(i).classcode;

  delete from D01_RISKCON  a
     WHERE a.policyno = benefit_rec(i)
      .policyno
         AND a.classcode = benefit_rec(i)
      .classcode;

       v_count := v_count + 1;
      COMMIT;

    END LOOP;

  END LOOP;
  CLOSE c_cur;

  v_End_Time := sysdate;
  v_Step_Desc :='结束';
  v_Execute_Flag := 'y';
  p_Pub_Insert_Log(v_Work_Date,
                   v_Proc_Type,
                   v_Proc_Name,
                   v_Step_No,
                   v_Step_Desc,
                   v_count,
                   v_Sql_Code,
                   v_Sql_Errm,
                   v_Execute_Flag,
                   v_Begin_Time,
                   v_End_Time);
  Exception
  When Others Then
    If c_cur%Isopen Then
      Close c_cur;
    End If;
    Rollback;
    v_Sql_Code     := Sqlcode;
    v_Sql_Errm     := Trim(Sqlerrm);
    v_End_Time     := Sysdate;
    v_Execute_Flag := 'n';
    p_Pub_Insert_Log(v_Work_Date,
                     v_Proc_Type,
                     v_Proc_Name,
                     v_Step_No,
                     v_Step_Desc,
                     v_count,
                     v_Sql_Code,
                     v_Sql_Errm,
                     v_Execute_Flag,
                     v_Begin_Time,
                     v_End_Time);
    close c_cur;
end;
//


CREATE OR REPLACE PROCEDURE PPS."PROC2020D_JOB_PAYDIFFERENCE" is

  TYPE BenefitTab IS TABLE OF JOB_PAYDIFFERENCE%ROWTYPE;
  benefit_rec BenefitTab;

  CURSOR c_cur IS
    select * from JOB_PAYDIFFERENCE  where branchid='00000000000019';

  v_count        number(19);
  v_Work_Date    Varchar2(8) := To_Char(Sysdate, 'yyyymmdd');
  v_Proc_Type    Varchar(10) := '';
  v_Proc_Name    Varchar2(50) := 'proc2020D_JOB_PAYDIFFERENCE';
  v_Step_No      Varchar2(10);
  v_Step_Desc    Varchar2(200);
  v_Begin_Time   Date;
  v_End_Time     Date;
  v_Sql_Code     Varchar2(10);
  v_Sql_Errm     Varchar2(200);
  v_Execute_Flag Varchar2(10);

BEGIN
  v_count      := 0;
  v_Begin_Time := sysdate;

  OPEN c_cur;

  LOOP
    FETCH c_cur BULK COLLECT
      INTO benefit_rec LIMIT 10000;

    EXIT WHEN benefit_rec.count = 0;


    FOR i IN benefit_rec.first .. benefit_rec.last LOOP

       v_Step_No   := 'step_1';
       v_Step_Desc := '保单号:' ||  benefit_rec(i).policyno || '险种:' ||  benefit_rec(i).classcode ||
                   '责任:' ||  benefit_rec(i).delcode || '子码:' ||  benefit_rec(i).typeno ||
                   '期次:' ||  benefit_rec(i).delnum;

   delete from JOB_PAYDIFFERENCE  a
     WHERE a.policyno = benefit_rec(i)
      .policyno
         AND a.classcode = benefit_rec(i)
      .classcode
         AND a.delcode = benefit_rec(i)
      .delcode
         AND a.typeno = benefit_rec(i)
      .typeno
         AND a.delnum = benefit_rec(i).delnum;

       v_count := v_count + 1;
      COMMIT;

    END LOOP;

  END LOOP;
  CLOSE c_cur;

  v_End_Time := sysdate;
  v_Step_Desc :='结束';
  v_Execute_Flag := 'y';
  p_Pub_Insert_Log(v_Work_Date,
                   v_Proc_Type,
                   v_Proc_Name,
                   v_Step_No,
                   v_Step_Desc,
                   v_count,
                   v_Sql_Code,
                   v_Sql_Errm,
                   v_Execute_Flag,
                   v_Begin_Time,
                   v_End_Time);
  Exception
  When Others Then
    If c_cur%Isopen Then
      Close c_cur;
    End If;
    Rollback;
    v_Sql_Code     := Sqlcode;
    v_Sql_Errm     := Trim(Sqlerrm);
    v_End_Time     := Sysdate;
    v_Execute_Flag := 'n';
    p_Pub_Insert_Log(v_Work_Date,
                     v_Proc_Type,
                     v_Proc_Name,
                     v_Step_No,
                     v_Step_Desc,
                     v_count,
                     v_Sql_Code,
                     v_Sql_Errm,
                     v_Execute_Flag,
                     v_Begin_Time,
                     v_End_Time);
    close c_cur;
end;
//

CREATE OR REPLACE PROCEDURE PPS."PROC2020D_JOB_POLICYNOPAY" is

  TYPE BenefitTab IS TABLE OF JOB_POLICYNOPAY%ROWTYPE;
  benefit_rec BenefitTab;

  CURSOR c_cur IS
    select * from JOB_POLICYNOPAY  where branchid='00000000000019';

  v_count        number(19);
  v_Work_Date    Varchar2(8) := To_Char(Sysdate, 'yyyymmdd');
  v_Proc_Type    Varchar(10) := '';
  v_Proc_Name    Varchar2(50) := 'proc2020D_JOB_POLICYNOPAY';
  v_Step_No      Varchar2(10);
  v_Step_Desc    Varchar2(200);
  v_Begin_Time   Date;
  v_End_Time     Date;
  v_Sql_Code     Varchar2(10);
  v_Sql_Errm     Varchar2(200);
  v_Execute_Flag Varchar2(10);

BEGIN
  v_count      := 0;
  v_Begin_Time := sysdate;

  OPEN c_cur;

  LOOP
    FETCH c_cur BULK COLLECT
      INTO benefit_rec LIMIT 10000;

    EXIT WHEN benefit_rec.count = 0;


    FOR i IN benefit_rec.first .. benefit_rec.last LOOP

       v_Step_No   := 'step_1';
       v_Step_Desc := '保单号:' ||  benefit_rec(i).policyno || '险种:' ||  benefit_rec(i).classcode ||
                   '责任:' ||  benefit_rec(i).delcode || '子码:' ||  benefit_rec(i).typeno;

   delete from JOB_POLICYNOPAY  a
     WHERE a.policyno = benefit_rec(i)
      .policyno
         AND a.classcode = benefit_rec(i)
      .classcode
         AND a.delcode = benefit_rec(i)
      .delcode
         AND a.typeno = benefit_rec(i)
      .typeno;

       v_count := v_count + 1;
      COMMIT;

    END LOOP;

  END LOOP;
  CLOSE c_cur;

  v_End_Time := sysdate;
  v_Step_Desc :='结束';
  v_Execute_Flag := 'y';
  p_Pub_Insert_Log(v_Work_Date,
                   v_Proc_Type,
                   v_Proc_Name,
                   v_Step_No,
                   v_Step_Desc,
                   v_count,
                   v_Sql_Code,
                   v_Sql_Errm,
                   v_Execute_Flag,
                   v_Begin_Time,
                   v_End_Time);
  Exception
  When Others Then
    If c_cur%Isopen Then
      Close c_cur;
    End If;
    Rollback;
    v_Sql_Code     := Sqlcode;
    v_Sql_Errm     := Trim(Sqlerrm);
    v_End_Time     := Sysdate;
    v_Execute_Flag := 'n';
    p_Pub_Insert_Log(v_Work_Date,
                     v_Proc_Type,
                     v_Proc_Name,
                     v_Step_No,
                     v_Step_Desc,
                     v_count,
                     v_Sql_Code,
                     v_Sql_Errm,
                     v_Execute_Flag,
                     v_Begin_Time,
                     v_End_Time);
    close c_cur;
end;
//

CREATE OR REPLACE PROCEDURE PPS."PROC2020D_TBL_BENEFIT_CHK_INFO" is

  TYPE BenefitTab IS TABLE OF TBL_BENEFIT_CHK_INFO%ROWTYPE;
  benefit_rec BenefitTab;

  CURSOR c_cur IS
    select * from TBL_BENEFIT_CHK_INFO  where branch_id='00000000000019';

  v_count        number(19);
  v_Work_Date    Varchar2(8) := To_Char(Sysdate, 'yyyymmdd');
  v_Proc_Type    Varchar(10) := '';
  v_Proc_Name    Varchar2(50) := 'proc2020D_TBL_BENEFIT_CHK_INFO';
  v_Step_No      Varchar2(10);
  v_Step_Desc    Varchar2(200);
  v_Begin_Time   Date;
  v_End_Time     Date;
  v_Sql_Code     Varchar2(10);
  v_Sql_Errm     Varchar2(200);
  v_Execute_Flag Varchar2(10);

BEGIN
  v_count      := 0;
  v_Begin_Time := sysdate;

  OPEN c_cur;

  LOOP
    FETCH c_cur BULK COLLECT
      INTO benefit_rec LIMIT 10000;

    EXIT WHEN benefit_rec.count = 0;


    FOR i IN benefit_rec.first .. benefit_rec.last LOOP

       v_Step_No   := 'step_1';
       v_Step_Desc := '保单号:' ||  benefit_rec(i).policyno_id || '险种:' ||  benefit_rec(i).class_code ||
                   '责任:' ||  benefit_rec(i).del_code || '子码:' ||  benefit_rec(i).type_no ||
                   '期次:' ||  benefit_rec(i).del_num;

   delete from TBL_BENEFIT_CHK_INFO  a
     WHERE a.policyno_id = benefit_rec(i)
      .policyno_id
         AND a.class_code = benefit_rec(i)
      .class_code
         AND a.del_code = benefit_rec(i)
      .del_code
         AND a.type_no = benefit_rec(i)
      .type_no
         AND a.del_num = benefit_rec(i).del_num;

       v_count := v_count + 1;
      COMMIT;

    END LOOP;

  END LOOP;
  CLOSE c_cur;

  v_End_Time := sysdate;
  v_Step_Desc :='结束';
  v_Execute_Flag := 'y';
  p_Pub_Insert_Log(v_Work_Date,
                   v_Proc_Type,
                   v_Proc_Name,
                   v_Step_No,
                   v_Step_Desc,
                   v_count,
                   v_Sql_Code,
                   v_Sql_Errm,
                   v_Execute_Flag,
                   v_Begin_Time,
                   v_End_Time);
  Exception
  When Others Then
    If c_cur%Isopen Then
      Close c_cur;
    End If;
    Rollback;
    v_Sql_Code     := Sqlcode;
    v_Sql_Errm     := Trim(Sqlerrm);
    v_End_Time     := Sysdate;
    v_Execute_Flag := 'n';
    p_Pub_Insert_Log(v_Work_Date,
                     v_Proc_Type,
                     v_Proc_Name,
                     v_Step_No,
                     v_Step_Desc,
                     v_count,
                     v_Sql_Code,
                     v_Sql_Errm,
                     v_Execute_Flag,
                     v_Begin_Time,
                     v_End_Time);
    close c_cur;
end;
//

CREATE OR REPLACE PROCEDURE PPS."PROC2020D_TBL_BEN_NOTI_INFO" is

  TYPE BenefitTab IS TABLE OF TBL_BEN_NOTI_INFO%ROWTYPE;
  benefit_rec BenefitTab;

  CURSOR c_cur IS
    select * from TBL_BEN_NOTI_INFO  where branch_id='00000000000019';

  v_count        number(19);
  v_Work_Date    Varchar2(8) := To_Char(Sysdate, 'yyyymmdd');
  v_Proc_Type    Varchar(10) := '';
  v_Proc_Name    Varchar2(50) := 'proc2020D_TBL_BEN_NOTI_INFO';
  v_Step_No      Varchar2(10);
  v_Step_Desc    Varchar2(200);
  v_Begin_Time   Date;
  v_End_Time     Date;
  v_Sql_Code     Varchar2(10);
  v_Sql_Errm     Varchar2(200);
  v_Execute_Flag Varchar2(10);

BEGIN
  v_count      := 0;
  v_Begin_Time := sysdate;

  OPEN c_cur;

  LOOP
    FETCH c_cur BULK COLLECT
      INTO benefit_rec LIMIT 10000;

    EXIT WHEN benefit_rec.count = 0;


    FOR i IN benefit_rec.first .. benefit_rec.last LOOP

       v_Step_No   := 'step_1';
       v_Step_Desc := '保单号:' ||  benefit_rec(i).policyno_id || '险种:' ||  benefit_rec(i).class_code ||
                   '责任:' ||  benefit_rec(i).del_code || '子码:' ||  benefit_rec(i).type_no ||
                   '期次:' ||  benefit_rec(i).del_num || '通知方式:' ||  benefit_rec(i).NOTI_TYPE;

  delete from TBL_BEN_NOTI_INFO  a
     WHERE a.policyno_id = benefit_rec(i)
      .policyno_id
         AND a.class_code = benefit_rec(i)
      .class_code
         AND a.del_code = benefit_rec(i)
      .del_code
         AND a.type_no = benefit_rec(i)
      .type_no
         AND a.del_num = benefit_rec(i).del_num
   AND a.NOTI_TYPE = benefit_rec(i).NOTI_TYPE;
       v_count := v_count + 1;
      COMMIT;

    END LOOP;

  END LOOP;
  CLOSE c_cur;

  v_End_Time := sysdate;
  v_Step_Desc :='结束';
  v_Execute_Flag := 'y';
  p_Pub_Insert_Log(v_Work_Date,
                   v_Proc_Type,
                   v_Proc_Name,
                   v_Step_No,
                   v_Step_Desc,
                   v_count,
                   v_Sql_Code,
                   v_Sql_Errm,
                   v_Execute_Flag,
                   v_Begin_Time,
                   v_End_Time);
  Exception
  When Others Then
    If c_cur%Isopen Then
      Close c_cur;
    End If;
    Rollback;
    v_Sql_Code     := Sqlcode;
    v_Sql_Errm     := Trim(Sqlerrm);
    v_End_Time     := Sysdate;
    v_Execute_Flag := 'n';
    p_Pub_Insert_Log(v_Work_Date,
                     v_Proc_Type,
                     v_Proc_Name,
                     v_Step_No,
                     v_Step_Desc,
                     v_count,
                     v_Sql_Code,
                     v_Sql_Errm,
                     v_Execute_Flag,
                     v_Begin_Time,
                     v_End_Time);
    close c_cur;
end;
//

CREATE OR REPLACE PROCEDURE PPS."PROC2020D_TBL_CUST_BEN_INFO" is

  TYPE BenefitTab IS TABLE OF TBL_CUST_BEN_INFO%ROWTYPE;
  benefit_rec BenefitTab;

  CURSOR c_cur IS
    select * from TBL_CUST_BEN_INFO  where branch_id='00000000000019';

  v_count        number(19);
  v_Work_Date    Varchar2(8) := To_Char(Sysdate, 'yyyymmdd');
  v_Proc_Type    Varchar(10) := '';
  v_Proc_Name    Varchar2(50) := 'proc2020D_TBL_CUST_BEN_INFO';
  v_Step_No      Varchar2(10);
  v_Step_Desc    Varchar2(200);
  v_Begin_Time   Date;
  v_End_Time     Date;
  v_Sql_Code     Varchar2(10);
  v_Sql_Errm     Varchar2(200);
  v_Execute_Flag Varchar2(10);

BEGIN
  v_count      := 0;
  v_Begin_Time := sysdate;

  OPEN c_cur;

  LOOP
    FETCH c_cur BULK COLLECT
      INTO benefit_rec LIMIT 10000;

    EXIT WHEN benefit_rec.count = 0;


    FOR i IN benefit_rec.first .. benefit_rec.last LOOP

       v_Step_No   := 'step_1';
       v_Step_Desc := '保单号:' ||  benefit_rec(i).policyno_id || '险种:' ||  benefit_rec(i).class_code ||
                   '责任:' ||  benefit_rec(i).del_code || '子码:' ||  benefit_rec(i).type_no ;

  delete from TBL_CUST_BEN_INFO  a
     WHERE a.policyno_id = benefit_rec(i)
      .policyno_id
         AND a.class_code = benefit_rec(i)
      .class_code
         AND a.del_code = benefit_rec(i)
      .del_code
         AND a.type_no = benefit_rec(i)
      .type_no;
       v_count := v_count + 1;
      COMMIT;

    END LOOP;

  END LOOP;
  CLOSE c_cur;

  v_End_Time := sysdate;
  v_Step_Desc :='结束';
  v_Execute_Flag := 'y';
  p_Pub_Insert_Log(v_Work_Date,
                   v_Proc_Type,
                   v_Proc_Name,
                   v_Step_No,
                   v_Step_Desc,
                   v_count,
                   v_Sql_Code,
                   v_Sql_Errm,
                   v_Execute_Flag,
                   v_Begin_Time,
                   v_End_Time);
  Exception
  When Others Then
    If c_cur%Isopen Then
      Close c_cur;
    End If;
    Rollback;
    v_Sql_Code     := Sqlcode;
    v_Sql_Errm     := Trim(Sqlerrm);
    v_End_Time     := Sysdate;
    v_Execute_Flag := 'n';
    p_Pub_Insert_Log(v_Work_Date,
                     v_Proc_Type,
                     v_Proc_Name,
                     v_Step_No,
                     v_Step_Desc,
                     v_count,
                     v_Sql_Code,
                     v_Sql_Errm,
                     v_Execute_Flag,
                     v_Begin_Time,
                     v_End_Time);
    close c_cur;
end;
//

CREATE OR REPLACE PROCEDURE PPS."PROC2020I_BENEFIT_INFO_COUNT" is

  type benefit_tab is table of TBL_BENEFIT_INFO_COUNT_2020bk%rowtype;

  benefit_rec benefit_tab;

  cursor benefit_cur is
    select * from TBL_BENEFIT_INFO_COUNT_2020bk;

  v_count        number(19);
  v_Work_Date    Varchar2(8) := To_Char(Sysdate, 'yyyymmdd');
  v_Proc_Type    Varchar(10) := '';
  v_Proc_Name    Varchar2(50) := 'proc2020I_BENEFIT_INFO_COUNT';
  v_Step_No      Varchar2(10);
  v_Step_Desc    Varchar2(200);
  v_Begin_Time   Date;
  v_End_Time     Date;
  v_Sql_Code     Varchar2(10);
  v_Sql_Errm     Varchar2(200);
  v_Execute_Flag Varchar2(10);

begin
  v_count      := 0;
  v_Begin_Time := sysdate;
  open benefit_cur;

  loop
    fetch benefit_cur bulk collect
      into benefit_rec limit 10000;

    -- dbms_output.put_line(benefit_cur%ROWCOUNT);
    -- dbms_output.put_line(benefit_rec.first || ' ' || benefit_rec.last);
    FOR i IN benefit_rec.first .. benefit_rec.last LOOP

     v_Step_No   := 'step_1';
       v_Step_Desc := '保单号:' ||  benefit_rec(i).policyno_id || '险种:' ||  benefit_rec(i).class_code ||
                   '责任:' ||  benefit_rec(i).del_code || '子码:' ||  benefit_rec(i).type_no ||
                   '期次:' ||  benefit_rec(i).del_num;

      insert into TBL_BENEFIT_INFO_COUNT values benefit_rec (i);

     v_count := v_count + 1;
      commit;
    end loop;
    exit when benefit_cur%NOTFOUND;
  end loop;
  close benefit_cur;

  v_End_Time := sysdate;
  v_Step_Desc :='结束';
  v_Execute_Flag := 'y';
  p_Pub_Insert_Log(v_Work_Date,
                   v_Proc_Type,
                   v_Proc_Name,
                   v_Step_No,
                   v_Step_Desc,
                   v_count,
                   v_Sql_Code,
                   v_Sql_Errm,
                   v_Execute_Flag,
                   v_Begin_Time,
                   v_End_Time);
Exception
  When Others Then
    If benefit_cur%Isopen Then
      dbms_output.put_line(sqlcode);
      Close benefit_cur;
    End If;
  Rollback;
   v_Sql_Code     := Sqlcode;
    v_Sql_Errm     := Trim(Sqlerrm);
    v_End_Time     := Sysdate;
    v_Execute_Flag := 'n';
    p_Pub_Insert_Log(v_Work_Date,
                     v_Proc_Type,
                     v_Proc_Name,
                     v_Step_No,
                     v_Step_Desc,
                     v_count,
                     v_Sql_Code,
                     v_Sql_Errm,
                     v_Execute_Flag,
                     v_Begin_Time,
                     v_End_Time);


end;
//

CREATE OR REPLACE PROCEDURE PPS."PROC2020I_D01_RISKCON" is

  type benefit_tab is table of D01_RISKCON_2020bk%rowtype;

  benefit_rec benefit_tab;

  cursor benefit_cur is
    select * from D01_RISKCON_2020bk;

  v_count        number(19);
  v_Work_Date    Varchar2(8) := To_Char(Sysdate, 'yyyymmdd');
  v_Proc_Type    Varchar(10) := '';
  v_Proc_Name    Varchar2(50) := 'proc2020I_D01_RISKCON';
  v_Step_No      Varchar2(10);
  v_Step_Desc    Varchar2(200);
  v_Begin_Time   Date;
  v_End_Time     Date;
  v_Sql_Code     Varchar2(10);
  v_Sql_Errm     Varchar2(200);
  v_Execute_Flag Varchar2(10);

begin
  v_count      := 0;
  v_Begin_Time := sysdate;
  open benefit_cur;

  loop
    fetch benefit_cur bulk collect
      into benefit_rec limit 10000;

    -- dbms_output.put_line(benefit_cur%ROWCOUNT);
    -- dbms_output.put_line(benefit_rec.first || ' ' || benefit_rec.last);
    FOR i IN benefit_rec.first .. benefit_rec.last LOOP

     v_Step_No   := 'step_1';
       v_Step_Desc := '保单号:' ||  benefit_rec(i).policyno || '险种:' ||  benefit_rec(i).classcode;

      insert into D01_RISKCON values benefit_rec (i);

     v_count := v_count + 1;
      commit;
    end loop;
    exit when benefit_cur%NOTFOUND;
  end loop;
  close benefit_cur;

  v_End_Time := sysdate;
  v_Step_Desc :='结束';
  v_Execute_Flag := 'y';
  p_Pub_Insert_Log(v_Work_Date,
                   v_Proc_Type,
                   v_Proc_Name,
                   v_Step_No,
                   v_Step_Desc,
                   v_count,
                   v_Sql_Code,
                   v_Sql_Errm,
                   v_Execute_Flag,
                   v_Begin_Time,
                   v_End_Time);
Exception
  When Others Then
    If benefit_cur%Isopen Then
      dbms_output.put_line(sqlcode);
      Close benefit_cur;
    End If;
  Rollback;
   v_Sql_Code     := Sqlcode;
    v_Sql_Errm     := Trim(Sqlerrm);
    v_End_Time     := Sysdate;
    v_Execute_Flag := 'n';
    p_Pub_Insert_Log(v_Work_Date,
                     v_Proc_Type,
                     v_Proc_Name,
                     v_Step_No,
                     v_Step_Desc,
                     v_count,
                     v_Sql_Code,
                     v_Sql_Errm,
                     v_Execute_Flag,
                     v_Begin_Time,
                     v_End_Time);


end;
//


CREATE OR REPLACE PROCEDURE PPS."PROC2020I_FAIL_BENEFIT_INFO" is

  type benefit_tab is table of TBL_FAIL_BENEFIT_INFO_2020bk%rowtype;

  benefit_rec benefit_tab;

  cursor benefit_cur is
    select * from TBL_FAIL_BENEFIT_INFO_2020bk;

  v_count        number(19);
  v_Work_Date    Varchar2(8) := To_Char(Sysdate, 'yyyymmdd');
  v_Proc_Type    Varchar(10) := '';
  v_Proc_Name    Varchar2(50) := 'proc2020I_FAIL_BENEFIT_INFO';
  v_Step_No      Varchar2(10);
  v_Step_Desc    Varchar2(200);
  v_Begin_Time   Date;
  v_End_Time     Date;
  v_Sql_Code     Varchar2(10);
  v_Sql_Errm     Varchar2(200);
  v_Execute_Flag Varchar2(10);

begin
  v_count      := 0;
  v_Begin_Time := sysdate;
  open benefit_cur;

  loop
    fetch benefit_cur bulk collect
      into benefit_rec limit 10000;

    -- dbms_output.put_line(benefit_cur%ROWCOUNT);
    -- dbms_output.put_line(benefit_rec.first || ' ' || benefit_rec.last);
    FOR i IN benefit_rec.first .. benefit_rec.last LOOP

    v_Step_No   := 'step_1';
       v_Step_Desc := '保单号:' ||  benefit_rec(i).policyno_id || '险种:' ||  benefit_rec(i).class_code ||
                   '责任:' ||  benefit_rec(i).del_code || '子码:' ||  benefit_rec(i).type_no ||
                   '期次:' ||  benefit_rec(i).del_num;

      insert into TBL_FAIL_BENEFIT_INFO values benefit_rec (i);

    v_count := v_count + 1;
      commit;
    end loop;
    exit when benefit_cur%NOTFOUND;
  end loop;
  close benefit_cur;

  v_End_Time := sysdate;
  v_Step_Desc :='结束';
  v_Execute_Flag := 'y';
  p_Pub_Insert_Log(v_Work_Date,
                   v_Proc_Type,
                   v_Proc_Name,
                   v_Step_No,
                   v_Step_Desc,
                   v_count,
                   v_Sql_Code,
                   v_Sql_Errm,
                   v_Execute_Flag,
                   v_Begin_Time,
                   v_End_Time);
Exception
  When Others Then
    If benefit_cur%Isopen Then
      dbms_output.put_line(sqlcode);
      Close benefit_cur;
    End If;
 Rollback;
  v_Sql_Code     := Sqlcode;
    v_Sql_Errm     := Trim(Sqlerrm);
    v_End_Time     := Sysdate;
    v_Execute_Flag := 'n';
    p_Pub_Insert_Log(v_Work_Date,
                     v_Proc_Type,
                     v_Proc_Name,
                     v_Step_No,
                     v_Step_Desc,
                     v_count,
                     v_Sql_Code,
                     v_Sql_Errm,
                     v_Execute_Flag,
                     v_Begin_Time,
                     v_End_Time);


end;
//

CREATE OR REPLACE PROCEDURE PPS."PROC2020I_JOB_PAYDIFFERENCE" is

  type benefit_tab is table of JOB_PAYDIFFERENCE_2020bk%rowtype;

  benefit_rec benefit_tab;

  cursor benefit_cur is
    select * from JOB_PAYDIFFERENCE_2020bk;

  v_count        number(19);
  v_Work_Date    Varchar2(8) := To_Char(Sysdate, 'yyyymmdd');
  v_Proc_Type    Varchar(10) := '';
  v_Proc_Name    Varchar2(50) := 'proc2020I_JOB_PAYDIFFERENCE';
  v_Step_No      Varchar2(10);
  v_Step_Desc    Varchar2(200);
  v_Begin_Time   Date;
  v_End_Time     Date;
  v_Sql_Code     Varchar2(10);
  v_Sql_Errm     Varchar2(200);
  v_Execute_Flag Varchar2(10);

begin
  v_count      := 0;
  v_Begin_Time := sysdate;
  open benefit_cur;

  loop
    fetch benefit_cur bulk collect
      into benefit_rec limit 10000;

    -- dbms_output.put_line(benefit_cur%ROWCOUNT);
    -- dbms_output.put_line(benefit_rec.first || ' ' || benefit_rec.last);
    FOR i IN benefit_rec.first .. benefit_rec.last LOOP

    v_Step_No   := 'step_1';
       v_Step_Desc := '保单号:' ||  benefit_rec(i).policyno || '险种:' ||  benefit_rec(i).classcode ||
                   '责任:' ||  benefit_rec(i).delcode || '子码:' ||  benefit_rec(i).typeno ||
                   '期次:' ||  benefit_rec(i).delnum;

      insert into JOB_PAYDIFFERENCE values benefit_rec (i);

    v_count := v_count + 1;
      commit;
    end loop;
    exit when benefit_cur%NOTFOUND;
  end loop;
  close benefit_cur;

  v_End_Time := sysdate;
  v_Step_Desc :='结束';
  v_Execute_Flag := 'y';
  p_Pub_Insert_Log(v_Work_Date,
                   v_Proc_Type,
                   v_Proc_Name,
                   v_Step_No,
                   v_Step_Desc,
                   v_count,
                   v_Sql_Code,
                   v_Sql_Errm,
                   v_Execute_Flag,
                   v_Begin_Time,
                   v_End_Time);
Exception
  When Others Then
    If benefit_cur%Isopen Then
      dbms_output.put_line(sqlcode);
      Close benefit_cur;
    End If;
 Rollback;
  v_Sql_Code     := Sqlcode;
    v_Sql_Errm     := Trim(Sqlerrm);
    v_End_Time     := Sysdate;
    v_Execute_Flag := 'n';
    p_Pub_Insert_Log(v_Work_Date,
                     v_Proc_Type,
                     v_Proc_Name,
                     v_Step_No,
                     v_Step_Desc,
                     v_count,
                     v_Sql_Code,
                     v_Sql_Errm,
                     v_Execute_Flag,
                     v_Begin_Time,
                     v_End_Time);


end;
//

CREATE OR REPLACE PROCEDURE PPS."PROC2020I_JOB_POLICYNOPAY" is

  type benefit_tab is table of JOB_POLICYNOPAY_2020bk%rowtype;

  benefit_rec benefit_tab;

  cursor benefit_cur is
    select * from JOB_POLICYNOPAY_2020bk;

  v_count        number(19);
  v_Work_Date    Varchar2(8) := To_Char(Sysdate, 'yyyymmdd');
  v_Proc_Type    Varchar(10) := '';
  v_Proc_Name    Varchar2(50) := 'proc2020I_JOB_POLICYNOPAY';
  v_Step_No      Varchar2(10);
  v_Step_Desc    Varchar2(200);
  v_Begin_Time   Date;
  v_End_Time     Date;
  v_Sql_Code     Varchar2(10);
  v_Sql_Errm     Varchar2(200);
  v_Execute_Flag Varchar2(10);

begin
  v_count      := 0;
  v_Begin_Time := sysdate;
  open benefit_cur;

  loop
    fetch benefit_cur bulk collect
      into benefit_rec limit 10000;

    -- dbms_output.put_line(benefit_cur%ROWCOUNT);
    -- dbms_output.put_line(benefit_rec.first || ' ' || benefit_rec.last);
    FOR i IN benefit_rec.first .. benefit_rec.last LOOP

    v_Step_No   := 'step_1';
       v_Step_Desc := '保单号:' ||  benefit_rec(i).policyno || '险种:' ||  benefit_rec(i).classcode ||
                   '责任:' ||  benefit_rec(i).delcode || '子码:' ||  benefit_rec(i).typeno;

      insert into JOB_POLICYNOPAY values benefit_rec (i);

    v_count := v_count + 1;
      commit;
    end loop;
    exit when benefit_cur%NOTFOUND;
  end loop;
  close benefit_cur;

  v_End_Time := sysdate;
  v_Step_Desc :='结束';
  v_Execute_Flag := 'y';
  p_Pub_Insert_Log(v_Work_Date,
                   v_Proc_Type,
                   v_Proc_Name,
                   v_Step_No,
                   v_Step_Desc,
                   v_count,
                   v_Sql_Code,
                   v_Sql_Errm,
                   v_Execute_Flag,
                   v_Begin_Time,
                   v_End_Time);
Exception
  When Others Then
    If benefit_cur%Isopen Then
      dbms_output.put_line(sqlcode);
      Close benefit_cur;
    End If;
 Rollback;
  v_Sql_Code     := Sqlcode;
    v_Sql_Errm     := Trim(Sqlerrm);
    v_End_Time     := Sysdate;
    v_Execute_Flag := 'n';
    p_Pub_Insert_Log(v_Work_Date,
                     v_Proc_Type,
                     v_Proc_Name,
                     v_Step_No,
                     v_Step_Desc,
                     v_count,
                     v_Sql_Code,
                     v_Sql_Errm,
                     v_Execute_Flag,
                     v_Begin_Time,
                     v_End_Time);


end;
//

CREATE OR REPLACE PROCEDURE PPS."PROC2020I_TBL_BENEFIT_CHK_INFO" is

  type benefit_tab is table of TBL_BENEFIT_CHK_INFO_2020bk%rowtype;

  benefit_rec benefit_tab;

  cursor benefit_cur is
    select * from TBL_BENEFIT_CHK_INFO_2020bk;

  v_count        number(19);
  v_Work_Date    Varchar2(8) := To_Char(Sysdate, 'yyyymmdd');
  v_Proc_Type    Varchar(10) := '';
  v_Proc_Name    Varchar2(50) := 'proc2020I_TBL_BENEFIT_CHK_INFO';
  v_Step_No      Varchar2(10);
  v_Step_Desc    Varchar2(200);
  v_Begin_Time   Date;
  v_End_Time     Date;
  v_Sql_Code     Varchar2(10);
  v_Sql_Errm     Varchar2(200);
  v_Execute_Flag Varchar2(10);

begin
  v_count      := 0;
  v_Begin_Time := sysdate;
  open benefit_cur;

  loop
    fetch benefit_cur bulk collect
      into benefit_rec limit 10000;

    -- dbms_output.put_line(benefit_cur%ROWCOUNT);
    -- dbms_output.put_line(benefit_rec.first || ' ' || benefit_rec.last);
    FOR i IN benefit_rec.first .. benefit_rec.last LOOP

    v_Step_No   := 'step_1';
       v_Step_Desc := '保单号:' ||  benefit_rec(i).policyno_id || '险种:' ||  benefit_rec(i).class_code ||
                   '责任:' ||  benefit_rec(i).del_code || '子码:' ||  benefit_rec(i).type_no ||
                   '期次:' ||  benefit_rec(i).del_num;

      insert into TBL_BENEFIT_CHK_INFO values benefit_rec (i);

    v_count := v_count + 1;
      commit;
    end loop;
    exit when benefit_cur%NOTFOUND;
  end loop;
  close benefit_cur;

  v_End_Time := sysdate;
  v_Step_Desc :='结束';
  v_Execute_Flag := 'y';
  p_Pub_Insert_Log(v_Work_Date,
                   v_Proc_Type,
                   v_Proc_Name,
                   v_Step_No,
                   v_Step_Desc,
                   v_count,
                   v_Sql_Code,
                   v_Sql_Errm,
                   v_Execute_Flag,
                   v_Begin_Time,
                   v_End_Time);
Exception
  When Others Then
    If benefit_cur%Isopen Then
      dbms_output.put_line(sqlcode);
      Close benefit_cur;
    End If;
 Rollback;
  v_Sql_Code     := Sqlcode;
    v_Sql_Errm     := Trim(Sqlerrm);
    v_End_Time     := Sysdate;
    v_Execute_Flag := 'n';
    p_Pub_Insert_Log(v_Work_Date,
                     v_Proc_Type,
                     v_Proc_Name,
                     v_Step_No,
                     v_Step_Desc,
                     v_count,
                     v_Sql_Code,
                     v_Sql_Errm,
                     v_Execute_Flag,
                     v_Begin_Time,
                     v_End_Time);


end;
//

CREATE OR REPLACE PROCEDURE PPS."PROC2020I_TBL_BEN_NOTI_INFO" is

  type benefit_tab is table of TBL_BEN_NOTI_INFO_2020bk%rowtype;

  benefit_rec benefit_tab;

  cursor benefit_cur is
    select * from TBL_BEN_NOTI_INFO_2020bk;

  v_count        number(19);
  v_Work_Date    Varchar2(8) := To_Char(Sysdate, 'yyyymmdd');
  v_Proc_Type    Varchar(10) := '';
  v_Proc_Name    Varchar2(50) := 'proc2020I_TBL_BEN_NOTI_INFO';
  v_Step_No      Varchar2(10);
  v_Step_Desc    Varchar2(200);
  v_Begin_Time   Date;
  v_End_Time     Date;
  v_Sql_Code     Varchar2(10);
  v_Sql_Errm     Varchar2(200);
  v_Execute_Flag Varchar2(10);

begin
  v_count      := 0;
  v_Begin_Time := sysdate;
  open benefit_cur;

  loop
    fetch benefit_cur bulk collect
      into benefit_rec limit 10000;

    -- dbms_output.put_line(benefit_cur%ROWCOUNT);
    -- dbms_output.put_line(benefit_rec.first || ' ' || benefit_rec.last);
    FOR i IN benefit_rec.first .. benefit_rec.last LOOP

     v_Step_No   := 'step_1';
       v_Step_Desc := '保单号:' ||  benefit_rec(i).policyno_id || '险种:' ||  benefit_rec(i).class_code ||
                   '责任:' ||  benefit_rec(i).del_code || '子码:' ||  benefit_rec(i).type_no ||
                   '期次:' ||  benefit_rec(i).del_num || '通知方式:' ||  benefit_rec(i).NOTI_TYPE;

      insert into TBL_BEN_NOTI_INFO values benefit_rec (i);

     v_count := v_count + 1;
      commit;
    end loop;
    exit when benefit_cur%NOTFOUND;
  end loop;
  close benefit_cur;

  v_End_Time := sysdate;
  v_Step_Desc :='结束';
  v_Execute_Flag := 'y';
  p_Pub_Insert_Log(v_Work_Date,
                   v_Proc_Type,
                   v_Proc_Name,
                   v_Step_No,
                   v_Step_Desc,
                   v_count,
                   v_Sql_Code,
                   v_Sql_Errm,
                   v_Execute_Flag,
                   v_Begin_Time,
                   v_End_Time);
Exception
  When Others Then
    If benefit_cur%Isopen Then
      dbms_output.put_line(sqlcode);
      Close benefit_cur;
    End If;
  Rollback;
   v_Sql_Code     := Sqlcode;
    v_Sql_Errm     := Trim(Sqlerrm);
    v_End_Time     := Sysdate;
    v_Execute_Flag := 'n';
    p_Pub_Insert_Log(v_Work_Date,
                     v_Proc_Type,
                     v_Proc_Name,
                     v_Step_No,
                     v_Step_Desc,
                     v_count,
                     v_Sql_Code,
                     v_Sql_Errm,
                     v_Execute_Flag,
                     v_Begin_Time,
                     v_End_Time);


end;
//

CREATE OR REPLACE PROCEDURE PPS."PROC2020I_TBL_CUST_BEN_INFO" is

  type benefit_tab is table of TBL_CUST_BEN_INFO_2020bk%rowtype;

  benefit_rec benefit_tab;

  cursor benefit_cur is
    select * from TBL_CUST_BEN_INFO_2020bk;

  v_count        number(19);
  v_Work_Date    Varchar2(8) := To_Char(Sysdate, 'yyyymmdd');
  v_Proc_Type    Varchar(10) := '';
  v_Proc_Name    Varchar2(50) := 'proc2020I_TBL_CUST_BEN_INFO';
  v_Step_No      Varchar2(10);
  v_Step_Desc    Varchar2(200);
  v_Begin_Time   Date;
  v_End_Time     Date;
  v_Sql_Code     Varchar2(10);
  v_Sql_Errm     Varchar2(200);
  v_Execute_Flag Varchar2(10);

begin
  v_count      := 0;
  v_Begin_Time := sysdate;
  open benefit_cur;

  loop
    fetch benefit_cur bulk collect
      into benefit_rec limit 10000;

    -- dbms_output.put_line(benefit_cur%ROWCOUNT);
    -- dbms_output.put_line(benefit_rec.first || ' ' || benefit_rec.last);
    FOR i IN benefit_rec.first .. benefit_rec.last LOOP

    v_Step_No   := 'step_1';
       v_Step_Desc := '保单号:' ||  benefit_rec(i).policyno_id || '险种:' ||  benefit_rec(i).class_code ||
                   '责任:' ||  benefit_rec(i).del_code || '子码:' ||  benefit_rec(i).type_no ;

      insert into TBL_CUST_BEN_INFO values benefit_rec (i);

    v_count := v_count + 1;
      commit;
    end loop;
    exit when benefit_cur%NOTFOUND;
  end loop;
  close benefit_cur;

  v_End_Time := sysdate;
  v_Step_Desc :='结束';
  v_Execute_Flag := 'y';
  p_Pub_Insert_Log(v_Work_Date,
                   v_Proc_Type,
                   v_Proc_Name,
                   v_Step_No,
                   v_Step_Desc,
                   v_count,
                   v_Sql_Code,
                   v_Sql_Errm,
                   v_Execute_Flag,
                   v_Begin_Time,
                   v_End_Time);
Exception
  When Others Then
    If benefit_cur%Isopen Then
      dbms_output.put_line(sqlcode);
      Close benefit_cur;
    End If;
 Rollback;
  v_Sql_Code     := Sqlcode;
    v_Sql_Errm     := Trim(Sqlerrm);
    v_End_Time     := Sysdate;
    v_Execute_Flag := 'n';
    p_Pub_Insert_Log(v_Work_Date,
                     v_Proc_Type,
                     v_Proc_Name,
                     v_Step_No,
                     v_Step_Desc,
                     v_count,
                     v_Sql_Code,
                     v_Sql_Errm,
                     v_Execute_Flag,
                     v_Begin_Time,
                     v_End_Time);


end;
//

CREATE OR REPLACE PROCEDURE PPS."PROC2020_D01_RISKCON" (i_Branchid In D01_RISKCON.BRANCH%Type)is
 cursor c_cur is
    select a.rowid
      from D01_RISKCON a
     where  a.branch = i_Branchid;


  v_count        number(19);
  v_Work_Date    Varchar2(8) := To_Char(Sysdate, 'yyyymmdd');
  v_Proc_Type    Varchar(10) := '';
  v_Proc_Name    Varchar2(50) := 'proc2020_D01_RISKCON';
  v_Step_No      Varchar2(10);
  v_Step_Desc    Varchar2(200);
  v_Begin_Time   Date;
  v_End_Time     Date;
  v_Sql_Code     Varchar2(10);
  v_Sql_Errm     Varchar2(200);
  v_Execute_Flag Varchar2(10);
  v_cur          c_cur%rowtype;
begin
  v_count      := 0;
  v_Begin_Time := sysdate;
  open c_cur;
  loop
    fetch c_cur
      into v_cur;
    exit when c_cur%notfound;

    v_Step_No   := 'step_1';
    v_Step_Desc := 'rowid:' || v_cur.rowid;

    update D01_RISKCON
       set branch = '00000000000019'
     where rowid = v_cur.rowid;

    -- dbms_output.put_line(sqlcode);
    v_count := v_count + 1;
    if mod(v_count, 10000) = 0 then
      commit;
    end if;
  end loop;
  commit;

  v_End_Time := sysdate;
  v_Step_Desc :='结束';
  v_Execute_Flag := 'y';
  p_Pub_Insert_Log(v_Work_Date,
                   v_Proc_Type,
                   v_Proc_Name,
                   v_Step_No,
                   v_Step_Desc,
                   v_count,
                   v_Sql_Code,
                   v_Sql_Errm,
                   v_Execute_Flag,
                   v_Begin_Time,
                   v_End_Time);

Exception
  When Others Then
    If c_cur%Isopen Then
      Close c_cur;
    End If;
    Rollback;
    v_Sql_Code     := Sqlcode;
    v_Sql_Errm     := Trim(Sqlerrm);
    v_End_Time     := Sysdate;
    v_Execute_Flag := 'n';
    p_Pub_Insert_Log(v_Work_Date,
                     v_Proc_Type,
                     v_Proc_Name,
                     v_Step_No,
                     v_Step_Desc,
                     v_count,
                     v_Sql_Code,
                     v_Sql_Errm,
                     v_Execute_Flag,
                     v_Begin_Time,
                     v_End_Time);
    close c_cur;
end;
//

CREATE OR REPLACE PROCEDURE PPS."PROC2020_JOB_PAYDETAILED" is
 cursor c_cur is
    select a.rowid
      from JOB_PAYDETAILED a
     where  a.branchid in ('00000000004511', '00000000004515',
            '00000000004519', '00000000004523');

  v_new_orgcode3 varchar2(14);
  v_new_orgcode2 varchar2(14);
  v_count        number(19);
  v_Work_Date    Varchar2(8) := To_Char(Sysdate, 'yyyymmdd');
  v_Proc_Type    Varchar(10) := '';
  v_Proc_Name    Varchar2(50) := 'proc2020_JOB_PAYDETAILED';
  v_Step_No      Varchar2(10);
  v_Step_Desc    Varchar2(200);
  v_Begin_Time   Date;
  v_End_Time     Date;
  v_Sql_Code     Varchar2(10);
  v_Sql_Errm     Varchar2(200);
  v_Execute_Flag Varchar2(10);
  v_cur          c_cur%rowtype;
begin
  v_count      := 0;
  v_Begin_Time := sysdate;
  open c_cur;
  loop
    fetch c_cur
      into v_cur;
    exit when c_cur%notfound;

    v_Step_No   := 'step_1';
    v_Step_Desc := 'rowid:' || v_cur.rowid;

    update JOB_PAYDETAILED
       set branchid = '00000000000019'
     where rowid = v_cur.rowid;

    -- dbms_output.put_line(sqlcode);
    v_count := v_count + 1;
    if mod(v_count, 10000) = 0 then
      commit;
    end if;
  end loop;
  commit;

  v_End_Time := sysdate;
  v_Step_Desc :='结束';
  v_Execute_Flag := 'y';
  p_Pub_Insert_Log(v_Work_Date,
                   v_Proc_Type,
                   v_Proc_Name,
                   v_Step_No,
                   v_Step_Desc,
                   v_count,
                   v_Sql_Code,
                   v_Sql_Errm,
                   v_Execute_Flag,
                   v_Begin_Time,
                   v_End_Time);

Exception
  When Others Then
    If c_cur%Isopen Then
      Close c_cur;
    End If;
    Rollback;
    v_Sql_Code     := Sqlcode;
    v_Sql_Errm     := Trim(Sqlerrm);
    v_End_Time     := Sysdate;
    v_Execute_Flag := 'n';
    p_Pub_Insert_Log(v_Work_Date,
                     v_Proc_Type,
                     v_Proc_Name,
                     v_Step_No,
                     v_Step_Desc,
                     v_count,
                     v_Sql_Code,
                     v_Sql_Errm,
                     v_Execute_Flag,
                     v_Begin_Time,
                     v_End_Time);
    close c_cur;
end;
//

CREATE OR REPLACE PROCEDURE PPS."PROC2020_TBL_IDS_DEBITREC" (i_Branchid In tbl_ids_debitrec.branch%Type)is
  CURSOR c_cur IS
    SELECT a.rowid
      FROM tbl_ids_debitrec a
     WHERE a.branch = i_Branchid;

  v_count        number(19);
  v_Work_Date    Varchar2(8) := To_Char(Sysdate, 'yyyymmdd');
  v_Proc_Type    Varchar(10) := '';
  v_Proc_Name    Varchar2(50) := 'proc2020_tbl_ids_debitrec';
  v_Step_No      Varchar2(10);
  v_Step_Desc    Varchar2(200);
  v_Begin_Time   Date;
  v_End_Time     Date;
  v_Sql_Code     Varchar2(10);
  v_Sql_Errm     Varchar2(200);
  v_Execute_Flag Varchar2(10);
  v_cur          c_cur%rowtype;

BEGIN
  v_count      := 0;
  v_Begin_Time := sysdate;

  OPEN c_cur;

  LOOP
    FETCH c_cur
      INTO v_cur;
    EXIT WHEN c_cur%notfound;

    v_Step_No   := 'step_1';
    v_Step_Desc := 'rowid:' || v_cur.rowid;

    UPDATE tbl_ids_debitrec a
         SET branch = '00000000000019'
       where rowid = v_cur.rowid;

    v_count := v_count + 1;
    if mod(v_count, 10000) = 0 then
      commit;
    end if;
  END LOOP;
  commit;
  CLOSE c_cur;

  v_End_Time := sysdate;
  v_Step_Desc :='结束';
  v_Execute_Flag := 'y';
  p_Pub_Insert_Log(v_Work_Date,
                   v_Proc_Type,
                   v_Proc_Name,
                   v_Step_No,
                   v_Step_Desc,
                   v_count,
                   v_Sql_Code,
                   v_Sql_Errm,
                   v_Execute_Flag,
                   v_Begin_Time,
                   v_End_Time);
  Exception
  When Others Then
    If c_cur%Isopen Then
      Close c_cur;
    End If;
    Rollback;
    v_Sql_Code     := Sqlcode;
    v_Sql_Errm     := Trim(Sqlerrm);
    v_End_Time     := Sysdate;
    v_Execute_Flag := 'n';
    p_Pub_Insert_Log(v_Work_Date,
                     v_Proc_Type,
                     v_Proc_Name,
                     v_Step_No,
                     v_Step_Desc,
                     v_count,
                     v_Sql_Code,
                     v_Sql_Errm,
                     v_Execute_Flag,
                     v_Begin_Time,
                     v_End_Time);
    close c_cur;
end;
//

CREATE OR REPLACE PROCEDURE PPS."PROC2020_TBL_IDS_MONEYSCH" (i_Branchid In tbl_ids_moneysch.Branch_Id%Type)is
 cursor c_cur is
    select a.rowid
      from tbl_ids_moneysch a
     where  a.branch_id = i_Branchid;


  v_count        number(19);
  v_Work_Date    Varchar2(8) := To_Char(Sysdate, 'yyyymmdd');
  v_Proc_Type    Varchar(10) := '';
  v_Proc_Name    Varchar2(50) := 'proc2020_tbl_ids_moneysch';
  v_Step_No      Varchar2(10);
  v_Step_Desc    Varchar2(200);
  v_Begin_Time   Date;
  v_End_Time     Date;
  v_Sql_Code     Varchar2(10);
  v_Sql_Errm     Varchar2(200);
  v_Execute_Flag Varchar2(10);
  v_cur          c_cur%rowtype;
begin
  v_count      := 0;
  v_Begin_Time := sysdate;
  open c_cur;
  loop
    fetch c_cur
      into v_cur;
    exit when c_cur%notfound;

    v_Step_No   := 'step_1';
    v_Step_Desc := 'rowid:' || v_cur.rowid;

    update tbl_ids_moneysch
       set branch_id = '00000000000019'
     where rowid = v_cur.rowid;

    -- dbms_output.put_line(sqlcode);
    v_count := v_count + 1;
    if mod(v_count, 10000) = 0 then
      commit;
    end if;
  end loop;
  commit;

  v_End_Time := sysdate;
  v_Step_Desc :='结束';
  v_Execute_Flag := 'y';
  p_Pub_Insert_Log(v_Work_Date,
                   v_Proc_Type,
                   v_Proc_Name,
                   v_Step_No,
                   v_Step_Desc,
                   v_count,
                   v_Sql_Code,
                   v_Sql_Errm,
                   v_Execute_Flag,
                   v_Begin_Time,
                   v_End_Time);

Exception
  When Others Then
    If c_cur%Isopen Then
      Close c_cur;
    End If;
    Rollback;
    v_Sql_Code     := Sqlcode;
    v_Sql_Errm     := Trim(Sqlerrm);
    v_End_Time     := Sysdate;
    v_Execute_Flag := 'n';
    p_Pub_Insert_Log(v_Work_Date,
                     v_Proc_Type,
                     v_Proc_Name,
                     v_Step_No,
                     v_Step_Desc,
                     v_count,
                     v_Sql_Code,
                     v_Sql_Errm,
                     v_Execute_Flag,
                     v_Begin_Time,
                     v_End_Time);
    close c_cur;
end;
//

CREATE OR REPLACE PROCEDURE PPS."PROC20210707"
AS
type sno_type is table of test20210707.sno%type;
type name_type is table of  test20210707.name%type;

sno1 sno_type;
name1 name_type;

cursor my_cur is select sno,name from test20210707;
BEGIN
        open my_cur;
        loop
        fetch my_cur bulk collect into sno1,name1 limit 2;
        Exit When sno1.count = 0;
        forall i in 1 .. sno1.count
        Execute immediate 'INSERT INTO test20210708 values(:1,:2)'
        using sno1(i),name1(i) ;
        commit;
        end loop;
        close my_cur;
END;
//

CREATE OR REPLACE PROCEDURE PPS."PROC_TRUNCATE_TABLE" (i_Table_Name In Varchar2) Is
Begin
   If Lower(i_Table_Name) = 'pre_cdelrec' Then
      Execute Immediate 'TRUNCATE TABLE JFPT.TBL_BENEFIT_INFO_TMP';
   Else
      Execute Immediate 'TRUNCATE TABLE jfpt.tbl_mid_' ||
                        Lower(i_Table_Name);
   End If;
Exception
   When Others Then
      p_Pub_Insert_Log(To_Char(Sysdate, 'yyyymmdd'),
                       '',
                       'Proc_Truncate_Table',
                       '',
                       '模型' || Lower(i_Table_Name) || ' truncate失败',
                       '',
                       Sqlcode,
                       Substr(Trim(Sqlerrm), 1, 1000),
                       'n',
                       '',
                       '');
End;
//

CREATE OR REPLACE PROCEDURE PPS."SP_EXCEPTION" (pexp_name in JOB_exception.sp_name%type  )
  is
   v_sqlstr job_exception.exp_des%type;
   POSITION job_exception.row_count%type ;

   l_owner varchar2(100);
   l_spname varchar2(100);
   l_lineno number;
   l_caller_t varchar2(100);

begin
  rollback;
/*
owa_util.who_called_me系统包不支持yd
*/
  --owa_util.who_called_me(owner => l_owner, name => l_spname, lineno => l_lineno, caller_t => l_caller_t);


  v_sqlstr := DBMS_UTILITY.FORMAT_ERROR_BACKTRACE ;
  --DBMS_SQL.LAST_ERROR_POSITION不支持 yd
  --POSITION:=DBMS_SQL.LAST_ERROR_POSITION;
  POSITION:=1;

  dbms_output.put_line(v_sqlstr||' sqlcode:'||sqlcode||',  sqlerrm:'||sqlerrm);

  -- 错误信息记录
  insert into job_exception(SP_NAME , exp_NAME , exp_DES , row_count , exp_time)
      values(l_spname, pexp_name, v_sqlstr, POSITION, sysdate);

  commit;
end sp_exception;
//

CREATE OR REPLACE PROCEDURE PPS."PS_JOB_IDS_LOG" (tab_name  varchar2,
                                              branch    varchar2,
                                              log_des   varchar2,
                                              row_count number) is

  l_owner    varchar2(100);
  l_spname   varchar2(100);
  l_lineno   number(22);
  l_caller_t varchar2(100);
begin
/*
owa_util.who_called_me系统包不支持yd
*/
/*
  owa_util.who_called_me(owner    => l_owner,
                         name     => l_spname,
                         lineno   => l_lineno,
                         caller_t => l_caller_t);
*/
  insert into job_ids_log
    (sno, sp_name, line, tab_name, branch, log_des, row_count, log_time)
  values
    (SEQ_job_ids_log_sno.nextval,
     l_spname,
     l_lineno,
     tab_name,
     branch,
     log_des,
     row_count,
     sysdate);
  commit;
exception
  when others then
    pps.sp_exception(to_char(sqlcode) || sqlerrm);
end ps_job_ids_log;
//

CREATE OR REPLACE PROCEDURE PPS."SP_CUST_INFO_DML" (i_Cust_Id      Varchar2
                                            ,i_Pid_Id       Varchar2
                                            ,i_Id_Type      Varchar2
                                            ,i_Cust_Name    Varchar2
                                            ,i_Sex_Flg      Varchar2
                                            ,i_Birth_Date   Varchar2
                                            ,i_Telphone     Varchar2
                                            ,i_Health_Sta   Varchar2
                                            ,i_Surv_Date    Varchar2
                                            ,i_Surv_Fre     Varchar2
                                            ,i_Branch_Id    Varchar2
                                            ,i_Sub_Pkey_Cd1 Varchar2) Is
   v_Count    Number(10);
   v_Sql_Code Varchar2(10);
   v_Sql_Errm Varchar2(200);
Begin
   Select Count(1)
     Into v_Count
     From Tbl_Cust_Info t
    Where t.Cust_Id = i_Cust_Id;

   If v_Count = 1 Then
      If i_Health_Sta = '02' Then
         Update Tbl_Cust_Info
            Set Pid_Id          = i_Pid_Id
               ,Id_Type         = i_Id_Type
               ,Cust_Name       = i_Cust_Name
               ,Sex_Flg         = i_Sex_Flg
               ,Birth_Date      = i_Birth_Date
               ,Telphone        = i_Telphone
               ,Health_Sta      = i_Health_Sta
               ,Branch_Id       = i_Branch_Id
               ,Sub_Pkey_Cd1    = i_Sub_Pkey_Cd1
               ,Last_Upd_Opr_Id = 'A002'
               ,Last_Upd_Txn_Id = 'A002'
               ,Last_Upd_Ts     = To_Char(Sysdate, 'yyyymmddhh24miss')
          Where Cust_Id = i_Cust_Id;
      Else
         Update Tbl_Cust_Info
            Set Pid_Id          = i_Pid_Id
               ,Id_Type         = i_Id_Type
               ,Cust_Name       = i_Cust_Name
               ,Sex_Flg         = i_Sex_Flg
               ,Birth_Date      = i_Birth_Date
               ,Telphone        = i_Telphone
               ,Health_Sta      = i_Health_Sta
               ,Branch_Id       = i_Branch_Id
               ,Surv_Date       = i_Surv_Date
               ,Surv_Fre        = i_Surv_Fre
               ,Last_Upd_Opr_Id = 'A002'
               ,Last_Upd_Txn_Id = 'A002'
               ,Last_Upd_Ts     = To_Char(Sysdate, 'yyyymmddhh24miss')
          Where Cust_Id = i_Cust_Id;
      End If;
   Else
      Insert Into Tbl_Cust_Info
         (Cust_Id
         ,Pid_Id
         ,Add_Id
         ,Id_Type
         ,Cust_Name
         ,Sex_Flg
         ,Birth_Date
         ,Telphone
         ,Vip_Type
         ,Health_Sta
         ,Surv_Date
         ,Surv_Fre
         ,Branch_Id
         ,Sub_Pkey_Cd1
         ,Sub_Pkey_Cd2
         ,Sub_Pkey_Cd3
         ,Misc_Tx
         ,Last_Upd_Opr_Id
         ,Last_Upd_Txn_Id
         ,Last_Upd_Ts)
      Values
         (i_Cust_Id
         ,i_Pid_Id
         ,''
         ,i_Id_Type
         ,i_Cust_Name
         ,i_Sex_Flg
         ,i_Birth_Date
         ,i_Telphone
         ,''
         ,i_Health_Sta
         ,i_Surv_Date
         ,i_Surv_Fre
         ,i_Branch_Id
         ,i_Sub_Pkey_Cd1
         ,''
         ,''
         ,''
         ,'A002'
         ,'A002'
         ,To_Char(Sysdate, 'yyyymmddhh24miss'));
   End If;

   Commit;
Exception
   When Others Then
      v_Sql_Code := Sqlcode;
      v_Sql_Errm := Trim(Sqlerrm);

      p_Pub_Insert_Log(To_Char(Sysdate, 'yyyymmdd'),
                       '',
                       'Sp_Cust_Info_Dml',
                       '',
                       '客户号:' || i_Cust_Id || ',有异常,请排查',
                       '',
                       v_Sql_Code,
                       v_Sql_Errm,
                       'n',
                       '',
                       '');
End;
//

CREATE OR REPLACE PROCEDURE PPS."SP_EXCEPTION_OTHER" (pexp_name in varchar2  )
  is


begin
dbms_output.put_line('');
end;
//

CREATE OR REPLACE PROCEDURE PPS."SP_JOB_IDS_LOG" (tab_name  varchar2,
                                              branch    varchar2,
                                              log_des   varchar2,
                                              row_count number) is
  /************************************************************************
  * Purpose : 为IDS加载程序添加日志,IDS加载专用,影响巨大,修改需仔细
  * Para.   : i_branch   branch or all
  * Version : 2015-06-01 Created  mxl
  *************************************************************************/
  l_owner    varchar2(100);
  l_spname   varchar2(100);
  l_lineno   number(22);
  l_caller_t varchar2(100);
begin
/*
owa_util.who_called_me系统包不支持yd
*/
/*
  owa_util.who_called_me(owner    => l_owner,
                         name     => l_spname,
                         lineno   => l_lineno,
                         caller_t => l_caller_t);
*/
  insert into job_ids_log
    (sno, sp_name, line, tab_name, branch, log_des, row_count, log_time)
  values
    (SEQ_job_ids_log_sno.nextval,
     l_spname,
     l_lineno,
     tab_name,
     branch,
     log_des,
     row_count,
     sysdate);
  commit;
exception
  when others then
    sp_exception(to_char(sqlcode) || sqlerrm);

end SP_job_ids_log;
//

CREATE OR REPLACE PROCEDURE PPS."SP_RISKCON1" (v_fgsdm in varchar2,v_fgsname in varchar2) is
   row_count1 integer;
   v_sql           varchar2(10000);
begin

-- 以POLICYNO ,CLASSCODE,BRANCH 作为删除规则
 /*日志信息-- 开始删除*/
   execute immediate 'truncate table job_temp_riskcon';
   commit;

    PS_job_ids_log('RISKCON', v_fgsdm, 'begin', null);

  INSERT INTO job_t_log
   values
  ('SP_RISKCON1',seq_job_t_log.nextval,v_fgsname||'_RISKCON开始执行数据删除',null,sysdate);
   v_sql := 'DELETE FROM '||v_fgsname||'_RISKCON A
    WHERE EXISTS (SELECT 1
             FROM JOB_IDS_RISKCON B
              WHERE a.policyno=b.policyno
              and a.classcode=b.classcode
              and b.branch = '||bmdm_const.v_char||v_fgsdm||bmdm_const.v_char||')';
   DBMS_OUTPUT.put_line(v_sql);
   execute immediate v_sql;

  row_count1 := sql%rowcount;
    /*日志信息-- 统计删除记录数*/
    INSERT INTO job_t_log
    values
      ('SP_RISKCON1', seq_job_t_log.nextval, '删除'||v_fgsname||'_RISKCON表信息的记录数', row_count1,
       sysdate);
    commit;

   /*日志信息-- 开始采集*/
  INSERT INTO job_t_log
   values
  ('SP_RISKCON1',seq_job_t_log.nextval,v_fgsname||'_RISKCON开始执行数据采集',null,sysdate);


   v_sql := 'INSERT INTO job_temp_riskcon
        (POLICYNO,POLIST,NPAYLEN,TMOUNT,PIECES,CLASSCODE,EMPNO,BEGDATE ,
         CONTNO ,FGSNO,PRELNAME, PID,JOB,APID,OPERNO ,STOPDAT,
         APPDATE,/*DELAGE,*/APPNO,APPF,PAYSEQ,UNSTDRATE,STDRATE ,SALEATTR,gpolicyno,etl_time,bankflag,idtype,
         AIDTYPE,APERSON_ID,BENPARAM,COMNUM,CSR_ID,CURRENCY,DCDM,
         DEL_DATE,DEL_TYPE,DESKPAY,DISCOUNT,EMPNO_ID,GCON_ID,ISCARD,
         OPDATE,OPER_ID,PERSON_ID,REASON,REG_CODE,RENEWDATE,RISKCON_ID,SPECAGR,RENEWID,
         SNO,TYPEID,SHARETYPE,BRANCH,BEGTIME,ENDTIME,SOUR_SYS,SRC_SYS,SALE_PROD_CODE,
         OWNER_SOURCE_ID,INSURED_SOURCE_ID,WORKNO,COMB_POLICY_NO,APP_AGE,SUB_AGT_NO,O_CLASSCODE,
         CROSS_SALE_IND,PREM_RATE_LEVEL ,ILL_SCORE,GROUP_NO ,DIGITAL_SIGN_IND,SUPPLY_INPUT_IND ,
         INI_INVOICE_STATUS,UNI_PAY_IND,APPOINTED_OPDATE_IND )
      SELECT  POLICYNO,POLIST,NPAYLEN,TMOUNT,PIECES,CLASSCODE,EMPNO,BEGDATE ,
       CSRNO ,'||bmdm_const.v_char||v_fgsname||bmdm_const.v_char||',PRELNAME,
       PID,JOB,APID,OPERNO ,STOPDATE,APPDATE,/*DELAGE,*/APPNO,APPF,PAYSEQ,
       UNSTDRATE,STDRATE ,trim(SALEATTR),gpolicyno,etl_time,bankflag,idtype,
       AIDTYPE,APERSON_ID,BENPARAM,COMNUM,CSR_ID,CURRENCY,DCDM,
       DEL_DATE,DEL_TYPE,DESKPAY,DISCOUNT,EMPNO_ID,GCON_ID,ISCARD,
       OPDATE,OPER_ID,PERSON_ID,REASON,REG_CODE,RENEWDATE,RISKCON_ID,SPECAGR,RENEWID,
       SNO,TYPEID,SHARETYPE,BRANCH,BEGTIME,ENDTIME,SOUR_SYS,SRC_SYS,SALE_PROD_CODE,
         OWNER_SOURCE_ID,INSURED_SOURCE_ID,WORKNO,COMB_POLICY_NO,APP_AGE,SUB_AGT_NO,O_CLASSCODE,
         CROSS_SALE_IND,PREM_RATE_LEVEL ,ILL_SCORE,GROUP_NO ,DIGITAL_SIGN_IND,SUPPLY_INPUT_IND ,
         INI_INVOICE_STATUS,UNI_PAY_IND,APPOINTED_OPDATE_IND
       FROM JOB_IDS_RISKCON a
    WHERE branch='||bmdm_const.v_char||v_fgsdm||bmdm_const.v_char||' and ENDTIME > SYSDATE';

    execute immediate v_sql;
    row_count1 := sql%rowcount;
    /*日志信息-- 统计采集记录数*/
    INSERT INTO job_t_log
    values
      ('SP_RISKCON1', seq_job_t_log.nextval, '插入RISKCON表信息的记录数', row_count1,sysdate);

     /*日志信息-- 完成数据采集*/
   INSERT INTO job_t_log
     values
    ('SP_RISKCON1', seq_job_t_log.nextval,v_fgsname||'_RISKCON正常完成数据采集', row_count1,sysdate);
     insert into job_m_dailylog(dailylogid,trandate,branchid,trantype,errinfor,tranflag,TYPEFLAG,trantime)
     values (SEQ_JOB_M_DAILYLOG_id.nextval,to_char(sysdate,'yyyymmdd'),v_fgsname||'0100','03',
     'SP_RISKCON1   插入'||v_fgsname||'_RISKCON表信息的记录数='||to_char(row_count1),'0','04',sysdate);
   commit;

   
    INSERT INTO job_t_log
    values
      ('SP_RISKCON1', seq_job_t_log.nextval, '更新'||v_fgsname||'_RISKCON表信息的记录数', row_count1,
       sysdate);
    commit;

   v_sql := 'INSERT INTO '||v_fgsname||'_RISKCON
                    (POLICYNO,POLIST,NPAYLEN,TMOUNT,PIECES,CLASSCODE,EMPNO,BEGDATE ,
                    CONTNO ,FGSNO,PRELNAME, PID,JOB,APID,OPERNO ,STOPDAT,recaddr,rectele,nretdate,payfmage,
                    APPDATE,DELAGE,APPNO,APPF,PAYSEQ,UNSTDRATE,STDRATE ,SALEATTR,
                    YEARNUM,PEDATE,PAYCODE,DELCODE,gpolicyno,etl_time,NEXTDATE,bankflag,idtype,
                    AIDTYPE,APERSON_ID,BENPARAM,COMNUM,CSR_ID,CURRENCY,DCDM,
                    DEL_DATE,DEL_TYPE,DESKPAY,DISCOUNT,EMPNO_ID,GCON_ID,ISCARD,
                    OPDATE,OPER_ID,PERSON_ID,REASON,REG_CODE,RENEWDATE,RISKCON_ID,SPECAGR,RENEWID,
                    SNO,TYPEID,SHARETYPE,BRANCH,BEGTIME,ENDTIME,SOUR_SYS,SRC_SYS,SALE_PROD_CODE,
                    OWNER_SOURCE_ID,INSURED_SOURCE_ID,WORKNO,COMB_POLICY_NO,APP_AGE,SUB_AGT_NO,O_CLASSCODE,
                    CROSS_SALE_IND,PREM_RATE_LEVEL ,ILL_SCORE,GROUP_NO ,DIGITAL_SIGN_IND,SUPPLY_INPUT_IND ,
                    INI_INVOICE_STATUS,UNI_PAY_IND,APPOINTED_OPDATE_IND
                    )
             SELECT POLICYNO,POLIST,NPAYLEN,TMOUNT,PIECES,CLASSCODE,EMPNO,BEGDATE ,
                    CONTNO ,FGSNO,PRELNAME, PID,JOB,APID,OPERNO ,STOPDAT,recaddr,rectele,nretdate,payfmage,
                    APPDATE,DELAGE,APPNO,APPF,PAYSEQ,UNSTDRATE,STDRATE ,SALEATTR,
                    YEARNUM,PEDATE,PAYCODE,DELCODE,gpolicyno,etl_time,NEXTDATE,bankflag,idtype,
                    AIDTYPE,APERSON_ID,BENPARAM,COMNUM,CSR_ID,CURRENCY,DCDM,
                    DEL_DATE,DEL_TYPE,DESKPAY,DISCOUNT,EMPNO_ID,GCON_ID,ISCARD,
                    OPDATE,OPER_ID,PERSON_ID,REASON,REG_CODE,RENEWDATE,RISKCON_ID,SPECAGR,RENEWID,
                    SNO,TYPEID,SHARETYPE,BRANCH,BEGTIME,ENDTIME,SOUR_SYS,SRC_SYS,SALE_PROD_CODE,
                    OWNER_SOURCE_ID,INSURED_SOURCE_ID,WORKNO,COMB_POLICY_NO,APP_AGE,SUB_AGT_NO,O_CLASSCODE,
                    CROSS_SALE_IND,PREM_RATE_LEVEL ,ILL_SCORE,GROUP_NO ,DIGITAL_SIGN_IND,SUPPLY_INPUT_IND ,
                    INI_INVOICE_STATUS,UNI_PAY_IND,APPOINTED_OPDATE_IND
             FROM job_temp_riskcon a';

   execute immediate v_sql;
   commit;

   execute immediate 'truncate table job_temp_riskcon';
   commit;

  ps_job_ids_log('RISKCON', v_fgsdm, 'end', row_count1);

exception
   WHEN DUP_VAL_ON_INDEX THEN
      SP_exception('DUP_VAL_ON_INDEX'|| sqlerrm);
   WHEN INVALID_CURSOR THEN
      SP_exception('INVALID_CURSOR'|| sqlerrm);
   WHEN INVALID_NUMBER THEN
      SP_exception('INVALID_NUMBER'|| sqlerrm);
   WHEN NO_DATA_FOUND THEN
      SP_exception('NO_DATA_FOUND'|| sqlerrm);
   WHEN TOO_MANY_ROWS THEN
      SP_exception('TOO_MANY_ROWS'|| sqlerrm);
   WHEN VALUE_ERROR THEN
      SP_exception('VALUE_ERROR'|| sqlerrm);
   WHEN OTHERS THEN
      SP_exception(to_char(SQLCODE) || SQLERRM);

end SP_RISKCON1;
//

CREATE OR REPLACE PROCEDURE PPS."SP_UPDATE_NFAILBENEFITINFO" IS


  V_ROW_COUNT NUMBER DEFAULT 1000;
  TYPE V_ROWID_TYPE IS TABLE OF ROWID INDEX BY BINARY_INTEGER;
  TYPE V_VARCHAR2_TYPE IS TABLE OF VARCHAR2(200) INDEX BY BINARY_INTEGER;



  V_ROWID V_ROWID_TYPE;

  V_ERRMESSAGE VARCHAR2(200);
  V_POLICYNO   VARCHAR2(200);
  V_COMPANY_NEW     VARCHAR2(200);
  V_BRANCH_NEW VARCHAR2(200);
  V_EMPNO_NEW    VARCHAR2(200);
  V_CHATYPE      VARCHAR2(200);
  V_branch      VARCHAR2(200);
  V_COMPANY      VARCHAR2(200);
  V_empno        VARCHAR2(200);
  v_xsqd        VARCHAR2(2);
  newbankno     VARCHAR2(200);
  newbankname    VARCHAR2(200);
   abankno         VARCHAR2(20);
  abankname          VARCHAR2(200);
  vnumber            number;




   cursor c_cur is
  select distinct (policyno_id) from tbl_fail_benefit_info where  branch_id='00000000000010' ;

  my_cur c_cur%rowtype;


BEGIN
  BEGIN

   open c_cur;
   loop

   fetch c_cur into my_cur;

   exit when c_cur%notfound;


select count(1) into vnumber  from tmp_policy where policyno_id=my_cur.policyno_id;



if(vnumber>0) then





   UPDATE tbl_fail_benefit_info
           SET branch_id='00000000000010',
                comp_id='14784981247544'
         WHERE policyno_id =my_cur.policyno_id ;





end if;

      COMMIT;

   end loop;

       close c_cur ;

  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_ERRMESSAGE := 'ERROR' || SQLCODE || ' ' || SQLERRM;
  END;


END SP_UPDATE_Nfailbenefitinfo ;
//

CREATE OR REPLACE PROCEDURE PPS."SP_UPDATE_NPREBENEFITDELETE" IS


  V_ROW_COUNT NUMBER DEFAULT 1000;
  TYPE V_ROWID_TYPE IS TABLE OF ROWID INDEX BY BINARY_INTEGER;
  TYPE V_VARCHAR2_TYPE IS TABLE OF VARCHAR2(200) INDEX BY BINARY_INTEGER;



  V_ROWID V_ROWID_TYPE;

  V_ERRMESSAGE VARCHAR2(200);
  V_POLICYNO   VARCHAR2(200);
  V_COMPANY_NEW     VARCHAR2(200);
  V_BRANCH_NEW VARCHAR2(200);
  V_EMPNO_NEW    VARCHAR2(200);
  V_CHATYPE      VARCHAR2(200);
  V_branch      VARCHAR2(200);
  V_COMPANY      VARCHAR2(200);
  V_empno        VARCHAR2(200);
  v_xsqd        VARCHAR2(2);
  newbankno     VARCHAR2(200);
  newbankname    VARCHAR2(200);
   abankno         VARCHAR2(20);
  abankname          VARCHAR2(200);
  vnumber            number;




   cursor c_cur is
  select distinct (policyno_id) from tbl_pre_benefit_delete where  branch_id='00000000000010'
and  comp_id='00000000001048';

  my_cur c_cur%rowtype;


BEGIN
  BEGIN

   open c_cur;
   loop

   fetch c_cur into my_cur;

   exit when c_cur%notfound;


-- update tbl_benefit_info set branch_id='' where policyno_id=my_cur.policyno_id;
select count(1) into vnumber  from tmp_policy where policyno_id=my_cur.policyno_id;



if(vnumber>0) then





   UPDATE tbl_pre_benefit_delete
           SET branch_id='00000000000010',
                comp_id='14784981247544'
         WHERE policyno_id =my_cur.policyno_id ;





end if;

      COMMIT;

   end loop;

       close c_cur ;

  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_ERRMESSAGE := 'ERROR' || SQLCODE || ' ' || SQLERRM;
  END;


END SP_UPDATE_Nprebenefitdelete ;
//

CREATE OR REPLACE PROCEDURE PPS."SP_UPDATE_TBL_IDS_CHGCON" IS




  V_ERRMESSAGE VARCHAR2(200);
  V_POLICYNO   VARCHAR2(30);
  V_COMPANY_NEW     VARCHAR2(30);
  V_BRANCH_NEW VARCHAR2(30);
  V_EMPNO_NEW    VARCHAR2(30);
  V_CHATYPE      VARCHAR2(30);
  V_branch      VARCHAR2(30);
  V_COMPANY      VARCHAR2(30);
  V_empno        VARCHAR2(30);
  vnumber       number;
  cursor c_cur is
    select distinct policyno
      from TBL_IDS_CHGCON
     where branch_id = '00000000000019';

  my_cur c_cur%rowtype;

BEGIN
  BEGIN

   open c_cur;

    LOOP


   fetch c_cur into my_cur;

   exit when c_cur%notfound;


   select count(1) into vnumber from tmp_div_policyno where policyno=my_cur.policyno;

   if(vnumber>0) then
    -- dbms_output.put_line('hh');
     select branch_new into  V_BRANCH_NEW
          from tmp_div_policyno where policyno=my_cur.policyno and rownum=1;

          update       TBL_IDS_CHGCON t set   t.branch_id=V_BRANCH_NEW
          where t.policyno=  my_cur.policyno;









      COMMIT;
end if;


    END LOOP;
        close c_cur ;

  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_ERRMESSAGE := 'ERROR' || SQLCODE || ' ' || SQLERRM;
  END;


END SP_UPDATE_TBL_IDS_CHGCON;
//

CREATE OR REPLACE PROCEDURE PPS."TESTLEAP" (vch in varchar) IS

       v_birthday varchar2(20);
       v_date varchar(20);
       v_sdate date;
       v_ndate number;
 begin

       v_birthday:=vch;-- 19960229

      --  v_date:=20+to_number(substr(v_birthday,1,4));

       v_ndate:=to_number(v_date);






       if(((mod(v_ndate,4)=0) and (mod(v_ndate,100)!=0)) or  (mod(v_ndate,400)=0)) then

       dbms_output.put_line('Leap Year===== the date is '||v_ndate);
       else

        dbms_output.put_line('Not Leap Year===== the date is '||v_ndate);

  end if;

end;
//

CREATE OR REPLACE PROCEDURE PPS."TRUNCATE_TBCI" IS

 -- 此存储过程是由程序A702调用,用来清空 tbl_benefit_chk_info表;

  V_ERRMESSAGE VARCHAR2(200);



BEGIN
  BEGIN


  EXECUTE IMMEDIATE 'truncate table tbl_benefit_chk_info';
  --  dbms_output.put_line('trunacate over!');



    -- 异常处理
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_ERRMESSAGE := 'ERROR' || SQLCODE || ' ' || SQLERRM;
  END;


END truncate_tbci ;
//
delimiter ;//

