
--disable_query_log

--echo ###测试前准备
create table t_program(t_namespace varchar2(200),t_name varchar2(200),t_owner varchar2(200),remark varchar2(200));
create table t_runtime(t_terminated varchar2(200),t_breakpoint varchar2(200),t_stackdepth varchar2(200),t_line varchar2(200),t_result varchar2(200),remark varchar2(200));
create table t_test1(t_op1 varchar2(200),t_op2 varchar2(200),remark varchar2(200));
create table t_test2(id varchar2(200),name varchar2(200),remark varchar2(200));
create table t_bp(bp varchar2(200),reason varchar2(200),result varchar2(200),remark varchar2(200));
create table t_show_bp(p_trace varchar2(15000),show_bp varchar2(15000),remark varchar2(200));

--echo ###所有需要的程序
--echo ###版本
delimiter //;
create or replace procedure c_pversion is
major binary_integer;
minor binary_integer;
begin
dbms_debug.probe_version(major,minor);
insert into t_test1 values(major,minor,'probe_version()功能');
end;
//
--echo ###initialize+debug_on执行
create or replace procedure cai_run is
v_debugid varchar2(200);
begin
delete from t_test1;
v_debugid:=dbms_debug.initialize;
insert into t_test1(t_op1,remark) values(v_debugid,'debug_id');
dbms_debug.debug_on;
end;
//
--echo ##attach_session
create or replace procedure cai_a_session is
v_debugid varchar2(200);
runtime_info dbms_debug.runtime_info;
result binary_integer;
begin
select t_op1 into v_debugid from t_test1;
dbms_debug.attach_session(v_debugid);
result:=dbms_debug.synchronize(runtime_info);
end;
//
--echo ###各种返回值，状态码等
--echo ##reason_none和success结果都是0
create or replace function cai_reason(v_input number) return varchar2 is
t_output varchar2(200);
begin
	if v_input=dbms_debug.reason_none then
	  t_output:='reason_none';
	 elsif v_input=dbms_debug.reason_interpreter_starting then
	  t_output:='Interpreter is starting.';
	 elsif v_input=dbms_debug.reason_breakpoint then
	  t_output:='Hit a breakpoint.';
	 elsif v_input=dbms_debug.reason_enter then
	  t_output:='Procedure entry.';
	 elsif v_input=dbms_debug.reason_return then
	  t_output:='Procedure is about to return.';
	 elsif v_input=dbms_debug.reason_finish then
	  t_output:='Procedure is finished.';
	 elsif v_input=dbms_debug.reason_line then
	  t_output:='Reached a new line.';
	 elsif v_input=dbms_debug.reason_interrupt then
	  t_output:='An interrupt occurred.';
	 elsif v_input=dbms_debug.reason_exception then
	  t_output:='An exception was raised.';
	 elsif v_input=dbms_debug.reason_exit then
	  t_output:='Interpreter is exiting (old form).';
	 elsif v_input=dbms_debug.reason_knl_exit then
	  t_output:='Kernel is exiting.';
	 elsif v_input=dbms_debug.reason_handler then
	  t_output:='Start exception-handler.';
	 elsif v_input=dbms_debug.reason_timeout then
	  t_output:='A timeout occurred.';
	 elsif v_input=dbms_debug.reason_instantiate then
	  t_output:='Instantiation block.';
	 elsif v_input=dbms_debug.reason_abort then
	  t_output:='Interpreter is aborting.';
	 else
	  t_output:='unknown';
	 end if;
	return t_output;
end;
//
create or replace function cai_result(v_input number) return varchar2 is
t_output varchar2(200);
begin
	if v_input=dbms_debug.success then
	  t_output:='success';
	 elsif v_input = dbms_debug.error_illegal_line then
	  t_output:='error_illegal_line';
     elsif v_input = dbms_debug.error_bad_handle then
	  t_output:='error_bad_handle';
	 elsif v_input = dbms_debug.error_no_such_breakpt then
	  t_output:='error_no_such_breakpt';
	 elsif v_input = dbms_debug.error_idle_breakpt then
	  t_output:='error_idle_breakpt';
	 elsif v_input = dbms_debug.error_stale_breakpt then
	  t_output:='error_stale_breakpt';
	 elsif v_input = dbms_debug.error_timeout then
	  t_output:='error_timeout';
	 elsif v_input = dbms_debug.error_communication then
	  t_output:='error_communication';
	 elsif v_input = dbms_debug.error_bogus_frame then
	  t_output:='error_bogus_frame';
	 elsif v_input = dbms_debug.error_no_debug_info then
	  t_output:='error_no_debug_info';
     elsif v_input = dbms_debug.error_no_such_object then
	  t_output:='error_no_such_object';
	 elsif v_input = dbms_debug.error_unknown_type then
	  t_output:='error_unknown_type';
	 elsif v_input = dbms_debug.error_nullvalue then
	  t_output:='error_nullvalue';
	 elsif v_input = dbms_debug.error_indexed_table then
	  t_output:='error_indexed_table';
	 elsif v_input = dbms_debug.retry_on_timeout then
	  t_output:='retry_on_timeout';
     elsif v_input = dbms_debug.continue_on_timeout then
	  t_output:='continue_on_timeout';
	 elsif v_input = dbms_debug.nodebug_on_timeout then
	  t_output:='nodebug_on_timeout';
	 elsif v_input = dbms_debug.error_deferred then
	  t_output:='error_deferred';
	 elsif v_input = dbms_debug.error_exception then
	  t_output:='error_exception';
	 elsif v_input = dbms_debug.abort_on_timeout then
	  t_output:='abort_on_timeout';
	 elsif v_input = dbms_debug.error_unimplemented then
	  t_output:='error_unimplemented';
	 else
	  t_output:='unknown';
	 end if;
	return t_output;
end;
//
--echo ###show_breakpoints+print_backtrace
create or replace procedure cai_show is
v_show varchar2(15000);
v_print varchar2(15000);
begin
dbms_debug.show_breakpoints(v_show);
dbms_debug.print_backtrace(v_print);
insert into t_show_bp values(v_print,v_show,'展示print_trace+show_breakpoints');
end;
//
--echo ##断点相关：set，disable，enable，delete，show
create or replace procedure cai_setbreakpoint(v_name varchar2,v_line binary_integer) is
v_program dbms_debug.program_info;
v_result binary_integer;
v_result1 varchar2(20);
v_breakpoint binary_integer;
begin
v_program.namespace:=dbms_debug.namespace_pkg_body;
v_program.name:=v_name;
v_program.owner:='cwx';
v_result:=dbms_debug.set_breakpoint(v_program,v_line,v_breakpoint);
v_result1:=cai_result(v_result);
insert into t_bp values(v_breakpoint,v_result1,v_result,v_name||'设置断点：第'||(v_line+1)||'行');
end;
//
create or replace package cai_dis_en_de_point is
function cai_dis_dis(a binary_integer) return binary_integer;
function cai_dis_en(a binary_integer) return binary_integer;
function cai_dis_de(a binary_integer) return binary_integer;
end cai_dis_en_de_point;
//
create or replace package body cai_dis_en_de_point is
function cai_dis_dis(a binary_integer) return binary_integer is
v1 binary_integer;
r1 varchar2(200);
cursor z_cur is select distinct(bp) from t_bp where reason not like '%delete%';
begin
delete from t_test2;
if a<-9.8 then
 for c in z_cur loop
 exit when z_cur%notfound;
 if c.bp>=0 then
  v1:=dbms_debug.disable_breakpoint(c.bp);
  r1:=cai_result(v1);
  insert into t_test2 values(c.bp,r1,'diasble');
 end if;
 end loop;
else
 v1:=dbms_debug.disable_breakpoint(a);
 r1:=cai_result(v1);
 insert into t_test2 values(a,r1,'diasble');
end if;
update t_bp set reason='disable',remark=remark||'断点状态变为disable' where bp in(select id from t_test2);
return v1;
end cai_dis_dis;
function cai_dis_en(a binary_integer) return binary_integer is
v1 binary_integer;
r1 varchar2(200);
cursor z_cur is select distinct(bp) from t_bp where reason not like '%delete%';
begin
delete from t_test2;
if a<-9.8 then
 for c in z_cur loop
 exit when z_cur%notfound;
 if c.bp>=0 then
  v1:=dbms_debug.enable_breakpoint(c.bp);
  r1:=cai_result(v1);
  insert into t_test2 values(c.bp,r1,'enable');
 end if;
 end loop;
else
 v1:=dbms_debug.enable_breakpoint(a);
 r1:=cai_result(v1);
 insert into t_test2 values(a,r1,'enable');
end if;
update t_bp set reason='enable',remark=remark||'断点状态变为enable' where bp in(select id from t_test2);
return v1;
end cai_dis_en;
function cai_dis_de(a binary_integer) return binary_integer is
v1 binary_integer;
r1 varchar2(200);
cursor z_cur is select distinct(bp) from t_bp where reason not like '%delete%';
begin
delete from t_test2;
if a<-9.8 then
 for c in z_cur loop
 exit when z_cur%notfound;
 if c.bp>=0 then
  v1:=dbms_debug.delete_breakpoint(c.bp);
  r1:=cai_result(v1);
  insert into t_test2 values(c.bp,r1,'delete');
 end if;
 end loop;
else
 v1:=dbms_debug.delete_breakpoint(a);
 r1:=cai_result(v1);
 insert into t_test2 values(a,r1,'delete');
end if;
update t_bp set reason='delete',remark=remark||'断点状态已被delete' where bp in(select id from t_test2);
return v1;
end cai_dis_de;
end cai_dis_en_de_point;
//
--echo ####get_value
create or replace procedure cai_getvalue(v_name varchar2,v_frame number) is
v_output varchar2(200);
v_result number;
v_result1 varchar2(200);
begin
v_result:=dbms_debug.get_value(v_name,v_frame,v_output,null);
v_result1:=cai_result(v_result);
insert into t_test2 values(v_result,v_result1,v_name||'='||v_output);
end;
//
--echo ###continue
create or replace procedure cai_continue(v_flag number,v_inforequested varchar2) is
runinfo dbms_debug.runtime_info;
pro_info dbms_debug.program_info;
v_result binary_integer;
v_reason varchar2(200);
v_reason1 varchar2(200);
begin
v_result:=dbms_debug.continue(runinfo,v_flag,v_inforequested);
v_reason:=cai_result(v_result);
v_reason1:=cai_reason(runinfo.reason);
pro_info:=runinfo.program;
insert into t_test2 values(v_result,v_reason,pro_info.name||'执行continue');
if v_result=dbms_debug.success then
insert into t_runtime values(runinfo.terminated,runinfo.breakpoint,runinfo.stackdepth,runinfo.line#,v_reason1,'runtime_info信息');
insert into t_program values(pro_info.namespace,pro_info.name,pro_info.owner,'program_info信息');
end if;
end;
//
--echo ####验证debug功能
create or replace procedure cai_begintest(v_test varchar2,v_name varchar2,i number) is
v_result varchar2(200);
begin
if v_test='set' then
cai_setbreakpoint(v_name,i);
elsif v_test='disable' then
v_result:=cai_dis_en_de_point.cai_dis_dis(i);
elsif v_test='enable' then
v_result:=cai_dis_en_de_point.cai_dis_en(i);
elsif v_test='delete' then
v_result:=cai_dis_en_de_point.cai_dis_de(i);
elsif v_test='continue' then
cai_continue(i,null);
elsif v_test='getvalue' then
cai_getvalue(v_name,i);
elsif v_test='show' then
cai_show;
end if;
end;
//

CREATE OR REPLACE PROCEDURE cnt_exit IS
  run_info dbms_debug.runtime_info;
  result binary_integer;
BEGIN
  <<label>> loop
    result := dbms_debug.continue(run_info, dbms_debug.break_next_line);
    if run_info.reason = dbms_debug.reason_exit OR result != dbms_debug.success THEN
      exit label;
    end if;
  end loop lable;
  dbms_output.put_line('target stop already! result = ' || result || ' reason = ' || run_info.reason);
END;
//

delimiter  ;//
#--enable_warnings
--enable_query_log 

