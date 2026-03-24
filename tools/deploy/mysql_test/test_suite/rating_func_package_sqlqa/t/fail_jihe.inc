
delimiter $;

  CREATE OR REPLACE FUNCTION "TARIFF_TITLE_INSERT1" (fee_flag number)
return sys_refcursor
as
kk sys_refcursor;
begin
    --插入表头数据
    insert into work_tariff_show(txt1,txt2,txt3,txt4,txt5,txt6,txt7,txt8,txt9,tariff_plan_id,item_sort)
    values('资费政策','资费政策号','资费政策名称','是否递归','优先级','优惠前处理方案',
    '优惠后处理方案','政策类型','说明',0,0);
    insert into work_tariff_show(txt1,txt2,txt3,txt4,txt5,txt6,txt7,txt8,txt9,txt10,
    txt11,txt12,txt13,txt14,txt15,txt16,tariff_plan_id,item_sort)
    values('  └资费政策项','资费政策号','计算方法','要计算的费用',
    '使用的资费模式','要计算的计费事件','优先级','生效时间','失效时间','生失效判断方法',
    '优惠费用','计算模式','基准费用','计费条件','资费切换方法','资费切换单位',1,0);
    insert into work_tariff_show(txt1,txt2,txt3,txt4,txt5,txt6,txt7,txt8,txt9,txt10,
    txt11,txt12,txt13,txt14,tariff_plan_id,item_sort)
    values('    └资费模式','资费模式号','资费模式名称','资费模式类型',
    '参数字段定义数','参数字段定义','参数匹配次序','匹配类型','应用时间判断方法','资费标记位置',
    '优惠费用','取整方法','取整精度','资费标记偏移',2,0);
    insert into work_tariff_show(txt1,txt2,txt3,txt4,txt5,txt6,txt7,txt8,txt9,txt10,txt11,tariff_plan_id,item_sort)
    values('      └资费模式项','生效时间','失效时间','资费条件','下级资费类型',
    '下级资费','附加(优惠)比率','资费标记符','参数串','优先级','动态资费',3,0);
    if(fee_flag=1) then
        insert into work_tariff_show(txt1,txt2,txt3,txt4,txt5,txt6,txt7,txt8,txt9,txt10,tariff_plan_id,item_sort)
        values('        ├费率','费率代码','费率名称','金额','币种','单位','单位种类',
        '取整单位','取整方法','计价属性',4,0);
        insert into work_tariff_show(txt1,txt2,txt3,tariff_plan_id,item_sort)
        values('        └费用项','费用项编码','费用项名称',5,0);
		dbms_output.put_line('1');
    end if;
    open kk for select txt1,txt2,txt3,txt4,txt5,txt6,txt7,txt8,txt9,txt10,txt11,txt12,txt13,txt14,txt15,txt16 from work_tariff_show where 1=2;
	dbms_output.put_line('2');
    return kk;
end;

$

  CREATE OR REPLACE FUNCTION "TARIFF_PLAN_SHOW1" (plan_id number, fee_flag number)
return sys_refcursor
as
kk sys_refcursor;
item_num number(10) := 0;
level_num int:=0;
level_lab varchar2(2048):='';
corner_lab varchar2(10):='└';
tee_lab varchar2(10):='├';
vline_lab varchar2(10):='│';
hline_lab varchar2(10):='─';
blank_lab varchar2(10):='  ';
plan_count int  :=0;
plan_ind int    :=0;
item_count int  :=0;
item_ind int    :=0;
proc_before_name varchar2(64);
proc_after_name varchar2(64);
procedure schema_deal(schema_id number, cur_ind int, max_count int)
as
schema_count int:=0;
schema_ind int  :=0;
schema_item_count int := 0;
schema_item_ind int   := 0;
rate_name varchar2(64);
feename varchar2(64);
begin
    --求资费模式记录数
    select count(*) into schema_count from tariff_schema where tariff_schema_id=schema_id;
    if(schema_count>0) then
                if(cur_ind <> max_count-1) then
                    level_lab := substrb(level_lab,1,(level_num-1)*2)||vline_lab;
					dbms_output.put_line('1');
                else
                    level_lab := substrb(level_lab,1,(level_num-1)*2)||blank_lab;
					dbms_output.put_line('2');
                end if;
        level_num   := level_num + 1;
        level_lab   := level_lab||tee_lab;
        schema_ind  := 0;
        --遍历资费模式记录
        for schema_row in(
        select a.tariff_schema_id,a.tariff_name,a.tariff_type,b.type_name,a.fieldcount,a.g_field_def,c.name match_order,
        d.name match_type,e.name apply_method,a.refid,f.fee_name discount_fee,g.name round_method,
        a.round_scale,a.ref_offset
        from tariff_schema a,tariff_type b,code_dict c,code_dict d,code_dict e,
        (select 0 fee_id,'【空值】' fee_name from dual union select fee_id,fee_name from fee) f,code_dict g
        where a.tariff_type=b.tariff_type and (c.catalog='T_MATCH_ORDER' and a.match_order=to_number(c.code))
        and (d.catalog='T_MATCH_TYPE' and a.match_type=to_number(d.code))
        and (e.catalog='T_APPLY_METHOD' and a.apply_method=to_number(e.code)) and a.discount_fee_id=f.fee_id
        and (g.catalog='T_ROUND_METHOD' and a.round_method=to_number(g.code)) and tariff_schema_id=schema_id)
        loop
            if(schema_ind = schema_count-1) then
                level_lab := substrb(level_lab,1,(level_num-1)*2)||corner_lab;
				dbms_output.put_line('3');
            else
                level_lab := substrb(level_lab,1,(level_num-1)*2)||tee_lab;
				dbms_output.put_line('4');
            end if;
            insert into work_tariff_show(
            txt1,txt2,txt3,txt4,txt5,txt6,txt7,txt8,txt9,txt10,txt11,txt12,txt13,txt14,tariff_plan_id,item_sort)
            values(
            level_lab||'资费模式【'||schema_id||'】:'||schema_row.tariff_name||'===<<'||schema_row.type_name||'>>',
            schema_row.tariff_schema_id,
            schema_row.tariff_name,
            schema_row.type_name,
            schema_row.fieldcount,
            schema_row.g_field_def,
            schema_row.match_order,
            schema_row.match_type,
            schema_row.apply_method,
            schema_row.refid,
            schema_row.discount_fee,
            schema_row.round_method,
            schema_row.round_scale,
            schema_row.ref_offset,
            plan_id,
            item_num);
            item_num := item_num + 1;
            --求资费模式项记录数
            select count(*) into schema_item_count from tariff_item where tariff_schema_id=schema_id;
            if(schema_item_count>0) then
                if(schema_ind <> schema_count-1) then
                    level_lab := substrb(level_lab,1,(level_num-1)*2)||vline_lab;
					dbms_output.put_line('5');
                else
                    level_lab := substrb(level_lab,1,(level_num-1)*2)||blank_lab;
					dbms_output.put_line('6');
                end if;
                level_num := level_num + 1;
                level_lab := level_lab||tee_lab;
                schema_item_ind := 0;
                --遍历资费模式项
                for tariff_item_row in(
                select a.*,b.name subtariff_type_name,c.name tariff_name
                from tariff_item a,code_dict b,
                (select name,code,type from(
                    SELECT '【'||LPAD(Code,6)||'】'||Name NAME, TO_NUMBER(Code) CODE ,type  FROM (
                        select 1 type,ratecode code,ratename name from charge_rate
                        union all select 2 type,tariff_schema_id code,tariff_name name from tariff_schema
                        union all select 3 type,fee_id code,fee_name name from fee)
                    )
                ORDER BY 2,1) c
                where (b.catalog='T_SUBTARIFF_TYPE' and a.subtariff_type=to_number(b.code))
                      and a.tariff_id=c.code and a.subtariff_type=c.type and tariff_schema_id=schema_id
                      order by a.tariff_criteria,a.applytime)
                loop
                    if(schema_item_ind = schema_item_count - 1) then
                        level_lab := substrb(level_lab,1,(level_num-1)*2)||corner_lab;
						dbms_output.put_line('7');
                    else
                        level_lab := substrb(level_lab,1,(level_num-1)*2)||tee_lab;
						dbms_output.put_line('8');
                    end if;
                    insert into work_tariff_show(
                    txt1,txt2,txt3,txt4,txt5,txt6,txt7,txt8,txt9,txt10,txt11,tariff_plan_id,item_sort)
                    values(
                    level_lab||'资费模式项',
                    to_char(tariff_item_row.applytime,'yyyy-mm-dd'),
                    to_char(tariff_item_row.expiretime,'yyyy-mm-dd'),
                    decode(schema_row.tariff_type,1,tariff_item_row.g_criteria,tariff_item_row.tariff_criteria),
                    tariff_item_row.subtariff_type_name,
                    tariff_item_row.tariff_name,
                    tariff_item_row.ratio,
                    tariff_item_row.ratetype,
                    tariff_item_row.g_param,
                    tariff_item_row.precedence,
                    tariff_item_row.is_dynamic,
                    plan_id,
                    item_num);
                    item_num := item_num + 1;
                    if(fee_flag=1 and tariff_item_row.subtariff_type=1) then --费率
                        if(schema_item_ind <> schema_item_count-1) then
                            level_lab := substrb(level_lab,1,(level_num-1)*2)||vline_lab;
							dbms_output.put_line('9');
                        else
                            level_lab := substrb(level_lab,1,(level_num-1)*2)||blank_lab;
							dbms_output.put_line('10');
                        end if;
                        insert into work_tariff_show(
                        txt1,txt2,txt3,txt4,txt5,txt6,txt7,txt8,txt9,txt10,tariff_plan_id,item_sort)
                        select
                        level_lab||corner_lab||'费率【'||tariff_item_row.tariff_id||'】:'||ratename,
                        a.ratecode,
                        a.ratename,
                        a.currency,
                        b.name currency_type,
                        a.units,
                        c.name unit_type,
                        a.round_unit,
                        d.name round_type,
                        e.name attr_class,
                        plan_id,
                        item_num
                        from charge_rate a,code_dict b,code_dict c,code_dict d,code_dict e
                        where ratecode=tariff_item_row.tariff_id
                        and (b.catalog='T_CURRENCY_TYPE' and a.currencytype=to_number(b.code))
                        and (c.catalog='T_UNITTYPE' and a.unittype=to_number(c.code))
                        and (d.catalog='T_ROUND_TYPE' and a.round_type=to_number(d.code))
                        and (e.catalog='T_EVENT_ATTR_CLASS' and a.attr_class=to_number(e.code));
                        item_num := item_num + 1;
                    else
                        if(fee_flag=1 and tariff_item_row.subtariff_type=3) then --费用项
                            if(schema_item_ind <> schema_item_count-1) then
                                level_lab := substrb(level_lab,1,(level_num-1)*2)||vline_lab;
								dbms_output.put_line('12');
                            else
                                level_lab := substrb(level_lab,1,(level_num-1)*2)||blank_lab;
								dbms_output.put_line('13');
                            end if;
                            insert into work_tariff_show(
                            txt1,txt2,txt3,tariff_plan_id,item_sort)
                            select
                            level_lab||corner_lab||'费用项【'||tariff_item_row.tariff_id||'】:'||fee_name,
                            fee_id,
                            fee_name,
                            plan_id,
                            item_num
                            from fee
                            where fee_id=tariff_item_row.tariff_id;
                            item_num := item_num + 1;
                        else
                            if(tariff_item_row.subtariff_type = 2) then  --资费模式
dbms_output.put_line('14');        
                        schema_deal(tariff_item_row.tariff_id, schema_item_ind, schema_item_count);
                            end if;
                        end if;
                    end if;
                    schema_item_ind := schema_item_ind + 1;
                end loop;
                level_num := level_num - 1;
                level_lab := substrb(level_lab,1,lengthb(level_lab)-2);
            end if;
            schema_ind := schema_ind + 1;
        end loop;
        level_num := level_num - 1;
        level_lab := substrb(level_lab,1,lengthb(level_lab)-2);
		dbms_output.put_line('15');
    end if;
end; --内部函数schema_deal结束
begin
    --求符合条件的资费政策记录数
    select count(*) into plan_count from tariff_plan where tariff_plan_id in(plan_id);
    --如果存在符合条件记录
    if(plan_count>0) then
        --初始化资费索引，资费树级数更新（后面会更新回来）
        level_num   := level_num + 1;
        level_lab   := level_lab||tee_lab;
        plan_ind    := 0;
        --遍历资费政策记录
        for plan_row in(select * from tariff_plan where tariff_plan_id in (plan_id))
        loop
            --如果是最后一个资费政策,用corner_lab,否则用tee_lab
            if(plan_ind = plan_count-1) then
                level_lab := substrb(level_lab,1,(level_num-1)*2)||corner_lab;
				dbms_output.put_line('16');
            else
                level_lab := substrb(level_lab,1,(level_num-1)*2)||tee_lab;
				dbms_output.put_line('17');
            end if;
            begin
                select proc_schema_name into proc_before_name from process_schema where proc_schema_id = plan_row.process_before;
				dbms_output.put_line('18');
            exception when no_data_found then
			dbms_output.put_line('19');
                proc_before_name := '【空值】';
            end;
            begin
                select proc_schema_name into proc_after_name from process_schema where proc_schema_id = plan_row.process_after;
				dbms_output.put_line('20');
            exception when no_data_found then
			dbms_output.put_line('21');
                proc_before_name := '【空值】';
            end;
            --往临时表插入一条数据
            insert into work_tariff_show(
            txt1,txt2,txt3,txt4,txt5,txt6,txt7,txt8,tariff_plan_id,item_sort)
            values(
            '资费政策【'||plan_row.tariff_plan_id||'】:'||plan_row.tariff_plan_name,
            plan_row.tariff_plan_id,
            plan_row.tariff_plan_name,
            plan_row.is_recursive,
            plan_row.precedence,
            proc_before_name,
            proc_after_name,
            plan_row.note,
            plan_id,
            item_num);
            item_num := item_num + 1;
            --求符合条件的下级资费政策项记录数
            select count(*) into item_count from tariff_plan_item where tariff_plan_id=plan_row.tariff_plan_id;
            --如果存在资费政策项
            if(item_count > 0) then
                --如果是最后一条资费政策,用竖线处理,否则用空格
                if(plan_ind <> plan_count-1) then
                    level_lab := substrb(level_lab,1,(level_num-1)*2)||vline_lab;
					dbms_output.put_line('22');
                else
                    level_lab := substrb(level_lab,1,(level_num-1)*2)||blank_lab;
					dbms_output.put_line('23');
                end if;
                level_num := level_num + 1;
                level_lab := level_lab||tee_lab;
                item_ind := 0;
                --遍历资费政策项
                for item_row in(
                select a.tariff_plan_id,b.name calctype,a.fee_id,c.fee_name fee,a.tariff_schema_id,d.tariff_name,
                e.event_name charging_event_name,a.precedence,a.applytime,a.expiretime,f.name apply_method,
                g.name discount_type,h.fee_name base_fee,i.fee_name discount_fee,a.g_charging_cond,
                j.name switch_method,a.switch_unit
                from tariff_plan_item a, code_dict b,
                (select 0 fee_id,'【空值】' fee_name from dual union select fee_id,fee_name from fee) c,
                tariff_schema d, charging_event_def e,code_dict f,code_dict g,
                (select 0 fee_id,'【空值】' fee_name from dual union select fee_id,fee_name from fee) h,
                (select 0 fee_id,'【空值】' fee_name from dual union select fee_id,fee_name from fee) i,code_dict j
                where (b.catalog='T_CALCTYPE' and a.calctype=to_number(b.code)) and a.fee_id=c.fee_id
                and a.tariff_schema_id=d.tariff_schema_id and a.charging_event_id=e.event_id
                and (f.catalog='T_APPLY_METHOD' and a.apply_method=to_number(f.code))
                and (g.catalog='T_DISCOUNT_TYPE' and a.discount_type=to_number(g.code))
                and a.base_fee_id=h.fee_id and a.discount_fee_id=i.fee_id
                and (j.catalog='T_SWITCH_TYPE' and a.switch_method=to_number(j.code))
                and a.tariff_plan_id=plan_row.tariff_plan_id order by precedence)
                loop
                    if(item_ind = item_count-1) then
                         level_lab := substrb(level_lab,1,(level_num-1)*2)||corner_lab;
						 dbms_output.put_line('24');
                    else
                         level_lab := substrb(level_lab,1,(level_num-1)*2)||tee_lab;
						 dbms_output.put_line('25');
                    end if;
                    insert into work_tariff_show(
                    txt1,txt2,txt3,txt4,txt5,txt6,txt7,txt8,txt9,txt10,txt11,txt12,txt13,txt14,txt15,txt16,tariff_plan_id,item_sort)
                    values(
                    level_lab||'资费政策项【'||item_row.fee||'】:'||plan_row.tariff_plan_name,
                    item_row.tariff_plan_id,
                    item_row.calctype,
                    item_row.fee,
                    item_row.tariff_name,
                    item_row.charging_event_name,
                    item_row.precedence,
                    to_char(item_row.applytime,'yyyy-mm-dd'),
                    to_char(item_row.expiretime,'yyyy-mm-dd'),
                    item_row.apply_method,
                    item_row.discount_type,
                    item_row.base_fee,
                    item_row.discount_fee,
                    item_row.g_charging_cond,
                    item_row.switch_method,
                    item_row.switch_unit,
                    plan_id,
                    item_num);
                    item_num := item_num + 1;
                    schema_deal(item_row.tariff_schema_id, item_ind, item_count);
                    item_ind := item_ind + 1;
                end loop;
                level_num := level_num - 1;
                level_lab := substrb(level_lab,1,lengthb(level_lab)-2);
            end if;
            plan_ind := plan_ind + 1;
        end loop;
        level_num := level_num - 1;
        level_lab := substrb(level_lab,1,lengthb(level_lab)-2);
		dbms_output.put_line('26');
    end if;
    open kk for select txt1,txt2,txt3,txt4,txt5,txt6,txt7,txt8,txt9,txt10,txt11,txt12,txt13,txt14,txt15,txt16 from work_tariff_show;
	dbms_output.put_line('11');
    return kk;
end;

$

  CREATE OR REPLACE PACKAGE "APK_COMMON_DATA_IMPORT" is

  function af_get_column_type(av_tablename in varchar2,av_columnname in varchar2) return varchar2;

  --通过模板向指定的表中插入数据,最多支持26列
  procedure ap_insert_data(av_module_id in number,
                           av_column1 in varchar2,
                           av_column2 in varchar2,
                           av_column3 in varchar2,
                           av_column4 in varchar2,
                           av_column5 in varchar2,
                           av_column6 in varchar2,
                           av_column7 in varchar2,
                           av_column8 in varchar2,
                           av_column9 in varchar2,
                           av_column10 in varchar2,
                           av_column11 in varchar2,
                           av_column12 in varchar2,
                           av_column13 in varchar2,
                           av_column14 in varchar2,
                           av_column15 in varchar2,
                           av_column16 in varchar2,
                           av_column17 in varchar2,
                           av_column18 in varchar2,
                           av_column19 in varchar2,
                           av_column20 in varchar2,
                           av_column21 in varchar2,
                           av_column22 in varchar2,
                           av_column23 in varchar2,
                           av_column24 in varchar2,
                           av_column25 in varchar2,
                           av_column26 in varchar2);

  --通过模板向指定的表中插入数据,最多支持26列
  procedure ap_insert_check_data(av_flag in number,
                           av_module_id in number,
                           av_column1 in varchar2,
                           av_column2 in varchar2,
                           av_column3 in varchar2,
                           av_column4 in varchar2,
                           av_column5 in varchar2,
                           av_column6 in varchar2,
                           av_column7 in varchar2,
                           av_column8 in varchar2,
                           av_column9 in varchar2,
                           av_column10 in varchar2,
                           av_column11 in varchar2,
                           av_column12 in varchar2,
                           av_column13 in varchar2,
                           av_column14 in varchar2,
                           av_column15 in varchar2,
                           av_column16 in varchar2,
                           av_column17 in varchar2,
                           av_column18 in varchar2,
                           av_column19 in varchar2,
                           av_column20 in varchar2,
                           av_column21 in varchar2,
                           av_column22 in varchar2,
                           av_column23 in varchar2,
                           av_column24 in varchar2,
                           av_column25 in varchar2,
                           av_column26 in varchar2);

  --同步数据过程之删除BOSS系统参数错误数据
  procedure ap_sync_parameter_del(av_module_id in number,
                           av_column1 in varchar2,
                           av_column2 in varchar2,
                           av_column3 in varchar2,
                           av_column4 in varchar2,
                           av_column5 in varchar2,
                           av_column6 in varchar2,
                           av_column7 in varchar2,
                           av_column8 in varchar2,
                           av_column9 in varchar2,
                           av_column10 in varchar2,
                           av_column11 in varchar2,
                           av_column12 in varchar2,
                           av_column13 in varchar2,
                           av_column14 in varchar2,
                           av_column15 in varchar2,
                           av_column16 in varchar2,
                           av_column17 in varchar2,
                           av_column18 in varchar2,
                           av_column19 in varchar2,
                           av_column20 in varchar2,
                           av_column21 in varchar2,
                           av_column22 in varchar2,
                           av_column23 in varchar2,
                           av_column24 in varchar2,
                           av_column25 in varchar2,
                           av_column26 in varchar2);

  --同步数据过程之插入数据平台系统正确的参数
  procedure ap_sync_parameter_ins(av_module_id in number,
                           av_column1 in varchar2,
                           av_column2 in varchar2,
                           av_column3 in varchar2,
                           av_column4 in varchar2,
                           av_column5 in varchar2,
                           av_column6 in varchar2,
                           av_column7 in varchar2,
                           av_column8 in varchar2,
                           av_column9 in varchar2,
                           av_column10 in varchar2,
                           av_column11 in varchar2,
                           av_column12 in varchar2,
                           av_column13 in varchar2,
                           av_column14 in varchar2,
                           av_column15 in varchar2,
                           av_column16 in varchar2,
                           av_column17 in varchar2,
                           av_column18 in varchar2,
                           av_column19 in varchar2,
                           av_column20 in varchar2,
                           av_column21 in varchar2,
                           av_column22 in varchar2,
                           av_column23 in varchar2,
                           av_column24 in varchar2,
                           av_column25 in varchar2,
                           av_column26 in varchar2);


end apk_common_data_import;



$

  CREATE OR REPLACE PACKAGE BODY "APK_COMMON_DATA_IMPORT" is

  type ref_cursor is ref cursor;

  TYPE tp_array IS VARRAY(26)  OF varchar2(4000);

  function af_get_column_type(av_tablename in varchar2,av_columnname in varchar2) return varchar2
  is
     v_datatype          varchar2(106);
  begin
     --获取列的类型，不需要例外处理
     select data_type into v_datatype from user_tab_cols
        where table_name=upper(av_tablename) and column_name=upper(av_columnname);
		dbms_output.put_line('1');
     return v_datatype;
  end af_get_column_type;

  --向对应的表中插入数据(通用导入界面程序调用)
  procedure ap_insert_data(av_module_id in number,
                           av_column1 in varchar2,
                           av_column2 in varchar2,
                           av_column3 in varchar2,
                           av_column4 in varchar2,
                           av_column5 in varchar2,
                           av_column6 in varchar2,
                           av_column7 in varchar2,
                           av_column8 in varchar2,
                           av_column9 in varchar2,
                           av_column10 in varchar2,
                           av_column11 in varchar2,
                           av_column12 in varchar2,
                           av_column13 in varchar2,
                           av_column14 in varchar2,
                           av_column15 in varchar2,
                           av_column16 in varchar2,
                           av_column17 in varchar2,
                           av_column18 in varchar2,
                           av_column19 in varchar2,
                           av_column20 in varchar2,
                           av_column21 in varchar2,
                           av_column22 in varchar2,
                           av_column23 in varchar2,
                           av_column24 in varchar2,
                           av_column25 in varchar2,
                           av_column26 in varchar2)
  is
     v_ins_sql       varchar2(32767);
     v_ins_value     varchar2(4000);
     v_ref_sql       varchar2(200);
     v_coltype       varchar2(106);
     v_tablename     varchar2(32);
     ref_cur         ref_cursor;
     v_rowtype       param_exp_imp_column_def%rowtype;
     v_array         tp_array;
     v_col_cnt       number(2);
  begin
     --初始化数组
     v_array := tp_array(av_column1,av_column2,av_column3,av_column4,av_column5,av_column6,av_column7,av_column8,
                         av_column9,av_column10,av_column11,av_column12,av_column13,av_column14,av_column15,
                         av_column16,av_column17,av_column18,av_column19,av_column20,av_column21,av_column22,
                         av_column23,av_column24,av_column25,av_column26);
     --获取表名
     select sql_text into v_tablename from param_exp_imp_module t
         where module_type=1 and module_id=av_module_id and inuse=1;

     --构建Insert SQL语句
     v_ins_sql:='insert into '||v_tablename||'(';
     --按照列进行循环，必须按照COLUMN_ID排序
     v_ref_sql:='select * from param_exp_imp_column_def t where t.module_id='
                   ||av_module_id||' and t.inuse=1 order by t.COLUMN_ID';
     --列数
     v_col_cnt:=0;
     v_ins_value:='';
     open ref_cur for v_ref_sql;
     loop
        fetch ref_cur into v_rowtype;
        exit when ref_cur%notfound;
        v_ins_sql:=v_ins_sql||v_rowtype.column_id||',';
        v_col_cnt:=v_col_cnt+1;
        --判断当前列的类型是否是DATE型
        v_coltype:=af_get_column_type(v_tablename,v_rowtype.column_id);
        if(v_coltype='DATE') then
           --判断日期是否为空
           if v_array(v_col_cnt)='' or v_array(v_col_cnt) is null then
              v_ins_value:=v_ins_value||'null,';
			  dbms_output.put_line('2');
           else
              v_ins_value:=v_ins_value||'to_date('''||v_array(v_col_cnt)||''',''yyyy-mm-dd hh24:mi:ss''),';
			  dbms_output.put_line('3');
           end if;
        else
           v_ins_value:=v_ins_value||''''||v_array(v_col_cnt)||''',';
		   dbms_output.put_line('4');
        end if;
     end loop;
     close ref_cur;
     v_ins_sql:=substr(v_ins_sql,1,length(v_ins_sql)-1)||') values(';
     v_ins_value:=substr(v_ins_value,1,length(v_ins_value)-1)||')';
     v_ins_sql:=v_ins_sql||v_ins_value;
     begin
          execute immediate v_ins_sql;
		  dbms_output.put_line('5');
     exception
       when others then
          raise_application_error(-20001,'插入数据失败'||chr(10)||sqlerrm||chr(10)||'SQL: '||v_ins_sql);
          return;
     end;

     commit work;
  end ap_insert_data;


  --向全局临时表中插入数据校验记录
  --向对应的表中插入数据(参数核对界面程序调用)
  procedure ap_insert_check_data(av_flag in number,
                           av_module_id in number,
                           av_column1 in varchar2,
                           av_column2 in varchar2,
                           av_column3 in varchar2,
                           av_column4 in varchar2,
                           av_column5 in varchar2,
                           av_column6 in varchar2,
                           av_column7 in varchar2,
                           av_column8 in varchar2,
                           av_column9 in varchar2,
                           av_column10 in varchar2,
                           av_column11 in varchar2,
                           av_column12 in varchar2,
                           av_column13 in varchar2,
                           av_column14 in varchar2,
                           av_column15 in varchar2,
                           av_column16 in varchar2,
                           av_column17 in varchar2,
                           av_column18 in varchar2,
                           av_column19 in varchar2,
                           av_column20 in varchar2,
                           av_column21 in varchar2,
                           av_column22 in varchar2,
                           av_column23 in varchar2,
                           av_column24 in varchar2,
                           av_column25 in varchar2,
                           av_column26 in varchar2)
  is
     v_ins_sql       varchar2(32767);
     v_ins_value     varchar2(4000);
     v_ref_sql       varchar2(200);
     v_coltype       varchar2(106);
     v_tablename     varchar2(32);
     ref_cur         ref_cursor;
     v_rowtype       param_exp_imp_column_def%rowtype;
     v_array         tp_array;
     v_col_cnt       number(2);
  begin
     --初始化数组
     v_array := tp_array(av_column1,av_column2,av_column3,av_column4,av_column5,av_column6,av_column7,av_column8,
                         av_column9,av_column10,av_column11,av_column12,av_column13,av_column14,av_column15,
                         av_column16,av_column17,av_column18,av_column19,av_column20,av_column21,av_column22,
                         av_column23,av_column24,av_column25,av_column26);
     --获取表名，模块类型为2:参数核对
     select sql_text into v_tablename from param_exp_imp_module t
         where module_type=2 and module_id=av_module_id and inuse=1;

     --构建Insert SQL语句
     v_ins_sql:='insert into '||v_tablename||'(';
     --按照列进行循环，必须按照COLUMN_ID排序
     v_ref_sql:='select * from param_exp_imp_column_def t where t.module_id='
                   ||av_module_id||' and t.inuse=1 order by t.COLUMN_ID';
     --列数
     v_col_cnt:=0;
     v_ins_value:='';
     open ref_cur for v_ref_sql;
     loop
        fetch ref_cur into v_rowtype;
        exit when ref_cur%notfound;
        v_ins_sql:=v_ins_sql||v_rowtype.column_id||',';
        v_col_cnt:=v_col_cnt+1;
        --判断当前列的类型是否是DATE型
        v_coltype:=af_get_column_type(v_tablename,v_rowtype.column_id);
        if(v_coltype='DATE') then
           --判断日期是否为空
           if v_array(v_col_cnt)='' or v_array(v_col_cnt) is null then
              v_ins_value:=v_ins_value||'null,';
			  dbms_output.put_line('6');
           else
              v_ins_value:=v_ins_value||'to_date('''||v_array(v_col_cnt)||''',''yyyy-mm-dd hh24:mi:ss''),';
			  dbms_output.put_line('7');
           end if;
        else
           v_ins_value:=v_ins_value||''''||v_array(v_col_cnt)||''',';
		   dbms_output.put_line('8');
        end if;
     end loop;
     close ref_cur;
     v_ins_sql:=v_ins_sql||'flag) values(';
     v_ins_value:=v_ins_value||av_flag||')';
     v_ins_sql:=v_ins_sql||v_ins_value;
     begin
          execute immediate v_ins_sql;
		  dbms_output.put_line('9');
     exception
       when others then
          raise_application_error(-20001,'插入数据失败'||chr(10)||sqlerrm||chr(10)||'SQL: '||v_ins_sql);
          return;
     end;

     commit work;
  end ap_insert_check_data;

  --同步参数核对结果数据(av_synctype:0 删除 1 插入)
  procedure ap_sync_parameter_del(av_module_id in number,
                           av_column1 in varchar2,
                           av_column2 in varchar2,
                           av_column3 in varchar2,
                           av_column4 in varchar2,
                           av_column5 in varchar2,
                           av_column6 in varchar2,
                           av_column7 in varchar2,
                           av_column8 in varchar2,
                           av_column9 in varchar2,
                           av_column10 in varchar2,
                           av_column11 in varchar2,
                           av_column12 in varchar2,
                           av_column13 in varchar2,
                           av_column14 in varchar2,
                           av_column15 in varchar2,
                           av_column16 in varchar2,
                           av_column17 in varchar2,
                           av_column18 in varchar2,
                           av_column19 in varchar2,
                           av_column20 in varchar2,
                           av_column21 in varchar2,
                           av_column22 in varchar2,
                           av_column23 in varchar2,
                           av_column24 in varchar2,
                           av_column25 in varchar2,
                           av_column26 in varchar2)
  is
     v_tablename        varchar2(32);
     v_ref_sql          varchar2(1000);
     v_del_head         varchar2(100);
     v_del_body         varchar2(32767);
     v_col_cnt          number(2);
     v_coltype          varchar2(106);
     v_sql              varchar2(32767);
     ref_cur            ref_cursor;
     v_array            tp_array;
     v_rowtype          param_exp_imp_column_def%rowtype;
  begin
     --初始化传入参数
     v_array := tp_array(av_column1,av_column2,av_column3,av_column4,av_column5,av_column6,av_column7,av_column8,
                         av_column9,av_column10,av_column11,av_column12,av_column13,av_column14,av_column15,
                         av_column16,av_column17,av_column18,av_column19,av_column20,av_column21,av_column22,
                         av_column23,av_column24,av_column25,av_column26);
     --获取表名，模块类型为3:参数同步
     select sql_text into v_tablename from param_exp_imp_module t
         where module_type=3 and module_id=av_module_id and inuse=1;
     --按照列进行循环,必须按照COLUMN_ID排序，程序中在向存储过程传入参数是的map也是按照column_id进行的
     v_ref_sql:='select * from param_exp_imp_column_def t where t.module_id='
                   ||av_module_id||' and t.inuse=1 order by t.COLUMN_ID';
     --列数
     v_col_cnt:=0;
     --
     v_del_head:='Delete From '||v_tablename||' Where ';
     open ref_cur for v_ref_sql;
     loop
        fetch ref_cur into v_rowtype;
        exit when ref_cur%notfound;
        v_col_cnt:=v_col_cnt+1;
        --判断当前列的类型是否是DATE型
        v_coltype:=af_get_column_type(v_tablename,v_rowtype.column_id);
        if(v_coltype='DATE') then
           --判断日期是否为空
           if v_array(v_col_cnt)='' or v_array(v_col_cnt) is null then
              null;
			  dbms_output.put_line('10');
           else
              v_del_body:=v_del_body||v_rowtype.column_id||'=to_date('''||v_array(v_col_cnt)||''',''yyyy-mm-dd hh24:mi:ss'') and ';
			  dbms_output.put_line('11');
           end if;
        else
           if v_array(v_col_cnt)='' or v_array(v_col_cnt) is null then
              null;
			  dbms_output.put_line('12');
           else
              v_del_body:=v_del_body||v_rowtype.column_id||'='''||v_array(v_col_cnt)||''' and ';
			  dbms_output.put_line('13');
           end if;
        end if;
     end loop;
     close ref_cur;
     v_sql:=v_del_head||v_del_body||' 1=1';
     --execute immediate v_sql;
     begin
          execute immediate v_sql;
		  dbms_output.put_line('14');
     exception
       when others then
          raise_application_error(-20001,'插入数据失败'||chr(10)||sqlerrm||chr(10)||'SQL: '||v_sql);
          return;
     end;
     --提交数据
     commit work;

  end ap_sync_parameter_del;

  --同步数据过程之插入数据平台系统正确的参数
  procedure ap_sync_parameter_ins(av_module_id in number,
                           av_column1 in varchar2,
                           av_column2 in varchar2,
                           av_column3 in varchar2,
                           av_column4 in varchar2,
                           av_column5 in varchar2,
                           av_column6 in varchar2,
                           av_column7 in varchar2,
                           av_column8 in varchar2,
                           av_column9 in varchar2,
                           av_column10 in varchar2,
                           av_column11 in varchar2,
                           av_column12 in varchar2,
                           av_column13 in varchar2,
                           av_column14 in varchar2,
                           av_column15 in varchar2,
                           av_column16 in varchar2,
                           av_column17 in varchar2,
                           av_column18 in varchar2,
                           av_column19 in varchar2,
                           av_column20 in varchar2,
                           av_column21 in varchar2,
                           av_column22 in varchar2,
                           av_column23 in varchar2,
                           av_column24 in varchar2,
                           av_column25 in varchar2,
                           av_column26 in varchar2)
  is
     v_tablename        varchar2(32);
     v_ref_sql          varchar2(1000);
     v_ins_head         varchar2(100);
     v_sql_body_field   varchar2(32767);
     v_sql_body_value   varchar2(32767);
     v_col_cnt          number(2);
     v_coltype          varchar2(106);
     v_sql              varchar2(32767);
     ref_cur            ref_cursor;
     v_array            tp_array;
     v_rowtype          param_exp_imp_column_def%rowtype;
  begin
     --初始化传入参数
     v_array := tp_array(av_column1,av_column2,av_column3,av_column4,av_column5,av_column6,av_column7,av_column8,
                         av_column9,av_column10,av_column11,av_column12,av_column13,av_column14,av_column15,
                         av_column16,av_column17,av_column18,av_column19,av_column20,av_column21,av_column22,
                         av_column23,av_column24,av_column25,av_column26);
     --获取表名，模块类型为3:参数同步
     select sql_text into v_tablename from param_exp_imp_module t
         where module_type=3 and module_id=av_module_id and inuse=1;
     --按照列进行循环,必须按照COLUMN_ID排序
     v_ref_sql:='select * from param_exp_imp_column_def t where t.module_id='
                   ||av_module_id||' and t.inuse=1 order by t.COLUMN_ID';
     --列数
     v_col_cnt:=0;
     --
     v_ins_head:='Insert Into '||v_tablename||'(';
     v_sql_body_field:='';
     v_sql_body_value:='';
     open ref_cur for v_ref_sql;
     loop
        fetch ref_cur into v_rowtype;
        exit when ref_cur%notfound;
        v_col_cnt:=v_col_cnt+1;
        --判断当前列的类型是否是DATE型
        v_coltype:=af_get_column_type(v_tablename,v_rowtype.column_id);
        if(v_coltype='DATE') then
           --判断日期是否为空
           if v_array(v_col_cnt)='' or v_array(v_col_cnt) is null then
              null;
			  dbms_output.put_line('15');
           else
              v_sql_body_field:=v_sql_body_field||v_rowtype.column_id||',';
              v_sql_body_value:=v_sql_body_value||'to_date('''||v_array(v_col_cnt)||''',''yyyy-mm-dd hh24:mi:ss''),';
			  dbms_output.put_line('16');
           end if;
        else
           if v_array(v_col_cnt)='' or v_array(v_col_cnt) is null then
              null;
			  dbms_output.put_line('17');
           else
              v_sql_body_field:=v_sql_body_field||v_rowtype.column_id||',';
              v_sql_body_value:=v_sql_body_value||''''||v_array(v_col_cnt)||''',';
			  dbms_output.put_line('18');
           end if;
        end if;
     end loop;
     close ref_cur;
     v_sql:=v_ins_head||substr(v_sql_body_field,1,length(v_sql_body_field)-1)||') values ('
                      ||substr(v_sql_body_value,1,length(v_sql_body_value)-1)||')';
     begin
          execute immediate v_sql;
		  dbms_output.put_line('19');
     exception
       when others then
          raise_application_error(-20001,'插入数据失败'||chr(10)||sqlerrm||chr(10)||'SQL: '||v_sql);
          return;
     end;
     --不可以提交数据
     commit work;

  end ap_sync_parameter_ins;


end apk_common_data_import;



$

CREATE OR REPLACE PACKAGE  "PKG_SOX_STAT" is

    -- Author  : 4A_XIAZHIYONG
    -- Created : 2009-10-20 9:14:11
    -- Purpose : 萨班斯检查相关数据支撑


    --文件非空性检查
    procedure ap_stat_empty_file(av_statday in varchar2 default to_char(sysdate-1,'yyyymmdd'));

    --采集与分拣文件大小平衡性检查
	--包体中无此procedure
    --procedure ap_stat_collect_sep_filesize(av_statday in varchar2 default to_char(sysdate-1,'yyyymmdd'));   

    --话单文件连续性检查
    /*
     参数说明:
         av_first_conn:0,不需要检查本日第一个文件与前日最后一个文件的连续性;
                       1,需要检查本日第一个文件与前日最后一个文件的连续性;
    */
    procedure ap_stat_cdr_continuity(av_statday in varchar2 default to_char(sysdate-1,'yyyymmdd'),av_first_conn in number);


    --
    procedure ap_check_config(av_statday in varchar2 default to_char(sysdate-1,'yyyymmdd'));

end pkg_sox_stat;


$

  CREATE OR REPLACE PACKAGE BODY  "PKG_SOX_STAT" is

    --采集非空性(空单,非Put的话单)
    procedure ap_stat_empty_file(av_statday in varchar2)
    is
        v_count         number(16);
        v_all_count     number(16);
        v_partition     varchar2(32);
        v_sql           varchar2(2048);
        v_tablename     varchar2(32);
    begin
        v_tablename:='CPRUN_'||substr(av_statday,1,6);
        v_partition:='P_00'||substr(av_statday,-2);
        --取配置历史表中昨天在用的非Put的采集配置
        for cur in (SELECT node,id,DESCRIPTION,operation_type,T.HOSTIP_PAIR FROM  cpconfig_his T
                        WHERE T.LOGTIME>=TO_DATE(av_statday,'YYYYMMDD')
                              AND T.LOGTIME<TO_DATE(av_statday,'YYYYMMDD')+1
                              AND T.OPERATION_TYPE<>'P'
                              and t.id not in('40j',
                                              '40k',
                                              '44d',
                                              '70c',
                                              '80c',
                                              '84h',
                                              'qdags18',
                                              '89j3',
                                              '89j4',
                                              '89j1'
                                              )

                   )
        loop
           v_sql:='select count(*)  from '||v_tablename||' partition ('||v_partition||')'
                   ||' where node=:node and id=:id and filesize=0 and day=:day';

            execute immediate v_sql into v_count using cur.node,cur.id,substr(av_statday,-2);

            if v_count>0 then
                v_sql:='select count(*)  from '||v_tablename||' partition ('||v_partition||')'
                       ||' where node=:node and id=:id and day=:day';
                execute immediate v_sql into v_all_count using cur.node,cur.id,substr(av_statday,-2);
                insert into sox_check_empty_file(statday,node,id,discritption,operation_type,zero_cnt,all_count)
                    values(av_statday,cur.node,cur.id,cur.description,cur.operation_type,v_count,v_all_count);
					dbms_output.put_line('1');
            end if;
        end loop;
        --提交
        commit;
    end ap_stat_empty_file;

    --话单连续性(非Put的话单，配置统计到了sox_cdr_continuity_config表中)
    procedure ap_stat_cdr_continuity(av_statday in varchar2,av_first_conn in number)
    is
        type tp_refcusor is ref cursor;
        v_refcur         tp_refcusor;
        type tp_record is record(node     VARCHAR2(1024),
                                 id       VARCHAR2(1024),
                                 filename VARCHAR2(1024),
                                 fileno   number(20));
        v_record         tp_record;
        v_record_null    tp_record;
        v_sql            varchar2(2048);
        v_tablename      varchar2(32);
        v_tablename_next varchar2(32);
        v_day            varchar2(32);
        v_day_next       varchar2(32);
        v_stat_day_next  varchar2(32);
        v_lastday        varchar2(32);
        v_partition      varchar2(32);
        v_partition_next varchar2(32);
        v_pre_no         number(20);
        v_pre_filename   varchar2(1024);
        v_cur_no         number(20);
        v_cur_filename   varchar2(1024);
        v_cnt            number(20);
    begin
        --
        v_day:=substr(av_statday,-2);
        v_tablename:='CPRUN_'||substr(av_statday,1,6);
        v_partition:='P_00'||v_day;
        --正常处理逻辑,需要取昨天最后一个文件跟今天的比对
        for cur in (select * from sox_cdr_continuity_config where inuse=1 )
        loop
            --判断统计当日日志表分区里是否包含相应的采集/分发网元的信息
            v_sql:='select count(*) from '||v_tablename||' partition('||v_partition||')'
                 --||' where node='''||cur.node||''' and id='''||cur.id||''' and filename like '''||cur.pattern||'''';
                 ||' where id='''||cur.id||''' and filename like '''||cur.pattern||'''';
            execute immediate v_sql into v_cnt;
            --日志表中有数据才处理
            if v_cnt>0 then
                --按最大序号翻转
                if cur.isdayreverse=0 then
                    --置上一次的文件名/序号
                    if av_first_conn=0 then
                        --从配置表中获取上次的信息
                        v_pre_filename:=cur.lastchkfilename;
                        v_pre_no:=substr(v_pre_filename,cur.filenostartpos,cur.filenoendpos-cur.filenostartpos+1);
						dbms_output.put_line('2');
                        if v_pre_no is null then
                           v_pre_no:=0;
						   dbms_output.put_line('3');
                        end if;
                    else
                        --以当前的采集日志中第一个文件的序号为开始序号(一定注意要初始化记录)
                        v_record:=v_record_null;
                        v_record.node:=cur.node;
                        v_record.id:=cur.id;
                        v_record.filename:='';
                        v_record.fileno:=0;
                        v_pre_no:=0;
                        --初始化上一个文件名（gsc 2015-3-2）
                        v_pre_filename:='';
						dbms_output.put_line('4');
                    end if;
                    --开始判断
                   -- if cur.id='30e2' then
                      v_sql:='select * from ( select node,id,filename,substr(filename,'||cur.filenostartpos||','||cur.filenoendpos||'-'||cur.filenostartpos||'+1) fileno '
                         ||' from '||v_tablename||' partition('||v_partition||') t where t.id='''||cur.id
                         --||''' and node='''||cur.node||''' and day='''||v_day
                         ||''' and day='''||v_day
                         ||''' and filename like '''||cur.pattern||'''and filename not like ''%.999'' order by begintime,filename)';
                        /* and filename not like '''%.999'''*/
                  /*  else
                      v_sql:='select * from (select node,id,filename,substr(filename,'||cur.filenostartpos||','||cur.filenoendpos||'-'||cur.filenostartpos||'+1) fileno '
                           ||' from '||v_tablename||' partition('||v_partition||') t where t.id='''||cur.id
                           --||''' and node='''||cur.node||''' and day='''||v_day
                           ||''' and day='''||v_day
                           ||''' and substr(filename,'||cur.filenostartpos||','||cur.filenoendpos||'-'||cur.filenostartpos||'+1)>'||v_pre_no
                           ||' and filename like '''||cur.pattern||''' order by filename)'
                           ||' union all '
                           ||'select * from ( select node,id,filename,substr(filename,'||cur.filenostartpos||','||cur.filenoendpos||'-'||cur.filenostartpos||'+1) fileno '
                           ||' from '||v_tablename||' partition('||v_partition||') t where t.id='''||cur.id
                           --||''' and node='''||cur.node||''' and day='''||v_day
                           ||''' and day='''||v_day
                           ||''' and substr(filename,'||cur.filenostartpos||','||cur.filenoendpos||'-'||cur.filenostartpos||'+1)<'||v_pre_no
                           ||' and filename like '''||cur.pattern||''' order by filename)';
                    end if;*/ --修改排序方式(hxy 20150518)
                    --获取采集日志数据开始处理
                    --gv_sql:=v_sql;
                    open v_refcur for v_sql;
                    loop
                        fetch v_refcur into v_record;
                        exit when v_refcur%notfound;
                        v_cur_filename:=v_record.filename;
                        v_cur_no:=substr(v_cur_filename,cur.filenostartpos,cur.filenoendpos-cur.filenostartpos+1);
                        --
                        if v_cur_no<>v_pre_no+1 then
                            --不到最大序号
                            if v_pre_no<>cur.maxreverseno then
                                insert into sox_check_files_conn(statday,node,id,discritption,fromfilename,tofilename)
                                    values(av_statday,v_record.node,v_record.id,cur.description,v_pre_filename,v_cur_filename);
									dbms_output.put_line('5');
                            else
                                --到了最大序号，开始序号却不等于默认开始序号
                                if v_cur_no<>cur.minstartno then
                                    insert into sox_check_files_conn(statday,node,id,discritption,fromfilename,tofilename)
                                        values(av_statday,v_record.node,v_record.id,cur.description,v_pre_filename,v_cur_filename);
										dbms_output.put_line('6');
                                end if;
                            end if;
                        end if;
                        --置上一次的文件名/序号
                        v_pre_filename:=v_cur_filename;
                        v_pre_no:=substr(v_pre_filename,cur.filenostartpos,cur.filenoendpos-cur.filenostartpos+1);
                    end loop;
                    close v_refcur;
                    --更新配置中本次检查的最后一个文件名
                    update sox_cdr_continuity_config t
                         set t.lastchkfilename=v_pre_filename
                         where t.node=cur.node and t.id=cur.id and t.pattern=cur.pattern;
                    commit;
                --按天翻转,数量一定(是否必要)
                elsif cur.isdayreverse=1 then
                    --v_day是否最后1天
                    v_lastday:=to_char(last_day(trunc(to_date(av_statday,'yyyymmdd'))-1),'dd');
                    --统计日av_statday是当月的最后1天
                    if v_day=v_lastday then
                        v_day_next:=to_char(to_date(av_statday,'yyyymmdd')+1,'dd');
                        v_partition_next:='P_00'||v_day_next;
                        v_stat_day_next:=to_char(to_date(av_statday,'yyyymmdd')+1,'yyyymmdd');
                        v_tablename_next:='CPRUN_'||substr(v_stat_day_next,1,6);
                        v_sql:='select * from (select node,id,filename,substr(filename,'||cur.filenostartpos||','||cur.filenoendpos||'-'||cur.filenostartpos||'+1) fileno '
                                 ||' from '||v_tablename||' partition('||v_partition||') t '
                                 --||' where t.id='''||cur.id||''' and node='''||cur.node||''' and day='''||v_day||''''
                                 ||' where t.id='''||cur.id||''' and day='''||v_day||''''
                                 ||' and filename like '''||cur.pattern||''''
                                 ||' and filename like ''%'||av_statday||'%'' order by filename)'
                                 ||' union all '
                                 ||'select * from (select node,id,filename,substr(filename,'||cur.filenostartpos||','||cur.filenoendpos||'-'||cur.filenostartpos||'+1) fileno '
                                 ||' from '||v_tablename_next||' partition('||v_partition_next||') t '
                                 --||'where t.id='''||cur.id||''' and node='''||cur.node||''' and day='''||v_day_next||''''
                                 ||'where t.id='''||cur.id||''' and day='''||v_day_next||''''
                                 ||' and filename like '''||cur.pattern||''''
                                 ||' and filename like ''%'||av_statday||'%'' order by filename)';
								 dbms_output.put_line('7');
                    else
                        v_day_next:=to_char(to_date(av_statday,'yyyymmdd')+1,'dd');
                        v_partition_next:='P_00'||v_day_next;
                        v_stat_day_next:=to_char(to_date(av_statday,'yyyymmdd')+1,'yyyymmdd');
                        v_sql:='select * from (select node,id,filename,substr(filename,'||cur.filenostartpos||','||cur.filenoendpos||'-'||cur.filenostartpos||'+1) fileno '
                                 ||' from '||v_tablename||' partition('||v_partition||') t '
                                 --||' where t.id='''||cur.id||''' and node='''||cur.node||''' and day='''||v_day||''''
                                 ||' where t.id='''||cur.id||''' and day='''||v_day||''''
                                 ||' and filename like '''||cur.pattern||''''
                                 ||' and filename like ''%'||av_statday||'%'' order by filename)'
                                 ||' union all '
                                 ||'select * from (select node,id,filename,substr(filename,'||cur.filenostartpos||','||cur.filenoendpos||'-'||cur.filenostartpos||'+1) fileno '
                                 ||' from '||v_tablename||' partition('||v_partition_next||') t '
                                 --||'where t.id='''||cur.id||''' and node='''||cur.node||''' and day='''||v_day_next||''''
                                 ||'where t.id='''||cur.id||''' and day='''||v_day_next||''''
                                 ||' and filename like '''||cur.pattern||''''
                                 ||' and filename like ''%'||av_statday||'%'' order by filename)';
								 dbms_output.put_line('8');
                    end if;
                    --
                    open v_refcur for v_sql;
                    --第一次执行
                    v_record:=v_record_null;
                    fetch v_refcur into v_record;
                    if v_record.node is not null then
                        v_pre_filename:=v_record.filename;
                        v_pre_no:=substr(v_pre_filename,cur.filenostartpos,cur.filenoendpos-cur.filenostartpos+1);
						dbms_output.put_line('9');
                    end if;
                    loop
                        fetch v_refcur into v_record;
                        exit when v_refcur%notfound;
                        v_cur_filename:=v_record.filename;
                        v_cur_no:=substr(v_cur_filename,cur.filenostartpos,cur.filenoendpos-cur.filenostartpos+1);
                        if v_cur_no<>v_pre_no+1 then
                            --不连续日志
                            insert into sox_check_files_conn(statday,node,id,discritption,fromfilename,tofilename)
                                values(av_statday,v_record.node,v_record.id,cur.description,v_pre_filename,v_cur_filename);
								dbms_output.put_line('10');
                        end if;
                        v_pre_no:=v_cur_no;
                        v_pre_filename:=v_cur_filename;
                    end loop;
                    close v_refcur;
                    --没到最大文件序号
                    if v_cur_no<>cur.maxreverseno then
                       --结束文件序号不等于文件的最小结束序号的不连续日志
                       /*
                       insert into sox_check_files_conn(statday,node,id,discritption,fromfilename,tofilename)
                           values(av_statday,v_record.node,v_record.id,cur.description,v_pre_filename,'MAX_DAY_ENDFILE');
                       */
                       null;
					   dbms_output.put_line('11');
                    end if;
                    commit;
                --按天翻转,数量不定
                elsif cur.isdayreverse=2 then
                    v_sql:='select node,id,filename,substr(filename,'||cur.filenostartpos||','||cur.filenoendpos||'-'||cur.filenostartpos||'+1) fileno '
                             ||' from '||v_tablename||' partition('||v_partition||') t '
                             --||' where t.id='''||cur.id||''' and node='''||cur.node||''' and day='''||v_day||''''
                             ||' where t.id='''||cur.id||''' and day='''||v_day||''''
                             ||' and filename like '''||cur.pattern||''''
                             ||' and filename like ''%'||av_statday||'%'' order by filename';
                    open v_refcur for v_sql;
                    --第一次执行
                    v_record:=v_record_null;
                    fetch v_refcur into v_record;
                    if v_record.node is not null then
                        v_pre_filename:=v_record.filename;
                        v_pre_no:=substr(v_pre_filename,cur.filenostartpos,cur.filenoendpos-cur.filenostartpos+1);
                        --
                        if v_pre_no<>cur.minstartno then
                            --开始文件序号不等于文件的最小开始序号的不连续日志
                            insert into sox_check_files_conn(statday,node,id,discritption,fromfilename,tofilename)
                                values(av_statday,v_record.node,v_record.id,cur.description,'MIN_DAY_STARTFILE',v_pre_filename);
								dbms_output.put_line('12');
                        end if;
                    end if;
                    --
                    loop
                        fetch v_refcur into v_record;
                        exit when v_refcur%notfound;
                        v_cur_filename:=v_record.filename;
                        v_cur_no:=substr(v_cur_filename,cur.filenostartpos,cur.filenoendpos-cur.filenostartpos+1);
                        if v_cur_no<>v_pre_no+1 then
                            --不到最大序号
                            insert into sox_check_files_conn(statday,node,id,discritption,fromfilename,tofilename)
                                values(av_statday,v_record.node,v_record.id,cur.description,v_pre_filename,v_cur_filename);
								dbms_output.put_line('13');
                        end if;
                        v_pre_no:=v_cur_no;
                        v_pre_filename:=v_cur_filename;
                    end loop;
                    close v_refcur;
                else
                    --系统未定义
					dbms_output.put_line('14');
                    Raise_Application_error(-20001,'系统未定义的isdayreverse值，请联系张博洋检查！');
					
                end if;
                commit;
        /*
        exception
            when others then
            dbms_output.put_line(gv_id);
            dbms_output.put_line(substr(gv_sql,1,250));
            dbms_output.put_line(substr(gv_sql,251,500));
            dbms_output.put_line(substr(gv_sql,501,750));
            dbms_output.put_line(substr(gv_sql,751,1000));
        */

            end if;
        end loop;

        --assurance commit
        commit;

    end ap_stat_cdr_continuity;


    --
    procedure ap_check_config(av_statday in varchar2)
    is
        --v_cnt               number(10);
        v_statday           varchar2(8);
        v_cur_day           varchar2(8);
        v_cur_hour          varchar2(2);
    begin
        v_statday:=av_statday;
        v_cur_day:=to_char(sysdate,'YYYYMMDD');
        --统计当前日的
        if v_statday=v_cur_day then
            select to_char(sysdate,'HH24') INTO v_cur_hour FROM DUAL;
            --可以从表中取出当天的配置进行比对，否则需要比对昨天/前天的配置
            IF v_cur_day>='06' THEN
                insert into sox_check_cpconfig(NODE,ID,DESCRIPTION,OPERATION_TYPE,GATHER_MODE,HOSTIP_PAIR,USER_PAIR,PASSWORD_PAIR,ENCRYPTION,SOURCE_PATTERN,SOURCE_NAME_FILTER,SOURCE_MOVEDEL,SOURCE_PATH,SOURCE_BACKUP_PATH,DEST_PATH,DEST_NAME_FORMAT,BUSY_TIME,BUSY_INTERVAL,FREE_INTERVAL,NOFILE_LIMIT,FILE_ANALYZER,SEQUENCE_CONTROL,SEQUENCE_SCOPE,SEQUENCE_SYNC,WARN_MODE,RECORD_SIZE,FTP_TIMEOUT,FTP_RESUME,FTP_PASSIVE,SIZE_COL,EXTRACOPY,STATUS)
                       select NODE,ID,DESCRIPTION,OPERATION_TYPE,GATHER_MODE,HOSTIP_PAIR,USER_PAIR,PASSWORD_PAIR,ENCRYPTION,SOURCE_PATTERN,SOURCE_NAME_FILTER,SOURCE_MOVEDEL,SOURCE_PATH,SOURCE_BACKUP_PATH,DEST_PATH,DEST_NAME_FORMAT,BUSY_TIME,BUSY_INTERVAL,FREE_INTERVAL,NOFILE_LIMIT,FILE_ANALYZER,SEQUENCE_CONTROL,SEQUENCE_SCOPE,SEQUENCE_SYNC,WARN_MODE,RECORD_SIZE,FTP_TIMEOUT,FTP_RESUME,FTP_PASSIVE,SIZE_COL,EXTRACOPY,STATUS
                           from cpconfig
                       minus
                       select NODE,ID,DESCRIPTION,OPERATION_TYPE,GATHER_MODE,HOSTIP_PAIR,USER_PAIR,PASSWORD_PAIR,ENCRYPTION,SOURCE_PATTERN,SOURCE_NAME_FILTER,SOURCE_MOVEDEL,SOURCE_PATH,SOURCE_BACKUP_PATH,DEST_PATH,DEST_NAME_FORMAT,BUSY_TIME,BUSY_INTERVAL,FREE_INTERVAL,NOFILE_LIMIT,FILE_ANALYZER,SEQUENCE_CONTROL,SEQUENCE_SCOPE,SEQUENCE_SYNC,WARN_MODE,RECORD_SIZE,FTP_TIMEOUT,FTP_RESUME,FTP_PASSIVE,SIZE_COL,EXTRACOPY,STATUS
                           from cpconfig_his
                           where trunc(logtime)=to_date(v_statday,'yyyymmdd')-1;
						   dbms_output.put_line('15');

            END IF;
        end if;
        --
        commit;
    end ap_check_config;


end pkg_sox_stat;


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
       commit;
	   dbms_output.put_line('1');
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
					dbms_output.put_line('4');
                    --如果目标记录不存在，则先增加记录再更新
                    if sql%rowcount=0 then
                         insert into CDR_BALANCECHECK_DETAIL(procdate,filename,sect_src,sect_dst,flow_type)
                                values(c1.proc_date,c1.filename, v_sect_src, v_sect_dst, v_flow_type);
                         execute immediate(v_update_sql);
						 dbms_output.put_line('5');
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
				dbms_output.put_line('6');
			else
				v_cmp_result := 0;
				dbms_output.put_line('7');
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
	dbms_output.put_line('8');
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
				dbms_output.put_line('9');
					return 1;
				end if;
				v_service_code := substr(v_service_code,v_tmp_pos+1);
				v_tmp_pos := instr(v_service_code,',');
			end loop;
			--如果未匹配到合适的业务类型，返回0
			dbms_output.put_line('10');
			return 0;
		end loop;
		dbms_output.put_line('11');
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
	dbms_output.put_line('12');
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
			dbms_output.put_line('13');
    end loop;
  else  --分阶段稽核
    v_tmp_sect  := sect_str;
    --如果v_tmp_sect不是以','结尾,则主动添加一个,方便下面的算法
    if(substr(v_tmp_sect,length(v_tmp_sect)-1,1)<>',') then
        v_tmp_sect := v_tmp_sect||',';
		dbms_output.put_line('14');
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
			dbms_output.put_line('15');
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
					dbms_output.put_line('16');
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
				dbms_output.put_line('17');
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
	   dbms_output.put_line('18');
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
							dbms_output.put_line('19');
					end if;
                    --实现阶段翻转
                    v_sect_tmp      := v_sect_dst;
                    v_sect_dst      := v_sect_src;
                    v_sect_src      := v_sect_tmp;
                    v_calc_factor   := -1;  --计算因子取-1,实现后面统计数据取反值
                else
                    v_calc_factor  := 1;
					dbms_output.put_line('20');
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
					dbms_output.put_line('21');
                    --如果目标记录不存在，则先增加记录再更新
                    if sql%rowcount=0 then
                         insert into CDR_BALANCECHECK_DETAIL(procdate,filename,sect_src,sect_dst,flow_type)
                                values(proc_date,c1.filename, v_sect_src, v_sect_dst, v_flow_type);
                         execute immediate(v_update_sql);
						 dbms_output.put_line('22');
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
				dbms_output.put_line('23');
			else
				v_cmp_result := 0;
				dbms_output.put_line('24');
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
		dbms_output.put_line('8');
    end;
    end loop;
    if v_result = 1 then
        dbms_output.put_line('In the entity_attr_def table, attr_class check-up have no problem.');
		dbms_output.put_line('9');
    else
        v_msg := 'A attribute class have many attribute in a entity, as follows:';
        v_rst := af_insert_data_tab(v_tmpserialno, v_msg);
		dbms_output.put_line('10');
        if v_rst = -1 then
		dbms_output.put_line('11');
            return -1;
        end if;
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
	  dbms_output.put_line('12');
          return false;
      end if;
      g_serialno := g_serialno + 1;
      a_result := 0;
	  dbms_output.put_line('13');
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
		  dbms_output.put_line('14');
      end if;

      select count(*) into v_count from free_usage_attr_def where entity_id=r.entity_id and attr_id=r.attr_id;
      if v_count > 0 then
          v_ret := checkit(r, v_result, '  total usage attr has defination in table free_usage_attr_def.');
		  dbms_output.put_line('15');
      end if;
      if v_ret = false then
	  dbms_output.put_line('16');
          return -1;
      end if;
	  end loop;

  --检查赠送量属性在累计量定义中有配置
	  for r in ( select * from entity_attr_def where attr_type=4 order by entity_id,attr_id )
	  loop
      select count(*) into v_count from free_usage_attr_def where entity_id=r.entity_id and attr_id=r.attr_id;
      if v_count > 1 then
          v_ret := checkit(r, v_result, '  free usage attr has more than 1 defination in table free_usage_attr_def.');
		  dbms_output.put_line('17');
      end if;

      select count(*) into v_count from total_usage_attr_def where entity_id=r.entity_id and attr_id=r.attr_id;
      if v_count > 0 then
          v_ret := checkit(r, v_result, '  free usage attr has defination in table total_usage_attr_def.');
		  dbms_output.put_line('18');
      end if;
      if v_ret = false then
	  dbms_output.put_line('19');
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
		dbms_output.put_line('20');
    else
        v_msg := 'Check charging_event attribute continuity ，event problem as follows:';
        v_rst := af_insert_data_tab(v_tmpserialno, v_msg);
        if v_rst = -1 then
		dbms_output.put_line('21');
            return -1;
        end if;
    end if;
    return v_result;
  end;

  function af_insert_data_tab(a_serialno number, a_msg varchar2) return number is
  begin
      insert into wrk_check_param_infotab(serialno , content) values(a_serialno, a_msg);
	  dbms_output.put_line('22');
      return 1;
  exception when others then
      return -1;
  end;

end pkg_check_param;

$





delimiter ;$


