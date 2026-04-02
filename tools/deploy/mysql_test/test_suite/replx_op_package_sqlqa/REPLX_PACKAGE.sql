delimiter //;
  CREATE OR REPLACE PACKAGE "PKG_REPLX_EVT" is
    procedure InsertLog(
        row_id in varchar2, 
        towner in varchar2,
        tname  in varchar2, 
        operation in char,
        key    in varchar2,
        hash_val in number,
        region in varchar2 := '300',
        priority in number := 0
    );
    procedure SetTriggerFlag(flag in number);
    procedure SetSrcDBName(src_db in varchar2);
end pkg_replx_evt;
//


  CREATE OR REPLACE PACKAGE "PKG_REPLX_TOOLKIT" is

  T_REPL_SYNC    constant varchar2(8) := 'SYNC';  
  T_REPL_CYCLE   constant varchar2(8) := 'CYCLE'; 
  T_REPL_MOVE    constant varchar2(8) := 'MOVE'; 

  OP_ALL     constant varchar2(3) := 'IDU';
  OP_DEFAULT constant varchar2(3) := 'IU';

 
  procedure ReplicateTable(
      table_owner in varchar2,
      table_name  in varchar2,
      region_col  in varchar2 := null,
      intime_col  in varchar2 := null,
      pkey_name   in varchar2 := null,
      dml_opt     in varchar2 := OP_ALL,
      priority    in number   := 0,
      repl_type   in varchar2 := T_REPL_SYNC,
      repl_cycle  in varchar2 := 'SYNC',
      fire_cond   in varchar2 := null
  );


  procedure ReplicateTable_new(
      table_owner in varchar2,
      table_name  in varchar2,
      region_col  in varchar2 := null,
      intime_col  in varchar2 := null,
      pkey_name   in varchar2 := null,
      dml_opt     in varchar2 := OP_ALL,
      priority    in number   := 0,
      repl_type   in varchar2 := T_REPL_SYNC,
      repl_cycle  in varchar2 := 'SYNC',
      fire_cond   in varchar2 := null
  );


   procedure ReplicateTable_new_pk(
      table_owner in varchar2,
      table_name  in varchar2,
      region_col  in varchar2 := null,
      intime_col  in varchar2 := null,
      pkey_name   in varchar2 := null,
      dml_opt     in varchar2 := OP_ALL,
      priority    in number   := 0,
      repl_type   in varchar2 := T_REPL_SYNC,
      repl_cycle  in varchar2 := 'SYNC',
      fire_cond   in varchar2 := null
  );

  procedure ReplicateTableA(
      table_owner in varchar2,
      table_name  in varchar2,
      region_col  in varchar2 := null, 
      pkey_name   in varchar2 := null, 
      priority    in number   := 0,
      dml_opt     in varchar2 := OP_ALL
  );

  procedure ReplicateTableL(
      table_owner in varchar2,
      table_name  in varchar2,
      region_col  in varchar2, 
      intime_col  in varchar2,
      fire_cond   in varchar2 := null,
      pkey_name   in varchar2 := null,  
      repl_cycle  in varchar2 := 'DAY'
  );

  procedure UnrepTable(
      table_owner in varchar2,
      table_name in varchar2
  );

 
  procedure GetTriggerSQL(
      table_owner in varchar2,
      table_name  in varchar2
  );

  procedure CreateTrigger(
      table_owner in varchar2,
      table_name  in varchar2
  );

  procedure TraceTriggerSQL;


  procedure ExecuteSQL(
      ddl_cmd in varchar2
  );

  function GetTriggerName(
      table_owner in varchar2,
      table_name  in varchar2
  ) return varchar2;

  procedure Reclaim(tname in varchar2 := 'REPLX_LOG');
end pkg_replx_toolkit;
//

--disable_warnings
--error 0,900
CREATE OR REPLACE PACKAGE BODY "PKG_REPLX_EVT" is
    DateNow  date;
    NextTime date;
    CurSID   number(2);
    PollGAP  number(4);
    SwitchFlag number(1);
    instance_number number(2);
    db_name varchar2(32);
    if_trigger binary_integer;
    procedure InitParam;
    procedure InsertLog(
        row_id in varchar2, 
        towner in varchar2, 
        tname  in varchar2, 
        operation in char,  
        key    in varchar2,
        hash_val in number,
        region in varchar2,
        priority in number
    )is
    begin
        if 0 = if_trigger then
            return;
        end if;

        DateNow := sysdate;
        if DateNow > NextTime then
            InitParam;
        end if;


        insert into replx_log
            (region, table_owner, table_name, operation, hash_val,
             row_id, pkey, instance_id,
             seqnum, priority, scn, src_db, intime)
        values
            (region, towner, tname, operation, hash_val,
             row_id, key, CurSID,
             replx_seq.nextval, priority, 0, db_name, DateNow);
    end;


    procedure SetTriggerFlag(flag in number)
    as
    begin
        if_trigger := flag;
    end;


    procedure SetSrcDBName(src_db in varchar2)
    as
    begin
        db_name := src_db;
    end;

    procedure InitParam
    is
    begin
        begin
            select switch_flag, poll_interval
              into SwitchFlag, PollGAP
              from replicate_switch
             where instance_id=instance_number and rownum<2;

            exception
                when others then
                    NextTime := DateNow+(1/24);
                    return;
        end;

        if SwitchFlag = 0 then
            CurSID := instance_number;
        else
            CurSID := instance_number*10;
        end if;
        NextTime := DateNow+(PollGAP/1440);
    end;

begin
    instance_number := 1;
    db_name := 'ob';
    SwitchFlag := 0;
    PollGAP := 60;
    CurSID := instance_number;
    if_trigger := 1;


    DateNow := sysdate;
    NextTime := DateNow+(1/24);

    begin
        select switch_flag, poll_interval
          into SwitchFlag, PollGAP
          from replicate_switch
         where instance_id=instance_number and rownum<2;

        exception
            when others then
                return;
    end;

    if SwitchFlag <> 0 then
        CurSID := instance_number*10;
    end if;
    NextTime := DateNow+(PollGAP/1440);

end pkg_replx_evt;
//


CREATE OR REPLACE PACKAGE BODY "PKG_REPLX_TOOLKIT" is
    user_name varchar2(64);
    sql_text dbms_sql.varchar2s;
    sql_rows binary_integer;
    procedure Append(text in varchar2, line_sep in varchar2);
    procedure Append(text in varchar2);
    function GetHashVal(
        v_tname in varchar2
    )return number is
    begin
        return 1;
    end;
    function GetTriggerName(
        table_owner in varchar2,
        table_name in varchar2
    ) return varchar2
    is
        l_trig varchar2(64);
        tname varchar2(64) := upper(table_name);
        towner varchar2(64) := upper(table_owner);
      
    begin
        select 'TRIG_REPLX30_'||object_id
          into l_trig
          from yd_objects a
         where a.object_name=tname and a.object_type='TABLE'
           and a.owner=towner;
        return l_trig;
    end;
    function GetRefCols(
        tname in varchar2
    ) return varchar2
    is
        l_cols varchar2(4096);
        l_cnt  number;
        l_lpad varchar2(12) := '        ';
    begin
        l_cols := '';
        select count(*) into l_cnt
        from replicate_table_cols
        where table_name=upper(tname) and need_copy=0;
        if l_cnt=0 then
            return l_cols;
        end if;

        l_cnt := 0;
        for c1 in (select * from replicate_table_cols
             where table_name=tname and need_copy<>0 order by sort_order
        )
        loop
            if l_cnt=0 then
                l_cols := l_lpad;
            else
                l_cols := l_cols||', ';
            end if;

            if mod(l_cnt, 6)=0 and l_cnt<> 0 then
                l_cols := l_cols||chr(10)||l_lpad;
            end if;
            l_cols := l_cols||c1.src_col;

            l_cnt := l_cnt + 1;
        end loop;
        l_cols := 'of'||chr(10)||l_cols||chr(10)||'    ';

        return l_cols;
    end;

    function GetTimeCol(
        towner in varchar2,
        tname  in  varchar2
    ) return varchar2
    is
        time_col varchar2(64);
    begin
        select t.column_name
         into time_col
         from yd_part_key_columns t
        where t.owner=towner and t.name=tname and t.object_type='TABLE'
           and exists(
             select 1 from yd_tab_columns a
             where a.owner=towner and a.table_name=tname
                and a.column_name=t.column_name and a.data_type='DATE'
        );
        return time_col;

        exception
            when others then
                return null;
    end;

    function GetDMLEvent(
        dml_opt in varchar2
    ) return varchar2
    is
        l_evt varchar2(1024);
    begin
        l_evt := '';
        if instr(dml_opt, 'I') > 0 then
            l_evt := 'insert';
        end if;

        if instr(dml_opt, 'D') > 0 then
            if length(l_evt) >  0 then
                l_evt := l_evt||' or delete';
            else
                l_evt := 'delete';
            end if;
        end if;

        if instr(dml_opt, 'U') > 0 then
            if length(l_evt) >  0 then
                l_evt := l_evt||' or update';
            else
                l_evt := 'update';
            end if;
        end if;

        return l_evt;
    end;

    procedure ReplicateTable(
        table_owner in varchar2,
        table_name in varchar2,
        region_col in varchar2,
        intime_col in varchar2,
        pkey_name  in varchar2,
        dml_opt    in varchar2,
        priority   in number,
        repl_type  in varchar2,
        repl_cycle in varchar2,
        fire_cond  in varchar2
    )
    is
        towner varchar2(64);
        tname  varchar2(64);
        l_rows number(12);
        l_idx  varchar2(32);
        l_hval number;
        l_null number;
    begin
        towner := upper(table_owner);
        tname := upper(table_name);
        l_hval := GetHashVal(tname);
        select count(*) into l_rows
        from replicate_table a
        where a.src_table=tname and a.src_owner=towner;

        if l_rows > 0 then
            raise_application_error(-20001, '11'||tname||'11');
        end if;

        if repl_type=T_REPL_CYCLE and
           (fire_cond is null or length(fire_cond)=0)
        then
            raise_application_error(-20002, '?'||tname||'??????,fire_cond????');
        end if;

        insert into replicate_table a
               (src_owner, src_table, dst_table, region_col, repl_type,
                key_opt, fire_cond, status, intime_col,
                dml_opt, priority, repl_cycle, hash_val )
        values (towner, tname, tname, region_col, repl_type,
                0, fire_cond, 1, intime_col,
                dml_opt, priority, repl_cycle, l_hval );

        delete from replicate_table_cols a
        where a.table_name=tname and a.table_owner=towner;

        l_rows := 0;
        for c1 in (select column_name, data_type, data_length, nullable, column_id
                   from yd_tab_cols a where table_name=tname and owner=towner)
        loop
            insert into replicate_table_cols
                (table_owner, table_name, src_col, dst_col, data_type,
                data_length, null_able, sort_order)
            values
                (towner, tname, c1.column_name, c1.column_name, c1.data_type,
                c1.data_length, decode(c1.nullable,'Y',1,0), c1.column_id);
            l_rows := l_rows+1;
        end loop;

        if l_rows = 0 then
            raise_application_error(-20001, '22'||towner||'.'||tname);
        end if;

        l_idx := null;
        if pkey_name is null then
            l_rows := 0;
            for c_cur in (select index_name, count(*) cols from yd_indexes a
                where a.table_name=tname and a.table_owner=towner
                    and a.uniqueness='UNIQUE' and owner=towner
                 group by index_name)
            loop
                if l_rows=0 or l_rows > c_cur.cols then
                    l_rows := c_cur.cols;
                    l_idx := c_cur.index_name;
                end if;
            end loop;

            if l_rows=0 and repl_type=T_REPL_SYNC then
                raise_application_error(-20002, tname||':33');

            end if;
        else
            l_idx := upper(pkey_name);
        end if;


        if l_idx = 'ALL' then

            update replicate_table_cols a
               set a.is_pkey=1
             where a.table_name=tname and a.table_owner=towner;
        elsif l_idx is not null then
            l_rows := 0;
            for c2 in (select column_name from yd_ind_columns a
                where a.index_name=l_idx and a.index_owner=towner)
            loop
                update replicate_table_cols a
                   set a.is_pkey=1
                 where a.table_name=tname and a.src_col=c2.column_name
                 returning a.null_able into l_null;
                l_rows := l_rows + sql%rowcount;
                if l_null=1 then
                    raise_application_error(-20002, '44'||l_idx||'44'||c2.column_name);
                end if;
            end loop;

            if l_rows=0 then
                raise_application_error(-20002, '55'||l_idx||'55');
            end if;
        end if;

        commit;
        return;

        exception
            when others then
                rollback;
                raise;
    end;


    procedure ReplicateTable_new(
        table_owner in varchar2,
        table_name in varchar2,
        region_col in varchar2,
        intime_col in varchar2,
        pkey_name  in varchar2,
        dml_opt    in varchar2,
        priority   in number,
        repl_type  in varchar2,
        repl_cycle in varchar2,
        fire_cond  in varchar2
    )
    is
        towner varchar2(64);
        tname  varchar2(64);
        l_rows number(12);
        l_idx  varchar2(32);
        l_hval number;
        l_null number;
    begin
        towner := upper(table_owner);
        tname := upper(table_name);
        l_hval := GetHashVal(tname);
        select count(*) into l_rows
        from replicate_table a
        where a.src_table=tname and a.src_owner=towner;

        if l_rows > 0 then
            raise_application_error(-20001, tname||'11');
        end if;

        if repl_type=T_REPL_CYCLE and
           (fire_cond is null or length(fire_cond)=0)
        then
            raise_application_error(-20002, tname||'22');
        end if;

        insert into replicate_table a
               (src_owner, src_table, dst_table, region_col, repl_type,
                key_opt, fire_cond, status, intime_col,
                dml_opt, priority, repl_cycle, hash_val )
        values (towner, tname, tname, region_col, repl_type,
                0, fire_cond, 1, intime_col,
                dml_opt, priority, repl_cycle, l_hval );

        delete from replicate_table_cols a
        where a.table_name=tname and a.table_owner=towner;

        l_rows := 0;
        for c1 in (select column_name, data_type, data_length, nullable, column_id
                   from yd_tab_cols a where table_name=tname and owner=towner)
        loop
            insert into replicate_table_cols
                (table_owner, table_name, src_col, dst_col, data_type,
                data_length, null_able, sort_order)
            values
                (towner, tname, c1.column_name, c1.column_name, c1.data_type,
                c1.data_length, decode(c1.nullable,'Y',1,0), c1.column_id);
            l_rows := l_rows+1;
        end loop;

        if l_rows = 0 then
            raise_application_error(-20001, towner||'.'||tname);
        end if;

        l_idx := null;
        if pkey_name is null then
            l_rows := 0;
            for c_cur in (select index_name, count(*) cols from yd_indexes a
                where a.table_name=tname and a.table_owner=towner
                    and a.uniqueness='UNIQUE' and owner=towner
                 group by index_name)
            loop
                if l_rows=0 or l_rows > c_cur.cols then
                    l_rows := c_cur.cols;
                    l_idx := c_cur.index_name;
                end if;
            end loop;

            if l_rows=0 and repl_type=T_REPL_SYNC then
                raise_application_error(-20002, tname||'33');

            end if;
        else
            l_idx := upper(pkey_name);
        end if;


        if l_idx = 'ALL' then

            update replicate_table_cols a
               set a.is_pkey=1
             where a.table_name=tname and a.table_owner=towner;
        elsif l_idx is not null then
            l_rows := 0;
            for c2 in (select column_name from yd_ind_columns a
                where a.index_name=l_idx and a.index_owner=towner)
            loop
                update replicate_table_cols a
                   set a.is_pkey=1
                 where a.table_name=tname and a.src_col=c2.column_name
                 returning a.null_able into l_null;
                l_rows := l_rows + sql%rowcount;
                if l_null=1 then

                    NULL;
                end if;
            end loop;

            if l_rows=0 then
                raise_application_error(-20002, l_idx||'44');
            end if;
        end if;

        commit;
        return;

        exception
            when others then
                rollback;
                raise;
    end;


    procedure ReplicateTable_new_pk(
        table_owner in varchar2,
        table_name in varchar2,
        region_col in varchar2,
        intime_col in varchar2,
        pkey_name  in varchar2,
        dml_opt    in varchar2,
        priority   in number,
        repl_type  in varchar2,
        repl_cycle in varchar2,
        fire_cond  in varchar2
    )
    is
        towner varchar2(64);
        tname  varchar2(64);
        l_rows number(12);
        l_idx  varchar2(32);
        l_pkx  varchar2(32);
        l_hval number;
        l_null number;
        l_pkn  number;
    begin
        towner := upper(table_owner);
        tname := upper(table_name);
        l_hval := GetHashVal(tname);
        select count(*) into l_rows
        from replicate_table a
        where a.src_table=tname and a.src_owner=towner;

        if repl_type=T_REPL_CYCLE and
           (fire_cond is null or length(fire_cond)=0)
        then
            raise_application_error(-20002, tname||'11');
        end if;

        if l_rows > 0 then

            DELETE FROM replicate_table t WHERE t.src_owner=towner AND t.src_table=tname;
            insert into replicate_table a
               (src_owner, src_table, dst_table, region_col, repl_type,
                key_opt, fire_cond, status, intime_col,
                dml_opt, priority, repl_cycle, hash_val )
            values (towner, tname, tname, region_col, repl_type,
                0, fire_cond, 1, intime_col,
                dml_opt, priority, repl_cycle, l_hval );

        end if;

       if l_rows = 0 then

        insert into replicate_table a
               (src_owner, src_table, dst_table, region_col, repl_type,
                key_opt, fire_cond, status, intime_col,
                dml_opt, priority, repl_cycle, hash_val )
        values (towner, tname, tname, region_col, repl_type,
                0, fire_cond, 1, intime_col,
                dml_opt, priority, repl_cycle, l_hval );
       END IF;
        delete from replicate_table_cols a
        where a.table_name=tname and a.table_owner=towner;

        l_rows := 0;
        for c1 in (select column_name, data_type, data_length, nullable, column_id
                   from yd_tab_cols a where table_name=tname and owner=towner)
        loop
            insert into replicate_table_cols
                (table_owner, table_name, src_col, dst_col, data_type,
                data_length, null_able, sort_order)
            values
                (towner, tname, c1.column_name, c1.column_name, c1.data_type,
                c1.data_length, decode(c1.nullable,'Y',1,0), c1.column_id);
            l_rows := l_rows+1;
        end loop;

        if l_rows = 0 then
            raise_application_error(-20001, towner||'.'||tname);
        end if;

        l_idx := null;
        if pkey_name is null then
            l_rows := 0;
            l_pkn  := 0;
            for c_cur in (select index_name, count(*) cols from yd_indexes a
                where a.table_name=tname and a.table_owner=towner
                    and a.uniqueness='UNIQUE' and owner=towner
                 group by index_name)
            loop
               l_rows := c_cur.cols;
               l_idx := c_cur.index_name;

               if l_idx is not null then

                  for c2 in (select column_name from yd_ind_columns a
                      where a.index_name=l_idx and a.index_owner=towner)
                  loop
                     SELECT null_able INTO l_null FROM replicate_table_cols a
                       where a.table_name=tname and a.src_col=c2.column_name;
                      l_rows := l_rows + sql%rowcount;

                      if l_null=1 THEN
                          l_pkn  := 0;
                          l_pkx := null;
                          GOTO pknext;
                      ELSE
                          l_pkn  := 1;
                          l_pkx := l_idx;

                      end if;
                  end loop;
                END IF;

                <<pknext>>

                IF l_pkn =1 THEN

                   for c2 in (select column_name from yd_ind_columns a
                        where a.index_name=l_pkx and a.index_owner=towner)
                    loop
                        update replicate_table_cols a
                           set a.is_pkey=1
                         where a.table_name=tname and a.src_col=c2.column_name;
                    end loop;

                    GOTO pkok;
                END IF;

            end loop;

            if l_rows=0 and repl_type=T_REPL_SYNC then
                raise_application_error(-20002, tname||'22');

            end if;
        ELSE

           if pkey_name <>'ALL' then
              l_idx := upper(pkey_name);

                for c2 in (select column_name from yd_ind_columns a
                        where a.index_name=l_idx and a.index_owner=towner)
                    loop
                        update replicate_table_cols a
                           set a.is_pkey=1
                         where a.table_name=tname and a.src_col=c2.column_name;
                end loop;
            END IF;

            l_idx := upper(pkey_name);

        end if;

        <<pkok>>

        if l_idx = 'ALL' then

            update replicate_table_cols a
               set a.is_pkey=1
             where a.table_name=tname and a.table_owner=towner;

        end if;

        commit;
        return;

        exception
            when others then
                rollback;
                raise;
    end;


    procedure ReplicateTableA(
        table_owner in varchar2,
        table_name  in varchar2,
        region_col  in varchar2,
        pkey_name   in varchar2,
        priority    in number,
        dml_opt     in varchar2
    )is
    begin
        ReplicateTable(
            table_owner=> table_owner,
            table_name => table_name,
            region_col => region_col,
            pkey_name  => pkey_name,
            repl_type  => T_REPL_SYNC,
            fire_cond  => null,
            priority   => priority,
            dml_opt    => dml_opt
        );
    end;
    procedure ReplicateTableL(
        table_owner in varchar2,
        table_name  in varchar2,
        region_col  in varchar2,
        intime_col  in varchar2,
        fire_cond   in varchar2,
        pkey_name   in varchar2,
        repl_cycle  in varchar2
    )is
        time_col varchar2(64);
        when_str varchar2(1024);
    begin
        if intime_col is null then
            time_col := GetTimeCol(upper(table_owner), upper(table_name));
            if time_col is null then
                raise_application_error(-20001, '???????????');
            end if;
        else
            time_col := intime_col;
        end if;

        if fire_cond is null then
            if repl_cycle = 'DAY' then
                when_str := 'old.'||time_col||'<trunc(sysdate)';
            end if;

            if repl_cycle = 'MONTH' then
                when_str := 'to_char(old.'||time_col||',''yymm'')<to_char(sysdate, ''yymm'')';
            end if;
        else
            when_str := fire_cond;
        end if;

        ReplicateTable(
            table_owner=> table_owner,
            table_name => table_name,
            region_col => region_col,
            intime_col => time_col,
            pkey_name  => pkey_name,
            repl_type  => T_REPL_CYCLE,
            fire_cond  => when_str,
            dml_opt    => 'U',
            repl_cycle => repl_cycle
        );
    end;

    procedure UnrepTable(
        table_owner in varchar2,
        table_name in varchar2
    )is
        towner varchar2(64) := upper(table_owner);
        tname varchar2(64) := upper(table_name);
        l_sql varchar2(1024);
    begin
        begin
            l_sql := 'drop trigger '||GetTriggerName(towner, tname);
            ExecuteSQL(l_sql);
            exception
                when others then
                    null;
        end;

        delete from replicate_table_cols a
        where a.table_name=tname and a.table_owner=towner;
        if sql%rowcount = 0 then
            raise_application_error(-20001, '????????:'||towner||'.'||tname);
        end if;

        delete from replicate_table a
        where a.src_table=upper(tname) and src_owner=towner;
        commit;

        exception
           when others then
               rollback;
               raise;
    end;


    procedure GenKeyData(
        table_owner in varchar2,
        table_name  in varchar2,
        operation   in char
    )is
        tname  varchar2(64) := upper(table_name);
        towner varchar2(32) := upper(table_owner);
        l_cnt  binary_integer := 0;
        l_key  varchar2(1024);
        l_coln varchar2(128);
        l_lpad varchar2(32);
        data_type varchar2(32);
    begin
        if operation = 'I' then
            data_type := ':new';
            l_lpad := '        ';
        else
            data_type := ':old';
            l_lpad := '    ';
        end if;

        for c1 in (select src_col, data_type, sort_order
                    from replicate_table_cols a
                   where a.table_owner=towner and a.table_name=tname
                    and a.is_pkey=1 order by sort_order)
        loop
            l_coln := data_type||'.'||c1.src_col;
            if c1.data_type = 'DATE' then
                l_key := ''''||c1.src_col||'=''||to_char('||l_coln||', ''yyyymmddhh24miss'')';
            else
                l_key := ''''||c1.src_col||'=''||'||l_coln;
            end if;

            if l_cnt > 0 then
                sql_text(sql_rows) := sql_text(sql_rows)||'||'',''||chr(3)||';
                Append(l_lpad||'          '||l_key);
            else
                l_key := l_lpad||'key_str :='||l_key;
                Append(l_key);
            end if;
            l_cnt := l_cnt + 1;
        end loop;
        sql_text(sql_rows) := sql_text(sql_rows)||';';

        if l_cnt=0 then
            raise_application_error(-20001, tname||'55');
        end if;
    end;


    procedure GenEvtSQL(
        table_owner in varchar2,
        table_name  in varchar2,
        region_col  in varchar2,
        operation   in char,
        pri         in binary_integer,
        hval        in binary_integer
    )is
        tname  varchar2(64) := upper(table_name);
        towner varchar2(32) := upper(table_owner);
        l_sql  varchar2(4000);
    begin
        l_sql := '        '||user_name||'.pkg_replx_evt.InsertLog('||chr(10)||
                 '            row_id=>:new.rowid,'||chr(10)||
                 '            towner=>'''||towner||''','||chr(10)||
                 '            tname=>'''||tname||''','||chr(10)||
                 '            key=>key_str,'||chr(10)||
                 '            priority=>'||pri||','||chr(10)||
                 '            hash_val=>'||hval||','||chr(10)||
                 '            operation=>'''||operation||'''';

        if region_col is not null  then
            l_sql := l_sql||','||chr(10)||
                 '            region=>:new.'||region_col;
        end if;
        l_sql := l_sql||chr(10)||
                 '        );'||chr(10)||
                 '        return;';
        Append(l_sql, chr(10));
    end;

    procedure GenInsertSQL(
        table_owner in varchar2,
        table_name  in varchar2,
        region_col  in varchar2,
        pri         in binary_integer,
        hval        in binary_integer
    )is
        tname  varchar2(64) := upper(table_name);
        towner varchar2(32) := upper(table_owner);
    begin
        Append('    if INSERTING then');
        GenKeyData(table_owner=>towner, table_name=>tname, operation=>'I');
        GenEvtSQL(
            table_owner => towner,
            table_name  => tname,
            region_col  => region_col,
            operation   => 'I',
            pri         => pri,
            hval        => hval
        );
        Append('    end if;');
    end;


    procedure GenUpdateSQL(
        table_owner in varchar2,
        table_name  in varchar2,
        region_col  in varchar2,
        pri         in binary_integer,
        hval        in binary_integer
    )is
        tname  varchar2(64) := upper(table_name);
        towner varchar2(32) := upper(table_owner);
    begin
        Append('    if UPDATING then');
        GenEvtSQL(
            table_owner => towner,
            table_name  => tname,
            region_col  => region_col,
            operation   => 'U',
            pri         => pri,
            hval        => hval
        );
        Append('    end if;');
    end;

    procedure GenDeleteSQL(
        table_owner in varchar2,
        table_name  in varchar2,
        region_col  in varchar2,
        pri         in binary_integer,
        hval        in binary_integer
    )is
        tname  varchar2(64) := upper(table_name);
        towner varchar2(32) := upper(table_owner);
    begin
        Append('    if DELETING then');
        GenEvtSQL(
            table_owner => towner,
            table_name  => tname,
            region_col  => region_col,
            operation   => 'D',
            pri         => pri,
            hval        => hval
        );
        Append('    end if;');
    end;

    procedure GetTriggerSQL(
        table_owner in varchar2,
        table_name  in varchar2
    )is
        towner varchar2(32) := upper(table_owner);
        tname varchar2(64) := upper(table_name);
        l_sql varchar2(8192);
        l_rcol varchar2(64);
        l_trig varchar2(32);
        l_str  varchar2(2048);
        l_ttype varchar2(12);
        l_fire varchar(256);
        l_dml_opt varchar2(4);
        l_pri number;
        l_hval number;
     begin
        sql_rows := 0;
        l_trig := GetTriggerName(towner, tname);

        select a.region_col, a.fire_cond, a.repl_type,
               a.dml_opt, a.priority, a.hash_val
          into l_rcol, l_fire, l_ttype, l_dml_opt,l_pri,l_hval
          from replicate_table a
        where a.src_table=tname;

        if l_hval = 0 then
            l_hval := GetHashVal(tname);
        end if;

        l_sql := 'create or replace trigger '||l_trig||chr(10)||
                 '    after <dml_event> <of_cols>'||
                 'on '||towner||'.'||tname||chr(10)||
                 '    for each row '||chr(10)||
                 '<when_cond>'||
                 'declare '||chr(10)||
                 '    key_str   varchar(1024);'||chr(10)||
                 'begin ';

        l_str := GetDMLEvent(l_dml_opt);
        l_sql := replace(l_sql, '<dml_event>', l_str);

        if l_fire is null or length(l_fire)=0 then
            l_str := '';
        else
            l_str := '    when ('||l_fire||')'||chr(10);
        end if;
        l_sql := replace(l_sql, '<when_cond>', l_str);
        l_sql := replace(l_sql, '<of_cols>', GetRefCols(tname));
        Append(l_sql, chr(10));


        if instr(l_dml_opt, 'I') > 0 then
            GenInsertSQL(
                table_owner=> towner,
                table_name => tname,
                region_col => l_rcol,
                pri        => l_pri,
                hval       => l_hval
            );
            Append(' ');
            Append(' ');
            Append(' ');
        end if;

        if instr(l_dml_opt, 'U') > 0  or instr(l_dml_opt, 'D') > 0 then
            GenKeyData(table_owner=>towner, table_name=>tname, operation=>'U');
        end if;

        if instr(l_dml_opt, 'U') > 0 then
            GenUpdateSQL(
                table_owner=> towner,
                table_name => tname,
                region_col => l_rcol,
                pri        => l_pri,
                hval       => l_hval
            );
            Append(' ');
            Append(' ');
            Append(' ');
        end if;


        if instr(l_dml_opt, 'D') > 0 then
            GenDeleteSQL(
                table_owner=> towner,
                table_name => tname,
                region_col => l_rcol,
                pri        => l_pri,
                hval       => l_hval
            );
        end if;
        Append('end;');
    end;



    procedure CreateTrigger(
        table_owner in varchar2,
        table_name  in varchar2
    )is
        l_handle integer;
        l_rows   integer;
        l_trigger varchar2(64);
    begin
      GetTriggerSQL(table_owner, table_name);
	  l_handle := dbms_sql.open_cursor;
	  --dbms_sql.parse(l_handle, sql_text, 1, sql_rows, true, 1);

        exception
            when others then
                if l_trigger is not null then
                    ExecuteSQL('drop trigger '||l_trigger);
                    raise_application_error(-20001, '???????:'||l_trigger);
                else
                    raise;
                end if;
    end;

    procedure TraceTriggerSQL
    is
    begin
       for i in 1..sql_rows
       loop
           dbms_output.put_line(sql_text(i));
       end loop;
    end;


    procedure Append(text in varchar2)
    is
    begin
        sql_rows := sql_rows + 1;
        sql_text(sql_rows) := text;
    end;

    procedure Append(text in varchar2, line_sep in varchar2)
    is
        l_pos1 number;
        l_pos2 number;
        l_slen number;
        l_len  number;
    begin
        l_len  := length(text);
        l_slen := length(line_sep);
        l_pos2 := 1;
        l_pos1 := instr(text, line_sep, 1);

        while l_pos1 > 0
        loop
            Append(substr(text, l_pos2, l_pos1-l_pos2));
            l_pos2 := l_pos1+l_slen;
            l_pos1 := instr(text, line_sep, l_pos2);
        end loop;

        if l_pos2 < l_len then
           Append(substr(text, l_pos2));
        end if;
    end;


    procedure ExecuteSQL(
        ddl_cmd in varchar2
    )is
        l_handle integer;
        l_rows   integer;
    begin
        l_handle := dbms_sql.open_cursor;
        dbms_sql.parse(l_handle, ddl_cmd, dbms_sql.native);
        l_rows := dbms_sql.execute(l_handle);
        dbms_sql.close_cursor(l_handle);

        select l_rows into l_rows from dual;
    end;


    procedure SwitchLogPart(
        maxsize binary_integer
    )is
        i_insid   number := 1;
        l_tname varchar2(64) := 'REPLX_LOG';
        l_busy_pname varchar2(64);
        i_bytes number(12);
    begin
        select l_tname||'_'||decode(switch_flag, 1, i_insid*10, i_insid)
          into l_busy_pname
          from replicate_switch
         where instance_id=i_insid;

        select bytes into i_bytes
          from yd_segments t
         where t.owner=user_name
              and segment_name=l_tname
              and partition_name=l_busy_pname;
        if i_bytes < maxsize then
            dbms_output.put_line(l_busy_pname|| ' current size ' || i_bytes
                ||', do not switch the partition.');
            return;
        end if;

        update replicate_switch
           set switch_flag=decode(switch_flag, 0, 1, 0),
               switch_time=sysdate
         where instance_id=i_insid;
        commit;

        dbms_output.put_line('Switch partition completed.');
    end;

    procedure Reclaim(tname in varchar2 := 'REPLX_LOG')
    is
        i_insid   number := 1;
        i_idle_part number;
        i_cnt     number;
        dt_last   date;
        i_gap     number;
        i_bytes   number(14);
        MAX_SIZE  number(14) := 10000000; --10M

        l_tname varchar2(64) := tname;
        l_idle_pname varchar2(64);
    begin
        select decode(switch_flag, 0, i_insid*10, i_insid),
               poll_interval, switch_time
          into i_idle_part, i_gap, dt_last
          from replicate_switch
         where instance_id=i_insid;
        if (dt_last + (i_gap+1)/1440) > sysdate then
            dbms_output.put_line('Can not reclaim, wait for a while pls.');
            return;
        end if;
        l_idle_pname := l_tname||'_'||i_idle_part;
        select bytes into i_bytes
          from yd_segments t
         where t.owner=user_name
              and segment_name=l_tname
              and partition_name=l_idle_pname;  
        if i_bytes < MAX_SIZE then
            dbms_output.put_line(l_idle_pname|| ' current size ' || i_bytes
                ||', do not reclaim.');
            SwitchLogPart(MAX_SIZE);
            return;
        end if;

        select count(1)
          into i_cnt
          from v_object k
         where exists
         (select t.object_id
            from yd_objects t
           where t.object_id = k.OBJECT_ID
             and t.object_name = l_tname
             and t.subobject_name = l_idle_pname);
        if i_cnt > 0 then
            dbms_output.put_line('Some transaction still be active on the partition '||l_idle_pname);
            return;
        end if;

        select count(*) into i_cnt
          from replx_log
         where instance_id=i_idle_part and rownum<2;
        if i_cnt > 0 then
            dbms_output.put_line('Still has log data on partition '||l_idle_pname);
            return;
        end if;
		dbms_output.put_line(l_tname);
		dbms_output.put_line(l_idle_pname);
        ExecuteSQL('alter table '||l_tname||'  truncate partition '||l_idle_pname);
        dbms_output.put_line(l_idle_pname||' reclaim compeleted.');
        SwitchLogPart(MAX_SIZE);
    end;
begin
    user_name := sys_context('userenv', 'current_user');
    sql_rows := 0;
end pkg_replx_toolkit;
//