suite2tags={
'OB_REGRESS_PLSQL':'pl,ps,returning_into',
'OB_REGRESS_SQLEXEC':'acs,ansi_join,auto_inc,auto_increment,bit_type,charset,datatype,default_value,distinct,enum_and_set,executor,foreign_key,found_rows,funcs,generated_column,global_index,groupby,hierarchical_query,index,insert,jit,join,lob,merge_into,multiget,new_online_use,online_use,order_by,otimestamp,parser,partition,part_mg,purge_query,replace,rownum,select,sequence,show,subquery,timeout,timestamp,udf,update,utf8,view,window_function,window_function_mysql,with_clause,interval',
'OB_REGRESS_META':'constraint,inner_table,information_schema,meta,meta_info,privilege,parameter,schema,show,synonym,sys_vars,table_location,tenant,time_zone,type_date',
'OB_REGRESS_DDL':'alter,alter_session,create_table,database,dcl,ddl,foreign_key,generated_column,global_index,index,part_mg,recyclebin,tenant',
'OB_REGRESS_DML':'default_value,delete,insert,load_data,merge_into,replace_and_insert_on_dup,update,dml',
'OB_REGRESS_OPTIMIZER':'acs,ansi_join,cost,histogram,join,new_online_use,online_use,optimizer,outline,partition,part_mg,plan_cache,rewrite,skyline,spm,subquery,transformer,view',
'OB_REGRESS_TXN':'trx',
'OB_REGRESS_PX':'px',
'OB_REGRESS_STORAGE':'index,innodb,lob,multiget'
}

composite_suite={
'OB_REGRESS_SQL':'OB_REGRESS_PLSQL,OB_REGRESS_SQLEXEC,OB_REGRESS_DML,OB_REGRESS_OPTIMIZER,OB_REGRESS_PX'
}
