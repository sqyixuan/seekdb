connection conn_oracle_lob;
set ob_trx_timeout=100000000;
set ob_query_timeout=100000000;
set ob_default_lob_inrow_threshold = 4096;

# 启动liboblog
ob1.oblog.set_extra_argument -m;
ob1.oblog.gen_conf tb_white_list=sys.oblog*.*|cdc_mysql.*.*|oracle.*.* log_level=DEBUG;
ob1.oblog.restart;

create table simple_xml_test(pk int primary key, x xmltype);
ob1.oblog.next;

# insert inrow
select LOWER(RAWTOHEX(DBMS_CRYPTO.HASH(TO_CLOB('<data><name>first</name></data>'),DBMS_CRYPTO.HASH_MD5))) as xml_md5 from dual;
insert into simple_xml_test values(1, xmltype('<data><name>first</name></data>'));
ob1.oblog.next;

# update inrow -> inrow
select LOWER(RAWTOHEX(DBMS_CRYPTO.HASH(TO_CLOB('<data><name>second</name></data>'),DBMS_CRYPTO.HASH_MD5))) as xml_md5 from dual;
update simple_xml_test set x = xmltype('<data><name>second</name></data>') where pk=1;
ob1.oblog.next;

# update inrow -> outrow
select LOWER(RAWTOHEX(DBMS_CRYPTO.HASH(TO_CLOB('<biglob>') || rpad(TO_CLOB('xxxx'), 10000, 'x') || '</biglob>', DBMS_CRYPTO.HASH_MD5))) as xml_md5 from dual;
update simple_xml_test set x = xmltype(TO_CLOB('<biglob>') || rpad(TO_CLOB('xxxx'), 10000, 'x') || '</biglob>') where pk=1;
ob1.oblog.next;

delete from simple_xml_test where pk = 1;
ob1.oblog.next;

# insert outrow
select LOWER(RAWTOHEX(DBMS_CRYPTO.HASH(TO_CLOB('<biglob>') || rpad(TO_CLOB('xxxx'), 10000, 'y') || '</biglob>', DBMS_CRYPTO.HASH_MD5))) as xml_md5 from dual;
insert into simple_xml_test values(3, xmltype(TO_CLOB('<biglob>') || rpad(TO_CLOB('xxxx'), 10000, 'y') || '</biglob>'));
ob1.oblog.next;

# update outrow -> outrow
select LOWER(RAWTOHEX(DBMS_CRYPTO.HASH(concat(concat(to_clob('<big>'), rpad(to_clob('d'), 5033, 'd')), '</big>'),DBMS_CRYPTO.HASH_MD5))) as xml_md5 from dual;
update simple_xml_test set x = xmltype(concat(concat(to_clob('<big>'), rpad(to_clob('d'), 5033, 'd')), '</big>')) where pk=3;
ob1.oblog.next;

# update outrow -> inrow
update simple_xml_test set x = xmltype('<data><name>third</name></data>') where pk=3;
select LOWER(RAWTOHEX(DBMS_CRYPTO.HASH(TO_CLOB('<data><name>third</name></data>'),DBMS_CRYPTO.HASH_MD5))) as xml_md5 from dual;
ob1.oblog.next;

delete from simple_xml_test where pk = 3;
ob1.oblog.next;

# update rowkey
select LOWER(RAWTOHEX(DBMS_CRYPTO.HASH(TO_CLOB('<biglob>') || rpad(TO_CLOB('xxxx'), 8000, 'z') || '</biglob>', DBMS_CRYPTO.HASH_MD5))) as xml_md5 from dual;
insert into simple_xml_test values(30001, xmltype(TO_CLOB('<biglob>') || rpad(TO_CLOB('xxxx'), 8000, 'z') || '</biglob>'));
ob1.oblog.next;
update simple_xml_test set pk = 30002 where pk = 30001;
ob1.oblog.next;
delete from simple_xml_test where pk = 30002;
ob1.oblog.next;

ob1.oblog.stop;
