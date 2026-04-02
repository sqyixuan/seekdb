SET global NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS';
SET global NLS_TIMESTAMP_FORMAT='YYYY-MM-DD HH24:MI:SS.FF';
SET global NLS_TIMESTAMP_TZ_FORMAT='YYYY-MM-DD HH24:MI:SS.FF TZR TZD';
set ob_query_timeout=100000000;

SET NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS';
SET NLS_TIMESTAMP_FORMAT='YYYY-MM-DD HH24:MI:SS.FF';
SET NLS_TIMESTAMP_TZ_FORMAT='YYYY-MM-DD HH24:MI:SS.FF TZR TZD';

create tablegroup tpch_tg_100g_lineitem_order_group;
create tablegroup tpch_tg_100g_partsupp_part;



delimiter /;
DECLARE v1 INT;
BEGIN
  select count(*) into v1 from user_tables where table_name='LINEITEM' ;
  IF (v1 = 0) then
    execute immediate 'CREATE TABLE lineitem (
    l_orderkey number NOT NULL,
    l_partkey number NOT NULL,
    l_suppkey number NOT NULL,
    l_linenumber number NOT NULL,
    l_quantity number NOT NULL,
    l_extendedprice number NOT NULL,
    l_discount number NOT NULL,
    l_tax number NOT NULL,
    l_returnflag char(1) DEFAULT NULL,
    l_linestatus char(1) DEFAULT NULL,
    l_shipdate date NOT NULL,
    l_commitdate date DEFAULT NULL,
    l_receiptdate date DEFAULT NULL,
    l_shipinstruct char(25) DEFAULT NULL,
    l_shipmode char(10) DEFAULT NULL,
    l_comment varchar(44) DEFAULT NULL,
    primary key(l_orderkey, l_linenumber))
    tablegroup = tpch_tg_100g_lineitem_order_group 
    partition by hash (l_orderkey) partitions 256';
  END IF;
END
/

DECLARE v1 INT;
BEGIN
  select count(*) into v1 from user_indexes where index_name='I_L_ORDERKEY';
  IF (v1 = 0) then
    execute immediate 'create index I_L_ORDERKEY on lineitem(l_orderkey) local';
    execute immediate 'create index I_L_SHIPDATE on lineitem(l_shipdate) local';
  END IF;
END
/

DECLARE v1 INT;
BEGIN
  select count(*) into v1 from user_tables where table_name='ORDERS' ;
  IF (v1 = 0) then
    execute immediate 'CREATE TABLE orders (
    o_orderkey number NOT NULL,
    o_custkey number NOT NULL,
    o_orderstatus char(1) DEFAULT NULL,
    o_totalprice number DEFAULT NULL,
    o_orderdate date NOT NULL,
    o_orderpriority char(15) DEFAULT NULL,
    o_clerk char(15) DEFAULT NULL,
    o_shippriority number DEFAULT NULL,
    o_comment varchar(79) DEFAULT NULL,
    PRIMARY KEY (o_orderkey)) 
    tablegroup = tpch_tg_100g_lineitem_order_group  
    partition by hash(o_orderkey) partitions 256';
  END IF;
END
/

DECLARE v1 INT;
BEGIN
  select count(*) into v1 from user_indexes where index_name='I_O_ORDERKEY';
  IF (v1 = 0) then
    execute immediate 'create index I_O_ORDERDATE on orders(o_orderdate) local';
  END IF;
END
/


DECLARE v1 INT;
BEGIN
  select count(*) into v1 from user_tables where table_name='PARTSUPP' ;
  IF (v1 = 0) then
    execute immediate 'CREATE TABLE partsupp (
    ps_partkey number NOT NULL,
    ps_suppkey number NOT NULL,
    ps_availqty number DEFAULT NULL,
    ps_supplycost number DEFAULT NULL,
    ps_comment varchar(199) DEFAULT NULL,
    PRIMARY KEY (ps_partkey, ps_suppkey))
    tablegroup tpch_tg_100g_partsupp_part 
    partition by hash(ps_partkey) partitions 256';
  END IF;
END
/


DECLARE v1 INT;
BEGIN
  select count(*) into v1 from user_tables where table_name='PART' ;
  IF (v1 = 0) then
    execute immediate 'CREATE TABLE part (
  p_partkey number NOT NULL,
  p_name varchar(55) DEFAULT NULL,
  p_mfgr char(25) DEFAULT NULL,
  p_brand char(10) DEFAULT NULL,
  p_type varchar(25) DEFAULT NULL,
  p_size number DEFAULT NULL,
  p_container char(10) DEFAULT NULL,
  p_retailprice number DEFAULT NULL,
  p_comment varchar(23) DEFAULT NULL,
  PRIMARY KEY (p_partkey)) 
  tablegroup tpch_tg_100g_partsupp_part
  partition by hash(p_partkey) partitions 256';
  END IF;
END
/


DECLARE v1 INT;
BEGIN
  select count(*) into v1 from user_tables where table_name='CUSTOMER' ;
  IF (v1 = 0) then
    execute immediate 'CREATE TABLE customer (
  c_custkey number NOT NULL,
  c_name varchar(25) DEFAULT NULL,
  c_address varchar(40) DEFAULT NULL,
  c_nationkey number DEFAULT NULL,
  c_phone char(15) DEFAULT NULL,
  c_acctbal number DEFAULT NULL,
  c_mktsegment char(10) DEFAULT NULL,
  c_comment varchar(117) DEFAULT NULL,
  PRIMARY KEY (c_custkey)) 
  partition by hash(c_custkey) partitions 256';
  END IF;
END
/

DECLARE v1 INT;
BEGIN
  select count(*) into v1 from user_tables where table_name='SUPPLIER' ;
  IF (v1 = 0) then
    execute immediate 'CREATE TABLE supplier (
  s_suppkey number NOT NULL,
  s_name char(25) DEFAULT NULL,
  s_address varchar(40) DEFAULT NULL,
  s_nationkey number DEFAULT NULL,
  s_phone char(15) DEFAULT NULL,
  s_acctbal number DEFAULT NULL,
  s_comment varchar(101) DEFAULT NULL,
  PRIMARY KEY (s_suppkey)
) partition by hash(s_suppkey) partitions 256';
  END IF;
END
/


DECLARE v1 INT;
BEGIN
  select count(*) into v1 from user_tables where table_name='NATION' ;
  IF (v1 = 0) then
    execute immediate 'CREATE TABLE nation (
  n_nationkey number NOT NULL,
  n_name char(25) DEFAULT NULL,
  n_regionkey number DEFAULT NULL,
  n_comment varchar(152) DEFAULT NULL,
  PRIMARY KEY (n_nationkey))';
  END IF;
END
/


DECLARE v1 INT;
BEGIN
  select count(*) into v1 from user_tables where table_name='REGION' ;
  IF (v1 = 0) then
    execute immediate 'CREATE TABLE region (
  r_regionkey number NOT NULL,
  r_name char(25) DEFAULT NULL,
  r_comment varchar(152) DEFAULT NULL,
  PRIMARY KEY (r_regionkey))';
  END IF;
END
/

delimiter ;/
