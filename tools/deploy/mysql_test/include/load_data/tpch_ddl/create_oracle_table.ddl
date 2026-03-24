SET global NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS';
SET global NLS_TIMESTAMP_FORMAT='YYYY-MM-DD HH24:MI:SS.FF';
SET global NLS_TIMESTAMP_TZ_FORMAT='YYYY-MM-DD HH24:MI:SS.FF TZR TZD';

delimiter //;

DECLARE v1 INT;
BEGIN
  select count(*) into v1 from dba_tables where table_name='LINEITEM';
  IF (v1 = 0) then
    execute immediate 'create table lineitem(
    l_orderkey           number(20) NOT NULL ,
    l_partkey            number(20) NOT NULL ,
    l_suppkey            number(20) NOT NULL ,
    l_linenumber         number(20) NOT NULL ,
    l_quantity           number(20) NOT NULL ,
    l_extendedprice      decimal(10,2) NOT NULL ,
    l_discount           decimal(10,2) NOT NULL ,
    l_tax                decimal(10,2) NOT NULL ,
    l_returnflag         char(1) DEFAULT NULL,
    l_linestatus         char(1) DEFAULT NULL,
    l_shipdate           date NOT NULL,
    l_commitdate         date DEFAULT NULL,
    l_receiptdate        date DEFAULT NULL,
    l_shipinstruct       char(25) DEFAULT NULL,
    l_shipmode           char(10) DEFAULT NULL,
    l_comment            varchar(44) DEFAULT NULL,
    primary key(L_ORDERKEY, L_LINENUMBER))';
  END IF;
END//;
DECLARE v1 INT;
BEGIN
  select count(*) into v1 from dba_indexes where index_name='I_L_ORDERKEY';
  IF (v1 = 0) then
    execute immediate 'create index I_L_ORDERKEY on lineitem(l_orderkey)';
  END IF;
END//;


DECLARE v1 INT;
BEGIN
  select count(*) into v1 from dba_tables where table_name='ORDERS';
  IF (v1 = 0) then
    execute immediate 'create table orders(
    o_orderkey           number(20) NOT NULL ,
    o_custkey            number(20) NOT NULL ,
    o_orderstatus        char(1) DEFAULT NULL,
    o_totalprice         decimal(10,2) DEFAULT NULL,
    o_orderdate          date NOT NULL,
    o_orderpriority      char(15) DEFAULT NULL,
    o_clerk              char(15) DEFAULT NULL,
    o_shippriority       number(20) DEFAULT NULL,
    o_comment            varchar(79) DEFAULT NULL,
    primary key(o_orderkey,o_orderdate,o_custkey))';
  END IF;
END//;
DECLARE v1 INT;
BEGIN
  select count(*) into v1 from dba_indexes where index_name='I_O_ORDERKEY';
  IF (v1 = 0) then
    execute immediate 'create index I_O_ORDERKEY on orders(o_orderkey)';
  END IF;
END//;


DECLARE v1 INT;
BEGIN
  select count(*) into v1 from dba_tables where table_name='PARTSUPP';
  IF (v1 = 0) then
    execute immediate 'create table partsupp(
    ps_partkey           number(20) NOT NULL ,
    ps_suppkey           number(20) NOT NULL ,
    ps_availqty          number(20) DEFAULT NULL,
    ps_supplycost        decimal(10,2) DEFAULT NULL,
    ps_comment           varchar(199) DEFAULT NULL,
    primary key (PS_PARTKEY, PS_SUPPKEY))';
  END IF;
END//;


DECLARE v1 INT;
BEGIN
  select count(*) into v1 from dba_tables where table_name='PART';
  IF (v1 = 0) then
    execute immediate 'create table part(
    p_partkey            number(20) NOT NULL ,
    p_name               varchar(55) DEFAULT NULL,
    p_mfgr               char(25) DEFAULT NULL,
    p_brand              char(10) DEFAULT NULL,
    p_type               varchar(25) DEFAULT NULL,
    p_size               number(20) DEFAULT NULL,
    p_container          char(10) DEFAULT NULL,
    p_retailprice        decimal(10,2) DEFAULT NULL,
    p_comment            varchar(23) DEFAULT NULL,
    primary key (P_PARTKEY))';
  END IF;
END//;


DECLARE v1 INT;
BEGIN
  select count(*) into v1 from dba_tables where table_name='CUSTOMER';
  IF (v1 = 0) then 
    execute immediate 'create table customer(
    c_custkey            number(20) NOT NULL ,
    c_name               varchar(25) DEFAULT NULL,
    c_address            varchar(40) DEFAULT NULL,
    c_nationkey          number(20) DEFAULT NULL,
    c_phone              char(15) DEFAULT NULL,
    c_acctbal            decimal(10,2) DEFAULT NULL,
    c_mktsegment         char(10) DEFAULT NULL,
    c_comment            varchar(117) DEFAULT NULL,
    primary key(C_CUSTKEY))';
  END IF;
END//;


DECLARE v1 INT;
BEGIN
  select count(*) into v1 from dba_tables where table_name='SUPPLIER';
  IF (v1 = 0) then 
    execute immediate 'create table supplier(
    s_suppkey            number(20) NOT NULL ,
    s_name               char(25) DEFAULT NULL,
    s_address            varchar(40) DEFAULT NULL,
    s_nationkey          number(20) DEFAULT NULL,
    s_phone              char(15) DEFAULT NULL,
    s_acctbal            number(20) DEFAULT NULL,
    s_comment            varchar(101) DEFAULT NULL,
    primary key (S_SUPPKEY))';
  END IF;
END//;


DECLARE v1 INT;
BEGIN
  select count(*) into v1 from dba_tables where table_name='NATION';
  IF (v1 = 0) then 
    execute immediate 'create table nation(
    n_nationkey          number(20) NOT NULL ,
    n_name               char(25) DEFAULT NULL,
    n_regionkey          number(20) DEFAULT NULL,
    n_comment            varchar(152) DEFAULT NULL,
    primary key (N_NATIONKEY))';
  END IF;
END//;

DECLARE v1 INT;
BEGIN
  select count(*) into v1 from dba_tables where table_name='REGION';
  IF (v1 = 0) then 
    execute immediate 'create table region(
    r_regionkey          number(20) NOT NULL,
    r_name               char(25) DEFAULT NULL,
    r_comment            varchar(152) DEFAULT NULL,
    primary key (R_REGIONKEY))';
  END IF;
END//;
