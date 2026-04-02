
create table lineitem(
    l_orderkey           bigint NOT NULL ,
    l_partkey            bigint NOT NULL ,
    l_suppkey            bigint NOT NULL ,
    l_linenumber         bigint NOT NULL ,
    l_quantity           bigint NOT NULL ,
    l_extendedprice      number(10,2) NOT NULL ,
    l_discount           number(10,2) NOT NULL ,
    l_tax                number(10,2) NOT NULL ,
    l_returnflag         char(1) ,
    l_linestatus         char(1) ,
    l_shipdate           date ,
    l_commitdate         date ,
    l_receiptdate        date ,
    l_shipinstruct       char(25) ,
    l_shipmode           char(10) ,
    l_comment            varchar(44),
    primary key(L_ORDERKEY, L_LINENUMBER, l_shipdate, l_partkey)
)

partition by range columns (l_shipdate)
(
partition item1 values less than ('1992-01-01')
,
partition item2 values less than ('1992-02-01')
,
partition item3 values less than ('1992-03-01')
,
partition item4 values less than ('1992-04-01')
,
partition item5 values less than ('1992-05-01')
,
partition item6 values less than ('1992-06-01')
,
partition item7 values less than ('1992-07-01')
,
partition item8 values less than ('1992-08-01')
,
partition item9 values less than ('1992-09-01')
,
partition item10 values less than ('1992-10-01')
,
partition item11 values less than ('1992-11-01')
,
partition item12 values less than ('1992-12-01')
,
partition item13 values less than ('1993-01-01')
,
partition item14 values less than ('1993-02-01')
,
partition item15 values less than ('1993-03-01')
,
partition item16 values less than ('1993-04-01')
,
partition item17 values less than ('1993-05-01')
,
partition item18 values less than ('1993-06-01')
,
partition item19 values less than ('1993-07-01')
,
partition item20 values less than ('1993-08-01')
,
partition item21 values less than ('1993-09-01')
,
partition item22 values less than ('1993-10-01')
,
partition item23 values less than ('1993-11-01')
,
partition item24 values less than ('1993-12-01')
,
partition item25 values less than ('1994-01-01')
,
partition item26 values less than ('1994-02-01')
,
partition item27 values less than ('1994-03-01')
,
partition item28 values less than ('1994-04-01')
,
partition item29 values less than ('1994-05-01')
,
partition item30 values less than ('1994-06-01')
,
partition item31 values less than ('1994-07-01')
,
partition item32 values less than ('1994-08-01')
,
partition item33 values less than ('1994-09-01')
,
partition item34 values less than ('1994-10-01')
,
partition item35 values less than ('1994-11-01')
,
partition item36 values less than ('1994-12-01')
,
partition item37 values less than ('1995-01-01')
,
partition item38 values less than ('1995-02-01')
,
partition item39 values less than ('1995-03-01')
,
partition item40 values less than ('1995-04-01')
,
partition item41 values less than ('1995-05-01')
,
partition item42 values less than ('1995-06-01')
,
partition item43 values less than ('1995-07-01')
,
partition item44 values less than ('1995-08-01')
,
partition item45 values less than ('1995-09-01')
,
partition item46 values less than ('1995-10-01')
,
partition item47 values less than ('1995-11-01')
,
partition item48 values less than ('1995-12-01')
,
partition item49 values less than ('1996-01-01')
,
partition item50 values less than ('1996-02-01')
,
partition item51 values less than ('1996-03-01')
,
partition item52 values less than ('1996-04-01')
,
partition item53 values less than ('1996-05-01')
,
partition item54 values less than ('1996-06-01')
,
partition item55 values less than ('1996-07-01')
,
partition item56 values less than ('1996-08-01')
,
partition item57 values less than ('1996-09-01')
,
partition item58 values less than ('1996-10-01')
,
partition item59 values less than ('1996-11-01')
,
partition item60 values less than ('1996-12-01')
,
partition item61 values less than ('1997-01-01')
,
partition item62 values less than ('1997-02-01')
,
partition item63 values less than ('1997-03-01')
,
partition item64 values less than ('1997-04-01')
,
partition item65 values less than ('1997-05-01')
,
partition item66 values less than ('1997-06-01')
,
partition item67 values less than ('1997-07-01')
,
partition item68 values less than ('1997-08-01')
,
partition item69 values less than ('1997-09-01')
,
partition item70 values less than ('1997-10-01')
,
partition item71 values less than ('1997-11-01')
,
partition item72 values less than ('1997-12-01')
,
partition item73 values less than ('1998-01-01')
,
partition item74 values less than ('1998-02-01')
,
partition item75 values less than ('1998-03-01')
,
partition item76 values less than ('1998-04-01')
,
partition item77 values less than ('1998-05-01')
,
partition item78 values less than ('1998-06-01')
,
partition item79 values less than ('1998-07-01')
,
partition item80 values less than ('1998-08-01')
,
partition item81 values less than ('1998-09-01')
,
partition item82 values less than ('1998-10-01')
,
partition item83 values less than ('1998-11-01')
,
partition item84 values less than (MAXVALUE)
);
