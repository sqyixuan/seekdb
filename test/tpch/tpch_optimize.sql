set _force_parallel_query_dop =36;
set global parallel_servers_target=200;
set global ob_sql_work_area_percentage=80; 
set global _groupby_nopushdown_cut_ratio=1;
set global _nlj_batching_enabled=true;
alter system set enable_sql_extension=true;
select sleep(10);

analyze table lineitem COMPUTE STATISTICS for columns  l_returnflag size auto;
analyze table lineitem COMPUTE STATISTICS for columns  l_linestatus size auto;
analyze table lineitem COMPUTE STATISTICS for columns  l_quantity size auto;
analyze table lineitem COMPUTE STATISTICS for columns  l_extendedprice size auto;
analyze table lineitem COMPUTE STATISTICS for columns  l_discount size auto;
analyze table lineitem COMPUTE STATISTICS for columns  l_tax size auto;
analyze table lineitem COMPUTE STATISTICS for columns  l_shipdate size auto;
analyze table lineitem COMPUTE STATISTICS for columns  l_orderkey size auto;
analyze table lineitem COMPUTE STATISTICS for columns  l_commitdate size auto;
analyze table lineitem COMPUTE STATISTICS for columns  l_receiptdate size auto;
analyze table lineitem COMPUTE STATISTICS for columns  l_suppkey size auto;
analyze table lineitem COMPUTE STATISTICS for columns  l_partkey size auto;
analyze table lineitem COMPUTE STATISTICS for columns  l_shipinstruct size auto;
analyze table lineitem COMPUTE STATISTICS for columns  l_shipmode size auto;


analyze table part COMPUTE STATISTICS for columns  p_partkey  size auto;
analyze table part COMPUTE STATISTICS for columns  p_size size auto;
analyze table part COMPUTE STATISTICS for columns  p_type size auto;
analyze table part COMPUTE STATISTICS for columns  p_name size auto;
analyze table part COMPUTE STATISTICS for columns  p_brand size auto;
analyze table part COMPUTE STATISTICS for columns  p_container size auto;


analyze table supplier COMPUTE STATISTICS for columns  s_suppkey size auto;
analyze table supplier COMPUTE STATISTICS for columns  s_nationkey size auto;
analyze table supplier COMPUTE STATISTICS for columns  s_comment size auto;
analyze table supplier COMPUTE STATISTICS for columns  s_name size auto;


analyze table partsupp COMPUTE STATISTICS for columns  ps_partkey size auto;
analyze table partsupp COMPUTE STATISTICS for columns  ps_suppkey size auto;
analyze table partsupp COMPUTE STATISTICS for columns  ps_supplycost size auto;
analyze table partsupp COMPUTE STATISTICS for columns  ps_availqty size auto;


analyze table orders COMPUTE STATISTICS for columns o_orderdate size auto;
analyze table orders COMPUTE STATISTICS for columns o_orderkey size auto;
analyze table orders COMPUTE STATISTICS for columns o_shippriority size auto;
analyze table orders COMPUTE STATISTICS for columns o_orderpriority size auto;
analyze table orders COMPUTE STATISTICS for columns o_custkey size auto;
analyze table orders COMPUTE STATISTICS for columns o_totalprice size auto;
analyze table orders COMPUTE STATISTICS for columns o_orderstatus size auto;


analyze table customer COMPUTE STATISTICS for columns  c_mktsegment size auto;
analyze table customer COMPUTE STATISTICS for columns  c_custkey size auto;
analyze table customer COMPUTE STATISTICS for columns  c_nationkey size auto;
analyze table customer COMPUTE STATISTICS for columns  c_name size auto;
analyze table customer COMPUTE STATISTICS for columns  c_acctbal size auto;
analyze table customer COMPUTE STATISTICS for columns  c_phone size auto;
analyze table customer COMPUTE STATISTICS for columns  c_address size auto;


