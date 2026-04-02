#
# let $load_parallel = 4;
# let $data_path = /data/tpch_1g_data/;

eval load data /*+ parallel($load_parallel)*/ infile '$data_path/lineitem.tbl' into table lineitem fields terminated by '|';
eval select /*+ parallel($load_parallel) */ count(*) from lineitem;
eval load data /*+ parallel($load_parallel)*/ infile '$data_path/customer.tbl' into table customer fields terminated by '|';
eval select /*+ parallel($load_parallel) */ count(*) from customer;
eval load data /*+ parallel($load_parallel)*/ infile '$data_path/nation.tbl' into table nation fields terminated by '|';
eval select /*+ parallel($load_parallel) */ count(*) from nation;
eval load data /*+ parallel($load_parallel)*/ infile '$data_path/orders.tbl' into table orders fields terminated by '|';
eval select /*+ parallel($load_parallel) */ count(*) from orders;
eval load data /*+ parallel($load_parallel)*/ infile '$data_path/partsupp.tbl' into table partsupp fields terminated by '|';
eval select /*+ parallel($load_parallel) */ count(*) from partsupp;
eval load data /*+ parallel($load_parallel)*/ infile '$data_path/part.tbl' into table part fields terminated by '|';
eval select /*+ parallel($load_parallel) */ count(*) from part;
eval load data /*+ parallel($load_parallel)*/ infile '$data_path/region.tbl' into table region fields terminated by '|';
eval select /*+ parallel($load_parallel) */ count(*) from region;
eval load data /*+ parallel($load_parallel)*/ infile '$data_path/supplier.tbl' into table supplier fields terminated by '|';
eval select /*+ parallel($load_parallel) */ count(*) from supplier;
