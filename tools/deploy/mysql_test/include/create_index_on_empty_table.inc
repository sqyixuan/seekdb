# owner : buming.lj
# owner group: DDL
# desription : check create index on empty table with different path


eval ALTER SYSTEM SET _parallel_ddl_control = 'CREATE_INDEX:OFF'; 

eval create index idx1 on `ocean base` (col2) local;

eval create index idx2 on `ocean base` (col2) global;

eval ALTER SYSTEM SET _parallel_ddl_control = 'CREATE_INDEX:ON';

eval create index idx3 on `ocean base` (col2) local;

eval create index idx4 on `ocean base` (col2) global;

eval alter table `ocean base` add index idx5 (col2) local;

eval alter table `ocean base` add index idx6 (col2) global;

eval alter table `ocean base` add index idx7 (col2) local, add index idx8 (col2) global;