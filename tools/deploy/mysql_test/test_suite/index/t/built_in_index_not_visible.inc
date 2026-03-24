# fts index built in index
create table t1 (a text character set utf8mb4, b int, fulltext index(a));
create index fts_rowkey_doc on t1(b);
create index fts_doc_rowkey on t1(b);
create index a_fts_doc_word on t1(b);

# vector index built in index
create table t2 (c1 int, c2 vector(3), primary key(c1), vector index idx1(c2) with (distance=l2, type=hnsw, lib=vsag)) partition by hash(c1) partitions 2;

create index rowkey_vid_table on t2(c1);
create index vid_rowkey_table on t2(c1);
create index idx1_index_id_table on t2(c1);

drop table t1;
drop table t2;