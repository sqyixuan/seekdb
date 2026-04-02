--explain_protocol 2
--echo ###### 1. one rank winfunc ###################
select /*+PQ_DISTRIBUTE_WINDOW(@"SEL$2" (0) NONE PARTITION_SORT WF_TOPN)*/ count(*) from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk from wf_hash) where rk = 1;
select /*+NO_PRED_DEDUCE*/count(*) from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk from wf_hash) where rk = 1;

select /*+PQ_DISTRIBUTE_WINDOW(@"SEL$2" (0) NONE PARTITION_SORT WF_TOPN)*/ count(*) from (select c1,c2,c0,row_number() over (partition by c1,c2 order by c0) rk from wf_hash) where rk = 1;
select /*+NO_PRED_DEDUCE*/count(*) from (select c1,c2,c0,row_number() over (partition by c1,c2 order by c0) rk from wf_hash) where rk = 1;

--echo #dense_rank (don't extract topn filter)
select count(*) from (select c1,c2,c0,dense_rank() over (partition by c1,c2 order by c0) rk from wf_hash) where rk = 1;

select /*+PQ_DISTRIBUTE_WINDOW(@"SEL$2" (0) NONE PARTITION_SORT WF_TOPN)*/ count(*) from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk from wf_hash) where rk = 2;
select /*+NO_PRED_DEDUCE*/count(*) from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk from wf_hash) where rk = 2;
select /*+PQ_DISTRIBUTE_WINDOW(@"SEL$2" (0) NONE PARTITION_SORT WF_TOPN)*/ count(*) from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk from wf_hash) where rk = 3;
select /*+NO_PRED_DEDUCE*/count(*) from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk from wf_hash) where rk = 3;

select /*+PQ_DISTRIBUTE_WINDOW(@"SEL$2" (0) NONE PARTITION_SORT WF_TOPN)*/ count(*) from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk from wf_hash) where rk < 2;
select /*+NO_PRED_DEDUCE*/count(*) from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk from wf_hash) where rk < 2;
select /*+PQ_DISTRIBUTE_WINDOW(@"SEL$2" (0) NONE PARTITION_SORT WF_TOPN)*/ count(*) from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk from wf_hash) where rk < 4;
select /*+NO_PRED_DEDUCE*/count(*) from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk from wf_hash) where rk < 4;

select /*+PQ_DISTRIBUTE_WINDOW(@"SEL$2" (0) NONE PARTITION_SORT WF_TOPN)*/ count(*) from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk from wf_hash) where rk <= 2;
select /*+NO_PRED_DEDUCE*/count(*) from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk from wf_hash) where rk <= 2;
select /*+PQ_DISTRIBUTE_WINDOW(@"SEL$2" (0) NONE PARTITION_SORT WF_TOPN)*/ count(*) from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk from wf_hash) where rk <= 4;
select /*+NO_PRED_DEDUCE*/count(*) from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk from wf_hash) where rk <= 4;

select /*+PQ_DISTRIBUTE_WINDOW(@"SEL$2" (0) NONE PARTITION_SORT WF_TOPN)*/ count(*) from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk from wf_hash) where 2 >= rk;
select /*+NO_PRED_DEDUCE*/count(*) from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk from wf_hash) where 2 >= rk;
select /*+PQ_DISTRIBUTE_WINDOW(@"SEL$2" (0) NONE PARTITION_SORT WF_TOPN)*/ count(*) from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk from wf_hash) where 4 >= rk;
select /*+NO_PRED_DEDUCE*/count(*) from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk from wf_hash) where 4 >= rk;

select /*+PQ_DISTRIBUTE_WINDOW(@"SEL$2" (0) NONE PARTITION_SORT WF_TOPN)*/ count(*) from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk from wf_hash) where 2 = rk;
select /*+NO_PRED_DEDUCE*/count(*) from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk from wf_hash) where 2 = rk;
select /*+PQ_DISTRIBUTE_WINDOW(@"SEL$2" (0) NONE PARTITION_SORT WF_TOPN)*/ count(*) from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk from wf_hash) where 3 = rk;
select /*+NO_PRED_DEDUCE*/count(*) from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk from wf_hash) where 3 = rk;

select /*+PQ_DISTRIBUTE_WINDOW(@"SEL$2" (0) NONE PARTITION_SORT WF_TOPN)*/ count(*) from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk from wf_hash) where 2 > rk;
select /*+NO_PRED_DEDUCE*/count(*) from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk from wf_hash) where 2 > rk;
select /*+PQ_DISTRIBUTE_WINDOW(@"SEL$2" (0) NONE PARTITION_SORT WF_TOPN)*/ count(*) from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk from wf_hash) where 4 > rk;
select /*+NO_PRED_DEDUCE*/count(*) from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk from wf_hash) where 4 > rk;

select count(*) from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk from wf_hash) where rk >= 2;
select count(*) from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk from wf_hash) where rk > 2;
select count(*) from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk from wf_hash) where rk >= 4;
select count(*) from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk from wf_hash) where rk > 4;

--echo #test rownumber
select /*+PQ_DISTRIBUTE_WINDOW(@"SEL$2" (0) NONE PARTITION_SORT WF_TOPN)*/ count(*) from (select c1,c2,c0,row_number() over (partition by c1,c2 order by c0) rk from wf_hash) where rk = 2;
select /*+NO_PRED_DEDUCE*/count(*) from (select c1,c2,c0,row_number() over (partition by c1,c2 order by c0) rk from wf_hash) where rk = 2;
select /*+PQ_DISTRIBUTE_WINDOW(@"SEL$2" (0) NONE PARTITION_SORT WF_TOPN)*/ count(*) from (select c1,c2,c0,row_number() over (partition by c1,c2 order by c0) rk from wf_hash) where rk = 4;
select /*+NO_PRED_DEDUCE*/count(*) from (select c1,c2,c0,row_number() over (partition by c1,c2 order by c0) rk from wf_hash) where rk = 4;

select /*+PQ_DISTRIBUTE_WINDOW(@"SEL$2" (0) NONE PARTITION_SORT WF_TOPN)*/ count(*) from (select c1,c2,c0,row_number() over (partition by c1,c2 order by c0) rk from wf_hash) where rk < 2;
select /*+NO_PRED_DEDUCE*/count(*) from (select c1,c2,c0,row_number() over (partition by c1,c2 order by c0) rk from wf_hash) where rk < 2;
select /*+PQ_DISTRIBUTE_WINDOW(@"SEL$2" (0) NONE PARTITION_SORT WF_TOPN)*/ count(*) from (select c1,c2,c0,row_number() over (partition by c1,c2 order by c0) rk from wf_hash) where rk < 4;
select /*+NO_PRED_DEDUCE*/count(*) from (select c1,c2,c0,row_number() over (partition by c1,c2 order by c0) rk from wf_hash) where rk < 4;

select /*+PQ_DISTRIBUTE_WINDOW(@"SEL$2" (0) NONE PARTITION_SORT WF_TOPN)*/ count(*) from (select c1,c2,c0,row_number() over (partition by c1,c2 order by c0) rk from wf_hash) where rk <= 2;
select /*+NO_PRED_DEDUCE*/count(*) from (select c1,c2,c0,row_number() over (partition by c1,c2 order by c0) rk from wf_hash) where rk <= 2;
select /*+PQ_DISTRIBUTE_WINDOW(@"SEL$2" (0) NONE PARTITION_SORT WF_TOPN)*/ count(*) from (select c1,c2,c0,row_number() over (partition by c1,c2 order by c0) rk from wf_hash) where rk <= 4;
select /*+NO_PRED_DEDUCE*/count(*) from (select c1,c2,c0,row_number() over (partition by c1,c2 order by c0) rk from wf_hash) where rk <= 4;

select /*+PQ_DISTRIBUTE_WINDOW(@"SEL$2" (0) NONE PARTITION_SORT WF_TOPN)*/ count(*) from (select c1,c2,c0,row_number() over (partition by c1,c2 order by c0) rk from wf_hash) where 2 >= rk;
select /*+NO_PRED_DEDUCE*/count(*) from (select c1,c2,c0,row_number() over (partition by c1,c2 order by c0) rk from wf_hash) where 2 >= rk;
select /*+PQ_DISTRIBUTE_WINDOW(@"SEL$2" (0) NONE PARTITION_SORT WF_TOPN)*/ count(*) from (select c1,c2,c0,row_number() over (partition by c1,c2 order by c0) rk from wf_hash) where 4 >= rk;
select /*+NO_PRED_DEDUCE*/count(*) from (select c1,c2,c0,row_number() over (partition by c1,c2 order by c0) rk from wf_hash) where 4 >= rk;

select /*+PQ_DISTRIBUTE_WINDOW(@"SEL$2" (0) NONE PARTITION_SORT WF_TOPN)*/ count(*) from (select c1,c2,c0,row_number() over (partition by c1,c2 order by c0) rk from wf_hash) where 2 = rk;
select /*+NO_PRED_DEDUCE*/count(*) from (select c1,c2,c0,row_number() over (partition by c1,c2 order by c0) rk from wf_hash) where 2 = rk;
select /*+PQ_DISTRIBUTE_WINDOW(@"SEL$2" (0) NONE PARTITION_SORT WF_TOPN)*/ count(*) from (select c1,c2,c0,row_number() over (partition by c1,c2 order by c0) rk from wf_hash) where 4 = rk;
select /*+NO_PRED_DEDUCE*/count(*) from (select c1,c2,c0,row_number() over (partition by c1,c2 order by c0) rk from wf_hash) where 4 = rk;

select /*+PQ_DISTRIBUTE_WINDOW(@"SEL$2" (0) NONE PARTITION_SORT WF_TOPN)*/ count(*) from (select c1,c2,c0,row_number() over (partition by c1,c2 order by c0) rk from wf_hash) where 2 > rk;
select /*+NO_PRED_DEDUCE*/count(*) from (select c1,c2,c0,row_number() over (partition by c1,c2 order by c0) rk from wf_hash) where 2 > rk;
select count(*) from (select c1,c2,c0,row_number() over (partition by c1,c2 order by c0) rk from wf_hash) where 4 > rk;
select /*+NO_PRED_DEDUCE*/count(*) from (select c1,c2,c0,row_number() over (partition by c1,c2 order by c0) rk from wf_hash) where 4 > rk;

select count(*) from (select c1,c2,c0,row_number() over (partition by c1,c2 order by c0) rk from wf_hash) where rk >= 2;
select count(*) from (select c1,c2,c0,row_number() over (partition by c1,c2 order by c0) rk from wf_hash) where rk > 2;
select count(*) from (select c1,c2,c0,row_number() over (partition by c1,c2 order by c0) rk from wf_hash) where rk >= 4;
select count(*) from (select c1,c2,c0,row_number() over (partition by c1,c2 order by c0) rk from wf_hash) where rk > 4;

--echo ###### 2. window no partition by ###################(test topn sort)
select /*+PQ_DISTRIBUTE_WINDOW(@"SEL$2" (0) NONE WF_TOPN)*/count(*) from (select c1,c2,c0,rank() over (order by c1,c2,c0) rk from wf_hash) where rk = 1;
select /*+NO_PRED_DEDUCE*/count(*) from (select c1,c2,c0,rank() over (order by c0,c1,c2) rk from wf_hash) where rk = 1;

select /*+PQ_DISTRIBUTE_WINDOW(@"SEL$2" (0) NONE WF_TOPN)*/count(*) from (select c1,c2,c0,rank() over (order by c1,c2,c0) rk from wf_hash) where rk < 4;
select /*+NO_PRED_DEDUCE*/count(*) from (select c1,c2,c0,rank() over (order by c0,c1,c2) rk from wf_hash) where rk < 4;

select /*+PQ_DISTRIBUTE_WINDOW(@"SEL$2" (0) NONE WF_TOPN)*/count(*) from (select c1,c2,c0,row_number() over (order by c1,c2,c0) rk from wf_hash) where rk = 1;
select /*+NO_PRED_DEDUCE*/count(*) from (select c1,c2,c0,rank() over (order by c0,c1,c2) rk from wf_hash) where rk = 1;

select /*+PQ_DISTRIBUTE_WINDOW(@"SEL$2" (0) NONE WF_TOPN)*/count(*) from (select c1,c2,c0,row_number() over (order by c1,c2,c0) rk from wf_hash) where rk < 4;
select /*+NO_PRED_DEDUCE*/count(*) from (select c1,c2,c0,rank() over (order by c0,c1,c2) rk from wf_hash) where rk < 4;

#dense_rank don't use topn sort
select count(*) from (select c1,c2,c0,dense_rank() over (order by c1,c2,c0) rk from wf_hash) where rk = 1;
select count(*) from (select c1,c2,c0,dense_rank() over (order by c1,c2,c0) rk from wf_hash) where rk < 4;

--echo ###### 3. window no order by ###################
select /*+PQ_DISTRIBUTE_WINDOW(@"SEL$2" (0) NONE PARTITION_SORT WF_TOPN)*/count(*) from (select c1,c2,c0,row_number() over (partition by c1,c2) rk from wf_hash) where rk = 1;
select /*+NO_PRED_DEDUCE*/count(*) from (select c1,c2,c0,row_number() over (partition by c1,c2) rk from wf_hash) where rk = 1;
select /*+PQ_DISTRIBUTE_WINDOW(@"SEL$2" (0) NONE PARTITION_SORT WF_TOPN)*/count(*) from (select c1,c2,c0,row_number() over () rk from wf_hash) where rk = 1;
select /*+NO_PRED_DEDUCE*/count(*) from (select c1,c2,c0,row_number() over () rk from wf_hash) where rk = 1;

select /*+PQ_DISTRIBUTE_WINDOW(@"SEL$2" (0) NONE PARTITION_SORT WF_TOPN)*/count(*) from (select c1,c2,c0,row_number() over (partition by c1,c2) rk from wf_hash) where rk < 4;
select /*+NO_PRED_DEDUCE*/count(*) from (select c1,c2,c0,row_number() over (partition by c1,c2) rk from wf_hash) where rk < 4;
select /*+PQ_DISTRIBUTE_WINDOW(@"SEL$2" (0) NONE PARTITION_SORT WF_TOPN)*/count(*) from (select c1,c2,c0,row_number() over () rk from wf_hash) where rk < 4;
select /*+NO_PRED_DEDUCE*/count(*) from (select c1,c2,c0,row_number() over () rk from wf_hash) where rk < 4;

select /*+PQ_DISTRIBUTE_WINDOW(@"SEL$2" (0) NONE PARTITION_SORT WF_TOPN)*/count(*) from (select c1,c2,c0,rank() over (partition by c1,c2) rk from wf_hash) where rk = 1;
select /*+NO_PRED_DEDUCE*/count(*) from (select c1,c2,c0,rank() over (partition by c1,c2) rk from wf_hash) where rk = 1;
select /*+PQ_DISTRIBUTE_WINDOW(@"SEL$2" (0) NONE PARTITION_SORT WF_TOPN)*/count(*) from (select c1,c2,c0,rank() over () rk from wf_hash) where rk = 1;
select /*+NO_PRED_DEDUCE*/count(*) from (select c1,c2,c0,rank() over () rk from wf_hash) where rk = 1;

select /*+PQ_DISTRIBUTE_WINDOW(@"SEL$2" (0) NONE PARTITION_SORT WF_TOPN)*/count(*) from (select c1,c2,c0,rank() over (partition by c1,c2) rk from wf_hash) where rk < 4;
select /*+NO_PRED_DEDUCE*/count(*) from (select c1,c2,c0,rank() over (partition by c1,c2) rk from wf_hash) where rk < 4;
select /*+PQ_DISTRIBUTE_WINDOW(@"SEL$2" (0) NONE WF_TOPN)*/count(*) from (select c1,c2,c0,rank() over () rk from wf_hash) where rk < 4;
select /*+NO_PRED_DEDUCE*/count(*) from (select c1,c2,c0,rank() over () rk from wf_hash) where rk < 4;

--echo ###### 4. with limit ######################## (don't generate qualify filter using limit expr)
select count(*) from (select c1,c2,c0,row_number() over (partition by c1,c2 order by c0) rk from wf_hash) limit 5;
select /*+PQ_DISTRIBUTE_WINDOW(@"SEL$2" (0) NONE PARTITION_SORT WF_TOPN)*/ count(*) from (select c1,c2,c0,row_number() over (partition by c1,c2 order by c0) rk from wf_hash) where rk = 2 limit 5;
select /*+NO_REWRITE*/ count(*) from (select c1,c2,c0,row_number() over (partition by c1,c2 order by c0) rk from wf_hash) where rk = 2 limit 5;

select count(*) from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk from wf_hash) limit 5;
select /*+PQ_DISTRIBUTE_WINDOW(@"SEL$2" (0) NONE PARTITION_SORT WF_TOPN)*/count(*) from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk from wf_hash) where rk = 2 limit 5;
select /*+NO_REWRITE*/count(*) from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk from wf_hash) where rk = 2 limit 5;

--echo ###### 5. topn not integer ##################(don't use topn because of cast)
select count(*) from (select c1,c2,c0,row_number() over (partition by c1,c2 order by c0) rk from wf_hash) where rk < 2.1;
select count(*) from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk from wf_hash) where rk < 2.1;
--echo ###### 6. multi rank winfunc ################
--echo #basic test
select /*+PQ_DISTRIBUTE_WINDOW(@"SEL$2"  (0) NONE PARTITION_SORT WF_TOPN) */ count(*) from (
      select /*+NO_PROJECT_PRUNE*/
             rank() over (partition by c1,c2 order by c0) my_rank,
             row_number() over (partition by c1,c2 order by c0) my_rownum,
             dense_rank() over (partition by c1,c2 order by c0) my_drank
      from wf_hash)
where my_rank = 1;

select /*+NO_PRED_DEDUCE*/count(*) from (
      select /*+NO_PROJECT_PRUNE*/
             rank() over (partition by c1,c2 order by c0) my_rank,
             row_number() over (partition by c1,c2 order by c0) my_rownum,
             dense_rank() over (partition by c1,c2 order by c0) my_drank
      from wf_hash)
where my_rank = 1;

--echo #not only one group
select count(*) from (
      select /*+NO_PROJECT_PRUNE*/
             rank() over (partition by c1 order by c0) my_rank,
             row_number() over (partition by c1,c2 order by c0) my_rownum,
             dense_rank() over (partition by c1,c2 order by c0) my_drank
      from wf_hash)
where my_rank = 1;

--echo #different partition by
select count(*) from (
      select /*+NO_PROJECT_PRUNE*/
             rank() over (partition by c1,c2 order by c2,c1) my_rank,
             row_number() over (order by c2,c1) my_rownum,
             dense_rank() over (order by c2,c1) my_drank
      from wf_hash)
where my_rank = 1;

select count(*) from (
      select /*+NO_PROJECT_PRUNE*/
             rank() over (partition by c1,c2 order by c2,c1) my_rank,
             row_number() over (order by c2,c1) my_rownum,
             dense_rank() over (order by c2,c1) my_drank
      from wf_hash)
where my_rownum = 1;

select count(*) from (
      select /*+NO_PROJECT_PRUNE*/
             rank() over (partition by c1,c2) my_rank,
             row_number() over (order by c2,c1) my_rownum,
             dense_rank() over (order by c2,c1) my_drank
      from wf_hash)
where my_rownum = 3;

--echo #different order by
--echo ##order by is the same with partition by
select count(*) from (
      select /*+NO_PROJECT_PRUNE*/
             rank() over (partition by c1,c2 order by c2,c1 asc) my_rank,
             row_number() over (partition by c1,c2 order by c2,c1 desc) my_rownum
      from wf_hash)
where my_rank = 1;

--echo ##order by is the different from partition by
select count(*) from (
      select /*+NO_PROJECT_PRUNE*/
             rank() over (partition by c1 order by c2 asc) my_rank,
             row_number() over (partition by c1 order by c2 desc) my_rownum
      from wf_hash)
where my_rank = 1;

select count(*) from (
      select /*+NO_PROJECT_PRUNE*/
             rank() over (partition by c1 order by c2,c0 asc) my_rank,
             row_number() over (partition by c1 order by c2 asc) my_rownum
      from wf_hash)
where my_rank = 1;

select count(*) from (
      select /*+NO_PROJECT_PRUNE*/
             rank() over (partition by c1 order by c2,c0 asc) my_rank,
             row_number() over (partition by c1 order by c2 asc) my_rownum
      from wf_hash)
where my_rownum = 2;

--echo #different partition by order by but only one group
select count(*) from (
      select /*+NO_PROJECT_PRUNE*/
             rank() over (partition by c2,c1 order by c2,c1) my_rank,
             row_number() over (order by c2,c1) my_rownum,
             dense_rank() over (order by c2,c1) my_drank
      from wf_hash)
where my_rank = 2;

--echo #different partition expr
select count(*) from (
      select /*+NO_PROJECT_PRUNE*/
             rank() over (partition by c1,c2 order by c0) my_rank,
             row_number() over (partition by c1,c2) my_rownum,
             dense_rank() over (partition by c1) my_drank
      from wf_hash)
where my_rank = 1;

--echo #not rank winfunc
select count(*) from (
      select /*+NO_PROJECT_PRUNE*/
             rank() over (partition by c1,c2 order by c0) my_rank,
             max(c0) over (partition by c1,c2 order by c0) my_sum,
             dense_rank() over (partition by c1,c2 order by c0) my_drank
      from wf_hash)
where my_rank = 1;
--echo ###### 7. sort after winfunc ################
select * from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk from wf_hash) where rk = 1 order by c1,c2;
select /*+NO_PRED_DEDUCE*/* from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk from wf_hash) where rk = 1 order by c1,c2;
select * from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk from wf_hash order by c1,c2) where rk = 1;
select /*+NO_PRED_DEDUCE*/* from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk from wf_hash order by c1,c2) where rk = 1 order by c1,c2;

select * from (select c1,c2,c0,row_number() over (partition by c1,c2 order by c0) rk from wf_hash) where rk = 1 order by c1,c2;
select /*+NO_PRED_DEDUCE*/* from (select c1,c2,c0,row_number() over (partition by c1,c2 order by c0) rk from wf_hash) where rk = 1 order by c1,c2;
select * from (select c1,c2,c0,row_number() over (partition by c1,c2 order by c0) rk from wf_hash order by c1,c2) where rk = 1;
select /*+NO_PRED_DEDUCE*/* from (select c1,c2,c0,row_number() over (partition by c1,c2 order by c0) rk from wf_hash order by c1,c2) where rk = 1 order by c1,c2;

--explain_protocol 0
select * from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk from wf_hash) where rk = 3 order by c1,c2;
select /*+NO_PRED_DEDUCE*/* from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk from wf_hash) where rk = 3 order by c1,c2;
select * from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk from wf_hash order by c1,c2) where rk = 3;
select /*+NO_PRED_DEDUCE*/* from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk from wf_hash order by c1,c2) where rk = 3 order by c1,c2;

select * from (select c1,c2,c0,row_number() over (partition by c1,c2 order by c0) rk from wf_hash) where rk = 3 order by c1,c2;
select /*+NO_PRED_DEDUCE*/* from (select c1,c2,c0,row_number() over (partition by c1,c2 order by c0) rk from wf_hash) where rk = 3 order by c1,c2;
select * from (select c1,c2,c0,row_number() over (partition by c1,c2 order by c0) rk from wf_hash order by c1,c2) where rk = 3;
select /*+NO_PRED_DEDUCE*/* from (select c1,c2,c0,row_number() over (partition by c1,c2 order by c0) rk from wf_hash order by c1,c2) where rk = 3 order by c1,c2;

--echo ###### 8. sort before winfunc ################
--echo #use prefix sort instead of part topn
--explain_protocol 2
select count(*) from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk from (select * from wf_hash order by c1, c2)) where rk = 1;
select count(*) from (select c1,c2,c0,row_number() over (partition by c1,c2 order by c0) rk from (select * from wf_hash order by c1, c2)) where rk = 1;

--echo ###### 9. groupby before winfunc ################
--echo #use prefix sort
select count(*) from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk from wf_hash group by c1) where rk = 1;
--echo #use topn sort
select /*+PQ_DISTRIBUTE_WINDOW(@"SEL$2" (0) NONE PARTITION_SORT WF_TOPN)*/count(*) from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk from wf_hash group by c1,c2,c0) where rk = 2;
select /*+NO_PRED_DEDUCE*/count(*) from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk from wf_hash group by c1,c2,c0) where rk = 2;

select count(*) from (select c1,c2,c0,row_number() over (partition by c1,c2 order by c0) rk from wf_hash group by c1) where rk = 1;
select /*+PQ_DISTRIBUTE_WINDOW(@"SEL$2" (0) NONE PARTITION_SORT WF_TOPN)*/count(*) from (select c1,c2,c0,row_number() over (partition by c1,c2 order by c0) rk from wf_hash group by c1,c2,c0) where rk = 2;
select /*+NO_PRED_DEDUCE*/count(*) from (select c1,c2,c0,row_number() over (partition by c1,c2 order by c0) rk from wf_hash group by c1,c2,c0) where rk = 2;

--echo ###### 10. groupby after winfunc ################
select /*+PQ_DISTRIBUTE_WINDOW(@"SEL$2" (0) NONE PARTITION_SORT WF_TOPN)*/count(*) from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk from wf_hash) where rk = 2 group by c1,c2;
select /*+NO_PRED_DEDUCE*/count(*) from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk from wf_hash) where rk = 2 group by c1,c2;
select /*+PQ_DISTRIBUTE_WINDOW(@"SEL$2" (0) NONE PARTITION_SORT WF_TOPN)*/count(*) from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk from wf_hash) where rk = 2 group by c2;
select /*+NO_PRED_DEDUCE*/count(*) from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk from wf_hash) where rk = 2 group by c2;

select /*+PQ_DISTRIBUTE_WINDOW(@"SEL$2" (0) NONE PARTITION_SORT WF_TOPN)*/count(*) from (select c1,c2,c0,row_number() over (partition by c1,c2 order by c0) rk from wf_hash) where rk = 2 group by c1,c2;
select /*+NO_PRED_DEDUCE*/count(*) from (select c1,c2,c0,row_number() over (partition by c1,c2 order by c0) rk from wf_hash) where rk = 2 group by c1,c2;
select /*+PQ_DISTRIBUTE_WINDOW(@"SEL$2" (0) NONE PARTITION_SORT WF_TOPN)*/count(*) from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk from wf_hash) where rk = 2 group by c2;
select /*+NO_PRED_DEDUCE*/count(*) from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk from wf_hash) where rk = 2 group by c2;

--echo ###### 11. with rollup ################
select /*+PQ_DISTRIBUTE_WINDOW(@"SEL$2" (0) NONE PARTITION_SORT WF_TOPN) */ count(*) from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk from wf_hash group by c1 with rollup) where rk = 1;
select /*+NO_PRED_DEDUCE*/count(*) from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk from wf_hash group by c1 with rollup) where rk = 1;
select /*+PQ_DISTRIBUTE_WINDOW(@"SEL$2"  (0) NONE PARTITION_SORT WF_TOPN)*/ count(*) from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk from wf_hash group by c1,c2,c0 with rollup) where rk = 2;
select /*+NO_PRED_DEDUCE*/count(*) from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk from wf_hash group by c1,c2,c0 with rollup) where rk = 2;

select /*+PQ_DISTRIBUTE_WINDOW(@"SEL$2" (0) NONE PARTITION_SORT WF_TOPN) */ count(*) from (select c1,c2,c0,row_number() over (partition by c1,c2 order by c0) rk from wf_hash group by c1 with rollup) where rk = 1;
select /*+NO_PRED_DEDUCE*/count(*) from (select c1,c2,c0,row_number() over (partition by c1,c2 order by c0) rk from wf_hash group by c1 with rollup) where rk = 1;
select /*+PQ_DISTRIBUTE_WINDOW(@"SEL$2"  (0) NONE PARTITION_SORT WF_TOPN)*/ count(*) from (select c1,c2,c0,row_number() over (partition by c1,c2 order by c0) rk from wf_hash group by c1,c2,c0 with rollup) where rk = 2;
select /*+NO_PRED_DEDUCE*/count(*) from (select c1,c2,c0,row_number() over (partition by c1,c2 order by c0) rk from wf_hash group by c1,c2,c0 with rollup) where rk = 2;

select /*+PQ_DISTRIBUTE_WINDOW(@"SEL$2" (0) NONE PARTITION_SORT WF_TOPN) */count(*) from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk from wf_hash) where rk = 2 group by c1,c2 with rollup;
select /*+NO_PRED_DEDUCE*/count(*) from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk from wf_hash) where rk = 2 group by c1,c2 with rollup;
select /*+PQ_DISTRIBUTE_WINDOW(@"SEL$2" (0) NONE PARTITION_SORT WF_TOPN) */count(*) from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk from wf_hash) where rk = 2 group by c2 with rollup;
select /*+NO_PRED_DEDUCE*/count(*) from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk from wf_hash) where rk = 2 group by c2 with rollup;

select /*+PQ_DISTRIBUTE_WINDOW(@"SEL$2" (0) NONE PARTITION_SORT WF_TOPN) */count(*) from (select c1,c2,c0,row_number() over (partition by c1,c2 order by c0) rk from wf_hash) where rk = 2 group by c1,c2 with rollup;
select /*+NO_PRED_DEDUCE*/count(*) from (select c1,c2,c0,row_number() over (partition by c1,c2 order by c0) rk from wf_hash) where rk = 2 group by c1,c2 with rollup;
select /*+PQ_DISTRIBUTE_WINDOW(@"SEL$2" (0) NONE PARTITION_SORT WF_TOPN) */count(*) from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk from wf_hash) where rk = 2 group by c2 with rollup;
select /*+NO_PRED_DEDUCE*/count(*) from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk from wf_hash) where rk = 2 group by c2 with rollup;

--echo ###### 12.test multi qualify filters ################(use the first qualify filter to calc topn)
--echo #only one wingunc group
select count(*) from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk, row_number() over (partition by c1,c2 order by c0) rk2 from wf_hash) where rk < 3 and rk2 < 2;
select /*+NO_PRED_DEDUCE*/ count(*) from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk, row_number() over (partition by c1,c2 order by c0) rk2 from wf_hash) where rk < 3 and rk2 < 2;

select count(*) from (
      select /*+NO_PROJECT_PRUNE*/
             rank() over (partition by c1 order by c2,c0 asc) my_rank,
             row_number() over (partition by c1 order by c2 asc) my_rownum
      from wf_hash)
where my_rownum = 2 and my_rank = 1;

select /*+NO_PRED_DEDUCE*/ count(*) from (
      select /*+NO_PROJECT_PRUNE*/
             rank() over (partition by c1 order by c2,c0 asc) my_rank,
             row_number() over (partition by c1 order by c2 asc) my_rownum
      from wf_hash)
where my_rownum = 2 and my_rank = 1;

--echo #multi winfunc groups
select count(*) from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk, row_number() over (partition by c2 order by c0) rk2 from wf_hash) where rk < 3 and rk2 < 2;
select /*+NO_PRED_DEDUCE*/ count(*) from (select c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk, row_number() over (partition by c2 order by c0) rk2 from wf_hash) where rk < 3 and rk2 < 2;

--echo ###### 13.test user variable ################(don't pushdown rank filter)
set @a:= 1;
select /*+PQ_DISTRIBUTE_WINDOW(@"SEL$2" (0) NONE PARTITION_SORT WF_TOPN)*/ count(*) from (select @a := @a+1, c1,c2,c0,rank() over (partition by c1,c2 order by c0) rk from wf_hash) where rk < 3;
