--echo [INFO] Start insert into tables1 t1 ~ t6 for $insert_row_count lines

--disable_warnings
eval insert into t1 select seq1.nextval, seq1.nextval + 1 from table(generator($insert_row_count));
eval insert into t4 select seq4.nextval, seq4.nextval + 1 from table(generator($insert_row_count));

eval insert into t2 (c1, c2, c3) select seq2.nextval, seq2.nextval + 1, seq2.nextval + 2 from table(generator($insert_row_count));
eval insert into t5 (c1, c2, c3) select seq5.nextval, seq5.nextval + 1, seq5.nextval + 2 from table(generator($insert_row_count));

eval insert into t3 select seq3.nextval, seq3.nextval + 1, seq3.nextval + 2, seq3.nextval + 3, seq3.nextval + 4, seq3.nextval + 5, seq3.nextval + 6 from table(generator($insert_row_count));
eval insert into t6 select seq6.nextval, seq6.nextval + 1, seq6.nextval + 2, seq6.nextval + 3, seq6.nextval + 4, seq6.nextval + 5, seq6.nextval + 6 from table(generator($insert_row_count));
--enable_warnings

select count(*) from t1;
select count(*) from t2;
select count(*) from t3;
select count(*) from t4;
select count(*) from t5;
select count(*) from t6;