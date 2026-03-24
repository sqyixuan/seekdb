--echo Start insert into tables1 t1, t2 for $insert_row_count lines

eval INSERT INTO t1 select seq1.nextval, seq1.nextval + 1 from table(generator($insert_row_count));
eval INSERT INTO t2 select seq2.nextval, seq2.nextval + 1, seq2.nextval + 2, seq2.nextval + 3, seq2.nextval + 4, seq2.nextval + 5, seq2.nextval + 6 from table(generator($insert_row_count));

select count(*) from t1;
select count(*) from t2;
