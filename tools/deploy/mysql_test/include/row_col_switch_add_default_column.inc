# make sure connection obsys to tenant root@sys exists
# input param: $table, with t1(c1 int not null, c2 int, c3 varchar(16) collate utf16_general_ci default 'dft_c3  ', c4 char(16) collate utf16_general_ci default 'dft_c4') 

--echo [INFO] Start testing add column with default value when row col switch for $table
--let table_name="$table"

## Case 1: before insert, only 1 major for each table
--source row_col_switch_check_major_sstables.inc

## Case 2: add c6/c7 with default value in column store
eval create index idx_c3_$table on t1(c3);
eval create index idx_c4_$table on t1(c4);
eval insert into $table (c1, c2) values (1, 1), (2, 2);
eval alter table $table modify c1 decimal(20, 2); 
eval alter table $table add c5 int default 0;
eval alter table $table add column c6 varchar(16) collate utf16_general_ci default 'dft_c6  ';
eval alter table $table add column c7    char(16) collate utf16_general_ci default 'dft_c7  ';
eval create index idx_c6_$table on t1(c6);
eval create index idx_c7_$table on t1(c7);
eval insert into $table (c1, c5, c6, c7) values (3, 3, 'pad_c6  ', 'pad_c7  ');
eval select *, char_length(c3), char_length(c4), char_length(c6), char_length(c7) from $table;

--echo [INFO] Start major freeze, column store -> row store
eval alter system major freeze;
--source wait_daily_merge.inc
--source row_col_switch_check_major_sstables.inc
eval select *, char_length(c3), char_length(c4), char_length(c6), char_length(c7) from $table;

## Case 3: add c8/c9 with default value in row store
eval insert into $table (c1, c2) values (4, 4), (5, 5);
eval alter table $table modify c1 decimal(20, 3); 
eval alter table $table add column c8 varchar(16) collate utf16_general_ci default 'dft_c8  ';
eval alter table $table add column c9    char(16) collate utf16_general_ci default 'dft_c9  ';
eval create index idx_c8_$table on t1(c8);
eval create index idx_c9_$table on t1(c9);
eval insert into $table (c1, c8, c9) values (6, 'pad_c8  ', 'pad_c9  ');
eval select *, char_length(c3), char_length(c4), char_length(c6), char_length(c7), char_length(c8), char_length(c9) from $table;

--echo [INFO] Start major freeze, row store -> row store
eval alter system major freeze;
--source wait_daily_merge.inc
--source row_col_switch_check_major_sstables.inc
eval select *, char_length(c3), char_length(c4), char_length(c6), char_length(c7), char_length(c8), char_length(c9) from $table;

## Case 4: insert many values, return back to column store
--let $count=0
--let $target_cnt=25000
--echo [INFO] Start insert into table $table $target_cnt lines
--disable_query_log
--disable_warnings

while ($count < $target_cnt)
{
    eval INSERT INTO $table (c1, c2, c5) VALUES ($count+1, $count+2, $count+3), ($count+2, $count+3, $count+4), ($count+3, $count+4, $count+5), ($count+4, $count+5, $count+6), ($count+5, $count+6, $count+7), ($count+6, $count+7, $count+8), ($count+7, $count+8, $count+9), ($count+8, $count+9, $count+10), ($count+9, $count+10, $count+11), ($count+10, $count+11, $count+12), ($count+11, $count+12, $count+13), ($count+12, $count+13, $count+14), ($count+13, $count+14, $count+15), ($count+14, $count+15, $count+16), ($count+15, $count+16, $count+17), ($count+16, $count+17, $count+18), ($count+17, $count+18, $count+19), ($count+18, $count+19, $count+20), ($count+19, $count+20, $count+21), ($count+20, $count+21, $count+22), ($count+21, $count+22, $count+23), ($count+22, $count+23, $count+24), ($count+23, $count+24, $count+25), ($count+24, $count+25, $count+26), ($count+25, $count+26, $count+27), ($count+26, $count+27, $count+28), ($count+27, $count+28, $count+29), ($count+28, $count+29, $count+30), ($count+29, $count+30, $count+31), ($count+30, $count+31, $count+32), ($count+31, $count+32, $count+33), ($count+32, $count+33, $count+34), ($count+33, $count+34, $count+35), ($count+34, $count+35, $count+36), ($count+35, $count+36, $count+37), ($count+36, $count+37, $count+38), ($count+37, $count+38, $count+39), ($count+38, $count+39, $count+40), ($count+39, $count+40, $count+41), ($count+40, $count+41, $count+42), ($count+41, $count+42, $count+43), ($count+42, $count+43, $count+44), ($count+43, $count+44, $count+45), ($count+44, $count+45, $count+46), ($count+45, $count+46, $count+47), ($count+46, $count+47, $count+48), ($count+47, $count+48, $count+49), ($count+48, $count+49, $count+50), ($count+49, $count+50, $count+51), ($count+50, $count+51, $count+52), ($count+51, $count+52, $count+53), ($count+52, $count+53, $count+54), ($count+53, $count+54, $count+55), ($count+54, $count+55, $count+56), ($count+55, $count+56, $count+57), ($count+56, $count+57, $count+58), ($count+57, $count+58, $count+59), ($count+58, $count+59, $count+60), ($count+59, $count+60, $count+61), ($count+60, $count+61, $count+62), ($count+61, $count+62, $count+63), ($count+62, $count+63, $count+64), ($count+63, $count+64, $count+65), ($count+64, $count+65, $count+66), ($count+65, $count+66, $count+67), ($count+66, $count+67, $count+68), ($count+67, $count+68, $count+69), ($count+68, $count+69, $count+70), ($count+69, $count+70, $count+71), ($count+70, $count+71, $count+72), ($count+71, $count+72, $count+73), ($count+72, $count+73, $count+74), ($count+73, $count+74, $count+75), ($count+74, $count+75, $count+76), ($count+75, $count+76, $count+77), ($count+76, $count+77, $count+78), ($count+77, $count+78, $count+79), ($count+78, $count+79, $count+80), ($count+79, $count+80, $count+81), ($count+80, $count+81, $count+82), ($count+81, $count+82, $count+83), ($count+82, $count+83, $count+84), ($count+83, $count+84, $count+85), ($count+84, $count+85, $count+86), ($count+85, $count+86, $count+87), ($count+86, $count+87, $count+88), ($count+87, $count+88, $count+89), ($count+88, $count+89, $count+90), ($count+89, $count+90, $count+91), ($count+90, $count+91, $count+92), ($count+91, $count+92, $count+93), ($count+92, $count+93, $count+94), ($count+93, $count+94, $count+95), ($count+94, $count+95, $count+96), ($count+95, $count+96, $count+97), ($count+96, $count+97, $count+98), ($count+97, $count+98, $count+99), ($count+98, $count+99, $count+100), ($count+99, $count+100, $count+101), ($count+100, $count+101, $count+102);
    --let $i=0
    while($i < 100)
    {
        inc $count;
        inc $i;
    }
}
--enable_warnings
--enable_query_log
eval select *, char_length(c3), char_length(c4), char_length(c6), char_length(c7), char_length(c8), char_length(c9) from $table limit 10;

--echo [INFO] Start major freeze, row store -> column store
eval alter system major freeze;
--source wait_daily_merge.inc
--source row_col_switch_check_major_sstables.inc
eval select *, char_length(c3), char_length(c4), char_length(c6), char_length(c7), char_length(c8), char_length(c9) from $table limit 10;

## Case 5: add c10/c11 with default value in column store
eval alter table $table add column c10 varchar(16) collate utf16_general_ci default 'dft_c10  ';
eval alter table $table add column c11    char(16) collate utf16_general_ci default 'dft_c11  ';
eval create index idx_c10_$table on t1(c10);
eval create index idx_c11_$table on t1(c11);
eval insert into $table (c1, c10, c11) values (7, 'pad_c10  ', 'pad_c11  ');
eval select *, char_length(c3), char_length(c4), char_length(c6), char_length(c7), char_length(c8), char_length(c9), char_length(c10), char_length(c11) from $table limit 10;

--echo [INFO] Start major freeze, column store -> column store
eval alter system major freeze;
--source wait_daily_merge.inc
--source row_col_switch_check_major_sstables.inc
eval select *, char_length(c3), char_length(c4), char_length(c6), char_length(c7), char_length(c8), char_length(c9), char_length(c10), char_length(c11) from $table limit 10;