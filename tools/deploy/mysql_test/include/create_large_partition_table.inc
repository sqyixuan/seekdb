--disable_query_log
--disable_result_log


--let $loop=1
--let $create_t1_sql=create table $TABLE_NAME (pk1 int, pk2 int
--let $column=1
while($column<= $LARGE_COLUMN_COUNT)
{
  --let $var_1kb=c$column varchar($COLUMN_WIDTH)
  --let $create_t1_sql=$create_t1_sql,$var_1kb
  inc $column;
}
--let $create_t1_sql=$create_t1_sql , primary key (pk1, pk2)) PARTITION BY HASH(pk1) PARTITIONS 2;
--echo $create_t1_sql;
eval $create_t1_sql;

#1kb
--let $sep = '
--let $var_1kb=$sep
--let $c = 0
--let $my64=0123456789012345678901234567890123456789012345678901234567890123

while ($c < $COLUMN_WIDTH)
{
  --let $var_1kb = $var_1kb$my64

  #advance 64 bytes
  let $loop = 0;
  while ($loop < 64)
  {
    inc $c;
    inc $loop;
  }
}
--let $var_1kb = $var_1kb$sep


# insert data
--let $row=1
while($row <= $ROW_COUNT)
{
  --let $one_row_value = ($row % 2 + 1, $row
  --let $column=1
  while($column<= $LARGE_COLUMN_COUNT)
  {
    --let $one_row_value=$one_row_value,$var_1kb
    inc $column;
  }
  --let $one_row_value = $one_row_value )

  #最后一行不加逗号

  --let $insert_t1_sql=INSERT INTO $TABLE_NAME VALUES $one_row_value

  eval $insert_t1_sql;

  inc $row;
}


--enable_query_log
--enable_result_log


