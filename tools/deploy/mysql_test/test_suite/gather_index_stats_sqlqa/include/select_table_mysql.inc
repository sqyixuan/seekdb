
--echo  ###GENERAL_TAB_UNIFORM###
--sorted_result
SELECT table_name, partition_name, partition_position, subpartition_name, subpartition_position, object_type, num_rows, avg_row_len/*, last_analyzed*/ FROM oceanbase.DBA_TAB_STATISTICS WHERE table_name='GENERAL_TAB_UNIFORM' ORDER BY subpartition_name,partition_name;
--sorted_result
SELECT table_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets/*, last_analyzed*/, sample_size, avg_col_len, histogram FROM oceanbase.DBA_TAB_COL_STATISTICS WHERE table_name='GENERAL_TAB_UNIFORM' ORDER BY column_name;
--sorted_result
SELECT table_name, partition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_PART_COL_STATISTICS WHERE table_name='GENERAL_TAB_UNIFORM' ORDER BY partition_name,column_name;
--sorted_result
SELECT table_name, subpartition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_SUBPART_COL_STATISTICS WHERE table_name='GENERAL_TAB_UNIFORM' ORDER BY subpartition_name,column_name;
--sorted_result
SELECT table_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_TAB_HISTOGRAMS WHERE table_name='GENERAL_TAB_UNIFORM' ORDER BY column_name,endpoint_number;
--sorted_result
SELECT table_name, partition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_PART_HISTOGRAMS WHERE table_name='GENERAL_TAB_UNIFORM' ORDER BY partition_name,column_name,endpoint_number;
--sorted_result
SELECT table_name, subpartition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_SUBPART_HISTOGRAMS WHERE table_name='GENERAL_TAB_UNIFORM' ORDER BY subpartition_name,column_name,endpoint_number;


--echo  ###GENERAL_TAB_GAUSSIAN###
--sorted_result
SELECT table_name, partition_name, partition_position, subpartition_name, subpartition_position, object_type, num_rows, avg_row_len/*, last_analyzed*/ FROM oceanbase.DBA_TAB_STATISTICS WHERE table_name='GENERAL_TAB_GAUSSIAN' ORDER BY subpartition_name,partition_name;
--sorted_result
SELECT table_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets/*, last_analyzed*/, sample_size, avg_col_len, histogram FROM oceanbase.DBA_TAB_COL_STATISTICS WHERE table_name='GENERAL_TAB_GAUSSIAN' ORDER BY column_name;
--sorted_result
SELECT table_name, partition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_PART_COL_STATISTICS WHERE table_name='GENERAL_TAB_GAUSSIAN' ORDER BY partition_name,column_name;
--sorted_result
SELECT table_name, subpartition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_SUBPART_COL_STATISTICS WHERE table_name='GENERAL_TAB_GAUSSIAN' ORDER BY subpartition_name,column_name;
--sorted_result
SELECT table_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_TAB_HISTOGRAMS WHERE table_name='GENERAL_TAB_GAUSSIAN' ORDER BY column_name,endpoint_number;
--sorted_result
SELECT table_name, partition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_PART_HISTOGRAMS WHERE table_name='GENERAL_TAB_GAUSSIAN' ORDER BY partition_name,column_name,endpoint_number;
--sorted_result
SELECT table_name, subpartition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_SUBPART_HISTOGRAMS WHERE table_name='GENERAL_TAB_GAUSSIAN' ORDER BY subpartition_name,column_name,endpoint_number;


--echo  ###PARTITIONED_RANGE_TAB_UNIFORM###
--sorted_result
SELECT table_name, partition_name, partition_position, subpartition_name, subpartition_position, object_type, num_rows, avg_row_len/*, last_analyzed*/ FROM oceanbase.DBA_TAB_STATISTICS WHERE table_name='PARTITIONED_RANGE_TAB_UNIFORM' ORDER BY subpartition_name,partition_name;
--sorted_result
SELECT table_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets/*, last_analyzed*/, sample_size, avg_col_len, histogram FROM oceanbase.DBA_TAB_COL_STATISTICS WHERE table_name='PARTITIONED_RANGE_TAB_UNIFORM' ORDER BY column_name;
--sorted_result
SELECT table_name, partition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_PART_COL_STATISTICS WHERE table_name='PARTITIONED_RANGE_TAB_UNIFORM' ORDER BY partition_name,column_name;
--sorted_result
SELECT table_name, subpartition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_SUBPART_COL_STATISTICS WHERE table_name='PARTITIONED_RANGE_TAB_UNIFORM' ORDER BY subpartition_name,column_name;
--sorted_result
SELECT table_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_TAB_HISTOGRAMS WHERE table_name='PARTITIONED_RANGE_TAB_UNIFORM' ORDER BY column_name,endpoint_number;
--sorted_result
SELECT table_name, partition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_PART_HISTOGRAMS WHERE table_name='PARTITIONED_RANGE_TAB_UNIFORM' ORDER BY partition_name,column_name,endpoint_number;
--sorted_result
SELECT table_name, subpartition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_SUBPART_HISTOGRAMS WHERE table_name='PARTITIONED_RANGE_TAB_UNIFORM' ORDER BY subpartition_name,column_name,endpoint_number;


--echo  ###PARTITIONED_RANGE_TAB_GAUSSIAN###
--sorted_result
SELECT table_name, partition_name, partition_position, subpartition_name, subpartition_position, object_type, num_rows, avg_row_len/*, last_analyzed*/ FROM oceanbase.DBA_TAB_STATISTICS WHERE table_name='PARTITIONED_RANGE_TAB_GAUSSIAN' ORDER BY subpartition_name,partition_name;
--sorted_result
SELECT table_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets/*, last_analyzed*/, sample_size, avg_col_len, histogram FROM oceanbase.DBA_TAB_COL_STATISTICS WHERE table_name='PARTITIONED_RANGE_TAB_GAUSSIAN' ORDER BY column_name;
--sorted_result
SELECT table_name, partition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_PART_COL_STATISTICS WHERE table_name='PARTITIONED_RANGE_TAB_GAUSSIAN' ORDER BY partition_name,column_name;
--sorted_result
SELECT table_name, subpartition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_SUBPART_COL_STATISTICS WHERE table_name='PARTITIONED_RANGE_TAB_GAUSSIAN' ORDER BY subpartition_name,column_name;
--sorted_result
SELECT table_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_TAB_HISTOGRAMS WHERE table_name='PARTITIONED_RANGE_TAB_GAUSSIAN' ORDER BY column_name,endpoint_number;
--sorted_result
SELECT table_name, partition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_PART_HISTOGRAMS WHERE table_name='PARTITIONED_RANGE_TAB_GAUSSIAN' ORDER BY partition_name,column_name,endpoint_number;
--sorted_result
SELECT table_name, subpartition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_SUBPART_HISTOGRAMS WHERE table_name='PARTITIONED_RANGE_TAB_GAUSSIAN' ORDER BY subpartition_name,column_name,endpoint_number;


--echo  ###PARTITIONED_HASH_TAB_UNIFORM###
--sorted_result
SELECT table_name, partition_name, partition_position, subpartition_name, subpartition_position, object_type, num_rows, avg_row_len/*, last_analyzed*/ FROM oceanbase.DBA_TAB_STATISTICS WHERE table_name='PARTITIONED_HASH_TAB_UNIFORM' ORDER BY subpartition_name,partition_name;
--sorted_result
SELECT table_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets/*, last_analyzed*/, sample_size, avg_col_len, histogram FROM oceanbase.DBA_TAB_COL_STATISTICS WHERE table_name='PARTITIONED_HASH_TAB_UNIFORM' ORDER BY column_name;
--sorted_result
SELECT table_name, partition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_PART_COL_STATISTICS WHERE table_name='PARTITIONED_HASH_TAB_UNIFORM' ORDER BY partition_name,column_name;
--sorted_result
SELECT table_name, subpartition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_SUBPART_COL_STATISTICS WHERE table_name='PARTITIONED_HASH_TAB_UNIFORM' ORDER BY subpartition_name,column_name;
--sorted_result
SELECT table_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_TAB_HISTOGRAMS WHERE table_name='PARTITIONED_HASH_TAB_UNIFORM' ORDER BY column_name,endpoint_number;
--sorted_result
SELECT table_name, partition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_PART_HISTOGRAMS WHERE table_name='PARTITIONED_HASH_TAB_UNIFORM' ORDER BY partition_name,column_name,endpoint_number;
--sorted_result
SELECT table_name, subpartition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_SUBPART_HISTOGRAMS WHERE table_name='PARTITIONED_HASH_TAB_UNIFORM' ORDER BY subpartition_name,column_name,endpoint_number;


--echo  ###PARTITIONED_HASH_TAB_GAUSSIAN###
--sorted_result
SELECT table_name, partition_name, partition_position, subpartition_name, subpartition_position, object_type, num_rows, avg_row_len/*, last_analyzed*/ FROM oceanbase.DBA_TAB_STATISTICS WHERE table_name='PARTITIONED_HASH_TAB_GAUSSIAN' ORDER BY subpartition_name,partition_name;
--sorted_result
SELECT table_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets/*, last_analyzed*/, sample_size, avg_col_len, histogram FROM oceanbase.DBA_TAB_COL_STATISTICS WHERE table_name='PARTITIONED_HASH_TAB_GAUSSIAN' ORDER BY column_name;
--sorted_result
SELECT table_name, partition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_PART_COL_STATISTICS WHERE table_name='PARTITIONED_HASH_TAB_GAUSSIAN' ORDER BY partition_name,column_name;
--sorted_result
SELECT table_name, subpartition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_SUBPART_COL_STATISTICS WHERE table_name='PARTITIONED_HASH_TAB_GAUSSIAN' ORDER BY subpartition_name,column_name;
--sorted_result
SELECT table_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_TAB_HISTOGRAMS WHERE table_name='PARTITIONED_HASH_TAB_GAUSSIAN' ORDER BY column_name,endpoint_number;
--sorted_result
SELECT table_name, partition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_PART_HISTOGRAMS WHERE table_name='PARTITIONED_HASH_TAB_GAUSSIAN' ORDER BY partition_name,column_name,endpoint_number;
--sorted_result
SELECT table_name, subpartition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_SUBPART_HISTOGRAMS WHERE table_name='PARTITIONED_HASH_TAB_GAUSSIAN' ORDER BY subpartition_name,column_name,endpoint_number;


--echo  ###PARTITIONED_LIST_TAB_UNIFORM###
--sorted_result
SELECT table_name, partition_name, partition_position, subpartition_name, subpartition_position, object_type, num_rows, avg_row_len/*, last_analyzed*/ FROM oceanbase.DBA_TAB_STATISTICS WHERE table_name='PARTITIONED_LIST_TAB_UNIFORM' ORDER BY subpartition_name,partition_name;
--sorted_result
SELECT table_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets/*, last_analyzed*/, sample_size, avg_col_len, histogram FROM oceanbase.DBA_TAB_COL_STATISTICS WHERE table_name='PARTITIONED_LIST_TAB_UNIFORM' ORDER BY column_name;
--sorted_result
SELECT table_name, partition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_PART_COL_STATISTICS WHERE table_name='PARTITIONED_LIST_TAB_UNIFORM' ORDER BY partition_name,column_name;
--sorted_result
SELECT table_name, subpartition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_SUBPART_COL_STATISTICS WHERE table_name='PARTITIONED_LIST_TAB_UNIFORM' ORDER BY subpartition_name,column_name;
--sorted_result
SELECT table_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_TAB_HISTOGRAMS WHERE table_name='PARTITIONED_LIST_TAB_UNIFORM' ORDER BY column_name,endpoint_number;
--sorted_result
SELECT table_name, partition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_PART_HISTOGRAMS WHERE table_name='PARTITIONED_LIST_TAB_UNIFORM' ORDER BY partition_name,column_name,endpoint_number;
--sorted_result
SELECT table_name, subpartition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_SUBPART_HISTOGRAMS WHERE table_name='PARTITIONED_LIST_TAB_UNIFORM' ORDER BY subpartition_name,column_name,endpoint_number;


--echo  ###PARTITIONED_LIST_TAB_GAUSSIAN###
--sorted_result
SELECT table_name, partition_name, partition_position, subpartition_name, subpartition_position, object_type, num_rows, avg_row_len/*, last_analyzed*/ FROM oceanbase.DBA_TAB_STATISTICS WHERE table_name='PARTITIONED_LIST_TAB_GAUSSIAN' ORDER BY subpartition_name,partition_name;
--sorted_result
SELECT table_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets/*, last_analyzed*/, sample_size, avg_col_len, histogram FROM oceanbase.DBA_TAB_COL_STATISTICS WHERE table_name='PARTITIONED_LIST_TAB_GAUSSIAN' ORDER BY column_name;
--sorted_result
SELECT table_name, partition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_PART_COL_STATISTICS WHERE table_name='PARTITIONED_LIST_TAB_GAUSSIAN' ORDER BY partition_name,column_name;
--sorted_result
SELECT table_name, subpartition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_SUBPART_COL_STATISTICS WHERE table_name='PARTITIONED_LIST_TAB_GAUSSIAN' ORDER BY subpartition_name,column_name;
--sorted_result
SELECT table_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_TAB_HISTOGRAMS WHERE table_name='PARTITIONED_LIST_TAB_GAUSSIAN' ORDER BY column_name,endpoint_number;
--sorted_result
SELECT table_name, partition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_PART_HISTOGRAMS WHERE table_name='PARTITIONED_LIST_TAB_GAUSSIAN' ORDER BY partition_name,column_name,endpoint_number;
--sorted_result
SELECT table_name, subpartition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_SUBPART_HISTOGRAMS WHERE table_name='PARTITIONED_LIST_TAB_GAUSSIAN' ORDER BY subpartition_name,column_name,endpoint_number;


--echo  ###PARTITIONED_RANGEHASH_TAB_UNIFORM###
--sorted_result
SELECT table_name, partition_name, partition_position, subpartition_name, subpartition_position, object_type, num_rows, avg_row_len/*, last_analyzed*/ FROM oceanbase.DBA_TAB_STATISTICS WHERE table_name='PARTITIONED_RANGEHASH_TAB_UNIFORM' ORDER BY subpartition_name,partition_name;
--sorted_result
SELECT table_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets/*, last_analyzed*/, sample_size, avg_col_len, histogram FROM oceanbase.DBA_TAB_COL_STATISTICS WHERE table_name='PARTITIONED_RANGEHASH_TAB_UNIFORM' ORDER BY column_name;
--sorted_result
SELECT table_name, partition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_PART_COL_STATISTICS WHERE table_name='PARTITIONED_RANGEHASH_TAB_UNIFORM' ORDER BY partition_name,column_name;
--sorted_result
SELECT table_name, subpartition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_SUBPART_COL_STATISTICS WHERE table_name='PARTITIONED_RANGEHASH_TAB_UNIFORM' ORDER BY subpartition_name,column_name;
--sorted_result
SELECT table_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_TAB_HISTOGRAMS WHERE table_name='PARTITIONED_RANGEHASH_TAB_UNIFORM' ORDER BY column_name,endpoint_number;
--sorted_result
SELECT table_name, partition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_PART_HISTOGRAMS WHERE table_name='PARTITIONED_RANGEHASH_TAB_UNIFORM' ORDER BY partition_name,column_name,endpoint_number;
--sorted_result
SELECT table_name, subpartition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_SUBPART_HISTOGRAMS WHERE table_name='PARTITIONED_RANGEHASH_TAB_UNIFORM' ORDER BY subpartition_name,column_name,endpoint_number;


--echo  ###PARTITIONED_RANGEHASH_TAB_GAUSSIAN###
--sorted_result
SELECT table_name, partition_name, partition_position, subpartition_name, subpartition_position, object_type, num_rows, avg_row_len/*, last_analyzed*/ FROM oceanbase.DBA_TAB_STATISTICS WHERE table_name='PARTITIONED_RANGEHASH_TAB_GAUSSIAN' ORDER BY subpartition_name,partition_name;
--sorted_result
SELECT table_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets/*, last_analyzed*/, sample_size, avg_col_len, histogram FROM oceanbase.DBA_TAB_COL_STATISTICS WHERE table_name='PARTITIONED_RANGEHASH_TAB_GAUSSIAN' ORDER BY column_name;
--sorted_result
SELECT table_name, partition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_PART_COL_STATISTICS WHERE table_name='PARTITIONED_RANGEHASH_TAB_GAUSSIAN' ORDER BY partition_name,column_name;
--sorted_result
SELECT table_name, subpartition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_SUBPART_COL_STATISTICS WHERE table_name='PARTITIONED_RANGEHASH_TAB_GAUSSIAN' ORDER BY subpartition_name,column_name;
--sorted_result
SELECT table_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_TAB_HISTOGRAMS WHERE table_name='PARTITIONED_RANGEHASH_TAB_GAUSSIAN' ORDER BY column_name,endpoint_number;
--sorted_result
SELECT table_name, partition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_PART_HISTOGRAMS WHERE table_name='PARTITIONED_RANGEHASH_TAB_GAUSSIAN' ORDER BY partition_name,column_name,endpoint_number;
--sorted_result
SELECT table_name, subpartition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_SUBPART_HISTOGRAMS WHERE table_name='PARTITIONED_RANGEHASH_TAB_GAUSSIAN' ORDER BY subpartition_name,column_name,endpoint_number;


--echo  ###PARTITIONED_RANGELIST_TAB_UNIFORM###
--sorted_result
SELECT table_name, partition_name, partition_position, subpartition_name, subpartition_position, object_type, num_rows, avg_row_len/*, last_analyzed*/ FROM oceanbase.DBA_TAB_STATISTICS WHERE table_name='PARTITIONED_RANGELIST_TAB_UNIFORM' ORDER BY subpartition_name,partition_name;
--sorted_result
SELECT table_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets/*, last_analyzed*/, sample_size, avg_col_len, histogram FROM oceanbase.DBA_TAB_COL_STATISTICS WHERE table_name='PARTITIONED_RANGELIST_TAB_UNIFORM' ORDER BY column_name;
--sorted_result
SELECT table_name, partition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_PART_COL_STATISTICS WHERE table_name='PARTITIONED_RANGELIST_TAB_UNIFORM' ORDER BY partition_name,column_name;
--sorted_result
SELECT table_name, subpartition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_SUBPART_COL_STATISTICS WHERE table_name='PARTITIONED_RANGELIST_TAB_UNIFORM' ORDER BY subpartition_name,column_name;
--sorted_result
SELECT table_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_TAB_HISTOGRAMS WHERE table_name='PARTITIONED_RANGELIST_TAB_UNIFORM' ORDER BY column_name,endpoint_number;
--sorted_result
SELECT table_name, partition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_PART_HISTOGRAMS WHERE table_name='PARTITIONED_RANGELIST_TAB_UNIFORM' ORDER BY partition_name,column_name,endpoint_number;
--sorted_result
SELECT table_name, subpartition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_SUBPART_HISTOGRAMS WHERE table_name='PARTITIONED_RANGELIST_TAB_UNIFORM' ORDER BY subpartition_name,column_name,endpoint_number;


--echo  ###PARTITIONED_RANGELIST_TAB_GAUSSIAN###
--sorted_result
SELECT table_name, partition_name, partition_position, subpartition_name, subpartition_position, object_type, num_rows, avg_row_len/*, last_analyzed*/ FROM oceanbase.DBA_TAB_STATISTICS WHERE table_name='PARTITIONED_RANGELIST_TAB_GAUSSIAN' ORDER BY subpartition_name,partition_name;
--sorted_result
SELECT table_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets/*, last_analyzed*/, sample_size, avg_col_len, histogram FROM oceanbase.DBA_TAB_COL_STATISTICS WHERE table_name='PARTITIONED_RANGELIST_TAB_GAUSSIAN' ORDER BY column_name;
--sorted_result
SELECT table_name, partition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_PART_COL_STATISTICS WHERE table_name='PARTITIONED_RANGELIST_TAB_GAUSSIAN' ORDER BY partition_name,column_name;
--sorted_result
SELECT table_name, subpartition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_SUBPART_COL_STATISTICS WHERE table_name='PARTITIONED_RANGELIST_TAB_GAUSSIAN' ORDER BY subpartition_name,column_name;
--sorted_result
SELECT table_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_TAB_HISTOGRAMS WHERE table_name='PARTITIONED_RANGELIST_TAB_GAUSSIAN' ORDER BY column_name,endpoint_number;
--sorted_result
SELECT table_name, partition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_PART_HISTOGRAMS WHERE table_name='PARTITIONED_RANGELIST_TAB_GAUSSIAN' ORDER BY partition_name,column_name,endpoint_number;
--sorted_result
SELECT table_name, subpartition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_SUBPART_HISTOGRAMS WHERE table_name='PARTITIONED_RANGELIST_TAB_GAUSSIAN' ORDER BY subpartition_name,column_name,endpoint_number;


--echo  ###PARTITIONED_LISTRANGE_TAB_UNIFORM###
--sorted_result
SELECT table_name, partition_name, partition_position, subpartition_name, subpartition_position, object_type, num_rows, avg_row_len/*, last_analyzed*/ FROM oceanbase.DBA_TAB_STATISTICS WHERE table_name='PARTITIONED_LISTRANGE_TAB_UNIFORM' ORDER BY subpartition_name,partition_name;
--sorted_result
SELECT table_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets/*, last_analyzed*/, sample_size, avg_col_len, histogram FROM oceanbase.DBA_TAB_COL_STATISTICS WHERE table_name='PARTITIONED_LISTRANGE_TAB_UNIFORM' ORDER BY column_name;
--sorted_result
SELECT table_name, partition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_PART_COL_STATISTICS WHERE table_name='PARTITIONED_LISTRANGE_TAB_UNIFORM' ORDER BY partition_name,column_name;
--sorted_result
SELECT table_name, subpartition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_SUBPART_COL_STATISTICS WHERE table_name='PARTITIONED_LISTRANGE_TAB_UNIFORM' ORDER BY subpartition_name,column_name;
--sorted_result
SELECT table_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_TAB_HISTOGRAMS WHERE table_name='PARTITIONED_LISTRANGE_TAB_UNIFORM' ORDER BY column_name,endpoint_number;
--sorted_result
SELECT table_name, partition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_PART_HISTOGRAMS WHERE table_name='PARTITIONED_LISTRANGE_TAB_UNIFORM' ORDER BY partition_name,column_name,endpoint_number;
--sorted_result
SELECT table_name, subpartition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_SUBPART_HISTOGRAMS WHERE table_name='PARTITIONED_LISTRANGE_TAB_UNIFORM' ORDER BY subpartition_name,column_name,endpoint_number;


--echo  ###PARTITIONED_LISTRANGE_TAB_GAUSSIAN###
--sorted_result
SELECT table_name, partition_name, partition_position, subpartition_name, subpartition_position, object_type, num_rows, avg_row_len/*, last_analyzed*/ FROM oceanbase.DBA_TAB_STATISTICS WHERE table_name='PARTITIONED_LISTRANGE_TAB_GAUSSIAN' ORDER BY subpartition_name,partition_name;
--sorted_result
SELECT table_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets/*, last_analyzed*/, sample_size, avg_col_len, histogram FROM oceanbase.DBA_TAB_COL_STATISTICS WHERE table_name='PARTITIONED_LISTRANGE_TAB_GAUSSIAN' ORDER BY column_name;
--sorted_result
SELECT table_name, partition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_PART_COL_STATISTICS WHERE table_name='PARTITIONED_LISTRANGE_TAB_GAUSSIAN' ORDER BY partition_name,column_name;
--sorted_result
SELECT table_name, subpartition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_SUBPART_COL_STATISTICS WHERE table_name='PARTITIONED_LISTRANGE_TAB_GAUSSIAN' ORDER BY subpartition_name,column_name;
--sorted_result
SELECT table_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_TAB_HISTOGRAMS WHERE table_name='PARTITIONED_LISTRANGE_TAB_GAUSSIAN' ORDER BY column_name,endpoint_number;
--sorted_result
SELECT table_name, partition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_PART_HISTOGRAMS WHERE table_name='PARTITIONED_LISTRANGE_TAB_GAUSSIAN' ORDER BY partition_name,column_name,endpoint_number;
--sorted_result
SELECT table_name, subpartition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_SUBPART_HISTOGRAMS WHERE table_name='PARTITIONED_LISTRANGE_TAB_GAUSSIAN' ORDER BY subpartition_name,column_name,endpoint_number;


--echo  ###PARTITIONED_LISTRANGE_TAB_GAUSSIAN2###
--sorted_result
SELECT table_name, partition_name, partition_position, subpartition_name, subpartition_position, object_type, num_rows, avg_row_len/*, last_analyzed*/ FROM oceanbase.DBA_TAB_STATISTICS WHERE table_name='PARTITIONED_LISTRANGE_TAB_GAUSSIAN2' ORDER BY subpartition_name,partition_name;
--sorted_result
SELECT table_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets/*, last_analyzed*/, sample_size, avg_col_len, histogram FROM oceanbase.DBA_TAB_COL_STATISTICS WHERE table_name='PARTITIONED_LISTRANGE_TAB_GAUSSIAN2' ORDER BY column_name;
--sorted_result
SELECT table_name, partition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_PART_COL_STATISTICS WHERE table_name='PARTITIONED_LISTRANGE_TAB_GAUSSIAN2' ORDER BY partition_name,column_name;
--sorted_result
SELECT table_name, subpartition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_SUBPART_COL_STATISTICS WHERE table_name='PARTITIONED_LISTRANGE_TAB_GAUSSIAN2' ORDER BY subpartition_name,column_name;
--sorted_result
SELECT table_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_TAB_HISTOGRAMS WHERE table_name='PARTITIONED_LISTRANGE_TAB_GAUSSIAN2' ORDER BY column_name,endpoint_number;
--sorted_result
SELECT table_name, partition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_PART_HISTOGRAMS WHERE table_name='PARTITIONED_LISTRANGE_TAB_GAUSSIAN2' ORDER BY partition_name,column_name,endpoint_number;
--sorted_result
SELECT table_name, subpartition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_SUBPART_HISTOGRAMS WHERE table_name='PARTITIONED_LISTRANGE_TAB_GAUSSIAN2' ORDER BY subpartition_name,column_name,endpoint_number;


--echo  ###PARTITIONED_LISTRANGE_TAB_UNIFORM2###
--sorted_result
SELECT table_name, partition_name, partition_position, subpartition_name, subpartition_position, object_type, num_rows, avg_row_len/*, last_analyzed*/ FROM oceanbase.DBA_TAB_STATISTICS WHERE table_name='PARTITIONED_LISTRANGE_TAB_UNIFORM2' ORDER BY subpartition_name,partition_name;
--sorted_result
SELECT table_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets/*, last_analyzed*/, sample_size, avg_col_len, histogram FROM oceanbase.DBA_TAB_COL_STATISTICS WHERE table_name='PARTITIONED_LISTRANGE_TAB_UNIFORM2' ORDER BY column_name;
--sorted_result
SELECT table_name, partition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_PART_COL_STATISTICS WHERE table_name='PARTITIONED_LISTRANGE_TAB_UNIFORM2' ORDER BY partition_name,column_name;
--sorted_result
SELECT table_name, subpartition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_SUBPART_COL_STATISTICS WHERE table_name='PARTITIONED_LISTRANGE_TAB_UNIFORM2' ORDER BY subpartition_name,column_name;
--sorted_result
SELECT table_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_TAB_HISTOGRAMS WHERE table_name='PARTITIONED_LISTRANGE_TAB_UNIFORM2' ORDER BY column_name,endpoint_number;
--sorted_result
SELECT table_name, partition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_PART_HISTOGRAMS WHERE table_name='PARTITIONED_LISTRANGE_TAB_UNIFORM2' ORDER BY partition_name,column_name,endpoint_number;
--sorted_result
SELECT table_name, subpartition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_SUBPART_HISTOGRAMS WHERE table_name='PARTITIONED_LISTRANGE_TAB_UNIFORM2' ORDER BY subpartition_name,column_name,endpoint_number;


--echo  ###PARTITIONED_LISTHASH_TAB_UNIFORM###
--sorted_result
SELECT table_name, partition_name, partition_position, subpartition_name, subpartition_position, object_type, num_rows, avg_row_len/*, last_analyzed*/ FROM oceanbase.DBA_TAB_STATISTICS WHERE table_name='PARTITIONED_LISTHASH_TAB_UNIFORM' ORDER BY subpartition_name,partition_name;
--sorted_result
SELECT table_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets/*, last_analyzed*/, sample_size, avg_col_len, histogram FROM oceanbase.DBA_TAB_COL_STATISTICS WHERE table_name='PARTITIONED_LISTHASH_TAB_UNIFORM' ORDER BY column_name;
--sorted_result
SELECT table_name, partition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_PART_COL_STATISTICS WHERE table_name='PARTITIONED_LISTHASH_TAB_UNIFORM' ORDER BY partition_name,column_name;
--sorted_result
SELECT table_name, subpartition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_SUBPART_COL_STATISTICS WHERE table_name='PARTITIONED_LISTHASH_TAB_UNIFORM' ORDER BY subpartition_name,column_name;
--sorted_result
SELECT table_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_TAB_HISTOGRAMS WHERE table_name='PARTITIONED_LISTHASH_TAB_UNIFORM' ORDER BY column_name,endpoint_number;
--sorted_result
SELECT table_name, partition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_PART_HISTOGRAMS WHERE table_name='PARTITIONED_LISTHASH_TAB_UNIFORM' ORDER BY partition_name,column_name,endpoint_number;
--sorted_result
SELECT table_name, subpartition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_SUBPART_HISTOGRAMS WHERE table_name='PARTITIONED_LISTHASH_TAB_UNIFORM' ORDER BY subpartition_name,column_name,endpoint_number;


--echo  ###PARTITIONED_LISTHASH_TAB_GAUSSIAN###
--sorted_result
SELECT table_name, partition_name, partition_position, subpartition_name, subpartition_position, object_type, num_rows, avg_row_len/*, last_analyzed*/ FROM oceanbase.DBA_TAB_STATISTICS WHERE table_name='PARTITIONED_LISTHASH_TAB_GAUSSIAN' ORDER BY subpartition_name,partition_name;
--sorted_result
SELECT table_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets/*, last_analyzed*/, sample_size, avg_col_len, histogram FROM oceanbase.DBA_TAB_COL_STATISTICS WHERE table_name='PARTITIONED_LISTHASH_TAB_GAUSSIAN' ORDER BY column_name;
--sorted_result
SELECT table_name, partition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_PART_COL_STATISTICS WHERE table_name='PARTITIONED_LISTHASH_TAB_GAUSSIAN' ORDER BY partition_name,column_name;
--sorted_result
SELECT table_name, subpartition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_SUBPART_COL_STATISTICS WHERE table_name='PARTITIONED_LISTHASH_TAB_GAUSSIAN' ORDER BY subpartition_name,column_name;
--sorted_result
SELECT table_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_TAB_HISTOGRAMS WHERE table_name='PARTITIONED_LISTHASH_TAB_GAUSSIAN' ORDER BY column_name,endpoint_number;
--sorted_result
SELECT table_name, partition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_PART_HISTOGRAMS WHERE table_name='PARTITIONED_LISTHASH_TAB_GAUSSIAN' ORDER BY partition_name,column_name,endpoint_number;
--sorted_result
SELECT table_name, subpartition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_SUBPART_HISTOGRAMS WHERE table_name='PARTITIONED_LISTHASH_TAB_GAUSSIAN' ORDER BY subpartition_name,column_name,endpoint_number;


--echo  ###COMPOSITE_PARTITIONED_TAB###
--sorted_result
SELECT table_name, partition_name, partition_position, subpartition_name, subpartition_position, object_type, num_rows, avg_row_len/*, last_analyzed*/ FROM oceanbase.DBA_TAB_STATISTICS WHERE table_name='COMPOSITE_PARTITIONED_TAB' ORDER BY subpartition_name,partition_name;
--sorted_result
SELECT table_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets/*, last_analyzed*/, sample_size, avg_col_len, histogram FROM oceanbase.DBA_TAB_COL_STATISTICS WHERE table_name='COMPOSITE_PARTITIONED_TAB' ORDER BY column_name;
--sorted_result
SELECT table_name, partition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_PART_COL_STATISTICS WHERE table_name='COMPOSITE_PARTITIONED_TAB' ORDER BY partition_name,column_name;
--sorted_result
SELECT table_name, subpartition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_SUBPART_COL_STATISTICS WHERE table_name='COMPOSITE_PARTITIONED_TAB' ORDER BY subpartition_name,column_name;
--sorted_result
SELECT table_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_TAB_HISTOGRAMS WHERE table_name='COMPOSITE_PARTITIONED_TAB' ORDER BY column_name,endpoint_number;
--sorted_result
SELECT table_name, partition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_PART_HISTOGRAMS WHERE table_name='COMPOSITE_PARTITIONED_TAB' ORDER BY partition_name,column_name,endpoint_number;
--sorted_result
SELECT table_name, subpartition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_SUBPART_HISTOGRAMS WHERE table_name='COMPOSITE_PARTITIONED_TAB' ORDER BY subpartition_name,column_name,endpoint_number;


--echo  ###TMP_COMPOSITE_PARTITIONED_TAB###
--sorted_result
SELECT table_name, partition_name, partition_position, subpartition_name, subpartition_position, object_type, num_rows, avg_row_len/*, last_analyzed*/ FROM oceanbase.DBA_TAB_STATISTICS WHERE table_name='TMP_COMPOSITE_PARTITIONED_TAB' ORDER BY subpartition_name,partition_name;
--sorted_result
SELECT table_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets/*, last_analyzed*/, sample_size, avg_col_len, histogram FROM oceanbase.DBA_TAB_COL_STATISTICS WHERE table_name='TMP_COMPOSITE_PARTITIONED_TAB' ORDER BY column_name;
--sorted_result
SELECT table_name, partition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_PART_COL_STATISTICS WHERE table_name='TMP_COMPOSITE_PARTITIONED_TAB' ORDER BY partition_name,column_name;
--sorted_result
SELECT table_name, subpartition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_SUBPART_COL_STATISTICS WHERE table_name='TMP_COMPOSITE_PARTITIONED_TAB' ORDER BY subpartition_name,column_name;
--sorted_result
SELECT table_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_TAB_HISTOGRAMS WHERE table_name='TMP_COMPOSITE_PARTITIONED_TAB' ORDER BY column_name,endpoint_number;
--sorted_result
SELECT table_name, partition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_PART_HISTOGRAMS WHERE table_name='TMP_COMPOSITE_PARTITIONED_TAB' ORDER BY partition_name,column_name,endpoint_number;
--sorted_result
SELECT table_name, subpartition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_SUBPART_HISTOGRAMS WHERE table_name='TMP_COMPOSITE_PARTITIONED_TAB' ORDER BY subpartition_name,column_name,endpoint_number;


--echo  ###PARTITIONED_TAB###
--sorted_result
SELECT table_name, partition_name, partition_position, subpartition_name, subpartition_position, object_type, num_rows, avg_row_len/*, last_analyzed*/ FROM oceanbase.DBA_TAB_STATISTICS WHERE table_name='PARTITIONED_TAB' ORDER BY subpartition_name,partition_name;
--sorted_result
SELECT table_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets/*, last_analyzed*/, sample_size, avg_col_len, histogram FROM oceanbase.DBA_TAB_COL_STATISTICS WHERE table_name='PARTITIONED_TAB' ORDER BY column_name;
--sorted_result
SELECT table_name, partition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_PART_COL_STATISTICS WHERE table_name='PARTITIONED_TAB' ORDER BY partition_name,column_name;
--sorted_result
SELECT table_name, subpartition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_SUBPART_COL_STATISTICS WHERE table_name='PARTITIONED_TAB' ORDER BY subpartition_name,column_name;
--sorted_result
SELECT table_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_TAB_HISTOGRAMS WHERE table_name='PARTITIONED_TAB' ORDER BY column_name,endpoint_number;
--sorted_result
SELECT table_name, partition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_PART_HISTOGRAMS WHERE table_name='PARTITIONED_TAB' ORDER BY partition_name,column_name,endpoint_number;
--sorted_result
SELECT table_name, subpartition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_SUBPART_HISTOGRAMS WHERE table_name='PARTITIONED_TAB' ORDER BY subpartition_name,column_name,endpoint_number;


--echo  ###GENERAL_TAB###
--sorted_result
SELECT table_name, partition_name, partition_position, subpartition_name, subpartition_position, object_type, num_rows, avg_row_len/*, last_analyzed*/ FROM oceanbase.DBA_TAB_STATISTICS WHERE table_name='GENERAL_TAB' ORDER BY subpartition_name,partition_name;
--sorted_result
SELECT table_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets/*, last_analyzed*/, sample_size, avg_col_len, histogram FROM oceanbase.DBA_TAB_COL_STATISTICS WHERE table_name='GENERAL_TAB' ORDER BY column_name;
--sorted_result
SELECT table_name, partition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_PART_COL_STATISTICS WHERE table_name='GENERAL_TAB' ORDER BY partition_name,column_name;
--sorted_result
SELECT table_name, subpartition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_SUBPART_COL_STATISTICS WHERE table_name='GENERAL_TAB' ORDER BY subpartition_name,column_name;
--sorted_result
SELECT table_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_TAB_HISTOGRAMS WHERE table_name='GENERAL_TAB' ORDER BY column_name,endpoint_number;
--sorted_result
SELECT table_name, partition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_PART_HISTOGRAMS WHERE table_name='GENERAL_TAB' ORDER BY partition_name,column_name,endpoint_number;
--sorted_result
SELECT table_name, subpartition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_SUBPART_HISTOGRAMS WHERE table_name='GENERAL_TAB' ORDER BY subpartition_name,column_name,endpoint_number;


--echo  ###I_GENERAL_TAB_UNIFORM###
--sorted_result
SELECT table_name, partition_name, partition_position, subpartition_name, subpartition_position, object_type, num_rows, avg_row_len/*, last_analyzed*/ FROM oceanbase.DBA_TAB_STATISTICS WHERE table_name='I_GENERAL_TAB_UNIFORM' ORDER BY subpartition_name,partition_name;
--sorted_result
SELECT table_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets/*, last_analyzed*/, sample_size, avg_col_len, histogram FROM oceanbase.DBA_TAB_COL_STATISTICS WHERE table_name='I_GENERAL_TAB_UNIFORM' ORDER BY column_name;
--sorted_result
SELECT table_name, partition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_PART_COL_STATISTICS WHERE table_name='I_GENERAL_TAB_UNIFORM' ORDER BY partition_name,column_name;
--sorted_result
SELECT table_name, subpartition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_SUBPART_COL_STATISTICS WHERE table_name='I_GENERAL_TAB_UNIFORM' ORDER BY subpartition_name,column_name;
--sorted_result
SELECT table_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_TAB_HISTOGRAMS WHERE table_name='I_GENERAL_TAB_UNIFORM' ORDER BY column_name,endpoint_number;
--sorted_result
SELECT table_name, partition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_PART_HISTOGRAMS WHERE table_name='I_GENERAL_TAB_UNIFORM' ORDER BY partition_name,column_name,endpoint_number;
--sorted_result
SELECT table_name, subpartition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_SUBPART_HISTOGRAMS WHERE table_name='I_GENERAL_TAB_UNIFORM' ORDER BY subpartition_name,column_name,endpoint_number;


--echo  ###I_GENERAL_TAB_GAUSSIAN###
--sorted_result
SELECT table_name, partition_name, partition_position, subpartition_name, subpartition_position, object_type, num_rows, avg_row_len/*, last_analyzed*/ FROM oceanbase.DBA_TAB_STATISTICS WHERE table_name='I_GENERAL_TAB_GAUSSIAN' ORDER BY subpartition_name,partition_name;
--sorted_result
SELECT table_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets/*, last_analyzed*/, sample_size, avg_col_len, histogram FROM oceanbase.DBA_TAB_COL_STATISTICS WHERE table_name='I_GENERAL_TAB_GAUSSIAN' ORDER BY column_name;
--sorted_result
SELECT table_name, partition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_PART_COL_STATISTICS WHERE table_name='I_GENERAL_TAB_GAUSSIAN' ORDER BY partition_name,column_name;
--sorted_result
SELECT table_name, subpartition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_SUBPART_COL_STATISTICS WHERE table_name='I_GENERAL_TAB_GAUSSIAN' ORDER BY subpartition_name,column_name;
--sorted_result
SELECT table_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_TAB_HISTOGRAMS WHERE table_name='I_GENERAL_TAB_GAUSSIAN' ORDER BY column_name,endpoint_number;
--sorted_result
SELECT table_name, partition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_PART_HISTOGRAMS WHERE table_name='I_GENERAL_TAB_GAUSSIAN' ORDER BY partition_name,column_name,endpoint_number;
--sorted_result
SELECT table_name, subpartition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_SUBPART_HISTOGRAMS WHERE table_name='I_GENERAL_TAB_GAUSSIAN' ORDER BY subpartition_name,column_name,endpoint_number;


--echo  ###I_PARTITIONED_RANGE_TAB_UNIFORM###
--sorted_result
SELECT table_name, partition_name, partition_position, subpartition_name, subpartition_position, object_type, num_rows, avg_row_len/*, last_analyzed*/ FROM oceanbase.DBA_TAB_STATISTICS WHERE table_name='I_PARTITIONED_RANGE_TAB_UNIFORM' ORDER BY subpartition_name,partition_name;
--sorted_result
SELECT table_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets/*, last_analyzed*/, sample_size, avg_col_len, histogram FROM oceanbase.DBA_TAB_COL_STATISTICS WHERE table_name='I_PARTITIONED_RANGE_TAB_UNIFORM' ORDER BY column_name;
--sorted_result
SELECT table_name, partition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_PART_COL_STATISTICS WHERE table_name='I_PARTITIONED_RANGE_TAB_UNIFORM' ORDER BY partition_name,column_name;
--sorted_result
SELECT table_name, subpartition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_SUBPART_COL_STATISTICS WHERE table_name='I_PARTITIONED_RANGE_TAB_UNIFORM' ORDER BY subpartition_name,column_name;
--sorted_result
SELECT table_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_TAB_HISTOGRAMS WHERE table_name='I_PARTITIONED_RANGE_TAB_UNIFORM' ORDER BY column_name,endpoint_number;
--sorted_result
SELECT table_name, partition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_PART_HISTOGRAMS WHERE table_name='I_PARTITIONED_RANGE_TAB_UNIFORM' ORDER BY partition_name,column_name,endpoint_number;
--sorted_result
SELECT table_name, subpartition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_SUBPART_HISTOGRAMS WHERE table_name='I_PARTITIONED_RANGE_TAB_UNIFORM' ORDER BY subpartition_name,column_name,endpoint_number;


--echo  ###I_PARTITIONED_RANGE_TAB_GAUSSIAN###
--sorted_result
SELECT table_name, partition_name, partition_position, subpartition_name, subpartition_position, object_type, num_rows, avg_row_len/*, last_analyzed*/ FROM oceanbase.DBA_TAB_STATISTICS WHERE table_name='I_PARTITIONED_RANGE_TAB_GAUSSIAN' ORDER BY subpartition_name,partition_name;
--sorted_result
SELECT table_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets/*, last_analyzed*/, sample_size, avg_col_len, histogram FROM oceanbase.DBA_TAB_COL_STATISTICS WHERE table_name='I_PARTITIONED_RANGE_TAB_GAUSSIAN' ORDER BY column_name;
--sorted_result
SELECT table_name, partition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_PART_COL_STATISTICS WHERE table_name='I_PARTITIONED_RANGE_TAB_GAUSSIAN' ORDER BY partition_name,column_name;
--sorted_result
SELECT table_name, subpartition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_SUBPART_COL_STATISTICS WHERE table_name='I_PARTITIONED_RANGE_TAB_GAUSSIAN' ORDER BY subpartition_name,column_name;
--sorted_result
SELECT table_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_TAB_HISTOGRAMS WHERE table_name='I_PARTITIONED_RANGE_TAB_GAUSSIAN' ORDER BY column_name,endpoint_number;
--sorted_result
SELECT table_name, partition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_PART_HISTOGRAMS WHERE table_name='I_PARTITIONED_RANGE_TAB_GAUSSIAN' ORDER BY partition_name,column_name,endpoint_number;
--sorted_result
SELECT table_name, subpartition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_SUBPART_HISTOGRAMS WHERE table_name='I_PARTITIONED_RANGE_TAB_GAUSSIAN' ORDER BY subpartition_name,column_name,endpoint_number;


--echo  ###I_PARTITIONED_HASH_TAB_UNIFORM###
--sorted_result
SELECT table_name, partition_name, partition_position, subpartition_name, subpartition_position, object_type, num_rows, avg_row_len/*, last_analyzed*/ FROM oceanbase.DBA_TAB_STATISTICS WHERE table_name='I_PARTITIONED_HASH_TAB_UNIFORM' ORDER BY subpartition_name,partition_name;
--sorted_result
SELECT table_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets/*, last_analyzed*/, sample_size, avg_col_len, histogram FROM oceanbase.DBA_TAB_COL_STATISTICS WHERE table_name='I_PARTITIONED_HASH_TAB_UNIFORM' ORDER BY column_name;
--sorted_result
SELECT table_name, partition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_PART_COL_STATISTICS WHERE table_name='I_PARTITIONED_HASH_TAB_UNIFORM' ORDER BY partition_name,column_name;
--sorted_result
SELECT table_name, subpartition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_SUBPART_COL_STATISTICS WHERE table_name='I_PARTITIONED_HASH_TAB_UNIFORM' ORDER BY subpartition_name,column_name;
--sorted_result
SELECT table_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_TAB_HISTOGRAMS WHERE table_name='I_PARTITIONED_HASH_TAB_UNIFORM' ORDER BY column_name,endpoint_number;
--sorted_result
SELECT table_name, partition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_PART_HISTOGRAMS WHERE table_name='I_PARTITIONED_HASH_TAB_UNIFORM' ORDER BY partition_name,column_name,endpoint_number;
--sorted_result
SELECT table_name, subpartition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_SUBPART_HISTOGRAMS WHERE table_name='I_PARTITIONED_HASH_TAB_UNIFORM' ORDER BY subpartition_name,column_name,endpoint_number;


--echo  ###I_PARTITIONED_HASH_TAB_GAUSSIAN###
--sorted_result
SELECT table_name, partition_name, partition_position, subpartition_name, subpartition_position, object_type, num_rows, avg_row_len/*, last_analyzed*/ FROM oceanbase.DBA_TAB_STATISTICS WHERE table_name='I_PARTITIONED_HASH_TAB_GAUSSIAN' ORDER BY subpartition_name,partition_name;
--sorted_result
SELECT table_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets/*, last_analyzed*/, sample_size, avg_col_len, histogram FROM oceanbase.DBA_TAB_COL_STATISTICS WHERE table_name='I_PARTITIONED_HASH_TAB_GAUSSIAN' ORDER BY column_name;
--sorted_result
SELECT table_name, partition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_PART_COL_STATISTICS WHERE table_name='I_PARTITIONED_HASH_TAB_GAUSSIAN' ORDER BY partition_name,column_name;
--sorted_result
SELECT table_name, subpartition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_SUBPART_COL_STATISTICS WHERE table_name='I_PARTITIONED_HASH_TAB_GAUSSIAN' ORDER BY subpartition_name,column_name;
--sorted_result
SELECT table_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_TAB_HISTOGRAMS WHERE table_name='I_PARTITIONED_HASH_TAB_GAUSSIAN' ORDER BY column_name,endpoint_number;
--sorted_result
SELECT table_name, partition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_PART_HISTOGRAMS WHERE table_name='I_PARTITIONED_HASH_TAB_GAUSSIAN' ORDER BY partition_name,column_name,endpoint_number;
--sorted_result
SELECT table_name, subpartition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_SUBPART_HISTOGRAMS WHERE table_name='I_PARTITIONED_HASH_TAB_GAUSSIAN' ORDER BY subpartition_name,column_name,endpoint_number;


--echo  ###I_PARTITIONED_LIST_TAB_UNIFORM###
--sorted_result
SELECT table_name, partition_name, partition_position, subpartition_name, subpartition_position, object_type, num_rows, avg_row_len/*, last_analyzed*/ FROM oceanbase.DBA_TAB_STATISTICS WHERE table_name='I_PARTITIONED_LIST_TAB_UNIFORM' ORDER BY subpartition_name,partition_name;
--sorted_result
SELECT table_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets/*, last_analyzed*/, sample_size, avg_col_len, histogram FROM oceanbase.DBA_TAB_COL_STATISTICS WHERE table_name='I_PARTITIONED_LIST_TAB_UNIFORM' ORDER BY column_name;
--sorted_result
SELECT table_name, partition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_PART_COL_STATISTICS WHERE table_name='I_PARTITIONED_LIST_TAB_UNIFORM' ORDER BY partition_name,column_name;
--sorted_result
SELECT table_name, subpartition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_SUBPART_COL_STATISTICS WHERE table_name='I_PARTITIONED_LIST_TAB_UNIFORM' ORDER BY subpartition_name,column_name;
--sorted_result
SELECT table_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_TAB_HISTOGRAMS WHERE table_name='I_PARTITIONED_LIST_TAB_UNIFORM' ORDER BY column_name,endpoint_number;
--sorted_result
SELECT table_name, partition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_PART_HISTOGRAMS WHERE table_name='I_PARTITIONED_LIST_TAB_UNIFORM' ORDER BY partition_name,column_name,endpoint_number;
--sorted_result
SELECT table_name, subpartition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_SUBPART_HISTOGRAMS WHERE table_name='I_PARTITIONED_LIST_TAB_UNIFORM' ORDER BY subpartition_name,column_name,endpoint_number;


--echo  ###I_PARTITIONED_LIST_TAB_GAUSSIAN###
--sorted_result
SELECT table_name, partition_name, partition_position, subpartition_name, subpartition_position, object_type, num_rows, avg_row_len/*, last_analyzed*/ FROM oceanbase.DBA_TAB_STATISTICS WHERE table_name='I_PARTITIONED_LIST_TAB_GAUSSIAN' ORDER BY subpartition_name,partition_name;
--sorted_result
SELECT table_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets/*, last_analyzed*/, sample_size, avg_col_len, histogram FROM oceanbase.DBA_TAB_COL_STATISTICS WHERE table_name='I_PARTITIONED_LIST_TAB_GAUSSIAN' ORDER BY column_name;
--sorted_result
SELECT table_name, partition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_PART_COL_STATISTICS WHERE table_name='I_PARTITIONED_LIST_TAB_GAUSSIAN' ORDER BY partition_name,column_name;
--sorted_result
SELECT table_name, subpartition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_SUBPART_COL_STATISTICS WHERE table_name='I_PARTITIONED_LIST_TAB_GAUSSIAN' ORDER BY subpartition_name,column_name;
--sorted_result
SELECT table_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_TAB_HISTOGRAMS WHERE table_name='I_PARTITIONED_LIST_TAB_GAUSSIAN' ORDER BY column_name,endpoint_number;
--sorted_result
SELECT table_name, partition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_PART_HISTOGRAMS WHERE table_name='I_PARTITIONED_LIST_TAB_GAUSSIAN' ORDER BY partition_name,column_name,endpoint_number;
--sorted_result
SELECT table_name, subpartition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_SUBPART_HISTOGRAMS WHERE table_name='I_PARTITIONED_LIST_TAB_GAUSSIAN' ORDER BY subpartition_name,column_name,endpoint_number;


--echo  ###I_PARTITIONED_RANGEHASH_TAB_UNIFORM###
--sorted_result
SELECT table_name, partition_name, partition_position, subpartition_name, subpartition_position, object_type, num_rows, avg_row_len/*, last_analyzed*/ FROM oceanbase.DBA_TAB_STATISTICS WHERE table_name='I_PARTITIONED_RANGEHASH_TAB_UNIFORM' ORDER BY subpartition_name,partition_name;
--sorted_result
SELECT table_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets/*, last_analyzed*/, sample_size, avg_col_len, histogram FROM oceanbase.DBA_TAB_COL_STATISTICS WHERE table_name='I_PARTITIONED_RANGEHASH_TAB_UNIFORM' ORDER BY column_name;
--sorted_result
SELECT table_name, partition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_PART_COL_STATISTICS WHERE table_name='I_PARTITIONED_RANGEHASH_TAB_UNIFORM' ORDER BY partition_name,column_name;
--sorted_result
SELECT table_name, subpartition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_SUBPART_COL_STATISTICS WHERE table_name='I_PARTITIONED_RANGEHASH_TAB_UNIFORM' ORDER BY subpartition_name,column_name;
--sorted_result
SELECT table_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_TAB_HISTOGRAMS WHERE table_name='I_PARTITIONED_RANGEHASH_TAB_UNIFORM' ORDER BY column_name,endpoint_number;
--sorted_result
SELECT table_name, partition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_PART_HISTOGRAMS WHERE table_name='I_PARTITIONED_RANGEHASH_TAB_UNIFORM' ORDER BY partition_name,column_name,endpoint_number;
--sorted_result
SELECT table_name, subpartition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_SUBPART_HISTOGRAMS WHERE table_name='I_PARTITIONED_RANGEHASH_TAB_UNIFORM' ORDER BY subpartition_name,column_name,endpoint_number;


--echo  ###I_PARTITIONED_RANGEHASH_TAB_GAUSSIAN###
--sorted_result
SELECT table_name, partition_name, partition_position, subpartition_name, subpartition_position, object_type, num_rows, avg_row_len/*, last_analyzed*/ FROM oceanbase.DBA_TAB_STATISTICS WHERE table_name='I_PARTITIONED_RANGEHASH_TAB_GAUSSIAN' ORDER BY subpartition_name,partition_name;
--sorted_result
SELECT table_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets/*, last_analyzed*/, sample_size, avg_col_len, histogram FROM oceanbase.DBA_TAB_COL_STATISTICS WHERE table_name='I_PARTITIONED_RANGEHASH_TAB_GAUSSIAN' ORDER BY column_name;
--sorted_result
SELECT table_name, partition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_PART_COL_STATISTICS WHERE table_name='I_PARTITIONED_RANGEHASH_TAB_GAUSSIAN' ORDER BY partition_name,column_name;
--sorted_result
SELECT table_name, subpartition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_SUBPART_COL_STATISTICS WHERE table_name='I_PARTITIONED_RANGEHASH_TAB_GAUSSIAN' ORDER BY subpartition_name,column_name;
--sorted_result
SELECT table_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_TAB_HISTOGRAMS WHERE table_name='I_PARTITIONED_RANGEHASH_TAB_GAUSSIAN' ORDER BY column_name,endpoint_number;
--sorted_result
SELECT table_name, partition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_PART_HISTOGRAMS WHERE table_name='I_PARTITIONED_RANGEHASH_TAB_GAUSSIAN' ORDER BY partition_name,column_name,endpoint_number;
--sorted_result
SELECT table_name, subpartition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_SUBPART_HISTOGRAMS WHERE table_name='I_PARTITIONED_RANGEHASH_TAB_GAUSSIAN' ORDER BY subpartition_name,column_name,endpoint_number;


--echo  ###I_PARTITIONED_RANGELIST_TAB_UNIFORM###
--sorted_result
SELECT table_name, partition_name, partition_position, subpartition_name, subpartition_position, object_type, num_rows, avg_row_len/*, last_analyzed*/ FROM oceanbase.DBA_TAB_STATISTICS WHERE table_name='I_PARTITIONED_RANGELIST_TAB_UNIFORM' ORDER BY subpartition_name,partition_name;
--sorted_result
SELECT table_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets/*, last_analyzed*/, sample_size, avg_col_len, histogram FROM oceanbase.DBA_TAB_COL_STATISTICS WHERE table_name='I_PARTITIONED_RANGELIST_TAB_UNIFORM' ORDER BY column_name;
--sorted_result
SELECT table_name, partition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_PART_COL_STATISTICS WHERE table_name='I_PARTITIONED_RANGELIST_TAB_UNIFORM' ORDER BY partition_name,column_name;
--sorted_result
SELECT table_name, subpartition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_SUBPART_COL_STATISTICS WHERE table_name='I_PARTITIONED_RANGELIST_TAB_UNIFORM' ORDER BY subpartition_name,column_name;
--sorted_result
SELECT table_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_TAB_HISTOGRAMS WHERE table_name='I_PARTITIONED_RANGELIST_TAB_UNIFORM' ORDER BY column_name,endpoint_number;
--sorted_result
SELECT table_name, partition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_PART_HISTOGRAMS WHERE table_name='I_PARTITIONED_RANGELIST_TAB_UNIFORM' ORDER BY partition_name,column_name,endpoint_number;
--sorted_result
SELECT table_name, subpartition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_SUBPART_HISTOGRAMS WHERE table_name='I_PARTITIONED_RANGELIST_TAB_UNIFORM' ORDER BY subpartition_name,column_name,endpoint_number;


--echo  ###I_PARTITIONED_RANGELIST_TAB_GAUSSIAN###
--sorted_result
SELECT table_name, partition_name, partition_position, subpartition_name, subpartition_position, object_type, num_rows, avg_row_len/*, last_analyzed*/ FROM oceanbase.DBA_TAB_STATISTICS WHERE table_name='I_PARTITIONED_RANGELIST_TAB_GAUSSIAN' ORDER BY subpartition_name,partition_name;
--sorted_result
SELECT table_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets/*, last_analyzed*/, sample_size, avg_col_len, histogram FROM oceanbase.DBA_TAB_COL_STATISTICS WHERE table_name='I_PARTITIONED_RANGELIST_TAB_GAUSSIAN' ORDER BY column_name;
--sorted_result
SELECT table_name, partition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_PART_COL_STATISTICS WHERE table_name='I_PARTITIONED_RANGELIST_TAB_GAUSSIAN' ORDER BY partition_name,column_name;
--sorted_result
SELECT table_name, subpartition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_SUBPART_COL_STATISTICS WHERE table_name='I_PARTITIONED_RANGELIST_TAB_GAUSSIAN' ORDER BY subpartition_name,column_name;
--sorted_result
SELECT table_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_TAB_HISTOGRAMS WHERE table_name='I_PARTITIONED_RANGELIST_TAB_GAUSSIAN' ORDER BY column_name,endpoint_number;
--sorted_result
SELECT table_name, partition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_PART_HISTOGRAMS WHERE table_name='I_PARTITIONED_RANGELIST_TAB_GAUSSIAN' ORDER BY partition_name,column_name,endpoint_number;
--sorted_result
SELECT table_name, subpartition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_SUBPART_HISTOGRAMS WHERE table_name='I_PARTITIONED_RANGELIST_TAB_GAUSSIAN' ORDER BY subpartition_name,column_name,endpoint_number;


--echo  ###I_PARTITIONED_LISTRANGE_TAB_UNIFORM###
--sorted_result
SELECT table_name, partition_name, partition_position, subpartition_name, subpartition_position, object_type, num_rows, avg_row_len/*, last_analyzed*/ FROM oceanbase.DBA_TAB_STATISTICS WHERE table_name='I_PARTITIONED_LISTRANGE_TAB_UNIFORM' ORDER BY subpartition_name,partition_name;
--sorted_result
SELECT table_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets/*, last_analyzed*/, sample_size, avg_col_len, histogram FROM oceanbase.DBA_TAB_COL_STATISTICS WHERE table_name='I_PARTITIONED_LISTRANGE_TAB_UNIFORM' ORDER BY column_name;
--sorted_result
SELECT table_name, partition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_PART_COL_STATISTICS WHERE table_name='I_PARTITIONED_LISTRANGE_TAB_UNIFORM' ORDER BY partition_name,column_name;
--sorted_result
SELECT table_name, subpartition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_SUBPART_COL_STATISTICS WHERE table_name='I_PARTITIONED_LISTRANGE_TAB_UNIFORM' ORDER BY subpartition_name,column_name;
--sorted_result
SELECT table_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_TAB_HISTOGRAMS WHERE table_name='I_PARTITIONED_LISTRANGE_TAB_UNIFORM' ORDER BY column_name,endpoint_number;
--sorted_result
SELECT table_name, partition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_PART_HISTOGRAMS WHERE table_name='I_PARTITIONED_LISTRANGE_TAB_UNIFORM' ORDER BY partition_name,column_name,endpoint_number;
--sorted_result
SELECT table_name, subpartition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_SUBPART_HISTOGRAMS WHERE table_name='I_PARTITIONED_LISTRANGE_TAB_UNIFORM' ORDER BY subpartition_name,column_name,endpoint_number;


--echo  ###I_PARTITIONED_LISTRANGE_TAB_GAUSSIAN###
--sorted_result
SELECT table_name, partition_name, partition_position, subpartition_name, subpartition_position, object_type, num_rows, avg_row_len/*, last_analyzed*/ FROM oceanbase.DBA_TAB_STATISTICS WHERE table_name='I_PARTITIONED_LISTRANGE_TAB_GAUSSIAN' ORDER BY subpartition_name,partition_name;
--sorted_result
SELECT table_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets/*, last_analyzed*/, sample_size, avg_col_len, histogram FROM oceanbase.DBA_TAB_COL_STATISTICS WHERE table_name='I_PARTITIONED_LISTRANGE_TAB_GAUSSIAN' ORDER BY column_name;
--sorted_result
SELECT table_name, partition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_PART_COL_STATISTICS WHERE table_name='I_PARTITIONED_LISTRANGE_TAB_GAUSSIAN' ORDER BY partition_name,column_name;
--sorted_result
SELECT table_name, subpartition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_SUBPART_COL_STATISTICS WHERE table_name='I_PARTITIONED_LISTRANGE_TAB_GAUSSIAN' ORDER BY subpartition_name,column_name;
--sorted_result
SELECT table_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_TAB_HISTOGRAMS WHERE table_name='I_PARTITIONED_LISTRANGE_TAB_GAUSSIAN' ORDER BY column_name,endpoint_number;
--sorted_result
SELECT table_name, partition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_PART_HISTOGRAMS WHERE table_name='I_PARTITIONED_LISTRANGE_TAB_GAUSSIAN' ORDER BY partition_name,column_name,endpoint_number;
--sorted_result
SELECT table_name, subpartition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_SUBPART_HISTOGRAMS WHERE table_name='I_PARTITIONED_LISTRANGE_TAB_GAUSSIAN' ORDER BY subpartition_name,column_name,endpoint_number;


--echo  ###I_PARTITIONED_LISTRANGE_TAB_GAUSSIAN2###
--sorted_result
SELECT table_name, partition_name, partition_position, subpartition_name, subpartition_position, object_type, num_rows, avg_row_len/*, last_analyzed*/ FROM oceanbase.DBA_TAB_STATISTICS WHERE table_name='I_PARTITIONED_LISTRANGE_TAB_GAUSSIAN2' ORDER BY subpartition_name,partition_name;
--sorted_result
SELECT table_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets/*, last_analyzed*/, sample_size, avg_col_len, histogram FROM oceanbase.DBA_TAB_COL_STATISTICS WHERE table_name='I_PARTITIONED_LISTRANGE_TAB_GAUSSIAN2' ORDER BY column_name;
--sorted_result
SELECT table_name, partition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_PART_COL_STATISTICS WHERE table_name='I_PARTITIONED_LISTRANGE_TAB_GAUSSIAN2' ORDER BY partition_name,column_name;
--sorted_result
SELECT table_name, subpartition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_SUBPART_COL_STATISTICS WHERE table_name='I_PARTITIONED_LISTRANGE_TAB_GAUSSIAN2' ORDER BY subpartition_name,column_name;
--sorted_result
SELECT table_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_TAB_HISTOGRAMS WHERE table_name='I_PARTITIONED_LISTRANGE_TAB_GAUSSIAN2' ORDER BY column_name,endpoint_number;
--sorted_result
SELECT table_name, partition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_PART_HISTOGRAMS WHERE table_name='I_PARTITIONED_LISTRANGE_TAB_GAUSSIAN2' ORDER BY partition_name,column_name,endpoint_number;
--sorted_result
SELECT table_name, subpartition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_SUBPART_HISTOGRAMS WHERE table_name='I_PARTITIONED_LISTRANGE_TAB_GAUSSIAN2' ORDER BY subpartition_name,column_name,endpoint_number;


--echo  ###I_PARTITIONED_LISTRANGE_TAB_UNIFORM2###
--sorted_result
SELECT table_name, partition_name, partition_position, subpartition_name, subpartition_position, object_type, num_rows, avg_row_len/*, last_analyzed*/ FROM oceanbase.DBA_TAB_STATISTICS WHERE table_name='I_PARTITIONED_LISTRANGE_TAB_UNIFORM2' ORDER BY subpartition_name,partition_name;
--sorted_result
SELECT table_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets/*, last_analyzed*/, sample_size, avg_col_len, histogram FROM oceanbase.DBA_TAB_COL_STATISTICS WHERE table_name='I_PARTITIONED_LISTRANGE_TAB_UNIFORM2' ORDER BY column_name;
--sorted_result
SELECT table_name, partition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_PART_COL_STATISTICS WHERE table_name='I_PARTITIONED_LISTRANGE_TAB_UNIFORM2' ORDER BY partition_name,column_name;
--sorted_result
SELECT table_name, subpartition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_SUBPART_COL_STATISTICS WHERE table_name='I_PARTITIONED_LISTRANGE_TAB_UNIFORM2' ORDER BY subpartition_name,column_name;
--sorted_result
SELECT table_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_TAB_HISTOGRAMS WHERE table_name='I_PARTITIONED_LISTRANGE_TAB_UNIFORM2' ORDER BY column_name,endpoint_number;
--sorted_result
SELECT table_name, partition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_PART_HISTOGRAMS WHERE table_name='I_PARTITIONED_LISTRANGE_TAB_UNIFORM2' ORDER BY partition_name,column_name,endpoint_number;
--sorted_result
SELECT table_name, subpartition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_SUBPART_HISTOGRAMS WHERE table_name='I_PARTITIONED_LISTRANGE_TAB_UNIFORM2' ORDER BY subpartition_name,column_name,endpoint_number;


--echo  ###I_PARTITIONED_LISTHASH_TAB_UNIFORM###
--sorted_result
SELECT table_name, partition_name, partition_position, subpartition_name, subpartition_position, object_type, num_rows, avg_row_len/*, last_analyzed*/ FROM oceanbase.DBA_TAB_STATISTICS WHERE table_name='I_PARTITIONED_LISTHASH_TAB_UNIFORM' ORDER BY subpartition_name,partition_name;
--sorted_result
SELECT table_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets/*, last_analyzed*/, sample_size, avg_col_len, histogram FROM oceanbase.DBA_TAB_COL_STATISTICS WHERE table_name='I_PARTITIONED_LISTHASH_TAB_UNIFORM' ORDER BY column_name;
--sorted_result
SELECT table_name, partition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_PART_COL_STATISTICS WHERE table_name='I_PARTITIONED_LISTHASH_TAB_UNIFORM' ORDER BY partition_name,column_name;
--sorted_result
SELECT table_name, subpartition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_SUBPART_COL_STATISTICS WHERE table_name='I_PARTITIONED_LISTHASH_TAB_UNIFORM' ORDER BY subpartition_name,column_name;
--sorted_result
SELECT table_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_TAB_HISTOGRAMS WHERE table_name='I_PARTITIONED_LISTHASH_TAB_UNIFORM' ORDER BY column_name,endpoint_number;
--sorted_result
SELECT table_name, partition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_PART_HISTOGRAMS WHERE table_name='I_PARTITIONED_LISTHASH_TAB_UNIFORM' ORDER BY partition_name,column_name,endpoint_number;
--sorted_result
SELECT table_name, subpartition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_SUBPART_HISTOGRAMS WHERE table_name='I_PARTITIONED_LISTHASH_TAB_UNIFORM' ORDER BY subpartition_name,column_name,endpoint_number;


--echo  ###I_PARTITIONED_LISTHASH_TAB_GAUSSIAN###
--sorted_result
SELECT table_name, partition_name, partition_position, subpartition_name, subpartition_position, object_type, num_rows, avg_row_len/*, last_analyzed*/ FROM oceanbase.DBA_TAB_STATISTICS WHERE table_name='I_PARTITIONED_LISTHASH_TAB_GAUSSIAN' ORDER BY subpartition_name,partition_name;
--sorted_result
SELECT table_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets/*, last_analyzed*/, sample_size, avg_col_len, histogram FROM oceanbase.DBA_TAB_COL_STATISTICS WHERE table_name='I_PARTITIONED_LISTHASH_TAB_GAUSSIAN' ORDER BY column_name;
--sorted_result
SELECT table_name, partition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_PART_COL_STATISTICS WHERE table_name='I_PARTITIONED_LISTHASH_TAB_GAUSSIAN' ORDER BY partition_name,column_name;
--sorted_result
SELECT table_name, subpartition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_SUBPART_COL_STATISTICS WHERE table_name='I_PARTITIONED_LISTHASH_TAB_GAUSSIAN' ORDER BY subpartition_name,column_name;
--sorted_result
SELECT table_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_TAB_HISTOGRAMS WHERE table_name='I_PARTITIONED_LISTHASH_TAB_GAUSSIAN' ORDER BY column_name,endpoint_number;
--sorted_result
SELECT table_name, partition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_PART_HISTOGRAMS WHERE table_name='I_PARTITIONED_LISTHASH_TAB_GAUSSIAN' ORDER BY partition_name,column_name,endpoint_number;
--sorted_result
SELECT table_name, subpartition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_SUBPART_HISTOGRAMS WHERE table_name='I_PARTITIONED_LISTHASH_TAB_GAUSSIAN' ORDER BY subpartition_name,column_name,endpoint_number;


--echo  ###I_COMPOSITE_PARTITIONED_TAB###
--sorted_result
SELECT table_name, partition_name, partition_position, subpartition_name, subpartition_position, object_type, num_rows, avg_row_len/*, last_analyzed*/ FROM oceanbase.DBA_TAB_STATISTICS WHERE table_name='I_COMPOSITE_PARTITIONED_TAB' ORDER BY subpartition_name,partition_name;
--sorted_result
SELECT table_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets/*, last_analyzed*/, sample_size, avg_col_len, histogram FROM oceanbase.DBA_TAB_COL_STATISTICS WHERE table_name='I_COMPOSITE_PARTITIONED_TAB' ORDER BY column_name;
--sorted_result
SELECT table_name, partition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_PART_COL_STATISTICS WHERE table_name='I_COMPOSITE_PARTITIONED_TAB' ORDER BY partition_name,column_name;
--sorted_result
SELECT table_name, subpartition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_SUBPART_COL_STATISTICS WHERE table_name='I_COMPOSITE_PARTITIONED_TAB' ORDER BY subpartition_name,column_name;
--sorted_result
SELECT table_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_TAB_HISTOGRAMS WHERE table_name='I_COMPOSITE_PARTITIONED_TAB' ORDER BY column_name,endpoint_number;
--sorted_result
SELECT table_name, partition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_PART_HISTOGRAMS WHERE table_name='I_COMPOSITE_PARTITIONED_TAB' ORDER BY partition_name,column_name,endpoint_number;
--sorted_result
SELECT table_name, subpartition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_SUBPART_HISTOGRAMS WHERE table_name='I_COMPOSITE_PARTITIONED_TAB' ORDER BY subpartition_name,column_name,endpoint_number;


--echo  ###I_TMP_COMPOSITE_PARTITIONED_TAB###
--sorted_result
SELECT table_name, partition_name, partition_position, subpartition_name, subpartition_position, object_type, num_rows, avg_row_len/*, last_analyzed*/ FROM oceanbase.DBA_TAB_STATISTICS WHERE table_name='I_TMP_COMPOSITE_PARTITIONED_TAB' ORDER BY subpartition_name,partition_name;
--sorted_result
SELECT table_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets/*, last_analyzed*/, sample_size, avg_col_len, histogram FROM oceanbase.DBA_TAB_COL_STATISTICS WHERE table_name='I_TMP_COMPOSITE_PARTITIONED_TAB' ORDER BY column_name;
--sorted_result
SELECT table_name, partition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_PART_COL_STATISTICS WHERE table_name='I_TMP_COMPOSITE_PARTITIONED_TAB' ORDER BY partition_name,column_name;
--sorted_result
SELECT table_name, subpartition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_SUBPART_COL_STATISTICS WHERE table_name='I_TMP_COMPOSITE_PARTITIONED_TAB' ORDER BY subpartition_name,column_name;
--sorted_result
SELECT table_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_TAB_HISTOGRAMS WHERE table_name='I_TMP_COMPOSITE_PARTITIONED_TAB' ORDER BY column_name,endpoint_number;
--sorted_result
SELECT table_name, partition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_PART_HISTOGRAMS WHERE table_name='I_TMP_COMPOSITE_PARTITIONED_TAB' ORDER BY partition_name,column_name,endpoint_number;
--sorted_result
SELECT table_name, subpartition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_SUBPART_HISTOGRAMS WHERE table_name='I_TMP_COMPOSITE_PARTITIONED_TAB' ORDER BY subpartition_name,column_name,endpoint_number;


--echo  ###I_PARTITIONED_TAB###
--sorted_result
SELECT table_name, partition_name, partition_position, subpartition_name, subpartition_position, object_type, num_rows, avg_row_len/*, last_analyzed*/ FROM oceanbase.DBA_TAB_STATISTICS WHERE table_name='I_PARTITIONED_TAB' ORDER BY subpartition_name,partition_name;
--sorted_result
SELECT table_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets/*, last_analyzed*/, sample_size, avg_col_len, histogram FROM oceanbase.DBA_TAB_COL_STATISTICS WHERE table_name='I_PARTITIONED_TAB' ORDER BY column_name;
--sorted_result
SELECT table_name, partition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_PART_COL_STATISTICS WHERE table_name='I_PARTITIONED_TAB' ORDER BY partition_name,column_name;
--sorted_result
SELECT table_name, subpartition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_SUBPART_COL_STATISTICS WHERE table_name='I_PARTITIONED_TAB' ORDER BY subpartition_name,column_name;
--sorted_result
SELECT table_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_TAB_HISTOGRAMS WHERE table_name='I_PARTITIONED_TAB' ORDER BY column_name,endpoint_number;
--sorted_result
SELECT table_name, partition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_PART_HISTOGRAMS WHERE table_name='I_PARTITIONED_TAB' ORDER BY partition_name,column_name,endpoint_number;
--sorted_result
SELECT table_name, subpartition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_SUBPART_HISTOGRAMS WHERE table_name='I_PARTITIONED_TAB' ORDER BY subpartition_name,column_name,endpoint_number;


--echo  ###I_GENERAL_TAB###
--sorted_result
SELECT table_name, partition_name, partition_position, subpartition_name, subpartition_position, object_type, num_rows, avg_row_len/*, last_analyzed*/ FROM oceanbase.DBA_TAB_STATISTICS WHERE table_name='I_GENERAL_TAB' ORDER BY subpartition_name,partition_name;
--sorted_result
SELECT table_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets/*, last_analyzed*/, sample_size, avg_col_len, histogram FROM oceanbase.DBA_TAB_COL_STATISTICS WHERE table_name='I_GENERAL_TAB' ORDER BY column_name;
--sorted_result
SELECT table_name, partition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_PART_COL_STATISTICS WHERE table_name='I_GENERAL_TAB' ORDER BY partition_name,column_name;
--sorted_result
SELECT table_name, subpartition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_SUBPART_COL_STATISTICS WHERE table_name='I_GENERAL_TAB' ORDER BY subpartition_name,column_name;
--sorted_result
SELECT table_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_TAB_HISTOGRAMS WHERE table_name='I_GENERAL_TAB' ORDER BY column_name,endpoint_number;
--sorted_result
SELECT table_name, partition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_PART_HISTOGRAMS WHERE table_name='I_GENERAL_TAB' ORDER BY partition_name,column_name,endpoint_number;
--sorted_result
SELECT table_name, subpartition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_SUBPART_HISTOGRAMS WHERE table_name='I_GENERAL_TAB' ORDER BY subpartition_name,column_name,endpoint_number;


--echo  ###GENERAL_TAB_FRE###
--sorted_result
SELECT table_name, partition_name, partition_position, subpartition_name, subpartition_position, object_type, num_rows, avg_row_len/*, last_analyzed*/ FROM oceanbase.DBA_TAB_STATISTICS WHERE table_name='GENERAL_TAB_FRE' ORDER BY subpartition_name,partition_name;
--sorted_result
SELECT table_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets/*, last_analyzed*/, sample_size, avg_col_len, histogram FROM oceanbase.DBA_TAB_COL_STATISTICS WHERE table_name='GENERAL_TAB_FRE' ORDER BY column_name;
--sorted_result
SELECT table_name, partition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_PART_COL_STATISTICS WHERE table_name='GENERAL_TAB_FRE' ORDER BY partition_name,column_name;
--sorted_result
SELECT table_name, subpartition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_SUBPART_COL_STATISTICS WHERE table_name='GENERAL_TAB_FRE' ORDER BY subpartition_name,column_name;
--sorted_result
SELECT table_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_TAB_HISTOGRAMS WHERE table_name='GENERAL_TAB_FRE' ORDER BY column_name,endpoint_number;
--sorted_result
SELECT table_name, partition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_PART_HISTOGRAMS WHERE table_name='GENERAL_TAB_FRE' ORDER BY partition_name,column_name,endpoint_number;
--sorted_result
SELECT table_name, subpartition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_SUBPART_HISTOGRAMS WHERE table_name='GENERAL_TAB_FRE' ORDER BY subpartition_name,column_name,endpoint_number;


--echo  ###GENERAL_TAB_TOPK###
--sorted_result
SELECT table_name, partition_name, partition_position, subpartition_name, subpartition_position, object_type, num_rows, avg_row_len/*, last_analyzed*/ FROM oceanbase.DBA_TAB_STATISTICS WHERE table_name='GENERAL_TAB_TOPK' ORDER BY subpartition_name,partition_name;
--sorted_result
SELECT table_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets/*, last_analyzed*/, sample_size, avg_col_len, histogram FROM oceanbase.DBA_TAB_COL_STATISTICS WHERE table_name='GENERAL_TAB_TOPK' ORDER BY column_name;
--sorted_result
SELECT table_name, partition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_PART_COL_STATISTICS WHERE table_name='GENERAL_TAB_TOPK' ORDER BY partition_name,column_name;
--sorted_result
SELECT table_name, subpartition_name, column_name, num_distinct, low_value, high_value, density, num_nulls, num_buckets, sample_size/*, last_analyzed*/, avg_col_len, histogram FROM oceanbase.DBA_SUBPART_COL_STATISTICS WHERE table_name='GENERAL_TAB_TOPK' ORDER BY subpartition_name,column_name;
--sorted_result
SELECT table_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_TAB_HISTOGRAMS WHERE table_name='GENERAL_TAB_TOPK' ORDER BY column_name,endpoint_number;
--sorted_result
SELECT table_name, partition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_PART_HISTOGRAMS WHERE table_name='GENERAL_TAB_TOPK' ORDER BY partition_name,column_name,endpoint_number;
--sorted_result
SELECT table_name, subpartition_name, column_name, endpoint_number, endpoint_actual_value, endpoint_repeat_count FROM oceanbase.DBA_SUBPART_HISTOGRAMS WHERE table_name='GENERAL_TAB_TOPK' ORDER BY subpartition_name,column_name,endpoint_number;