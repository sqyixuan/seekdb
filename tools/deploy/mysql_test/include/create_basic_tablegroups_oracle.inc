--disable_warnings
# owner : zhaoziqian.zzq
# owner group: RS
# input : $post_fix
# desription :
# 1. hash like - hash like/ range like - range like / list like - list like are not supported.
# 2. key、range、list are invalid for tablegroup in oracle.

# case 1: non-partitioned
eval create tablegroup non_partitioned_tg_$postfix;

# case 2: partitioned

## key
#eval create tablegroup key_tg_$postfix partition by key 2 partitions 2;

# hash
eval create tablegroup hash_tg_$postfix partition by hash partitions 2;

## range
#eval create tablegroup range_tg_$postfix partition by range (partition p0 values less than (100), partition p1 values less than (200));

# range columns
eval create tablegroup range_columns_tg_$postfix partition by range columns 2 (partition p0 values less than (100, 100), partition p1 values less than (200, 200));

## list
#eval create tablegroup list_$postfix partition by list (partition p0 values (0, 1), partition p1 values (2, 3));

# list columns
eval create tablegroup list_columns_$postfix partition by list columns 2 (partition p0 values (('00', 0), ('01', 1)), partition p1 values (('02', 2), ('03', 3)));

# case 3: composite-partitioned

## case 3.1: key - *
## key - key
#eval create tablegroup key_key_tg_$postfix partition by key 2 subpartition by key 2 subpartitions 2 partitions 2;
#
## key - hash
#eval create tablegroup key_hash_tg_$postfix partition by key 2 subpartition by hash subpartitions 2 partitions 2;
#
## key - range
#eval create tablegroup key_range_tg_$postfix partition by key 2 subpartition by range subpartition template (subpartition p0 values less than (100), subpartition p1 values less than (200)) partitions 2;
#
## key - range columns
#eval create tablegroup key_range_columns_tg_$postfix partition by key 2 subpartition by range columns 2 subpartition template (subpartition p0 values less than (100, 100), subpartition p1 values less than (200, 200)) partitions 2;
#
## key - list
#eval create tablegroup key_list_tg_$postfix partition by key 2 subpartition by list subpartition template (subpartition p0 values (0, 1), subpartition p1 values (2, 3)) partitions 2;
#
## key - list columns
#eval create tablegroup key_list_columns_tg_$postfix partition by key 2 subpartition by list columns 2 subpartition template (subpartition p0 values (('00', 0), ('01', 1)), subpartition p1 values (('02', 2), ('03', 3))) partitions 2;

# case 3.2: hash - *
## hash - key
#eval create tablegroup hash_key_tg_$postfix partition by hash subpartition by key 2 subpartitions 2 partitions 2;
#
## hash - hash
#eval create tablegroup hash_hash_tg_$postfix partition by hash subpartition by hash subpartitions 2 partitions 2;
#
## hash - range
#eval create tablegroup hash_range_tg_$postfix partition by hash subpartition by range subpartition template (subpartition p0 values less than (100), subpartition p1 values less than (200)) partitions 2;

# hash - range columns
eval create tablegroup hash_range_columns_tg_$postfix partition by hash subpartition by range columns 2 subpartition template (subpartition p0 values less than (100, 100), subpartition p1 values less than (200, 200)) partitions 2;

## hash - list
#eval create tablegroup hash_list_tg_$postfix partition by hash subpartition by list subpartition template (subpartition p0 values (0, 1), subpartition p1 values (2, 3)) partitions 2;

# hash - list columns
eval create tablegroup hash_list_columns_tg_$postfix partition by hash subpartition by list columns 2 subpartition template (subpartition p0 values (('00', 0), ('01', 1)), subpartition p1 values (('02', 2), ('03', 3))) partitions 2;

## case 3.3: range - *
## range - key
#eval create tablegroup range_key_tg_$postfix partition by range subpartition by key 2 subpartitions 2 (partition p0 values less than (100), partition p1 values less than (200));
#
## range - hash
#eval create tablegroup range_hash_tg_$postfix partition by range subpartition by hash subpartitions 2 (partition p0 values less than (100), partition p1 values less than (200));
#
## range - range
#eval create tablegroup range_range_tg_$postfix partition by range subpartition by range subpartition template (subpartition p0 values less than (100), subpartition p1 values less than (200)) (partition p0 values less than (100), partition p1 values less than (200));
#
## range - range columns
#eval create tablegroup range_range_columns_tg_$postfix partition by range subpartition by range columns 2 subpartition template (subpartition p0 values less than (100, 100), subpartition p1 values less than (200, 200)) (partition p0 values less than (100), partition p1 values less than (200));
#
## range - list
#eval create tablegroup range_list_tg_$postfix partition by range subpartition by list subpartition template (subpartition p0 values (0, 1), subpartition p1 values (2, 3)) (partition p0 values less than (100), partition p1 values less than (200));
#
## range - list columns
#eval create tablegroup range_list_columns_tg_$postfix partition by range subpartition by list columns 2 subpartition template (subpartition p0 values (('00', 0), ('01', 1)), subpartition p1 values (('02', 2), ('03', 3))) (partition p0 values less than (100), partition p1 values less than (200));

# case 3.4: range columns - *
## range columns - key
#eval create tablegroup range_columns_key_tg_$postfix partition by range columns 2 subpartition by key 2 subpartitions 2 (partition p0 values less than (100, 100), partition p1 values less than (200, 200));

# range columns - hash
eval create tablegroup range_columns_hash_tg_$postfix partition by range columns 2 subpartition by hash subpartitions 2 (partition p0 values less than (100, 100), partition p1 values less than (200, 200));

## range columns - range
#eval create tablegroup range_columns_range_tg_$postfix partition by range columns 2 subpartition by range columns 2 subpartition template (subpartition p0 values less than (100), subpartition p1 values less than (200)) (partition p0 values less than (100, 100), partition p1 values less than (200, 200));
#
## range columns - range columns
#eval create tablegroup range_columns_range_columns_tg_$postfix partition by range columns 2 subpartition by range columns 2 subpartition template (subpartition p0 values less than (100, 100), subpartition p1 values less than (200, 200)) (partition p0 values less than (100, 100), partition p1 values less than (200, 200));
#
## range columns - list
#eval create tablegroup range_columns_list_tg_$postfix partition by range columns 2 subpartition by list subpartition template (subpartition p0 values (0, 1), subpartition p1 values (2, 3)) (partition p0 values less than (100, 100), partition p1 values less than (200, 200));
# range columns - list columns
eval create tablegroup range_columns_list_columns_tg_$postfix partition by range columns 2 subpartition by list columns 2 subpartition template (subpartition p0 values (('00', 0), ('01', 1)), subpartition p1 values (('02', 2), ('03', 3))) (partition p0 values less than (100, 100), partition p1 values less than (200, 200));

## case 3.5: list - *
## list - key
#eval create tablegroup list_key_tg_$postfix partition by list subpartition by key 2 subpartitions 2 (partition p0 values (0, 1), partition p1 values (2, 3));
#
## list - hash
#eval create tablegroup list_hash_tg_$postfix partition by list subpartition by hash subpartitions 2 (partition p0 values (0, 1), partition p1 values (2, 3));
#
## list - range
#eval create tablegroup list_range_tg_$postfix partition by list subpartition by list subpartition template (subpartition p0 values less than (100), subpartition p1 values less than (200)) (partition p0 values (0, 1), partition p1 values (2, 3));
#
## list - range columns
#eval create tablegroup list_range_columns_tg_$postfix partition by list subpartition by range columns 2 subpartition template (subpartition p0 values less than (100, 100), subpartition p1 values less than (200, 200)) (partition p0 values (0, 1), partition p1 values (2, 3));
#
## list - list
#eval create tablegroup list_list_tg_$postfix partition by list subpartition by list subpartition template (subpartition p0 values (0, 1), subpartition p1 values (2, 3)) (partition p0 values (0, 1), partition p1 values (2, 3));
#
## list - list columns
#eval create tablegroup list_list_columns_tg_$postfix partition by list subpartition by list columns 2 subpartition template (subpartition p0 values (('00', 0), ('01', 1)), subpartition p1 values (('02', 2), ('03', 3))) (partition p0 values (0, 1), partition p1 values (2, 3));

# case 3.6: list columns - *
## list columns - key
#eval create tablegroup list_columns_key_tg_$postfix partition by list columns 2 subpartition by key 2 subpartitions 2 (partition p0 values (('00', 0), ('01', 1)), partition p1 values (('02', 2), ('03', 3)));

# list columns - hash
eval create tablegroup list_columns_hash_tg_$postfix partition by list columns 2 subpartition by hash subpartitions 2 (partition p0 values (('00', 0), ('01', 1)), partition p1 values (('02', 2), ('03', 3)));

## list columns - range
#eval create tablegroup list_columns_range_tg_$postfix partition by list columns 2 subpartition by list columns 2 subpartition template (subpartition p0 values less than (100), subpartition p1 values less than (200)) (partition p0 values (('00', 0), ('01', 1)), partition p1 values (('02', 2), ('03', 3)));

# list columns - range columns
eval create tablegroup list_columns_range_columns_tg_$postfix partition by list columns 2 subpartition by range columns 2 subpartition template (subpartition p0 values less than (100, 100), subpartition p1 values less than (200, 200)) (partition p0 values (('00', 0), ('01', 1)), partition p1 values (('02', 2), ('03', 3)));
--enable_warnings

## list columns - list
#eval create tablegroup list_columns_list_tg_$postfix partition by list columns 2 subpartition by list columns 2 subpartition template (subpartition p0 values (0, 1), subpartition p1 values (2, 3)) (partition p0 values (('00', 0), ('01', 1)), partition p1 values (('02', 2), ('03', 3)));

## list columns - list columns
#eval create tablegroup list_columns_list_columns_tg_$postfix partition by list columns 2 subpartition by list columns 2 subpartition template (subpartition p0 values (('00', 0), ('01', 1)), subpartition p1 values (('02', 2), ('03', 3))) (partition p0 values (('00', 0), ('01', 1)), partition p1 values (('02', 2), ('03', 3)));