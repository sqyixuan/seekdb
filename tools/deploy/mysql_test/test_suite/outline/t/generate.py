#!/usr/bin/env python
OUTLINE_ID = 1100611139404777;
OUTLINE_NAME_ID = 0;
def print_head(file_name):
  head = ''
  if file_name == 'basic':
    head = """#owner: xiaoyi.xy
#owner group: SQL4
##
## Test Name: outline_basic
##
## Scope: Test basic function of outline match
##
## INFO:  if you want to add case; you should read ../README.md;
#         don't use sys tenant, because when parallel run on obfarm, the hit_count can't be expected;
#         so we use new tenant, new plan cache will be  used for this case only;
##
##
## Date: 2016-6-12
##"""
  elif file_name == 'check_right':
    head = """#owner: xiaoyi.xy
#owner group: SQL4
##
## Test Name: outline_check_right
##
## Scope: Test  whether match the right plan with outline
##
## INFO:  if you want to add case; you should read ../README.md;
#         don't use sys tenant, because when parallel run on obfarm, the hit_count can't be expected;
#         so we use new tenant, new plan cache will be  used for this case only;
##
##
## Date: 2016-6-12
##"""
  elif file_name=="no_hint_check_right":
    head = """#owner: xiaoyi.xy
#owner group: SQL4
##
## Test Name: outline_no_hint_check_right
##
## Scope: Test  whether match the right plan with outline
##
## INFO:  if you want to add case; you should read ../README.md;
#         don't use sys tenant, because when parallel run on obfarm, the hit_count can't be expected;
#         so we use new tenant, new plan cache will be  used for this case only;
##
##
## Date: 2016-6-12
##"""
  elif file_name=="no_hint_check_hit":
    head = """#owner: xiaoyi.xy
#owner group: SQL4
##
## Test Name: outline_no_hint_check_hit
##
## Scope: Test  whether match the right plan with outline
##
## INFO:  if you want to add case; you should read ../README.md;
#         don't use sys tenant, because when parallel run on obfarm, the hit_count can't be expected;
#         so we use new tenant, new plan cache will be  used for this case only;
##
##
## Date: 2016-6-12
##"""

  fout.write(head);
  head_2 = """

--disable_info
--disable_metadata

--disable_query_log
--disable_result_log
drop tenant if exists yyy_tenant_1 force;

eval create resource unit if not exists yyy_box_1 max_cpu 3, memory_size '1G';
let $zone_name=query_get_value(select zone from oceanbase.DBA_OB_ZONES where zone != '' limit 1, zone, 1);

let $obs_num = query_get_value(select count(1) as cnt from oceanbase.DBA_OB_SERVERS group by zone asc limit 1,cnt, 1);
eval create resource pool if not exists yyy_pool_1 unit = 'yyy_box_1', unit_num = $obs_num;
eval create tenant if not exists yyy_tenant_1 primary_zone='$zone_name', resource_pool_list('yyy_pool_1') set ob_tcp_invited_nodes='%';
set ob_trx_timeout=1000000000;
alter system flush plan cache;
connect (conn_admin,$OBMYSQL_MS0,admin@sys,admin,*NO-ONE*,$OBMYSQL_PORT);
connect (conn1,$OBMYSQL_MS0,root@yyy_tenant_1,,*NO-ONE*,$OBMYSQL_PORT);
set ob_trx_timeout=1000000000;
select effective_tenant();

set autocommit = 0;
set ob_read_consistency = 'weak';

# --echo
--disable_warnings
drop database if exists yyy_db;
--enable_warnings
create database yyy_db;
use yyy_db;

"""
  fout.write(head_2);

def print_end():
  end = """
--disable_query_log
--disable_result_log

commit;
set autocommit = 1;
--disable_warnings
drop database if exists yyy_db;
--enable_warnings

# login using admin acount
connection conn_admin;
drop tenant yyy_tenant_1 force;
drop resource pool yyy_pool_1;
drop resource unit yyy_box_1;

"""
  fout.write(end);

def print_origin(line):
  fout.write(line);

def print_case(line1, line2, line3):
  global OUTLINE_ID
  fout.write("--echo\n")
  fout.write("--echo **********************" + line1.rstrip(';\n') + "****************\n");
  fout.write("--echo ********************** Outline:" + line2.rstrip(';\n') + "****************\n");
  fout.write("--echo **********************" + line3.rstrip(';\n') + "****************\n");
  fout.write(line1);
  fout.write(line2);
  fout.write("sleep 2;\n");
  outline_str = str(OUTLINE_ID);
  fout.write("connection conn_admin;\n");
  fout.write("select tenant_id, name, hex(substr(signature, 1, 1)), substr(signature, 2, conv(hex(substr(signature, 1, 1)), 16,10)), outline_content, sql_text, owner from oceanbase.__all_virtual_outline where outline_id = " + outline_str + ";\n\n");
  fout.write("connection conn1;\n");
  OUTLINE_ID += 1;
  fout.write(line1);
  fout.write("select hit_count from oceanbase.V$OB_PLAN_CACHE_PLAN_STAT where statement = \"" + line1.rstrip(';\n') + "\";\n\n");
  fout.write(line3);
  fout.write("select hit_count from oceanbase.V$OB_PLAN_CACHE_PLAN_STAT where statement = \"" + line1.rstrip(';\n') + "\";\n\n");

def generate_hit_diff_param_test(input_file_1, outline_file, input_file_2, out_put_file):
  global fout
  fin_1 = open(input_file_1, 'r');
  fin_2 = open(outline_file, 'r');
  fin_3 = open(input_file_2, 'r');
  fout = open(out_put_file, 'w');
  print_head('basic');
  table_init = """
--disable_query_log
--disable_result_log
connection conn1;
source mysql_test/test_suite/outline/t/outline_table_init.sql;
--enable_result_log
  """
  fout.write(table_init);
  lines_1 = fin_1.readlines();
  lines_2 = fin_2.readlines();
  lines_3 = fin_3.readlines();
  fin_1.close();
  fin_2.close();
  fin_3.close();
  is_case = 1;
  count_1 = len(lines_1);
  count_2 = len(lines_2);
  count_3 = len(lines_3);
  if count_1 == count_2 and count_2 == count_3:
    for i in range(count_1):
      tmp_line = lines_1[i].lstrip();
      if tmp_line.startswith('#origin'):
        fout.write("--disable_query_log\n");
        fout.write("--disable_result_log\n");
        is_case = 0;
        continue;
      elif tmp_line.startswith('#end'):
        is_case = 1;
        fout.write("--enable_query_log\n");
        fout.write("--enable_result_log\n");
        continue;
      if tmp_line.startswith('#') or len(tmp_line) == 0 or is_case == 0:
        print_origin(lines_1[i]);
      else:
        print_case(lines_1[i], lines_2[i], lines_3[i]);
    print_end()
  else:
    print 'count of two file is not same', count_1, count_2, count_3
  fout.close()


def generate_check_outline_right_test(out_put_file):
  global fout
  fout = open(out_put_file, 'w');
  print_head('check_right');
  content = """
--disable_query_log
--disable_result_log
SET NAMES 'utf8mb4' COLLATE 'utf8mb4_general_ci';
set @@ob_enable_plan_cache = 0;
--sleep 1
source mysql_test/test_suite/outline/t/outline_table_init.sql;
--enable_query_log
--enable_result_log
source mysql_test/test_suite/outline/t/outline_change_param.sql;
--disable_query_log
--disable_result_log
set @@ob_enable_plan_cache = 1;
source mysql_test/test_suite/outline/t/outline_init.sql;
sleep 5;
--enable_query_log
--enable_result_log
source mysql_test/test_suite/outline/t/outline.sql;
source mysql_test/test_suite/outline/t/outline_change_param.sql;
"""
  fout.write(content)
  print_end()
  fout.close()

##def generate_no_hint_check_right_test(input_file_1, output_file):
##  global fout
##  global OUTLINE_NAME_ID
##  OUTLINE_NAME_ID = 0;
##  fin_1 = open(input_file_1, 'r');
##  fout = open(output_file, 'w');
##  print_head('no_hint_check_right');
##  content_part_one = """
##--disable_query_log
##--disable_result_log
##SET NAMES 'utf8mb4' COLLATE 'utf8mb4_general_ci';
##set @@ob_enable_plan_cache = 0;
##--sleep 1
##source mysql_test/test_suite/outline/t/outline_table_init.sql;
##--enable_query_log
##--enable_result_log
##source mysql_test/test_suite/outline/t/outline_simple.sql;
##set @@ob_enable_plan_cache = 1;
##"""
##  fout.write(content_part_one)
##  lines_1 = fin_1.readlines();
##  is_case =1;
##  count_1 = len(lines_1);
##  ##if count_1 == count_2 and count_2 == count_3:
##  for i in range(count_1):
##    tmp_line = lines_1[i].lstrip();
##    if tmp_line.startswith('#origin'):
##      fout.write("--disable_query_log\n");
##      fout.write("--disable_result_log\n");
##      is_case = 0;
##      continue;
##    elif tmp_line.startswith('#end'):
##      is_case = 1;
##      fout.write("--enable_query_log\n");
##      fout.write("--enable_result_log\n");
##      continue;
##
##    if tmp_line.startswith('#') or len(tmp_line) == 0 or is_case == 0:
##      print_origin(lines_1[i]);
##    else:
##        outline_name = "ol_" + str(OUTLINE_NAME_ID);
##        fout.write("create outline  " + outline_name + " on " + lines_1[i].rstrip(';\n') + ";\n\n");
##        fout.write(lines_2[i]);
##        fout.write("select hit_count from oceanbase.V$OB_PLAN_CACHE_PLAN_STAT where statement = \"" + line_1[i].rstrip(';\n') + "\";\n\n");
##        OUTLINE_NAME_ID +=1;
##  ##else:
## ##   print 'count of two file is not same', count_1, count_2, count_3
##  fout.write("select count(*) from oceanbase.__all_outline;\n\n");
##  content_part_two = """
##sleep 5;
##--enable_query_log
##--enable_result_log
##source mysql_test/test_suite/outline/t/outline_simple.sql;
##"""
##  fout.write(content_part_two)
##  print_end()
##  fout.close()

def generate_no_hint_select_check_hit_test(input_file_1, input_file_2, output_file):
  global fout
  global OUTLINE_NAME_ID
  OUTLINE_NAME_ID = 0;
  fin_1 = open(input_file_1, 'r');
  fin_2 = open(input_file_2, 'r');
  fout = open(output_file, 'w');
  print_head('no_hint_check_hit');
  content_part_one = """
--disable_result_log
SET NAMES 'utf8mb4' COLLATE 'utf8mb4_general_ci';
source mysql_test/test_suite/outline/t/outline_table_init.sql;
--disable_result_log
source mysql_test/test_suite/outline/t/""" + input_file_1 + """;
--enable_result_log """
  fout.write(content_part_one)
  lines_1 = fin_1.readlines();
  lines_2 = fin_2.readlines();
  is_case =1;
  count_1 = len(lines_1);
  count_2 = len(lines_2);
  if count_1 == count_2:
    for i in range(count_1):
      tmp_line = lines_1[i].lstrip();
      if tmp_line.startswith('#origin'):
        fout.write("--disable_query_log\n");
        fout.write("--disable_result_log\n");
        is_case = 0;
        continue;
      elif tmp_line.startswith('#end'):
        is_case = 1;
        fout.write("--enable_query_log\n");
        fout.write("--enable_result_log\n");
        continue;
      if tmp_line.startswith('#') or len(tmp_line) == 0 or is_case == 0:
        print_origin(lines_1[i]);
      else :
        fout.write("--echo\n")
        fout.write("--echo **********************" + lines_1[i].rstrip(';\n') + "****************\n");
        fout.write("--echo **********************" + lines_2[i].rstrip(';\n') + "****************\n");
        outline_name = "ol_" + str(OUTLINE_NAME_ID);
        fout.write("create outline  " + outline_name + " on " + lines_1[i].rstrip(';\n') + ";\n");
        fout.write("sleep 1;\n");
        fout.write("--disable_query_log\n");
        fout.write("--disable_result_log\n");
        fout.write(lines_1[i]);
        fout.write("--enable_result_log\n");
        fout.write("select hit_count from oceanbase.V$OB_PLAN_CACHE_PLAN_STAT where sql_text = \"" + lines_1[i].rstrip(';\n') + "\";\n");
        fout.write("--disable_query_log\n");
        fout.write("--disable_result_log\n");
        fout.write(lines_2[i]);
        fout.write("--enable_result_log\n");
        fout.write("select hit_count from oceanbase.V$OB_PLAN_CACHE_PLAN_STAT where sql_text = \"" + lines_1[i].rstrip(';\n') + "\";\n\n");
        OUTLINE_NAME_ID +=1;
  #
  else:
    print 'count of two file is not same', count_1, count_2
  fout.write("connection conn_admin;\n");
  fout.write("select count(*) from oceanbase.__all_outline;\n");
  fout.write("connection conn1;\n");

  print_end()
  fout.close()

def generate_no_hint_select_check_right_test(input_file_1, input_file_2, output_file):
  global fout
  global OUTLINE_NAME_ID
  OUTLINE_NAME_ID = 0;
  fin_1 = open(input_file_1, 'r');
  fin_2 = open(input_file_2, 'r');
  fout = open(output_file, 'w');
  print_head('no_hint_select');
  content_part_one = """ --disable_result_log
SET NAMES 'utf8mb4' COLLATE 'utf8mb4_general_ci';
set @@ob_enable_plan_cache = 0;
--sleep 1
source mysql_test/test_suite/outline/t/outline_table_init.sql;
--enable_query_log
--enable_result_log
source mysql_test/test_suite/outline/t/""" + input_file_2 + """;
set @@ob_enable_plan_cache = 1;
--disable_result_log
--disable_query_log
source mysql_test/test_suite/outline/t/""" + input_file_1+ """;
--enable_result_log
--enable_query_log
"""
  fout.write(content_part_one)
  lines_1 = fin_1.readlines();
  lines_2 = fin_2.readlines();
  is_case =1;
  count_1 = len(lines_1);
  count_2 = len(lines_2);
  if count_1 == count_2:
    for i in range(count_1):
      tmp_line = lines_1[i].lstrip();
      if tmp_line.startswith('#origin'):
        fout.write("--disable_query_log\n");
        fout.write("--disable_result_log\n");
        is_case = 0;
        continue;
      elif tmp_line.startswith('#end'):
        is_case = 1;
        fout.write("--enable_query_log\n");
        fout.write("--enable_result_log\n");
        continue;
      if tmp_line.startswith('#') or len(tmp_line) == 0 or is_case == 0:
        print_origin(lines_1[i]);
      else:
          outline_name = "ol_" + str(OUTLINE_NAME_ID);
          fout.write("create outline  " + outline_name + " on " + lines_1[i].rstrip(';\n') + ";\n\n");
          OUTLINE_NAME_ID +=1;
  else:
    print 'count of two file is not same', count_1, count_2, count_3
  fout.write("connection conn_admin;\n");
  fout.write("select count(*) from oceanbase.__all_outline;\n");
  fout.write("connection conn1;\n");
  content_part_two = """
sleep 5;
--disable_result_log
--disable_query_log
source mysql_test/test_suite/outline/t/""" + input_file_1+ """;
--enable_query_log
--enable_result_log
source mysql_test/test_suite/outline/t/""" + input_file_2+ ";"
  fout.write(content_part_two)

  print_end()
  fout.close()

if __name__ == "__main__":
  print "generate_test ..."

  generate_hit_diff_param_test("./outline.sql", "./outline_init.sql", "./outline_change_param.sql", "outline_basic.test");
  generate_check_outline_right_test('outline_check_right.test');
  generate_no_hint_select_check_hit_test("./outline_no_hint.sql", "./outline_no_hint_change_param.sql", "outline_no_hint_check_hit.test");
  generate_no_hint_select_check_right_test("./outline_no_hint.sql", "./outline_no_hint_change_param.sql", "outline_no_hint_check_right.test");

  print "done!"
