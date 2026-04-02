def print_head(file_name):
  head = ''
  if file_name == 'basic':
    head = """
#owner: xiaoyi.xy
#owner group: sql1
##
## Test Name: plan_cache_basic
##
## Scope: Test basic function of plan cache match
##
## INFO:  if you want to add case; you should read ../README.md;
#         don't use sys tenant, because when parallel run on obfarm, the hit_count can't be expected;
#         so we use new tenant, new plan cache will be  used for this case only;
##
##
## Date: 2015-9-16
##
    """
  elif file_name == 'check_right':
    head = """
#owner: xiaoyi.xy
#owner group: sql1
##
## Test Name: plan_cache_check_right
##
## Scope: Test plan cache whether match the right plan
##
## INFO:  if you want to add case; you should read ../README.md;
#         don't use sys tenant, because when parallel run on obfarm, the hit_count can't be expected;
#         so we use new tenant, new plan cache will be  used for this case only;
##
##
## Date: 2015-9-22
##
    """
  fout.write(head);
  head_2 = """

--disable_info
--disable_metadata
--disable_abort_on_error

--disable_query_log
--disable_result_log
--error 0,5157
drop tenant xy_tenant_1 force;

eval create resource unit if not exists xy_box_1 max_cpu 3, memory_size '1G';
let $zone_name=query_get_value(select zone from oceanbase.__all_zone where zone != '' limit 1, zone, 1);

let $obs_num = query_get_value(select count(1) as cnt from oceanbase.__all_server group by zone asc limit 1,cnt, 1);
eval create resource pool if not exists xy_pool_1 unit = 'xy_box_1', unit_num = $obs_num;
eval create tenant if not exists xy_tenant_1 primary_zone='$zone_name', resource_pool_list('xy_pool_1') set ob_tcp_invited_nodes='%';

alter system flush plan cache;
connect (conn1,$OBMYSQL_MS0,root@xy_tenant_1,,*NO-ONE*,$OBMYSQL_PORT);
select effective_tenant();

set autocommit = 0;
set ob_read_consistency = 'weak';

# --echo
--disable_warnings
drop database if exists xy_db;
--enable_warnings
create database xy_db;
use xy_db;

"""
  fout.write(head_2);

def print_end():
  end = """
--disable_query_log
--disable_result_log

commit;
set autocommit = 1;
--disable_warnings
drop database if exists xy_db;
--enable_warnings

# login using admin acount
connect (conn_admin,$OBMYSQL_MS0,admin@sys,admin,*NO-ONE*,$OBMYSQL_PORT);

drop tenant xy_tenant_1 force;
drop resource pool xy_pool_1;
drop resource unit xy_box_1;

"""
  fout.write(end);

def print_origin(line):
  fout.write(line);

def print_case(line1, line2):
  fout.write("--echo\n")
  fout.write("--echo **********************" + line1.rstrip(';\n') + "****************\n");
  fout.write("--echo **********************" + line2.rstrip(';\n') + "****************\n");
  fout.write("--disable_query_log\n");
  fout.write("--disable_result_log\n");
  fout.write(line1);
  fout.write(line2);
  fout.write("--enable_result_log\n");
  fout.write("select hit_count from oceanbase.V$OB_PLAN_CACHE_PLAN_STAT where statement = \"" + line1.rstrip(';\n') + "\";\n\n");

def generate_hit_same_param_test(input_file, out_put_file):
  global fout
  fin = open(input_file, 'r');
  fout = open(out_put_file, 'w');
  print_head('basic');
  table_init = """
--disable_query_log
--disable_result_log
source mysql_test/test_suite/t/plan_cache_table_init.sql;
  """
  fout.write(table_init);
  lines = fin.readlines();
  fin.close();
  is_case = 1;
  for line in lines:
    tmp_line = line.lstrip();
    if tmp_line.startswith('#origin'):
      is_case = 0;
      continue;
    elif tmp_line.startswith('#end'):
      is_case = 1;
      continue;
    if tmp_line.startswith('#') or len(tmp_line) == 0 or is_case == 0:
      print_origin(line);
    else:
      print_case(line, line);
  print_end()
  fout.close()

def generate_hit_diff_param_test(input_file_1, input_file_2, out_put_file):
  global fout
  fin_1 = open(input_file_1, 'r');
  fin_2 = open(input_file_2, 'r');
  fout = open(out_put_file, 'w');
  print_head('basic');
  table_init = """
--disable_query_log
--disable_result_log
source mysql_test/test_suite/plan_cache/t/plan_cache_table_init.sql;
  """
  fout.write(table_init);
  lines_1 = fin_1.readlines();
  lines_2 = fin_2.readlines();
  fin_1.close();
  fin_2.close();
  is_case = 1;
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
        print_case(lines_1[i], lines_2[i]);
    print_end()
  else:
    print 'count of two file is not same'
  fout.close()

def generate_check_plan_cache_right_test(out_put_file):
  global fout
  fout = open(out_put_file, 'w');
  print_head('check_right');
  content = """
--disable_query_log
--disable_result_log
SET NAMES 'utf8mb4' COLLATE 'utf8mb4_general_ci';
set @@ob_enable_plan_cache = 0;
--sleep 1
source mysql_test/test_suite/plan_cache/t/plan_cache_table_init.sql;
source mysql_test/test_suite/plan_cache/t/plan_cache.sql;
--enable_query_log
--enable_result_log
source mysql_test/test_suite/plan_cache/t/plan_cache_change_param.sql;
"""
  fout.write(content)
  print_end()
  fout.close()


if __name__ == "__main__":
  print "generate_test ..."

  generate_hit_diff_param_test("./plan_cache.sql", "./plan_cache_change_param.sql", "plan_cache_basic.test");
  generate_check_plan_cache_right_test('plan_cache_check_right.test')

  print "done!"
