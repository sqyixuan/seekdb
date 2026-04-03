# include/wait_fork_database.inc
#
# SUMMARY
#
#    Waits for all fork-table DDL tasks (DDL_FORK_TABLE=10009) to complete.
#    This is used after FORK DATABASE command which creates multiple fork table tasks.
#
# USAGE
#
#    --source mysql_test/test_suite/fork_database/include/wait_fork_database.inc
#
#   OR
#
#    let $wait_timeout= 120; # Override default 60 seconds with 120.
#    --source mysql_test/test_suite/fork_database/include/wait_fork_database.inc
#
# PARAMETERS
#
#    $wait_timeout - Timeout in seconds (default: 60; consumed then reset to 0 by this include)
#

--disable_query_log
--disable_result_log

# Set default timeout to 60 seconds if not specified (longer than fork_table since multiple tables)
let $resolved_wait_timeout= 60;
if ($wait_timeout)
{
  let $resolved_wait_timeout= $wait_timeout;
}
# Reset $wait_timeout ASAP so that its value won't leak into subsequent calls/cases.
let $wait_timeout= 0;

let $wait_counter= `SELECT $resolved_wait_timeout * 10`;

# Connect to system connection for querying system tables
connect (obsys_wait_fork_db,$OBMYSQL_MS0,root@sys,,oceanbase,$OBMYSQL_PORT);
connection obsys_wait_fork_db;

# Wait for all fork table tasks to complete (count(*) == 0)
let $wait_satisfied= 0;
let $fork_task_cnt= 1;
while ($wait_counter > 0)
{
  # Count all fork table tasks (ddl_type = 10009)
  let $fork_task_cnt= `select count(*) from oceanbase.__all_virtual_ddl_task_status where ddl_type = 10009`;

  if ($fork_task_cnt == 0)
  {
    let $wait_satisfied= 1;
    let $wait_counter= 0;
  }

  if ($wait_counter > 0)
  {
    real_sleep 0.1;
    dec $wait_counter;
  }
}

--enable_query_log
--enable_result_log

if ($wait_satisfied == 0)
{
  --echo # Warning: wait_fork_database.inc timed out
  --echo # Current fork table ddl tasks:
  select ddl_type, status, object_id, target_object_id, task_id, gmt_create, gmt_modified 
    from oceanbase.__all_virtual_ddl_task_status 
    where ddl_type = 10009 
    order by gmt_create desc, task_id desc;
  disconnect obsys_wait_fork_db;
  connection default;
  die Timeout waiting for fork database (all fork table tasks) to complete;
}

disconnect obsys_wait_fork_db;
connection default;

# Reset variables
let $fork_task_cnt=;
let $wait_satisfied=;
let $resolved_wait_timeout=;
