require("utils")
require("sql_utils")
local inspect=require("inspect")
local time=require("time")

function check_empty(conn, sql)
  local rows, ok = conn:query(sql);
  assert(ok)
  local empty = not rows:next()
  rows:close()
  return empty
end

function remove_unstable(hold, holds)
  local hold_unstable = 0
  local hold_unstables = {}
  local unstable_mod = G.input.unstable_mod
  for i=1,#unstable_mod,1 do
    local k,v = unstable_mod[i], holds[unstable_mod[i]]
    if v ~= nil then
      hold_unstable = hold_unstable + v
      hold_unstables[k] = v
    end
    holds[k] = nil
  end
  return hold - hold_unstable, hold_unstable, hold_unstables
end

function main()
  local _, ok = G.sys_conn:exec("alter system set _force_explict_500_malloc = true");
  assert(ok)

  -- disable minor merge
  local _, ok = G.sys_conn:exec("alter system set minor_compact_trigger = 16 tenant = sys");
  assert(ok)
  local _, ok = G.sys_conn:exec("alter system set minor_compact_trigger = 16 tenant = all_meta");
  assert(ok)
  local _, ok = G.sys_conn:exec("alter system set minor_compact_trigger = 16 tenant = all_user");
  assert(ok)

  if os.execute(string.format('cd ./sysbench/src/lua; export -n LUA_PATH; ihost=%s iport=%s itenant=%s sh tmysql_sysbench.sh', G.addr.host, G.addr.port, 'sys')) ~= 0 then
    print("run sysbench failed!")
    return 1
  end

  local _, ok = G.sys_conn:exec("alter system minor freeze tenant = sys");
  assert(ok)
  local _, ok = G.sys_conn:exec("alter system minor freeze tenant = all_meta");
  assert(ok)
  local _, ok = G.sys_conn:exec("alter system minor freeze tenant = all_user");
  assert(ok)

  --wait compaction done
  while true do
    local goon = not check_empty(G.sys_conn, "select 1 from oceanbase.__all_virtual_memstore_info where is_active='NO'") or
        not check_empty(G.sys_conn, "select 1 from oceanbase.__all_virtual_tx_data_table where state in ('FROZEN', 'FREEZING')")
    if not goon then break end
    time.sleep(5)
    print('wait compaction done')
  end

  refresh_mem_stat(G.sys_conn)
  local hold, holds = get_mem_hold(G.sys_conn, G.addr)
  local hold, hold_unstable, holds_unstable = remove_unstable(hold, holds)
  --hold
  print("hold:")
  compare_holds(holds, {})
  --unstable
  print("hold_unstable:")
  compare_holds(holds_unstable, {})
  --unmanaged_memory
  local unmanaged_memory_size = query_scalar(G.sys_conn, 'select value from __all_virtual_sysstat where tenant_id=1 and name="unmanaged memory size"');
  print(string.format("unmanaged_memory_size: %d", unmanaged_memory_size))

  G.create_metrics({
      hold=hold,
      hold_unstable=hold_unstable,
      unmanaged_memory_size=unmanaged_memory_size})
end
