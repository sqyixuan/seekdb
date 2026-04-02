function get_mem_hold(sys_conn, addr)
    local rows, ok = sys_conn:query(string.format("select *, sum(hold) over() from (select mod_name, sum(hold) as hold from __all_virtual_memory_info where svr_ip = '%s' and svr_port = %d and ctx_name not in ('MEMSTORE_CTX_ID', 'LIBEASY') and mod_name != 'KvstorCacheMb' group by mod_name)", addr.host, addr.svr_port))
    local mod, hold, total_hold
    local holds = {}
    while rows:next() do
      local r = rows:scan()
      mod, hold, total_hold = unpack(r)
      holds[mod] = tonumber(hold)
    end
    rows:close()

    --print(string.format("time: %d, total_hold: %d", cur_time(), total_hold))

    return tonumber(total_hold), holds
end

function refresh_mem_stat(sys_conn)
    local _, ok = sys_conn:exec("alter system refresh memory stat")
    if not ok then
      print("refresh failed")
    end
    sleep(5)
end

function compare_holds(holds_a, holds_b)
   t = {}
   for key,value in pairs(holds_a) do
     if holds_b[key] ~= nil then
       local diff = value - holds_b[key]
       if diff ~= 0 then
         table.insert(t, {key=key, diff=diff})
       end
     else
       if value > 0 then
         table.insert(t, {key=key, diff=value})
       end
     end
   end
   for key,value in pairs(holds_b) do
     if holds_a[key] == nil then
       if value > 0 then
         table.insert(t, {key=key, diff=-value})
       end
     end
   end
   table.sort(t, function(a, b) return a.diff > b. diff end)
   for i=1,#t,1 do
      if t[i].diff > 0 then
        print(string.format("%80s +%d", t[i].key, t[i].diff))
      else
        print(string.format("%80s %d", t[i].key, t[i].diff))
      end
   end
   return t
end

function merge_and_wait()
     local c, ok, errcode = obsbench.drv:conn(sys_addr.host, sys_addr.port, sys_addr.user, sys_addr.passwd, sys_addr.db)
     if not ok then
       error("create sys conn err: " .. errcode)
     end
     c:exec('alter system major freeze')
     sleep(30)
     local last_log_time = os.time() 
     while true do
       local rows = c:query("select *from __all_zone where name = 'merge_status' and info != 'IDLE'")
       local goon = rows:next()
       rows:close()
       if not goon then break end
       sleep(1)
       local cur_time = os.time() 
       if cur_time - last_log_time > 120 then
         print("wait merge done")
         last_log_time = cur_time
       end
     end
     c:close()
end

function query_dump(conn, sql)
     local rows = conn:query(sql)
     while rows:next() do
       local r = rows:scan()
       print(unpack(r))
     end
     rows:close()
end

function query_scalar(conn, sql)
     local v
     local rows = conn:query(sql)
     if rows:next() then
       v, ok = rows:scan()
       assert(ok)
     end 
     rows:close()
     return unpack(v)
end
