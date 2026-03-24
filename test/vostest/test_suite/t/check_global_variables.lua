require("utils")
local cmd = require("cmd")
local inspect=require("inspect")
local strings=require("strings")

function main()
  local res, err = cmd.exec([[nm --print-size --size-sort --radix=d bin/observer | grep ' [bBV] ' | awk 'BEGIN{s=0}{if ($2>8192){s+=$2;$1="";$3="";print s" "$0}}']], 100)
  if err then error(err) end
  local res_tail, err = cmd.exec(string.format([[echo "%s" | grep -v '^$' | tail -1]], res.stdout))
  if err then error(err) end
  local hold_total = tonumber(strings.split(res_tail.stdout, " ")[1])
  G.create_metrics({hold_total=hold_total})
  print(res.stdout)
end
