require("utils")
local cmd = require("cmd")
local inspect=require("inspect")
local strings=require("strings")

function main()
  local res, err = cmd.exec([[nm --print-size --size-sort --radix=d -fs --format=SysV bin/observer | grep 'TLS|' | awk -F '|' 'BEGIN{s=0}{if ($5>256) {s+=$5;print s,$5,$1}}' | c++filt]], 100)
  if err then error(err) end
  local res_tail, err = cmd.exec(string.format([[echo "%s" | grep -v '^$' | tail -1]], res.stdout))
  local hold_total = tonumber(strings.split(res_tail.stdout, " ")[1])
  G.create_metrics({hold_total=hold_total})
  print(res.stdout)
end
