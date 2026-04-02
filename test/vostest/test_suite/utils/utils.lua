_key = ""
function printTable(table , level)
  level = level or 1
  local indent = ""
  for i = 1, level do
    indent = indent.."  "
  end

  if _key ~= "" then
    print(indent.._key.." ".."=".." ".."{")
  else
    print(indent .. "{")
  end

  _key = ""
  for k,v in pairs(table) do
     if type(v) == "table" then
        _key = k
        printTable(v, level + 1)
     else
        local content = string.format("%s%s = %s", indent .. "  ",tostring(k), tostring(v))
      print(content)
      end
  end
  print(indent .. "}")

end

function sleep(n)
  os.execute("sleep " .. tonumber(n))
end

function cur_time()
  return os.time()
end

function split(inputstr, sep)
  if sep == nil then
     sep = "%s"
  end
  local t={}
  for str in string.gmatch(inputstr, "([^"..sep.."]+)") do
     table.insert(t, str)
  end
  return t
end
