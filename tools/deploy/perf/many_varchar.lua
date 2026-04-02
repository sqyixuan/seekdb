--
-- many_varchar_table schema:
-- +------------+-------------+----------+------+---------+-------+---------+
-- | field      | type        | nullable | key  | default | extra | comment |
-- +------------+-------------+----------+------+---------+-------+---------+
-- | gmt_create | CREATETIME  | YES      |    0 | NULL    |       |         |
-- | gmt_modify | MODIFYTIME  | YES      |    0 | NULL    |       |         |
-- | id         | INT         | YES      |    1 | NULL    |       |         |
-- | str        | VARCHAR(64) | YES      |    2 | NULL    |       |         |
-- | v1         | VARCHAR(64) | YES      |    0 | NULL    |       |         |
-- | v2         | VARCHAR(64) | YES      |    0 | NULL    |       |         |
-- | v3         | VARCHAR(64) | YES      |    0 | NULL    |       |         |
-- | ...        | VARCHAR(64) | YES      |    0 | NULL    |       |         |
-- | ...        | VARCHAR(64) | YES      |    0 | NULL    |       |         |
-- | v50        | VARCHAR(64) | YES      |    0 | NULL    |       |         |
-- +------------+-------------+----------+------+---------+-------+---------+
-- totally 54 columns
--

TABLE_NAME = "many_varchar_table"
COLUMN_NUM = 52
LINE_PER_SQL = 2
MAX_N = 10000

column_list = "(id,str"
for i = 1, COLUMN_NUM - 2 do
  column_list = column_list .. ",v" .. tostring(i)
end
column_list = column_list .. ")"

prepared_list = "(?"
for i = 1, COLUMN_NUM - 1 do
  prepared_list = prepared_list .. ",?"
end
prepared_list = prepared_list .. ")"

replace_sql = "replace into " .. TABLE_NAME .. " " .. column_list .. " values" .. prepared_list
for i = 1, LINE_PER_SQL - 1 do
  replace_sql = replace_sql .. "," .. prepared_list
end

params_table = {}
for j = 1, COLUMN_NUM * LINE_PER_SQL do
  if j % COLUMN_NUM == 1 then
    params_table[j] = {
      type = "int",
      range_min = 1,
      range_max = MAX_N,
    }
  else
    params_table[j] = {
      type = "varchar",
      range_min = 1,
      range_max = MAX_N,
      varchar_length = 64,
    }
  end
end


stress_sqls = {
  {
    sql = replace_sql,
    params = params_table,
  },
}
prepare_sqls = {
  {
    sql = replace_sql,
    params = params_table,
  },
}
