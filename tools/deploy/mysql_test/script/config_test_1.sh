#!/bin/bash

# Specify the path to the JSON file
variables_json="../../src/share/system_variable/default_system_variable.json"
parameters_json="../../src/share/parameter/default_parameter.json"
default_file="./mysql_test/include/config_default.inc"                 # Default configuration file

output_file="./mysql_test/include/config_test.inc"                     # Main output file
output_file_cluster="./mysql_test/include/config_cluster.inc"          # Cluster configuration output file
output_file_tenant="./mysql_test/include/config_tenant.inc"            # Tenant configuration output file
output_default_file_cluster="./mysql_test/include/config_default_cluster.inc"  # Cluster default configuration restore file
output_default_file_tenant="./mysql_test/include/config_default_tenant.inc"    # Tenant default configuration restore file
output_config_reboot_value="./mysql_test/include/config_reboot_value.inc"
ob_config="../../tools/deploy/init.sql"  # Deployment configuration file

# Clear old output files
rm -f "$output_file" "$output_file_cluster" "$output_file_tenant"
rm -f "$output_default_file_cluster" "$output_default_file_tenant" "$output_config_reboot_value"

# Clear output files
> "$output_file"
> "$output_file_cluster"
> "$output_file_tenant"
> "$output_default_file_cluster"
> "$output_default_file_tenant"
> "$output_config_reboot_value" 


# 检查文件是否存在
if [ ! -f "$variables_json" ]; then
  echo "文件 $variables_json 不存在."
  exit 1
fi

if [ ! -f "$parameters_json" ]; then
  echo "文件 $parameters_json 不存在."
  exit 1
fi

if [ ! -f "$default_file" ]; then
  echo "默认配置文件 $default_file 不存在."
  exit 1
fi

if [ ! -f "$ob_config" ]; then
  echo "文件 $ob_config 不存在."
  exit 1
fi

# 使用 Python 处理 JSON 文件、生成 SQL，同时还原默认配置
python3 <<EOF
import json
import re

def process_value(value):
    """处理配置值"""
    if isinstance(value, bool):
        return str(value).lower()
    elif isinstance(value, str):
        if value.lower() in ('true', 'false'):
            return value.lower()
        return f"'{value}'"
    elif isinstance(value, int):
        return value
    else:
        return f"'{value}'"

def parse_default_config(file_path):
    """解析默认配置文件，返回一个字典"""
    default_map = {}
    regex = r"(SET GLOBAL|alter system set) (\w+)=(.+);"

    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            match = re.match(regex, line.strip())
            if match:
                param_type = match.group(1)  # SET GLOBAL or alter system set
                param_name = match.group(2) # 参数名
                param_value = match.group(3) # 默认值
                default_map[param_name] = (param_type, param_value)
    return default_map

def find_matches_in_init(param_name, init_file): 
    matches = []
    with open(init_file, "r", encoding="utf-8") as f:
        for line in f:
            if param_name in line:
                matches.append(line.strip())
    return matches

# 加载默认配置
default_config = parse_default_config("$default_file")

# 打开输出文件写入
with open("$output_file", "a", encoding="utf-8") as output_f, \
     open("$output_file_cluster", "a", encoding="utf-8") as cluster_f, \
     open("$output_file_tenant", "a", encoding="utf-8") as tenant_f, \
     open("$output_default_file_cluster", "a", encoding="utf-8") as default_cluster_f, \
     open("$output_config_reboot_value", "a", encoding="utf-8") as reboot_value, \
     open("$output_default_file_tenant", "a", encoding="utf-8") as default_tenant_f:

    # 读取 variables_json 文件并处理
    with open("$variables_json", "r", encoding="utf-8") as f:
        variables_data = json.load(f)
        for scenario in variables_data:
            if 'variables' in scenario and 'tenant' in scenario['variables']:
                for tenant in scenario['variables']['tenant']:
                    var_name = tenant.get('name')
                    var_value = tenant.get('value')

                    processed_value = process_value(var_value)
                    sql_statement = f"SET GLOBAL {var_name}={processed_value};\n"

                    # 将语句写入总文件和租户文件
                    output_f.write(sql_statement)
                    tenant_f.write(sql_statement)

                    # 还原默认配置（仅当 var_name 存在于 config_default.inc 中）
                    if var_name in default_config:
                        param_type, default_value = default_config[var_name]
                        default_sql = f"{param_type} {var_name}={default_value};\n"
                        default_tenant_f.write(default_sql)

                    # 在 init.sql 文件中查找匹配，并写入 config_default_cluster.inc
                    matches = find_matches_in_init(var_name, "$ob_config")
                    for match in matches:
                        reboot_value.write(match + "\n")

    # 读取 parameters_json 文件并处理
    with open("$parameters_json", "r", encoding="utf-8") as f:
        parameters_data = json.load(f)
        for scenario in parameters_data:
            if 'parameters' in scenario:
                # cluster 配置处理
                if 'cluster' in scenario['parameters']:
                    for cluster in scenario['parameters']['cluster']:
                        param_name = cluster.get('name')
                        param_value = cluster.get('value')

                        processed_value = process_value(param_value)
                        sql_statement = f"alter system set {param_name}={processed_value};\n"

                        # 将语句写入总文件和 cluster 文件
                        output_f.write(sql_statement)
                        cluster_f.write(sql_statement)

                        # 还原默认配置（仅当 param_name 存在于 config_default.inc 中）
                        if param_name in default_config:
                            param_type, default_value = default_config[param_name]
                            default_sql = f"{param_type} {param_name}={default_value};\n"
                            default_cluster_f.write(default_sql)

                        # 在 init.sql 文件中查找匹配，并写入 config_default_cluster.inc
                        matches = find_matches_in_init(param_name, "$ob_config")
                        for match in matches:
                            reboot_value.write(match + "\n")

                # tenant 配置处理
                if 'tenant' in scenario['parameters']:
                    for tenant in scenario['parameters']['tenant']:
                        param_name = tenant.get('name')
                        param_value = tenant.get('value')

                        processed_value = process_value(param_value)
                        sql_statement = f"alter system set {param_name}={processed_value};\n"

                        # 将语句写入总文件和租户文件
                        output_f.write(sql_statement)
                        tenant_f.write(sql_statement)

                        # 还原默认配置（仅当 param_name 存在于 config_default.inc 中）
                        if param_name in default_config:
                            param_type, default_value = default_config[param_name]
                            default_sql = f"{param_type} {param_name}={default_value};\n"
                            default_tenant_f.write(default_sql)

                        # 在 init.sql 文件中查找匹配，并写入 config_default_cluster.inc
                        matches = find_matches_in_init(param_name, "$ob_config")
                        for match in matches:
                            reboot_value.write(match + "\n")

EOF

echo "分租户生成 inc 文件以及还原默认配置完成"