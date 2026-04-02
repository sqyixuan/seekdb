## quick start
show trace json版信息使用
1. 将show trace的json版信息放入到show_trace_data.json
2. 使用./flt_analyzer <trace_id> 显示出show trace的完整版本

全链路诊断信息使用
1. 根据<trace_id>到proxy, server上收集到一个文件中
2. 将全链路诊断收集到的信息输入到flt_info.log文件中
3. 使用./flt_analyzer <trace_id> 显示出全链路诊断信息的完整版本

### 全链路trace(full link tracer)
> 根据指定的id回溯trace_log

日志范例`trace.log.20220815174230:[2022-08-15 17:13:03.722834] [68536][T1004_TNT_L0_G0][T1004][Y963645869EB-0005E62FBA0C7AD1-0-0] {"trace_id":"0005e644-0829-5684-192e-c726f3270bd5","name":"close","id":"0005e630-00cc-6881-0000-0000000041d9","start_ts":1660554783505513,"end_ts":1660554783505528,"parent_id":"0005e630-00ce-e6c8-0000-0000000041d4","is_follow":false}`

## 运行
### 1. 不使用配置文件，命令行直接运行

format:

    python3 main.py <trace_id> --no-config [-d local_dir1 @host[:port]/remote_dir2 @host[:port]/remote_dir3] [-f @host[:port]/remote_file1 local_file2 ] -u <username> -p <password> -r <max_recursion>

example:

    python3 main.py 0005e821-25a9-3d9f-f257-4299077565e1 --no-config -d /data/logs/obs0/log /data/logs/obs1/log @11.124.9.7/data/logs/obproxy/log -u 工号 -p 密码 -r 0

### 2. 根据配置文件运行

在flt_analyzer.py同级目录创建config.ini

    # NOTE: 所有值请勿加单双引号
    [env]
    # 要访问远程路径则必须配置username
    username=root
    
    # 设置私钥文件路径以免密登录
    key_filename=
    
    # 多个路径以;分隔，远程路径以@host_ip开头
    # NOTE: json文件必须在log_files中指定
    log_files=./trace_res.json; ./trace_res2.json;  ./trace_res3.log
    # NOTE:文件夹中只会访问*trace.log*文件
    log_dirs=@100.8.109.24/data/log1/oblt.obs0/log; @100.8.109.24:22/data/log1/oblt.obs1/log; /data/log1/local.obproxy/log
    
    [parse]
    max_recursion = -1

配置完成后运行:

    python3 flt_analyzer.py <trace_id>

### 工作流程
1. 从每台服务器的日志文件中搜索trace_id并聚合为trace_merged文件
2. 将每台服务器的trace_merged文件传输回本机
3. 解析trace_merged文件并构建关系树
4. 输出到本地文件

