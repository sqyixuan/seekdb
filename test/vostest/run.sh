_host=${HOST:-127.0.0.1}
_svr_port=${RPC_PORT:-16603}
_mysql_port=${MYSQL_PORT:-16605}
_path=${DEPLOY_PATH:-~/}
_devname=${DEVNAME:-lo}

_path=$(echo $_path | sed -r 's/\//\\\//g')
cat benchmark.yml | sed -r "s/\&host.*/\&host $_host/g" | sed -r "s/\&path.*/\&path $_path/g" | sed -r "s/mysql_port:/mysql_port: $_mysql_port/g" | sed -r "s/svr_port:/svr_port: $_svr_port/g" | sed -r "s/devname:.*/devname: $_devname/g"  > benchmark.farm.yml 

keep_items=(
  "check_global_variables"
  "check_thread_local"
)
IFS=, keep_param="${keep_items[*]}"

run_param="reboot[mem_hold_basic],keep[${keep_param}]"
time ./obmeter/obmeter -r"$run_param" -b benchmark.farm.yml -s
