
CUR_DIR=`pwd`
function get_one_server() {
  if [ -f $CUR_DIR/server.list ]; then
    ip=`head -n 1 $CUR_DIR/server.list`
    if [ -z $ip ]; then
      ip=`hostname -i`
    fi
  else
    ip=`hostname -i`
  fi
  echo $ip
}

