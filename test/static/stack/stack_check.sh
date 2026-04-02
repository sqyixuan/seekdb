#!/bin/bash

log() {
  echo "$@"
}
export script_dir=`readlink -f $0 | xargs dirname`

myobjdump() { $script_dir/../../../deps/3rd/usr/local/oceanbase/devtools/bin/llvm-objdump "$@"; }
myc++filt() { $script_dir/../../../deps/3rd/usr/local/oceanbase/devtools/bin/c++filt "$@"; }
export -f myobjdump myc++filt

m="INC"
usage() { printf "Usage: $0\n\
  [-c <check function stack usage>]\n\
  [-d <decode mangled symbol while disassemble>]\n\
  [-b <binary file>]\n\
  [-w <whitelist file>]\n\
  [-p <parallel>]\n\
  [-t <stack size threshold>]\n\
  [-r <eval plugin while failed>]\n\
  [-u <update whitelist use specified db file>]\n\
  [-m <update mode [INC|FULL] INC default>]\n\
  [-f <output encoded address range of specified function>]\n\
  [-e <disassemble address range>] " 1>&2; exit 1; }
while getopts "cdb:w:p:t:r:u:m:e:f:" o; do
    case "$o" in
        c)
            c=true
            ;;
        d)
            d=true
            ;;
        b)
            b=$OPTARG
            if [ ! -e "$b" ]; then
              log "binary file not exit!!!"
              usage
            fi
            if [ -z "$(readelf -S $b | grep symtab)" ]; then
              log "lack symtab section!!!"
              usage
            fi
            ;;
        w)
            w=$OPTARG
            if [ ! -e "$w" ]; then
              log "whitelist file not exist!!!"
              usage
            fi
            ;;
        p)
            p=$OPTARG
            ((p > 0)) || usage
            ;;
        t)
            t=$OPTARG
            ;;
        r)
            r=$OPTARG
            ;;
        u)
            u=$OPTARG
            ;;
        m)

            m=$OPTARG
            [ "$m" == "INC" ] || [ "$m" == "FULL" ] || usage
            ;;
        f)
            f=$OPTARG
            ;;
        e)
            e=$OPTARG
            ;;
        *)
            usage
            ;;
    esac
done
shift $((OPTIND-1))
if [ -z "$b" ]; then
  usage
fi
if [ -z "$p" ]; then
  p=4
fi
if [ -z "$t" ]; then
  t=8192
fi
binary_path=$b
n_proc=$p
threshold=$t

if [ -n "$e" ]; then
  #decode hex string to addr_range
  read -r addr size <<<`echo $e | xxd -r -p | awk -F '_' '{print $1,$2}'`
  start_addr=$addr
  stop_addr=`printf "%x" $((0x$addr+$size))`
  myobjdump --start-address=0x$start_addr --stop-address=0x$stop_addr -d $binary_path `[ -n "$d" ] && echo "-C" || echo ""`
  exit
elif [ -n "$f" ]; then
  cat <(paste <(readelf -sW $binary_path | awk '$4 == "FUNC"{print $2"_"$3,$8}') <(readelf -sW $binary_path | awk '$4 == "FUNC"{print $8}' | myc++filt) | grep -F "$f") | while read line
  do
    read -r addr_range func <<< $line
    # encode addr_range to hex string
    echo `echo -n $addr_range | od -A n -t x1 | xargs | sed 's/ //g'`, $func | myc++filt
  done
  exit
elif [ -n "$u" ]; then
  if [ "$m" == "INC" ]; then
  # cmd="sqlite3 $u 'select distinct demangle_name from function union select demangle_name from whitelist order by 1'"
    cmd="sqlite3 $u 'select distinct demangle_name from function except select demangle_name from whitelist order by 1'"
  else
    cmd="sqlite3 $u 'select distinct demangle_name from function order by 1'"
  fi
  eval $cmd > new_whitelist
  printf "new whitelist generated: %s" `realpath new_whitelist`
  exit
fi

IFS=$'\n'
sorted_funcs=(`readelf -sW $binary_path | awk '$4 == "FUNC" {print $2,$3}' | sort`)
unset IFS
n_line=${#sorted_funcs[*]}
batch=$((($n_line+$n_proc-1)/$n_proc))

childs=()
kill_childs(){
  for child in ${childs[*]}
  do
    pgrep -P$child | xargs -r kill -9
    kill -9 $child 2>/dev/null
    wait $child 2>/dev/null
  done
  exit 1
}
trap "kill_childs" 2

tmp_dir="scan_result"
rm -rf $tmp_dir && mkdir $tmp_dir
db=$tmp_dir/test.db
log "db file: "$db
sqlite3 $db <<EOF
create table function (name TEXT, stack_size INTEGER, demangle_name TEXT, CONSTRAINT uk UNIQUE (name, stack_size));
create table whitelist (demangle_name TEXT primary key);
EOF

if [ -n "$c" ]; then
  for index in `seq 0 $(($n_proc-1))`
  do
    {
      begin=$(($batch*$index))
      end=$(($begin+$batch-1))
      if [[ $end -ge $n_line ]]; then
        end=$(($n_line-1))
      fi
      start_addr=`echo ${sorted_funcs[$begin]} | awk '{print $1}'`
      read -r addr size <<<`echo ${sorted_funcs[$end]}`
      stop_addr=`printf "%x" $((0x$addr+size))`
      if [[ 0x$stop_addr -gt 0x$start_addr ]]; then
        myobjdump --start-address=0x$start_addr --stop-address=0x$stop_addr -d $binary_path | python $script_dir/stack_check.py -l $threshold | grep -v 18446744073709551488 > $tmp_dir/funcs_$index
      fi
    } &
    childs+=($!)
  done

  wait
  cat $tmp_dir/funcs_* | sort | uniq | awk '{"myc++filt '\''"$2"'\''" | getline demangle_name; print $2"|"$1"|"demangle_name}' > $tmp_dir/dump_csv
  sqlite3 $db <<EOF
.mode csv
.separator "|"
.import $tmp_dir/dump_csv function
EOF

  if [ -n "$w" ]; then
    cat $w > $tmp_dir/whitelist_csv
    sqlite3 $db <<EOF
.mode csv
.separator "|"
.import $tmp_dir/whitelist_csv whitelist 
EOF
    cmd="sqlite3 $db 'select \"  --> stack_size: \" || stack_size || \", demangle_name: \" || demangle_name from function where demangle_name not in (select demangle_name from whitelist)'"
    if [[ -n `eval $cmd` ]]; then
      eval $cmd
      log `printf "exist functions whose stack usage exceed %s!!! query_cmd: %s" $threshold "$cmd"`
      eval $r
      exit 1
    fi
  else
    # dump only
    cmd="sqlite3 $db 'select stack_size, demangle_name from function order by stack_size desc'"
    log "query_cmd: "$cmd
  fi
fi

exit 0 
