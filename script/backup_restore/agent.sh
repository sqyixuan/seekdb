#!/bin/sh
# -------------------------------------------------------------------------------
# Filename:    agent.sh
# Revision:    1.0
# Date:        2017/11/22
# Author:      chaoguang.cy
# Description: The wrapper to manage agentserver as well as agentrestore
# Notes:       Entry point 'main' tag
# -------------------------------------------------------------------------------

TOPDIR=`dirname "$0"|xargs readlink -f|xargs dirname`
BINDIR="$TOPDIR"/bin
LIBDIR="$TOPDIR"/lib
ETCDIR="$TOPDIR"/etc
RUNDIR="$TOPDIR"/run
DEFAULT_AGENTBACKUP_PORT=2911
DEFAULT_MEM_SIZE=32
STACK_SIZE=10240

export LD_LIBRARY_PATH=$LIBDIR:$LD_LIBRARY_PATH
export PATH=/opt/taobao/java/bin/:$PATH
export JAVA_HOME=/opt/taobao/java/

ulimit -s 10240
CUR_STACK_SIZE=`ulimit -s`
if [ $CUR_STACK_SIZE -lt $STACK_SIZE ]; then
  echo "stack size[$CUR_STACK_SIZE] less than [$STACK_SIZE], must correct it first.";
  exit 1;
fi

function usage
{
      echo "Usage: agent.sh [-h|start_backup portnumber|stop_backup|force_stop_backup|exist_backup|start_restore memory_limit|stop_restore|force_stop_restore|exist_restore]"

}

#Get the pid number of agentbackup
#RETURNS:
#     -1  :  cannot get valid pid number. Directory of file doesn't exist
#     valid pid number
function get_agentbackup_pid
{
   pid_file=${RUNDIR}/agentserver.pid
   if [ ! -d "$RUNDIR" ] || [ ! -f "$pid_file" ];then
     echo 0
   else
     echo `cat $pid_file`
   fi
}

#Get the pid number of agentrestore
#RETURNS:
#     0  :  cannot get valid pid number. Directory of file doesn't exist
#     valid pid number
function get_agentrestore_pid
{
   pid_file=${RUNDIR}/agentrestore.pid
   if [ ! -d "$RUNDIR" ] || [ ! -f "$pid_file" ];then
     echo 0
   else
     echo `cat $pid_file`
   fi
}


#Check if the specified pid is alive or not
#PARAMETERS:
#   $1  -   Pid number
#RETURNS:
#   1   -   Does exist.
#   0   -   agentbackup not exists.
function is_process_alive
{
   if [ "$1" = 0 ];then
     echo 0
   else
     kill -0 "$1" 1>/dev/null 2>/dev/null
     if [ "$?" = 0 ];then
       echo 1
     else 
       echo 0
     fi
   fi
}

#Simply start the agentserver without any returning values.
#In other words, this tage won't guarantee if backup process runs succesfully or not.
#If we havn't give any port number, we are using default which is 2911 for now.
#PARAMETERS:
#   $1  -  portnumber  
function start_backup_process
{
  exe_file=${BINDIR}/agentserver
  conf_file=${ETCDIR}/agentserver.conf
  if [ x"$1" = x ];then
    port_num=$DEFAULT_AGENTBACKUP_PORT
  else
    port_num=$1
  fi
  $exe_file -f $conf_file -P $port_num 1>nohup.backup.log 2>&1
}

#Simply start the agentrestore without any returning values.
#In other words, this tage won't guarantee if backup process runs succesfully or not.
#If we havn't parameterized the memory size, we are using default size which is 32g.
#PARAMETERS:
#   $1  -  memory size
function start_restore_process
{
  exe_file=${BINDIR}/agentrestore.jar
  conf_file=${ETCDIR}/agentrestore.conf
  if [ x"$1" = x ];then
    mem_size=32
  else
    mem_size=$1
  fi
  nohup java -XX:+UseG1GC -XX:+UseStringDeduplication -Xms${mem_size}g -Xmx${mem_size}g -jar ${exe_file} file=${conf_file}  1>nohup.restore.log 2>&1 &
}

#Stop a process
#PARAMETERS:
#   $1  -   Pid number
#   $2  -   Kill mode.

function stop_process
{
  pid_num=$1
  kill_mode=$2
  if [ "$pid_num" = 0 ];then
    exit
  fi
  kill $kill_mode $pid_num
}

function main
{
  case "x$1" in
      x-h)
          usage
          ;;
      xstart_backup)
          start_backup_process "$2"
          ;;
      xstop_backup)
          pid_num=`get_agentbackup_pid`
          stop_process ${pid_num} "-15"
          ;;
      xforce_stop_backup)
          pid_num=`get_agentbackup_pid`
          stop_process ${pid_num} "-9"
          ;;
      xexist_backup)
          pid_num=`get_agentbackup_pid`
          echo `is_process_alive ${pid_num}`
          ;;
      xstart_restore)
          start_restore_process "$2"
          ;;
      xstop_restore)
          pid_num=`get_agentrestore_pid`
          stop_process ${pid_num} "-15"
          ;;
      xforce_stop_restore)
          pid_num=`get_agentrestore_pid`
          stop_process ${pid_num} "-9"
          ;;
      xexist_restore)
          pid_num=`get_agentrestore_pid`
          echo `is_process_alive ${pid_num}`
          ;;
      xjava_env)
          echo "PATH=$PATH"
          echo "JAVA_HOME=$JAVA_HOME"
          echo "which java=`which java`"
          java -version
          ;;
      x*)
          if [ x"$@" = x ];then
            echo "Command cannot be empty. Check 'agent.sh -h' for valid command."
          else  
            echo "Command '$@' doesn't support. Check 'agent.sh -h' for valid command."
          fi
          ;;
  esac
}

main $@

