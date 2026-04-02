#!/bin/bash
export OB_BASE_DIR=$(readlink -f "$(dirname ${BASH_SOURCE[0]})/../..")
export OB_DO_GLOBAL_CONFIG=${OB_DO_GLOBAL_CONFIG:-~/.ob_do_global}
[[ -f $OB_DO_GLOBAL_CONFIG ]] && source $OB_DO_GLOBAL_CONFIG
export DEPLOY_PATH=$OB_BASE_DIR/tools/deploy
export OBD_FORCE_UPDATE_PLUGINS=1
if grep 'dep_create.sh' $OB_BASE_DIR/build.sh 2>&1 >/dev/null
then
    export DEP_PATH=$OB_BASE_DIR/deps/3rd
else
    export DEP_PATH=$OB_BASE_DIR/rpm/.dep_create/var
fi
export ARCHITECTURES=$(arch)
export OBD_DEPS_PATH="$OB_BASE_DIR/deps/init/oceanbase.el7.$ARCHITECTURES.deps"
export OBD_BIN=${_OBD_BIN:-$DEP_PATH/usr/bin/obd}
export OBDO_CMD_BIN=${_OBDO_CMD_BIN:-$DEP_PATH/usr/bin/ob_do_cmd}
alias obd="${OBD_BIN}"
export OBD_HOME=${_OBD_HOME:-$OB_BASE_DIR/tools/deploy}
export OBD_INSTALL_PRE=${_OBD_INSTALL_PRE:-$DEP_PATH} 
export OBD_PORT_GEN=$((100*($(id -u)%500)+10000))

if [ ${_OBD_PROFILE} ]; then
    source ${_OBD_PROFILE}
fi

if [ -f $OBD_INSTALL_PRE/etc/profile.d/obd.sh ] 
then
  source $OBD_INSTALL_PRE/etc/profile.d/obd.sh 
fi
if [[ "${OB_DO_NO_GLOBAL_CLUSTER:-0}" == "0" ]]; then
    obd_business=${OBD_HOME_GLOBAL:-~/.obd_business}
    mkdir -p $obd_business/cluster
    mkdir -p $OBD_HOME/.obd
    [ ! -d $OBD_HOME/.obd/cluster ] && ln -fs $obd_business/cluster $OBD_HOME/.obd
fi

if [ -n "$BASH_VERSION" ]; then
    complete -F _obd_sh_complete_func ./obd.sh
fi

function _obd_sh_reply_deploy_names() {
    if [[ -d $OBD_HOME/.obd/cluster ]]
    then
    res=$(ls -p $OBD_HOME/.obd/cluster 2>/dev/null | sed "s#/##")
    COMPREPLY=( $(compgen -o filenames -W "${res}" -- ${cur}) )
    fi
}

function _obd_sh_reply_current_files() {
    filename=${cur##*/}
    dirname=${cur%*$filename}
    res=$(ls -a -p $dirname 2>/dev/null | sed "s#^#$dirname#")
    compopt -o nospace
    COMPREPLY=( $(compgen -o filenames -W "${res}" -- ${cur}) )
}

function _obd_sh_reply_yaml_files() {
    filename=${cur##*/}
    dirname=${cur%*$filename}
    res=$(ls -a -p $dirname 2>/dev/null | grep -E '(*.yaml|*.yml)' | sed "s#^#$dirname#")
    compopt -o nospace
    COMPREPLY=( $(compgen -o filenames -W "${res}" -- ${cur}) )
}

function _obd_sh_complete_func
{
  local all_cmds
  declare -A all_cmds
  COMPREPLY=()
  cur="${COMP_WORDS[COMP_CWORD]}"
  prev="${COMP_WORDS[COMP_CWORD-1]}"
  all_cmds["$*"]="prepare deploy redeploy reinstall start stop restart destroy upgrade mysqltest pid ssh less gdb sql mysql conn_mysql connmysql oracle conn_oracle connoracle edit list sys display create_tenant drop_tenant sysbench tpch tpch_obp tpcc tpcds bmsql oci graph display-trace set-config leases remove-lease it"
  case $prev in
  list)
    return 0
    ;;
  -p|--path)
  _obd_sh_reply_current_files
  ;;
  -c|--config)
  _obd_sh_reply_yaml_files
  ;;
  -n|--deploy-name)
  _obd_sh_reply_deploy_names
  ;;
  *)
    valid_len=$COMP_CWORD
    words=( ${COMP_WORDS[@]::valid_len} )
    index=valid_len
    while (( index >= 1 )); do
        target="${words[*]}"
        cmd=${all_cmds[$target]}
        if [[ "$cmd" != "" ]]
        then
          if [[ $cmd =~ ^_obd_sh_reply.* ]]
          then
            $cmd
            break
          else
            if [[ $cmd =~ .*'=' ]]
            then
            compopt -o nospace
            fi
            COMPREPLY=( $(compgen -W "${cmd}" -- ${cur}) )
            break
          fi
        fi
        index=$(( index - 1))
        tmp=${words[*]::index}
        [[ "$tmp" != "" ]] && parent_cmd=${all_cmds[$tmp]}
        if [[ "$parent_cmd" =~ ^_obd_sh_reply.*  || " $parent_cmd " =~ " ${words[index]} " ]]; then
          words[index]='*'
        else
          break
        fi
    done
    ;;
  esac


}
