#!/bin/bash

DEPLOY_PATH=$(readlink -f "$(dirname ${BASH_SOURCE[0]})")

shopt -s expand_aliases
source $DEPLOY_PATH/activate_obd.sh
BASE_DIR="$OB_BASE_DIR"

current_path=$(pwd)
if [[ "$current_path" != "$DEPLOY_PATH" ]]
then
  echo "Switching basedir to [$DEPLOY_PATH]..."
  cd $BASE_DIR/tools/deploy || exit 1
fi

function main() {
  ob_do_args=$@
  $OBDO_CMD_BIN $ob_do_args
}

main "$@"