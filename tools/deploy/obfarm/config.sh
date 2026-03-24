#!/bin/bash

export CUR_DIR=`pwd`
export ROOT_DIR="$CUR_DIR/../../.."
export DEPLOY_DIR="$CUR_DIR/.."
export CONFIG_FILE="$DEPLOY_DIR/config2.py"
export RESULT_FILE="$CUR_DIR/result_report"

export CC="ccache gcc"
export CXX="ccache g++"
export CCACHE_COMPILERCHECK=content
export CCACHE_SLOPPINESS=include_file_mtime,time_macros,file_macro
