#!/bin/bash
target_dir=t/compat/obguard_collect
cd "$target_dir" || { echo "无法进入目录 $target_dir"; exit 1; }
for file in log_error*.log*; do
    if [ -f "$file" ]; then
        echo "正在读取文件: $file"
        cat "$file"
        echo "------------------------"
    fi
done
cd -
