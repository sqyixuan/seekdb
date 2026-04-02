#!/usr/bin/env sh
scp hierarchical_sort_oracle.sql muhang.zb@100.81.152.95:~/hierarchical_basic.sql
ssh muhang.zb@100.81.152.95 "/home/tingshuai.yts/exec_sql2.sh" > hierarchical_basic_oracle.result
