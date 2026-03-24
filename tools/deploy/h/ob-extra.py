@ObServerBuilder
def ObServerL2(**__kw):
   # gcda related functions
    mkdir_for_gcda = '''!ssh: cd $obdir; strings bin/observer |egrep '*.gcda$'|xargs --no-run-if-empty -L1 dirname|cut -d / -f 4-|xargs --no-run-if-empty -L1 mkdir -p '''
    local_gcda_dir = '$collected_gcda_dir/${ob_name}.$name'
    collect_gcda = '''!sh: mkdir -p $local_gcda_dir && ssh $usr@$ip sync && rsync -r $usr@$ip:$obdir/src/ "$local_gcda_dir" # NoException DiscardOutput'''
    tree_gcda = '''!ssht: cd $obdir/src; tree -a .'''
    ls_gcda = '''!ssht: cd $obdir/src && find . -name '*.gcda' '''
    rm_gcda = '''!ssht: cd $obdir/src && find . -name '*.gcda' -print -exec rm {} + '''
    local_ls_gcda = '''!sh: find ${local_gcda_dir} -name '*.gcda' '''
    local_rm_gcda = '''!sh: rm -rf ${local_gcda_dir} '''
    return build_dict(locals(), **__kw)

@ObInstanceBuilder
def ObInstanceL0(**__kw):
    # gcda related functions
    mkdir_for_gcda = '!all: all_server mkdir_for_gcda'
    ls_gcda = '!all: all_server ls_gcda'
    rm_gcda = '!all: all_server rm_gcda'
    collected_gcda_dir = '$local_dir/coverage_data'
    collect_gcda ='!all: all_server collect_gcda'
    local_rm_gcda = '!all: all_server local_rm_gcda'
    local_ls_gcda = '!all: all_server local_ls_gcda'
    return build_dict(locals(), **__kw)
