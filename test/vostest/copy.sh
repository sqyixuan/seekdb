set -x
script_dir=`readlink -f $0 | xargs dirname`
bin_path=$1
vostest_path=$2
 [ -n "$bin_path" ] || ! echo "empty observer path!" || exit 1
 [ -n "$vostest_path" ] || ! echo "empty vostest path!" || exit 1
cp $bin_path $vostest_path/bin/
cp -r $script_dir/test_suite $vostest_path/
cp $script_dir/benchmark.yml $vostest_path/
cp $script_dir/run.sh $vostest_path/
readlink -f $script_dir/../.. > $vostest_path/source_code_path
