import platform
def build_wget_task(url, target, **__kw):
    md5 = '!popen: curl -s $url/$target.md5'
    wget = '!sh: if [[ ! -f ./$target ]] || [[ ! $md5 == $(md5sum ./$target | cut -d\' \' -f1) ]]; then wget $url/$target -O $target.new -o $target.down && mv $target.new $target && chmod 777 $target; fi'
    return build_dict(locals(), __kw)

def build_ob_wget_task(**__kw):
    oceanbase_deps = '$rpm_dir/oceanbase.el7.x86_64.deps'
    obclient_version = r"!popen: grep -m 2 obclient- $oceanbase_deps | grep -v lib | sed 's/obclient-\(.*\)[.]el.*/\1/'"
    platform_version = platform.platform()
    os_version = platform_version[platform_version.index(".x86_64") - 1]
    obclient_url="http://11.166.86.153:8877/obclient_binary/$obclient_version.el$os_version"
    obclient = build_wget_task(obclient_url, 'obclient')
    mysqltest = build_wget_task(obclient_url, 'mysqltest')
    wget = '!seq: obclient.wget mysqltest.wget'
    def after_load(kw):
        call(kw, 'wget')
    return build_dict(locals(), __kw)

rpm_dir = '$src_dir/deps/init/'
ob_wget_task = build_ob_wget_task()
