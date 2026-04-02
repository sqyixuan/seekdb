# -*- coding: UTF-8 *-*
ObClusterBuilder = ChainBuilder()

@ObClusterBuilder
def ObClusterL0(**__kw):
    all_obi = '!filter: is_match role ^obi$'
    reboot = '!all: all_obi reboot'
    id = '!all: all_obi id'
    pid = '!all: all_obi pid'
    stop = '!all: all_obi stop'
    return build_dict(locals(), **__kw)

def OC(**kw):
    id = cluster_id_gen.next()
    oc = ObClusterBuilder.build()
    oc.update(name='oc%d'%(id), app_name='cluster%d.$usr.$local_ip'%(id), is_multi_cluster=1)
    oc.update(kw)
    obi_list = get_match_child(oc, '^obi$')
    if not obi_list: raise Fail('no obi defined for cluster')
    obi_list = sorted(obi_list.items(), key=lambda x: x[0])
    # Set primary_cluster variable
    oc.update(primary_cluster=obi_list[0][0], primary_rs_list="${%s.rs_server_list}" % (obi_list[0][0]));
    # Set the cluster_id for each cluster
    for idx, (name, obi) in enumerate(obi_list):
      obi.update(cluster_id=idx+1);

    for name, obi in obi_list[1:]:
        obi.update(cluster_role='standby', bootstrap_param='CLUSTER STANDBY $basic_bootstrap_param')
    obi_list[0][1].update(cluster_role='primary', bootstrap_param='CLUSTER PRIMARY $basic_bootstrap_param')

    return oc

