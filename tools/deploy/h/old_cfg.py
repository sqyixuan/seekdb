class ObCfgWrapper(dict):
    def __init__(self):
        dict.__init__(self)
        self.init_config = dict()
    def __setattr__(self, key, v):
        self[key] = v
        globals()[key] = v
    def __getattr__(self, key):
        return self[key]

gcfg = dict(stop=False)
ObCfg = ObCfgWrapper()
def patch_old_cfg():
    def get_config_dict(i):
        tmp = {}
        if isinstance(i, str):
          for item in i.split(','):
            if item.strip() != "":
              tmp[item.split('=')[0].strip()] = item.split('=')[1].strip("' ")
        else:
          tmp = i
        return tmp
    logger.debug('### patch obcfg for compatability ###')
    init_config = ObCfg.get('init_config', '')
    if type(init_config) == str:
        init_config = get_config_dict(init_config)
    obs_cfg.update(init_config)
    globals().update((k, v) for (k, v) in ObCfg.items() if not k.startswith('_') and not callable(v))

def after_load(kw):
    patch_old_cfg()
