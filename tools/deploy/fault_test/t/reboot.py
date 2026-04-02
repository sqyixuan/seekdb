need_reboot = (not disable_reboot) and _mode_ != 'interactive'
if need_reboot:
    obi.reboot
    setup

