alter system set direct_load_allow_fallback=True;
let $__parameter_name__=direct_load_allow_fallback;
let $__parameter_value__=True;
--source include/liboblog/check_parameter.inc
