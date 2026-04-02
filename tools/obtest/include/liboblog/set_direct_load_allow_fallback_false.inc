alter system set direct_load_allow_fallback=False;
let $__parameter_name__=direct_load_allow_fallback;
let $__parameter_value__=False;
--source include/liboblog/check_parameter.inc
