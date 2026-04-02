--disable_query_log
connection sys;
--echo #########################
--echo ###  展示 $op_id 号算子的op cost
--echo #########################
inc $op_id;
eval alter system set_tp tp_no = 566, error_code = $op_id , frequency = 1;
connection default;
--enable_query_log
