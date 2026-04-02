#
# set $__tenant_name__ outside
#
--disable_all_result_log
ob1.mysql alter system set minor_compact_trigger = 10 tenant $__tenant_name__;
ob1.mysql alter system set ob_compaction_schedule_interval = '3s' tenant $__tenant_name__;
ob1.mysql alter system set merger_check_interval = '10s' tenant $__tenant_name__;
ob1.mysql alter system set compaction_low_thread_score=20 tenant $__tenant_name__;
ob1.mysql alter system set compaction_high_thread_score=10 tenant $__tenant_name__;
ob1.mysql alter system set _enable_adaptive_compaction = true tenant $__tenant_name__;
ob1.mysql alter system set major_freeze_duty_time = 'Disable' tenant $__tenant_name__;
--enable_all_result_log