

ob1.mysql alter system set dump_data_dictionary_to_log_interval = '3s' tenant sys;
ob1.mysql alter system set dump_data_dictionary_to_log_interval = '3s' tenant all_user;
ob1.mysql alter system set dump_data_dictionary_to_log_interval = '3s' tenant all_meta;
ob1.check_until_timeout select count(*) from oceanbase.__all_virtual_data_dictionary_in_log where tenant_id=1006, 1, 60;
sleep 6;
#select * from __all_data_dictionary_in_log;
