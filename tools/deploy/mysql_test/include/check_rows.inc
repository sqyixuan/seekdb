if ($TENANT == 'mysql') {
--replace_regex /version=\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{6}+/version=stat_version/
select dbms_xplan.display_cursor('', 'all');
}
if ($TENANT == 'oracle') {
--replace_regex /version=\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{6}+/version=stat_version/
select * from table(dbms_xplan.display_cursor(format=>'all'));
}