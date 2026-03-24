#!/usr/bin/env perl
# create date: 11 Jan 2016
# description:
use strict;
use warnings;
use Data::Dumper;
use Getopt::Std;
our ($opt_t, $opt_c, $opt_h, $opt_b, $opt_p, $opt_i, $opt_o);
my $OBI = "oby";

exit main(@ARGV);

package main;
sub compile_and_copy
{
    system('cd /tmpfs/zhuweng.yzf/src/ && ob-make observer/observer');
    system('./copy.sh /tmpfs/zhuweng.yzf/');
}

sub check_and_download_sysbench
{
    system('if [ ! -x sysbench/sysbench ] ; then rm -f sysbench/sysbench; wget http://11.166.86.153:8877/sysbench -O sysbench/sysbench; chmod +x sysbench/sysbench; fi;');
}

sub run_mysql
{
    my $sql = shift;
    system("echo \"$sql\"|./deploy.py ${OBI}.obmysql");
}

sub modify_params
{
    my $cpu_num = 25;
    run_mysql("alter system set resource_hard_limit = 300");
    run_mysql("create resource unit box1 max_cpu ${cpu_num}, memory_size 30000000000, MIN_CPU 10;");
    run_mysql("create resource pool pool_large unit = 'box1', unit_num = 1, zone_list = ('test');");
    run_mysql("create tenant tt1 replica_num = 1,primary_zone='test', resource_pool_list=('pool_large');");
    run_mysql("alter system set large_query_threshold = '900ms';");
    run_mysql("alter system set enable_record_trace_log = false;");
}

sub purge_plan_cache
{
    run_mysql("alter system flush plan cache;");
}

sub open_perf_event
{
    system("./deploy.py ${OBI}.obs0.kill -50");
}

sub close_perf_event
{
    system("./deploy.py ${OBI}.obs0.kill -51");
}

sub close_all_log
{
    #system("./deploy.py ${OBI}.obs0.kill -42");
    system("./deploy.py ${OBI}.obs0.kill -42");
}

sub run
{
    my ($case_name, $lua_script, $thread, $trace_log_pattern) = @_;
    my $sql_user = 'root@tt1';
    my $sysbench_bin = 'sysbench/sysbench';
    my $sysbench_db = 'test';
    my $sysbench_max_time = 120;
    my $sysbench_table_size = 100000;
    my $other_args = "sql_user=${sql_user} sysbench_bin=${sysbench_bin} sysbench_db=${sysbench_db} sysbench_max_time=${sysbench_max_time} sysbench_table_size=${sysbench_table_size}";
    print "================ $case_name $lua_script $thread ================\n";
    my $sysbench_result_file="sysbench_${case_name}_${thread}.log";
    my $trace_log_file="focus_${case_name}_${thread}";
    my $rt_file = "avg_rt_${case_name}_${thread}";

    system("./deploy.py ${OBI}.reboot");
    modify_params();
    system("./deploy.py ${OBI}.sysbench prepare sysbench_threads=${thread} sysbench_lua=sysbench/${lua_script} ${other_args}");
    purge_plan_cache();
    close_all_log();
    open_perf_event() unless $opt_p;

    system("./deploy.py ${OBI}.sysbench run sysbench_threads=${thread} sysbench_lua=sysbench/${lua_script}  ${other_args}|tee ${sysbench_result_file}");
    close_perf_event();
    system("./deploy.py ${OBI}.stop");
    #system("./deploy.py ${OBI}.obs0.cat|grep \"${trace_log_pattern}\" >${trace_log_file}");
    #system("./trace_log_avg.pl <${trace_log_file} >${rt_file}");
    system("./deploy.py ${OBI}.obs0.cat|grep \"perf_event\" >${trace_log_file}");
    system("./format_perf_event_stat.pl <${trace_log_file} >${rt_file} && cat ${rt_file}");
}

sub print_help
{
    print "run_sysbench [options]\n";
    print "    -c {get|insert|update|tcbuyer|tcbuyer_select}\n";
    print "    -t <thread count>\n";
    print "    -i <deploy_obi_name>\n";
    print "    -h print this message\n";
    print "    -o create tenant etc.\n";
    print "    -p disable all observer log\n\n";
    print "After run, see avg_rt*, sysbench_*.log, focus_* for the results. ";
    print "If no options specified, run all the case with 1,8,16,32 threads seperately. \n";
}

sub main
{
    my @threads = (1, 32, 64);
    my %cases = (
	"get" => ["select.lua", "SELECT pad"],
	"insert" => ["tcbuyer_insert.lua", "insert into   tc_biz_order"],
	"update" => ["tcbuyer_update.lua", "update tc_biz_order SET"],
	"tcbuyer" => ["tcbuyer_test.lua", "trace_log"],
	"tcbuyer_select" => ["tcbuyer_select.lua", "SeLecT"],
	"rand" => ["select_random_points.lua", "SELECT id, k, c, pad"],
  "distributed_dml" => ["distributed_dml"],
    );
    my @cases_name = ("get", "insert", "update", "tcbuyer", "tcbuyer_select", "distributed_dml");
    if (!getopts('t:c:hbpi:o')) {
	return 1;
    } elsif (defined $opt_h) {
	print_help();
    } elsif (defined $opt_o) {
	if (defined $opt_i) {
	    $OBI = $opt_i;
	}
	modify_params();
    } else {
	if (defined $opt_i) {
	    $OBI = $opt_i;
	}
	print "Use ${OBI} as instance\n";
	if (defined $opt_b) {
	    compile_and_copy();
	}
	check_and_download_sysbench();
	if (defined $opt_t && defined $opt_c) {
	    my $case = $opt_c;
	    my $thread = $opt_t;
	    run($case, ${$cases{$case}}[0], $thread, ${$cases{$case}}[1]);
	} elsif (defined $opt_c) {
	    my $case = $opt_c;
	    for my $thread (@threads) {
		run($case, ${$cases{$case}}[0], $thread, ${$cases{$case}}[1]);
	    }
	} elsif (defined $opt_t) {
	    my $thread = $opt_t;
	    for my $case (@cases_name)
	    {
		run($case, ${$cases{$case}}[0], $thread, ${$cases{$case}}[1]);
	    }
	} else {

	    print "Run ALL cases\n";
	    for my $case (@cases_name)
	    {
		for my $thread (@threads)
		{
		    run($case, ${$cases{$case}}[0], $thread, ${$cases{$case}}[1]);
		}
	    }
	}
    }
}
